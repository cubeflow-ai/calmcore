//! 联邦查询执行器 - 使用 datafusion-federation 框架

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::datasource::empty::EmptyTable;
use datafusion::datasource::view::ViewTable;
use datafusion::prelude::*;
use datafusion_federation::sql::{SQLFederationProvider, SQLTableSource};
use datafusion_federation::{default_session_state, FederatedTableProviderAdaptor};

use crate::catalog::Catalog;
use crate::cluster::ClusterManager;
use crate::compute::{
    udf::fulltext_udf::{build_score_stream, register_fulltext_udfs},
    NormalizedSql,
};
use crate::engine::Engine;
use crate::utils::error::{CoreError, CoreResult};

use super::flight_sql_executor::FlightSQLExecutor;

/// 联邦查询执行器
///
/// 使用 datafusion-federation 框架实现分布式查询
///
/// 关键特性：
/// 1. 自动识别可下推的子计划
/// 2. Filter/Projection/Limit/Join/Aggregate 智能下推
/// 3. 跨节点查询优化
pub struct FederatedQueryExecutor {
    catalog: Arc<Catalog>,
    engine: Arc<Engine>,
    cluster_manager: Arc<ClusterManager>,
}

impl FederatedQueryExecutor {
    pub fn new(
        catalog: Arc<Catalog>,
        engine: Arc<Engine>,
        cluster_manager: Arc<ClusterManager>,
    ) -> Self {
        Self {
            catalog,
            engine,
            cluster_manager,
        }
    }

    /// 执行 SQL 查询（返回流）
    pub async fn execute(
        &self,
        normalized: &NormalizedSql,
    ) -> CoreResult<datafusion::physical_plan::SendableRecordBatchStream> {
        self.execute_internal(normalized, None, false).await
    }

    /// 执行本地 SQL 查询（不进行 Federation，只查本地数据）
    pub async fn execute_local(
        &self,
        normalized: &NormalizedSql,
    ) -> CoreResult<datafusion::physical_plan::SendableRecordBatchStream> {
        self.execute_internal(normalized, None, true).await
    }

    /// 带分区提示执行查询
    pub async fn execute_with_partitions(
        &self,
        normalized: &NormalizedSql,
        partition_hint: Option<&[String]>,
        local_only: bool,
    ) -> CoreResult<datafusion::physical_plan::SendableRecordBatchStream> {
        self.execute_internal(normalized, partition_hint, local_only)
            .await
    }

    async fn execute_internal(
        &self,
        normalized: &NormalizedSql,
        partition_hint: Option<&[String]>,
        local_only: bool,
    ) -> CoreResult<datafusion::physical_plan::SendableRecordBatchStream> {
        let sql = &normalized.rewritten_sql;
        log::debug!(
            "[FederatedQueryExecutor] Executing SQL (local_only={}, partitions_hint={:?}): {}",
            local_only,
            partition_hint,
            sql
        );

        let table_names = self.parse_table_names(sql)?;
        if table_names.is_empty() {
            return Err(CoreError::Internal("No tables found in SQL".to_string()));
        }

        let my_node_id = self.cluster_manager.node_id().to_string();
        let partition_hint_set: Option<HashSet<String>> =
            partition_hint.map(|names| names.iter().cloned().collect::<HashSet<String>>());

        // 预先计算每个表需要访问的分区分布
        let mut table_partition_nodes: HashMap<String, HashMap<String, Vec<String>>> =
            HashMap::new();
        let mut table_schemas: HashMap<String, datafusion::arrow::datatypes::SchemaRef> =
            HashMap::new();

        for table_name in &table_names {
            let table_info = self.catalog.get_or_load_table(table_name).await?;
            let partitions = table_info.partitions.read().await;
            let all_partition_names: Vec<String> = partitions.keys().cloned().collect();

            let partition_filter_set = if normalized.partition_filters.scan_all() {
                None
            } else {
                Some(
                    normalized
                        .partition_filters
                        .resolve_partitions(&all_partition_names)
                        .into_iter()
                        .collect::<HashSet<_>>(),
                )
            };

            let mut nodes: HashMap<String, Vec<String>> = HashMap::new();

            for (partition_name, partition_meta) in partitions.iter() {
                if !Self::partition_selected(
                    partition_name,
                    partition_filter_set.as_ref(),
                    partition_hint_set.as_ref(),
                ) {
                    continue;
                }

                nodes
                    .entry(partition_meta.owner.clone())
                    .or_default()
                    .push(partition_name.clone());
            }

            table_schemas.insert(
                table_name.clone(),
                table_info.table.schema.to_arrow_schema(),
            );
            log::info!(
                "📊 [FederatedQueryExecutor] Table '{}' partition distribution: {:?}",
                table_name,
                nodes
                    .iter()
                    .map(|(node, parts)| (node.clone(), parts.len()))
                    .collect::<Vec<_>>()
            );
            table_partition_nodes.insert(table_name.clone(), nodes);
        }

        let state = default_session_state();
        let ctx = SessionContext::new_with_state(state);
        let fulltext_context = register_fulltext_udfs(&ctx);
        if normalized.needs_score_column {
            fulltext_context.clear_scores();
        }

        for table_name in &table_names {
            let schema = table_schemas
                .get(table_name)
                .cloned()
                .ok_or_else(|| CoreError::Internal("Missing table schema".to_string()))?;
            let nodes = table_partition_nodes.get(table_name).unwrap();

            if local_only {
                let local_partitions = nodes.get(&my_node_id).cloned().unwrap_or_default();
                if local_partitions.is_empty() {
                    self.register_empty_table(&ctx, table_name, schema.clone())?;
                } else {
                    self.register_local_table(
                        &ctx,
                        table_name,
                        local_partitions,
                        normalized.needs_score_column,
                    )
                    .await?;
                }
                continue;
            }

            if nodes.is_empty() {
                log::info!("⚠️  [FederatedQueryExecutor] Table '{}' has no partitions, registering empty table", table_name);
                self.register_empty_table(&ctx, table_name, schema.clone())?;
                continue;
            }

            log::info!(
                "🔍 [FederatedQueryExecutor] Table '{}' decision: nodes.len()={}, contains_my_node={}, my_node_id={}",
                table_name,
                nodes.len(),
                nodes.contains_key(&my_node_id),
                my_node_id
            );

            if nodes.len() == 1 && nodes.contains_key(&my_node_id) {
                let local_partitions = nodes.get(&my_node_id).cloned().unwrap_or_default();
                if local_partitions.is_empty() {
                    log::info!("🏠 [FederatedQueryExecutor] Table '{}': All local but no partitions, using empty table", table_name);
                    self.register_empty_table(&ctx, table_name, schema.clone())?;
                } else {
                    log::info!("🏠 [FederatedQueryExecutor] Table '{}': All {} partition(s) on local node, using local table", table_name, local_partitions.len());
                    self.register_local_table(
                        &ctx,
                        table_name,
                        local_partitions,
                        normalized.needs_score_column,
                    )
                    .await?;
                }
            } else if !nodes.contains_key(&my_node_id) {
                log::info!("🌐 [FederatedQueryExecutor] Table '{}': All partition(s) on remote node, using federated table", table_name);
                let (target_node, partitions) = nodes
                    .iter()
                    .next()
                    .map(|(node, partitions)| (node.clone(), partitions.clone()))
                    .ok_or_else(|| CoreError::Internal("No remote nodes found".to_string()))?;
                self.register_federated_table(&ctx, table_name, &target_node, partitions)
                    .await?;
            } else {
                log::info!("🔀 [FederatedQueryExecutor] Table '{}': Partitions distributed across {} nodes, using merged table", table_name, nodes.len());
                self.register_merged_table(&ctx, table_name, nodes, normalized.needs_score_column)
                    .await?;
            }
        }

        log::info!(
            "📝 [FederatedQueryExecutor] All tables registered, parsing SQL: {}",
            sql
        );
        let df = ctx
            .sql(sql)
            .await
            .map_err(|e| CoreError::Internal(format!("Failed to parse SQL: {}", e)))?;
        log::info!("✅ [FederatedQueryExecutor] SQL parsed successfully");

        log::info!("🚀 [FederatedQueryExecutor] Executing query stream...");
        let stream = df
            .execute_stream()
            .await
            .map_err(|e| CoreError::Internal(format!("Failed to execute query: {}", e)))?;
        log::info!("✅ [FederatedQueryExecutor] Query stream created");

        let stream = if normalized.needs_score_column {
            build_score_stream(stream, fulltext_context.clone(), &normalized.score)
        } else {
            stream
        };

        log::info!("✅ [FederatedQueryExecutor] Query execution started");
        Ok(stream)
    }

    /// 注册本地表
    async fn register_local_table(
        &self,
        ctx: &SessionContext,
        table_name: &str,
        partition_names: Vec<String>,
        needs_internal_id: bool,
    ) -> CoreResult<()> {
        if partition_names.is_empty() {
            log::debug!(
                "[FederatedQueryExecutor] Skipping local table '{}' due to empty partition list",
                table_name
            );
            return Ok(());
        }

        let schema = self
            .catalog
            .get_or_load_table(table_name)
            .await?
            .table
            .schema
            .to_arrow_schema();

        if let Some(provider) = self
            .create_local_provider(
                table_name,
                &partition_names,
                schema.clone(),
                needs_internal_id,
            )
            .await?
        {
            ctx.register_table(table_name, provider).map_err(|e| {
                CoreError::Internal(format!("Failed to register local table: {}", e))
            })?;

            log::debug!(
                "[FederatedQueryExecutor] Registered local table '{}' with {} partition(s)",
                table_name,
                partition_names.len()
            );
        } else {
            self.register_empty_table(ctx, table_name, schema)?;
        }
        Ok(())
    }

    /// 注册联邦表（所有分区都在单个远程节点）
    async fn register_federated_table(
        &self,
        ctx: &SessionContext,
        table_name: &str,
        remote_node_id: &str,
        partition_names: Vec<String>,
    ) -> CoreResult<()> {
        if partition_names.is_empty() {
            self.register_empty_table(
                ctx,
                table_name,
                self.catalog
                    .get_or_load_table(table_name)
                    .await?
                    .table
                    .schema
                    .to_arrow_schema(),
            )?;
            return Ok(());
        }

        log::debug!(
            "[FederatedQueryExecutor] Registering federated table '{}' from node '{}' ({} partitions)",
            table_name,
            remote_node_id,
            partition_names.len()
        );

        let grpc_addr = self
            .cluster_manager
            .get_node_grpc_addr(remote_node_id)
            .ok_or_else(|| {
                CoreError::Internal(format!("Node {} gRPC address not found", remote_node_id))
            })?;

        let executor = Arc::new(FlightSQLExecutor::new(
            remote_node_id.to_string(),
            grpc_addr,
            self.catalog.clone(),
            Some(partition_names.clone()),
        ));

        let provider = Arc::new(SQLFederationProvider::new(executor.clone()));

        // 创建 SQLTableSource 然后包装为 FederatedTableProviderAdaptor
        use datafusion_federation::sql::RemoteTableRef;
        let table_ref = RemoteTableRef::parse_with_default_dialect(table_name)
            .map_err(|e| CoreError::Internal(format!("Failed to parse table name: {}", e)))?;
        let table_source = Arc::new(
            SQLTableSource::new(provider, table_ref)
                .await
                .map_err(|e| {
                    CoreError::Internal(format!("Failed to create table source: {}", e))
                })?,
        );
        let federated_provider = FederatedTableProviderAdaptor::new(table_source);

        ctx.register_table(table_name, Arc::new(federated_provider))
            .map_err(|e| {
                CoreError::Internal(format!("Failed to register federated table: {}", e))
            })?;

        log::debug!(
            "[FederatedQueryExecutor] Registered federated table '{}' via node '{}'",
            table_name,
            remote_node_id
        );
        Ok(())
    }

    /// 注册合并表（分区跨多个节点）
    async fn register_merged_table(
        &self,
        ctx: &SessionContext,
        table_name: &str,
        nodes: &HashMap<String, Vec<String>>,
        needs_internal_id: bool,
    ) -> CoreResult<()> {
        let my_node_id = self.cluster_manager.node_id().to_string();
        let table_schema = self
            .catalog
            .get_or_load_table(table_name)
            .await?
            .table
            .schema
            .to_arrow_schema();

        log::debug!(
            "[FederatedQueryExecutor] Registering merged table '{}' from nodes {:?}",
            table_name,
            nodes.keys().collect::<Vec<_>>()
        );

        let mut temp_tables = Vec::new();

        for (node_id, partitions) in nodes {
            if partitions.is_empty() {
                continue;
            }

            let temp_table_name = format!(
                "__{}_{}",
                table_name,
                node_id.replace(|c: char| !c.is_alphanumeric(), "_")
            );

            if node_id == &my_node_id {
                log::info!(
                    "📝 [register_merged_table] Registering local temp table '{}' for node '{}'",
                    temp_table_name,
                    node_id
                );
                if let Some(provider) = self
                    .create_local_provider(
                        table_name,
                        partitions,
                        table_schema.clone(),
                        needs_internal_id,
                    )
                    .await?
                {
                    ctx.register_table(&temp_table_name, provider)
                        .map_err(|e| {
                            CoreError::Internal(format!(
                                "Failed to register local temp table: {}",
                                e
                            ))
                        })?;
                    log::info!(
                        "✅ [register_merged_table] Local temp table '{}' registered",
                        temp_table_name
                    );
                    temp_tables.push(temp_table_name);
                }
            } else {
                log::info!(
                    "🌐 [register_merged_table] Registering remote temp table '{}' for node '{}'",
                    temp_table_name,
                    node_id
                );

                let grpc_addr = self
                    .cluster_manager
                    .get_node_grpc_addr(node_id)
                    .ok_or_else(|| {
                        CoreError::Internal(format!("Node {} gRPC address not found", node_id))
                    })?;
                log::info!(
                    "🔗 [register_merged_table] Resolved gRPC address: {}",
                    grpc_addr
                );

                let executor = Arc::new(FlightSQLExecutor::new(
                    node_id.clone(),
                    grpc_addr,
                    self.catalog.clone(),
                    Some(partitions.clone()),
                ));
                log::info!("🔧 [register_merged_table] Created FlightSQLExecutor");

                let provider = Arc::new(SQLFederationProvider::new(executor.clone()));
                log::info!("🔧 [register_merged_table] Created SQLFederationProvider");

                use datafusion_federation::sql::RemoteTableRef;
                let table_ref =
                    RemoteTableRef::parse_with_default_dialect(table_name).map_err(|e| {
                        CoreError::Internal(format!("Failed to parse table name: {}", e))
                    })?;
                log::info!(
                    "🔧 [register_merged_table] Parsed table ref: {:?}",
                    table_ref
                );

                log::info!("⏳ [register_merged_table] About to call SQLTableSource::new (may trigger schema fetch)...");
                let table_source = Arc::new(
                    SQLTableSource::new(provider, table_ref)
                        .await
                        .map_err(|e| {
                            CoreError::Internal(format!("Failed to create table source: {}", e))
                        })?,
                );
                log::info!("✅ [register_merged_table] SQLTableSource::new completed");

                let federated_provider = FederatedTableProviderAdaptor::new(table_source);

                ctx.register_table(&temp_table_name, Arc::new(federated_provider))
                    .map_err(|e| {
                        CoreError::Internal(format!("Failed to register remote temp table: {}", e))
                    })?;
                log::info!(
                    "✅ [register_merged_table] Remote temp table '{}' registered",
                    temp_table_name
                );
                temp_tables.push(temp_table_name);
            }
        }

        if temp_tables.is_empty() {
            log::info!("⚠️ [register_merged_table] No temp tables created, using empty table");
            self.register_empty_table(ctx, table_name, table_schema)?;
            return Ok(());
        }

        log::info!(
            "🔀 [register_merged_table] Creating union of {} temp tables: {:?}",
            temp_tables.len(),
            temp_tables
        );

        log::info!(
            "📋 [register_merged_table] Step 1: Creating DataFrame from first temp table '{}'",
            &temp_tables[0]
        );
        let mut df = ctx
            .table(&temp_tables[0])
            .await
            .map_err(|e| CoreError::Internal(format!("Failed to create DataFrame: {}", e)))?;
        log::info!("✅ [register_merged_table] Step 1 completed");

        log::info!(
            "📋 [register_merged_table] Step 2: Union remaining {} temp tables",
            temp_tables.len() - 1
        );
        for (idx, temp) in temp_tables.iter().skip(1).enumerate() {
            log::info!(
                "  🔗 [register_merged_table] Unioning temp table {} of {}: '{}'",
                idx + 1,
                temp_tables.len() - 1,
                temp
            );
            let next_df = ctx
                .table(temp)
                .await
                .map_err(|e| CoreError::Internal(format!("Failed to create DataFrame: {}", e)))?;
            df = df
                .union(next_df)
                .map_err(|e| CoreError::Internal(format!("Failed to union DataFrame: {}", e)))?;
            log::info!("  ✅ [register_merged_table] Union {} completed", idx + 1);
        }
        log::info!("✅ [register_merged_table] Step 2 completed");

        log::info!("📋 [register_merged_table] Step 3: Creating ViewTable");
        let plan = df.logical_plan().clone();
        let view = ViewTable::try_new(plan, None)
            .map_err(|e| CoreError::Internal(format!("Failed to create ViewTable: {}", e)))?;
        log::info!("✅ [register_merged_table] Step 3 completed");

        log::info!(
            "📋 [register_merged_table] Step 4: Registering merged view '{}'",
            table_name
        );
        ctx.register_table(table_name, Arc::new(view))
            .map_err(|e| CoreError::Internal(format!("Failed to register merged view: {}", e)))?;

        log::info!(
            "✅ [register_merged_table] Registered merged view '{}' with {} temp source(s)",
            table_name,
            temp_tables.len()
        );
        Ok(())
    }

    async fn create_local_provider(
        &self,
        table_name: &str,
        partition_names: &[String],
        schema: datafusion::arrow::datatypes::SchemaRef,
        needs_internal_id: bool,
    ) -> CoreResult<Option<Arc<crate::compute::UnionTableProvider>>> {
        if partition_names.is_empty() {
            return Ok(None);
        }

        let mut local_partitions = Vec::new();
        for partition_name in partition_names {
            match self.engine.get_partition(table_name, partition_name).await {
                Some(partition) => local_partitions.push(partition),
                None => log::warn!(
                    "[FederatedQueryExecutor] Partition '{}' not found on node for table '{}'",
                    partition_name,
                    table_name
                ),
            }
        }

        if local_partitions.is_empty() {
            return Ok(None);
        }

        let provider = crate::compute::UnionTableProvider::new(
            local_partitions,
            table_name.to_string(),
            self.engine.clone(),
            schema,
            needs_internal_id,
        )?;

        Ok(Some(Arc::new(provider)))
    }

    fn register_empty_table(
        &self,
        ctx: &SessionContext,
        table_name: &str,
        schema: datafusion::arrow::datatypes::SchemaRef,
    ) -> CoreResult<()> {
        let empty = EmptyTable::new(schema);
        ctx.register_table(table_name, Arc::new(empty))
            .map_err(|e| {
                CoreError::Internal(format!(
                    "Failed to register empty table '{}': {}",
                    table_name, e
                ))
            })?;
        Ok(())
    }

    fn partition_selected(
        partition_name: &str,
        filter_set: Option<&HashSet<String>>,
        override_set: Option<&HashSet<String>>,
    ) -> bool {
        let matches_filter = filter_set.map_or(true, |set| set.contains(partition_name));
        let matches_override = override_set.map_or(true, |set| set.contains(partition_name));
        matches_filter && matches_override
    }

    /// 解析 SQL 获取表名
    fn parse_table_names(&self, sql: &str) -> CoreResult<Vec<String>> {
        use sqlparser::ast::Statement;
        use sqlparser::dialect::GenericDialect;
        use sqlparser::parser::Parser;

        let dialect = GenericDialect {};
        let statements = Parser::parse_sql(&dialect, sql)
            .map_err(|e| CoreError::Internal(format!("Failed to parse SQL: {}", e)))?;

        let mut table_names = Vec::new();

        for statement in statements {
            match statement {
                Statement::Query(query) => {
                    self.extract_table_names_from_query(&query, &mut table_names);
                }
                _ => {}
            }
        }

        Ok(table_names)
    }

    /// 从 Query 中提取表名
    fn extract_table_names_from_query(
        &self,
        query: &sqlparser::ast::Query,
        table_names: &mut Vec<String>,
    ) {
        use sqlparser::ast::{SetExpr, TableFactor};

        if let SetExpr::Select(select) = query.body.as_ref() {
            for table_with_joins in &select.from {
                if let TableFactor::Table { name, .. } = &table_with_joins.relation {
                    let table_name = name.to_string();
                    if !table_names.contains(&table_name) {
                        table_names.push(table_name);
                    }
                }
            }
        }
    }
}
