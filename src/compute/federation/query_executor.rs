//! 联邦查询执行器 - 使用 datafusion-federation 框架

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::datasource::empty::EmptyTable;
use datafusion::datasource::view::ViewTable;
use datafusion::prelude::*;

use crate::catalog::Catalog;
use crate::cluster::ClusterManager;
use crate::compute::{
    udf::fulltext_udf::{build_score_stream, register_fulltext_udfs},
    NormalizedSql,
};
use crate::engine::Engine;
use crate::utils::error::{CoreError, CoreResult};

use super::flight_executor::FlightExecutor;

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
            log::debug!(
                "📊 [FederatedQueryExecutor] Table '{}' partition distribution: {:?}",
                table_name,
                nodes
                    .iter()
                    .map(|(node, parts)| (node.clone(), parts.len()))
                    .collect::<Vec<_>>()
            );
            table_partition_nodes.insert(table_name.clone(), nodes);
        }

        // 🚀 使用 DataFusion 默认的 SessionState，而不是 datafusion-federation 的
        // 这样避免使用 FederatedQueryPlanner，直接使用我们的 RemoteTableProvider
        let ctx = SessionContext::new();
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

            log::debug!(
                "🔍 [FederatedQueryExecutor] Table '{}' decision: nodes.len()={}, contains_my_node={}, my_node_id={}",
                table_name,
                nodes.len(),
                nodes.contains_key(&my_node_id),
                my_node_id
            );

            if nodes.len() == 1 && nodes.contains_key(&my_node_id) {
                let local_partitions = nodes.get(&my_node_id).cloned().unwrap_or_default();
                if local_partitions.is_empty() {
                    log::debug!("🏠 [FederatedQueryExecutor] Table '{}': All local but no partitions, using empty table", table_name);
                    self.register_empty_table(&ctx, table_name, schema.clone())?;
                } else {
                    log::debug!("🏠 [FederatedQueryExecutor] Table '{}': All {} partition(s) on local node, using local table", table_name, local_partitions.len());
                    self.register_local_table(
                        &ctx,
                        table_name,
                        local_partitions,
                        normalized.needs_score_column,
                    )
                    .await?;
                }
            } else if !nodes.contains_key(&my_node_id) {
                log::debug!(
                    "[FederatedQueryExecutor] Table '{}': Using federated table",
                    table_name
                );
                let (target_node, partitions) = nodes
                    .iter()
                    .next()
                    .map(|(node, partitions)| (node.clone(), partitions.clone()))
                    .ok_or_else(|| CoreError::Internal("No remote nodes found".to_string()))?;
                self.register_federated_table(&ctx, table_name, &target_node, partitions)
                    .await?;
            } else {
                log::debug!("🔀 [FederatedQueryExecutor] Table '{}': Partitions distributed across {} nodes, using merged table", table_name, nodes.len());
                self.register_merged_table(&ctx, table_name, nodes, normalized.needs_score_column)
                    .await?;
            }
        }

        log::debug!("[FederatedQueryExecutor] Parsing SQL: {}", sql);
        let df = ctx
            .sql(sql)
            .await
            .map_err(|e| CoreError::Internal(format!("Failed to parse SQL: {}", e)))?;

        let stream = df
            .execute_stream()
            .await
            .map_err(|e| CoreError::Internal(format!("Failed to execute query: {}", e)))?;

        let stream = if normalized.needs_score_column {
            build_score_stream(stream, fulltext_context.clone(), &normalized.score)
        } else {
            stream
        };

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

        // 🚀 使用我们自己的 RemoteTableProvider 而不是 datafusion-federation
        // 这样可以完全控制投影逻辑，避免 wrap_projection() 自动展开所有列
        let table_info = self.catalog.get_or_load_table(table_name).await?;
        let schema = table_info.table.schema.to_arrow_schema();

        let flight_executor = Arc::new(FlightExecutor::new(remote_node_id.to_string(), grpc_addr));

        let remote_provider = crate::compute::federation::remote_provider::RemoteTableProvider::new(
            table_name.to_string(),
            partition_names,
            schema,
            flight_executor,
        );

        ctx.register_table(table_name, Arc::new(remote_provider))
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

        // 收集本地分区
        let mut local_partitions = Vec::new();
        if let Some(partition_names) = nodes.get(&my_node_id) {
            for partition_name in partition_names {
                match self.engine.get_partition(table_name, partition_name).await {
                    Some(partition) => local_partitions.push(partition),
                    None => log::warn!(
                        "[FederatedQueryExecutor] Partition '{}' not found locally for table '{}'",
                        partition_name,
                        table_name
                    ),
                }
            }
        }

        // 收集远程节点信息
        let mut remote_nodes = Vec::new();
        for (node_id, partition_names) in nodes {
            if node_id == &my_node_id || partition_names.is_empty() {
                continue;
            }

            let grpc_addr = self
                .cluster_manager
                .get_node_grpc_addr(node_id)
                .ok_or_else(|| {
                    CoreError::Internal(format!("Node {} gRPC address not found", node_id))
                })?;

            log::debug!(
                "[register_merged_table] Adding remote node '{}' with {} partitions, addr={}",
                node_id,
                partition_names.len(),
                grpc_addr
            );

            remote_nodes.push(super::mixed_provider::NodePartitions {
                node_id: node_id.clone(),
                partition_names: partition_names.clone(),
                grpc_addr: Some(grpc_addr),
            });
        }

        // 创建 MixedTableProvider
        let mixed_provider = super::mixed_provider::MixedTableProvider::new(
            table_name.to_string(),
            table_schema.clone(),
            my_node_id.clone(),
            local_partitions,
            remote_nodes,
            self.engine.clone(),
            needs_internal_id,
        );

        ctx.register_table(table_name, Arc::new(mixed_provider))
            .map_err(|e| CoreError::Internal(format!("Failed to register mixed table: {}", e)))?;

        log::debug!(
            "[register_merged_table] Registered mixed table '{}'",
            table_name
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
