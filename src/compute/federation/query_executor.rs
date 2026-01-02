//! 联邦查询执行器 - 使用 datafusion-federation 框架

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::datasource::view::ViewTable;
use datafusion::prelude::*;
use datafusion_federation::sql::{SQLFederationProvider, SQLTableSource};
use datafusion_federation::{default_session_state, FederatedTableProviderAdaptor};

use crate::catalog::Catalog;
use crate::cluster::ClusterManager;
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
        sql: &str,
    ) -> CoreResult<datafusion::physical_plan::SendableRecordBatchStream> {
        self.execute_internal(sql, false).await
    }

    /// 执行本地 SQL 查询（不进行 Federation，只查本地数据）
    pub async fn execute_local(
        &self,
        sql: &str,
    ) -> CoreResult<datafusion::physical_plan::SendableRecordBatchStream> {
        self.execute_internal(sql, true).await
    }

    async fn execute_internal(
        &self,
        sql: &str,
        local_only: bool,
    ) -> CoreResult<datafusion::physical_plan::SendableRecordBatchStream> {
        log::debug!(
            "[FederatedQueryExecutor] Executing SQL (local_only={}): {}",
            local_only,
            sql
        );

        // 1. 解析 SQL 获取表名
        let table_names = self.parse_table_names(sql)?;

        if table_names.is_empty() {
            return Err(CoreError::Internal("No tables found in SQL".to_string()));
        }

        // 2. 检查每个表的分区分布
        let my_node_id = self.cluster_manager.node_id().to_string();

        // 按表名分组统计分区分布
        let mut table_partition_nodes: HashMap<String, Vec<String>> = HashMap::new();

        for table_name in &table_names {
            let table_info = self.catalog.get_or_load_table(table_name).await?;
            let partitions = table_info.partitions.read().await;

            let mut nodes = Vec::new();
            for (_partition_name, partition_meta) in partitions.iter() {
                if !nodes.contains(&partition_meta.owner) {
                    nodes.push(partition_meta.owner.clone());
                }
            }

            table_partition_nodes.insert(table_name.clone(), nodes);
        }

        // 3. 创建 SessionContext with federation optimizer rules
        let state = default_session_state();
        let ctx = SessionContext::new_with_state(state);

        // 4. 为每个表创建统一的 provider（合并本地和远程）
        for table_name in &table_names {
            if local_only {
                // 强制只注册本地表
                self.register_local_table(&ctx, table_name).await?;
            } else {
                let nodes = table_partition_nodes.get(table_name).unwrap();

                log::debug!(
                    "[FederatedQueryExecutor] Table '{}' has partitions on {} node(s): {:?}",
                    table_name,
                    nodes.len(),
                    nodes
                );

                if nodes.len() == 1 && nodes[0] == my_node_id {
                    // 情况1: 所有分区都在本地 - 直接注册本地表
                    self.register_local_table(&ctx, table_name).await?;
                } else if nodes.iter().all(|n| n != &my_node_id) {
                    // 情况2: 所有分区都在远程 - 使用 federation
                    // 选择第一个节点作为查询目标（因为每个节点都有完整的 catalog）
                    let target_node = &nodes[0];
                    self.register_federated_table(&ctx, table_name, target_node)
                        .await?;
                } else {
                    // 情况3: 分区跨多个节点（包含本地和远程）- 需要合并
                    // 使用 UNION ALL 方式合并本地和所有远程节点的数据
                    self.register_merged_table(&ctx, table_name, nodes).await?;
                }
            }
        }

        // 6. 执行查询
        let df = ctx
            .sql(sql)
            .await
            .map_err(|e| CoreError::Internal(format!("Failed to parse SQL: {}", e)))?;

        let stream = df
            .execute_stream()
            .await
            .map_err(|e| CoreError::Internal(format!("Failed to execute query: {}", e)))?;

        log::debug!("[FederatedQueryExecutor] Query execution started");
        Ok(stream)
    }

    /// 注册本地表
    async fn register_local_table(&self, ctx: &SessionContext, table_name: &str) -> CoreResult<()> {
        let table_info = self.catalog.get_or_load_table(table_name).await?;
        let my_node_id = self.cluster_manager.node_id().to_string();

        // 获取本地分区
        let mut local_partitions = Vec::new();
        for (partition_name, partition_meta) in table_info.partitions.read().await.iter() {
            if partition_meta.owner == my_node_id {
                if let Some(partition) = self.engine.get_partition(table_name, partition_name).await
                {
                    local_partitions.push(partition);
                }
            }
        }

        if local_partitions.is_empty() {
            log::warn!(
                "[FederatedQueryExecutor] No local partitions for table {}",
                table_name
            );
            return Ok(());
        }

        let schema = table_info.table.schema.to_arrow_schema();
        let provider = crate::compute::UnionTableProvider::new(
            local_partitions,
            table_name.to_string(),
            self.engine.clone(),
            schema,
        )?;

        ctx.register_table(table_name, Arc::new(provider))
            .map_err(|e| CoreError::Internal(format!("Failed to register local table: {}", e)))?;

        log::debug!(
            "[FederatedQueryExecutor] Registered local table: {}",
            table_name
        );
        Ok(())
    }

    /// 注册联邦表（所有分区都在单个远程节点）
    async fn register_federated_table(
        &self,
        ctx: &SessionContext,
        table_name: &str,
        remote_node_id: &str,
    ) -> CoreResult<()> {
        log::debug!(
            "[FederatedQueryExecutor] Registering federated table '{}' from node '{}'",
            table_name,
            remote_node_id
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
            "[FederatedQueryExecutor] Registered federated table: {}",
            table_name
        );
        Ok(())
    }

    /// 注册合并表（分区跨多个节点）
    async fn register_merged_table(
        &self,
        ctx: &SessionContext,
        table_name: &str,
        nodes: &[String],
    ) -> CoreResult<()> {
        log::debug!(
            "[FederatedQueryExecutor] Registering merged table '{}' from {} nodes: {:?}",
            table_name,
            nodes.len(),
            nodes
        );

        let my_node_id = self.cluster_manager.node_id().to_string();
        let table_info = self.catalog.get_or_load_table(table_name).await?;
        let schema = table_info.table.schema.to_arrow_schema();

        // 临时表名列表
        let mut temp_tables = Vec::new();

        for node_id in nodes {
            // 生成唯一的临时表名
            let temp_table_name = format!(
                "__{}_{}",
                table_name,
                node_id.replace(|c: char| !c.is_alphanumeric(), "_")
            );

            if node_id == &my_node_id {
                // 本地节点 - 创建 UnionTableProvider
                let mut local_partitions = Vec::new();

                for (partition_name, partition_meta) in table_info.partitions.read().await.iter() {
                    if &partition_meta.owner == node_id {
                        if let Some(partition) =
                            self.engine.get_partition(table_name, partition_name).await
                        {
                            local_partitions.push(partition);
                        }
                    }
                }

                if !local_partitions.is_empty() {
                    let provider = crate::compute::UnionTableProvider::new(
                        local_partitions,
                        table_name.to_string(),
                        self.engine.clone(),
                        schema.clone(),
                    )?;
                    ctx.register_table(&temp_table_name, Arc::new(provider))
                        .map_err(|e| {
                            CoreError::Internal(format!(
                                "Failed to register local temp table: {}",
                                e
                            ))
                        })?;
                    temp_tables.push(temp_table_name);
                }
            } else {
                // 远程节点 - 创建 FederatedTableProviderAdaptor
                let grpc_addr = self
                    .cluster_manager
                    .get_node_grpc_addr(node_id)
                    .ok_or_else(|| {
                        CoreError::Internal(format!("Node {} gRPC address not found", node_id))
                    })?;

                let executor = Arc::new(FlightSQLExecutor::new(
                    node_id.clone(),
                    grpc_addr,
                    self.catalog.clone(),
                ));

                let provider = Arc::new(SQLFederationProvider::new(executor.clone()));

                use datafusion_federation::sql::RemoteTableRef;
                let table_ref =
                    RemoteTableRef::parse_with_default_dialect(table_name).map_err(|e| {
                        CoreError::Internal(format!("Failed to parse table name: {}", e))
                    })?;
                let table_source = Arc::new(
                    SQLTableSource::new(provider, table_ref)
                        .await
                        .map_err(|e| {
                            CoreError::Internal(format!("Failed to create table source: {}", e))
                        })?,
                );
                let federated_provider = FederatedTableProviderAdaptor::new(table_source);

                ctx.register_table(&temp_table_name, Arc::new(federated_provider))
                    .map_err(|e| {
                        CoreError::Internal(format!("Failed to register remote temp table: {}", e))
                    })?;
                temp_tables.push(temp_table_name);
            }
        }

        if temp_tables.is_empty() {
            return Err(CoreError::Internal(format!(
                "No sources found for table '{}'",
                table_name
            )));
        }

        // 创建 View 来合并所有临时表
        // 使用 DataFrame API 构建 Union Plan
        let mut df = ctx
            .table(&temp_tables[0])
            .await
            .map_err(|e| CoreError::Internal(format!("Failed to create DataFrame: {}", e)))?;

        for i in 1..temp_tables.len() {
            let next_df = ctx
                .table(&temp_tables[i])
                .await
                .map_err(|e| CoreError::Internal(format!("Failed to create DataFrame: {}", e)))?;
            df = df
                .union(next_df)
                .map_err(|e| CoreError::Internal(format!("Failed to union DataFrame: {}", e)))?;
        }

        // 获取 LogicalPlan 并创建 ViewTable
        let plan = df.logical_plan().clone();
        let view = ViewTable::try_new(plan, None)
            .map_err(|e| CoreError::Internal(format!("Failed to create ViewTable: {}", e)))?;

        ctx.register_table(table_name, Arc::new(view))
            .map_err(|e| CoreError::Internal(format!("Failed to register merged view: {}", e)))?;

        log::debug!(
            "[FederatedQueryExecutor] Registered merged view: {} ({} sources)",
            table_name,
            temp_tables.len()
        );
        Ok(())
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
