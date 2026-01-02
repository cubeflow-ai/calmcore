//! 联邦查询执行器 - 协调本地和远程查询

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::prelude::*;

use crate::catalog::Catalog;
use crate::cluster::ClusterManager;
use crate::engine::Engine;
use crate::utils::error::{CoreError, CoreResult};

/// 联邦查询执行器
///
/// 负责：
/// 1. 解析 SQL 获取表名
/// 2. 判断分区位置（本地 vs 远程）
/// 3. 创建混合 TableProvider（本地 + 远程）
/// 4. 注册表并执行查询
pub struct FederatedQueryExecutor {
    catalog: Arc<Catalog>,
    engine: Arc<Engine>,
    cluster_manager: Arc<ClusterManager>,
}

impl FederatedQueryExecutor {
    /// 创建新的联邦查询执行器
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
        log::debug!("[FederatedQueryExecutor] Executing SQL: {}", sql);

        // 1. 创建 SessionContext
        let ctx = SessionContext::new();

        // 2. 解析 SQL 获取表名
        let table_names = self.parse_table_names(sql)?;

        // 3. 为每个表创建并注册 TableProvider
        for table_name in table_names {
            self.register_table(&ctx, &table_name).await?;
        }

        // 4. 执行查询
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
                    // 从 SELECT 语句中提取表名
                    self.extract_table_names_from_query(&query, &mut table_names);
                }
                _ => {
                    // 其他语句暂不支持
                }
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

    /// 注册表到 SessionContext
    async fn register_table(&self, ctx: &SessionContext, table_name: &str) -> CoreResult<()> {
        log::debug!("[FederatedQueryExecutor] Registering table: {}", table_name);

        // 1. 加载表元数据
        let table_info = self.catalog.get_or_load_table(table_name).await?;

        // 2. 获取当前节点 ID
        let my_node_id = self.cluster_manager.node_id().to_string();

        // 3. 分类分区：本地 vs 远程
        let mut local_partitions = Vec::new();
        let mut remote_partitions_by_node: HashMap<String, Vec<String>> = HashMap::new();

        for (partition_name, partition_meta) in table_info.partitions.read().await.iter() {
            let owner = &partition_meta.owner;

            if owner == &my_node_id {
                // 本地分区
                if let Some(partition) = self.engine.get_partition(table_name, partition_name).await
                {
                    local_partitions.push(partition);
                }
            } else {
                // 远程分区
                remote_partitions_by_node
                    .entry(owner.clone())
                    .or_default()
                    .push(partition_name.clone());
            }
        }

        // 4. 创建 TableProvider
        if remote_partitions_by_node.is_empty() {
            // 纯本地查询：使用 UnionTableProvider
            let provider = crate::compute::UnionTableProvider::new(
                local_partitions,
                table_name.to_string(),
                self.engine.clone(),
                table_info.table.schema.to_arrow_schema(),
            )?;
            ctx.register_table(table_name, Arc::new(provider))
                .map_err(|e| CoreError::Internal(format!("Failed to register table: {}", e)))?;

            log::debug!(
                "[FederatedQueryExecutor] Registered local table: {}",
                table_name
            );
        } else {
            // 混合查询：本地 + 远程
            // TODO: 实现混合 Provider
            log::warn!(
                "[FederatedQueryExecutor] Remote partitions detected but federation not fully implemented yet"
            );

            // 暂时只注册本地分区
            if !local_partitions.is_empty() {
                let provider = crate::compute::UnionTableProvider::new(
                    local_partitions,
                    table_name.to_string(),
                    self.engine.clone(),
                    table_info.table.schema.to_arrow_schema(),
                )?;
                ctx.register_table(table_name, Arc::new(provider))
                    .map_err(|e| CoreError::Internal(format!("Failed to register table: {}", e)))?;
            }
        }

        Ok(())
    }
}
