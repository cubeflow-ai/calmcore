//! Flight SQL Executor - 实现 datafusion-federation 的 SQLExecutor trait

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::Statistics;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::sql::unparser::dialect::{DefaultDialect, Dialect};
use datafusion_federation::sql::SQLExecutor;
use std::sync::Arc;

use crate::catalog::Catalog;
use crate::compute::federation::flight_executor::FlightExecutor;

/// Flight SQL Executor
///
/// 封装 FlightExecutor 并实现 datafusion-federation 的 SQLExecutor trait
pub struct FlightSQLExecutor {
    node_id: String,
    grpc_addr: String,
    flight_executor: Arc<FlightExecutor>,
    catalog: Arc<Catalog>,
    partition_names: Option<Vec<String>>,
}

impl FlightSQLExecutor {
    pub fn new(
        node_id: String,
        grpc_addr: String,
        catalog: Arc<Catalog>,
        partition_names: Option<Vec<String>>,
    ) -> Self {
        let flight_executor = Arc::new(FlightExecutor::new(node_id.clone(), grpc_addr.clone()));
        Self {
            node_id,
            grpc_addr,
            flight_executor,
            catalog,
            partition_names,
        }
    }
}

#[async_trait]
impl SQLExecutor for FlightSQLExecutor {
    fn name(&self) -> &str {
        "calm_flight"
    }

    fn compute_context(&self) -> Option<String> {
        // 使用 node_id 作为 compute context，确保不同节点不会混淆
        Some(self.node_id.clone())
    }

    fn dialect(&self) -> Arc<dyn Dialect> {
        // 使用 DataFusion 的默认 SQL 方言
        Arc::new(DefaultDialect {})
    }

    fn execute(
        &self,
        query: &str,
        _schema: SchemaRef,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        log::debug!(
            "[FlightSQLExecutor] Executing SQL on node {}: {}",
            self.node_id,
            query
        );

        let partition_hint = self.partition_names.clone();
        // 使用 tokio 的 block_in_place 在同步上下文中执行异步代码
        let stream = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async move {
                let result = if let Some(names) = partition_hint.as_ref() {
                    self.flight_executor
                        .execute_sql_with_partitions(query, names)
                        .await
                } else {
                    self.flight_executor.execute_sql(query).await
                };

                result.map_err(|e| DataFusionError::External(Box::new(e)))
            })
        })?;

        Ok(stream)
    }

    async fn statistics(&self, _plan: &LogicalPlan) -> DataFusionResult<Statistics> {
        // 暂时返回未知统计信息
        // TODO: 可以通过 RPC 从远程节点获取统计信息
        Ok(Statistics::new_unknown(_plan.schema().as_arrow()))
    }

    async fn table_names(&self) -> DataFusionResult<Vec<String>> {
        // 从本地 Catalog 获取表列表
        let table_names = self.catalog.list_tables().await;
        Ok(table_names)
    }

    async fn get_table_schema(&self, table_name: &str) -> DataFusionResult<SchemaRef> {
        log::debug!(
            "[FlightSQLExecutor] Getting table schema for '{}' from local catalog",
            table_name
        );

        // 从本地 Catalog 获取表的 schema
        let table_info = self
            .catalog
            .get_or_load_table(table_name)
            .await
            .map_err(|e| {
                DataFusionError::Plan(format!(
                    "Failed to get table '{}' from catalog: {}",
                    table_name, e
                ))
            })?;

        // 将 Calm Schema 转换为 Arrow SchemaRef
        let arrow_schema = table_info.table.schema.to_arrow_schema();
        Ok(arrow_schema)
    }
}
