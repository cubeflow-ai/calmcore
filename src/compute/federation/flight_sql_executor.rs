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
use tokio::runtime::Runtime;

use crate::catalog::Catalog;
use crate::compute::federation::flight_executor::FlightExecutor;

// 全局独立的 tokio runtime，专门用于执行远程查询
// 使用 lazy_static 确保只初始化一次
use std::sync::OnceLock;

static QUERY_RUNTIME: OnceLock<Runtime> = OnceLock::new();

fn get_query_runtime() -> &'static Runtime {
    QUERY_RUNTIME.get_or_init(|| {
        log::info!("🚀 [FlightSQLExecutor] Creating dedicated query runtime");
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(16)
            .thread_name("calm-query")
            .enable_all()
            .build()
            .expect("Failed to create query runtime")
    })
}

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
        log::info!(
            "🌐 [FlightSQLExecutor] Executing SQL on remote node '{}' with partitions {:?}: {}",
            self.node_id,
            self.partition_names,
            query
        );

        let partition_hint = self.partition_names.clone();
        let node_id = self.node_id.clone();
        let flight_executor = self.flight_executor.clone();
        let query = query.to_string();

        // 🔧 关键修复：使用全局独立的 runtime 避免死锁
        // DataFusion 的 execute() 是同步方法，会阻塞调用线程
        // 使用全局独立 runtime 的 spawn() 执行异步任务，通过 channel 同步获取结果
        log::info!(
            "🚀 [FlightSQLExecutor] Spawning task on global query runtime for node '{}'",
            node_id
        );

        let (tx, rx) = std::sync::mpsc::sync_channel(1);

        // 获取全局独立的查询 runtime
        let runtime = get_query_runtime();

        // 在独立 runtime 上执行异步任务
        runtime.spawn(async move {
            log::info!(
                "📡 [FlightSQLExecutor] Async task started for node '{}'",
                node_id
            );

            let result = if let Some(names) = partition_hint.as_ref() {
                flight_executor
                    .execute_sql_with_partitions(&query, names)
                    .await
            } else {
                flight_executor.execute_sql(&query).await
            };

            log::info!(
                "✅ [FlightSQLExecutor] Remote call completed for node '{}'",
                node_id
            );

            let _ = tx.send(result.map_err(|e| DataFusionError::External(Box::new(e))));
        });

        log::info!("⏳ [FlightSQLExecutor] Waiting for result from runtime...");

        // 阻塞等待结果
        let stream = rx
            .recv()
            .map_err(|e| {
                DataFusionError::External(Box::new(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Channel recv failed: {}", e),
                )))
            })?
            .map_err(|e| {
                log::error!("❌ [FlightSQLExecutor] Remote call failed: {}", e);
                e
            })?;

        log::info!(
            "✅ [FlightSQLExecutor] Stream obtained from node '{}'",
            self.node_id
        );
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
