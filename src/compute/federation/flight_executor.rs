//! Arrow Flight 执行器 - 封装远程节点调用

use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::error::FlightError;
use arrow_flight::{FlightClient, Ticket};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::stream::{self, StreamExt};
use tonic::transport::Channel;

use crate::utils::error::{CoreError, CoreResult};

/// Flight 执行器 - 负责与远程节点通信
#[derive(Clone)]
pub struct FlightExecutor {
    /// 远程节点 ID
    node_id: String,
    /// Flight 端点地址
    endpoint: String,
}

impl FlightExecutor {
    /// 创建新的 Flight 执行器
    pub fn new(node_id: String, endpoint: String) -> Self {
        Self { node_id, endpoint }
    }

    /// 连接到远程节点
    async fn connect(&self) -> CoreResult<FlightClient> {
        log::info!(
            "🔌 [FlightExecutor::connect] Creating channel to endpoint: {}",
            self.endpoint
        );

        let channel = Channel::from_shared(self.endpoint.clone()).map_err(|e| {
            log::error!(
                "❌ [FlightExecutor::connect] Invalid endpoint '{}': {}",
                self.endpoint,
                e
            );
            CoreError::Internal(format!("Invalid endpoint: {}", e))
        })?;

        log::info!("🔌 [FlightExecutor::connect] Channel created, connecting...");

        let channel = channel.connect().await.map_err(|e| {
            log::error!(
                "❌ [FlightExecutor::connect] Failed to connect to '{}': {}",
                self.endpoint,
                e
            );
            CoreError::Network(format!("Failed to connect: {}", e))
        })?;

        log::info!(
            "✅ [FlightExecutor::connect] Connected successfully to {}",
            self.endpoint
        );
        Ok(FlightClient::new(channel))
    }

    /// 执行远程 SQL 查询（带分区过滤）
    pub async fn execute_sql_with_partitions(
        &self,
        sql: &str,
        partition_names: &[String],
    ) -> CoreResult<SendableRecordBatchStream> {
        log::info!(
            "🌐 [FlightExecutor] Executing SQL on node '{}' (endpoint: {}) with partitions {:?}: {}",
            self.node_id,
            self.endpoint,
            partition_names,
            sql
        );

        log::info!(
            "🔌 [FlightExecutor] Connecting to node '{}'...",
            self.node_id
        );
        let mut client = self.connect().await?;
        log::info!("✅ [FlightExecutor] Connected to node '{}'", self.node_id);

        // 创建结构化的 Ticket payload
        let ticket_payload = serde_json::json!({
            "sql": sql,
            "partition_names": partition_names,
            "internal": true,
        });
        let ticket_json = serde_json::to_string(&ticket_payload).unwrap();
        log::info!(
            "📝 [FlightExecutor] Ticket JSON for node '{}': {}",
            self.node_id,
            ticket_json
        );

        let ticket_bytes = serde_json::to_vec(&ticket_payload)
            .map_err(|e| CoreError::Internal(format!("Failed to serialize ticket: {}", e)))?;

        let ticket = Ticket::new(ticket_bytes);

        // 执行 do_get
        log::info!(
            "📡 [FlightExecutor] Calling do_get on node '{}'...",
            self.node_id
        );
        let mut flight_stream = client.do_get(ticket).await.map_err(|e| {
            log::error!(
                "❌ [FlightExecutor] do_get failed for node '{}': {}",
                self.node_id,
                e
            );
            CoreError::Network(format!("Flight do_get failed: {}", e))
        })?;

        log::info!(
            "✅ [FlightExecutor] do_get succeeded, stream started from node '{}'",
            self.node_id
        );

        // FlightRecordBatchStream 的 schema 只有在收到第一条消息后才会填充，
        // 因此需要在缺失时主动触发一次读取
        if flight_stream.schema().is_none() {
            log::info!(
                "📋 [FlightExecutor] Schema not available, fetching first item from node '{}'",
                self.node_id
            );
            let first_item = flight_stream.next().await;

            let schema = flight_stream
                .schema()
                .ok_or_else(|| CoreError::Internal("Flight stream has no schema".to_string()))?
                .clone();

            match first_item {
                Some(batch) => {
                    log::info!(
                        "✅ [FlightExecutor] Received first batch from node '{}'",
                        self.node_id
                    );
                    let stream = stream::once(async { batch }).chain(flight_stream);
                    return Self::adapt_record_batch_stream(schema, stream);
                }
                None => {
                    log::info!(
                        "⚠️ [FlightExecutor] Empty stream from node '{}'",
                        self.node_id
                    );
                    let empty_stream = stream::empty::<
                        Result<datafusion::arrow::record_batch::RecordBatch, FlightError>,
                    >();
                    return Self::adapt_record_batch_stream(schema, empty_stream);
                }
            }
        }

        let schema = flight_stream
            .schema()
            .ok_or_else(|| CoreError::Internal("Flight stream has no schema".to_string()))?
            .clone();

        log::info!(
            "✅ [FlightExecutor] Stream ready from node '{}'",
            self.node_id
        );
        Self::adapt_record_batch_stream(schema, flight_stream)
    }

    /// 执行远程 SQL 查询（向后兼容，不带分区过滤）
    pub async fn execute_sql(&self, sql: &str) -> CoreResult<SendableRecordBatchStream> {
        // 不指定分区，远程节点会扫描所有本地分区
        self.execute_sql_with_partitions(sql, &[]).await
    }

    /// 获取节点 ID
    pub fn node_id(&self) -> &str {
        &self.node_id
    }
}

impl FlightExecutor {
    fn adapt_record_batch_stream<S>(
        schema: datafusion::arrow::datatypes::SchemaRef,
        stream: S,
    ) -> CoreResult<SendableRecordBatchStream>
    where
        S: futures::Stream<
                Item = Result<datafusion::arrow::record_batch::RecordBatch, FlightError>,
            > + Send
            + 'static,
    {
        let adapted = stream
            .map(|batch| {
                batch.map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))
            })
            .boxed();

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, adapted)))
    }
}
