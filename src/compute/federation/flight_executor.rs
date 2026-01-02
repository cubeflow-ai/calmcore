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
        let channel = Channel::from_shared(self.endpoint.clone())
            .map_err(|e| CoreError::Internal(format!("Invalid endpoint: {}", e)))?
            .connect()
            .await
            .map_err(|e| CoreError::Network(format!("Failed to connect: {}", e)))?;

        Ok(FlightClient::new(channel))
    }

    /// 执行远程 SQL 查询（带分区过滤）
    pub async fn execute_sql_with_partitions(
        &self,
        sql: &str,
        partition_names: &[String],
    ) -> CoreResult<SendableRecordBatchStream> {
        log::debug!(
            "[FlightExecutor] Executing SQL on node '{}' with partitions {:?}: {}",
            self.node_id,
            partition_names,
            sql
        );

        let mut client = self.connect().await?;

        // 创建结构化的 Ticket payload
        let ticket_payload = serde_json::json!({
            "sql": sql,
            "partition_names": partition_names,
            "internal": true,
        });
        let ticket_bytes = serde_json::to_vec(&ticket_payload)
            .map_err(|e| CoreError::Internal(format!("Failed to serialize ticket: {}", e)))?;

        let ticket = Ticket::new(ticket_bytes);

        // 执行 do_get
        let mut flight_stream = client
            .do_get(ticket)
            .await
            .map_err(|e| CoreError::Network(format!("Flight do_get failed: {}", e)))?;

        log::debug!(
            "[FlightExecutor] Successfully started stream from node '{}'",
            self.node_id
        );

        // FlightRecordBatchStream 的 schema 只有在收到第一条消息后才会填充，
        // 因此需要在缺失时主动触发一次读取
        if flight_stream.schema().is_none() {
            let first_batch = flight_stream.next().await.ok_or_else(|| {
                CoreError::Internal("Flight stream ended before schema".to_string())
            })?;

            let schema = flight_stream
                .schema()
                .ok_or_else(|| CoreError::Internal("Flight stream has no schema".to_string()))?
                .clone();

            let stream = stream::once(async { first_batch }).chain(flight_stream);
            return Self::adapt_record_batch_stream(schema, stream);
        }

        let schema = flight_stream
            .schema()
            .ok_or_else(|| CoreError::Internal("Flight stream has no schema".to_string()))?
            .clone();

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
