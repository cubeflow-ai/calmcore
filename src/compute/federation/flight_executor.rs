//! Arrow Flight 执行器 - 封装远程节点调用

use arrow_flight::{FlightClient, Ticket};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::stream::StreamExt;
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

    /// 执行远程 SQL 查询
    pub async fn execute_sql(&self, sql: &str) -> CoreResult<SendableRecordBatchStream> {
        log::debug!(
            "[FlightExecutor] Executing SQL on node '{}': {}",
            self.node_id,
            sql
        );

        let mut client = self.connect().await?;

        // 创建 Ticket（包含 SQL）
        let ticket = Ticket::new(sql.as_bytes().to_vec());

        // 执行 do_get
        let flight_stream = client
            .do_get(ticket)
            .await
            .map_err(|e| CoreError::Network(format!("Flight do_get failed: {}", e)))?;

        log::debug!(
            "[FlightExecutor] Successfully started stream from node '{}'",
            self.node_id
        );

        // 获取 schema
        let schema = flight_stream
            .schema()
            .ok_or_else(|| CoreError::Internal("Flight stream has no schema".to_string()))?
            .clone();

        // 将 FlightRecordBatchStream 转换为 SendableRecordBatchStream
        // 使用 RecordBatchStreamAdapter 包装
        let adapted = flight_stream.map(|result| {
            result.map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, adapted)))
    }

    /// 获取节点 ID
    pub fn node_id(&self) -> &str {
        &self.node_id
    }
}
