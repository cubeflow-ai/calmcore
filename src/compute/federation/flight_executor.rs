//! Arrow Flight 执行器 - 封装远程节点调用

use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::error::FlightError;
use arrow_flight::{FlightClient, Ticket};
use base64::prelude::*;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion_proto::logical_plan::to_proto::serialize_expr;
use datafusion_proto::logical_plan::DefaultLogicalExtensionCodec;
use futures::stream::{self, StreamExt};
use prost::Message;
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
        log::debug!("[FlightExecutor] Connecting to {}", self.endpoint);

        let channel = Channel::from_shared(self.endpoint.clone()).map_err(|e| {
            log::error!(
                "[FlightExecutor] Invalid endpoint '{}': {}",
                self.endpoint,
                e
            );
            CoreError::Internal(format!("Invalid endpoint: {}", e))
        })?;

        let channel = channel.connect().await.map_err(|e| {
            log::error!(
                "[FlightExecutor] Failed to connect to '{}': {}",
                self.endpoint,
                e
            );
            CoreError::Network(format!("Failed to connect: {}", e))
        })?;

        Ok(FlightClient::new(channel))
    }

    /// 执行远程 SQL 查询（带分区过滤）
    pub async fn execute_sql_with_partitions(
        &self,
        sql: &str,
        partition_names: &[String],
    ) -> CoreResult<SendableRecordBatchStream> {
        log::debug!(
            "[FlightExecutor] Executing SQL on '{}': {}",
            self.node_id,
            sql
        );

        let mut client = self.connect().await?;
        log::debug!("✅ [FlightExecutor] Connected to node '{}'", self.node_id);

        // 创建结构化的 Ticket payload
        let ticket_payload = serde_json::json!({
            "sql": sql,
            "partition_names": partition_names,
            "internal": true,
        });
        let ticket_json = serde_json::to_string(&ticket_payload).unwrap();
        log::debug!(
            "📝 [FlightExecutor] Ticket JSON for node '{}': {}",
            self.node_id,
            ticket_json
        );

        let ticket_bytes = serde_json::to_vec(&ticket_payload)
            .map_err(|e| CoreError::Internal(format!("Failed to serialize ticket: {}", e)))?;

        let ticket = Ticket::new(ticket_bytes);

        self.do_get_with_ticket(ticket).await
    }

    /// 执行远程扫描（直接传递 projection/filters/limit，不使用 SQL）
    pub async fn execute_scan(
        &self,
        table_name: &str,
        partition_names: &[String],
        projection: Option<Vec<usize>>,
        filters: Vec<Expr>,
        limit: Option<usize>,
        count_only: bool,
    ) -> CoreResult<SendableRecordBatchStream> {
        log::debug!(
            "[FlightExecutor] Executing scan on '{}': table={}, partitions={:?}, projection={:?}, filters={}, limit={:?}",
            self.node_id,
            table_name,
            partition_names,
            projection,
            filters.len(),
            limit
        );

        // 序列化 filters
        let filters_encoded = if !filters.is_empty() {
            let codec = DefaultLogicalExtensionCodec {};
            let mut proto_filters = Vec::new();

            for filter in &filters {
                let proto_expr = serialize_expr(filter, &codec).map_err(|e| {
                    CoreError::Internal(format!("Failed to serialize filter: {}", e))
                })?;

                // 序列化为 bytes (prost 0.14)
                let buf = proto_expr.encode_to_vec();

                proto_filters.push(BASE64_STANDARD.encode(&buf));
            }

            log::debug!(
                "[FlightExecutor] Serialized {} filters for remote pushdown",
                proto_filters.len()
            );
            Some(proto_filters)
        } else {
            None
        };

        // 创建 scan ticket（包含序列化的 filters 和 count_only）
        let ticket_payload = serde_json::json!({
            "type": "scan",
            "table_name": table_name,
            "partition_names": partition_names,
            "projection": projection,
            "filters": filters_encoded,
            "limit": limit,
            "count_only": count_only,
            "internal": true,
        });

        log::debug!(
            "📝 [FlightExecutor] Scan ticket for node '{}': table={}, {} filters",
            self.node_id,
            table_name,
            filters.len()
        );

        let ticket_bytes = serde_json::to_vec(&ticket_payload)
            .map_err(|e| CoreError::Internal(format!("Failed to serialize ticket: {}", e)))?;

        let ticket = Ticket::new(ticket_bytes);

        self.do_get_with_ticket(ticket).await
    }

    /// 通用的 do_get 实现
    async fn do_get_with_ticket(&self, ticket: Ticket) -> CoreResult<SendableRecordBatchStream> {
        let mut client = self.connect().await?;

        // 执行 do_get
        log::debug!(
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

        log::debug!(
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
                    log::debug!(
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
