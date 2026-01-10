//! Arrow Flight 服务 - 用于分布式查询的数据传输
//!
//! 实现 Arrow Flight 协议，提供高效的 RecordBatch 传输能力

use std::pin::Pin;
use std::sync::Arc;

use arrow_flight::{
    encode::FlightDataEncoderBuilder,
    error::FlightError,
    flight_service_server::{FlightService as ArrowFlightService, FlightServiceServer},
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PutResult, SchemaResult, Ticket,
};
use base64::prelude::*;
use datafusion::arrow::ipc::writer::IpcWriteOptions;
use datafusion::logical_expr::Expr;
use datafusion_proto::logical_plan::from_proto;
use datafusion_proto::logical_plan::DefaultLogicalExtensionCodec;
use futures::{Stream, StreamExt, TryStreamExt};
use tonic::{Request, Response, Status, Streaming};

use crate::calm::CalmService;

/// Arrow Flight 服务实现
///
/// 允许其他节点通过 Arrow Flight 协议查询本地数据
pub struct CalmFlightService {
    calm_service: Arc<CalmService>,
}

impl CalmFlightService {
    pub fn new(calm_service: Arc<CalmService>) -> Self {
        Self { calm_service }
    }

    /// 处理 SQL ticket
    async fn handle_sql_ticket(
        &self,
        json: &serde_json::Value,
        ticket_str: &str,
    ) -> Result<datafusion::physical_plan::SendableRecordBatchStream, Status> {
        let sql = json
            .get("sql")
            .and_then(|v| v.as_str())
            .ok_or_else(|| Status::invalid_argument("Missing 'sql' field in ticket"))?
            .to_string();

        let is_internal = json
            .get("internal")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        let partition_hint: Option<Vec<String>> = json
            .get("partition_names")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|value| value.as_str().map(|s| s.to_string()))
                    .collect::<Vec<_>>()
            })
            .filter(|partitions| !partitions.is_empty());

        log::info!(
            "🛫️  [Flight Service] Executing SQL query (internal={}, partitions={:?}): {}",
            is_internal,
            partition_hint,
            sql
        );
        log::debug!("[Flight Service] Full ticket JSON: {}", ticket_str);

        // 执行查询
        let stream = if is_internal {
            if let Some(partitions) = partition_hint.as_ref() {
                self.calm_service
                    .execute_local_query_stream_with_partitions(&sql, partitions)
                    .await
                    .map_err(|e| {
                        Status::internal(format!(
                            "Local query execution failed (partitions): {}",
                            e
                        ))
                    })?
            } else {
                self.calm_service
                    .execute_local_query_stream(&sql)
                    .await
                    .map_err(|e| Status::internal(format!("Local query execution failed: {}", e)))?
            }
        } else {
            self.calm_service
                .execute_query_stream(&sql)
                .await
                .map_err(|e| Status::internal(format!("Query execution failed: {}", e)))?
        };

        Ok(stream)
    }

    /// 处理 scan ticket（直接 partition 扫描，支持 projection/filters/limit pushdown）
    async fn handle_scan_ticket(
        &self,
        json: &serde_json::Value,
    ) -> Result<datafusion::physical_plan::SendableRecordBatchStream, Status> {
        let table_name = json
            .get("table_name")
            .and_then(|v| v.as_str())
            .ok_or_else(|| Status::invalid_argument("Missing 'table_name' in scan ticket"))?
            .to_string();

        let partition_names: Vec<String> = json
            .get("partition_names")
            .and_then(|v| v.as_array())
            .ok_or_else(|| Status::invalid_argument("Missing 'partition_names' in scan ticket"))?
            .iter()
            .filter_map(|v| v.as_str().map(|s| s.to_string()))
            .collect();

        let projection: Option<Vec<usize>> =
            json.get("projection")
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_u64().map(|n| n as usize))
                        .collect()
                });

        let limit: Option<usize> = json
            .get("limit")
            .and_then(|v| v.as_u64())
            .map(|n| n as usize);

        let count_only: bool = json
            .get("count_only")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        // 反序列化 filters
        let filters: Vec<Expr> = if let Some(filters_array) =
            json.get("filters").and_then(|v| v.as_array())
        {
            let codec = DefaultLogicalExtensionCodec {};
            let ctx = datafusion::prelude::SessionContext::new();
            let mut exprs = Vec::new();

            for filter_value in filters_array {
                if let Some(encoded_str) = filter_value.as_str() {
                    // Base64 解码
                    let bytes = BASE64_STANDARD
                        .decode(encoded_str)
                        .map_err(|e| Status::internal(format!("Failed to decode filter: {}", e)))?;

                    // Protobuf 解码 (prost 0.14)
                    use prost::Message;
                    let proto_expr =
                        datafusion_proto::protobuf::LogicalExprNode::decode(bytes.as_slice())
                            .map_err(|e| {
                                Status::internal(format!("Failed to parse filter protobuf: {}", e))
                            })?;

                    // 转换为 Expr
                    let expr = from_proto::parse_expr(&proto_expr, &ctx, &codec).map_err(|e| {
                        Status::internal(format!("Failed to deserialize filter: {}", e))
                    })?;

                    exprs.push(expr);
                }
            }

            log::debug!(
                "🛫️  [Flight Service] Deserialized {} filters for local execution",
                exprs.len()
            );
            exprs
        } else {
            Vec::new()
        };

        log::info!(
            "🛫️  [Flight Service] Executing scan: table={}, partitions={:?}, projection={:?}, filters={}, limit={:?}, count_only={}",
            table_name,
            partition_names,
            projection,
            filters.len(),
            limit,
            count_only
        );

        // 执行本地 partition 扫描
        let stream = self
            .calm_service
            .execute_local_partition_scan(&table_name, &partition_names, projection, filters, limit, count_only)
            .await
            .map_err(|e| Status::internal(format!("Local partition scan failed: {}", e)))?;

        Ok(stream)
    }

    /// 创建 FlightServiceServer 用于 Tonic gRPC
    pub fn into_server(self) -> FlightServiceServer<Self> {
        FlightServiceServer::new(self)
    }

    /// 处理 Flight Action（控制面操作）
    async fn handle_action(
        &self,
        action: super::flight_actions::FlightAction,
    ) -> crate::utils::error::CoreResult<super::flight_actions::FlightActionResponse> {
        use super::flight_actions::{FlightAction, FlightActionResponse};
        use crate::utils::error::CoreError;

        match action {
            FlightAction::ListTables => {
                let tables = self.calm_service.catalog.list_tables().await;
                Ok(FlightActionResponse::TableList(tables))
            }

            FlightAction::IsCoordinator => {
                let is_coord = self.calm_service.cluster_manager.am_i_coord_node();
                Ok(FlightActionResponse::Bool(is_coord))
            }

            FlightAction::ListAllPartitions => {
                let partitions = self.calm_service.engine.list_all_spartitions().await;
                Ok(FlightActionResponse::AllPartitions(partitions))
            }

            FlightAction::ListPartitions { table_name } => {
                let partitions = self
                    .calm_service
                    .catalog
                    .get_partition_names(&table_name)
                    .await?;
                Ok(FlightActionResponse::PartitionList(partitions))
            }

            // 其他操作需要 coordinator 路由，暂时返回错误提示
            _ => Err(CoreError::Internal(
                "This action requires coordinator routing. Use tarpc RPC for now.".to_string(),
            )),
        }
    }
}

#[tonic::async_trait]
impl ArrowFlightService for CalmFlightService {
    type HandshakeStream =
        Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send + 'static>>;
    type ListFlightsStream =
        Pin<Box<dyn Stream<Item = Result<FlightInfo, Status>> + Send + 'static>>;
    type DoGetStream = Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + 'static>>;
    type DoPutStream = Pin<Box<dyn Stream<Item = Result<PutResult, Status>> + Send + 'static>>;
    type DoActionStream =
        Pin<Box<dyn Stream<Item = Result<arrow_flight::Result, Status>> + Send + 'static>>;
    type ListActionsStream =
        Pin<Box<dyn Stream<Item = Result<ActionType, Status>> + Send + 'static>>;
    type DoExchangeStream =
        Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + 'static>>;

    /// 认证握手（暂不实现）
    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("handshake not implemented"))
    }

    /// 列出可用的 flights（暂不实现）
    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("list_flights not implemented"))
    }

    /// 获取 flight 信息（暂不实现）
    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("get_flight_info not implemented"))
    }

    /// 获取 schema（暂不实现）
    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("get_schema not implemented"))
    }

    /// **核心方法**: 执行查询并返回数据流
    ///
    /// 支持两种 Ticket：
    /// 1. SQL ticket: {"sql": "...", "internal": true}
    /// 2. Scan ticket: {"type": "scan", "table_name": "...", "projection": [...], "filters": [...]}
    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket = request.into_inner();

        let ticket_str = String::from_utf8(ticket.ticket.to_vec())
            .map_err(|e| Status::invalid_argument(format!("Invalid UTF-8 in ticket: {}", e)))?;

        // 严格解析 JSON Ticket
        let json: serde_json::Value = serde_json::from_str(&ticket_str)
            .map_err(|e| Status::invalid_argument(format!("Invalid JSON ticket: {}", e)))?;

        // 根据 ticket 类型路由
        let ticket_type = json.get("type").and_then(|v| v.as_str());

        let stream = match ticket_type {
            Some("scan") => {
                // 新方式：结构化扫描
                log::debug!("🛫️ [Flight Service] Handling scan ticket");
                self.handle_scan_ticket(&json).await?
            }
            _ => {
                // 旧方式：SQL 查询（兼容性）
                log::debug!("🛫️ [Flight Service] Handling SQL ticket");
                self.handle_sql_ticket(&json, &ticket_str).await?
            }
        };

        // 获取 schema
        let schema = stream.schema();

        // 将 RecordBatch 流的错误类型从 DataFusionError 转换为 FlightError
        let stream_with_flight_error = stream.map_err(|e| FlightError::ExternalError(Box::new(e)));

        // 将 RecordBatch 流转换为 FlightData 流
        let flight_data_stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .with_options(IpcWriteOptions::default())
            .build(stream_with_flight_error)
            .map(|result| result.map_err(|e| Status::internal(format!("Encoding error: {}", e))));

        Ok(Response::new(Box::pin(flight_data_stream)))
    }

    /// 写入数据 - 用于集群环境下的远程插入
    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        use arrow_flight::decode::FlightRecordBatchStream;
        use datafusion::arrow::record_batch::RecordBatch;

        log::debug!("[Flight] Received do_put request");

        let mut stream = request.into_inner();

        log::debug!("[Flight] Reading first message...");

        // 第一个消息包含 FlightDescriptor（表名和分区名）
        let first_message = stream
            .message()
            .await
            .map_err(|e| {
                log::error!("❌ [Flight] Failed to receive first message: {}", e);
                Status::internal(format!("Failed to receive first message: {}", e))
            })?
            .ok_or_else(|| {
                log::error!("❌ [Flight] Empty stream");
                Status::invalid_argument("Empty stream")
            })?;

        log::debug!("[Flight] Parsing FlightDescriptor...");

        // 从 FlightData 中提取 FlightDescriptor
        let descriptor_bytes = first_message.flight_descriptor.as_ref().ok_or_else(|| {
            log::error!("❌ [Flight] Missing flight descriptor");
            Status::invalid_argument("Missing flight descriptor")
        })?;

        // descriptor.path 格式: ["table_name", "partition_name"]
        if descriptor_bytes.path.len() != 2 {
            log::error!(
                "❌ [Flight] Invalid path length: {}",
                descriptor_bytes.path.len()
            );
            return Err(Status::invalid_argument(
                "FlightDescriptor path must contain [table_name, partition_name]",
            ));
        }

        let table_name = descriptor_bytes.path[0].clone();
        let partition_name = descriptor_bytes.path[1].clone();

        log::debug!(
            "[Flight] do_put: table='{}', partition='{}'",
            table_name,
            partition_name
        );

        // 将 FlightData 流解码为 RecordBatch 流
        let batch_stream = FlightRecordBatchStream::new_from_flight_data(
            futures::stream::once(async { Ok(first_message) })
                .chain(stream.map_err(|e| arrow_flight::error::FlightError::Tonic(Box::new(e)))),
        );

        // 收集所有 RecordBatch
        let batches: Vec<RecordBatch> = batch_stream
            .try_collect()
            .await
            .map_err(|e| Status::internal(format!("Failed to decode batches: {}", e)))?;

        log::debug!("📥 [Flight] Received {} batches", batches.len());

        // 直接插入 RecordBatch 到指定分区（不需要 JSON 转换！）
        let _table_info = self
            .calm_service
            .catalog
            .get_or_load_table(&table_name)
            .await
            .map_err(|e| Status::not_found(format!("Table not found: {}", e)))?;

        let mut total_rows = 0;
        for batch in batches {
            let rows = batch.num_rows();

            // 检查分区是否存在（由 insert_data 负责按需创建）
            if self
                .calm_service
                .engine
                .get_partition(&table_name, &partition_name)
                .await
                .is_none()
            {
                return Err(Status::not_found(format!(
                    "Partition '{}/{}' does not exist. Partition should be created by insert_data before sending to remote node.",
                    table_name, partition_name
                )));
            }

            // 直接插入 RecordBatch（无需 JSON 转换）
            self.calm_service
                .engine
                .insert_batch(&table_name, &partition_name, batch)
                .await
                .map_err(|e| Status::internal(format!("Insert failed: {}", e)))?;

            total_rows += rows;
        }

        log::debug!("[Flight] do_put completed: {} rows inserted", total_rows);

        // 返回结果流
        let result = PutResult {
            app_metadata: total_rows.to_string().into_bytes().into(),
        };

        let stream = futures::stream::once(async { Ok(result) });
        Ok(Response::new(Box::pin(stream)))
    }

    /// 执行控制面操作 - RPC 调用
    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        use super::flight_actions::FlightAction;

        let action = request.into_inner();

        log::debug!("🎯 [Flight] Received action: type={}", action.r#type);

        // 解析 Action
        let flight_action = FlightAction::from_flight_action(&action)
            .map_err(|e| Status::invalid_argument(format!("Invalid action: {}", e)))?;

        log::debug!("🎯 [Flight] Parsed action: {:?}", flight_action);

        // 执行 Action
        let response = self
            .handle_action(flight_action)
            .await
            .map_err(|e| Status::internal(format!("Action execution failed: {}", e)))?;

        // 序列化响应
        let result = response
            .to_flight_result()
            .map_err(|e| Status::internal(format!("Failed to serialize response: {}", e)))?;

        // 返回结果流
        let stream = futures::stream::once(async { Ok(result) });
        Ok(Response::new(Box::pin(stream)))
    }

    /// 列出可用的动作
    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        let actions = vec![
            ActionType {
                r#type: "create_table".to_string(),
                description: "Create a new table".to_string(),
            },
            ActionType {
                r#type: "drop_table".to_string(),
                description: "Drop a table".to_string(),
            },
            ActionType {
                r#type: "list_tables".to_string(),
                description: "List all tables".to_string(),
            },
            ActionType {
                r#type: "insert_data".to_string(),
                description: "Insert data to partition (use do_put instead)".to_string(),
            },
        ];

        let stream = futures::stream::iter(actions.into_iter().map(Ok));
        Ok(Response::new(Box::pin(stream)))
    }

    /// 双向流交换（暂不实现）
    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do_exchange not implemented"))
    }

    /// 轮询 flight 信息（暂不实现）
    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<arrow_flight::PollInfo>, Status> {
        Err(Status::unimplemented("poll_flight_info not implemented"))
    }
}

/// 启动 Arrow Flight 服务器
///
/// 监听指定地址，提供 Arrow Flight 查询服务
pub async fn start_flight_server(
    calm_service: Arc<CalmService>,
    addr: std::net::SocketAddr,
    startup_tx: tokio::sync::oneshot::Sender<()>,
) -> Result<(), Box<dyn std::error::Error>> {
    let flight_service = CalmFlightService::new(calm_service);
    let server = flight_service.into_server();

    log::info!("🛩️  Arrow Flight server listening on {}", addr);

    // 通知服务已启动
    let _ = startup_tx.send(());

    tonic::transport::Server::builder()
        .add_service(server)
        .serve(addr)
        .await?;

    Ok(())
}
