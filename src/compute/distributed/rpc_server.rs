//! 分布式查询 RPC 服务端点
//!
//! 提供 HTTP 端点用于接收分布式查询请求和 Shuffle 数据。
//!
//! ## Endpoints
//!
//! - POST /distributed/query - 接收查询请求
//! - POST /distributed/shuffle - 接收 Shuffle 数据
//! - POST /distributed/cancel - 取消查询
//! - POST /distributed/health - 健康检查
//!
//! ## Requirements
//!
//! - 3.2: 使用 HTTP 传输 SQL 和执行参数
//! - 3.3: 以 Arrow IPC 格式流式接收 RecordBatch
//! - 6.5: 接收远程节点发来的 Shuffle 数据

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::record_batch::RecordBatch;
use poem::web::{Data, Json};
use poem::{handler, EndpointExt, Route};
use tokio::sync::{mpsc, RwLock};

use super::node_client::{
    deserialize_batches, serialize_batch, CancelRequest, CancelResponse, HealthRequest,
    HealthResponse, QueryRequest, QueryResponse, QueryResultChunk, ShuffleAck, ShuffleChunk,
};
use crate::engine::Engine;
use crate::utils::error::CoreResult;

// ============================================================================
// 服务状态
// ============================================================================

/// 分布式查询服务状态
///
/// 管理活跃查询和 Shuffle 数据接收器。
pub struct DistributedQueryServiceState {
    /// 本地引擎
    engine: Arc<Engine>,
    /// 本节点 ID
    node_id: String,
    /// 活跃查询计数
    active_queries: std::sync::atomic::AtomicU64,
    /// Shuffle 数据接收器（query_id -> sender）
    shuffle_receivers: RwLock<HashMap<String, mpsc::Sender<RecordBatch>>>,
    /// 已取消的查询
    cancelled_queries: RwLock<std::collections::HashSet<String>>,
}

impl DistributedQueryServiceState {
    /// 创建新的服务状态
    pub fn new(engine: Arc<Engine>, node_id: String) -> Self {
        Self {
            engine,
            node_id,
            active_queries: std::sync::atomic::AtomicU64::new(0),
            shuffle_receivers: RwLock::new(HashMap::new()),
            cancelled_queries: RwLock::new(std::collections::HashSet::new()),
        }
    }

    /// 注册 Shuffle 接收器
    ///
    /// 用于接收远程节点发来的 Shuffle 数据。
    pub async fn register_shuffle_receiver(
        &self,
        query_id: &str,
        sender: mpsc::Sender<RecordBatch>,
    ) {
        let mut receivers = self.shuffle_receivers.write().await;
        receivers.insert(query_id.to_string(), sender);
        log::debug!(
            "[DistributedQueryService] Registered shuffle receiver for query {}",
            query_id
        );
    }

    /// 注销 Shuffle 接收器
    pub async fn unregister_shuffle_receiver(&self, query_id: &str) {
        let mut receivers = self.shuffle_receivers.write().await;
        receivers.remove(query_id);
        log::debug!(
            "[DistributedQueryService] Unregistered shuffle receiver for query {}",
            query_id
        );
    }

    /// 获取 Shuffle 接收器
    pub async fn get_shuffle_receiver(&self, query_id: &str) -> Option<mpsc::Sender<RecordBatch>> {
        let receivers = self.shuffle_receivers.read().await;
        receivers.get(query_id).cloned()
    }

    /// 标记查询为已取消
    pub async fn cancel_query(&self, query_id: &str) {
        let mut cancelled = self.cancelled_queries.write().await;
        cancelled.insert(query_id.to_string());
    }

    /// 检查查询是否已取消
    pub async fn is_cancelled(&self, query_id: &str) -> bool {
        let cancelled = self.cancelled_queries.read().await;
        cancelled.contains(query_id)
    }

    /// 清理已取消的查询
    pub async fn cleanup_cancelled(&self, query_id: &str) {
        let mut cancelled = self.cancelled_queries.write().await;
        cancelled.remove(query_id);
    }

    /// 增加活跃查询计数
    pub fn increment_active_queries(&self) {
        self.active_queries
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }

    /// 减少活跃查询计数
    pub fn decrement_active_queries(&self) {
        self.active_queries
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    }

    /// 获取活跃查询数量
    pub fn get_active_queries(&self) -> u64 {
        self.active_queries
            .load(std::sync::atomic::Ordering::SeqCst)
    }
}

// ============================================================================
// HTTP 处理器
// ============================================================================

/// 处理查询请求
///
/// # Requirements
/// - 3.2: 使用 HTTP 传输 SQL 和执行参数
/// - 3.3: 以 Arrow IPC 格式流式接收 RecordBatch
#[handler]
pub async fn handle_query(
    state: Data<&Arc<DistributedQueryServiceState>>,
    Json(request): Json<QueryRequest>,
) -> Json<QueryResponse> {
    log::info!(
        "[DistributedQueryService] Received query request: query_id={}, sql={}",
        request.query_id,
        request.sql
    );

    state.increment_active_queries();

    let result = execute_query_internal(&state, &request).await;

    state.decrement_active_queries();

    match result {
        Ok(response) => Json(response),
        Err(e) => Json(QueryResponse {
            query_id: request.query_id,
            chunks: Vec::new(),
            error: Some(e.to_string()),
        }),
    }
}

/// 内部查询执行
async fn execute_query_internal(
    state: &Arc<DistributedQueryServiceState>,
    request: &QueryRequest,
) -> CoreResult<QueryResponse> {
    use crate::compute::UnionTableProvider;
    use datafusion::prelude::*;
    use futures::StreamExt;

    // 检查是否已取消
    if state.is_cancelled(&request.query_id).await {
        return Ok(QueryResponse {
            query_id: request.query_id.clone(),
            chunks: Vec::new(),
            error: Some("Query cancelled".to_string()),
        });
    }

    // 提取表名
    let table_name = extract_table_name(&request.sql)?;

    // 获取指定的 Partition（如果有过滤）
    let partitions = if request.partition_filter.is_empty() {
        // 获取所有本地 Partition
        let partition_names = state.engine.list_partitions(&table_name).await;
        let mut partitions = Vec::new();
        for partition_name in partition_names {
            if let Some(partition) = state
                .engine
                .get_partition(&table_name, &partition_name)
                .await
            {
                partitions.push(partition);
            }
        }
        partitions
    } else {
        // 只获取指定的 Partition
        let mut partitions = Vec::new();
        for partition_name in &request.partition_filter {
            if let Some(partition) = state
                .engine
                .get_partition(&table_name, partition_name)
                .await
            {
                partitions.push(partition);
            }
        }
        partitions
    };

    if partitions.is_empty() {
        return Ok(QueryResponse {
            query_id: request.query_id.clone(),
            chunks: Vec::new(),
            error: None,
        });
    }

    // 创建 UnionTableProvider
    let union_table = UnionTableProvider::new(partitions).map_err(|e| {
        crate::utils::error::CoreError::Internal(format!("Failed to create UnionTable: {}", e))
    })?;

    // 创建 DataFusion SessionContext
    let config = SessionConfig::new().with_target_partitions(1);
    let ctx = SessionContext::new_with_config(config);

    // 注册表
    ctx.register_table(&table_name, Arc::new(union_table))
        .map_err(|e| {
            crate::utils::error::CoreError::Internal(format!("Failed to register table: {}", e))
        })?;

    // 执行查询
    let df = ctx.sql(&request.sql).await.map_err(|e| {
        crate::utils::error::CoreError::Internal(format!("Query execution error: {}", e))
    })?;

    let mut stream = df.execute_stream().await.map_err(|e| {
        crate::utils::error::CoreError::Internal(format!("Failed to execute stream: {}", e))
    })?;

    // 收集结果
    let mut chunks = Vec::new();
    let mut chunk_index = 0u64;

    while let Some(batch_result) = stream.next().await {
        // 检查是否已取消
        if state.is_cancelled(&request.query_id).await {
            return Ok(QueryResponse {
                query_id: request.query_id.clone(),
                chunks,
                error: Some("Query cancelled".to_string()),
            });
        }

        match batch_result {
            Ok(batch) => {
                if batch.num_rows() > 0 {
                    let data = serialize_batch(&batch)?;
                    chunks.push(QueryResultChunk {
                        query_id: request.query_id.clone(),
                        chunk_index,
                        data,
                        is_last: false,
                        error: None,
                    });
                    chunk_index += 1;
                }
            }
            Err(e) => {
                return Ok(QueryResponse {
                    query_id: request.query_id.clone(),
                    chunks,
                    error: Some(e.to_string()),
                });
            }
        }
    }

    // 标记最后一个 chunk
    if let Some(last) = chunks.last_mut() {
        last.is_last = true;
    }

    log::info!(
        "[DistributedQueryService] Query {} completed with {} chunks",
        request.query_id,
        chunks.len()
    );

    Ok(QueryResponse {
        query_id: request.query_id.clone(),
        chunks,
        error: None,
    })
}

/// 从 SQL 中提取表名
fn extract_table_name(sql: &str) -> CoreResult<String> {
    let sql_upper = sql.to_uppercase();

    if let Some(from_pos) = sql_upper.find(" FROM ") {
        let after_from = &sql[from_pos + 6..].trim();
        let table_name = after_from
            .split_whitespace()
            .next()
            .unwrap_or("")
            .trim_matches(|c: char| !c.is_alphanumeric() && c != '_' && c != '.');

        if !table_name.is_empty() {
            return Ok(table_name.to_string());
        }
    }

    Err(crate::utils::error::CoreError::InvalidParam(format!(
        "Could not extract table name from SQL: {}",
        sql
    )))
}

/// 处理 Shuffle 数据
///
/// # Requirements
/// - 6.5: 接收远程节点发来的 Shuffle 数据
#[handler]
pub async fn handle_shuffle(
    state: Data<&Arc<DistributedQueryServiceState>>,
    Json(chunk): Json<ShuffleChunk>,
) -> Json<ShuffleAck> {
    log::trace!(
        "[DistributedQueryService] Received shuffle chunk: query_id={}, source={}, chunk_index={}, is_eof={}",
        chunk.query_id,
        chunk.source_node,
        chunk.chunk_index,
        chunk.is_eof
    );

    // 获取对应的接收器
    let receiver = state.get_shuffle_receiver(&chunk.query_id).await;

    match receiver {
        Some(sender) => {
            if chunk.is_eof {
                // EOF 信号，不需要发送数据
                log::debug!(
                    "[DistributedQueryService] Received EOF for query {} from {}",
                    chunk.query_id,
                    chunk.source_node
                );
                Json(ShuffleAck {
                    success: true,
                    chunks_received: chunk.chunk_index,
                    error: None,
                })
            } else {
                // 反序列化数据
                match deserialize_batches(&chunk.data) {
                    Ok(batches) => {
                        let mut success = true;
                        for batch in batches {
                            if sender.send(batch).await.is_err() {
                                success = false;
                                break;
                            }
                        }
                        Json(ShuffleAck {
                            success,
                            chunks_received: chunk.chunk_index + 1,
                            error: if success {
                                None
                            } else {
                                Some("Failed to send batch to receiver".to_string())
                            },
                        })
                    }
                    Err(e) => Json(ShuffleAck {
                        success: false,
                        chunks_received: chunk.chunk_index,
                        error: Some(format!("Failed to deserialize batch: {}", e)),
                    }),
                }
            }
        }
        None => {
            log::warn!(
                "[DistributedQueryService] No receiver found for query {}",
                chunk.query_id
            );
            Json(ShuffleAck {
                success: false,
                chunks_received: 0,
                error: Some(format!(
                    "No receiver registered for query {}",
                    chunk.query_id
                )),
            })
        }
    }
}

/// 处理取消请求
///
/// # Requirements
/// - 8.1: 任一节点失败时取消其他节点的查询
#[handler]
pub async fn handle_cancel(
    state: Data<&Arc<DistributedQueryServiceState>>,
    Json(request): Json<CancelRequest>,
) -> Json<CancelResponse> {
    log::info!(
        "[DistributedQueryService] Received cancel request for query {}",
        request.query_id
    );

    state.cancel_query(&request.query_id).await;

    Json(CancelResponse {
        success: true,
        message: Some(format!(
            "Query {} marked for cancellation",
            request.query_id
        )),
    })
}

/// 处理健康检查
#[handler]
pub async fn handle_health(
    state: Data<&Arc<DistributedQueryServiceState>>,
    Json(_request): Json<HealthRequest>,
) -> Json<HealthResponse> {
    Json(HealthResponse {
        healthy: true,
        node_id: state.node_id.clone(),
        active_queries: state.get_active_queries(),
    })
}

// ============================================================================
// 路由配置
// ============================================================================

/// 创建分布式查询服务路由
///
/// 返回配置好的 Poem 路由，包含所有分布式查询端点。
pub fn create_distributed_routes(state: Arc<DistributedQueryServiceState>) -> impl poem::Endpoint {
    Route::new()
        .at("/distributed/query", poem::post(handle_query))
        .at("/distributed/shuffle", poem::post(handle_shuffle))
        .at("/distributed/cancel", poem::post(handle_cancel))
        .at("/distributed/health", poem::post(handle_health))
        .data(state)
}

// ============================================================================
// 测试
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_table_name() {
        assert_eq!(extract_table_name("SELECT * FROM users").unwrap(), "users");
        assert_eq!(
            extract_table_name("SELECT id, name FROM orders WHERE id > 10").unwrap(),
            "orders"
        );
        assert_eq!(
            extract_table_name("select count(*) from products").unwrap(),
            "products"
        );
    }

    #[test]
    fn test_extract_table_name_error() {
        assert!(extract_table_name("SELECT 1").is_err());
        assert!(extract_table_name("INSERT INTO users VALUES (1)").is_err());
    }
}
