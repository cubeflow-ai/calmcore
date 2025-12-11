//! 节点间通信客户端 (基于 HTTP + Arrow IPC)
//!
//! 使用 HTTP 实现节点间高效通信：
//! - 查询请求：POST /query
//! - 查询结果：Streaming response with Arrow IPC
//! - Shuffle 数据：POST /shuffle
//!
//! 数据格式使用 Arrow IPC，实现高效的零拷贝传输。
//!
//! ## Requirements
//!
//! - 3.1: 与相关 Worker 节点建立连接
//! - 3.2: 使用 HTTP 传输 SQL 和执行参数
//! - 3.3: 以 Arrow IPC 格式流式接收 RecordBatch
//! - 3.4: 连接失败返回错误并标记节点不可用
//! - 3.5: 节点恢复时自动重新建立连接

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::arrow::record_batch::RecordBatch;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use super::config::DistributedConfig;
use crate::cluster::ClusterManager;
use crate::utils::error::{CoreError, CoreResult};

// ============================================================================
// Proto 消息定义（手动定义，避免 build.rs 复杂性）
// ============================================================================

/// 查询请求
///
/// # Requirements
/// - 3.2: 使用 HTTP 传输 SQL 和执行参数
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryRequest {
    /// SQL 查询语句
    pub sql: String,
    /// 查询 ID
    pub query_id: String,
    /// 超时时间（毫秒）
    pub timeout_ms: u64,
    /// 只查询指定的 partition
    pub partition_filter: Vec<String>,
}

/// 查询结果块
///
/// # Requirements
/// - 3.3: 以 Arrow IPC 格式流式接收 RecordBatch
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResultChunk {
    /// 查询 ID
    pub query_id: String,
    /// 块索引
    pub chunk_index: u64,
    /// Arrow IPC 格式的数据 (base64 encoded for JSON transport)
    #[serde(with = "base64_bytes")]
    pub data: Vec<u8>,
    /// 是否为最后一块
    pub is_last: bool,
    /// 错误信息
    pub error: Option<String>,
}

/// Shuffle 数据块
///
/// # Requirements
/// - 6.5: 通过网络发送到目标节点
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShuffleChunk {
    /// 查询 ID
    pub query_id: String,
    /// 源节点 ID
    pub source_node: String,
    /// 目标节点 ID
    pub target_node: String,
    /// 块索引
    pub chunk_index: u64,
    /// Arrow IPC 格式的数据 (base64 encoded for JSON transport)
    #[serde(with = "base64_bytes")]
    pub data: Vec<u8>,
    /// 是否为 EOF
    pub is_eof: bool,
}

/// Shuffle 确认
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShuffleAck {
    pub success: bool,
    pub chunks_received: u64,
    pub error: Option<String>,
}

/// Base64 encoding/decoding for binary data in JSON
mod base64_bytes {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S>(bytes: &Vec<u8>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use base64::Engine;
        let encoded = base64::engine::general_purpose::STANDARD.encode(bytes);
        encoded.serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        use base64::Engine;
        let encoded = String::deserialize(deserializer)?;
        base64::engine::general_purpose::STANDARD
            .decode(&encoded)
            .map_err(serde::de::Error::custom)
    }
}

// ============================================================================
// Arrow IPC 序列化/反序列化
// ============================================================================

/// 序列化 RecordBatch 为 Arrow IPC 格式
pub fn serialize_batch(batch: &RecordBatch) -> CoreResult<Vec<u8>> {
    use std::io::Cursor;

    let mut buffer = Cursor::new(Vec::new());
    {
        let mut writer = StreamWriter::try_new(&mut buffer, &batch.schema())
            .map_err(|e| CoreError::Internal(format!("Failed to create IPC writer: {}", e)))?;

        writer
            .write(batch)
            .map_err(|e| CoreError::Internal(format!("Failed to write batch to IPC: {}", e)))?;

        writer
            .finish()
            .map_err(|e| CoreError::Internal(format!("Failed to finish IPC writer: {}", e)))?;
    }

    Ok(buffer.into_inner())
}

/// 反序列化 Arrow IPC 数据为 RecordBatch 列表
pub fn deserialize_batches(data: &[u8]) -> CoreResult<Vec<RecordBatch>> {
    use std::io::Cursor;

    if data.is_empty() {
        return Ok(Vec::new());
    }

    let cursor = Cursor::new(data);
    let reader = StreamReader::try_new(cursor, None)
        .map_err(|e| CoreError::Internal(format!("Failed to create IPC reader: {}", e)))?;

    let mut batches = Vec::new();
    for batch_result in reader {
        let batch = batch_result
            .map_err(|e| CoreError::Internal(format!("Failed to read batch from IPC: {}", e)))?;
        batches.push(batch);
    }

    Ok(batches)
}

// ============================================================================
// 节点客户端
// ============================================================================

/// 取消查询请求
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CancelRequest {
    pub query_id: String,
}

/// 取消查询响应
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CancelResponse {
    pub success: bool,
    pub message: Option<String>,
}

/// 健康检查请求
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthRequest {
    pub node_id: String,
}

/// 健康检查响应
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthResponse {
    pub healthy: bool,
    pub node_id: String,
    pub active_queries: u64,
}

/// 查询响应（包含多个结果块）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResponse {
    pub query_id: String,
    pub chunks: Vec<QueryResultChunk>,
    pub error: Option<String>,
}

// ============================================================================
// 节点客户端
// ============================================================================

/// 节点客户端
///
/// 负责与单个远程节点的 HTTP 通信
///
/// # Requirements
///
/// - 3.1: 与相关 Worker 节点建立连接
/// - 3.2: 使用 HTTP 传输 SQL 和执行参数
/// - 3.3: 以 Arrow IPC 格式流式接收 RecordBatch
/// - 3.4: 连接失败返回错误并标记节点不可用
/// - 3.5: 节点恢复时自动重新建立连接
pub struct NodeClient {
    // Note: reqwest::Client doesn't implement Debug, so we can't derive Debug
    /// 节点 ID
    node_id: String,
    /// 节点地址（host:port）
    addr: String,
    /// 是否可用
    available: RwLock<bool>,
    /// 配置
    config: DistributedConfig,
    /// HTTP 客户端
    http_client: reqwest::Client,
}

impl std::fmt::Debug for NodeClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeClient")
            .field("node_id", &self.node_id)
            .field("addr", &self.addr)
            .field("config", &self.config)
            .finish()
    }
}

impl NodeClient {
    /// 创建新的节点客户端
    ///
    /// # Requirements
    /// - 3.1: 与相关 Worker 节点建立连接
    pub fn new(node_id: String, addr: String, config: DistributedConfig) -> Self {
        // 创建 HTTP 客户端，配置超时
        let http_client = reqwest::Client::builder()
            .connect_timeout(Duration::from_millis(config.connect_timeout_ms))
            .timeout(Duration::from_millis(config.request_timeout_ms))
            .pool_max_idle_per_host(10)
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());

        Self {
            node_id,
            addr,
            available: RwLock::new(true),
            config,
            http_client,
        }
    }

    /// 获取节点 ID
    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    /// 获取节点地址
    pub fn addr(&self) -> &str {
        &self.addr
    }

    /// 检查节点是否可用
    pub async fn is_available(&self) -> bool {
        *self.available.read().await
    }

    /// 标记节点为不可用
    ///
    /// # Requirements
    /// - 3.4: 连接失败返回错误并标记节点不可用
    pub async fn mark_unavailable(&self) {
        *self.available.write().await = false;
        log::warn!("[NodeClient] Node {} marked as unavailable", self.node_id);
    }

    /// 标记节点为可用
    ///
    /// # Requirements
    /// - 3.5: 节点恢复时自动重新建立连接
    pub async fn mark_available(&self) {
        *self.available.write().await = true;
        log::info!("[NodeClient] Node {} marked as available", self.node_id);
    }

    /// 构建完整的 URL
    fn build_url(&self, path: &str) -> String {
        format!("http://{}{}", self.addr, path)
    }

    /// 执行查询（HTTP POST + JSON）
    ///
    /// 发送查询请求，返回所有结果 RecordBatch
    ///
    /// # Requirements
    /// - 3.2: 使用 HTTP 传输 SQL 和执行参数
    /// - 3.3: 以 Arrow IPC 格式流式接收 RecordBatch
    /// - 3.4: 连接失败返回错误并标记节点不可用
    pub async fn execute_query(&self, request: &QueryRequest) -> CoreResult<Vec<RecordBatch>> {
        log::debug!(
            "[NodeClient] Executing query on node {}: {}",
            self.node_id,
            request.sql
        );

        let url = self.build_url("/distributed/query");

        // 发送 HTTP POST 请求
        let response = match self.http_client.post(&url).json(request).send().await {
            Ok(resp) => resp,
            Err(e) => {
                // 连接失败，标记节点不可用
                self.mark_unavailable().await;
                return Err(CoreError::Network(format!(
                    "Failed to connect to node {}: {}",
                    self.node_id, e
                )));
            }
        };

        // 检查响应状态
        if !response.status().is_success() {
            let status = response.status();
            let error_text = response.text().await.unwrap_or_default();
            return Err(CoreError::Network(format!(
                "Query failed on node {} with status {}: {}",
                self.node_id, status, error_text
            )));
        }

        // 解析响应
        let query_response: QueryResponse = match response.json().await {
            Ok(resp) => resp,
            Err(e) => {
                return Err(CoreError::Internal(format!(
                    "Failed to parse query response from node {}: {}",
                    self.node_id, e
                )));
            }
        };

        // 检查错误
        if let Some(error) = query_response.error {
            return Err(CoreError::Internal(format!(
                "Query error from node {}: {}",
                self.node_id, error
            )));
        }

        // 反序列化所有结果块
        let mut all_batches = Vec::new();
        for chunk in query_response.chunks {
            if !chunk.data.is_empty() {
                let batches = deserialize_batches(&chunk.data)?;
                all_batches.extend(batches);
            }
            if let Some(error) = chunk.error {
                if !error.is_empty() {
                    return Err(CoreError::Internal(format!(
                        "Chunk error from node {}: {}",
                        self.node_id, error
                    )));
                }
            }
        }

        log::debug!(
            "[NodeClient] Received {} batches from node {}",
            all_batches.len(),
            self.node_id
        );

        Ok(all_batches)
    }

    /// 发送 Shuffle 数据块
    ///
    /// # Requirements
    /// - 6.5: 通过网络发送到目标节点
    pub async fn send_shuffle_chunk(&self, chunk: &ShuffleChunk) -> CoreResult<ShuffleAck> {
        log::trace!(
            "[NodeClient] Sending shuffle chunk to node {}: query_id={}, chunk_index={}, is_eof={}",
            self.node_id,
            chunk.query_id,
            chunk.chunk_index,
            chunk.is_eof
        );

        let url = self.build_url("/distributed/shuffle");

        // 发送 HTTP POST 请求
        let response = match self.http_client.post(&url).json(chunk).send().await {
            Ok(resp) => resp,
            Err(e) => {
                self.mark_unavailable().await;
                return Err(CoreError::Network(format!(
                    "Failed to send shuffle chunk to node {}: {}",
                    self.node_id, e
                )));
            }
        };

        // 检查响应状态
        if !response.status().is_success() {
            let status = response.status();
            let error_text = response.text().await.unwrap_or_default();
            return Err(CoreError::Network(format!(
                "Shuffle failed on node {} with status {}: {}",
                self.node_id, status, error_text
            )));
        }

        // 解析响应
        let ack: ShuffleAck = match response.json().await {
            Ok(resp) => resp,
            Err(e) => {
                return Err(CoreError::Internal(format!(
                    "Failed to parse shuffle ack from node {}: {}",
                    self.node_id, e
                )));
            }
        };

        if !ack.success {
            return Err(CoreError::Internal(format!(
                "Shuffle rejected by node {}: {}",
                self.node_id,
                ack.error.unwrap_or_default()
            )));
        }

        Ok(ack)
    }

    /// 取消查询
    ///
    /// # Requirements
    /// - 8.1: 任一节点失败时取消其他节点的查询
    pub async fn cancel_query(&self, query_id: &str) -> CoreResult<()> {
        log::debug!(
            "[NodeClient] Cancelling query on node {}: query_id={}",
            self.node_id,
            query_id
        );

        let url = self.build_url("/distributed/cancel");
        let request = CancelRequest {
            query_id: query_id.to_string(),
        };

        // 发送取消请求（忽略错误，因为节点可能已经完成或失败）
        match self.http_client.post(&url).json(&request).send().await {
            Ok(response) => {
                if response.status().is_success() {
                    log::debug!(
                        "[NodeClient] Query {} cancelled on node {}",
                        query_id,
                        self.node_id
                    );
                }
            }
            Err(e) => {
                log::warn!(
                    "[NodeClient] Failed to cancel query {} on node {}: {}",
                    query_id,
                    self.node_id,
                    e
                );
            }
        }

        Ok(())
    }

    /// 健康检查
    ///
    /// # Requirements
    /// - 3.5: 节点恢复时自动重新建立连接
    pub async fn health_check(&self) -> bool {
        let url = self.build_url("/distributed/health");
        let request = HealthRequest {
            node_id: self.node_id.clone(),
        };

        match self.http_client.post(&url).json(&request).send().await {
            Ok(response) => {
                if response.status().is_success() {
                    if let Ok(health_response) = response.json::<HealthResponse>().await {
                        return health_response.healthy;
                    }
                }
                false
            }
            Err(_) => false,
        }
    }
}

// ============================================================================
// 节点客户端管理器
// ============================================================================

/// 节点客户端管理器
///
/// 管理与所有远程节点的连接
pub struct NodeClientManager {
    /// 节点连接池
    clients: RwLock<HashMap<String, Arc<NodeClient>>>,
    /// 集群管理器
    cluster_manager: Arc<ClusterManager>,
    /// 配置
    config: DistributedConfig,
}

impl NodeClientManager {
    /// 创建新的节点客户端管理器
    pub fn new(cluster_manager: Arc<ClusterManager>, config: DistributedConfig) -> Self {
        Self {
            clients: RwLock::new(HashMap::new()),
            cluster_manager,
            config,
        }
    }

    /// 获取或创建节点客户端
    pub async fn get_client(&self, node_id: &str) -> CoreResult<Arc<NodeClient>> {
        // 先尝试从缓存获取
        {
            let clients = self.clients.read().await;
            if let Some(client) = clients.get(node_id) {
                if client.is_available().await {
                    return Ok(client.clone());
                }
            }
        }

        // 获取节点地址
        let node_addr = self.get_node_addr(node_id).await?;

        // 创建新客户端
        let client = Arc::new(NodeClient::new(
            node_id.to_string(),
            node_addr,
            self.config.clone(),
        ));

        // 缓存客户端
        {
            let mut clients = self.clients.write().await;
            clients.insert(node_id.to_string(), client.clone());
        }

        Ok(client)
    }

    /// 获取节点地址
    async fn get_node_addr(&self, node_id: &str) -> CoreResult<String> {
        todo!()
        // // 从集群管理器获取节点信息
        // let membership = self.cluster_manager.get_membership().await;

        // for member in &membership.members {
        //     if member.node_id == node_id {
        //         // 构建 gRPC 地址（使用 RPC 端口）
        //         // member.gossip_addr 格式为 "host:gossip_port"
        //         // 我们需要替换为 RPC 端口
        //         let parts: Vec<&str> = member.gossip_addr.split(':').collect();
        //         if !parts.is_empty() {
        //             let host = parts[0];
        //             let addr = format!("{}:{}", host, self.config.rpc_port);
        //             return Ok(addr);
        //         }
        //     }
        // }

        // Err(CoreError::NotExisted(format!(
        //     "Node {} not found in cluster",
        //     node_id
        // )))
    }

    /// 获取所有可用节点的客户端
    pub async fn get_all_available_clients(&self) -> Vec<Arc<NodeClient>> {
        let clients = self.clients.read().await;
        let mut available = Vec::new();

        for client in clients.values() {
            if client.is_available().await {
                available.push(client.clone());
            }
        }

        available
    }

    /// 移除节点客户端
    pub async fn remove_client(&self, node_id: &str) {
        let mut clients = self.clients.write().await;
        clients.remove(node_id);
        log::info!("[NodeClientManager] Removed client for node {}", node_id);
    }

    /// 健康检查所有节点
    pub async fn health_check_all(&self) {
        let clients = self.clients.read().await;
        for (node_id, client) in clients.iter() {
            let healthy = client.health_check().await;
            if healthy {
                client.mark_available().await;
            } else {
                client.mark_unavailable().await;
            }
            log::debug!(
                "[NodeClientManager] Health check for node {}: {}",
                node_id,
                if healthy { "healthy" } else { "unhealthy" }
            );
        }
    }

    /// 广播取消查询到所有节点
    pub async fn broadcast_cancel(&self, query_id: &str) {
        let clients = self.clients.read().await;
        for (node_id, client) in clients.iter() {
            if let Err(e) = client.cancel_query(query_id).await {
                log::warn!(
                    "[NodeClientManager] Failed to cancel query {} on node {}: {}",
                    query_id,
                    node_id,
                    e
                );
            }
        }
    }
}

// ============================================================================
// 测试
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc as StdArc;

    #[test]
    fn test_serialize_deserialize_batch() {
        // 创建测试 schema
        let schema = StdArc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Int32, true),
        ]));

        // 创建测试数据
        let id_array = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let value_array = Int32Array::from(vec![Some(10), Some(20), None, Some(40), Some(50)]);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![StdArc::new(id_array), StdArc::new(value_array)],
        )
        .unwrap();

        // 序列化
        let serialized = serialize_batch(&batch).unwrap();
        assert!(!serialized.is_empty());

        // 反序列化
        let deserialized = deserialize_batches(&serialized).unwrap();
        assert_eq!(deserialized.len(), 1);

        let result_batch = &deserialized[0];
        assert_eq!(result_batch.num_rows(), 5);
        assert_eq!(result_batch.num_columns(), 2);
    }

    #[test]
    fn test_query_request() {
        let request = QueryRequest {
            sql: "SELECT * FROM users".to_string(),
            query_id: "test-123".to_string(),
            timeout_ms: 30000,
            partition_filter: vec!["p1".to_string(), "p2".to_string()],
        };

        assert_eq!(request.sql, "SELECT * FROM users");
        assert_eq!(request.query_id, "test-123");
        assert_eq!(request.timeout_ms, 30000);
        assert_eq!(request.partition_filter.len(), 2);
    }

    #[test]
    fn test_shuffle_chunk() {
        let chunk = ShuffleChunk {
            query_id: "q1".to_string(),
            source_node: "node-1".to_string(),
            target_node: "node-2".to_string(),
            chunk_index: 0,
            data: vec![1, 2, 3, 4],
            is_eof: false,
        };

        assert_eq!(chunk.query_id, "q1");
        assert_eq!(chunk.source_node, "node-1");
        assert_eq!(chunk.target_node, "node-2");
        assert!(!chunk.is_eof);
    }
}
