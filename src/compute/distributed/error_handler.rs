//! 分布式查询错误处理
//!
//! 实现分布式查询的错误处理机制，包括：
//! - 查询取消（任一节点失败时取消其他节点）
//! - 超时处理
//! - 节点不可达处理
//! - Shuffle 失败处理
//!
//! ## Requirements
//!
//! - 8.1: 任一节点失败时取消其他节点的查询
//! - 8.2: 节点不可达时返回包含节点信息的错误
//! - 8.3: 查询超时返回超时错误
//! - 8.4: Shuffle 过程中节点失败返回 Shuffle 失败错误

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::RwLock;
use tokio::time::timeout;

use super::node_client::NodeClientManager;
use crate::utils::error::{CoreError, CoreResult};

// ============================================================================
// 分布式查询错误类型
// ============================================================================

/// 分布式查询错误
#[derive(Debug, Clone)]
pub enum DistributedQueryError {
    /// 节点不可达
    ///
    /// # Requirements
    /// - 8.2: 节点不可达时返回包含节点信息的错误
    NodeUnreachable {
        node_id: String,
        node_addr: String,
        reason: String,
    },

    /// 查询超时
    ///
    /// # Requirements
    /// - 8.3: 查询超时返回超时错误
    QueryTimeout {
        query_id: String,
        timeout_ms: u64,
        nodes_involved: Vec<String>,
    },

    /// 查询被取消
    ///
    /// # Requirements
    /// - 8.1: 任一节点失败时取消其他节点的查询
    QueryCancelled {
        query_id: String,
        reason: String,
        failed_node: Option<String>,
    },

    /// Shuffle 失败
    ///
    /// # Requirements
    /// - 8.4: Shuffle 过程中节点失败返回 Shuffle 失败错误
    ShuffleFailed {
        query_id: String,
        source_node: String,
        target_node: String,
        reason: String,
    },

    /// 部分节点失败
    PartialFailure {
        query_id: String,
        failed_nodes: Vec<String>,
        successful_nodes: Vec<String>,
        errors: Vec<String>,
    },
}

impl std::fmt::Display for DistributedQueryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DistributedQueryError::NodeUnreachable {
                node_id,
                node_addr,
                reason,
            } => {
                write!(
                    f,
                    "Node {} ({}) is unreachable: {}",
                    node_id, node_addr, reason
                )
            }
            DistributedQueryError::QueryTimeout {
                query_id,
                timeout_ms,
                nodes_involved,
            } => {
                write!(
                    f,
                    "Query {} timed out after {}ms (nodes: {})",
                    query_id,
                    timeout_ms,
                    nodes_involved.join(", ")
                )
            }
            DistributedQueryError::QueryCancelled {
                query_id,
                reason,
                failed_node,
            } => {
                if let Some(node) = failed_node {
                    write!(
                        f,
                        "Query {} cancelled due to failure on node {}: {}",
                        query_id, node, reason
                    )
                } else {
                    write!(f, "Query {} cancelled: {}", query_id, reason)
                }
            }
            DistributedQueryError::ShuffleFailed {
                query_id,
                source_node,
                target_node,
                reason,
            } => {
                write!(
                    f,
                    "Shuffle failed for query {} from {} to {}: {}",
                    query_id, source_node, target_node, reason
                )
            }
            DistributedQueryError::PartialFailure {
                query_id,
                failed_nodes,
                successful_nodes: _,
                errors,
            } => {
                write!(
                    f,
                    "Query {} partially failed on nodes [{}]: {}",
                    query_id,
                    failed_nodes.join(", "),
                    errors.join("; ")
                )
            }
        }
    }
}

impl std::error::Error for DistributedQueryError {}

impl From<DistributedQueryError> for CoreError {
    fn from(err: DistributedQueryError) -> Self {
        match err {
            DistributedQueryError::NodeUnreachable { .. } => CoreError::Network(err.to_string()),
            DistributedQueryError::QueryTimeout { .. } => CoreError::Timeout(err.to_string()),
            DistributedQueryError::QueryCancelled { .. } => CoreError::Internal(err.to_string()),
            DistributedQueryError::ShuffleFailed { .. } => CoreError::Internal(err.to_string()),
            DistributedQueryError::PartialFailure { .. } => CoreError::Internal(err.to_string()),
        }
    }
}

// ============================================================================
// 查询取消管理器
// ============================================================================

/// 查询取消管理器
///
/// 管理分布式查询的取消操作，当任一节点失败时取消其他节点的查询。
///
/// # Requirements
/// - 8.1: 任一节点失败时取消其他节点的查询
pub struct QueryCancellationManager {
    /// 节点客户端管理器
    node_clients: Arc<NodeClientManager>,
    /// 已取消的查询 ID
    cancelled_queries: RwLock<HashSet<String>>,
    /// 活跃查询及其涉及的节点
    active_queries: RwLock<std::collections::HashMap<String, Vec<String>>>,
}

impl QueryCancellationManager {
    /// 创建新的查询取消管理器
    pub fn new(node_clients: Arc<NodeClientManager>) -> Self {
        Self {
            node_clients,
            cancelled_queries: RwLock::new(HashSet::new()),
            active_queries: RwLock::new(std::collections::HashMap::new()),
        }
    }

    /// 注册活跃查询
    pub async fn register_query(&self, query_id: &str, node_ids: Vec<String>) {
        let mut active = self.active_queries.write().await;
        active.insert(query_id.to_string(), node_ids);
        log::debug!("[QueryCancellationManager] Registered query {}", query_id);
    }

    /// 注销查询
    pub async fn unregister_query(&self, query_id: &str) {
        let mut active = self.active_queries.write().await;
        active.remove(query_id);

        let mut cancelled = self.cancelled_queries.write().await;
        cancelled.remove(query_id);

        log::debug!("[QueryCancellationManager] Unregistered query {}", query_id);
    }

    /// 检查查询是否已取消
    pub async fn is_cancelled(&self, query_id: &str) -> bool {
        let cancelled = self.cancelled_queries.read().await;
        cancelled.contains(query_id)
    }

    /// 取消查询
    ///
    /// 当任一节点失败时，取消其他所有节点上的查询。
    ///
    /// # Requirements
    /// - 8.1: 任一节点失败时取消其他节点的查询
    pub async fn cancel_query(&self, query_id: &str, reason: &str, failed_node: Option<&str>) {
        // 标记为已取消
        {
            let mut cancelled = self.cancelled_queries.write().await;
            if cancelled.contains(query_id) {
                // 已经取消过了
                return;
            }
            cancelled.insert(query_id.to_string());
        }

        log::warn!(
            "[QueryCancellationManager] Cancelling query {}: {} (failed_node: {:?})",
            query_id,
            reason,
            failed_node
        );

        // 获取涉及的节点
        let node_ids = {
            let active = self.active_queries.read().await;
            active.get(query_id).cloned().unwrap_or_default()
        };

        // 向所有节点发送取消请求（除了失败的节点）
        for node_id in &node_ids {
            if Some(node_id.as_str()) == failed_node {
                continue;
            }

            if let Ok(client) = self.node_clients.get_client(node_id).await {
                if let Err(e) = client.cancel_query(query_id).await {
                    log::warn!(
                        "[QueryCancellationManager] Failed to cancel query {} on node {}: {}",
                        query_id,
                        node_id,
                        e
                    );
                }
            }
        }
    }

    /// 广播取消到所有已知节点
    pub async fn broadcast_cancel(&self, query_id: &str) {
        self.node_clients.broadcast_cancel(query_id).await;
    }
}

// ============================================================================
// 超时处理
// ============================================================================

/// 带超时的异步操作执行器
///
/// # Requirements
/// - 8.3: 查询超时返回超时错误
pub struct TimeoutExecutor {
    /// 默认超时时间
    default_timeout_ms: u64,
}

impl TimeoutExecutor {
    /// 创建新的超时执行器
    pub fn new(default_timeout_ms: u64) -> Self {
        Self { default_timeout_ms }
    }

    /// 执行带超时的异步操作
    ///
    /// # Requirements
    /// - 8.3: 查询超时返回超时错误
    pub async fn execute_with_timeout<F, T>(
        &self,
        query_id: &str,
        nodes_involved: Vec<String>,
        timeout_ms: Option<u64>,
        future: F,
    ) -> CoreResult<T>
    where
        F: std::future::Future<Output = CoreResult<T>>,
    {
        let timeout_duration = Duration::from_millis(timeout_ms.unwrap_or(self.default_timeout_ms));

        match timeout(timeout_duration, future).await {
            Ok(result) => result,
            Err(_) => {
                let err = DistributedQueryError::QueryTimeout {
                    query_id: query_id.to_string(),
                    timeout_ms: timeout_ms.unwrap_or(self.default_timeout_ms),
                    nodes_involved,
                };
                log::error!("[TimeoutExecutor] {}", err);
                Err(err.into())
            }
        }
    }
}

// ============================================================================
// 节点不可达处理
// ============================================================================

/// 创建节点不可达错误
///
/// # Requirements
/// - 8.2: 节点不可达时返回包含节点信息的错误
pub fn node_unreachable_error(node_id: &str, node_addr: &str, reason: &str) -> CoreError {
    let err = DistributedQueryError::NodeUnreachable {
        node_id: node_id.to_string(),
        node_addr: node_addr.to_string(),
        reason: reason.to_string(),
    };
    log::error!("[DistributedQuery] {}", err);
    err.into()
}

/// 检查错误是否为节点不可达
pub fn is_node_unreachable_error(error: &CoreError) -> bool {
    matches!(error, CoreError::Network(_))
}

// ============================================================================
// Shuffle 失败处理
// ============================================================================

/// 创建 Shuffle 失败错误
///
/// # Requirements
/// - 8.4: Shuffle 过程中节点失败返回 Shuffle 失败错误
pub fn shuffle_failed_error(
    query_id: &str,
    source_node: &str,
    target_node: &str,
    reason: &str,
) -> CoreError {
    let err = DistributedQueryError::ShuffleFailed {
        query_id: query_id.to_string(),
        source_node: source_node.to_string(),
        target_node: target_node.to_string(),
        reason: reason.to_string(),
    };
    log::error!("[DistributedQuery] {}", err);
    err.into()
}

// ============================================================================
// 错误聚合
// ============================================================================

/// 聚合多个节点的执行结果
///
/// 当部分节点失败时，决定是返回部分结果还是完全失败。
pub struct ErrorAggregator {
    query_id: String,
    failed_nodes: Vec<String>,
    successful_nodes: Vec<String>,
    errors: Vec<String>,
}

impl ErrorAggregator {
    /// 创建新的错误聚合器
    pub fn new(query_id: &str) -> Self {
        Self {
            query_id: query_id.to_string(),
            failed_nodes: Vec::new(),
            successful_nodes: Vec::new(),
            errors: Vec::new(),
        }
    }

    /// 记录成功的节点
    pub fn record_success(&mut self, node_id: &str) {
        self.successful_nodes.push(node_id.to_string());
    }

    /// 记录失败的节点
    pub fn record_failure(&mut self, node_id: &str, error: &str) {
        self.failed_nodes.push(node_id.to_string());
        self.errors.push(format!("{}: {}", node_id, error));
    }

    /// 检查是否有失败
    pub fn has_failures(&self) -> bool {
        !self.failed_nodes.is_empty()
    }

    /// 检查是否全部失败
    pub fn all_failed(&self) -> bool {
        self.successful_nodes.is_empty() && !self.failed_nodes.is_empty()
    }

    /// 获取失败的节点列表
    pub fn failed_nodes(&self) -> &[String] {
        &self.failed_nodes
    }

    /// 转换为错误（如果有失败）
    pub fn into_error(self) -> Option<CoreError> {
        if self.failed_nodes.is_empty() {
            None
        } else {
            let err = DistributedQueryError::PartialFailure {
                query_id: self.query_id,
                failed_nodes: self.failed_nodes,
                successful_nodes: self.successful_nodes,
                errors: self.errors,
            };
            Some(err.into())
        }
    }
}

// ============================================================================
// 测试
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_distributed_query_error_display() {
        let err = DistributedQueryError::NodeUnreachable {
            node_id: "node-1".to_string(),
            node_addr: "192.168.1.1:7947".to_string(),
            reason: "Connection refused".to_string(),
        };
        assert!(err.to_string().contains("node-1"));
        assert!(err.to_string().contains("192.168.1.1:7947"));
        assert!(err.to_string().contains("Connection refused"));

        let err = DistributedQueryError::QueryTimeout {
            query_id: "q-123".to_string(),
            timeout_ms: 30000,
            nodes_involved: vec!["node-1".to_string(), "node-2".to_string()],
        };
        assert!(err.to_string().contains("q-123"));
        assert!(err.to_string().contains("30000ms"));

        let err = DistributedQueryError::QueryCancelled {
            query_id: "q-456".to_string(),
            reason: "Node failure".to_string(),
            failed_node: Some("node-3".to_string()),
        };
        assert!(err.to_string().contains("q-456"));
        assert!(err.to_string().contains("node-3"));

        let err = DistributedQueryError::ShuffleFailed {
            query_id: "q-789".to_string(),
            source_node: "node-1".to_string(),
            target_node: "node-2".to_string(),
            reason: "Network error".to_string(),
        };
        assert!(err.to_string().contains("q-789"));
        assert!(err.to_string().contains("node-1"));
        assert!(err.to_string().contains("node-2"));
    }

    #[test]
    fn test_error_aggregator() {
        let mut aggregator = ErrorAggregator::new("q-123");

        assert!(!aggregator.has_failures());
        assert!(!aggregator.all_failed());

        aggregator.record_success("node-1");
        assert!(!aggregator.has_failures());

        aggregator.record_failure("node-2", "Connection timeout");
        assert!(aggregator.has_failures());
        assert!(!aggregator.all_failed());

        let error = aggregator.into_error();
        assert!(error.is_some());
    }

    #[test]
    fn test_error_aggregator_all_failed() {
        let mut aggregator = ErrorAggregator::new("q-456");

        aggregator.record_failure("node-1", "Error 1");
        aggregator.record_failure("node-2", "Error 2");

        assert!(aggregator.has_failures());
        assert!(aggregator.all_failed());
    }

    #[test]
    fn test_timeout_executor_creation() {
        let executor = TimeoutExecutor::new(30000);
        assert_eq!(executor.default_timeout_ms, 30000);
    }

    #[test]
    fn test_node_unreachable_error() {
        let err = node_unreachable_error("node-1", "192.168.1.1:7947", "Connection refused");
        assert!(matches!(err, CoreError::Network(_)));
    }

    #[test]
    fn test_shuffle_failed_error() {
        let err = shuffle_failed_error("q-123", "node-1", "node-2", "Network error");
        assert!(matches!(err, CoreError::Internal(_)));
    }
}
