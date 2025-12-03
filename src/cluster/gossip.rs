//! Gossip 协议管理
//!
//! ## Gossip 协议原理
//!
//! ### 核心思想：去中心化的状态传播
//!
//! 每个节点都维护一份**集群状态的副本**（内存），包括：
//! - 所有节点列表 (nodes: HashMap<NodeId, NodeInfo>)
//! - 共享键值对 (kv_store: HashMap<String, String>)
//!
//! 节点之间通过 Gossip 协议**相互传递状态更新**：
//!
//! ```text
//! 1. 节点 A 修改状态（例如：设置 partition_owner:table1:0 = nodeA）
//!    ↓
//! 2. 每隔 500ms，节点 A 随机选择 2-3 个邻居
//!    ↓
//! 3. 发送自己的状态更新给这些邻居
//!    ↓
//! 4. 邻居收到后，合并状态（版本号更高的胜出）
//!    ↓
//! 5. 邻居继续传播给它们的邻居
//!    ↓
//! 6. 最终所有节点的状态收敛一致（O(log N) 轮）
//! ```
//!
//! ### 关键特性
//!
//! - **最终一致性**：状态变更会在几秒内传播到所有节点
//! - **去中心化**：无需 Master，任意节点可以修改状态
//! - **容错性**：部分节点故障不影响协议运行
//! - **可扩展**：节点数增加时，收敛时间只是 O(log N)
//!
//! ### Phase 1 简化实现
//!
//! 当前是单机内存模拟，没有真正的网络传播。
//! Phase 2 将集成 Chitchat 实现真正的 Gossip 协议。
//!
//! TODO: 集成 Chitchat 或其他 Gossip 协议库use std::collections::HashMap;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use super::node::{NodeId, NodeInfo, NodeState};
use crate::utils::error::CoreResult;

/// 集群管理器（简化实现）
pub struct ClusterManager {
    /// 本节点 ID
    node_id: NodeId,

    /// 节点列表（共享状态）
    nodes: Arc<RwLock<HashMap<NodeId, NodeInfo>>>,

    /// 键值存储（用于 Partition Owner 等）
    kv_store: Arc<RwLock<HashMap<String, String>>>,

    /// 故障回调
    failure_callbacks: Arc<RwLock<Vec<Box<dyn Fn(NodeId) + Send + Sync>>>>,

    /// 心跳间隔（秒）
    heartbeat_interval_secs: u64,

    /// 故障检测超时（秒）
    failure_timeout_secs: u64,
}

impl ClusterManager {
    /// 创建并启动集群管理器（简化实现）
    pub async fn new(
        node_id: String,
        _cluster_id: String,
        listen_addr: String,
        _seed_nodes: Vec<String>,
    ) -> CoreResult<Self> {
        log::info!("🚀 [Cluster] Starting node {}", node_id);
        log::info!("📡 [Cluster] Listening on {}", listen_addr);

        let mut nodes = HashMap::new();
        nodes.insert(node_id.clone(), NodeInfo::new(node_id.clone(), listen_addr));

        let manager = Self {
            node_id: node_id.clone(),
            nodes: Arc::new(RwLock::new(nodes)),
            kv_store: Arc::new(RwLock::new(HashMap::new())),
            failure_callbacks: Arc::new(RwLock::new(Vec::new())),
            heartbeat_interval_secs: 5,
            failure_timeout_secs: 30,
        };

        // 启动心跳任务
        manager.start_heartbeat_task();

        // 启动故障检测任务
        manager.start_failure_detection_task();

        log::info!("✅ [Cluster] Heartbeat and failure detection started");

        Ok(manager)
    }

    /// 获取本节点 ID
    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    /// 获取所有活跃节点
    pub async fn live_nodes(&self) -> Vec<NodeInfo> {
        let nodes = self.nodes.read().await;
        nodes.values().filter(|n| n.is_alive()).cloned().collect()
    }

    /// 获取节点数量
    pub async fn node_count(&self) -> usize {
        self.live_nodes().await.len()
    }

    /// 设置键值对（用于存储 Partition Owner 等信息）
    pub async fn set_key_value(&self, key: String, value: String) -> CoreResult<()> {
        let mut kv = self.kv_store.write().await;
        kv.insert(key.clone(), value.clone());
        log::debug!("📝 [Cluster] Set key: {} = {}", key, value);
        Ok(())
    }

    /// 获取键值
    pub async fn get_key_value(&self, key: &str) -> Option<String> {
        let kv = self.kv_store.read().await;
        kv.get(key).cloned()
    }

    /// 获取所有键值对
    pub async fn get_all_keys(&self) -> HashMap<String, String> {
        let kv = self.kv_store.read().await;
        kv.clone()
    }

    /// 删除键值对
    pub async fn delete_key(&self, key: &str) -> CoreResult<()> {
        let mut kv = self.kv_store.write().await;
        kv.remove(key);
        log::debug!("🗑️  [Cluster] Deleted key: {}", key);
        Ok(())
    }

    /// 监听节点故障事件
    pub async fn watch_failures<F>(&self, callback: F)
    where
        F: Fn(NodeId) + Send + Sync + 'static,
    {
        let mut callbacks = self.failure_callbacks.write().await;
        callbacks.push(Box::new(callback));
        log::info!("✅ [Cluster] Registered failure callback");
    }

    /// 优雅关闭
    pub async fn shutdown(&self) -> CoreResult<()> {
        log::info!("👋 [Cluster] Shutting down cluster manager");
        Ok(())
    }

    /// 手动触发节点故障（用于测试）
    pub async fn _simulate_node_failure(&self, node_id: &str) -> CoreResult<()> {
        let mut nodes = self.nodes.write().await;
        if let Some(node) = nodes.get_mut(node_id) {
            node.state = NodeState::Dead;
        }

        // 触发回调
        let callbacks = self.failure_callbacks.read().await;
        for callback in callbacks.iter() {
            callback(node_id.to_string());
        }

        Ok(())
    }

    /// 启动心跳任务（定期更新本节点的心跳时间）
    fn start_heartbeat_task(&self) {
        let node_id = self.node_id.clone();
        let nodes = self.nodes.clone();
        let interval = self.heartbeat_interval_secs;

        tokio::spawn(async move {
            loop {
                tokio::time::sleep(tokio::time::Duration::from_secs(interval)).await;

                let mut nodes_guard = nodes.write().await;
                if let Some(node) = nodes_guard.get_mut(&node_id) {
                    node.last_heartbeat = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs();

                    log::debug!("💓 [Cluster] Heartbeat updated for {}", node_id);
                }
            }
        });
    }

    /// 启动故障检测任务（定期检查节点心跳超时）
    fn start_failure_detection_task(&self) {
        let nodes = self.nodes.clone();
        let failure_callbacks = self.failure_callbacks.clone();
        let timeout = self.failure_timeout_secs;
        let check_interval = self.heartbeat_interval_secs;

        tokio::spawn(async move {
            loop {
                tokio::time::sleep(tokio::time::Duration::from_secs(check_interval)).await;

                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs();

                let mut failed_nodes = Vec::new();

                // 检查超时节点
                {
                    let mut nodes_guard = nodes.write().await;
                    for (node_id, node) in nodes_guard.iter_mut() {
                        if node.state == NodeState::Alive {
                            let elapsed = now.saturating_sub(node.last_heartbeat);
                            if elapsed > timeout {
                                log::warn!(
                                    "⚠️  [Cluster] Node {} heartbeat timeout ({} seconds)",
                                    node_id,
                                    elapsed
                                );
                                node.state = NodeState::Dead;
                                failed_nodes.push(node_id.clone());
                            }
                        }
                    }
                }

                // 触发故障回调
                if !failed_nodes.is_empty() {
                    let callbacks = failure_callbacks.read().await;
                    for node_id in failed_nodes {
                        log::error!("💀 [Cluster] Node {} marked as DEAD", node_id);
                        for callback in callbacks.iter() {
                            callback(node_id.clone());
                        }
                    }
                }
            }
        });
    }

    /// 手动注册新节点（用于模拟节点加入）
    pub async fn register_node(&self, node_id: String, gossip_addr: String) -> CoreResult<()> {
        let mut nodes = self.nodes.write().await;
        nodes.insert(node_id.clone(), NodeInfo::new(node_id.clone(), gossip_addr));
        log::info!("✅ [Cluster] Node {} registered", node_id);
        Ok(())
    }
}
