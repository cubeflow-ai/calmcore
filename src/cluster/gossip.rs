//! Gossip Protocol Management with Chitchat Integration
//!
//! This module implements the ClusterManager using the Chitchat library for
//! decentralized node discovery, failure detection, and state propagation.
//!
//! ## Key Features
//!
//! - **Node Discovery**: Automatic discovery via Gossip protocol
//! - **Failure Detection**: SWIM-like failure detection built into Chitchat
//! - **State Propagation**: Key-value state propagation across all nodes
//! - **Event Broadcasting**: Cluster events via tokio broadcast channels

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use chitchat::transport::UdpTransport;
use chitchat::{
    spawn_chitchat, Chitchat, ChitchatConfig, ChitchatHandle, ChitchatId, FailureDetectorConfig,
};
use tokio::sync::Mutex;

use crate::cluster::node_manager::NodeManager;
use crate::cluster::PartitionManager;
use crate::config::cluster::ClusterSettings;
use crate::config::Config;
use crate::utils::error::{CoreError, CoreResult};

use crate::cluster::keys::*;

pub struct PartitionRouter {
    pub table_name: String,
    pub partition_name: String,
    pub owner_node_id: String,
    pub internal_addr: String,
}

/// Cluster Manager with Chitchat integration
///
/// The central component managing cluster membership and node information.
/// Uses Chitchat for Gossip-based communication and failure detection.
pub struct ClusterManager {
    /// Chitchat handle for Gossip communication
    chitchat_handle: Option<ChitchatHandle>,

    /// Chitchat instance (cloneable Arc for background tasks)
    pub chitchat: Arc<Mutex<Chitchat>>,

    pub node_manager: Arc<NodeManager>,

    /// Local node ID (UUID, immutable for node lifetime)
    node_id: String,
}

impl ClusterManager {
    /// Create and start cluster manager
    ///
    /// # Requirements
    /// - Requirements 1.2: Join cluster via seed nodes when configured
    pub async fn new(config: &Config) -> CoreResult<Self> {
        let cluster_config = config
            .cluster
            .as_ref()
            .ok_or_else(|| CoreError::ConfigError("Cluster config missing".to_string()))?;

        let host = config.host.clone().unwrap();

        let gossip_listen_addr = format!("{}:{}", host, cluster_config.gossip_port)
            .parse::<SocketAddr>()
            .map_err(|e| CoreError::ConfigError(format!("Invalid gossip listen address: {}", e)))?;

        // nodeid format is millsecods timstamp yyyyMMddHHssmmMMM  since unix epoch + "_"+ host
        let node_id = format!("{}_{}", chrono::Utc::now().format("%Y%m%d%H%M%S%3f"), host);

        log::info!("🚀 [Cluster] Starting node {}", node_id);
        log::info!("📡 [Cluster] gossip Listen address: {}", gossip_listen_addr);
        log::info!(
            "🌐 [Cluster] Running in CLUSTER mode with {} seed nodes",
            cluster_config.seed_nodes.len()
        );

        // Initialize Chitchat
        let chitchat_handle =
            Self::init_chitchat(node_id.clone(), cluster_config, gossip_listen_addr).await?;
        let chitchat = chitchat_handle.chitchat().clone();

        let manager = Self {
            chitchat_handle: Some(chitchat_handle),
            chitchat: chitchat.clone(),
            node_id: node_id.clone(),
            node_manager: Arc::new(NodeManager::new(chitchat.clone()).await),
        };

        manager.set_my_status_preparing().await;

        log::info!("✅ [Cluster] ClusterManager started successfully");

        Ok(manager)
    }

    /// Initialize Chitchat for cluster communication
    ///
    /// # Requirements
    /// - Requirements 1.2: Join cluster via seed nodes
    /// - Requirements 1.3: Receive current cluster membership and topology
    /// - Requirements 1.4: Fail if cannot contact any seed node
    async fn init_chitchat(
        node_id: String,
        config: &ClusterSettings,
        listen_addr: SocketAddr,
    ) -> CoreResult<ChitchatHandle> {
        // Create Chitchat ID
        let chitchat_id = ChitchatId::new(
            node_id,
            0, // generation (increments on restart)
            listen_addr,
        );

        // Configure failure detector
        let failure_detector_config = FailureDetectorConfig {
            phi_threshold: 8.0,
            initial_interval: config.gossip_interval_ms,
            ..Default::default()
        };

        // Create Chitchat config - seed_nodes are strings for DNS resolution
        let chitchat_config = ChitchatConfig {
            chitchat_id,
            cluster_id: config.cluster_id.clone(),
            gossip_interval: config.gossip_interval_ms,
            listen_addr,
            seed_nodes: config.seed_nodes.clone(),
            failure_detector_config,
            marked_for_deletion_grace_period: Duration::from_secs(3600),
            catchup_callback: None,
            extra_liveness_predicate: None,
        };

        log::info!("🔧 [Cluster] Initializing Chitchat with config:");
        log::info!("   - Cluster ID: {}", config.cluster_id);
        log::info!("   - Listen: {}", listen_addr);
        log::info!("   - Seeds: {:?}", config.seed_nodes);

        // Create UDP transport
        let transport = UdpTransport;

        // Spawn Chitchat
        let chitchat_handle = spawn_chitchat(chitchat_config, vec![], &transport)
            .await
            .map_err(|e| CoreError::Internal(format!("Failed to start Chitchat: {}", e)))?;

        // If we have seed nodes, decide whether to wait or start immediately
        let is_seed = config.is_seed_node(listen_addr);

        if is_seed {
            // Seed nodes start immediately without waiting
            log::info!("🌱 [Cluster] Starting as SEED node (will not wait for other seeds)");
            log::info!("🔗 [Cluster] Will accept connections from other nodes...");

            tokio::time::sleep(Duration::from_secs(15)).await; // Give time to start
        } else {
            // Regular nodes must wait for seed nodes
            log::info!("🔗 [Cluster] Starting as WORKER node, connecting to seed nodes...");

            let chitchat = chitchat_handle.chitchat();
            let mut attempts = 0;

            loop {
                attempts += 1;

                // Check if we've discovered any peers
                let guard = chitchat.lock().await;
                let live_nodes = guard.live_nodes().count();
                drop(guard);

                if live_nodes > 1 {
                    log::info!(
                        "✅ [Cluster] Successfully joined cluster! Discovered {} live nodes",
                        live_nodes
                    );
                    break;
                } else {
                    if attempts == 1 {
                        log::warn!("⏳ [Cluster] Waiting for seed nodes to become available...");
                    }
                    log::info!(
                        "🔄 [Cluster] Attempting to connect to seed nodes... (attempt #{})",
                        attempts
                    );
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }

        Ok(chitchat_handle)
    }

    pub async fn run_election(&self) -> CoreResult<ChitchatId> {
        let mut live_nodes: Vec<ChitchatId> =
            self.chitchat.lock().await.live_nodes().cloned().collect();

        live_nodes.sort_by(|a, b| a.node_id.cmp(&b.node_id));

        let coord_node = live_nodes
            .first()
            .cloned()
            .ok_or_else(|| CoreError::Internal("No live nodes found for election".to_string()))?;

        if self.get_node_status(&coord_node).await? == NodeStatus::Ready {
            return Err(CoreError::Internal(format!(
                "Elected coord node {} is dead",
                coord_node.node_id
            )));
        }

        Ok(coord_node)
    }

    pub async fn find_partition_routes(&self) -> CoreResult<Vec<PartitionRouter>> {
        let mut routers = Vec::new();

        let guard = self.chitchat.lock().await;

        for (node_id, state) in guard.node_states() {
            for (key, value) in state.key_values() {
                if key.starts_with("partition:") {
                    let parts: Vec<&str> = key.split(':').collect();
                    if parts.len() == 3 {
                        let table_name = parts[1].to_string();
                        let partition_name = parts[2].to_string();
                        let owner_node_id = node_id.node_id.clone();
                        let internal_addr = value.to_string();

                        routers.push(PartitionRouter {
                            table_name,
                            partition_name,
                            owner_node_id,
                            internal_addr,
                        });
                    }
                }
            }
        }

        Ok(routers)
    }

    fn partition_key(table_name: &str, partition_name: &str) -> String {
        format!("partition:{}:{}", table_name, partition_name)
    }

    pub async fn put_partition(&self, table_name: &str, partition_name: &str) {
        let key = Self::partition_key(table_name, partition_name);
        let mut guard = self.chitchat.lock().await;
        guard
            .self_node_state()
            .set(key.clone(), self.node_id.clone());
    }

    pub async fn del_partition(&self, table_name: &str, partition_name: &str) {
        let key = Self::partition_key(table_name, partition_name);
        let mut guard = self.chitchat.lock().await;
        guard.self_node_state().delete(&key);
    }

    pub async fn get_partition(
        &self,
        table_name: &str,
        partition_name: &str,
    ) -> Option<(String, String)> {
        let key = Self::partition_key(table_name, partition_name);

        let guard = self.chitchat.lock().await;

        for (_, state) in guard.node_states() {
            if let Some(value) = state.get(&key) {
                return Some(value.to_string());
            }
        }
        None
    }

    /// Get local node ID
    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    pub async fn set_my_status_preparing(&self) {
        self.gossip_set(KEY_NODE_STATUS, VALUE_NODE_STATUS_PREPARING)
            .await;
        log::info!(
            "🔄 [Cluster] Updated node status to '{}'",
            VALUE_NODE_STATUS_PREPARING
        );
    }

    pub async fn set_my_status_ready(&self) {
        self.gossip_set(KEY_NODE_STATUS, VALUE_NODE_STATUS_READY)
            .await;
        log::info!(
            "✅ [Cluster] Updated node status to '{}'",
            VALUE_NODE_STATUS_READY
        );
    }

    pub async fn set_my_status_decommissioning(&self) {
        self.gossip_set(KEY_NODE_STATUS, VALUE_NODE_STATUS_DECOMMISSIONING)
            .await;
        log::info!(
            "⚠️  [Cluster] Updated node status to '{}'",
            VALUE_NODE_STATUS_DECOMMISSIONING
        );
    }

    pub async fn set_internal_addr(&self, addr: &str) {
        self.chitchat
            .lock()
            .await
            .self_node_state()
            .set(INTERNAL_ADDR_KEY, addr);
    }

    // ========================================================================
    // Gossip KV Operations (Requirements 4.3)
    // ========================================================================

    /// Set key-value in Gossip state
    ///
    /// # Requirements
    /// - Requirements 4.3: Propagate topology changes via Gossip
    pub async fn gossip_set(&self, key: &str, value: &str) {
        let mut guard = self.chitchat.lock().await;
        guard.self_node_state().set(key, value);
        log::debug!("📝 [Gossip] Set key: {} = {}", key, value);
    }

    /// Get key-value from Gossip state
    pub async fn gossip_get(&self, key: &str) -> Option<String> {
        let mut guard = self.chitchat.lock().await;

        // First check our own state
        if let Some(value) = guard.self_node_state().get(key) {
            return Some(value.to_string());
        }

        // Then check other nodes' states
        for (_chitchat_id, node_state) in guard.node_states() {
            if let Some(value) = node_state.get(key) {
                return Some(value.to_string());
            }
        }
        None
    }

    /// Set key-value (legacy API)
    pub async fn set_key_value(&self, key: String, value: String) -> CoreResult<()> {
        self.gossip_set(&key, &value).await;
        Ok(())
    }

    /// Get key-value (legacy API)
    pub async fn get_key_value(&self, key: &str) -> Option<String> {
        self.gossip_get(key).await
    }

    /// Get all keys from Gossip state
    pub async fn get_all_keys(&self) -> HashMap<String, String> {
        let mut result = HashMap::new();
        let mut guard = self.chitchat.lock().await;

        // Collect from all node states
        for (_chitchat_id, node_state) in guard.node_states() {
            for (key, value) in node_state.key_values() {
                result.insert(key.to_string(), value.to_string());
            }
        }

        // Also include our own state
        for (key, value) in guard.self_node_state().key_values() {
            result.insert(key.to_string(), value.to_string());
        }

        result
    }
    /// Delete key from Gossip state
    pub async fn delete_key(&self, key: &str) -> CoreResult<()> {
        let mut guard = self.chitchat.lock().await;
        guard.self_node_state().delete(key);
        log::debug!("🗑️  [Gossip] Deleted key: {}", key);
        Ok(())
    }

    // ========================================================================
    // Node Metrics Broadcasting (Requirements 2.1, 2.4)
    // ========================================================================

    /// Update my node metrics and broadcast via Gossip
    pub async fn update_my_metrics(
        &self,
        partition_count: usize,
        load: f64,
        memory_usage_bytes: u64,
    ) {
        // Broadcast via Gossip
        self.gossip_set(KEY_PARTITION_COUNT, &partition_count.to_string())
            .await;
        self.gossip_set(KEY_LOAD, &load.to_string()).await;
        self.gossip_set(KEY_MEMORY, &memory_usage_bytes.to_string())
            .await;

        log::debug!(
            "📊 [Cluster] Updated metrics: partitions={}, load={:.2}, memory={}",
            partition_count,
            load,
            memory_usage_bytes
        );
    }

    pub async fn find_coord_node(&self) -> CoreResult<String> {
        let chitchat = self.chitchat.lock().await;

        // 从 live_nodes 和他们的 state 里面的 KEY_NODE_STATUS 中寻找最小的 node_id
        // 要求状态为 READY，如果最小节点是 PREPARING 或没有 state 则返回 None
        // 如果是 DECOMMISSIONING 则跳过，找下一个最小的

        // 1. 收集所有已知节点：包括 live_nodes 和从 state 中 CENTER_NODE_KEY 声明的节点
        let mut all_known_nodes = HashSet::new();

        // 添加所有 live nodes
        for node in chitchat.live_nodes() {
            all_known_nodes.insert(node.node_id.clone());
        }

        // 添加从各个节点 state 中发现的 CENTER_NODE_KEY 声明的协调节点
        for (_node_id, state) in chitchat.node_states() {
            if let Some(coord_node) = state.get(CENTER_NODE_KEY) {
                all_known_nodes.insert(coord_node.to_string());
            }
        }

        // 2. 将所有已知节点按 node_id 排序
        let mut sorted_nodes: Vec<String> = all_known_nodes.into_iter().collect();
        sorted_nodes.sort();

        // 3. 按从小到大的顺序检查每个节点的状态
        for node_id in sorted_nodes {
            // 查找该节点的状态
            let node_status = chitchat
                .live_nodes()
                .find(|n| n.node_id == node_id)
                .and_then(|n| chitchat.node_state(n))
                .and_then(|state| state.get(KEY_NODE_STATUS));

            match node_status {
                Some(status) => match status {
                    VALUE_NODE_STATUS_READY => {
                        // 找到第一个 READY 的节点，返回
                        return Ok(node_id);
                    }
                    VALUE_NODE_STATUS_DECOMMISSIONING => {
                        // 跳过正在退役的节点，继续找下一个
                        continue;
                    }
                    VALUE_NODE_STATUS_PREPARING => {
                        // 当前最小可用节点还在准备中，返回 None 等待
                        return Err(CoreError::ClusterState(format!(
                            "Node:{} is preparing",
                            node_id
                        )));
                    }
                    _ => {
                        // 未知状态，返回 None 等待
                        return Err(CoreError::ClusterState(format!(
                            "Node:{} has unknown status",
                            node_id
                        )));
                    }
                },
                None => {
                    // 当前最小可用节点没有状态信息，返回 None 等待
                    return Err(CoreError::ClusterState(format!(
                        "Node:{} has no status",
                        node_id
                    )));
                }
            }
        }

        // 至少能找到自己所以这里不会发生
        Err(CoreError::ClusterState(
            "No suitable coordinator node found".to_string(),
        ))
    }
}
