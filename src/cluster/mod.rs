pub mod gossip;

use chitchat::Chitchat;
use tokio::sync::Mutex;

use crate::cluster::gossip::GossipManager;
use crate::config::Config;
use crate::utils::error::{CoreError, CoreResult};
use keys::*;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

pub mod keys {
    use crate::utils::error::{CoreError, CoreResult};

    pub const SINGLE_NODE_CLUSTER_ID: &str = "CALMCORE_SINGLE_NODE_CLUSTER";

    /// Gossip key prefixes for different types of data
    pub const KEY_NODE_STATUS: &str = "status"; // e.g., "healthy", "suspect", "dead"
    pub const VALUE_NODE_STATUS_PREPARING: &str = "PREPARING";
    pub const VALUE_NODE_STATUS_READY: &str = "READY";
    pub const VALUE_NODE_STATUS_DECOMMISSIONING: &str = "DECOMMISSIONING";

    pub const CENTER_NODE_KEY: &str = "coord_node";

    pub const PARTITION_PREFIX: &str = "partition:";

    pub fn make_partition_key(table_name: &str, partition_name: &str, version: u64) -> String {
        format!(
            "{}{}:{}:{}",
            PARTITION_PREFIX, table_name, partition_name, version
        )
    }

    pub fn make_partition_key_prefix(table_name: &str, partition_name: &str) -> String {
        format!("{}{}:{}:", PARTITION_PREFIX, table_name, partition_name)
    }

    pub fn parse_partition_key(key: &str) -> CoreResult<(String, String, u64)> {
        let parts: Vec<&str> = key.split(':').collect();
        if parts.len() == 4 && parts[0] == "partition" {
            let version = parts[3].parse::<u64>().map_err(|_| {
                CoreError::InvalidParam(format!("Invalid version in partition key: {}", key))
            })?;
            return Ok((parts[1].to_string(), parts[2].to_string(), version));
        }
        Err(CoreError::InvalidParam(format!(
            "Invalid partition key: {}",
            key
        )))
    }

    pub fn make_node_id(internal_addr: &str) -> String {
        format!(
            "{}_{}",
            chrono::Utc::now().format("%Y%m%d%H%M%S%3f"),
            internal_addr
        )
    }

    pub fn parse_node_id(node_id: &str) -> CoreResult<(String, String)> {
        let parts: Vec<&str> = node_id.splitn(2, '_').collect();
        if parts.len() == 2 {
            return Ok((parts[0].to_string(), parts[1].to_string()));
        }
        Err(CoreError::InvalidParam(format!(
            "Invalid node ID: {}",
            node_id
        )))
    }
}

/// Cluster Manager with Chitchat integration
///
/// The central component managing cluster membership and node information.
/// Uses Chitchat for Gossip-based communication and failure detection.
pub struct ClusterManager {
    /// Chitchat instance (cloneable Arc for background tasks)
    pub gossip: GossipManager,

    pub chitchat: Arc<Mutex<Chitchat>>,

    /// Local node ID (UUID, immutable for node lifetime)
    node_id: String,

    internal_addr: String,

    pub coord_node: RwLock<Option<String>>,
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

        let gossip_listen_addr = config.gossip_addr()?;

        // nodeid format is millsecods timstamp yyyyMMddHHssmmMMM  since unix epoch + "_"+ host
        let internal_addr = config.internal_addr()?;
        let node_id = keys::make_node_id(&internal_addr);

        log::info!("🚀 [Cluster] Starting node {}", node_id);
        log::info!(
            "📡 [Cluster] gossip Listen address: {} internal address:{}",
            gossip_listen_addr,
            internal_addr
        );
        log::info!(
            "🌐 [Cluster] Running in CLUSTER mode with {} seed nodes",
            cluster_config.seed_nodes.len()
        );

        let gossip =
            GossipManager::new(node_id.clone(), &cluster_config, gossip_listen_addr).await?;

        let chitchat = gossip.chitchat();

        let manager = Self {
            gossip,
            chitchat,
            node_id,
            coord_node: RwLock::new(None),
            internal_addr,
        };

        manager.set_my_status_preparing().await;

        log::info!("✅ [Cluster] ClusterManager started successfully");

        Ok(manager)
    }

    pub async fn set_my_status_preparing(&self) {
        self.gossip
            .set(KEY_NODE_STATUS, VALUE_NODE_STATUS_PREPARING)
            .await;
        log::info!(
            "🔄 [Cluster] Updated node status to '{}'",
            VALUE_NODE_STATUS_PREPARING
        );
    }

    pub async fn set_my_status_ready(&self) {
        self.gossip
            .set(KEY_NODE_STATUS, VALUE_NODE_STATUS_READY)
            .await;
        log::info!(
            "✅ [Cluster] Updated node status to '{}'",
            VALUE_NODE_STATUS_READY
        );
    }

    pub async fn set_my_status_decommissioning(&self) {
        self.gossip
            .set(KEY_NODE_STATUS, VALUE_NODE_STATUS_DECOMMISSIONING)
            .await;
        log::info!(
            "⚠️  [Cluster] Updated node status to '{}'",
            VALUE_NODE_STATUS_DECOMMISSIONING
        );
    }

    /// Get local node ID
    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    /// =========================================== nodes operations ===========================================
    pub async fn idle_nodes(&self) -> Vec<String> {
        let chitchat = self.chitchat.lock().await;
        let mut idle_nodes = Vec::new();

        for node in chitchat.live_nodes() {
            if let Some(status) = chitchat
                .node_state(node)
                .and_then(|state| state.get(KEY_NODE_STATUS))
            {
                if status == VALUE_NODE_STATUS_READY && node.node_id != self.node_id {
                    idle_nodes.push(node.node_id.clone());
                }
            }
        }

        idle_nodes
    }

    /// =========================================== table_partition operations ===========================================
    pub async fn put_partition(&self, table_name: &str, partition_name: &str, version: u64) {
        let key = make_partition_key(table_name, partition_name, version);
        self.gossip.set(&key, &self.node_id).await;
    }

    pub async fn del_partition(&self, table_name: &str, partition_name: &str) {
        let key = make_partition_key_prefix(table_name, partition_name);
        for (k, v) in self.gossip.find_local_by_prefix(&key).await {
            log::info!("🗑️  [Cluster] Deleting partition key: {} onwer:{}", k, v);
            self.gossip.delete(&k).await;
        }
    }

    /// find the owner node ID of a partition
    /// if found multiple owners, return None and log a warning
    pub async fn find_partition_owner(
        &self,
        table_name: &str,
        partition_name: &str,
    ) -> Option<String> {
        let key = make_partition_key_prefix(table_name, partition_name);
        let partitions = self.gossip.find_all_by_prefix(&key).await;

        let mut max_version = None;
        let mut latest_owner: Option<String> = None;

        for (key, owner_node_id) in partitions.iter() {
            if let Ok((_, _, version)) = parse_partition_key(key) {
                log::debug!(
                    "🔍 [Cluster] Found partition owner: table='{}', partition='{}', version={}, owner='{}'",
                    table_name,
                    partition_name,
                    version,
                    owner_node_id
                );
                match max_version {
                    Some(v) if version < v => continue,
                    _ => {
                        max_version = Some(version);
                        latest_owner = Some(owner_node_id.clone());
                    }
                }
            } else {
                log::warn!("⚠️  [Cluster] Invalid partition key format found: {}", key);
            }
        }

        latest_owner
    }

    pub async fn find_partition_routes(&self) -> HashMap<(String, String), (String, u64)> {
        let chitchat = self.chitchat.lock().await;
        let mut routers = HashMap::new();

        // Scan all node states for partition keys
        for (_chitchat_id, node_state) in chitchat.node_states() {
            for (key, owner_node_id) in node_state.key_values() {
                if key.starts_with("partition:") {
                    match parse_partition_key(key) {
                        Ok((table_name, partition_name, version)) => {
                            routers
                                .entry((table_name, partition_name))
                                .and_modify(|(existing_owner, existing_version)| {
                                    if version > *existing_version {
                                        *existing_owner = owner_node_id.to_string();
                                        *existing_version = version;
                                    }
                                })
                                .or_insert((owner_node_id.to_string(), version));
                        }
                        Err(e) => {
                            log::warn!(
                                "⚠️  [Cluster] find_partition_routes Failed to parse partition key '{}': {:?}",
                                key,
                                e
                            );
                        }
                    }
                }
            }
        }

        routers
    }

    /// =========================================== coord operations ===========================================
    pub async fn get_coord(&self) -> Option<String> {
        let coord_node = self.coord_node.read().unwrap();
        coord_node.clone()
    }

    pub async fn reset_coord(&self) -> CoreResult<()> {
        let mut coord_node = self.coord_node.write().unwrap();
        *coord_node = None;
        self.gossip.delete(CENTER_NODE_KEY).await;
        Ok(())
    }

    pub async fn set_coord_node(&self, node_id: &str) -> CoreResult<()> {
        let mut coord_node = self.coord_node.write().unwrap();
        *coord_node = Some(node_id.to_string());
        self.gossip.set(CENTER_NODE_KEY, node_id).await;
        Ok(())
    }

    pub fn am_i_coord_node(&self) -> bool {
        let coord_node = self.coord_node.read().unwrap();
        if let Some(coord_id) = coord_node.as_ref() {
            coord_id == &self.node_id
        } else {
            false
        }
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
