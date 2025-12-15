//! 集群管理模块
//!
//! 负责：
//! - 节点发现和健康检查（基于 Chitchat）
//! - Partition Owner 映射维护
//! - 元数据同步
//! - 自动故障转移
//! - 投票协调（Voting-based failover）

pub(crate) mod event;
pub mod gossip;
pub(crate) mod node_manager;
pub(crate) mod partition_manager;

use chitchat::{Chitchat, ChitchatHandle};
pub use event::ClusterEvent;
pub use gossip::ClusterManager;
pub use partition_manager::PartitionManager;
use tokio::sync::Mutex;

use crate::cluster::gossip::GossipManager;
use crate::config::Config;
use crate::utils::error::{CoreError, CoreResult};
use keys::*;
use std::sync::Arc;

mod keys {

    pub const LOCAL_MODEL_NODE_NAME: &str = "local";

    /// Gossip key prefixes for different types of data
    pub const KEY_LOAD: &str = "load";
    pub const KEY_MEMORY: &str = "memory";
    pub const KEY_PARTITION_COUNT: &str = "partition_count";
    pub const KEY_NODE_STATUS: &str = "status"; // e.g., "healthy", "suspect", "dead"
    pub const VALUE_NODE_STATUS_PREPARING: &str = "PREPARING";
    pub const VALUE_NODE_STATUS_READY: &str = "READY";
    pub const VALUE_NODE_STATUS_DECOMMISSIONING: &str = "DECOMMISSIONING";

    pub const CENTER_NODE_KEY: &str = "coord_node";

    pub fn make_partition_key(table_name: &str, partition_name: &str) -> String {
        format!("partition:{}:{}", table_name, partition_name)
    }

    pub fn make_node_id(internal_addr: &str) -> String {
        format!(
            "{}_{}",
            chrono::Utc::now().format("%Y%m%d%H%M%S%3f"),
            internal_addr
        )
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

    pub async fn pub_coord_node(&self, node_id: &str) {
        self.gossip.set(CENTER_NODE_KEY, node_id).await;
    }

    pub async fn put_partition(&self, table_name: &str, partition_name: &str) {
        let key = make_partition_key(table_name, partition_name);
        self.gossip.set(&key, &self.node_id).await;
    }

    pub async fn del_partition(&self, table_name: &str, partition_name: &str) {
        let key = make_partition_key(table_name, partition_name);
        self.gossip.delete(&key).await;
    }

    /// find the owner node ID of a partition
    /// if found multiple owners, return None and log a warning
    pub async fn find_partition_owner(
        &self,
        table_name: &str,
        partition_name: &str,
    ) -> Option<String> {
        let key = make_partition_key(table_name, partition_name);
        let partitions = self.gossip.find_all(&key).await;
        if partitions.len() == 1 {
            Some(partitions.into_iter().next().unwrap())
        } else {
            log::warn!(
                "⚠️  [Cluster] Partition owner lookup for '{}/{}' returned {:?} entries",
                table_name,
                partition_name,
                partitions
            );
            None
        }
    }
}
