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

pub use event::ClusterEvent;
pub use gossip::ClusterManager;
pub use partition_manager::PartitionManager;

use crate::utils::error::CoreResult;
use std::net::SocketAddr;
use std::time::Duration;

mod keys {
    /// Gossip key prefixes for different types of data
    pub const KEY_LOAD: &str = "load";
    pub const KEY_MEMORY: &str = "memory";
    pub const KEY_PARTITION_COUNT: &str = "partition_count";
    pub const KEY_NODE_STATUS: &str = "status"; // e.g., "healthy", "suspect", "dead"
    pub const VALUE_NODE_STATUS_PREPARING: &str = "PREPARING";
    pub const VALUE_NODE_STATUS_READY: &str = "READY";
    pub const VALUE_NODE_STATUS_DECOMMISSIONING: &str = "DECOMMISSIONING";

    pub const INTERNAL_ADDR_KEY: &str = "internal_addr";
    pub const CENTER_NODE_KEY: &str = "coord_node";
}

/// 集群配置
#[derive(Debug, Clone)]
pub struct ClusterConfig {
    /// 集群 ID（同一集群的节点必须相同）
    pub cluster_id: String,

    /// Gossip 监听端口 (UDP)
    pub gossip_port: u16,

    /// 内部 RPC 服务端口 (TCP)
    pub internal_port: u16,

    /// 种子节点列表（用于初始加入集群）
    /// Empty = standalone mode
    /// Non-empty = must join cluster or fail
    pub seed_nodes: Vec<String>,

    /// Gossip 间隔
    pub gossip_interval: Duration,

    /// Time before marking node as Suspect
    pub failure_timeout: Duration,

    /// Additional time before marking Suspect as Dead
    pub suspect_timeout: Duration,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            cluster_id: "calm-cluster".to_string(),
            gossip_port: 7946,
            internal_port: 7947,
            seed_nodes: vec![],
            gossip_interval: Duration::from_millis(500),
            failure_timeout: Duration::from_secs(10),
            suspect_timeout: Duration::from_secs(10),
        }
    }
}

impl ClusterConfig {
    /// Parse seed nodes to SocketAddr list
    pub fn parse_seed_nodes(&self) -> CoreResult<Vec<SocketAddr>> {
        self.seed_nodes
            .iter()
            .map(|s| {
                s.parse().map_err(|e| {
                    crate::utils::error::CoreError::Internal(format!(
                        "Invalid seed node address '{}': {}",
                        s, e
                    ))
                })
            })
            .collect()
    }

    pub fn real_ip(&self) -> Option<String> {
        // Try to get real IP by connecting to a seed node
        for seed in &self.seed_nodes {
            if let Some(real_ip) = crate::utils::net::get_real_ip(seed, Duration::from_secs(1)) {
                return Some(real_ip);
            }
        }
        None
    }

    /// Check if this node is a seed node
    ///
    /// Logic:
    /// - If seed_nodes is empty → We ARE a seed node (bootstrap/standalone)
    /// - If seed_nodes is configured → Check if our IP is in the list
    ///   - NOT in list → We ARE a seed node
    ///   - In list → We are a worker node
    pub fn is_seed_node(&self, my_addr: SocketAddr) -> bool {
        // If no seed nodes configured, default to being a seed node
        if self.seed_nodes.is_empty() {
            return true;
        }

        // Check if our IP appears in any seed node address
        let found_self = self.seed_nodes.iter().any(|seed| {
            seed.parse::<SocketAddr>()
                .map(|addr| addr.ip() == my_addr.ip())
                .unwrap_or(false)
        });

        // If we're NOT in the seed list, we're a seed node
        !found_self
    }

    /// Calculate quorum threshold for given node count
    pub fn quorum_threshold(&self, node_count: usize) -> usize {
        node_count / 2 + 1
    }

    /// Validate configuration
    pub fn validate(&self) -> CoreResult<()> {
        // Cluster mode requires seed nodes
        if self.seed_nodes.is_empty() {
            return Err(crate::utils::error::CoreError::ConfigError(
                "seed_nodes cannot be empty in cluster mode. For single-node deployment, do not initialize ClusterManager.".to_string(),
            ));
        }
        // Validate seed nodes
        self.parse_seed_nodes()?;

        Ok(())
    }
}
