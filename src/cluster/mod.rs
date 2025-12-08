//! 集群管理模块
//!
//! 负责：
//! - 节点发现和健康检查（基于 Chitchat）
//! - Partition Owner 映射维护
//! - 元数据同步
//! - 自动故障转移
//! - 投票协调（Voting-based failover）
//! - 查询路由（Query routing）

pub mod gossip;
pub mod node;
pub mod partition_manager;
pub mod query_router;
pub mod voting;

pub use gossip::{
    ClusterManager, ClusterMembership, ClusterMetrics, ClusterState, ClusterStatus, MemberInfo,
    NodeMetrics,
};
pub use node::{NodeId, NodeInfo, NodeState};
pub use partition_manager::{
    PartitionManager, PartitionTopology, TableTopology, TableTopologySummary, TopologySummary,
};
pub use query_router::QueryRouter;
pub use voting::{PartitionKey, VoteRecord, VoteRound, VotingCoordinator};

use crate::utils::error::CoreResult;
use std::net::SocketAddr;
use std::time::Duration;

/// 集群配置
#[derive(Debug, Clone)]
pub struct ClusterConfig {
    /// 是否启用集群模式
    pub enabled: bool,

    /// 本节点 ID（唯一标识，generated on first start）
    pub node_id: String,

    /// 集群 ID（同一集群的节点必须相同）
    pub cluster_id: String,

    /// Gossip 监听地址 (UDP)
    pub listen_addr: String,

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

    /// Vote timeout (per round)
    pub vote_timeout: Duration,

    /// Minimum cluster size for operation
    pub min_cluster_size: usize,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            node_id: format!("node-{}", uuid::Uuid::new_v4().to_string()[..8].to_string()),
            cluster_id: "calm-cluster".to_string(),
            listen_addr: "0.0.0.0:7946".to_string(),
            seed_nodes: vec![],
            gossip_interval: Duration::from_millis(500),
            failure_timeout: Duration::from_secs(10),
            suspect_timeout: Duration::from_secs(10),
            vote_timeout: Duration::from_secs(10),
            min_cluster_size: 3,
        }
    }
}

impl ClusterConfig {
    /// 从环境变量加载配置（环境变量覆盖文件配置）
    pub fn from_env() -> Self {
        let mut config = Self::default();

        if let Ok(val) = std::env::var("CALM_CLUSTER_ENABLED") {
            config.enabled = val.parse().unwrap_or(false);
        }

        if let Ok(val) = std::env::var("CALM_NODE_ID") {
            config.node_id = val;
        }

        if let Ok(val) = std::env::var("CALM_CLUSTER_ID") {
            config.cluster_id = val;
        }

        if let Ok(val) = std::env::var("CALM_GOSSIP_ADDR") {
            config.listen_addr = val;
        }

        if let Ok(val) = std::env::var("CALM_SEED_NODES") {
            config.seed_nodes = val
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
        }

        if let Ok(val) = std::env::var("CALM_GOSSIP_INTERVAL_MS") {
            if let Ok(ms) = val.parse::<u64>() {
                config.gossip_interval = Duration::from_millis(ms);
            }
        }

        if let Ok(val) = std::env::var("CALM_FAILURE_TIMEOUT_SECS") {
            if let Ok(secs) = val.parse::<u64>() {
                config.failure_timeout = Duration::from_secs(secs);
            }
        }

        if let Ok(val) = std::env::var("CALM_SUSPECT_TIMEOUT_SECS") {
            if let Ok(secs) = val.parse::<u64>() {
                config.suspect_timeout = Duration::from_secs(secs);
            }
        }

        if let Ok(val) = std::env::var("CALM_VOTE_TIMEOUT_SECS") {
            if let Ok(secs) = val.parse::<u64>() {
                config.vote_timeout = Duration::from_secs(secs);
            }
        }

        if let Ok(val) = std::env::var("CALM_MIN_CLUSTER_SIZE") {
            if let Ok(size) = val.parse::<usize>() {
                config.min_cluster_size = size;
            }
        }

        config
    }

    /// 从 TOML 配置文件加载配置
    pub fn from_toml(toml_content: &str) -> CoreResult<Self> {
        let mut config = Self::default();

        if let Ok(value) = toml_content.parse::<toml::Table>() {
            if let Some(cluster) = value.get("cluster").and_then(|v| v.as_table()) {
                if let Some(val) = cluster.get("cluster_id").and_then(|v| v.as_str()) {
                    config.cluster_id = val.to_string();
                }

                if let Some(val) = cluster.get("node_id").and_then(|v| v.as_str()) {
                    config.node_id = val.to_string();
                }

                if let Some(val) = cluster.get("listen_addr").and_then(|v| v.as_str()) {
                    config.listen_addr = val.to_string();
                }

                if let Some(seeds) = cluster.get("seed_nodes").and_then(|v| v.as_array()) {
                    config.seed_nodes = seeds
                        .iter()
                        .filter_map(|v| v.as_str())
                        .map(|s| s.to_string())
                        .collect();
                }

                if let Some(val) = cluster
                    .get("gossip_interval_ms")
                    .and_then(|v| v.as_integer())
                {
                    config.gossip_interval = Duration::from_millis(val as u64);
                }

                if let Some(val) = cluster
                    .get("failure_timeout_secs")
                    .and_then(|v| v.as_integer())
                {
                    config.failure_timeout = Duration::from_secs(val as u64);
                }

                if let Some(val) = cluster
                    .get("suspect_timeout_secs")
                    .and_then(|v| v.as_integer())
                {
                    config.suspect_timeout = Duration::from_secs(val as u64);
                }

                if let Some(val) = cluster
                    .get("vote_timeout_secs")
                    .and_then(|v| v.as_integer())
                {
                    config.vote_timeout = Duration::from_secs(val as u64);
                }

                if let Some(val) = cluster.get("min_cluster_size").and_then(|v| v.as_integer()) {
                    config.min_cluster_size = val as usize;
                }

                // Enable cluster mode if seed_nodes are configured
                config.enabled = !config.seed_nodes.is_empty();
            }
        }

        Ok(config)
    }

    /// Load configuration from file with environment variable override
    /// Environment variables take precedence over file configuration
    pub fn load(config_path: Option<&str>) -> CoreResult<Self> {
        let mut config = if let Some(path) = config_path {
            if let Ok(content) = std::fs::read_to_string(path) {
                Self::from_toml(&content)?
            } else {
                Self::default()
            }
        } else {
            // Try default path
            if let Ok(content) = std::fs::read_to_string("calm.toml") {
                Self::from_toml(&content)?
            } else {
                Self::default()
            }
        };

        // Apply environment variable overrides
        config.apply_env_overrides();

        Ok(config)
    }

    /// Apply environment variable overrides to existing config
    fn apply_env_overrides(&mut self) {
        if let Ok(val) = std::env::var("CALM_CLUSTER_ENABLED") {
            self.enabled = val.parse().unwrap_or(self.enabled);
        }

        if let Ok(val) = std::env::var("CALM_NODE_ID") {
            self.node_id = val;
        }

        if let Ok(val) = std::env::var("CALM_CLUSTER_ID") {
            self.cluster_id = val;
        }

        if let Ok(val) = std::env::var("CALM_GOSSIP_ADDR") {
            self.listen_addr = val;
        }

        if let Ok(val) = std::env::var("CALM_SEED_NODES") {
            let seeds: Vec<String> = val
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
            if !seeds.is_empty() {
                self.seed_nodes = seeds;
            }
        }

        if let Ok(val) = std::env::var("CALM_GOSSIP_INTERVAL_MS") {
            if let Ok(ms) = val.parse::<u64>() {
                self.gossip_interval = Duration::from_millis(ms);
            }
        }

        if let Ok(val) = std::env::var("CALM_FAILURE_TIMEOUT_SECS") {
            if let Ok(secs) = val.parse::<u64>() {
                self.failure_timeout = Duration::from_secs(secs);
            }
        }

        if let Ok(val) = std::env::var("CALM_SUSPECT_TIMEOUT_SECS") {
            if let Ok(secs) = val.parse::<u64>() {
                self.suspect_timeout = Duration::from_secs(secs);
            }
        }

        if let Ok(val) = std::env::var("CALM_VOTE_TIMEOUT_SECS") {
            if let Ok(secs) = val.parse::<u64>() {
                self.vote_timeout = Duration::from_secs(secs);
            }
        }

        if let Ok(val) = std::env::var("CALM_MIN_CLUSTER_SIZE") {
            if let Ok(size) = val.parse::<usize>() {
                self.min_cluster_size = size;
            }
        }
    }

    /// Parse listen address to SocketAddr
    pub fn parse_listen_addr(&self) -> CoreResult<SocketAddr> {
        self.listen_addr.parse().map_err(|e| {
            crate::utils::error::CoreError::Internal(format!(
                "Invalid listen address '{}': {}",
                self.listen_addr, e
            ))
        })
    }

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

    /// Get our advertise address (real IP + port)
    ///
    /// If listen_addr is 0.0.0.0, tries to determine real IP by connecting to seed nodes.
    /// Returns "hostname:port" format.
    pub fn get_advertise_addr(&self) -> Option<String> {
        let listen_addr = self.parse_listen_addr().ok()?;
        let port = listen_addr.port();

        // If not binding to 0.0.0.0, use listen address as-is
        let ip_str = listen_addr.ip().to_string();
        if !ip_str.starts_with("0.0.0.0") && !ip_str.starts_with("::") {
            return Some(format!("{}:{}", ip_str, port));
        }

        // Try to get real IP by connecting to a seed node
        for seed in &self.seed_nodes {
            if let Some(real_ip) = crate::utils::net::get_real_ip(seed, Duration::from_secs(1)) {
                return Some(format!("{}:{}", real_ip, port));
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
    pub fn is_seed_node(&self) -> bool {
        // If no seed nodes configured, default to being a seed node
        if self.seed_nodes.is_empty() {
            return true;
        }

        // Get our advertise address (real IP + port)
        let my_addr = match self.get_advertise_addr() {
            Some(addr) => addr,
            None => {
                // Fallback: if we can't determine real IP, assume we're a seed node
                log::warn!("[Cluster] Cannot determine real IP, assuming seed node");
                return true;
            }
        };

        // Extract IP from our address
        let my_ip = if let Some(colon_pos) = my_addr.rfind(':') {
            &my_addr[..colon_pos]
        } else {
            &my_addr
        };

        // Check if our IP appears in any seed node address
        let found_self = self.seed_nodes.iter().any(|seed| {
            if let Some(colon_pos) = seed.rfind(':') {
                let seed_ip = &seed[..colon_pos];
                seed_ip == my_ip
            } else {
                seed == my_ip
            }
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
        // Validate listen address
        self.parse_listen_addr()?;

        // Cluster mode requires seed nodes
        if self.seed_nodes.is_empty() {
            return Err(crate::utils::error::CoreError::Internal(
                "seed_nodes cannot be empty in cluster mode. For single-node deployment, do not initialize ClusterManager.".to_string(),
            ));
        }

        // Validate seed nodes
        self.parse_seed_nodes()?;

        // Validate min_cluster_size
        if self.min_cluster_size < 1 {
            return Err(crate::utils::error::CoreError::Internal(
                "min_cluster_size must be at least 1".to_string(),
            ));
        }

        Ok(())
    }
}

/// 从配置文件加载集群配置
pub fn load_cluster_config(config_path: &str) -> CoreResult<ClusterConfig> {
    ClusterConfig::load(Some(config_path))
}

/// Cluster event types for subscribers
#[derive(Debug, Clone)]
pub enum ClusterEvent {
    /// A new node joined the cluster
    NodeJoined(NodeId),
    /// A node is suspected to be failing
    NodeSuspect(NodeId),
    /// A node has been confirmed dead
    NodeDead(NodeId),
    /// A previously dead node has recovered
    NodeRecovered(NodeId),
    /// Partition topology has changed
    TopologyChanged { table: String, partition: String },
}
