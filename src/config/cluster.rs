use std::{net::SocketAddr, time::Duration};

use serde::{Deserialize, Serialize};

use crate::utils::error::CoreResult;

/// 集群配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterSettings {
    /// 集群 ID（同一集群的节点必须相同）
    #[serde(default = "default_cluster_id")]
    pub cluster_id: String,

    /// Gossip 监听端口 (UDP)
    #[serde(default = "default_gossip_port")]
    pub gossip_port: u16,

    /// 内部 RPC 端口（tarpc 控制面）
    #[serde(default = "default_internal_port")]
    pub internal_port: u16,

    /// 种子节点列表（用于初始加入集群）
    #[serde(default)]
    pub seed_nodes: Vec<String>,

    /// Gossip 间隔（毫秒）
    #[serde(default = "default_gossip_interval_ms")]
    pub gossip_interval_ms: u64,

    /// 故障检测超时（秒）
    #[serde(default = "default_failure_timeout_secs")]
    pub failure_timeout_secs: u64,

    /// 可疑状态超时（秒）
    #[serde(default = "default_suspect_timeout_secs")]
    pub suspect_timeout_secs: u64,

    /// 分布式查询配置
    #[serde(default)]
    pub distributed: DistributedSettings,
}

fn default_cluster_id() -> String {
    "".to_string()
}

fn default_gossip_port() -> u16 {
    7946
}

fn default_internal_port() -> u16 {
    7950
}

fn default_gossip_interval_ms() -> u64 {
    500
}

fn default_failure_timeout_secs() -> u64 {
    10
}

fn default_suspect_timeout_secs() -> u64 {
    10
}

impl Default for ClusterSettings {
    fn default() -> Self {
        Self {
            cluster_id: default_cluster_id(),
            gossip_port: default_gossip_port(),
            internal_port: default_internal_port(),
            seed_nodes: vec![],
            gossip_interval_ms: default_gossip_interval_ms(),
            failure_timeout_secs: default_failure_timeout_secs(),
            suspect_timeout_secs: default_suspect_timeout_secs(),
            distributed: DistributedSettings::default(),
        }
    }
}

impl ClusterSettings {
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

        log::info!(
            "Checking if my: {} is a seed node... seeds:{:?}",
            my_addr,
            self.seed_nodes
        );

        // Check if our IP appears in any seed node address
        let found_self = self.seed_nodes.iter().any(|seed| {
            seed.parse::<SocketAddr>()
                .map(|addr| addr.ip() == my_addr.ip() && addr.port() == my_addr.port())
                .unwrap_or(false)
        });

        found_self
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

/// 分布式查询配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DistributedSettings {
    /// gRPC 服务端口（用于 Arrow Flight，None 表示自动分配）
    #[serde(default)]
    pub grpc_port: Option<u16>,

    /// 连接超时时间（毫秒）
    #[serde(default = "default_connect_timeout_ms")]
    pub connect_timeout_ms: u64,

    /// 请求超时时间（毫秒）
    #[serde(default = "default_request_timeout_ms")]
    pub request_timeout_ms: u64,
}

fn default_connect_timeout_ms() -> u64 {
    5_000
}

fn default_request_timeout_ms() -> u64 {
    30_000
}

impl Default for DistributedSettings {
    fn default() -> Self {
        Self {
            grpc_port: None,
            connect_timeout_ms: default_connect_timeout_ms(),
            request_timeout_ms: default_request_timeout_ms(),
        }
    }
}
