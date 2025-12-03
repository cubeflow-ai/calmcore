//! 集群管理模块
//!
//! 负责：
//! - 节点发现和健康检查（基于 Chitchat）
//! - Partition Owner 映射维护
//! - 元数据同步
//! - 自动故障转移

pub mod gossip;
pub mod node;
pub mod partition_manager;

pub use gossip::ClusterManager;
pub use node::{NodeId, NodeInfo, NodeState};
pub use partition_manager::PartitionManager;

use crate::utils::error::CoreResult;

/// 集群配置
#[derive(Debug, Clone)]
pub struct ClusterConfig {
    /// 是否启用集群模式
    pub enabled: bool,

    /// 本节点 ID（唯一标识）
    pub node_id: String,

    /// 集群 ID（同一集群的节点必须相同）
    pub cluster_id: String,

    /// Gossip 监听地址
    pub listen_addr: String,

    /// 种子节点列表（用于初始加入集群）
    pub seed_nodes: Vec<String>,

    /// 故障检测超时（秒）
    pub failure_timeout_secs: u64,

    /// Gossip 间隔（毫秒）
    pub gossip_interval_ms: u64,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            node_id: format!("node-{}", uuid::Uuid::new_v4().to_string()[..8].to_string()),
            cluster_id: "calm-cluster".to_string(),
            listen_addr: "0.0.0.0:7946".to_string(),
            seed_nodes: vec![],
            failure_timeout_secs: 30,
            gossip_interval_ms: 500,
        }
    }
}

impl ClusterConfig {
    /// 从环境变量加载配置
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
            config.seed_nodes = val.split(',').map(|s| s.trim().to_string()).collect();
        }

        config
    }
}

/// 从配置文件加载集群配置
pub fn load_cluster_config(_config_path: &str) -> CoreResult<ClusterConfig> {
    // TODO: 从 calm.toml 加载配置
    // 暂时从环境变量加载
    Ok(ClusterConfig::from_env())
}
