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

    pub const LOCAL_MODEL_NODE_NAME: &str = "local";

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
