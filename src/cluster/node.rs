//! 节点信息定义

use serde::{Deserialize, Serialize};
use std::fmt;

/// 节点唯一 ID
pub type NodeId = String;

/// 节点信息
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct NodeInfo {
    /// 节点 ID
    pub id: NodeId,

    /// Gossip 地址
    pub gossip_addr: String,

    /// 节点状态
    pub state: NodeState,

    /// 最后心跳时间（Unix 时间戳）
    pub last_heartbeat: u64,
}

/// 节点状态
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum NodeState {
    /// 活跃
    Alive,

    /// 疑似故障
    Suspect,

    /// 已故障
    Dead,
}

impl fmt::Display for NodeState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NodeState::Alive => write!(f, "Alive"),
            NodeState::Suspect => write!(f, "Suspect"),
            NodeState::Dead => write!(f, "Dead"),
        }
    }
}

impl NodeInfo {
    pub fn new(id: NodeId, gossip_addr: String) -> Self {
        Self {
            id,
            gossip_addr,
            state: NodeState::Alive,
            last_heartbeat: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        }
    }

    /// 是否存活
    pub fn is_alive(&self) -> bool {
        self.state == NodeState::Alive
    }
}
