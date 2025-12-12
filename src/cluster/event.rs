/// Cluster event types for subscribers
#[derive(Debug, Clone)]
pub enum ClusterEvent {
    /// 中央节点变更
    CenterNodeChanged {
        old_node: Option<String>,
        new_node: String,
    },

    /// A new node joined the cluster
    NodeJoined(String),

    /// A node is suspected to be failing
    NodeSuspect(String),

    /// A node has been confirmed dead
    NodeDead(String),

    /// A previously dead node has recovered
    NodeRecovered(String),

    /// Partition topology has changed
    TopologyChanged { table: String, partition: String },

    /// 集群版本更新（节点数量变化）
    ClusterVersionChanged {
        old_version: usize,
        new_version: usize,
    },
}
