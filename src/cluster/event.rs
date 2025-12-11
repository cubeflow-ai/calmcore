/// Cluster event types for subscribers
#[derive(Debug, Clone)]
pub enum ClusterEvent {
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
}
