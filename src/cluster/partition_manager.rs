//! Partition 管理器
//!
//! 负责：
//! - 维护 Partition -> Owner 映射
//! - 维护 Table Topology (write_node, read_nodes)
//! - 自动故障转移
//! - 负载均衡

use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use super::gossip::ClusterManager;
use super::node::NodeId;
use super::voting::{PartitionKey, VotingCoordinator};
use crate::utils::error::{CoreError, CoreResult};

/// Table topology information
///
/// Contains the mapping of all partitions to their write and read nodes
/// for a single table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableTopology {
    /// Name of the table
    pub table_name: String,
    /// Mapping of partition_id -> PartitionTopology
    pub partitions: HashMap<String, PartitionTopology>,
}

impl TableTopology {
    /// Create a new empty TableTopology
    pub fn new(table_name: impl Into<String>) -> Self {
        Self {
            table_name: table_name.into(),
            partitions: HashMap::new(),
        }
    }

    /// Get the number of partitions
    pub fn partition_count(&self) -> usize {
        self.partitions.len()
    }
}

/// Partition topology
///
/// Defines which node is responsible for writes and which nodes
/// can serve reads for a specific partition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionTopology {
    /// Unique identifier for this partition
    pub partition_id: String,
    /// Single node responsible for write operations
    pub write_node: NodeId,
    /// Nodes that can serve read operations (includes write_node)
    pub read_nodes: Vec<NodeId>,
}

impl PartitionTopology {
    pub fn new(partition_id: String, write_node: NodeId) -> Self {
        // Write node is automatically a read node
        Self {
            partition_id,
            write_node: write_node.clone(),
            read_nodes: vec![write_node],
        }
    }
}

/// Summary of all table topologies for monitoring
///
/// # Requirements
/// - Requirements 12.4: Return current table topology
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopologySummary {
    /// Number of tables
    pub table_count: usize,
    /// Total number of partitions across all tables
    pub total_partition_count: usize,
    /// Summary for each table
    pub tables: Vec<TableTopologySummary>,
}

/// Summary of a single table's topology
///
/// # Requirements
/// - Requirements 12.4: Return current table topology
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableTopologySummary {
    /// Name of the table
    pub table_name: String,
    /// Number of partitions in this table
    pub partition_count: usize,
    /// Number of unique nodes serving as write_node
    pub unique_write_node_count: usize,
}

/// Partition Manager
///
/// Manages table topology and partition ownership.
///
/// # Requirements
/// - Requirements 4.1: Create TableTopology with write_node assignments
/// - Requirements 4.2: Fast lookup from memory
/// - Requirements 4.3: Propagate topology changes via Gossip
/// - Requirements 4.4: Update local topology cache from Gossip
pub struct PartitionManager {
    /// Reference to ClusterManager for node information and Gossip
    cluster: Arc<ClusterManager>,
    /// Reference to VotingCoordinator for failover voting
    voting: Arc<VotingCoordinator>,
    /// Table topologies (in memory)
    /// Key: table_name, Value: TableTopology
    topologies: Arc<RwLock<HashMap<String, TableTopology>>>,
}

impl PartitionManager {
    /// Create a new PartitionManager
    ///
    /// # Arguments
    /// * `cluster` - Reference to ClusterManager for node information and Gossip
    /// * `voting` - Reference to VotingCoordinator for failover voting
    ///
    /// # Requirements
    /// - Requirements 4.1: Store topologies HashMap (in memory)
    pub fn new(cluster: Arc<ClusterManager>, voting: Arc<VotingCoordinator>) -> Self {
        Self {
            cluster,
            voting,
            topologies: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Create a PartitionManager without VotingCoordinator (for backward compatibility)
    ///
    /// This creates a VotingCoordinator internally with default timeout.
    pub fn new_standalone(cluster: Arc<ClusterManager>) -> Self {
        let voting = Arc::new(VotingCoordinator::new(cluster.config().vote_timeout));
        Self {
            cluster,
            voting,
            topologies: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Get reference to the VotingCoordinator
    pub fn voting(&self) -> &Arc<VotingCoordinator> {
        &self.voting
    }

    /// Get reference to the ClusterManager
    pub fn cluster(&self) -> &Arc<ClusterManager> {
        &self.cluster
    }

    /// Get partition topology (fast, from memory)
    ///
    /// Returns the write_node and read_nodes for a specific partition.
    /// This is a fast lookup from the in-memory topology cache.
    ///
    /// # Arguments
    /// * `table` - Name of the table
    /// * `partition` - Partition ID
    ///
    /// # Returns
    /// The PartitionTopology if found, None otherwise
    ///
    /// # Requirements
    /// - Requirements 4.2: Return write_node and read_nodes within 1ms from memory
    pub async fn get_topology(&self, table: &str, partition: &str) -> Option<PartitionTopology> {
        let topologies = self.topologies.read().await;
        topologies
            .get(table)
            .and_then(|t| t.partitions.get(partition))
            .cloned()
    }

    /// Get write node for a partition
    ///
    /// Fast lookup from memory for the single node responsible for writes.
    ///
    /// # Arguments
    /// * `table` - Name of the table
    /// * `partition` - Partition ID
    ///
    /// # Returns
    /// The write_node NodeId if found, None otherwise
    ///
    /// # Requirements
    /// - Requirements 4.2: Fast lookup from memory
    pub async fn get_write_node(&self, table: &str, partition: &str) -> Option<NodeId> {
        self.get_topology(table, partition)
            .await
            .map(|t| t.write_node)
    }

    /// Get read nodes for a partition
    ///
    /// Fast lookup from memory for all nodes that can serve reads.
    /// This list always includes the write_node.
    ///
    /// # Arguments
    /// * `table` - Name of the table
    /// * `partition` - Partition ID
    ///
    /// # Returns
    /// The list of read_nodes if found, None otherwise
    ///
    /// # Requirements
    /// - Requirements 4.2: Fast lookup from memory
    pub async fn get_read_nodes(&self, table: &str, partition: &str) -> Option<Vec<NodeId>> {
        self.get_topology(table, partition)
            .await
            .map(|t| t.read_nodes)
    }

    /// Get table topology
    ///
    /// Returns the complete topology for a table including all partitions.
    ///
    /// # Arguments
    /// * `table` - Name of the table
    ///
    /// # Returns
    /// The TableTopology if found, None otherwise
    pub async fn get_table_topology(&self, table: &str) -> Option<TableTopology> {
        let topologies = self.topologies.read().await;
        topologies.get(table).cloned()
    }

    /// Get all table topologies
    ///
    /// Returns a map of all table names to their topologies.
    ///
    /// # Requirements
    /// - Requirements 12.4: Return current table topology
    pub async fn get_all_topologies(&self) -> HashMap<String, TableTopology> {
        let topologies = self.topologies.read().await;
        topologies.clone()
    }

    /// Get topology summary for monitoring
    ///
    /// Returns a summary of all topologies including partition counts per table.
    ///
    /// # Requirements
    /// - Requirements 12.4: Return current table topology
    pub async fn get_topology_summary(&self) -> TopologySummary {
        let topologies = self.topologies.read().await;

        let tables: Vec<TableTopologySummary> = topologies
            .values()
            .map(|topo| {
                let partition_count = topo.partitions.len();
                let write_nodes: std::collections::HashSet<_> = topo
                    .partitions
                    .values()
                    .map(|p| p.write_node.clone())
                    .collect();

                TableTopologySummary {
                    table_name: topo.table_name.clone(),
                    partition_count,
                    unique_write_node_count: write_nodes.len(),
                }
            })
            .collect();

        let total_partitions: usize = tables.iter().map(|t| t.partition_count).sum();

        TopologySummary {
            table_count: tables.len(),
            total_partition_count: total_partitions,
            tables,
        }
    }

    /// Add a read node for a partition
    ///
    /// Adds a node to the read_nodes list for a partition, allowing it to serve
    /// read queries for that partition.
    ///
    /// # Arguments
    /// * `table` - Name of the table
    /// * `partition` - Partition ID
    /// * `node` - NodeId to add as a read node
    ///
    /// # Requirements
    /// - Requirements 6.2: Add node to read_nodes list when requested
    pub async fn add_read_node(
        &self,
        table: &str,
        partition: &str,
        node: &NodeId,
    ) -> CoreResult<()> {
        let updated_topology = {
            let mut topologies = self.topologies.write().await;
            if let Some(table_topo) = topologies.get_mut(table) {
                if let Some(part_topo) = table_topo.partitions.get_mut(partition) {
                    if !part_topo.read_nodes.contains(node) {
                        part_topo.read_nodes.push(node.clone());
                        log::info!(
                            "➕ [Partition] Added read node {} for {}:{}",
                            node,
                            table,
                            partition
                        );
                        Some(part_topo.clone())
                    } else {
                        None
                    }
                } else {
                    None
                }
            } else {
                None
            }
        };

        // Broadcast topology change via Gossip (Requirements 4.3)
        if let Some(topo) = updated_topology {
            self.broadcast_partition_topology(table, partition, &topo)
                .await;
        }

        Ok(())
    }

    /// Remove a read node for a partition
    ///
    /// Removes a node from the read_nodes list for a partition.
    /// This is typically called when a node fails.
    ///
    /// # Arguments
    /// * `table` - Name of the table
    /// * `partition` - Partition ID
    /// * `node` - NodeId to remove from read nodes
    ///
    /// # Requirements
    /// - Requirements 6.3: Remove failed node from read_nodes list
    pub async fn remove_read_node(
        &self,
        table: &str,
        partition: &str,
        node: &NodeId,
    ) -> CoreResult<()> {
        let updated_topology = {
            let mut topologies = self.topologies.write().await;
            if let Some(table_topo) = topologies.get_mut(table) {
                if let Some(part_topo) = table_topo.partitions.get_mut(partition) {
                    let original_len = part_topo.read_nodes.len();
                    part_topo.read_nodes.retain(|n| n != node);
                    if part_topo.read_nodes.len() < original_len {
                        log::info!(
                            "➖ [Partition] Removed read node {} from {}:{}",
                            node,
                            table,
                            partition
                        );
                        Some(part_topo.clone())
                    } else {
                        None
                    }
                } else {
                    None
                }
            } else {
                None
            }
        };

        // Broadcast topology change via Gossip (Requirements 4.3)
        if let Some(topo) = updated_topology {
            self.broadcast_partition_topology(table, partition, &topo)
                .await;
        }

        Ok(())
    }

    /// Broadcast a single partition's topology via Gossip
    ///
    /// # Requirements
    /// - Requirements 4.3: Propagate topology changes via Gossip
    async fn broadcast_partition_topology(
        &self,
        table: &str,
        partition: &str,
        topology: &PartitionTopology,
    ) {
        let key = PartitionKey::new(table, partition);
        if let Ok(json) = serde_json::to_string(topology) {
            self.cluster.gossip_set(&key.topology_key(), &json).await;
        }
    }

    /// Set write node for a partition (used during failover)
    ///
    /// Updates the write_node for a partition and ensures the new write_node
    /// is also in the read_nodes list.
    ///
    /// # Requirements
    /// - Requirements 7.6: Update topology when node wins vote
    pub async fn set_write_node(
        &self,
        table: &str,
        partition: &str,
        node: &NodeId,
    ) -> CoreResult<()> {
        let updated_topology = {
            let mut topologies = self.topologies.write().await;
            if let Some(table_topo) = topologies.get_mut(table) {
                if let Some(part_topo) = table_topo.partitions.get_mut(partition) {
                    part_topo.write_node = node.clone();
                    // Ensure write node is also a read node (Requirements 6.1)
                    if !part_topo.read_nodes.contains(node) {
                        part_topo.read_nodes.push(node.clone());
                    }
                    log::info!(
                        "🔄 [Partition] Set write node {} for {}:{}",
                        node,
                        table,
                        partition
                    );
                    Some(part_topo.clone())
                } else {
                    None
                }
            } else {
                None
            }
        };

        // Broadcast topology change via Gossip (Requirements 4.3, 7.6)
        if let Some(topo) = updated_topology {
            self.broadcast_partition_topology(table, partition, &topo)
                .await;
        }

        Ok(())
    }

    /// Handle node failure - initiate voting for orphaned partitions
    ///
    /// This method is called when a node is detected as failed. It:
    /// 1. Gets orphaned partitions (where failed node was write_node)
    /// 2. Removes failed node from all read_nodes
    /// 3. Calculates proposed owners using deterministic hash
    /// 4. Starts voting rounds for each orphaned partition
    ///
    /// # Arguments
    /// * `failed_node` - NodeId of the failed node
    ///
    /// # Requirements
    /// - Requirements 7.1: Initiate voting round with epoch=1 for orphaned partitions
    /// - Requirements 7.2: Calculate proposed write_node using deterministic hash
    /// - Requirements 12.2, 12.3: Structured logging for failover operations
    pub async fn on_node_failure(&self, failed_node: &NodeId) -> CoreResult<()> {
        // Requirements 12.2: Structured logging for failover operations
        log::warn!(
            target: "cluster::failover",
            "⚠️  [Partition] Failover initiated: failed_node={}, operation=on_node_failure",
            failed_node
        );

        // Get alive nodes (excluding failed node)
        let mut alive_nodes = self.cluster.live_nodes().await;
        alive_nodes.retain(|n| &n.id != failed_node);
        alive_nodes.sort_by(|a, b| a.id.cmp(&b.id)); // Sort for deterministic hash

        if alive_nodes.is_empty() {
            log::warn!(
                target: "cluster::failover",
                "⚠️  [Partition] Failover aborted: failed_node={}, reason=no_alive_nodes",
                failed_node
            );
            return Ok(());
        }

        // Check minimum cluster size
        if alive_nodes.len() < self.cluster.config().min_cluster_size {
            log::warn!(
                target: "cluster::failover",
                "⚠️  [Partition] Failover aborted: failed_node={}, reason=cluster_size_below_minimum, cluster_size={}, min_required={}",
                failed_node,
                alive_nodes.len(),
                self.cluster.config().min_cluster_size
            );
            return Ok(());
        }

        // Find orphaned partitions and remove failed node from read_nodes
        let orphaned_partitions = self.get_orphaned_partitions_and_cleanup(failed_node).await;

        if orphaned_partitions.is_empty() {
            log::info!(
                target: "cluster::failover",
                "📋 [Partition] Failover complete: failed_node={}, orphaned_partitions=0, reason=no_write_partitions",
                failed_node
            );
            return Ok(());
        }

        // Requirements 12.3: Log failover operation with partition count
        log::info!(
            target: "cluster::failover",
            "🗳️  [Partition] Failover voting started: failed_node={}, orphaned_partition_count={}, alive_node_count={}",
            failed_node,
            orphaned_partitions.len(),
            alive_nodes.len()
        );

        let my_node_id = self.cluster.my_node_id().clone();

        // Start voting rounds for each orphaned partition
        for partition_key in orphaned_partitions {
            // Calculate proposed owner using deterministic hash (Requirements 7.2)
            let proposed_owner = self.calculate_proposed_owner(&partition_key, &alive_nodes);

            // Start voting round with epoch=1 (Requirements 7.1)
            let epoch = self
                .voting
                .start_round(&partition_key, &proposed_owner)
                .await;

            // Cast my vote
            self.voting
                .cast_vote(&partition_key, &proposed_owner, epoch, &my_node_id)
                .await;

            // Broadcast vote via Gossip
            self.broadcast_vote(&partition_key, &proposed_owner, epoch, &my_node_id)
                .await;

            log::info!(
                "🗳️  [Partition] Started vote for {} -> {} (epoch={})",
                partition_key,
                proposed_owner,
                epoch
            );
        }

        Ok(())
    }

    /// Handle a node leaving the cluster gracefully
    ///
    /// This method is called when a node broadcasts a leaving notification.
    /// It initiates voting for the leaving node's partitions, similar to
    /// on_node_failure but without the failure detection delay.
    ///
    /// # Arguments
    /// * `leaving_node` - NodeId of the node that is leaving
    ///
    /// # Requirements
    /// - Requirements 8.2: Initiate voting for leaving node's partitions
    /// - Requirements 8.3: Remove node from membership list after partitions reassigned
    pub async fn on_node_leaving(&self, leaving_node: &NodeId) -> CoreResult<()> {
        log::info!(
            "👋 [Partition] Handling graceful departure of node '{}'",
            leaving_node
        );

        // Reuse the same logic as node failure - initiate voting for orphaned partitions
        self.on_node_failure(leaving_node).await
    }

    /// Get all partitions where the specified node is the write_node
    ///
    /// Returns a list of PartitionKeys for partitions owned by the node.
    pub async fn get_write_partitions_of(&self, node_id: &NodeId) -> Vec<PartitionKey> {
        let topologies = self.topologies.read().await;
        let mut partitions = Vec::new();

        for (table_name, table_topo) in topologies.iter() {
            for (partition_id, part_topo) in table_topo.partitions.iter() {
                if &part_topo.write_node == node_id {
                    partitions.push(PartitionKey::new(table_name, partition_id));
                }
            }
        }

        partitions
    }

    /// Check if a node still owns any partitions as write_node
    ///
    /// Returns true if the node has no write partitions (all transferred).
    ///
    /// # Requirements
    /// - Requirements 8.4: Check if all partitions have been reassigned
    pub async fn has_no_write_partitions(&self, node_id: &NodeId) -> bool {
        self.get_write_partitions_of(node_id).await.is_empty()
    }

    /// Get orphaned partitions (where failed node was write_node) and remove failed node from read_nodes
    ///
    /// Returns a list of PartitionKeys for partitions that need new write_nodes.
    async fn get_orphaned_partitions_and_cleanup(&self, failed_node: &NodeId) -> Vec<PartitionKey> {
        let mut orphaned = Vec::new();

        let mut topologies = self.topologies.write().await;
        for (table_name, table_topo) in topologies.iter_mut() {
            for (partition_id, part_topo) in table_topo.partitions.iter_mut() {
                // Check if this partition's write_node is the failed node
                if &part_topo.write_node == failed_node {
                    orphaned.push(PartitionKey::new(table_name, partition_id));
                }

                // Remove failed node from read_nodes (Requirements 6.3)
                let original_len = part_topo.read_nodes.len();
                part_topo.read_nodes.retain(|n| n != failed_node);
                if part_topo.read_nodes.len() < original_len {
                    log::debug!(
                        "  Removed {} from read_nodes of {}:{}",
                        failed_node,
                        table_name,
                        partition_id
                    );
                }
            }
        }

        orphaned
    }

    /// Calculate proposed owner using deterministic hash
    ///
    /// Uses a hash of the partition key to deterministically select a node.
    /// All nodes will calculate the same proposed owner for the same partition.
    ///
    /// # Requirements
    /// - Requirements 7.2: Deterministic hash of partition ID
    fn calculate_proposed_owner(
        &self,
        partition: &PartitionKey,
        alive_nodes: &[crate::cluster::NodeInfo],
    ) -> NodeId {
        let mut hasher = DefaultHasher::new();
        partition.table_name.hash(&mut hasher);
        partition.partition_id.hash(&mut hasher);
        let hash = hasher.finish();

        let idx = (hash as usize) % alive_nodes.len();
        alive_nodes[idx].id.clone()
    }

    /// Broadcast a vote via Gossip
    ///
    /// # Requirements
    /// - Requirements 7.3: Propagate vote via Gossip KV
    async fn broadcast_vote(
        &self,
        partition: &PartitionKey,
        proposed_owner: &NodeId,
        epoch: u64,
        voter: &NodeId,
    ) {
        use super::voting::VoteRecord;

        let vote = VoteRecord::new(voter.clone(), epoch, proposed_owner.clone());
        let key = partition.vote_key(epoch, voter);

        if let Some(json) = super::voting::VotingCoordinator::serialize_vote(&vote) {
            self.cluster.gossip_set(&key, &json).await;
        }
    }

    /// Process vote results periodically
    ///
    /// This method should be called periodically to:
    /// 1. Check quorum for active voting rounds
    /// 2. Update topology if this node won the vote
    /// 3. Broadcast topology change via Gossip
    /// 4. Handle timeouts and split votes
    ///
    /// # Requirements
    /// - Requirements 7.5: Confirm ownership when quorum reached
    /// - Requirements 7.6: Update topology and broadcast via Gossip when won
    /// - Requirements 7.7: Handle vote timeout
    /// - Requirements 7.8: Handle split votes
    pub async fn process_vote_results(&self) {
        let node_count = self.cluster.node_count().await;
        let quorum = self.cluster.quorum_threshold(node_count);
        let my_id = self.cluster.my_node_id().clone();

        // Get all active rounds
        let active_rounds = self.voting.active_rounds();
        let rounds_snapshot: Vec<PartitionKey> = {
            let rounds = active_rounds.read().await;
            rounds.keys().cloned().collect()
        };

        // Check each active round for quorum
        for partition in rounds_snapshot {
            // Check if quorum is reached (Requirements 7.5)
            if let Some(winner) = self.voting.check_quorum(&partition, quorum).await {
                // Check if I won the vote
                if winner == my_id {
                    // Requirements 12.3: Log failover completion with partition info
                    log::info!(
                        target: "cluster::failover",
                        "🏆 [Partition] Failover complete: table={}, partition={}, new_write_node={}, quorum={}, operation=vote_won",
                        partition.table_name,
                        partition.partition_id,
                        my_id,
                        quorum
                    );

                    // Update topology (Requirements 7.6)
                    if let Err(e) = self
                        .set_write_node(&partition.table_name, &partition.partition_id, &my_id)
                        .await
                    {
                        log::error!(
                            target: "cluster::failover",
                            "❌ [Partition] Failover error: table={}, partition={}, error={}, operation=set_write_node",
                            partition.table_name,
                            partition.partition_id,
                            e
                        );
                    }

                    // Clear voting round
                    self.voting.clear_round(&partition).await;
                }
            }
        }

        // Check for timeouts (Requirements 7.7)
        let timed_out = self.voting.check_timeouts().await;
        for partition in timed_out {
            log::warn!("⏰ [Partition] Vote timeout for {}", partition);

            // Handle timeout - increment epoch and restart
            if let Some((new_epoch, proposed_owner)) = self.voting.handle_timeout(&partition).await
            {
                // Re-cast vote with new epoch
                self.voting
                    .cast_vote(&partition, &proposed_owner, new_epoch, &my_id)
                    .await;
                self.broadcast_vote(&partition, &proposed_owner, new_epoch, &my_id)
                    .await;

                log::info!(
                    "🔄 [Partition] Restarted vote for {} at epoch={}",
                    partition,
                    new_epoch
                );
            }
        }

        // Check for split votes (Requirements 7.8)
        let active_rounds = self.voting.active_rounds();
        let rounds_for_split_check: Vec<PartitionKey> = {
            let rounds = active_rounds.read().await;
            rounds.keys().cloned().collect()
        };

        for partition in rounds_for_split_check {
            if self
                .voting
                .is_split_vote(&partition, quorum, node_count)
                .await
            {
                log::warn!("🔀 [Partition] Split vote detected for {}", partition);

                // Handle split vote - increment epoch and restart
                if let Some((new_epoch, proposed_owner)) =
                    self.voting.handle_split_vote(&partition).await
                {
                    // Re-cast vote with new epoch
                    self.voting
                        .cast_vote(&partition, &proposed_owner, new_epoch, &my_id)
                        .await;
                    self.broadcast_vote(&partition, &proposed_owner, new_epoch, &my_id)
                        .await;

                    log::info!(
                        "🔄 [Partition] Restarted vote for {} at epoch={} after split",
                        partition,
                        new_epoch
                    );
                }
            }
        }
    }

    /// Receive and process votes from Gossip
    ///
    /// This method should be called when vote updates are received via Gossip.
    /// It parses the vote and adds it to the appropriate voting round.
    ///
    /// # Arguments
    /// * `key` - The Gossip key (format: vote:{table}:{partition}:{epoch}:{voter})
    /// * `value` - The JSON-serialized VoteRecord
    pub async fn receive_vote_from_gossip(&self, key: &str, value: &str) {
        // Parse key: vote:{table}:{partition}:{epoch}:{voter}
        let parts: Vec<&str> = key.split(':').collect();
        if parts.len() != 5 || parts[0] != "vote" {
            return;
        }

        let table = parts[1];
        let partition_id = parts[2];
        let partition = PartitionKey::new(table, partition_id);

        // Deserialize vote
        if let Some(vote) = super::voting::VotingCoordinator::deserialize_vote(value) {
            // Add vote to the round
            self.voting.receive_vote(&partition, vote).await;
        }
    }

    /// Receive topology updates from Gossip
    ///
    /// This method should be called when topology updates are received via Gossip.
    /// It parses the topology and updates the local cache.
    ///
    /// # Arguments
    /// * `key` - The Gossip key (format: topology:{table}:{partition})
    /// * `value` - The JSON-serialized PartitionTopology
    ///
    /// # Requirements
    /// - Requirements 4.4: Update local topology cache from Gossip
    pub async fn receive_topology_from_gossip(&self, key: &str, value: &str) {
        // Parse key: topology:{table}:{partition}
        let parts: Vec<&str> = key.split(':').collect();
        if parts.len() != 3 || parts[0] != "topology" {
            return;
        }

        let table = parts[1];
        let partition_id = parts[2];

        // Deserialize topology
        if let Ok(topology) = serde_json::from_str::<PartitionTopology>(value) {
            self.update_partition_topology(table, partition_id, topology)
                .await;
        }
    }

    /// Update a single partition's topology in the local cache
    ///
    /// # Requirements
    /// - Requirements 4.4: Update local topology cache immediately
    async fn update_partition_topology(
        &self,
        table: &str,
        partition_id: &str,
        topology: PartitionTopology,
    ) {
        let mut topologies = self.topologies.write().await;

        // Get or create table topology
        let table_topo = topologies
            .entry(table.to_string())
            .or_insert_with(|| TableTopology::new(table));

        // Update partition topology
        let old_write_node = table_topo
            .partitions
            .get(partition_id)
            .map(|p| p.write_node.clone());

        table_topo
            .partitions
            .insert(partition_id.to_string(), topology.clone());

        // Log if write_node changed
        if let Some(old) = old_write_node {
            if old != topology.write_node {
                log::info!(
                    "🔄 [Partition] Topology update from Gossip: {}:{} write_node {} -> {}",
                    table,
                    partition_id,
                    old,
                    topology.write_node
                );
            }
        } else {
            log::debug!(
                "📥 [Partition] Received topology from Gossip: {}:{} -> {}",
                table,
                partition_id,
                topology.write_node
            );
        }
    }

    /// Sync all topology from Gossip
    ///
    /// Scans all Gossip keys for topology entries and updates the local cache.
    /// This is useful when a node joins an existing cluster.
    ///
    /// # Requirements
    /// - Requirements 4.4: Update local topology cache from Gossip
    /// - Requirements 9.4: Receive complete topology from peers via Gossip
    pub async fn sync_topology_from_gossip(&self) {
        let all_keys = self.cluster.get_all_keys().await;

        let mut topology_count = 0;
        for (key, value) in all_keys.iter() {
            if key.starts_with("topology:") {
                self.receive_topology_from_gossip(key, value).await;
                topology_count += 1;
            }
        }

        if topology_count > 0 {
            log::info!(
                "📥 [Partition] Synced {} partition topologies from Gossip",
                topology_count
            );
        }
    }

    /// Sync all votes from Gossip
    ///
    /// Scans all Gossip keys for vote entries and processes them.
    /// This is useful when a node joins during an active voting round.
    pub async fn sync_votes_from_gossip(&self) {
        let all_keys = self.cluster.get_all_keys().await;

        let mut vote_count = 0;
        for (key, value) in all_keys.iter() {
            if key.starts_with("vote:") {
                self.receive_vote_from_gossip(key, value).await;
                vote_count += 1;
            }
        }

        if vote_count > 0 {
            log::info!("📥 [Partition] Synced {} votes from Gossip", vote_count);
        }
    }

    /// Start background task to periodically process votes and sync from Gossip
    ///
    /// This spawns a background task that:
    /// 1. Processes vote results
    /// 2. Syncs topology and votes from Gossip
    pub fn start_background_sync(self: &Arc<Self>) {
        let pm = Arc::clone(self);
        let interval = pm.cluster.config().gossip_interval;

        tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;

                // Process vote results
                pm.process_vote_results().await;

                // Sync from Gossip (in case we missed updates)
                pm.sync_votes_from_gossip().await;
            }
        });

        log::info!("✅ [Partition] Started background sync task");
    }

    /// Start listening for cluster events and handle them
    ///
    /// This spawns a background task that listens for cluster events
    /// (NodeDead, NodeRecovered, etc.) and handles them appropriately.
    ///
    /// # Requirements
    /// - Requirements 8.2: Handle leaving notification by initiating voting
    pub fn start_event_listener(self: &Arc<Self>) {
        let pm = Arc::clone(self);
        let mut receiver = pm.cluster.subscribe();

        tokio::spawn(async move {
            loop {
                match receiver.recv().await {
                    Ok(event) => {
                        match event {
                            super::ClusterEvent::NodeDead(node_id) => {
                                // Handle node failure or graceful departure
                                log::info!(
                                    "📥 [Partition] Received NodeDead event for {}",
                                    node_id
                                );
                                if let Err(e) = pm.on_node_failure(&node_id).await {
                                    log::error!(
                                        "Failed to handle node failure for {}: {}",
                                        node_id,
                                        e
                                    );
                                }
                            }
                            super::ClusterEvent::NodeRecovered(node_id) => {
                                // Cancel pending votes for recovered node's partitions
                                log::info!(
                                    "📥 [Partition] Received NodeRecovered event for {}",
                                    node_id
                                );
                                pm.on_node_recovered(&node_id).await;
                            }
                            super::ClusterEvent::TopologyChanged { table, partition } => {
                                log::debug!(
                                    "📥 [Partition] Topology changed: {}:{}",
                                    table,
                                    partition
                                );
                            }
                            _ => {
                                // Ignore other events
                            }
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        log::warn!("📥 [Partition] Event listener lagged by {} events", n);
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        log::info!("📥 [Partition] Event channel closed, stopping listener");
                        break;
                    }
                }
            }
        });

        log::info!("✅ [Partition] Started event listener");
    }

    /// Handle node recovery - cancel pending votes for recovered node's partitions
    ///
    /// When a node recovers, we should cancel any pending voting rounds for
    /// partitions that were owned by that node, allowing it to reclaim ownership.
    /// The recovered node can then reclaim its partitions without going through
    /// the voting process.
    ///
    /// # Arguments
    /// * `recovered_node` - NodeId of the node that has recovered
    ///
    /// # Requirements
    /// - Requirements 3.5: Cancel pending votes for recovered node's partitions
    ///                     Let recovered node reclaim ownership
    pub async fn on_node_recovered(&self, recovered_node: &NodeId) {
        log::info!(
            "🟢 [Partition] Handling recovery of node '{}'",
            recovered_node
        );

        // Get all active voting rounds
        let active_rounds = self.voting.active_rounds();
        let rounds_snapshot: Vec<PartitionKey> = {
            let rounds = active_rounds.read().await;
            rounds.keys().cloned().collect()
        };

        let mut cancelled_count = 0;

        // Cancel voting rounds where:
        // 1. The recovered node was the proposed owner (it can reclaim its partitions)
        // 2. The voting was initiated due to this node's failure
        for partition in rounds_snapshot {
            let should_cancel = {
                let rounds = active_rounds.read().await;
                if let Some(round) = rounds.get(&partition) {
                    // If the recovered node was the proposed owner, cancel the round
                    // This allows the node to reclaim its partitions
                    round.proposed_owner == *recovered_node
                } else {
                    false
                }
            };

            if should_cancel {
                log::info!(
                    "🔄 [Partition] Cancelling vote for {} - node {} recovered and can reclaim ownership",
                    partition,
                    recovered_node
                );
                self.voting.clear_round(&partition).await;
                cancelled_count += 1;

                // Restore the recovered node as the write_node for this partition
                // This allows the node to reclaim ownership without voting
                if let Err(e) = self
                    .set_write_node(
                        &partition.table_name,
                        &partition.partition_id,
                        recovered_node,
                    )
                    .await
                {
                    log::error!(
                        "Failed to restore write_node for {} to {}: {}",
                        partition,
                        recovered_node,
                        e
                    );
                } else {
                    log::info!(
                        "✅ [Partition] Restored {} as write_node for {}",
                        recovered_node,
                        partition
                    );
                }
            }
        }

        if cancelled_count > 0 {
            log::info!(
                "✅ [Partition] Cancelled {} voting rounds for recovered node '{}'",
                cancelled_count,
                recovered_node
            );
        } else {
            log::debug!(
                "📋 [Partition] No active voting rounds to cancel for recovered node '{}'",
                recovered_node
            );
        }
    }

    /// Perform graceful shutdown with partition transfer
    ///
    /// This method orchestrates the complete graceful shutdown process:
    /// 1. Broadcasts leaving notification via ClusterManager
    /// 2. Waits for all partitions to be transferred to other nodes
    /// 3. Completes the shutdown
    ///
    /// # Arguments
    /// * `timeout` - Maximum time to wait for partition transfer
    ///
    /// # Requirements
    /// - Requirements 8.1: Broadcast leaving notification
    /// - Requirements 8.2: Initiate voting for leaving node's partitions
    /// - Requirements 8.3: Remove node from membership after partitions reassigned
    /// - Requirements 8.4: Block until all partitions reassigned or timeout
    ///
    /// # Returns
    /// Ok(true) if all partitions were transferred before timeout
    /// Ok(false) if timeout expired before all partitions were transferred
    /// - Requirements 12.3: Structured logging for failover operations
    pub async fn graceful_shutdown(&self, timeout: std::time::Duration) -> CoreResult<bool> {
        let my_node_id = self.cluster.my_node_id().clone();

        // Requirements 12.3: Structured logging for graceful shutdown
        log::info!(
            target: "cluster::failover",
            "👋 [Partition] Graceful shutdown initiated: node_id={}, timeout_secs={}, operation=graceful_shutdown",
            my_node_id,
            timeout.as_secs()
        );

        // Step 1: Get current partition count for logging
        let initial_partitions = self.get_write_partitions_of(&my_node_id).await;
        log::info!(
            target: "cluster::failover",
            "📊 [Partition] Graceful shutdown: node_id={}, partition_count={}, operation=partition_transfer_start",
            my_node_id,
            initial_partitions.len()
        );

        // Step 2: Broadcast leaving notification
        // This will trigger NodeDead event which initiates voting
        self.cluster.shutdown().await?;

        // If we have no partitions, we're done
        if initial_partitions.is_empty() {
            log::info!("✅ [Partition] No partitions to transfer, shutdown complete");
            self.cluster.complete_shutdown().await?;
            return Ok(true);
        }

        // Step 3: Wait for partition transfer
        log::info!(
            "⏳ [Partition] Waiting for {} partitions to be transferred (timeout: {:?})",
            initial_partitions.len(),
            timeout
        );

        let start = std::time::Instant::now();
        let check_interval = std::time::Duration::from_millis(500);

        while start.elapsed() < timeout {
            // Process any pending vote results
            self.process_vote_results().await;

            // Check if all partitions have been transferred
            let remaining = self.get_write_partitions_of(&my_node_id).await;

            if remaining.is_empty() {
                // Requirements 12.3: Log successful partition transfer
                log::info!(
                    target: "cluster::failover",
                    "✅ [Partition] Graceful shutdown complete: node_id={}, transferred_partition_count={}, elapsed_ms={}, operation=partition_transfer_complete",
                    my_node_id,
                    initial_partitions.len(),
                    start.elapsed().as_millis()
                );
                self.cluster.complete_shutdown().await?;
                return Ok(true);
            }

            log::debug!(
                "⏳ [Partition] {} partitions remaining, waiting...",
                remaining.len()
            );

            // Wait before checking again
            tokio::time::sleep(check_interval).await;
        }

        // Timeout expired
        let remaining = self.get_write_partitions_of(&my_node_id).await;
        log::warn!(
            "⚠️  [Partition] Shutdown timeout after {:?}, {} partitions not transferred",
            timeout,
            remaining.len()
        );

        // Complete shutdown anyway
        self.cluster.complete_shutdown().await?;
        Ok(false)
    }

    /// Get the count of partitions owned by a node
    pub async fn get_partition_count(&self, node_id: &NodeId) -> usize {
        self.get_write_partitions_of(node_id).await.len()
    }

    /// Initialize topology for a new table
    ///
    /// Creates a TableTopology with round-robin write_node assignment across alive nodes.
    /// Each partition's write_node is automatically added to its read_nodes list.
    ///
    /// # Arguments
    /// * `table` - Name of the table
    /// * `partitions` - List of partition IDs to create
    ///
    /// # Requirements
    /// - Requirements 4.1: Create TableTopology with write_node assignments
    /// - Requirements 5.1: Round-robin assign write_node to partitions
    /// - Requirements 6.1: Auto-add write_node to read_nodes
    pub async fn initialize_table(&self, table: &str, partitions: Vec<String>) -> CoreResult<()> {
        let nodes = self.cluster.live_nodes().await;
        if nodes.is_empty() {
            return Err(CoreError::Internal(
                "No live nodes available for partition assignment".to_string(),
            ));
        }

        log::info!(
            "🎯 [Partition] Initializing {} partitions for table '{}' across {} nodes",
            partitions.len(),
            table,
            nodes.len()
        );

        let mut partition_map = HashMap::new();

        // Round-robin assignment (Requirements 5.1)
        // Property 4: Each node gets floor(N/M) or ceil(N/M) partitions
        for (idx, partition_id) in partitions.iter().enumerate() {
            let owner_idx = idx % nodes.len();
            let owner_node = &nodes[owner_idx];

            // PartitionTopology::new automatically adds write_node to read_nodes (Requirements 6.1)
            let topology = PartitionTopology::new(partition_id.clone(), owner_node.id.clone());

            partition_map.insert(partition_id.clone(), topology);

            log::debug!(
                "  Partition {} -> Node {} (write + read)",
                partition_id,
                owner_node.id
            );
        }

        let table_topology = TableTopology {
            table_name: table.to_string(),
            partitions: partition_map,
        };

        // Store in local cache
        {
            let mut topologies = self.topologies.write().await;
            topologies.insert(table.to_string(), table_topology.clone());
        }

        // Broadcast topology via Gossip (Requirements 4.3)
        self.broadcast_table_topology(table, &table_topology).await;

        log::info!(
            "✅ [Partition] Initialized {} partitions for table '{}' across {} nodes",
            partitions.len(),
            table,
            nodes.len()
        );

        Ok(())
    }

    /// Broadcast table topology via Gossip
    ///
    /// # Requirements
    /// - Requirements 4.3: Propagate topology changes via Gossip
    async fn broadcast_table_topology(&self, table: &str, topology: &TableTopology) {
        // Serialize and broadcast each partition's topology
        for (partition_id, part_topo) in &topology.partitions {
            let key = PartitionKey::new(table, partition_id);
            if let Ok(json) = serde_json::to_string(part_topo) {
                self.cluster.gossip_set(&key.topology_key(), &json).await;
            }
        }
    }

    /// Create a new on-demand partition and assign to lowest-load node
    ///
    /// This method is used for on-demand partition types (Range, DatetimeRange, Custom)
    /// where partitions are created dynamically as data arrives. The partition is
    /// assigned to the node with the lowest current load to maintain balance.
    ///
    /// # Arguments
    /// * `table` - Name of the table
    /// * `partition_id` - ID of the new partition to create
    ///
    /// # Returns
    /// The NodeId of the node assigned as write_node for the new partition
    ///
    /// # Requirements
    /// - Requirements 5.2: Assign write_node to the node with the lowest load
    /// - Requirements 6.1: Auto-add write_node to read_nodes
    /// - Requirements 4.3: Propagate topology changes via Gossip
    pub async fn create_on_demand_partition(
        &self,
        table: &str,
        partition_id: &str,
    ) -> CoreResult<NodeId> {
        let nodes = self.cluster.live_nodes().await;
        if nodes.is_empty() {
            return Err(CoreError::Internal(
                "No live nodes available for partition assignment".to_string(),
            ));
        }

        // Find the node with the lowest load (Requirements 5.2)
        // Property 5: Lowest-Load Assignment
        let lowest_load_node = self.find_lowest_load_node(&nodes);

        log::info!(
            "🎯 [Partition] Creating on-demand partition '{}:{}' -> Node {} (load: {:.2})",
            table,
            partition_id,
            lowest_load_node.id,
            lowest_load_node.load
        );

        // Create partition topology with write_node as read_node (Requirements 6.1)
        let topology =
            PartitionTopology::new(partition_id.to_string(), lowest_load_node.id.clone());

        // Store in local cache
        {
            let mut topologies = self.topologies.write().await;
            let table_topo = topologies
                .entry(table.to_string())
                .or_insert_with(|| TableTopology::new(table));
            table_topo
                .partitions
                .insert(partition_id.to_string(), topology.clone());
        }

        // Broadcast topology via Gossip (Requirements 4.3)
        self.broadcast_partition_topology(table, partition_id, &topology)
            .await;

        log::info!(
            "✅ [Partition] Created on-demand partition '{}:{}' assigned to node '{}'",
            table,
            partition_id,
            lowest_load_node.id
        );

        Ok(lowest_load_node.id.clone())
    }

    /// Find the node with the lowest load among alive nodes
    ///
    /// This is used for on-demand partition assignment to maintain load balance.
    /// If multiple nodes have the same load, the first one (by node ID order) is chosen
    /// for deterministic behavior.
    ///
    /// # Arguments
    /// * `nodes` - List of alive nodes to choose from
    ///
    /// # Returns
    /// Reference to the node with the lowest load
    ///
    /// # Requirements
    /// - Requirements 5.2: Assign to node with lowest load
    fn find_lowest_load_node<'a>(
        &self,
        nodes: &'a [crate::cluster::NodeInfo],
    ) -> &'a crate::cluster::NodeInfo {
        // Find node with lowest load
        // In case of tie, prefer node with fewer partitions
        // In case of another tie, prefer node with lower ID (deterministic)
        nodes
            .iter()
            .min_by(|a, b| {
                // Primary: compare by load
                match a.load.partial_cmp(&b.load) {
                    Some(std::cmp::Ordering::Equal) | None => {
                        // Secondary: compare by partition count
                        match a.partition_count.cmp(&b.partition_count) {
                            std::cmp::Ordering::Equal => {
                                // Tertiary: compare by node ID for determinism
                                a.id.cmp(&b.id)
                            }
                            other => other,
                        }
                    }
                    Some(other) => other,
                }
            })
            .expect("nodes list should not be empty")
    }

    /// Initialize topology for a new table with partitions (legacy API)
    pub async fn initialize_table_topology(
        &self,
        table_name: &str,
        partition_ids: Vec<String>,
    ) -> CoreResult<()> {
        self.initialize_table(table_name, partition_ids).await
    }

    /// 初始化表的分区所有权
    ///
    /// # 参数
    /// - `table_name`: 表名
    /// - `num_partitions`: 分区数量
    pub async fn initialize_partitions(
        &self,
        table_name: &str,
        num_partitions: u32,
    ) -> CoreResult<()> {
        log::info!(
            "🎯 [Partition] Initializing {} partitions for table '{}'",
            num_partitions,
            table_name
        );

        let nodes = self.cluster.live_nodes().await;
        if nodes.is_empty() {
            return Err(CoreError::Internal(
                "No live nodes available for partition assignment".to_string(),
            ));
        }

        // 轮询分配分区给节点
        for partition_id in 0..num_partitions {
            let owner_idx = (partition_id as usize) % nodes.len();
            let owner_node = &nodes[owner_idx];

            // 存储到 Gossip 共享状态
            let key = self.partition_key(table_name, partition_id);
            self.cluster
                .set_key_value(key, owner_node.id.clone())
                .await?;

            log::debug!("  Partition {} -> Node {}", partition_id, owner_node.id);
        }

        log::info!(
            "✅ [Partition] Initialized {} partitions across {} nodes",
            num_partitions,
            nodes.len()
        );

        Ok(())
    }

    /// 获取分区的 Owner 节点
    pub async fn get_partition_owner(
        &self,
        table_name: &str,
        partition_id: u32,
    ) -> CoreResult<NodeId> {
        let key = self.partition_key(table_name, partition_id);
        self.cluster.get_key_value(&key).await.ok_or_else(|| {
            CoreError::Internal(format!(
                "Partition owner not found: {} (partition {})",
                table_name, partition_id
            ))
        })
    }

    /// 设置分区的 Owner 节点（用于故障转移）
    pub async fn set_partition_owner(
        &self,
        table_name: &str,
        partition_id: u32,
        owner_node: &str,
    ) -> CoreResult<()> {
        let key = self.partition_key(table_name, partition_id);
        self.cluster
            .set_key_value(key, owner_node.to_string())
            .await?;

        log::info!(
            "🔄 [Partition] Reassigned partition {} of '{}' to node '{}'",
            partition_id,
            table_name,
            owner_node
        );

        Ok(())
    }

    /// 获取某个节点负责的所有分区
    pub async fn get_node_partitions(&self, node_id: &str) -> HashMap<String, Vec<u32>> {
        let all_keys = self.cluster.get_all_keys().await;
        let mut result: HashMap<String, Vec<u32>> = HashMap::new();

        for (key, owner) in all_keys.iter() {
            if owner == node_id && key.starts_with("partition_owner:") {
                // 解析 key: partition_owner:table_name:partition_id
                let parts: Vec<&str> = key.split(':').collect();
                if parts.len() == 3 {
                    let table_name = parts[1].to_string();
                    if let Ok(partition_id) = parts[2].parse::<u32>() {
                        result
                            .entry(table_name)
                            .or_insert_with(Vec::new)
                            .push(partition_id);
                    }
                }
            }
        }

        result
    }

    /// 处理节点故障，自动转移分区
    ///
    /// 协调机制：
    /// 1. 使用确定性算法选出一个 Coordinator 节点
    /// 2. 只有 Coordinator 执行实际的分区转移
    /// 3. 其他节点跳过，避免冲突
    pub async fn handle_node_failure(&self, failed_node: &str) -> CoreResult<()> {
        log::warn!("⚠️  [Partition] Detected failure of node '{}'", failed_node);

        // 1. 选举 Coordinator（确定性选择：按字典序最小的活跃节点）
        let coordinator = self.elect_coordinator_for_failover(failed_node).await?;
        let my_node_id = self.cluster.node_id();

        if coordinator != my_node_id {
            log::info!(
                "📋 [Partition] Node '{}' is coordinator, I'll skip (I'm '{}')",
                coordinator,
                my_node_id
            );
            return Ok(());
        }

        log::info!("👑 [Partition] I am the coordinator, handling failover...");

        // 2. 找出该节点负责的所有分区
        let partitions = self.get_node_partitions(failed_node).await;
        if partitions.is_empty() {
            log::info!("  Node '{}' had no partitions", failed_node);
            return Ok(());
        }

        // 3. 获取活跃节点（排除故障节点）
        let alive_nodes: Vec<_> = self
            .cluster
            .live_nodes()
            .await
            .into_iter()
            .filter(|n| n.id != failed_node)
            .collect();

        if alive_nodes.is_empty() {
            return Err(CoreError::Internal(
                "No alive nodes for partition reassignment".to_string(),
            ));
        }

        // 4. 使用一致性哈希分配分区（避免热点）
        let mut reassigned_count = 0;
        for (table_name, partition_ids) in partitions {
            for partition_id in partition_ids {
                // 使用哈希选择新 Owner（确定性 + 负载均衡）
                let new_owner = self
                    .select_new_owner(&table_name, partition_id, &alive_nodes)
                    .await;

                // CAS 操作：只有当前值还是 failed_node 时才更新
                let success = self
                    .cas_partition_owner(&table_name, partition_id, failed_node, &new_owner.id)
                    .await?;

                if success {
                    log::info!(
                        "  ✅ Partition {}:{} → {}",
                        table_name,
                        partition_id,
                        new_owner.id
                    );
                    reassigned_count += 1;
                } else {
                    log::warn!(
                        "  ⚠️  Partition {}:{} already reassigned (race condition avoided)",
                        table_name,
                        partition_id
                    );
                }
            }
        }

        log::info!(
            "✅ [Partition] Coordinator '{}' reassigned {} partitions from '{}' to {} alive nodes",
            my_node_id,
            reassigned_count,
            failed_node,
            alive_nodes.len()
        );

        Ok(())
    }

    /// 启动自动故障转移监听
    pub async fn start_auto_failover(&self) {
        let partition_manager = Arc::new(PartitionManager {
            cluster: self.cluster.clone(),
            voting: self.voting.clone(),
            topologies: self.topologies.clone(),
        });

        self.cluster
            .watch_failures(move |failed_node| {
                let pm = partition_manager.clone();
                tokio::spawn(async move {
                    if let Err(e) = pm.handle_node_failure(&failed_node).await {
                        log::error!("Failed to handle node failure: {}", e);
                    }
                });
            })
            .await;

        log::info!("✅ [Partition] Auto-failover enabled");
    }

    /// 生成分区键
    fn partition_key(&self, table_name: &str, partition_id: u32) -> String {
        format!("partition_owner:{}:{}", table_name, partition_id)
    }

    /// 选举 Coordinator 节点（用于故障转移）
    ///
    /// 策略：确定性选择 - 对所有活跃节点 + 故障节点 ID 排序，选第一个
    /// 这样所有节点都会选出同一个 Coordinator，避免冲突
    async fn elect_coordinator_for_failover(&self, failed_node: &str) -> CoreResult<String> {
        let mut alive_nodes = self.cluster.live_nodes().await;

        if alive_nodes.is_empty() {
            return Err(CoreError::Internal("No alive nodes".to_string()));
        }

        // 排序（字典序）
        alive_nodes.sort_by(|a, b| a.id.cmp(&b.id));

        // 使用故障节点 ID 作为种子，选择一个确定的 Coordinator
        // 这样即使不同时刻检测到故障，也会选出同一个节点
        let hash = self.hash_string(&format!("failover:{}", failed_node));
        let idx = (hash as usize) % alive_nodes.len();

        Ok(alive_nodes[idx].id.clone())
    }

    /// 为分区选择新的 Owner（负载均衡）
    async fn select_new_owner(
        &self,
        table_name: &str,
        partition_id: u32,
        alive_nodes: &[crate::cluster::NodeInfo],
    ) -> crate::cluster::NodeInfo {
        // 使用一致性哈希：table + partition 作为 key
        let key = format!("{}:{}", table_name, partition_id);
        let hash = self.hash_string(&key);
        let idx = (hash as usize) % alive_nodes.len();

        alive_nodes[idx].clone()
    }

    /// 简单的字符串哈希函数
    fn hash_string(&self, s: &str) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        s.hash(&mut hasher);
        hasher.finish()
    }

    /// CAS (Compare-And-Swap) 更新分区 Owner
    ///
    /// 只有当前值等于 expected 时才更新为 new_value
    /// 返回是否成功更新
    async fn cas_partition_owner(
        &self,
        table_name: &str,
        partition_id: u32,
        expected_owner: &str,
        new_owner: &str,
    ) -> CoreResult<bool> {
        let key = self.partition_key(table_name, partition_id);

        // 获取当前值
        let current = self.cluster.get_key_value(&key).await;

        // CAS 检查
        if let Some(current_owner) = current {
            if current_owner == expected_owner {
                // 值匹配，执行更新
                self.cluster
                    .set_key_value(key, new_owner.to_string())
                    .await?;
                return Ok(true);
            } else {
                // 值已被其他节点修改
                return Ok(false);
            }
        }

        // 键不存在，直接设置
        self.cluster
            .set_key_value(key, new_owner.to_string())
            .await?;
        Ok(true)
    }

    /// 重新平衡分区（手动触发）
    pub async fn rebalance_partitions(&self, table_name: &str) -> CoreResult<()> {
        log::info!("🔄 [Partition] Rebalancing partitions for '{}'", table_name);

        // 获取当前所有分区
        let all_keys = self.cluster.get_all_keys().await;
        let mut partitions = Vec::new();

        for key in all_keys.keys() {
            if key.starts_with(&format!("partition_owner:{}:", table_name)) {
                let parts: Vec<&str> = key.split(':').collect();
                if parts.len() == 3 {
                    if let Ok(partition_id) = parts[2].parse::<u32>() {
                        partitions.push(partition_id);
                    }
                }
            }
        }

        if partitions.is_empty() {
            return Ok(());
        }

        // 获取活跃节点
        let nodes = self.cluster.live_nodes().await;
        if nodes.is_empty() {
            return Err(CoreError::Internal("No live nodes".to_string()));
        }

        // 重新分配
        for (idx, partition_id) in partitions.iter().enumerate() {
            let owner_idx = idx % nodes.len();
            let owner = &nodes[owner_idx];
            self.set_partition_owner(table_name, *partition_id, &owner.id)
                .await?;
        }

        log::info!(
            "✅ [Partition] Rebalanced {} partitions across {} nodes",
            partitions.len(),
            nodes.len()
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::{ClusterConfig, ClusterManager};
    use std::sync::Arc;
    use std::time::Duration;

    async fn create_test_cluster_manager(node_id: &str, port: u16) -> Arc<ClusterManager> {
        let config = ClusterConfig {
            node_id: node_id.to_string(),
            cluster_id: "test-cluster".to_string(),
            listen_addr: format!("127.0.0.1:{}", port),
            seed_nodes: vec![],
            ..Default::default()
        };
        Arc::new(ClusterManager::new(config).await.unwrap())
    }

    // ========================================================================
    // Task 11: Graceful Shutdown Tests
    // ========================================================================

    #[tokio::test]
    async fn test_get_write_partitions_of() {
        // Test that we can get all partitions owned by a node
        let cluster = create_test_cluster_manager("test-node", 18001).await;
        let pm = PartitionManager::new_standalone(cluster.clone());

        // Initialize a table with partitions
        let partitions = vec![
            "partition_0".to_string(),
            "partition_1".to_string(),
            "partition_2".to_string(),
        ];
        pm.initialize_table("test_table", partitions).await.unwrap();

        // Get partitions owned by test-node (should be all of them in standalone mode)
        let owned = pm.get_write_partitions_of(&"test-node".to_string()).await;
        assert_eq!(owned.len(), 3);
    }

    #[tokio::test]
    async fn test_has_no_write_partitions_empty() {
        // Test that has_no_write_partitions returns true when node has no partitions
        let cluster = create_test_cluster_manager("test-node", 18002).await;
        let pm = PartitionManager::new_standalone(cluster.clone());

        // Node should have no partitions initially
        let has_none = pm.has_no_write_partitions(&"other-node".to_string()).await;
        assert!(has_none);
    }

    #[tokio::test]
    async fn test_has_no_write_partitions_with_partitions() {
        // Test that has_no_write_partitions returns false when node has partitions
        let cluster = create_test_cluster_manager("test-node", 18003).await;
        let pm = PartitionManager::new_standalone(cluster.clone());

        // Initialize a table with partitions
        let partitions = vec!["partition_0".to_string()];
        pm.initialize_table("test_table", partitions).await.unwrap();

        // Node should have partitions
        let has_none = pm.has_no_write_partitions(&"test-node".to_string()).await;
        assert!(!has_none);
    }

    #[tokio::test]
    async fn test_on_node_leaving() {
        // Test that on_node_leaving initiates voting for leaving node's partitions
        let cluster = create_test_cluster_manager("test-node", 18004).await;
        let pm = PartitionManager::new_standalone(cluster.clone());

        // Initialize a table with partitions
        let partitions = vec!["partition_0".to_string()];
        pm.initialize_table("test_table", partitions).await.unwrap();

        // Simulate node leaving (should not error even if no other nodes)
        let result = pm.on_node_leaving(&"test-node".to_string()).await;
        // In standalone mode with only one node, this will log a warning but not error
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_get_partition_count() {
        // Test that get_partition_count returns correct count
        let cluster = create_test_cluster_manager("test-node", 18005).await;
        let pm = PartitionManager::new_standalone(cluster.clone());

        // Initially no partitions
        let count = pm.get_partition_count(&"test-node".to_string()).await;
        assert_eq!(count, 0);

        // Initialize a table with partitions
        let partitions = vec!["partition_0".to_string(), "partition_1".to_string()];
        pm.initialize_table("test_table", partitions).await.unwrap();

        // Should have 2 partitions
        let count = pm.get_partition_count(&"test-node".to_string()).await;
        assert_eq!(count, 2);
    }

    #[tokio::test]
    async fn test_graceful_shutdown_no_partitions() {
        // Test graceful shutdown when node has no partitions
        let cluster = create_test_cluster_manager("test-node", 18006).await;
        let pm = PartitionManager::new_standalone(cluster.clone());

        // Graceful shutdown with no partitions should succeed immediately
        let result = pm.graceful_shutdown(Duration::from_secs(1)).await;
        assert!(result.is_ok());
        assert!(result.unwrap()); // Should return true (all partitions transferred)
    }

    #[tokio::test]
    async fn test_on_node_recovered_cancels_votes() {
        // Test that on_node_recovered cancels pending votes
        let cluster = create_test_cluster_manager("test-node", 18007).await;
        let pm = PartitionManager::new_standalone(cluster.clone());

        // Start a voting round
        let partition = PartitionKey::new("test_table", "partition_0");
        let proposed_owner = "recovered-node".to_string();
        pm.voting.start_round(&partition, &proposed_owner).await;

        // Verify voting round exists
        assert_eq!(pm.voting.active_rounds_count().await, 1);

        // Simulate node recovery
        pm.on_node_recovered(&"recovered-node".to_string()).await;

        // Voting round should be cancelled
        assert_eq!(pm.voting.active_rounds_count().await, 0);
    }

    #[tokio::test]
    async fn test_cluster_manager_is_node_leaving() {
        // Test that is_node_leaving works correctly
        let cluster = create_test_cluster_manager("test-node", 18008).await;

        // Initially not leaving
        let is_leaving = cluster.is_node_leaving("test-node").await;
        assert!(!is_leaving);

        // After shutdown, should be leaving
        cluster.shutdown().await.unwrap();
        let is_leaving = cluster.is_node_leaving("test-node").await;
        assert!(is_leaving);
    }

    // ========================================================================
    // Task 12.3: On-Demand Partition Creation Tests
    // ========================================================================

    #[tokio::test]
    async fn test_create_on_demand_partition() {
        // Test Requirements 5.2: On-demand partition assigned to lowest-load node
        let cluster = create_test_cluster_manager("test-node", 18009).await;
        let pm = PartitionManager::new_standalone(cluster.clone());

        // Create an on-demand partition
        let result = pm
            .create_on_demand_partition("test_table", "partition_0")
            .await;
        assert!(result.is_ok());

        // Verify partition was created
        let topology = pm.get_topology("test_table", "partition_0").await;
        assert!(topology.is_some());

        let topo = topology.unwrap();
        assert_eq!(topo.partition_id, "partition_0");
        assert_eq!(topo.write_node, "test-node");
        // Write node should be in read_nodes (Requirements 6.1)
        assert!(topo.read_nodes.contains(&"test-node".to_string()));
    }

    #[tokio::test]
    async fn test_create_multiple_on_demand_partitions() {
        // Test creating multiple on-demand partitions
        let cluster = create_test_cluster_manager("test-node", 18010).await;
        let pm = PartitionManager::new_standalone(cluster.clone());

        // Create multiple partitions
        for i in 0..5 {
            let partition_id = format!("partition_{}", i);
            let result = pm
                .create_on_demand_partition("test_table", &partition_id)
                .await;
            assert!(result.is_ok());
        }

        // Verify all partitions were created
        let table_topo = pm.get_table_topology("test_table").await;
        assert!(table_topo.is_some());
        assert_eq!(table_topo.unwrap().partitions.len(), 5);
    }

    #[tokio::test]
    async fn test_find_lowest_load_node() {
        // Test that find_lowest_load_node selects the node with lowest load
        let cluster = create_test_cluster_manager("test-node", 18011).await;
        let pm = PartitionManager::new_standalone(cluster.clone());

        // Create test nodes with different loads
        let nodes = vec![
            crate::cluster::NodeInfo {
                id: "node-1".to_string(),
                gossip_addr: "127.0.0.1:7946".to_string(),
                state: crate::cluster::NodeState::Alive,
                partition_count: 5,
                load: 0.8,
                memory_usage_bytes: 1000,
                last_heartbeat: 0,
            },
            crate::cluster::NodeInfo {
                id: "node-2".to_string(),
                gossip_addr: "127.0.0.1:7947".to_string(),
                state: crate::cluster::NodeState::Alive,
                partition_count: 3,
                load: 0.2, // Lowest load
                memory_usage_bytes: 500,
                last_heartbeat: 0,
            },
            crate::cluster::NodeInfo {
                id: "node-3".to_string(),
                gossip_addr: "127.0.0.1:7948".to_string(),
                state: crate::cluster::NodeState::Alive,
                partition_count: 4,
                load: 0.5,
                memory_usage_bytes: 800,
                last_heartbeat: 0,
            },
        ];

        let lowest = pm.find_lowest_load_node(&nodes);
        assert_eq!(lowest.id, "node-2"); // Should select node with load 0.2
    }

    #[tokio::test]
    async fn test_find_lowest_load_node_tie_breaker() {
        // Test that find_lowest_load_node uses partition_count as tie-breaker
        let cluster = create_test_cluster_manager("test-node", 18012).await;
        let pm = PartitionManager::new_standalone(cluster.clone());

        // Create test nodes with same load but different partition counts
        let nodes = vec![
            crate::cluster::NodeInfo {
                id: "node-1".to_string(),
                gossip_addr: "127.0.0.1:7946".to_string(),
                state: crate::cluster::NodeState::Alive,
                partition_count: 5, // More partitions
                load: 0.5,
                memory_usage_bytes: 1000,
                last_heartbeat: 0,
            },
            crate::cluster::NodeInfo {
                id: "node-2".to_string(),
                gossip_addr: "127.0.0.1:7947".to_string(),
                state: crate::cluster::NodeState::Alive,
                partition_count: 2, // Fewer partitions - should be selected
                load: 0.5,
                memory_usage_bytes: 500,
                last_heartbeat: 0,
            },
        ];

        let lowest = pm.find_lowest_load_node(&nodes);
        assert_eq!(lowest.id, "node-2"); // Should select node with fewer partitions
    }

    #[tokio::test]
    async fn test_on_node_recovered_restores_ownership() {
        // Test Requirements 3.5: Recovered node reclaims ownership
        let cluster = create_test_cluster_manager("test-node", 18013).await;
        let pm = PartitionManager::new_standalone(cluster.clone());

        // Initialize a table with a partition
        let partitions = vec!["partition_0".to_string()];
        pm.initialize_table("test_table", partitions).await.unwrap();

        // Start a voting round for the partition (simulating failure)
        let partition = PartitionKey::new("test_table", "partition_0");
        let proposed_owner = "test-node".to_string();
        pm.voting.start_round(&partition, &proposed_owner).await;

        // Verify voting round exists
        assert_eq!(pm.voting.active_rounds_count().await, 1);

        // Simulate node recovery
        pm.on_node_recovered(&"test-node".to_string()).await;

        // Voting round should be cancelled
        assert_eq!(pm.voting.active_rounds_count().await, 0);

        // Verify the recovered node is still the write_node
        let write_node = pm.get_write_node("test_table", "partition_0").await;
        assert_eq!(write_node, Some("test-node".to_string()));
    }
}
