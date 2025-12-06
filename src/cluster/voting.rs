//! Voting Coordinator for partition ownership transfer
//!
//! Manages the voting process with epoch-based rounds for safe partition
//! ownership transfer during node failures.
//!
//! ## Gossip Key-Value Schema for Votes
//!
//! Votes are propagated via Gossip KV with the key pattern:
//! `vote:{table}:{partition}:{epoch}:{voter}` -> JSON VoteRecord
//!
//! This allows all nodes to see votes cast by other nodes and reach consensus.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use super::node::NodeId;

/// A key identifying a specific partition
///
/// Used as a HashMap key for tracking voting rounds and partition topology.
/// Implements Hash and Eq for use in HashMaps.
#[derive(Debug, Clone, Hash, Eq, PartialEq, Serialize, Deserialize)]
pub struct PartitionKey {
    /// Name of the table this partition belongs to
    pub table_name: String,
    /// Unique identifier for the partition within the table
    pub partition_id: String,
}

impl PartitionKey {
    /// Create a new PartitionKey
    pub fn new(table_name: impl Into<String>, partition_id: impl Into<String>) -> Self {
        Self {
            table_name: table_name.into(),
            partition_id: partition_id.into(),
        }
    }

    /// Create a Gossip key for this partition's topology
    pub fn topology_key(&self) -> String {
        format!("topology:{}:{}", self.table_name, self.partition_id)
    }

    /// Create a Gossip key prefix for votes on this partition
    pub fn vote_key_prefix(&self) -> String {
        format!("vote:{}:{}", self.table_name, self.partition_id)
    }

    /// Create a Gossip key for a specific vote
    pub fn vote_key(&self, epoch: u64, voter: &str) -> String {
        format!(
            "vote:{}:{}:{}:{}",
            self.table_name, self.partition_id, epoch, voter
        )
    }
}

impl std::fmt::Display for PartitionKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.table_name, self.partition_id)
    }
}

/// Individual vote record
///
/// Represents a single vote cast by a node for a partition ownership proposal.
/// The epoch field ensures votes from different rounds are not mixed.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct VoteRecord {
    /// Node that cast this vote
    pub voter: NodeId,
    /// Voting round epoch (votes from different epochs are ignored)
    pub epoch: u64,
    /// The node being proposed as the new write_node
    pub proposed_owner: NodeId,
    /// Unix timestamp when the vote was cast
    pub voted_at: u64,
}

impl VoteRecord {
    /// Create a new vote record
    pub fn new(voter: NodeId, epoch: u64, proposed_owner: NodeId) -> Self {
        Self {
            voter,
            epoch,
            proposed_owner,
            voted_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        }
    }
}

/// A voting round for a partition
///
/// Manages the voting process for transferring ownership of a partition.
/// Each round has an epoch number that increments on timeout or split vote.
#[derive(Debug)]
pub struct VoteRound {
    /// The partition being voted on
    pub partition: PartitionKey,
    /// Current voting round number (increments on timeout/split)
    pub epoch: u64,
    /// The node proposed to become the new write_node
    pub proposed_owner: NodeId,
    /// Collected votes: voter_id -> VoteRecord
    pub votes: HashMap<NodeId, VoteRecord>,
    /// When this round started (for timeout detection)
    pub started_at: Instant,
}

impl VoteRound {
    /// Create a new voting round with epoch=1
    pub fn new(partition: PartitionKey, proposed_owner: NodeId) -> Self {
        Self {
            partition,
            epoch: 1,
            proposed_owner,
            votes: HashMap::new(),
            started_at: Instant::now(),
        }
    }

    /// Create a new voting round with a specific epoch
    pub fn with_epoch(partition: PartitionKey, proposed_owner: NodeId, epoch: u64) -> Self {
        Self {
            partition,
            epoch,
            proposed_owner,
            votes: HashMap::new(),
            started_at: Instant::now(),
        }
    }

    /// Get the count of valid votes (matching current epoch)
    pub fn valid_vote_count(&self) -> usize {
        self.votes
            .values()
            .filter(|v| v.epoch == self.epoch)
            .count()
    }

    /// Check if a specific voter has voted in the current epoch
    pub fn has_voted(&self, voter: &NodeId) -> bool {
        self.votes
            .get(voter)
            .map(|v| v.epoch == self.epoch)
            .unwrap_or(false)
    }
}

/// Voting Coordinator manages the voting process with epoch-based rounds
///
/// # Requirements
/// - Requirements 7.1: Initiate voting rounds for orphaned partitions
/// - Requirements 7.3: Include epoch in vote records
/// - Requirements 7.4: Only count votes with matching epoch
/// - Requirements 7.5: Confirm ownership when quorum reached
/// - Requirements 7.7: Handle vote timeout
/// - Requirements 7.8: Handle split votes
/// - Requirements 7.9: Clear old epoch votes
pub struct VotingCoordinator {
    /// Active voting rounds: partition -> VoteRound
    active_rounds: Arc<RwLock<HashMap<PartitionKey, VoteRound>>>,

    /// Vote timeout duration
    vote_timeout: Duration,
}

impl VotingCoordinator {
    /// Create a new VotingCoordinator
    ///
    /// # Arguments
    /// * `vote_timeout` - Duration before a voting round times out
    ///
    /// # Requirements
    /// - Requirements 7.1: Configure vote_timeout
    pub fn new(vote_timeout: Duration) -> Self {
        Self {
            active_rounds: Arc::new(RwLock::new(HashMap::new())),
            vote_timeout,
        }
    }

    /// Get the vote timeout duration
    pub fn vote_timeout(&self) -> Duration {
        self.vote_timeout
    }

    /// Get a reference to active rounds (for testing/monitoring)
    pub fn active_rounds(&self) -> Arc<RwLock<HashMap<PartitionKey, VoteRound>>> {
        self.active_rounds.clone()
    }

    /// Start a new voting round for a partition
    ///
    /// Creates a VoteRound with epoch=1 for new partitions, or increments
    /// the epoch from an existing round. Clears any votes from previous epochs.
    ///
    /// # Arguments
    /// * `partition` - The partition to start voting for
    /// * `proposed_owner` - The node proposed to become the new write_node
    ///
    /// # Returns
    /// The epoch number for this round
    ///
    /// # Requirements
    /// - Requirements 7.1: Create VoteRound with epoch=1 (or increment existing)
    /// - Requirements 7.9: Clear old epoch votes when new round starts
    pub async fn start_round(&self, partition: &PartitionKey, proposed_owner: &NodeId) -> u64 {
        let mut rounds = self.active_rounds.write().await;

        let epoch = if let Some(existing) = rounds.get(partition) {
            // Increment epoch from existing round
            existing.epoch + 1
        } else {
            // First round starts at epoch=1
            1
        };

        // Create new round with cleared votes (Requirements 7.9)
        let round = VoteRound::with_epoch(partition.clone(), proposed_owner.clone(), epoch);

        log::info!(
            "🗳️  [Voting] Starting round epoch={} for {} -> {}",
            epoch,
            partition,
            proposed_owner
        );

        rounds.insert(partition.clone(), round);
        epoch
    }

    /// Cast a vote for a partition ownership proposal
    ///
    /// Creates a VoteRecord with the current epoch and stores it in the active round.
    /// The vote is only recorded if there's an active round for the partition.
    ///
    /// # Arguments
    /// * `partition` - The partition being voted on
    /// * `proposed_owner` - The node being proposed as the new write_node
    /// * `epoch` - The voting round epoch
    /// * `voter` - The node casting the vote
    ///
    /// # Returns
    /// The VoteRecord that was created (for Gossip propagation)
    ///
    /// # Requirements
    /// - Requirements 7.3: Create VoteRecord with epoch
    pub async fn cast_vote(
        &self,
        partition: &PartitionKey,
        proposed_owner: &NodeId,
        epoch: u64,
        voter: &NodeId,
    ) -> Option<VoteRecord> {
        let vote = VoteRecord::new(voter.clone(), epoch, proposed_owner.clone());

        let mut rounds = self.active_rounds.write().await;
        if let Some(round) = rounds.get_mut(partition) {
            // Only accept votes for the current epoch
            if epoch == round.epoch {
                round.votes.insert(voter.clone(), vote.clone());
                log::debug!(
                    "🗳️  [Voting] Vote cast: {} votes for {} epoch={}",
                    voter,
                    partition,
                    epoch
                );
                return Some(vote);
            } else {
                log::debug!(
                    "🗳️  [Voting] Ignoring vote from {} for {} - epoch mismatch (got {}, expected {})",
                    voter,
                    partition,
                    epoch,
                    round.epoch
                );
            }
        }
        None
    }

    /// Serialize a VoteRecord to JSON for Gossip propagation
    ///
    /// # Requirements
    /// - Requirements 7.3: Propagate via Gossip KV
    pub fn serialize_vote(vote: &VoteRecord) -> Option<String> {
        serde_json::to_string(vote).ok()
    }

    /// Deserialize a VoteRecord from JSON (received from Gossip)
    ///
    /// # Requirements
    /// - Requirements 7.3: Parse incoming votes from Gossip
    pub fn deserialize_vote(json: &str) -> Option<VoteRecord> {
        serde_json::from_str(json).ok()
    }

    /// Receive a vote from Gossip
    ///
    /// Parses incoming votes from Gossip and adds them to the VoteRound.
    /// Only votes with matching epoch are accepted.
    ///
    /// # Arguments
    /// * `partition` - The partition the vote is for
    /// * `vote` - The VoteRecord received from Gossip
    ///
    /// # Returns
    /// true if the vote was accepted, false otherwise
    ///
    /// # Requirements
    /// - Requirements 7.3: Parse incoming votes from Gossip
    /// - Requirements 7.4: Only count votes with matching epoch
    pub async fn receive_vote(&self, partition: &PartitionKey, vote: VoteRecord) -> bool {
        let mut rounds = self.active_rounds.write().await;
        if let Some(round) = rounds.get_mut(partition) {
            // Requirements 7.4: Only count votes with matching epoch
            if vote.epoch == round.epoch {
                log::debug!(
                    "🗳️  [Voting] Received vote from {} for {} epoch={}",
                    vote.voter,
                    partition,
                    vote.epoch
                );
                round.votes.insert(vote.voter.clone(), vote);
                return true;
            } else {
                log::debug!(
                    "🗳️  [Voting] Ignoring vote from {} - epoch mismatch (got {}, expected {})",
                    vote.voter,
                    vote.epoch,
                    round.epoch
                );
            }
        }
        false
    }

    /// Check if current round has reached quorum
    ///
    /// Filters votes by current epoch and checks if any proposed owner
    /// has received votes from at least quorum_threshold nodes.
    ///
    /// # Arguments
    /// * `partition` - The partition to check
    /// * `quorum_threshold` - Minimum votes needed (typically N/2 + 1)
    ///
    /// # Returns
    /// The winning node if quorum is reached, None otherwise
    ///
    /// # Requirements
    /// - Requirements 7.4: Filter votes by current epoch
    /// - Requirements 7.5: Check if count >= quorum threshold
    pub async fn check_quorum(
        &self,
        partition: &PartitionKey,
        quorum_threshold: usize,
    ) -> Option<NodeId> {
        let rounds = self.active_rounds.read().await;
        if let Some(round) = rounds.get(partition) {
            // Requirements 7.4: Only count votes with matching epoch
            let valid_votes: Vec<_> = round
                .votes
                .values()
                .filter(|v| v.epoch == round.epoch)
                .collect();

            log::debug!(
                "🗳️  [Voting] Checking quorum for {}: {} valid votes, need {}",
                partition,
                valid_votes.len(),
                quorum_threshold
            );

            if valid_votes.len() >= quorum_threshold {
                // Count votes for each proposed owner
                let mut vote_counts: HashMap<&NodeId, usize> = HashMap::new();
                for vote in &valid_votes {
                    *vote_counts.entry(&vote.proposed_owner).or_insert(0) += 1;
                }

                // Requirements 7.5: Find winner with quorum
                for (owner, count) in vote_counts {
                    if count >= quorum_threshold {
                        log::info!(
                            "🗳️  [Voting] Quorum reached for {}: {} wins with {} votes",
                            partition,
                            owner,
                            count
                        );
                        return Some(owner.clone());
                    }
                }
            }
        }
        None
    }

    /// Check if there's a split vote (no clear winner despite having votes)
    ///
    /// A split vote occurs when votes are distributed among multiple candidates
    /// such that no single candidate can reach quorum.
    ///
    /// # Arguments
    /// * `partition` - The partition to check
    /// * `quorum_threshold` - Minimum votes needed for quorum
    /// * `total_voters` - Total number of nodes that could vote
    ///
    /// # Returns
    /// true if votes are split with no possibility of quorum
    ///
    /// # Requirements
    /// - Requirements 7.8: Detect when votes are split with no quorum
    pub async fn is_split_vote(
        &self,
        partition: &PartitionKey,
        quorum_threshold: usize,
        total_voters: usize,
    ) -> bool {
        let rounds = self.active_rounds.read().await;
        if let Some(round) = rounds.get(partition) {
            let valid_votes: Vec<_> = round
                .votes
                .values()
                .filter(|v| v.epoch == round.epoch)
                .collect();

            // If we haven't received all votes yet, it's not a split
            if valid_votes.len() < total_voters {
                return false;
            }

            // Count votes for each proposed owner
            let mut vote_counts: HashMap<&NodeId, usize> = HashMap::new();
            for vote in &valid_votes {
                *vote_counts.entry(&vote.proposed_owner).or_insert(0) += 1;
            }

            // Check if any candidate can reach quorum
            let max_votes = vote_counts.values().max().copied().unwrap_or(0);
            if max_votes < quorum_threshold {
                log::warn!(
                    "🔀 [Voting] Split vote detected for {}: max votes {} < quorum {}",
                    partition,
                    max_votes,
                    quorum_threshold
                );
                return true;
            }
        }
        false
    }

    /// Get current epoch for a partition's voting
    pub async fn current_epoch(&self, partition: &PartitionKey) -> Option<u64> {
        let rounds = self.active_rounds.read().await;
        rounds.get(partition).map(|r| r.epoch)
    }

    /// Handle timeout - increment epoch and prepare for new round
    pub async fn handle_timeout(&self, partition: &PartitionKey) -> Option<(u64, NodeId)> {
        let mut rounds = self.active_rounds.write().await;
        if let Some(round) = rounds.get_mut(partition) {
            round.epoch += 1;
            round.votes.clear();
            round.started_at = Instant::now();

            log::warn!(
                "⏰ [Voting] Timeout for {:?}, incrementing to epoch={}",
                partition,
                round.epoch
            );

            return Some((round.epoch, round.proposed_owner.clone()));
        }
        None
    }

    /// Handle split vote - increment epoch and prepare for new round
    pub async fn handle_split_vote(&self, partition: &PartitionKey) -> Option<(u64, NodeId)> {
        let mut rounds = self.active_rounds.write().await;
        if let Some(round) = rounds.get_mut(partition) {
            round.epoch += 1;
            round.votes.clear();
            round.started_at = Instant::now();

            log::warn!(
                "🔀 [Voting] Split vote for {:?}, incrementing to epoch={}",
                partition,
                round.epoch
            );

            return Some((round.epoch, round.proposed_owner.clone()));
        }
        None
    }

    /// Clear votes for a partition (after ownership confirmed)
    pub async fn clear_round(&self, partition: &PartitionKey) {
        let mut rounds = self.active_rounds.write().await;
        rounds.remove(partition);
        log::info!("🧹 [Voting] Cleared voting round for {:?}", partition);
    }

    /// Check for timed-out rounds
    pub async fn check_timeouts(&self) -> Vec<PartitionKey> {
        let rounds = self.active_rounds.read().await;
        let mut timed_out = Vec::new();

        for (partition, round) in rounds.iter() {
            if round.started_at.elapsed() > self.vote_timeout {
                timed_out.push(partition.clone());
            }
        }

        timed_out
    }

    /// Get active rounds count (for monitoring)
    pub async fn active_rounds_count(&self) -> usize {
        self.active_rounds.read().await.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ========================================================================
    // Task 6.1: VotingCoordinator struct tests
    // ========================================================================

    #[tokio::test]
    async fn test_voting_coordinator_creation() {
        let coordinator = VotingCoordinator::new(Duration::from_secs(10));
        assert_eq!(coordinator.vote_timeout(), Duration::from_secs(10));
        assert_eq!(coordinator.active_rounds_count().await, 0);
    }

    // ========================================================================
    // Task 6.2: start_round tests
    // ========================================================================

    #[tokio::test]
    async fn test_start_round_creates_epoch_1() {
        // Requirements 7.1: Create VoteRound with epoch=1 for new partitions
        let coordinator = VotingCoordinator::new(Duration::from_secs(10));
        let partition = PartitionKey::new("test_table", "partition_0");
        let proposed_owner = "node-1".to_string();

        let epoch = coordinator.start_round(&partition, &proposed_owner).await;

        assert_eq!(epoch, 1);
        assert_eq!(coordinator.current_epoch(&partition).await, Some(1));
    }

    #[tokio::test]
    async fn test_start_round_increments_epoch() {
        // Requirements 7.1: Increment epoch from existing round
        let coordinator = VotingCoordinator::new(Duration::from_secs(10));
        let partition = PartitionKey::new("test_table", "partition_0");
        let proposed_owner = "node-1".to_string();

        // First round
        let epoch1 = coordinator.start_round(&partition, &proposed_owner).await;
        assert_eq!(epoch1, 1);

        // Second round should increment
        let epoch2 = coordinator.start_round(&partition, &proposed_owner).await;
        assert_eq!(epoch2, 2);

        // Third round
        let epoch3 = coordinator.start_round(&partition, &proposed_owner).await;
        assert_eq!(epoch3, 3);
    }

    #[tokio::test]
    async fn test_start_round_clears_old_votes() {
        // Requirements 7.9: Clear old epoch votes when new round starts
        let coordinator = VotingCoordinator::new(Duration::from_secs(10));
        let partition = PartitionKey::new("test_table", "partition_0");
        let proposed_owner = "node-1".to_string();

        // Start first round and cast a vote
        let epoch1 = coordinator.start_round(&partition, &proposed_owner).await;
        coordinator
            .cast_vote(&partition, &proposed_owner, epoch1, &"voter-1".to_string())
            .await;

        // Verify vote exists
        let rounds = coordinator.active_rounds.read().await;
        assert_eq!(rounds.get(&partition).unwrap().votes.len(), 1);
        drop(rounds);

        // Start new round - should clear votes
        let _epoch2 = coordinator.start_round(&partition, &proposed_owner).await;

        // Verify votes are cleared
        let rounds = coordinator.active_rounds.read().await;
        assert_eq!(rounds.get(&partition).unwrap().votes.len(), 0);
    }

    // ========================================================================
    // Task 6.3: cast_vote tests
    // ========================================================================

    #[tokio::test]
    async fn test_cast_vote_creates_vote_record() {
        // Requirements 7.3: Create VoteRecord with epoch
        let coordinator = VotingCoordinator::new(Duration::from_secs(10));
        let partition = PartitionKey::new("test_table", "partition_0");
        let proposed_owner = "node-1".to_string();
        let voter = "voter-1".to_string();

        let epoch = coordinator.start_round(&partition, &proposed_owner).await;
        let vote = coordinator
            .cast_vote(&partition, &proposed_owner, epoch, &voter)
            .await;

        assert!(vote.is_some());
        let vote = vote.unwrap();
        assert_eq!(vote.voter, voter);
        assert_eq!(vote.epoch, epoch);
        assert_eq!(vote.proposed_owner, proposed_owner);
    }

    #[tokio::test]
    async fn test_cast_vote_rejects_wrong_epoch() {
        // Requirements 7.4: Only accept votes for current epoch
        let coordinator = VotingCoordinator::new(Duration::from_secs(10));
        let partition = PartitionKey::new("test_table", "partition_0");
        let proposed_owner = "node-1".to_string();
        let voter = "voter-1".to_string();

        let epoch = coordinator.start_round(&partition, &proposed_owner).await;

        // Try to cast vote with wrong epoch
        let vote = coordinator
            .cast_vote(&partition, &proposed_owner, epoch + 1, &voter)
            .await;

        assert!(vote.is_none());
    }

    #[tokio::test]
    async fn test_cast_vote_no_active_round() {
        // Vote should fail if no active round exists
        let coordinator = VotingCoordinator::new(Duration::from_secs(10));
        let partition = PartitionKey::new("test_table", "partition_0");
        let proposed_owner = "node-1".to_string();
        let voter = "voter-1".to_string();

        let vote = coordinator
            .cast_vote(&partition, &proposed_owner, 1, &voter)
            .await;

        assert!(vote.is_none());
    }

    // ========================================================================
    // Task 6.4: receive_vote tests
    // ========================================================================

    #[tokio::test]
    async fn test_receive_vote_accepts_matching_epoch() {
        // Requirements 7.3, 7.4: Parse incoming votes, only count matching epoch
        let coordinator = VotingCoordinator::new(Duration::from_secs(10));
        let partition = PartitionKey::new("test_table", "partition_0");
        let proposed_owner = "node-1".to_string();

        let epoch = coordinator.start_round(&partition, &proposed_owner).await;

        let vote = VoteRecord::new("voter-1".to_string(), epoch, proposed_owner.clone());
        let accepted = coordinator.receive_vote(&partition, vote).await;

        assert!(accepted);
    }

    #[tokio::test]
    async fn test_receive_vote_rejects_old_epoch() {
        // Requirements 7.4: Ignore votes from previous epochs
        let coordinator = VotingCoordinator::new(Duration::from_secs(10));
        let partition = PartitionKey::new("test_table", "partition_0");
        let proposed_owner = "node-1".to_string();

        // Start round at epoch 2
        coordinator.start_round(&partition, &proposed_owner).await;
        coordinator.start_round(&partition, &proposed_owner).await;

        // Try to receive vote from epoch 1
        let vote = VoteRecord::new("voter-1".to_string(), 1, proposed_owner.clone());
        let accepted = coordinator.receive_vote(&partition, vote).await;

        assert!(!accepted);
    }

    #[tokio::test]
    async fn test_vote_serialization() {
        // Requirements 7.3: Propagate via Gossip KV (serialization)
        let vote = VoteRecord::new("voter-1".to_string(), 1, "node-1".to_string());

        let json = VotingCoordinator::serialize_vote(&vote);
        assert!(json.is_some());

        let deserialized = VotingCoordinator::deserialize_vote(&json.unwrap());
        assert!(deserialized.is_some());

        let deserialized = deserialized.unwrap();
        assert_eq!(deserialized.voter, vote.voter);
        assert_eq!(deserialized.epoch, vote.epoch);
        assert_eq!(deserialized.proposed_owner, vote.proposed_owner);
    }

    // ========================================================================
    // Task 6.5: check_quorum tests
    // ========================================================================

    #[tokio::test]
    async fn test_check_quorum_not_reached() {
        // Requirements 7.4, 7.5: Check if count >= quorum threshold
        let coordinator = VotingCoordinator::new(Duration::from_secs(10));
        let partition = PartitionKey::new("test_table", "partition_0");
        let proposed_owner = "node-1".to_string();

        let epoch = coordinator.start_round(&partition, &proposed_owner).await;

        // Cast 1 vote, need 2 for quorum of 3
        coordinator
            .cast_vote(&partition, &proposed_owner, epoch, &"voter-1".to_string())
            .await;

        let winner = coordinator.check_quorum(&partition, 2).await;
        assert!(winner.is_none());
    }

    #[tokio::test]
    async fn test_check_quorum_reached() {
        // Requirements 7.5: Confirm ownership when quorum reached
        let coordinator = VotingCoordinator::new(Duration::from_secs(10));
        let partition = PartitionKey::new("test_table", "partition_0");
        let proposed_owner = "node-1".to_string();

        let epoch = coordinator.start_round(&partition, &proposed_owner).await;

        // Cast 2 votes for quorum of 2
        coordinator
            .cast_vote(&partition, &proposed_owner, epoch, &"voter-1".to_string())
            .await;
        coordinator
            .cast_vote(&partition, &proposed_owner, epoch, &"voter-2".to_string())
            .await;

        let winner = coordinator.check_quorum(&partition, 2).await;
        assert_eq!(winner, Some(proposed_owner));
    }

    #[tokio::test]
    async fn test_check_quorum_ignores_old_epoch_votes() {
        // Requirements 7.4: Only count votes with matching epoch
        let coordinator = VotingCoordinator::new(Duration::from_secs(10));
        let partition = PartitionKey::new("test_table", "partition_0");
        let proposed_owner = "node-1".to_string();

        // Start round 1 and cast votes
        let epoch1 = coordinator.start_round(&partition, &proposed_owner).await;
        coordinator
            .cast_vote(&partition, &proposed_owner, epoch1, &"voter-1".to_string())
            .await;
        coordinator
            .cast_vote(&partition, &proposed_owner, epoch1, &"voter-2".to_string())
            .await;

        // Start round 2 (clears votes)
        let epoch2 = coordinator.start_round(&partition, &proposed_owner).await;

        // Only cast 1 vote in epoch 2
        coordinator
            .cast_vote(&partition, &proposed_owner, epoch2, &"voter-1".to_string())
            .await;

        // Quorum should not be reached (only 1 vote in current epoch)
        let winner = coordinator.check_quorum(&partition, 2).await;
        assert!(winner.is_none());
    }

    // ========================================================================
    // Task 6.6: timeout handling tests
    // ========================================================================

    #[tokio::test]
    async fn test_check_timeouts_finds_expired_rounds() {
        // Requirements 7.7: check_timeouts() to find expired rounds
        let coordinator = VotingCoordinator::new(Duration::from_millis(10));
        let partition = PartitionKey::new("test_table", "partition_0");
        let proposed_owner = "node-1".to_string();

        coordinator.start_round(&partition, &proposed_owner).await;

        // Wait for timeout
        tokio::time::sleep(Duration::from_millis(20)).await;

        let timed_out = coordinator.check_timeouts().await;
        assert_eq!(timed_out.len(), 1);
        assert_eq!(timed_out[0], partition);
    }

    #[tokio::test]
    async fn test_handle_timeout_increments_epoch() {
        // Requirements 7.7: handle_timeout() to increment epoch and restart
        let coordinator = VotingCoordinator::new(Duration::from_secs(10));
        let partition = PartitionKey::new("test_table", "partition_0");
        let proposed_owner = "node-1".to_string();

        let epoch1 = coordinator.start_round(&partition, &proposed_owner).await;
        assert_eq!(epoch1, 1);

        // Handle timeout
        let result = coordinator.handle_timeout(&partition).await;
        assert!(result.is_some());

        let (new_epoch, owner) = result.unwrap();
        assert_eq!(new_epoch, 2);
        assert_eq!(owner, proposed_owner);

        // Verify epoch was incremented
        assert_eq!(coordinator.current_epoch(&partition).await, Some(2));
    }

    // ========================================================================
    // Task 6.7: split vote handling tests
    // ========================================================================

    #[tokio::test]
    async fn test_is_split_vote_detects_split() {
        // Requirements 7.8: Detect when votes are split with no quorum
        let coordinator = VotingCoordinator::new(Duration::from_secs(10));
        let partition = PartitionKey::new("test_table", "partition_0");
        let proposed_owner = "node-1".to_string();

        let epoch = coordinator.start_round(&partition, &proposed_owner).await;

        // Cast split votes: 2 for node-1, 2 for node-2
        coordinator
            .cast_vote(
                &partition,
                &"node-1".to_string(),
                epoch,
                &"voter-1".to_string(),
            )
            .await;
        coordinator
            .cast_vote(
                &partition,
                &"node-1".to_string(),
                epoch,
                &"voter-2".to_string(),
            )
            .await;
        coordinator
            .cast_vote(
                &partition,
                &"node-2".to_string(),
                epoch,
                &"voter-3".to_string(),
            )
            .await;
        coordinator
            .cast_vote(
                &partition,
                &"node-2".to_string(),
                epoch,
                &"voter-4".to_string(),
            )
            .await;

        // With 4 voters and quorum of 3, this is a split vote
        let is_split = coordinator.is_split_vote(&partition, 3, 4).await;
        assert!(is_split);
    }

    #[tokio::test]
    async fn test_handle_split_vote_increments_epoch() {
        // Requirements 7.8: Increment epoch and restart on split vote
        let coordinator = VotingCoordinator::new(Duration::from_secs(10));
        let partition = PartitionKey::new("test_table", "partition_0");
        let proposed_owner = "node-1".to_string();

        let epoch1 = coordinator.start_round(&partition, &proposed_owner).await;
        assert_eq!(epoch1, 1);

        // Handle split vote
        let result = coordinator.handle_split_vote(&partition).await;
        assert!(result.is_some());

        let (new_epoch, _) = result.unwrap();
        assert_eq!(new_epoch, 2);
    }

    // ========================================================================
    // Task 6.8: vote cleanup tests
    // ========================================================================

    #[tokio::test]
    async fn test_clear_round_removes_partition() {
        // Requirements 7.9: clear_round() after ownership confirmed
        let coordinator = VotingCoordinator::new(Duration::from_secs(10));
        let partition = PartitionKey::new("test_table", "partition_0");
        let proposed_owner = "node-1".to_string();

        coordinator.start_round(&partition, &proposed_owner).await;
        assert_eq!(coordinator.active_rounds_count().await, 1);

        coordinator.clear_round(&partition).await;
        assert_eq!(coordinator.active_rounds_count().await, 0);
        assert_eq!(coordinator.current_epoch(&partition).await, None);
    }

    // ========================================================================
    // PartitionKey tests
    // ========================================================================

    #[test]
    fn test_partition_key_equality() {
        let key1 = PartitionKey::new("table", "partition");
        let key2 = PartitionKey::new("table", "partition");
        let key3 = PartitionKey::new("table", "other");

        assert_eq!(key1, key2);
        assert_ne!(key1, key3);
    }

    #[test]
    fn test_partition_key_gossip_keys() {
        let key = PartitionKey::new("users", "p0");

        assert_eq!(key.topology_key(), "topology:users:p0");
        assert_eq!(key.vote_key_prefix(), "vote:users:p0");
        assert_eq!(key.vote_key(1, "node-1"), "vote:users:p0:1:node-1");
    }

    // ========================================================================
    // VoteRound tests
    // ========================================================================

    #[test]
    fn test_vote_round_creation() {
        let partition = PartitionKey::new("table", "p0");
        let round = VoteRound::new(partition.clone(), "node-1".to_string());

        assert_eq!(round.epoch, 1);
        assert_eq!(round.proposed_owner, "node-1");
        assert!(round.votes.is_empty());
    }

    #[test]
    fn test_vote_round_with_epoch() {
        let partition = PartitionKey::new("table", "p0");
        let round = VoteRound::with_epoch(partition.clone(), "node-1".to_string(), 5);

        assert_eq!(round.epoch, 5);
    }

    #[test]
    fn test_vote_round_valid_vote_count() {
        let partition = PartitionKey::new("table", "p0");
        let mut round = VoteRound::with_epoch(partition.clone(), "node-1".to_string(), 2);

        // Add votes with different epochs
        round.votes.insert(
            "voter-1".to_string(),
            VoteRecord::new("voter-1".to_string(), 2, "node-1".to_string()),
        );
        round.votes.insert(
            "voter-2".to_string(),
            VoteRecord::new("voter-2".to_string(), 2, "node-1".to_string()),
        );
        round.votes.insert(
            "voter-3".to_string(),
            VoteRecord::new("voter-3".to_string(), 1, "node-1".to_string()), // old epoch
        );

        // Only 2 votes should be counted (matching epoch 2)
        assert_eq!(round.valid_vote_count(), 2);
    }

    #[test]
    fn test_vote_round_has_voted() {
        let partition = PartitionKey::new("table", "p0");
        let mut round = VoteRound::with_epoch(partition.clone(), "node-1".to_string(), 2);

        round.votes.insert(
            "voter-1".to_string(),
            VoteRecord::new("voter-1".to_string(), 2, "node-1".to_string()),
        );
        round.votes.insert(
            "voter-2".to_string(),
            VoteRecord::new("voter-2".to_string(), 1, "node-1".to_string()), // old epoch
        );

        assert!(round.has_voted(&"voter-1".to_string()));
        assert!(!round.has_voted(&"voter-2".to_string())); // old epoch doesn't count
        assert!(!round.has_voted(&"voter-3".to_string())); // never voted
    }
}
