# Design Document: Cluster Management

## Overview

This document describes the technical design for a production-ready cluster management system for CalmDB. The system uses the Chitchat Gossip protocol library for decentralized node discovery and failure detection, implements a voting-based Quorum mechanism with epoch-based rounds for partition ownership transfer, and maintains all cluster state in memory.

### Key Design Decisions

1. **In-Memory State**: All cluster metadata is in memory, propagated via Gossip, no persistence
2. **Chitchat for Failure Detection**: Leverages Chitchat's built-in SWIM-like failure detection
3. **Voting Quorum with Epoch**: Requires majority vote within same epoch for ownership change
4. **Read/Write Separation**: Each partition has one write_node and multiple read_nodes
5. **Minimum 3 Nodes**: Cluster requires at least 3 nodes for proper Quorum operation
6. **Shared Storage**: All nodes access the same data files, no data migration needed

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         CalmDB Node                                  │
├─────────────────────────────────────────────────────────────────────┤
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────────┐  │
│  │  ClusterManager │  │ PartitionManager│  │   VotingCoordinator │  │
│  │                 │  │                 │  │                     │  │
│  │ - Node registry │  │ - Table topology│  │ - Vote collection   │  │
│  │ - Node metrics  │  │ - Write/Read    │  │ - Epoch management  │  │
│  │ - Gossip handle │  │ - Query routing │  │ - Quorum check      │  │
│  └────────┬────────┘  └────────┬────────┘  └──────────┬──────────┘  │
│           │                    │                      │             │
│           └────────────────────┼──────────────────────┘             │
│                                │                                     │
│  ┌─────────────────────────────┴─────────────────────────────────┐  │
│  │                      Chitchat Layer                            │  │
│  │  - UDP Gossip transport                                        │  │
│  │  - Key-Value state propagation                                 │  │
│  │  - SWIM-like failure detection (built-in)                      │  │
│  └───────────────────────────────────────────────────────────────┘  │
│                                │                                     │
│                         UDP Port 7946                                │
└────────────────────────────────┼────────────────────────────────────┘
                                 │
              ┌──────────────────┼──────────────────┐
              │                  │                  │
              ▼                  ▼                  ▼
        ┌──────────┐      ┌──────────┐      ┌──────────┐
        │  Node B  │      │  Node C  │      │  Node D  │
        └──────────┘      └──────────┘      └──────────┘
                                 │
                    ┌────────────┴────────────┐
                    │     Shared Storage      │
                    │   (S3/MinIO/HDFS/NFS)   │
                    │   All partition data    │
                    └─────────────────────────┘
```

## Components and Interfaces

### 1. ClusterManager

The central component managing cluster membership and node information.

```rust
pub struct ClusterManager {
    /// Chitchat handle for Gossip communication
    chitchat: Arc<Chitchat>,
    
    /// Local node ID (UUID, immutable for node lifetime)
    node_id: NodeId,
    
    /// Cluster configuration
    config: ClusterConfig,
    
    /// All known nodes with their metrics
    nodes: Arc<RwLock<HashMap<NodeId, NodeInfo>>>,
    
    /// Event subscribers
    event_tx: broadcast::Sender<ClusterEvent>,
}

/// Node information (physical view)
pub struct NodeInfo {
    pub id: NodeId,
    pub gossip_addr: SocketAddr,
    pub state: NodeState,           // Alive, Suspect, Dead
    pub partition_count: usize,     // Number of partitions this node serves
    pub load: f64,                  // CPU/memory load (0.0 - 1.0)
    pub memory_usage_bytes: u64,    // Memory usage
    pub last_heartbeat: Instant,
}

pub enum NodeState {
    Alive,
    Suspect,
    Dead,
}

pub enum ClusterEvent {
    NodeJoined(NodeId),
    NodeSuspect(NodeId),
    NodeDead(NodeId),
    NodeRecovered(NodeId),
    TopologyChanged { table: String, partition: String },
}

impl ClusterManager {
    /// Create cluster manager
    /// - No seed nodes: standalone mode
    /// - With seed nodes: must join cluster or fail
    pub async fn new(config: ClusterConfig) -> CoreResult<Self>;
    
    /// Get my node ID
    pub fn my_node_id(&self) -> &NodeId;
    
    /// Get all live nodes
    pub async fn live_nodes(&self) -> Vec<NodeInfo>;
    
    /// Get node count (for quorum calculation)
    pub async fn node_count(&self) -> usize;
    
    /// Calculate quorum threshold
    pub fn quorum_threshold(&self, node_count: usize) -> usize {
        node_count / 2 + 1
    }
    
    /// Update my node metrics (broadcast via Gossip)
    pub async fn update_my_metrics(&self, load: f64, memory: u64);
    
    /// Set key-value in Gossip state
    pub async fn gossip_set(&self, key: &str, value: &str);
    
    /// Get key-value from Gossip state
    pub async fn gossip_get(&self, key: &str) -> Option<String>;
    
    /// Subscribe to cluster events
    pub fn subscribe(&self) -> broadcast::Receiver<ClusterEvent>;
    
    /// Initiate graceful shutdown
    pub async fn shutdown(&self) -> CoreResult<()>;
}
```

### 2. PartitionManager

Manages table topology and partition ownership.

```rust
pub struct PartitionManager {
    cluster: Arc<ClusterManager>,
    voting: Arc<VotingCoordinator>,
    
    /// Table topologies (in memory)
    topologies: Arc<RwLock<HashMap<String, TableTopology>>>,
}

/// Table topology information
pub struct TableTopology {
    pub table_name: String,
    pub partitions: HashMap<String, PartitionTopology>,
}

/// Partition topology
pub struct PartitionTopology {
    pub partition_id: String,
    pub write_node: NodeId,         // Single write node
    pub read_nodes: Vec<NodeId>,    // Multiple read nodes (includes write_node)
}

impl PartitionManager {
    /// Initialize topology for a new table
    pub async fn initialize_table(&self, table: &str, partitions: Vec<String>) -> CoreResult<()>;
    
    /// Get partition topology (fast, from memory)
    pub fn get_topology(&self, table: &str, partition: &str) -> Option<PartitionTopology>;
    
    /// Get write node for a partition
    pub fn get_write_node(&self, table: &str, partition: &str) -> Option<NodeId>;
    
    /// Get read nodes for a partition
    pub fn get_read_nodes(&self, table: &str, partition: &str) -> Option<Vec<NodeId>>;
    
    /// Add a read node for a partition
    pub async fn add_read_node(&self, table: &str, partition: &str, node: &NodeId) -> CoreResult<()>;
    
    /// Remove a read node for a partition
    pub async fn remove_read_node(&self, table: &str, partition: &str, node: &NodeId) -> CoreResult<()>;
    
    /// Handle node failure - initiate voting for orphaned partitions
    pub async fn on_node_failure(&self, failed_node: &NodeId) -> CoreResult<()>;
    
    /// Process vote results periodically
    pub async fn process_vote_results(&self);
}
```

### 3. VotingCoordinator

Manages the voting process with epoch-based rounds.

```rust
pub struct VotingCoordinator {
    cluster: Arc<ClusterManager>,
    
    /// Active voting rounds: partition -> VoteRound
    active_rounds: Arc<RwLock<HashMap<PartitionKey, VoteRound>>>,
    
    /// Vote timeout
    vote_timeout: Duration,
}

/// A voting round for a partition
pub struct VoteRound {
    pub partition: PartitionKey,
    pub epoch: u64,                          // Voting round number
    pub proposed_owner: NodeId,              // Proposed new write_node
    pub votes: HashMap<NodeId, VoteRecord>,  // voter -> vote
    pub started_at: Instant,
}

/// Individual vote record
#[derive(Clone, Serialize, Deserialize)]
pub struct VoteRecord {
    pub voter: NodeId,
    pub epoch: u64,
    pub proposed_owner: NodeId,
    pub voted_at: u64,
}

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct PartitionKey {
    pub table_name: String,
    pub partition_id: String,
}

impl VotingCoordinator {
    /// Start a new voting round for a partition
    pub async fn start_round(&self, partition: &PartitionKey, proposed_owner: &NodeId) -> u64;
    
    /// Cast a vote (includes epoch)
    pub async fn cast_vote(&self, partition: &PartitionKey, proposed_owner: &NodeId, epoch: u64);
    
    /// Receive vote from Gossip
    pub async fn receive_vote(&self, vote: VoteRecord);
    
    /// Check if current round has reached quorum
    pub async fn check_quorum(&self, partition: &PartitionKey) -> Option<NodeId>;
    
    /// Get current epoch for a partition's voting
    pub async fn current_epoch(&self, partition: &PartitionKey) -> Option<u64>;
    
    /// Handle timeout - increment epoch and start new round
    pub async fn handle_timeout(&self, partition: &PartitionKey) -> CoreResult<()>;
    
    /// Handle split vote - increment epoch and start new round
    pub async fn handle_split_vote(&self, partition: &PartitionKey) -> CoreResult<()>;
    
    /// Clear votes for a partition (after ownership confirmed)
    pub async fn clear_round(&self, partition: &PartitionKey);
    
    /// Check for timed-out rounds
    pub async fn check_timeouts(&self) -> Vec<PartitionKey>;
}
```

### 4. QueryRouter

Routes queries to appropriate nodes.

```rust
pub struct QueryRouter {
    partition_manager: Arc<PartitionManager>,
}

impl QueryRouter {
    /// Route a write query to the partition's write_node
    pub fn route_write(&self, table: &str, partition: &str) -> CoreResult<NodeId>;
    
    /// Route a read query to one of the partition's read_nodes (load balanced)
    pub fn route_read(&self, table: &str, partition: &str) -> CoreResult<NodeId>;
}
```

### 5. ClusterConfig

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterConfig {
    /// Unique node identifier (generated on first start)
    pub node_id: String,
    
    /// Cluster identifier (nodes must match to join)
    pub cluster_id: String,
    
    /// Gossip listen address (UDP)
    pub listen_addr: SocketAddr,
    
    /// Seed nodes for initial discovery
    /// Empty = standalone mode
    /// Non-empty = must join cluster or fail
    pub seed_nodes: Vec<SocketAddr>,
    
    /// Gossip interval
    pub gossip_interval: Duration,  // default: 500ms
    
    /// Time before marking node as Suspect
    pub failure_timeout: Duration,  // default: 10s
    
    /// Additional time before marking Suspect as Dead
    pub suspect_timeout: Duration,  // default: 10s
    
    /// Vote timeout (per round)
    pub vote_timeout: Duration,     // default: 10s
    
    /// Minimum cluster size for operation
    pub min_cluster_size: usize,    // default: 3
}
```

## Data Models

### Gossip Key-Value Schema

| Key Pattern | Writer | Description |
|-------------|--------|-------------|
| `node:{node_id}:info` | Only that node | JSON NodeInfo (metrics) |
| `topology:{table}:{partition}` | Write node | JSON PartitionTopology |
| `vote:{table}:{partition}:{epoch}:{voter}` | Voter | JSON VoteRecord |

### In-Memory State

All state is maintained in memory and propagated via Gossip:

```rust
// ClusterManager
nodes: HashMap<NodeId, NodeInfo>

// PartitionManager  
topologies: HashMap<TableName, TableTopology>

// VotingCoordinator
active_rounds: HashMap<PartitionKey, VoteRound>
```

No persistence required. On restart:
- Join existing cluster → receive state via Gossip
- Cold start (all nodes restart) → first node scans data directory (future feature)



## Voting-Based Failover Flow

### Sequence Diagram

```
┌─────────┐     ┌─────────┐     ┌─────────┐     ┌─────────┐
│ Node A  │     │ Node B  │     │ Node C  │     │ Node D  │
│(Failed) │     │         │     │         │     │         │
└────┬────┘     └────┬────┘     └────┬────┘     └────┬────┘
     │               │               │               │
     X (crash)       │               │               │
     │               │               │               │
     │          Chitchat detects A is Dead           │
     │               │───────────────┼───────────────│
     │               │               │               │
     │          Start voting round epoch=1           │
     │               │               │               │
     │          Node A had: P0, P1, P2 (write_node)
     │          hash(P0) % 3 = 0 → B proposed
     │          hash(P1) % 3 = 1 → C proposed
     │          hash(P2) % 3 = 2 → D proposed
     │               │               │               │
     │          B votes: P0→B,e=1   C votes: P0→B,e=1  D votes: P0→B,e=1
     │               │──────────────>│<──────────────│
     │               │               │               │
     │          Votes propagate via Gossip...
     │               │               │               │
     │          B counts epoch=1 votes for P0→B: 3 (>= 2 quorum)
     │               │               │               │
     │          B becomes write_node for P0 ✓
     │          C becomes write_node for P1 ✓
     │          D becomes write_node for P2 ✓
     │               │               │               │
     │          Update topology via Gossip
     │               │               │               │
```

### Timeout/Split Vote Handling

```
Scenario: Split vote, no quorum reached

Round 1 (epoch=1):
- B, C vote for P0→B (2 votes)
- D, E vote for P0→D (2 votes)
- No quorum (need 3)

After vote_timeout:
- Increment epoch to 2
- Clear epoch=1 votes
- Start new round

Round 2 (epoch=2):
- Gossip has converged, all nodes agree
- B, C, D, E all vote for P0→B (4 votes)
- Quorum reached!
- B becomes write_node
```

### Failover Algorithm

```rust
impl PartitionManager {
    pub async fn on_node_failure(&self, failed_node: &NodeId) -> CoreResult<()> {
        // 1. Get partitions where failed_node was write_node
        let orphaned = self.get_write_partitions_of(failed_node).await;
        if orphaned.is_empty() {
            return Ok(());
        }
        
        // 2. Also remove failed_node from read_nodes
        self.remove_from_all_read_nodes(failed_node).await;
        
        // 3. Get alive nodes (sorted for deterministic calculation)
        let mut alive_nodes = self.cluster.live_nodes().await;
        alive_nodes.sort_by(|a, b| a.id.cmp(&b.id));
        
        if alive_nodes.len() < self.cluster.config.min_cluster_size {
            log::warn!("Cluster size {} below minimum {}", 
                alive_nodes.len(), self.cluster.config.min_cluster_size);
            return Ok(());
        }
        
        // 4. Start voting round for each orphaned partition
        for partition in orphaned {
            let proposed_idx = self.hash_partition(&partition) % alive_nodes.len();
            let proposed_owner = &alive_nodes[proposed_idx];
            
            // Start round with epoch=1
            let epoch = self.voting.start_round(&partition, &proposed_owner.id).await;
            
            // Cast my vote
            self.voting.cast_vote(&partition, &proposed_owner.id, epoch).await;
        }
        
        Ok(())
    }
    
    pub async fn process_vote_results(&self) {
        let node_count = self.cluster.node_count().await;
        let quorum = self.cluster.quorum_threshold(node_count);
        let my_id = self.cluster.my_node_id();
        
        // Check all active voting rounds
        for (partition, round) in self.voting.active_rounds.read().await.iter() {
            // Only count votes with matching epoch
            let valid_votes: Vec<_> = round.votes.values()
                .filter(|v| v.epoch == round.epoch)
                .collect();
            
            if valid_votes.len() >= quorum {
                // Check if I won
                if &round.proposed_owner == my_id {
                    log::info!("Won vote for {:?} epoch={} with {} votes",
                        partition, round.epoch, valid_votes.len());
                    
                    // Update topology
                    self.set_write_node(&partition.table_name, &partition.partition_id, my_id).await;
                    
                    // Clear voting round
                    self.voting.clear_round(partition).await;
                }
            }
        }
        
        // Check for timeouts
        for partition in self.voting.check_timeouts().await {
            self.voting.handle_timeout(&partition).await;
        }
    }
}
```

## Correctness Properties

*A property is a characteristic or behavior that should hold true across all valid executions of a system-essentially, a formal statement about what the system should do. Properties serve as the bridge between human-readable specifications and machine-verifiable correctness guarantees.*

### Property 1: Node State Machine Transitions

*For any* node, state transitions SHALL follow the valid state machine:
- Alive → Suspect (when heartbeat timeout exceeded)
- Suspect → Dead (when suspect timeout exceeded)
- Dead → Alive (when heartbeat received)
- Alive → Alive (heartbeat refresh)

No other transitions are valid.

**Validates: Requirements 3.2, 3.3, 3.5**

### Property 2: Node Metrics Consistency

*For any* node metrics received via Gossip, the local node registry SHALL be updated to reflect the latest metrics, and querying node information SHALL return metrics for all known nodes.

**Validates: Requirements 2.2, 2.3**

### Property 3: Table Topology Initialization

*For any* table created with N partitions and M alive nodes, the PartitionManager SHALL create a TableTopology where each partition has exactly one write_node assigned.

**Validates: Requirements 4.1**

### Property 4: Round-Robin Distribution

*For any* table with N pre-created partitions and M live nodes, after initialization, each node SHALL be assigned as write_node for either floor(N/M) or ceil(N/M) partitions.

**Validates: Requirements 5.1**

### Property 5: Lowest-Load Assignment

*For any* new on-demand partition, the write_node SHALL be assigned to the node with the lowest current load among alive nodes.

**Validates: Requirements 5.2**

### Property 6: Read Node List Consistency

*For any* partition:
- When a write_node is assigned, that node SHALL automatically be in read_nodes
- When a node is added as read_node, it SHALL appear in read_nodes
- When a read_node fails, it SHALL be removed from read_nodes

**Validates: Requirements 6.1, 6.2, 6.3**

### Property 7: Read Query Routing

*For any* read query to a partition, the QueryRouter SHALL return a node that is in the partition's read_nodes list.

**Validates: Requirements 6.4**

### Property 8: Write Query Routing

*For any* write query to a partition, the QueryRouter SHALL return the partition's write_node.

**Validates: Requirements 11.1**

### Property 9: Deterministic Vote Calculation

*For any* partition and sorted list of alive nodes, all nodes SHALL calculate the same proposed write_node using the hash function. The calculation is deterministic.

**Validates: Requirements 7.2**

### Property 10: Vote Epoch Inclusion

*For any* vote cast, the vote record SHALL include the current voting round's epoch.

**Validates: Requirements 7.3**

### Property 11: Same-Epoch Vote Counting

*For any* vote counting operation, only votes with epoch matching the current round's epoch SHALL be counted. Votes from previous epochs SHALL be ignored.

**Validates: Requirements 7.4**

### Property 12: Quorum-Based Ownership Confirmation

*For any* partition ownership change, the new write_node must have received votes from at least (N/2 + 1) nodes with the same epoch. No ownership change can occur with fewer votes.

**Validates: Requirements 5.4, 7.5**

### Property 13: Epoch Increment on Failure

*For any* voting round that times out OR results in a split vote (no quorum), the epoch SHALL be incremented and a new round SHALL be started.

**Validates: Requirements 7.7, 7.8**

### Property 14: Vote Cleanup on New Round

*For any* new voting round, all votes from previous epochs SHALL be cleared before the new round begins.

**Validates: Requirements 7.9**

### Property 15: Configuration Environment Override

*For any* configuration key that exists in both calm.toml and environment variables, the environment variable value SHALL take precedence.

**Validates: Requirements 10.2**

## Error Handling

### Network Errors

| Error | Handling Strategy |
|-------|-------------------|
| No seed nodes | Operate in standalone mode |
| Seed node unreachable | Fail startup with error |
| Gossip message timeout | Chitchat handles internally |

### Voting Errors

| Error | Handling Strategy |
|-------|-------------------|
| Vote timeout | Increment epoch, start new round |
| Split vote | Increment epoch, start new round |
| Node recovers during vote | Cancel votes, let node reclaim |

### Routing Errors

| Error | Handling Strategy |
|-------|-------------------|
| Write node unavailable | Return error, client retries |
| All read nodes unavailable | Return error |
| Partition not found | Return error |

## Testing Strategy

### Property-Based Testing Framework

We will use the `proptest` crate for Rust property-based testing. Each property test will:
- Run a minimum of 100 iterations
- Use smart generators that constrain inputs to valid state spaces
- Be tagged with the correctness property it validates

### Test Categories

#### Unit Tests
1. ClusterConfig parsing from TOML and environment
2. NodeState transitions
3. PartitionKey hashing consistency
4. Vote counting with epoch filtering
5. Quorum calculation

#### Property-Based Tests
1. Node state machine (Property 1)
2. Round-robin distribution (Property 4)
3. Deterministic vote calculation (Property 9)
4. Same-epoch vote counting (Property 11)
5. Quorum-based confirmation (Property 12)
6. Epoch increment on failure (Property 13)
7. Config env override (Property 15)

#### Integration Tests
1. Multi-node cluster formation (3+ nodes)
2. Node failure and voting
3. Vote timeout and epoch increment
4. Split vote recovery
5. Graceful shutdown

### Test Annotations

Each property-based test must include:
```rust
// **Feature: cluster-management, Property 11: Same-Epoch Vote Counting**
// **Validates: Requirements 7.4**
```

## Dependencies

| Crate | Version | Purpose |
|-------|---------|---------|
| `chitchat` | `0.8` | Gossip protocol with SWIM failure detection |
| `tokio` | `1.x` | Async runtime |
| `serde` | `1.x` | Serialization |
| `proptest` | `1.x` | Property-based testing |
| `tracing` | `0.1` | Structured logging |
| `uuid` | `1.x` | Node ID generation |

## Configuration Example

### calm.toml

```toml
[cluster]
cluster_id = "my-calm-cluster"
listen_addr = "0.0.0.0:7946"
seed_nodes = ["192.168.1.10:7946", "192.168.1.11:7946"]

gossip_interval_ms = 500
failure_timeout_secs = 10
suspect_timeout_secs = 10
vote_timeout_secs = 10
min_cluster_size = 3
```

### Environment Variables

```bash
CALM_CLUSTER_ID=my-calm-cluster
CALM_NODE_ID=node-abc123
CALM_GOSSIP_ADDR=0.0.0.0:7946
CALM_SEED_NODES=192.168.1.10:7946,192.168.1.11:7946
```
