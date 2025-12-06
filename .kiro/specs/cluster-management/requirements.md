# Requirements Document

## Introduction

This document specifies the requirements for a production-ready cluster management system for CalmDB. The system enables multiple nodes to form a cluster, discover each other via Gossip protocol, detect failures, and manage partition topology for query routing. All cluster state is maintained in memory and propagated via Gossip - no persistence is required for cluster metadata.

## Glossary

- **Node**: A single CalmDB server instance participating in the cluster
- **Cluster**: A group of nodes working together to store and serve data
- **Gossip Protocol**: A decentralized communication protocol where nodes periodically exchange state information with random peers
- **Partition**: A logical subset of table data distributed across nodes
- **Write Node**: The single node responsible for write operations on a partition
- **Read Node**: A node that can serve read operations for a partition (may include write node)
- **Table Topology**: The mapping of partitions to their write and read nodes
- **Seed Node**: A known node address used for initial cluster discovery
- **Quorum**: More than half of the cluster nodes (N/2 + 1)
- **Chitchat**: A Rust Gossip protocol library used for cluster communication

## Requirements

### Requirement 1: Node Discovery and Cluster Formation

**User Story:** As a system administrator, I want nodes to automatically discover each other and form a cluster, so that I can deploy a distributed database without manual coordination.

#### Acceptance Criteria

1. WHEN a node starts with NO seed nodes configured, THE ClusterManager SHALL operate in standalone mode without Gossip communication
2. WHEN a node starts with seed nodes configured, THE ClusterManager SHALL attempt to join the cluster by contacting seed nodes
3. WHEN a node successfully contacts a seed node, THE ClusterManager SHALL receive the current cluster membership and table topology
4. IF a node with seed nodes configured fails to contact any seed node, THEN THE ClusterManager SHALL return an error and prevent the node from starting
5. WHEN a new node joins the cluster, THE ClusterManager SHALL propagate the membership change to all nodes via Gossip

### Requirement 2: Node Information Management

**User Story:** As a cluster operator, I want to monitor node health and resource usage, so that I can make informed decisions about load balancing.

#### Acceptance Criteria

1. WHILE a node is part of the cluster, THE ClusterManager SHALL periodically broadcast its resource metrics (partition count, load, memory usage)
2. WHEN a node receives resource metrics from another node via Gossip, THE ClusterManager SHALL update the local node registry
3. WHEN querying node information, THE ClusterManager SHALL return current metrics for all known nodes
4. WHEN a node's resource metrics change significantly, THE ClusterManager SHALL broadcast the update within gossip_interval

### Requirement 3: Failure Detection

**User Story:** As a system operator, I want the cluster to automatically detect node failures, so that the system can maintain availability without manual intervention.

#### Acceptance Criteria

1. WHILE a node is part of the cluster, THE ClusterManager SHALL send heartbeat messages to random peers every gossip_interval milliseconds
2. WHEN a node does not receive a heartbeat from a peer for failure_timeout seconds, THE FailureDetector SHALL mark that peer as Suspect state
3. WHEN a node remains in Suspect state for an additional suspect_timeout seconds, THE FailureDetector SHALL mark that node as Dead state
4. WHEN a node is marked as Dead, THE ClusterManager SHALL trigger partition failover for partitions owned by that node
5. WHEN a previously Dead node sends a heartbeat, THE FailureDetector SHALL transition that node back to Alive state

### Requirement 4: Table Topology Management

**User Story:** As a database user, I want to query partition locations quickly, so that my queries can be routed to the correct nodes efficiently.

#### Acceptance Criteria

1. WHEN a table is created with partitions, THE PartitionManager SHALL create a TableTopology with write_node assignments for each partition
2. WHEN querying partition topology, THE PartitionManager SHALL return the write_node and read_nodes within 1 millisecond from memory
3. WHEN partition topology changes, THE PartitionManager SHALL propagate the change via Gossip to all nodes
4. WHEN a node receives topology updates via Gossip, THE PartitionManager SHALL update the local topology cache immediately

### Requirement 5: Partition Write Node Assignment

**User Story:** As a database system, I want each partition to have exactly one write node, so that write consistency is maintained.

#### Acceptance Criteria

1. WHEN a table with pre-created partitions (PKHash, Hash, None) is created, THE PartitionManager SHALL assign write_node using round-robin distribution across alive nodes
2. WHEN a new on-demand partition (Range, DatetimeRange, Custom) is created, THE PartitionManager SHALL assign write_node to the node with the lowest load
3. WHEN a partition's write_node fails, THE PartitionManager SHALL initiate voting to elect a new write_node
4. WHEN a write_node election vote receives quorum (>= N/2 + 1), THE PartitionManager SHALL update the partition's write_node

### Requirement 6: Partition Read Node Management

**User Story:** As a database user, I want read queries to be distributed across multiple nodes, so that read performance can scale horizontally.

#### Acceptance Criteria

1. WHEN a partition is assigned a write_node, THE PartitionManager SHALL automatically add that node as a read_node
2. WHEN a node requests to become a read_node for a partition, THE PartitionManager SHALL add it to the read_nodes list
3. WHEN a read_node fails, THE PartitionManager SHALL remove it from the read_nodes list
4. WHEN routing a read query, THE QueryRouter SHALL select from available read_nodes using load balancing

### Requirement 7: Voting-Based Failover

**User Story:** As a system operator, I want partition ownership to be transferred safely during node failures, so that data remains accessible.

#### Acceptance Criteria

1. WHEN a node failure is detected, THE PartitionManager SHALL initiate a new voting round with epoch=1 for orphaned partitions
2. WHEN initiating a voting round, THE PartitionManager SHALL calculate proposed write_node using deterministic hash of partition ID
3. WHEN a node casts a vote, THE VotingCoordinator SHALL include the current vote epoch in the vote record
4. WHEN counting votes, THE VotingCoordinator SHALL only count votes with matching epoch (ignore votes from previous rounds)
5. WHEN a partition ownership proposal receives votes from quorum nodes with same epoch, THE PartitionManager SHALL confirm the new write_node
6. WHEN a node wins the vote, THE PartitionManager SHALL update topology and broadcast via Gossip
7. IF vote_timeout expires without quorum, THEN THE VotingCoordinator SHALL increment epoch and initiate a new voting round
8. IF votes are split (e.g., 5:5) with no clear winner, THEN THE VotingCoordinator SHALL increment epoch and initiate a new voting round
9. WHEN a new voting round starts, THE VotingCoordinator SHALL clear all votes from previous epochs

### Requirement 8: Graceful Shutdown

**User Story:** As a system administrator, I want to gracefully remove a node from the cluster, so that partitions can be reassigned before the node stops.

#### Acceptance Criteria

1. WHEN a graceful shutdown is initiated, THE ClusterManager SHALL broadcast a leaving notification to all peers
2. WHEN a leaving notification is received, THE PartitionManager SHALL initiate voting for that node's partitions
3. WHEN all partitions are reassigned, THE ClusterManager SHALL remove the node from the membership list
4. WHEN the leaving node completes shutdown, THE ClusterManager SHALL have transferred all partition ownership

### Requirement 9: In-Memory State (No Persistence)

**User Story:** As a developer, I want cluster state to be purely in-memory, so that the system is simple and fast.

#### Acceptance Criteria

1. WHEN cluster state changes, THE ClusterManager SHALL NOT persist any cluster metadata to disk
2. WHEN a node restarts, THE ClusterManager SHALL rebuild state from Gossip messages received from other nodes
3. WHEN the entire cluster restarts (cold start), THE first node SHALL scan the data directory to rebuild partition topology (future feature)
4. WHEN a node joins an existing cluster, THE ClusterManager SHALL receive complete topology from peers via Gossip

### Requirement 10: Configuration Management

**User Story:** As a system administrator, I want to configure cluster parameters via configuration file and environment variables.

#### Acceptance Criteria

1. WHEN loading configuration, THE ClusterConfig SHALL read from calm.toml file if present
2. WHEN environment variables are set, THE ClusterConfig SHALL override file-based configuration with environment values
3. WHEN cluster mode is disabled (no seed nodes), THE ClusterManager SHALL operate in standalone mode
4. WHEN invalid configuration is detected, THE ClusterConfig SHALL return a descriptive error message

### Requirement 11: Query Routing

**User Story:** As a database user, I want my queries to be automatically routed to the correct node, so that I don't need to know the cluster topology.

#### Acceptance Criteria

1. WHEN a write query arrives, THE QueryRouter SHALL route it to the partition's write_node
2. WHEN a read query arrives, THE QueryRouter SHALL route it to one of the partition's read_nodes
3. WHEN the target node is unavailable, THE QueryRouter SHALL return an error indicating the partition is temporarily unavailable
4. WHEN partition topology changes, THE QueryRouter SHALL use the updated topology for subsequent queries

### Requirement 12: Monitoring and Observability

**User Story:** As a system operator, I want to monitor cluster health and topology, so that I can diagnose issues.

#### Acceptance Criteria

1. WHILE the cluster is running, THE ClusterManager SHALL expose node count, alive node count via metrics
2. WHEN a node state changes, THE ClusterManager SHALL emit a log entry with node ID, old state, and new state
3. WHEN a failover operation completes, THE PartitionManager SHALL emit a log entry with reassigned partition count
4. WHEN queried via API, THE ClusterManager SHALL return current cluster membership and table topology
