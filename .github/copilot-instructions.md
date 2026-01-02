# Calm Database - AI Coding Agent Instructions

## Project Overview

**Calm** is a distributed HTAP (Hybrid Transactional/Analytical Processing) database written in Rust, supporting multiple query protocols (GraphQL, MySQL, Elasticsearch). It uses **shared storage** (CubeFS/S3) with **stateless compute nodes** and **gossip-based coordination** (Chitchat).

Key architectural principle: **Partition Owner Model** - each partition has exactly one writer (owner) at any time, but all nodes can read all partitions from shared storage.

## Architecture

### Core Components (src/)

```
calm/           - CalmService orchestrator (owns Catalog + ClusterManager + Engine)
├── catalog/    - Table metadata & schema management
├── cluster/    - Gossip coordination, partition ownership
├── engine/     - Local partition operations (data_handler, partition_handler, persist_handler)
│   ├── data_handler.rs      - Engine::insert_batch() - core data insertion
│   ├── partition_handler.rs - Partition lifecycle management
│   └── persist_handler.rs   - Background persistence tasks
├── router/     - Partition routing logic (PKHash, Hash, Range, Custom)
├── storage/    - Partition, segment abstractions
│   └── segment/fulltext/    - Unified posting list architecture for FTS
├── compute/    - Query execution (DataFusion 51.0 integration)
│   └── federation/          - Distributed query via Arrow Flight
└── protocol/   - Multi-protocol servers (GraphQL, MySQL, ES)
    ├── graphql/ - ONLY DDL interface (createTable mutation)
    ├── mysql/   - Data operations (uses patched msql-srv)
    └── elasticsearch/ - Data operations (ES-compatible API)
```

**Critical**: `CalmService` is the top-level coordinator. `Engine::insert_batch(table_name, partition_name, batch)` is the unified data insertion method. `Router` determines partition destination.

### Data Flow

1. **Write Path**: Client → Protocol Layer → Router (determine partition) → ClusterManager (locate owner) → `Engine::insert_batch(table, partition, batch)` → Partition → Segment
2. **Read Path**: Unified gRPC/Arrow Flight for distributed queries. Federation module coordinates cross-node queries via `FlightExecutor`.

### Key Modules

- **CalmService** ([src/calm/mod.rs](src/calm/mod.rs)): Top-level coordinator owning Catalog + ClusterManager + Engine. Exposes RPC service and Arrow Flight.
- **Catalog** ([src/catalog/mod.rs](src/catalog/mod.rs)): Manages `TableMeta` and `PartitionMeta`. Single source of truth for schema.
- **ClusterManager** ([src/cluster/mod.rs](src/cluster/mod.rs)): Gossip-based coordination via Chitchat 0.9. Node IDs: `timestamp_host_grpc_port`.
- **Engine** ([src/engine/mod.rs](src/engine/mod.rs)): Handles local partition lifecycle, persistence (`persist_tx` channel), and data operations.
- **Router** ([src/router/mod.rs](src/router/mod.rs)): Unified routing logic for all protocols. Returns target partition name.
- **Federation** ([src/compute/federation/](src/compute/federation/)): Distributed query via `FlightExecutor`, `RemoteTableProvider`, and `FederatedQueryExecutor`.

## DDL Philosophy

**GraphQL is the ONLY DDL interface**. MySQL/ES protocols provide data operations but NOT schema creation.

### Creating Tables

Use GraphQL mutation `createTable` at port 9567. Example:

```graphql
mutation {
  createTable(input: {
    name: "events"
    primaryKey: "event_id"
    description: "Event tracking table"
    partitionStrategy: { pkHash: { numPartitions: 8 } }
    fields: [
      { name: "event_id", fieldType: U64, nullable: false }
      { name: "user_id", fieldType: U64, indexed: true }
      { name: "created_at", fieldType: TIMESTAMP, format: "ms" }
      { name: "tags", fieldType: KEYWORD, isArray: true }
    ]
  }) { name partitionCount }
}
```

**Partition Strategies**:
- `pkHash: { numPartitions: N }` - Hash on primary key (default)
- `hash: { field: "user_id", numPartitions: N }` - Hash on specific field
- `range: { field: "created_at", rangeType: TIMESTAMP, ranges: [...] }` - Time/int ranges
- `custom: { numPartitions: N }` - User-controlled routing
- `none: {}` - Single partition

## Development Workflows

### Building & Running

```bash
# Build
cargo build --release

# Run single node
cargo run --bin calm -- --config calm.toml

# Run cluster (2-node example)
./examples/cluster_test.sh start    # Start both nodes
./examples/cluster_test.sh logs     # View logs
./examples/cluster_test.sh stop     # Stop cluster
```

### Configuration

See [calm.toml](calm.toml).
- **Ports**: `graphql_port` (9567), `mysql_port` (3307), `es_port` (9200).
- **Internal**: `grpc_port` is auto-assigned (starting 52000) if not specified. Used for both RPC and Arrow Flight.
- **Data**: `data_dir` defaults to `./data`. Contains `tables/` subdirectory.

### Testing

**Shell scripts** (root directory):
- `test_graphql_ddl.sh` - GraphQL table creation (run this FIRST)
- `test_connection.sh` - MySQL JDBC connectivity
- `test_partition_filter.sh` - Partition pruning logic
- `test_router_architecture.sh` - Router logic verification
- `test_federation.sh` - Distributed query across nodes
- `examples/cluster_test.sh` - Full cluster lifecycle

**Rust tests**:
```bash
cargo test                          # All tests
cargo test --test test_timestamp_field  # Specific integration test
cargo test --package calm --lib catalog::tests  # Module tests
```

**Debugging**:
- Set `RUST_LOG=debug` or `RUST_LOG=calm=trace` for verbose logging
- Check `data/node*/logs/` for node-specific logs
- Use `test_list_node.sh` to inspect cluster state

## Code Conventions

### Error Handling

Use `CoreResult<T>` and `CoreError` from [src/utils/error.rs](src/utils/error.rs).

```rust
// Function signatures
pub async fn my_function() -> CoreResult<MyType> { ... }

// Error construction
Err(CoreError::Internal("descriptive message".to_string()))
Err(CoreError::NotExisted("table_name".to_string()))

// DataFusion errors auto-convert via From trait
let result = datafusion_operation()?;  // Auto-converts to CoreError
```

### Async Patterns

**Critical**: All I/O and long-running operations must be async. Use `tokio::sync::RwLock` (not `std::sync`).

```rust
// Correct
use tokio::sync::RwLock;
let partitions = Arc::new(RwLock::new(HashMap::new()));
let guard = partitions.read().await;

// Wrong - blocks tokio runtime
use std::sync::RwLock;  // ❌ Never use in async context
```

### Schema Evolution

When adding field options, update ALL 13 field types in [src/catalog/schema/field.rs](src/catalog/schema/field.rs):
- `TEXT`, `KEYWORD`, `I64`, `U64`, `F64`, `BOOL`, `TIMESTAMP`, `DATE`, `BINARY`, `JSON`, `GEO_POINT`, `IP`, `ARRAY`

### Partition Key Encoding

- **Partition Key**: `partition:table_name:partition_name:version` (used in Chitchat cluster state)
- **Node ID**: `timestamp_host_grpc_port` (e.g., `20231225120530123_127.0.0.1_52000`)
- **Partition Dir**: `data/tables/{table_name}/partitions/{partition_name}_{version}/`

### Full-Text Indexing

Unified posting list architecture in [src/storage/segment/fulltext/](src/storage/segment/fulltext/). All FTS operations go through `PostingListManager`.

## Dependencies

- **DataFusion 51.0**: Query engine (CRITICAL: upgrading requires protocol compatibility checks)
- **Chitchat 0.9**: Gossip protocol for cluster coordination
- **Tonic/Arrow Flight**: Unified gRPC (control + data plane, single port)
- **msql-srv (patched)**: MySQL wire protocol ([libs/msql-srv-patched/](libs/msql-srv-patched/))
- **Tokio**: Async runtime (use `tokio::sync` primitives, not `std::sync`)

## Common Patterns

### Accessing Services in Protocol Handlers

```rust
// In GraphQL/MySQL/ES handlers
let calm = ctx.data::<Arc<CalmService>>()?;  // GraphQL
let calm = self.calm.clone();                 // MySQL/ES

// Get components
let catalog = calm.catalog();
let engine = calm.engine();
let cluster_mgr = calm.cluster_manager();
```

### Data Insertion Flow

```rust
// 1. Route to partition
let router = calm.router();
let partition_name = router.route(table_name, &batch, None)?;

// 2. Locate owner (if remote, forward via Arrow Flight)
let owner = cluster_mgr.get_partition_owner(table_name, &partition_name).await?;

// 3. Insert locally (if this node is owner)
engine.insert_batch(table_name, &partition_name, batch).await?;
```

### Reading Partition State

```rust
// Get all partitions for a table
let partitions = engine.partitions.read().await;
let table_partitions: Vec<_> = partitions
    .iter()
    .filter(|((t, _), _)| t == table_name)
    .collect();

// Get specific partition
let partition = engine.get_partition(table_name, partition_name).await
    .ok_or(CoreError::NotExisted(format!("partition {}", partition_name)))?;
```

## Critical Gotchas

1. **GraphQL-only DDL**: Never implement table creation in MySQL/ES protocols. Always redirect to GraphQL.
2. **Partition Owner Check**: Before inserting, verify current node is partition owner or forward to owner.
3. **Async RwLock**: Use `tokio::sync::RwLock`, never `std::sync::RwLock` in async code.
4. **Port Conflicts**: `grpc_port` auto-assigns starting at 52000. Avoid conflicts with external services.
5. **Segment Persistence**: Background task via `persist_tx` channel. Don't block on persistence in write path.
