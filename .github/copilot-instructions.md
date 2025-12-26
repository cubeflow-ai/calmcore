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
├── engine/     - Local partition operations (data handler, persist handler)
├── storage/    - Partition, segment, router abstractions
├── compute/    - Query execution (DataFusion integration)
└── protocol/   - Multi-protocol servers (GraphQL, MySQL, ES)
```

**Critical**: `CalmService` is the top-level coordinator. `Engine` only handles local data operations and does NOT own catalog or cluster state.

### Data Flow

1. **Write Path**: Client → Protocol Layer → CalmService → ClusterManager (routing) → Partition Owner → Engine → Segment
2. **Read Path**: Any node can read any partition from shared storage (no data forwarding needed)

### Key Modules

- **Catalog** ([src/catalog/mod.rs](src/catalog/mod.rs)): Manages `TableMeta` and `PartitionMeta`. Single source of truth for schema.
- **ClusterManager** ([src/cluster/mod.rs](src/cluster/mod.rs)): Gossip-based coordination using Chitchat. Node IDs encode addresses: `timestamp_host_tarpc_port_flight_port`.
- **Engine** ([src/engine/mod.rs](src/engine/mod.rs)): Local partition lifecycle, background persistence tasks.
- **Partition** ([src/storage/partition/mod.rs](src/storage/partition/mod.rs)): In-memory segment management, primary key deduplication.

## DDL Philosophy

**GraphQL is the ONLY DDL interface**. MySQL/ES protocols provide data operations but NOT schema creation.

**Rationale**: Calm's partitioning strategies (PKHash, Range, Custom, None) and 14 numeric types cannot map to standard SQL DDL without compatibility issues.

### Creating Tables

Use GraphQL mutation `createTable`:

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

**Field Types**: I8-I64, U8-U64, F32, F64, BOOLEAN, KEYWORD, TIMESTAMP (see [docs/graphql_ddl_complete_example.md](docs/graphql_ddl_complete_example.md))

**Partition Strategies**:
- `PKHash`: Hash by primary key (recommended for keyed tables)
- `Hash`: Hash by specific field
- `Range`: Time-series (auto-creates partitions like `partition_1704067200000`)
- `Custom`: Manual partition naming
- `None`: Single partition (small tables)

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

See [calm.toml](calm.toml) for reference. Key sections:
- `[log]`: Levels (trace/debug/info/warn/error), rotation settings
- `[engine]`: `data_dir`, `persist_check_interval_secs`, `max_concurrent_persists`
- Ports: `graphql_port` (9567), `mysql_port` (3307), `es_port` (9200)

### Testing

**Shell scripts** (root directory) for functional testing:
- `test_connection.sh` - MySQL JDBC connectivity
- `test_graphql_ddl.sh` - GraphQL table creation
- `test_partition_filter.sh` - Partition pruning logic
- `test_natural_order.sh` - Natural key ordering
- `test_oneof_partition.sh` - All partition strategies

**Python tests** (`test_suite/`):
- `test_wikipedia.py` - Full-text search queries
- `test_nature_order.py` - Batch grouped aggregation

**Rust tests**:
```bash
cargo test                          # Unit tests
cargo test --test test_timestamp_field  # Specific integration test
```

## Code Conventions

### Error Handling

Use `CoreResult<T>` and `CoreError` from [src/utils/error.rs](src/utils/error.rs). Never panic in production code except for unrecoverable states.

```rust
pub type CoreResult<T> = Result<T, CoreError>;

// Good
fn load_partition(&self, name: &str) -> CoreResult<Arc<Partition>> {
    self.partitions.get(name)
        .ok_or_else(|| CoreError::NotFound(format!("Partition {} not found", name)))
}
```

### Schema Evolution

Field options MUST support backward compatibility:

```rust
// In src/catalog/schema/field.rs - all 13 field types have this pattern
pub struct KeywordFieldOption {
    pub indexed: bool,
    pub case_sensitive: bool,
    pub is_array: bool,              // ✅ New fields added as bool with defaults
    pub description: Option<String>, // ✅ or Option<T>
    pub default_value: Option<String>,
    pub nullable: bool,
}
```

When adding fields to `FieldOption`, update ALL enum variants (13 types) in [src/catalog/schema/field.rs](src/catalog/schema/field.rs).

### Partition Key Encoding

Cluster state keys use structured format (see [src/cluster/mod.rs](src/cluster/mod.rs)):

```rust
// Partition ownership: "partition:table_name:partition_name:version"
make_partition_key("events", "p0", 42)  // => "partition:events:p0:42"

// Node ID includes addresses: "timestamp_host_tarpc_port_flight_port"
make_node_id("127.0.0.1", 52000, 52001) // => "20231225120530123_127.0.0.1_52000_52001"
```

Parse these using `parse_partition_key()` and `parse_tarpc_address_from_node_id()`.

### Full-Text Indexing

**Unified posting list architecture** (NOT separate inverted_index + position_index):

```rust
// In src/storage/segment/fulltext/
pub struct PostingEntry {
    pub doc_id: u32,
    pub term_freq: u32,
    pub positions: Vec<u32>,  // All info in one entry
}

pub struct FullTextField {
    posting_lists: BTree<String, Vec<PostingEntry>>, // Single index structure
    // ...
}
```

See [docs/fulltext_unified_architecture.md](docs/fulltext_unified_architecture.md) for Lucene/Tantivy design comparison.

## Dependencies

- **DataFusion 51.0**: Query engine (use physical plans, not logical)
- **Chitchat 0.9**: Gossip protocol for cluster coordination
- **Tarpc 0.37**: RPC for control plane (DDL, metadata)
- **Arrow Flight 57**: Data plane for distributed queries (planned)
- **msql-srv (patched)**: MySQL wire protocol ([libs/msql-srv-patched/](libs/msql-srv-patched/))
- **mem_btree**: Custom B-tree with TTL support ([libs/mem_btree/](libs/mem_btree/))

## Common Patterns

### Accessing Services

```rust
// In protocol handlers (GraphQL/MySQL/ES)
let calm = ctx.data::<Arc<CalmService>>()?;
let catalog = calm.catalog();                    // Get catalog
let cluster = calm.cluster_manager().await?;     // Get cluster
let engine = calm.engine();                      // Get engine
```

### Adding GraphQL Types

Update [src/protocol/graphql/mod.rs](src/protocol/graphql/mod.rs):

```rust
#[derive(async_graphql::InputObject)]
pub struct MyInput {
    /// Field description - shown in GraphQL Playground
    /// 
    /// # MCP Note
    /// Include usage examples for AI agents
    pub field: Type,
}
```

Always add `/// description` comments for MCP integration (see [docs/mcp_integration.md](docs/mcp_integration.md)).

### Background Tasks

Use tokio spawn with proper shutdown handling:

```rust
// In Engine::new()
let handle = tokio::spawn(Self::persist_background_task(
    engine.clone(),
    persist_rx,
    partition_notify_rx,
    config.engine.persist_check_interval_secs,
));

*persist_task_handle.lock().await = Some(handle);
```

## Key Documentation

- [docs/distributed_architecture_design.md](docs/distributed_architecture_design.md) - Partition Owner model deep dive
- [docs/graphql_ddl_complete_example.md](docs/graphql_ddl_complete_example.md) - Complete DDL reference
- [docs/mcp_integration.md](docs/mcp_integration.md) - GraphQL documentation for AI agents
- [TRACING_GUIDE.md](TRACING_GUIDE.md) - Logging levels and debugging
- [JDBC_CONNECTION_GUIDE.md](JDBC_CONNECTION_GUIDE.md) - MySQL protocol usage

## Version Info

Calm uses git-based versioning:

```rust
// In main.rs
println!("📦 Version: {} ({})",
    version_macro::build_git_branch!(),    // Current branch
    version_macro::build_git_version!());  // Git commit hash
```

Build metadata embedded via [ddl_macros](crates/ddl_macros/) workspace member.
