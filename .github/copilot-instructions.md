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
├── engine/     - Local partition operations (split into metadata, lifecycle, persist, data_operations)
├── router/     - Partition routing logic (PKHash, Hash, Range, Custom)
├── storage/    - Partition, segment, router abstractions
├── compute/    - Query execution (DataFusion integration)
└── protocol/   - Multi-protocol servers (GraphQL, MySQL, ES)
```

**Critical**: `CalmService` is the top-level coordinator. `Engine` handles local data operations. `Router` determines partition destination.

### Data Flow

1. **Write Path**: Client → Protocol Layer → CalmService → Router (determine partition) → ClusterManager (locate owner) → Partition Owner → Engine::insert_batch → Segment
2. **Read Path**: Unified gRPC/Arrow Flight for distributed queries. Any node can read any partition from shared storage.

### Key Modules

- **Catalog** ([src/catalog/mod.rs](src/catalog/mod.rs)): Manages `TableMeta` and `PartitionMeta`. Single source of truth for schema.
- **ClusterManager** ([src/cluster/mod.rs](src/cluster/mod.rs)): Gossip-based coordination. Node IDs: `timestamp_host_grpc_port`.
- **Engine** ([src/engine/mod.rs](src/engine/mod.rs)): Refactored into submodules. Handles local partition lifecycle and persistence.
- **Router** ([src/router/mod.rs](src/router/mod.rs)): Unified routing logic for all protocols.
- **Partition** ([src/storage/partition/mod.rs](src/storage/partition/mod.rs)): In-memory segment management, primary key deduplication.

## DDL Philosophy

**GraphQL is the ONLY DDL interface**. MySQL/ES protocols provide data operations but NOT schema creation.

### Creating Tables

Use GraphQL mutation `createTable`. See `docs/mcp_integration.md` for detailed MCP-friendly documentation.

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
- **Internal**: `grpc_port` is auto-assigned (starting 52000) if not specified. No separate `internal_port` or `flight_port`.

### Testing

**Shell scripts** (root directory):
- `test_connection.sh` - MySQL JDBC connectivity
- `test_graphql_ddl.sh` - GraphQL table creation
- `test_partition_filter.sh` - Partition pruning logic
- `test_router_architecture.sh` - Router logic verification

**Rust tests**:
```bash
cargo test                          # Unit tests
cargo test --test test_timestamp_field  # Specific integration test
```

## Code Conventions

### Error Handling

Use `CoreResult<T>` and `CoreError` from [src/utils/error.rs](src/utils/error.rs).

### Schema Evolution

Update `FieldOption` in [src/catalog/schema/field.rs](src/catalog/schema/field.rs) for all 13 field types when adding options.

### Partition Key Encoding

- **Partition Key**: `partition:table_name:partition_name:version`
- **Node ID**: `timestamp_host_grpc_port` (e.g., `20231225120530123_127.0.0.1_52000`)

### Full-Text Indexing

Unified posting list architecture in [src/storage/segment/fulltext/](src/storage/segment/fulltext/).

## Dependencies

- **DataFusion 51.0**: Query engine.
- **Chitchat 0.9**: Gossip protocol.
- **Tonic/Arrow Flight**: Unified gRPC for control and data plane.
- **msql-srv (patched)**: MySQL wire protocol.

## Common Patterns

### Accessing Services

```rust
// In protocol handlers
let calm = ctx.data::<Arc<CalmService>>()?;
let engine = calm.engine();
// Use Engine::insert_batch for unified routing and insertion
let stats = engine.insert_batch(&table_name, batch).await?;
```
