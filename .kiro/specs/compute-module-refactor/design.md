# Design Document

## Overview

This document describes the design for refactoring the compute module structure. The refactoring will rename `distributed_executor` to `executor` and `query_optimizer` to `optimizer`, while maintaining all existing functionality and public APIs.

The refactoring is purely structural - no logic changes will be made. All existing tests and functionality will continue to work without modification.

## Architecture

### Current Structure

```
src/compute/
├── mod.rs
├── distributed_executor/
│   ├── mod.rs
│   ├── executor.rs          # Main distributed executor
│   ├── aggregation.rs       # Aggregation merger
│   └── query_builder.rs     # SQL query builder
├── query_optimizer/
│   ├── mod.rs
│   ├── plan_analyzer.rs     # Query plan analysis
│   ├── sort_limit_optimizer.rs  # Sort+Limit optimization
│   └── top_k_merger.rs      # TOP-K result merging
├── partition_table_provider.rs
└── segment_scanner.rs
```

### Target Structure

```
src/compute/
├── mod.rs
├── executor/
│   ├── mod.rs
│   ├── distributed.rs       # Main distributed executor (renamed from executor.rs)
│   ├── aggregation.rs       # Aggregation merger (unchanged)
│   └── query_builder.rs     # SQL query builder (unchanged)
├── optimizer/
│   ├── mod.rs
│   ├── plan_analyzer.rs     # Query plan analysis (unchanged)
│   ├── sort_limit.rs        # Sort+Limit optimization (renamed from sort_limit_optimizer.rs)
│   └── top_k_merger.rs      # TOP-K result merging (unchanged)
├── partition_table_provider.rs
└── segment_scanner.rs
```

## Components and Interfaces

### 1. Compute Module (src/compute/mod.rs)

**Current Exports:**
```rust
pub mod distributed_executor;
pub mod query_optimizer;
mod partition_table_provider;
mod segment_scanner;

pub use distributed_executor::{DistributedExecutor, QueryResult};
pub use partition_table_provider::PartitionTableProvider;
pub use query_optimizer::{analyze_query, QueryPlan, QueryType, SortLimitOptimizer, TopKMerger};
```

**New Exports:**
```rust
pub mod executor;
pub mod optimizer;
mod partition_table_provider;
mod segment_scanner;

pub use executor::{DistributedExecutor, QueryResult};
pub use partition_table_provider::PartitionTableProvider;
pub use optimizer::{analyze_query, QueryPlan, QueryType, SortLimitOptimizer, TopKMerger};
```

**Changes:**
- Rename `distributed_executor` module to `executor`
- Rename `query_optimizer` module to `optimizer`
- Update re-exports to use new module names
- All public types remain the same

### 2. Executor Module (src/compute/executor/)

**Current Structure:**
```rust
// src/compute/distributed_executor/mod.rs
mod aggregation;
mod executor;
mod query_builder;

pub use executor::{DistributedExecutor, QueryResult};
```

**New Structure:**
```rust
// src/compute/executor/mod.rs
mod aggregation;
mod distributed;
mod query_builder;

pub use distributed::{DistributedExecutor, QueryResult};
```

**Changes:**
- Rename `executor.rs` to `distributed.rs` for clarity
- Update mod.rs to import from `distributed` instead of `executor`
- All internal imports remain the same

**Files:**
- `distributed.rs` (renamed from `executor.rs`): Contains `DistributedExecutor` and `QueryResult`
- `aggregation.rs` (unchanged): Contains `AggregationMerger`
- `query_builder.rs` (unchanged): Contains `QueryBuilder`

### 3. Optimizer Module (src/compute/optimizer/)

**Current Structure:**
```rust
// src/compute/query_optimizer/mod.rs
mod plan_analyzer;
mod sort_limit_optimizer;
mod top_k_merger;

pub use plan_analyzer::{analyze_query, QueryPlan, QueryType};
pub use sort_limit_optimizer::SortLimitOptimizer;
pub use top_k_merger::TopKMerger;
```

**New Structure:**
```rust
// src/compute/optimizer/mod.rs
mod plan_analyzer;
mod sort_limit;
mod top_k_merger;

pub use plan_analyzer::{analyze_query, QueryPlan, QueryType};
pub use sort_limit::SortLimitOptimizer;
pub use top_k_merger::TopKMerger;
```

**Changes:**
- Rename `sort_limit_optimizer.rs` to `sort_limit.rs` for brevity
- Update mod.rs to import from `sort_limit` instead of `sort_limit_optimizer`
- All internal imports remain the same

**Files:**
- `plan_analyzer.rs` (unchanged): Contains query analysis logic
- `sort_limit.rs` (renamed from `sort_limit_optimizer.rs`): Contains `SortLimitOptimizer`
- `top_k_merger.rs` (unchanged): Contains `TopKMerger`

### 4. Internal Import Updates

**In src/compute/executor/distributed.rs:**

Current:
```rust
use crate::compute::query_optimizer::{analyze_query, QueryType, SortLimitOptimizer};
```

New:
```rust
use crate::compute::optimizer::{analyze_query, QueryType, SortLimitOptimizer};
```

## Data Models

No data model changes are required. All structs, enums, and types remain identical:

- `DistributedExecutor`
- `QueryResult`
- `AggregationMerger`
- `QueryBuilder`
- `QueryPlan`
- `QueryType`
- `SortLimitInfo`
- `RangeCondition`
- `SortLimitOptimizer`
- `TopKMerger`

## Migration Strategy

### Phase 1: Rename Directories
1. Rename `src/compute/distributed_executor/` to `src/compute/executor/`
2. Rename `src/compute/query_optimizer/` to `src/compute/optimizer/`

### Phase 2: Rename Files
1. Rename `src/compute/executor/executor.rs` to `src/compute/executor/distributed.rs`
2. Rename `src/compute/optimizer/sort_limit_optimizer.rs` to `src/compute/optimizer/sort_limit.rs`

### Phase 3: Update Module Declarations
1. Update `src/compute/mod.rs` to use new module names
2. Update `src/compute/executor/mod.rs` to import from `distributed`
3. Update `src/compute/optimizer/mod.rs` to import from `sort_limit`

### Phase 4: Update Internal Imports
1. Update `src/compute/executor/distributed.rs` to import from `optimizer` instead of `query_optimizer`

### Phase 5: Verification
1. Run `cargo check` to verify compilation
2. Run `cargo test` to verify all tests pass
3. Verify no external code is broken (all public APIs remain the same)

## Error Handling

No error handling changes are required. This is a pure refactoring with no logic changes.

## Testing Strategy

### Compilation Testing
- Run `cargo check` after each phase to ensure no compilation errors
- Verify all module paths are correctly updated

### Functional Testing
- Run existing test suite with `cargo test`
- All existing tests should pass without modification
- No new tests are required as no functionality is changing

### Integration Testing
- Verify that external code using the compute module continues to work
- Check that all public exports are accessible with the same names
- Ensure examples and demos continue to compile and run

## Backward Compatibility

The refactoring maintains full backward compatibility at the public API level:

- All public types (`DistributedExecutor`, `QueryResult`, etc.) remain accessible
- All public functions (`analyze_query`, etc.) remain accessible
- Module re-exports ensure external code doesn't need to change

The only changes are internal module organization, which is not visible to external consumers of the compute module.

## Implementation Notes

### File Operations Required

1. **Directory Renames:**
   - `mv src/compute/distributed_executor src/compute/executor`
   - `mv src/compute/query_optimizer src/compute/optimizer`

2. **File Renames:**
   - `mv src/compute/executor/executor.rs src/compute/executor/distributed.rs`
   - `mv src/compute/optimizer/sort_limit_optimizer.rs src/compute/optimizer/sort_limit.rs`

3. **File Edits:**
   - `src/compute/mod.rs`: Update module declarations and re-exports
   - `src/compute/executor/mod.rs`: Update module declaration for `distributed`
   - `src/compute/optimizer/mod.rs`: Update module declaration for `sort_limit`
   - `src/compute/executor/distributed.rs`: Update import path for optimizer

### Potential Issues

1. **Git History:** Directory and file renames may affect git history tracking
   - Solution: Use `git mv` commands to preserve history

2. **IDE Caching:** IDEs may cache old module paths
   - Solution: Restart IDE or rebuild project index after refactoring

3. **Compilation Order:** Rust may need clean rebuild after directory renames
   - Solution: Run `cargo clean` before `cargo check`

## Rationale

### Why Rename `distributed_executor` to `executor`?

1. **Redundancy:** The module is already under `compute`, so "distributed" is implied
2. **Brevity:** Shorter names are easier to work with
3. **Clarity:** The main struct is `DistributedExecutor`, so the module name `executor` is clear

### Why Rename `query_optimizer` to `optimizer`?

1. **Consistency:** Matches the pattern of `executor` (no redundant prefix)
2. **Brevity:** Shorter import paths
3. **Clarity:** It's obvious that an optimizer in the compute module is for queries

### Why Rename `executor.rs` to `distributed.rs`?

1. **Avoid Confusion:** Having `executor/executor.rs` is redundant
2. **Descriptive:** `distributed.rs` clearly indicates it's the distributed execution implementation
3. **Extensibility:** Leaves room for other executor types (e.g., `local.rs`, `parallel.rs`)

### Why Rename `sort_limit_optimizer.rs` to `sort_limit.rs`?

1. **Brevity:** The module is already called `optimizer`, so the suffix is redundant
2. **Consistency:** Matches the pattern of other files in the module
3. **Clarity:** The file contains `SortLimitOptimizer`, so the purpose is clear
