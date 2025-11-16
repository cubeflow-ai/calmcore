# Requirements Document

## Introduction

This document outlines the requirements for refactoring the compute module structure to improve naming clarity and module organization. The refactoring will rename `distributed_executor` to `executor` and `query_optimizer` to `optimizer`, while maintaining all existing functionality.

## Glossary

- **Compute Module**: The top-level module responsible for query execution and optimization
- **Executor Module**: The module handling distributed query execution (formerly `distributed_executor`)
- **Optimizer Module**: The module handling query plan optimization (formerly `query_optimizer`)
- **DistributedExecutor**: The main struct for executing queries across partitions
- **QueryResult**: The result type returned by query execution
- **SortLimitOptimizer**: Component that optimizes ORDER BY + LIMIT queries
- **TopKMerger**: Component that merges TOP-K results from multiple partitions

## Requirements

### Requirement 1

**User Story:** As a developer, I want clearer module names in the compute directory, so that the code structure is more intuitive and less redundant.

#### Acceptance Criteria

1. WHEN the compute module is examined, THE Compute Module SHALL expose an `executor` submodule instead of `distributed_executor`
2. WHEN the compute module is examined, THE Compute Module SHALL expose an `optimizer` submodule instead of `query_optimizer`
3. THE Compute Module SHALL maintain all existing public APIs without breaking changes
4. THE Compute Module SHALL preserve all internal file names within the renamed modules

### Requirement 2

**User Story:** As a developer, I want the executor module to have a clearer internal structure, so that I can easily understand the execution components.

#### Acceptance Criteria

1. THE Executor Module SHALL contain a `distributed.rs` file (renamed from `executor.rs`)
2. THE Executor Module SHALL maintain `aggregation.rs` and `query_builder.rs` files unchanged
3. THE Executor Module SHALL export `DistributedExecutor` and `QueryResult` types
4. WHEN code imports from the executor module, THE Executor Module SHALL provide the same public interface as before

### Requirement 3

**User Story:** As a developer, I want the optimizer module to have clearer file naming, so that the purpose of each file is immediately apparent.

#### Acceptance Criteria

1. THE Optimizer Module SHALL contain a `sort_limit.rs` file (renamed from `sort_limit_optimizer.rs`)
2. THE Optimizer Module SHALL maintain `plan_analyzer.rs` and `top_k_merger.rs` files unchanged
3. THE Optimizer Module SHALL export all existing types: `analyze_query`, `QueryPlan`, `QueryType`, `SortLimitOptimizer`, `TopKMerger`
4. WHEN code imports from the optimizer module, THE Optimizer Module SHALL provide the same public interface as before

### Requirement 4

**User Story:** As a developer, I want all existing code that uses the compute module to continue working, so that the refactoring doesn't break any functionality.

#### Acceptance Criteria

1. THE Compute Module SHALL update all internal imports to use the new module names
2. THE Compute Module SHALL maintain backward compatibility for all public exports
3. WHEN external code imports from `compute`, THE Compute Module SHALL provide all previously exported types and functions
4. THE Compute Module SHALL compile without errors after the refactoring
5. WHEN the refactoring is complete, THE Compute Module SHALL pass all existing tests
