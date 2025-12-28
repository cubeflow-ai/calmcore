# Runtime Panic Fix Summary

## Issue
The query `select count(*) from taxi_trips` was causing a panic: `thread 'tokio-runtime-worker' panicked at 'Cannot start a runtime from within a runtime'`.

## Cause Analysis
The panic was likely caused by implicit creation of a new `tokio::Runtime` inside an existing runtime task. This usually happens when:
1. `tokio::runtime::Runtime::new()` is called.
2. `SessionStateBuilder::new().build()` or `TaskContext::default()` is called, which creates a `RuntimeEnv`. If `RuntimeEnv` defaults to creating a new runtime (or checking for one in a way that conflicts), it triggers the panic.
3. This was happening inside `LazyPartitionExec::load_and_execute`, which runs in a `tokio::spawn`ed task (via `future_stream`).

## Fix Implementation

### 1. `src/compute/table_provider/partition_table_provider.rs`
- Added `scan_partition` method to `PartitionTableProvider`.
- This method allows creating an execution plan without requiring a `Session` object, bypassing the need for `SessionState`.

### 2. `src/compute/lazy_partition_exec.rs`
- **Removed `SessionStateBuilder`**: Instead of creating a temporary `SessionState` (which triggers `RuntimeEnv` creation), we now directly call `scan_partition`.
- **Reused `TaskContext`**: Instead of creating a new `TaskContext::default()` (which also creates a `RuntimeEnv`), we now reuse the `TaskContext` passed from the parent execution.
- **Updated Signatures**: Changed `load_and_execute` to accept `Arc<TaskContext>` to facilitate cloning and reuse.

## Verification
The code now compiles successfully. The removal of nested `RuntimeEnv` creation should resolve the panic.

## Next Steps
Please run the query `select count(*) from taxi_trips` again to verify the fix.
