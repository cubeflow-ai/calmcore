// Executor 层
mod distributed_executor;
mod engine_extension;
mod information_schema_executor;
pub mod natural_order_executor; // 公开给 MySQL protocol 层使用

// datafusion-distributed 集成
mod distributed_channel_resolver;
mod distributed_task_estimator;
mod lazy_partition_exec;
mod lazy_partition_codec;
mod simple_memory_exec;

// Table Provider 层
pub mod table_provider;

// SQL 工具
pub mod sql_normalizer;

use std::sync::Arc;

use crate::engine::Engine;

pub use distributed_executor::DistributedDataFusionExecutor;
pub use engine_extension::{EngineExtension, ENGINE_EXTENSION_KEY};
pub use lazy_partition_exec::LazyPartitionExec;
pub use lazy_partition_codec::LazyPartitionCodec;
pub use simple_memory_exec::SimpleMemoryExec;

// datafusion-distributed exports
pub use distributed_channel_resolver::CalmChannelResolver;
pub use distributed_task_estimator::PartitionAwareTaskEstimator;

// Re-exports
pub use sql_normalizer::{NormalizedSql, PartitionFilters, SqlNormalizer};
pub use table_provider::{PartitionTableProvider, UnionTableProvider};
