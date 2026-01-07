// Executor 层
mod information_schema_executor;
pub use information_schema_executor::InformationSchemaExecutor;

// Table Provider 层
pub mod table_provider;

// SQL 工具
pub mod sql_normalizer;

// Federation 层（联邦查询）
pub mod federation;

// UDFs
pub mod udf;

// Re-exports
pub use federation::FederatedQueryExecutor;
pub use sql_normalizer::{
    NormalizedSql, PartitionFilters, ScoreColumnPlacement, ScoreConfig, ScoreLimit, ScoreOrder,
    SqlNormalizer,
};
pub use table_provider::{PartitionTableProvider, UnionTableProvider};
