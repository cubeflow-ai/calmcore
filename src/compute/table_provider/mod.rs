/// Table Provider 层
///
/// 负责为 DataFusion 提供数据源，包含所有核心优化
pub(crate) mod partition_table_provider; // 允许 compute 模块内部访问
pub(crate) mod segment_scanner; // 允许 compute 模块内部访问
mod union_table;

pub use partition_table_provider::{create_multi_segment_exec, PartitionTableProvider};
pub use union_table::UnionTableProvider;
