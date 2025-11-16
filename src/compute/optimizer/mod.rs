/// 查询优化器模块
///
/// 负责分析和优化查询执行计划，特别是针对 ORDER BY + LIMIT 的场景

mod plan_analyzer;
mod sort_limit;
mod top_k_merger;

pub use plan_analyzer::{analyze_query, QueryPlan, QueryType, SortLimitInfo};
pub use sort_limit::SortLimitOptimizer;
pub use top_k_merger::TopKMerger;
