/// 查询优化器模块
///
/// 负责分析和优化查询执行计划，特别是针对 ORDER BY + LIMIT 的场景
///
/// 采用高效的字符串解析，无正则表达式依赖，生产级别实现
mod plan_analyzer;
mod top_k_merger;

pub use plan_analyzer::{analyze_query, QueryPlan, QueryType, SortLimitInfo};
pub use top_k_merger::TopKMerger;
