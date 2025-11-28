/// 查询优化器模块
///
/// 负责分析查询执行计划,特别是针对 ORDER BY _nature 的深度分页优化
mod plan_analyzer;

pub use plan_analyzer::{
    analyze_query, ExecutionHints, NaturalOrderInfo, PureLimitInfo, QueryPlan, QueryType,
};
