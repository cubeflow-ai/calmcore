mod aggregation_executor;
mod cursor_pagination;
mod natural_order_executor;
mod parallel_executor;
mod partition_executor;
mod result_merger;
mod scan_executor;
mod serial_executor;
mod sql_utils;

use std::sync::Arc;

use datafusion::arrow::record_batch::RecordBatch;

use crate::compute::optimizer::{analyze_query, QueryType};
use crate::compute::sql_normalizer::SqlNormalizer;
use crate::engine::Engine;
use crate::utils::error::{CoreError, CoreResult};

use aggregation_executor::AggregationExecutor;
use natural_order_executor::NaturalOrderExecutor;
use parallel_executor::ParallelExecutor;
use serial_executor::SerialExecutor;

pub use cursor_pagination::{CursorInfo, CursorPagination};
pub use scan_executor::ScanExecutor;

/// 查询结果
#[derive(Debug)]
pub struct QueryResult {
    /// 查询结果数据
    pub batch: RecordBatch,
    /// 命中的文档数量（在索引中匹配的记录数）
    pub matched_docs: usize,
}

/// 查询执行器（路由器）
///
/// 核心职责：
/// 1. SQL 标准化和查询分析
/// 2. 根据 QueryType 路由到专门的执行器
/// 3. 返回查询结果
///
/// 设计原则：
/// - 扁平化路由，无嵌套分支
/// - 单一职责,只做路由
/// - 执行逻辑由专门的 executor 负责
pub struct Executor {
    serial_executor: SerialExecutor,
    parallel_executor: ParallelExecutor,
    aggregation_executor: AggregationExecutor,
    natural_order_executor: NaturalOrderExecutor,
}

impl Executor {
    /// 创建新的查询执行器
    pub fn new(engine: Arc<Engine>) -> Self {
        Self {
            serial_executor: SerialExecutor::new(engine.clone()),
            parallel_executor: ParallelExecutor::new(engine.clone()),
            aggregation_executor: AggregationExecutor::new(engine.clone()),
            natural_order_executor: NaturalOrderExecutor::new(engine.clone()),
        }
    }

    /// 执行 SQL 查询 - 扁平化路由
    ///
    /// 根据 QueryType 直接路由到对应的执行器，无嵌套判断
    pub async fn execute_sql(&self, sql: &str) -> CoreResult<QueryResult> {
        log::info!("📥 [Executor] Received SQL: {}", sql);

        // 🔧 标准化 SQL：验证语法并转换 MySQL 特有语法
        let (statement, normalized_sql) = SqlNormalizer::normalize(sql)?;

        // 如果 SQL 被转换了，记录日志
        if normalized_sql != sql {
            log::info!("🔄 [SQL Normalized] {} -> {}", sql, normalized_sql);
        }

        // 分析查询，获取执行计划
        let plan = analyze_query(statement);

        // 根据 QueryType 路由到对应的执行器
        match plan {
            Some(plan) => {
                log::info!("🔍 [Query Type] {:?}", plan.query_type);

                match plan.query_type {
                    // ===== 聚合查询 =====
                    QueryType::CountOnly(info) => {
                        if info.has_where {
                            log::info!("🔢 [COUNT] with WHERE clause");
                            self.aggregation_executor
                                .execute_count_with_filter(&normalized_sql, &plan.table_name)
                                .await
                        } else {
                            log::info!("🔢 [COUNT] without WHERE (instant)");
                            self.aggregation_executor
                                .execute_count_only(&plan.table_name)
                                .await
                        }
                    }
                    QueryType::CountWithSingleGroupBy(info) => {
                        log::info!("📊 [COUNT] with GROUP BY: {}", info.group_by_field);
                        self.aggregation_executor
                            .execute_count_with_single_group_by(
                                &normalized_sql,
                                &plan.table_name,
                                &info.group_by_field,
                            )
                            .await
                    }
                    QueryType::GeneralAggregation(info) => {
                        log::info!(
                            "📊 [Aggregation] General query (COUNT={}, SUM={}, AVG={}, MAX={}, MIN={}, GROUP_BY={})",
                            info.has_count, info.has_sum, info.has_avg, info.has_max, info.has_min, info.group_by_count
                        );
                        // 使用聚合执行器处理通用聚合查询
                        self.aggregation_executor
                            .execute_general_aggregation(&normalized_sql, &plan.table_name)
                            .await
                    }

                    // ===== 串行扫描查询 =====
                    QueryType::SerialLimit(info) => {
                        log::info!(
                            "➡️  [Serial] LIMIT {}, has_where={}",
                            info.limit,
                            info.has_where_filter
                        );
                        self.serial_executor
                            .execute_serial_limit(
                                &normalized_sql,
                                &plan.table_name,
                                info.limit,
                                info.offset,
                                info.has_where_filter,
                            )
                            .await
                    }
                    QueryType::SerialFullScan => {
                        log::info!("📄 [Serial] Full scan without LIMIT/ORDER BY");
                        self.serial_executor
                            .execute_serial_full_scan(&normalized_sql, &plan.table_name, None, None)
                            .await
                    }

                    // ===== 并行排序查询 =====
                    QueryType::ParallelSortLimit(info) => {
                        log::info!("🔀 [Parallel] ORDER BY + LIMIT {}", info.limit);
                        self.parallel_executor
                            .execute_parallel_sort_limit(
                                &normalized_sql,
                                &plan.table_name,
                                info.sort_fields,
                                info.limit,
                                info.offset,
                            )
                            .await
                    }
                    QueryType::ParallelSortStreaming(info) => {
                        log::info!("🌊 [Parallel] ORDER BY without LIMIT");
                        self.parallel_executor
                            .execute_parallel_sort_streaming(
                                &normalized_sql,
                                &plan.table_name,
                                info.sort_fields,
                            )
                            .await
                    }

                    // ===== 自然序查询 =====
                    QueryType::NaturalOrder(info) => {
                        log::info!("🌿 [Natural Order] ORDER BY _nature");
                        let result = self
                            .natural_order_executor
                            .execute_natural_order(
                                &normalized_sql,
                                &plan.table_name,
                                info.limit,
                                info.offset.unwrap_or(0),
                                info.where_clause.as_deref(),
                                &info.projection_fields,
                                info.is_select_star,
                            )
                            .await?;

                        // 转换 natural_order_executor::QueryResult 到 executor::QueryResult
                        Ok(QueryResult {
                            batch: result.batch,
                            matched_docs: result.matched_docs,
                        })
                    }
                }
            }
            None => {
                // 无法识别的查询，返回错误
                log::warn!("⚠️  Unrecognized query pattern");
                Err(CoreError::Notsupport(
                    "Unrecognized query pattern".to_string(),
                ))
            }
        }
    }
}
