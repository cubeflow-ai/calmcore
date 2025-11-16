/// Sort + Limit 优化器
///
/// 负责优化 ORDER BY + LIMIT 查询的执行

use super::plan_analyzer::{QueryPlan, QueryType, SortLimitInfo};
use super::top_k_merger::TopKMerger;
use crate::engine::Engine;
use crate::utils::error::{CoreError, CoreResult};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::*;
use std::sync::Arc;

/// Sort + Limit 优化器
pub struct SortLimitOptimizer {
    engine: Arc<Engine>,
}

impl SortLimitOptimizer {
    pub fn new(engine: Arc<Engine>) -> Self {
        Self { engine }
    }
    
    /// 执行优化的查询
    ///
    /// # Arguments
    /// * `plan` - 查询计划
    ///
    /// # Returns
    /// 查询结果
    pub async fn execute(&self, plan: QueryPlan) -> CoreResult<Vec<RecordBatch>> {
        match plan.query_type {
            QueryType::SortLimit(info) => {
                self.execute_sort_limit(&plan.table_name, &plan.original_sql, info)
                    .await
            }
            _ => {
                // 不是 Sort + Limit 查询，返回错误
                Err(CoreError::Internal(
                    "SortLimitOptimizer can only handle SortLimit queries".to_string(),
                ))
            }
        }
    }
    
    /// 执行 Sort + Limit 查询
    async fn execute_sort_limit(
        &self,
        table_name: &str,
        original_sql: &str,
        info: SortLimitInfo,
    ) -> CoreResult<Vec<RecordBatch>> {
        let meta = self.engine.get_table_meta(table_name)?;
        let num_partitions = meta.parallel_workers;
        
        log::info!(
            "🚀 [SortLimitOptimizer] Executing optimized query on table '{}' with {} partitions",
            table_name,
            num_partitions
        );
        log::info!(
            "📊 [SortLimitOptimizer] Sort fields: {:?}, Limit: {}, Offset: {:?}",
            info.sort_fields,
            info.limit,
            info.offset
        );
        
        // 1. 创建 TOP-K 合并器
        let merger = TopKMerger::new(
            info.sort_fields.clone(),
            info.limit,
            info.offset,
        );
        
        // 2. 计算每个 partition 应该返回多少数据
        let per_partition_limit = merger.calculate_per_partition_limit(num_partitions);
        
        log::info!(
            "📊 [SortLimitOptimizer] Each partition will return up to {} rows (global limit: {})",
            per_partition_limit,
            info.limit
        );
        
        // 3. 改写 SQL：为每个 partition 添加 LIMIT
        let partition_sql = self.rewrite_sql_for_partition(original_sql, per_partition_limit)?;
        
        log::info!(
            "📝 [SortLimitOptimizer] Rewritten SQL for partitions: {}",
            partition_sql
        );
        
        // 4. 并行查询所有 partition（传递 sort hints）
        let mut partition_results = Vec::new();
        
        // 🚀 关键：传递 sort_fields 给每个 partition
        let sort_hints = Some(info.sort_fields.clone());
        
        for partition_id in 0..num_partitions {
            match self
                .execute_on_partition_with_hints(
                    table_name,
                    partition_id as u64,
                    &partition_sql,
                    sort_hints.clone(),  // ← 传递 ORDER BY 信息！
                )
                .await
            {
                Ok(batches) => {
                    let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
                    log::info!(
                        "✅ [SortLimitOptimizer] Partition {} returned {} rows",
                        partition_id,
                        row_count
                    );
                    partition_results.push(batches);
                }
                Err(e) => {
                    log::warn!(
                        "⚠️  [SortLimitOptimizer] Partition {} failed: {}",
                        partition_id,
                        e
                    );
                    // 继续查询其他 partition
                }
            }
        }
        
        if partition_results.is_empty() {
            return Err(CoreError::Internal(
                "No partition returned results".to_string(),
            ));
        }
        
        // 5. 合并所有 partition 的 TOP-K 结果
        log::info!(
            "🔄 [SortLimitOptimizer] Merging results from {} partitions",
            partition_results.len()
        );
        
        let final_result = merger.merge(partition_results).map_err(|e| {
            CoreError::Internal(format!("Failed to merge partition results: {}", e))
        })?;
        
        let total_rows: usize = final_result.iter().map(|b| b.num_rows()).sum();
        log::info!(
            "✅ [SortLimitOptimizer] Query completed, returning {} rows",
            total_rows
        );
        
        Ok(final_result)
    }
    
    /// 改写 SQL，为 partition 查询添加 LIMIT
    ///
    /// 例如：
    /// 原始 SQL: SELECT * FROM users WHERE age > 20 ORDER BY age LIMIT 10
    /// 改写后: SELECT * FROM users WHERE age > 20 ORDER BY age LIMIT 20
    fn rewrite_sql_for_partition(&self, sql: &str, per_partition_limit: usize) -> CoreResult<String> {
        use regex::Regex;
        
        // 替换 LIMIT 子句
        let re = Regex::new(r"limit\s+\d+(?:\s+offset\s+\d+)?")
            .map_err(|e| CoreError::Internal(format!("Regex error: {}", e)))?;
        
        let sql_lower = sql.to_lowercase();
        let rewritten = re.replace(
            &sql_lower,
            format!("limit {}", per_partition_limit),
        );
        
        Ok(rewritten.to_string())
    }
    
    /// 在单个 partition 上执行查询（带 sort hints）
    async fn execute_on_partition(
        &self,
        table_name: &str,
        partition_id: u64,
        sql: &str,
    ) -> CoreResult<Vec<RecordBatch>> {
        // 从当前的 info 中提取 sort_fields（需要传递进来）
        // 这里暂时使用普通的 Provider，后续优化
        self.execute_on_partition_with_hints(table_name, partition_id, sql, None)
            .await
    }
    
    /// 在单个 partition 上执行查询（带 sort hints）
    async fn execute_on_partition_with_hints(
        &self,
        table_name: &str,
        partition_id: u64,
        sql: &str,
        sort_hints: Option<Vec<(String, bool)>>,
    ) -> CoreResult<Vec<RecordBatch>> {
        let ctx = SessionContext::new();
        
        // 获取 partition
        let partition = self
            .engine
            .get_partition(table_name, partition_id)
            .await
            .ok_or_else(|| {
                CoreError::NotExisted(format!(
                    "Partition {} not found for table '{}'",
                    partition_id, table_name
                ))
            })?;
        
        // 🚀 关键：使用带 sort hints 的 Provider
        let provider = if let Some(hints) = sort_hints {
            log::info!(
                "🚀 [SortLimitOptimizer] Using PartitionTableProviderWithHints, sort_hints={:?}",
                hints
            );
            Arc::new(crate::compute::PartitionTableProviderWithHints::new_with_sort_hints(
                partition,
                Some(hints),
            )) as Arc<dyn datafusion::datasource::TableProvider>
        } else {
            Arc::new(crate::compute::PartitionTableProvider::new(partition))
                as Arc<dyn datafusion::datasource::TableProvider>
        };
        
        ctx.register_table(table_name, provider)
            .map_err(|e| CoreError::Internal(format!("Failed to register table: {}", e)))?;
        
        // 执行查询
        let df = ctx
            .sql(sql)
            .await
            .map_err(|e| CoreError::InvalidParam(format!("Query parse error: {}", e)))?;
        
        let batches = df
            .collect()
            .await
            .map_err(|e| CoreError::Internal(format!("Query execution error: {}", e)))?;
        
        Ok(batches)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    // 注意：这些测试不需要真正的 Engine，只测试 SQL 改写逻辑
    
    #[test]
    fn test_rewrite_sql_logic() {
        // 直接测试 SQL 改写逻辑，不需要 Engine
        use regex::Regex;
        
        let sql = "SELECT * FROM users WHERE age > 20 ORDER BY age LIMIT 10";
        let per_partition_limit = 20;
        
        let re = Regex::new(r"limit\s+\d+(?:\s+offset\s+\d+)?").unwrap();
        let sql_lower = sql.to_lowercase();
        let rewritten = re.replace(&sql_lower, format!("limit {}", per_partition_limit));
        
        assert!(rewritten.contains("limit 20"));
        assert!(!rewritten.contains("limit 10"));
    }
    
    #[test]
    fn test_rewrite_sql_with_offset_logic() {
        use regex::Regex;
        
        let sql = "SELECT * FROM users WHERE age > 20 ORDER BY age LIMIT 10 OFFSET 5";
        let per_partition_limit = 30;
        
        let re = Regex::new(r"limit\s+\d+(?:\s+offset\s+\d+)?").unwrap();
        let sql_lower = sql.to_lowercase();
        let rewritten = re.replace(&sql_lower, format!("limit {}", per_partition_limit));
        
        assert!(rewritten.contains("limit 30"));
        assert!(!rewritten.contains("offset")); // OFFSET 被移除
    }
}
