//! DataFusion-based executor with optimized parallelism
//!
//! 使用 DataFusion 作为查询执行引擎,简化代码:
//! - 利用 DataFusion 的查询优化器
//! - 自动处理并行执行
//! - 保留所有 SegmentScanner 的优化
//! - 为未来分布式执行做准备

use std::sync::Arc;

use datafusion::prelude::*;

use super::{natural_order_executor::NaturalOrderExecutor, QueryResult};
use crate::compute::{SqlNormalizer, UnionTableProvider};
use crate::engine::Engine;
use crate::utils::error::{CoreError, CoreResult};

/// DataFusion-based executor
///
/// 核心理念:
/// - DataFusion 处理查询优化、并行、聚合
/// - 我们只提供 TableProvider (包含所有优化)
/// - 代码量从 3000+ 行减少到 < 100 行
pub struct DataFusionExecutor {
    engine: Arc<Engine>,
    natural_order_executor: NaturalOrderExecutor,
}

impl DataFusionExecutor {
    pub fn new(engine: Arc<Engine>) -> Self {
        Self {
            natural_order_executor: NaturalOrderExecutor::new(engine.clone()),
            engine,
        }
    }

    /// 执行 SQL 查询
    ///
    /// 简单到令人惊讶:
    /// 1. 检查是否是特殊查询 (ORDER BY _nature)
    /// 2. 标准化 SQL (修复时间戳比较等)
    /// 3. 创建 DataFusion SessionContext
    /// 4. 注册我们的 TableProvider
    /// 5. 执行查询 - DataFusion 自动处理一切!
    pub async fn execute_sql(&self, sql: &str) -> CoreResult<QueryResult> {
        log::info!("🚀 [DataFusion Executor] Executing SQL: {}", sql);

        // 🔧 标准化 SQL：修复 MySQL 特有语法和类型不匹配
        let (_statement, normalized_sql) = SqlNormalizer::normalize(sql)?;

        if normalized_sql != sql {
            log::info!("🔄 [SQL Normalized] {} -> {}", sql, normalized_sql);
        }

        // 🌿 特殊处理: ORDER BY _nature (深度分页优化)
        let normalized_upper = normalized_sql.to_uppercase();
        if normalized_upper.contains("ORDER BY _NATURE")
            || normalized_upper.contains("ORDER BY `_NATURE`")
        {
            log::info!("🌿 [Natural Order] Detected ORDER BY _nature, using optimized executor");
            return self.execute_natural_order(&normalized_sql).await;
        }

        // 从 SQL 中提取表名 (简化版本,生产环境需要更健壮的解析)
        let table_name = self.extract_table_name(&normalized_sql)?;

        // 创建 DataFusion SessionContext
        // 自动使用所有 CPU 核心进行并行执行
        let config = SessionConfig::new().with_target_partitions(32); // 32 路并行 (4 partition × 8 segment)

        let ctx = SessionContext::new_with_config(config);

        // 获取所有 partition
        let partition_names = self.engine.list_partitions(&table_name).await;
        let mut partitions = Vec::new();

        for partition_name in partition_names {
            if let Some(partition) = self
                .engine
                .get_partition(&table_name, &partition_name)
                .await
            {
                partitions.push(partition);
            }
        }

        if partitions.is_empty() {
            return Err(CoreError::NotExisted(format!(
                "No partitions found for table '{}'",
                table_name
            )));
        }

        log::info!(
            "📦 [DataFusion Executor] Found {} partitions for table '{}'",
            partitions.len(),
            table_name
        );

        // 创建 UnionTableProvider (包含所有优化!)
        let union_table = UnionTableProvider::new(partitions)
            .map_err(|e| CoreError::Internal(format!("Failed to create UnionTable: {}", e)))?;

        // 注册到 DataFusion
        ctx.register_table(&table_name, Arc::new(union_table))
            .map_err(|e| CoreError::Internal(format!("Failed to register table: {}", e)))?;

        log::info!("✅ [DataFusion Executor] Table registered, executing query...");

        // 执行查询 - DataFusion 自动:
        // - 分析查询计划
        // - 下推 filter/projection/limit 到我们的 TableProvider
        // - 并行执行 (调用我们的 SegmentScanner 优化)
        // - 处理 shuffle 和聚合
        // - 返回结果
        let df = ctx
            .sql(&normalized_sql)
            .await
            .map_err(|e| CoreError::Internal(format!("Query execution error: {}", e)))?;

        // 收集结果
        let batches = df
            .collect()
            .await
            .map_err(|e| CoreError::Internal(format!("Failed to collect results: {}", e)))?;

        log::info!(
            "✅ [DataFusion Executor] Query completed, {} batches returned",
            batches.len()
        );

        // 合并所有 batches 成单个 RecordBatch
        use datafusion::arrow::compute::concat_batches;

        let batch = if batches.is_empty() {
            // 返回空结果
            use datafusion::arrow::array::RecordBatch as ArrowRecordBatch;
            use datafusion::arrow::datatypes::Schema;
            ArrowRecordBatch::new_empty(std::sync::Arc::new(Schema::empty()))
        } else if batches.len() == 1 {
            batches.into_iter().next().unwrap()
        } else {
            let schema = batches[0].schema();
            concat_batches(&schema, &batches)
                .map_err(|e| CoreError::Internal(format!("Failed to concat batches: {}", e)))?
        };

        // TODO: 实现 matched_docs 统计 (需要从 SegmentScanner 传递上来)
        let matched_docs = batch.num_rows();

        Ok(QueryResult {
            batch,
            matched_docs,
        })
    }

    /// 执行 ORDER BY _nature 查询 (深度分页优化)
    async fn execute_natural_order(&self, sql: &str) -> CoreResult<QueryResult> {
        // 直接调用 natural_order_executor，它会自己解析 SQL
        let result = self
            .natural_order_executor
            .execute_natural_order(sql)
            .await?;

        Ok(QueryResult {
            batch: result.batch,
            matched_docs: result.matched_docs,
        })
    }

    /// 从 SQL 中提取表名 (简化版本)
    fn extract_table_name(&self, sql: &str) -> CoreResult<String> {
        let sql_upper = sql.to_uppercase();

        // 查找 FROM 关键字
        if let Some(from_pos) = sql_upper.find(" FROM ") {
            let after_from = &sql[from_pos + 6..].trim();

            // 提取第一个单词作为表名
            let table_name = after_from
                .split_whitespace()
                .next()
                .unwrap_or("")
                .trim_matches(|c: char| !c.is_alphanumeric() && c != '_');

            if !table_name.is_empty() {
                return Ok(table_name.to_string());
            }
        }

        Err(CoreError::InvalidParam(
            "Could not extract table name from SQL".to_string(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_table_name() {
        let executor = DataFusionExecutor::new(Arc::new(Engine::new()));

        assert_eq!(
            executor.extract_table_name("SELECT * FROM users").unwrap(),
            "users"
        );
        assert_eq!(
            executor
                .extract_table_name("SELECT COUNT(*) FROM taxi_trips WHERE x > 10")
                .unwrap(),
            "taxi_trips"
        );
        assert_eq!(
            executor
                .extract_table_name("SELECT a, b FROM my_table GROUP BY a")
                .unwrap(),
            "my_table"
        );
    }
}
