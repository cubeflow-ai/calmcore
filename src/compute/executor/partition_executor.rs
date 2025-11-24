/// 单 Partition 执行器
///
/// 负责在单个 partition 上执行查询

use std::sync::Arc;

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::*;

use crate::engine::Engine;
use crate::utils::error::{CoreError, CoreResult};

/// Partition 执行器
pub struct PartitionExecutor {
    engine: Arc<Engine>,
}

impl PartitionExecutor {
    pub fn new(engine: Arc<Engine>) -> Self {
        Self { engine }
    }

    /// 在单个 partition 上执行查询
    ///
    /// # 参数
    /// * `table_name` - 表名
    /// * `partition_name` - 分区名
    /// * `sql` - SQL 查询（已清理虚拟字段）
    /// * `sort_hints` - 排序提示
    /// * `limit_hint` - LIMIT 提示（用于下推优化）
    pub async fn execute_on_partition(
        &self,
        table_name: &str,
        partition_name: &str,
        sql: &str,
        sort_hints: Option<Vec<(String, bool)>>,
        limit_hint: Option<usize>,
    ) -> CoreResult<Vec<RecordBatch>> {
        log::info!(
            "🔧 [PartitionExecutor] partition={}, sql='{}', sort_hints={:?}, limit_hint={:?}",
            partition_name,
            sql,
            sort_hints,
            limit_hint
        );

        let ctx = SessionContext::new();

        let partition = self
            .engine
            .get_partition(table_name, partition_name)
            .await
            .ok_or_else(|| {
                CoreError::NotExisted(format!(
                    "Partition {} not found for table '{}'",
                    partition_name, table_name
                ))
            })?;

        // 🔧 使用带 hints 的 Provider（sort 和 limit 都是 Option，自动传递）
        let provider = Arc::new(
            crate::compute::PartitionTableProviderWithHints::new_with_hints(
                partition,
                sort_hints,
                limit_hint,
            ),
        );

        ctx.register_table(table_name, provider)
            .map_err(|e| CoreError::Internal(format!("Failed to register table: {}", e)))?;

        let df = ctx
            .sql(sql)
            .await
            .map_err(|e| CoreError::InvalidParam(format!("Query parse error: {}", e)))?;

        let batches = df
            .collect()
            .await
            .map_err(|e| CoreError::Internal(format!("Query execution error: {}", e)))?;

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        log::info!(
            "✅ [PartitionExecutor] partition={}, returned {} batches with {} rows",
            partition_name,
            batches.len(),
            total_rows
        );

        Ok(batches)
    }

    /// 在单个 partition 上执行查询（旧版本，用于聚合）
    pub async fn execute_sql_on_partition(
        &self,
        table_name: &str,
        partition_name: &str,
        sql: &str,
    ) -> CoreResult<Vec<RecordBatch>> {
        let ctx = SessionContext::new();

        let partition = self
            .engine
            .get_partition(table_name, partition_name)
            .await
            .ok_or_else(|| {
                CoreError::NotExisted(format!(
                    "Partition {} not found for table '{}'",
                    partition_name, table_name
                ))
            })?;

        let provider = Arc::new(crate::compute::PartitionTableProvider::new(partition));

        ctx.register_table(table_name, provider)
            .map_err(|e| CoreError::Internal(format!("Failed to register table: {}", e)))?;

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
