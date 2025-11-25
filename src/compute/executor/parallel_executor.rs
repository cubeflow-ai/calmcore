/// 并行查询执行器
///
/// 负责执行并行排序查询：
/// - ParallelSortLimit: ORDER BY + LIMIT
/// - ParallelSortStreaming: ORDER BY 无 LIMIT
///
/// 核心特性：
/// - Partition 并行查询
/// - TopK 合并（有 LIMIT）
/// - 流式输出（无 LIMIT）
use std::sync::Arc;

use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::*;

use crate::compute::TopKMerger;
use crate::engine::Engine;
use crate::utils::error::{CoreError, CoreResult};

use super::QueryResult;

pub struct ParallelExecutor {
    engine: Arc<Engine>,
}

impl ParallelExecutor {
    pub fn new(engine: Arc<Engine>) -> Self {
        Self { engine }
    }

    /// 执行并行排序 + LIMIT（ORDER BY + LIMIT）
    pub async fn execute_parallel_sort_limit(
        &self,
        sql: &str,
        table_name: &str,
        sort_fields: Vec<(String, bool)>,
        limit: usize,
        offset: Option<usize>,
    ) -> CoreResult<QueryResult> {
        log::info!(
            "🔀 [ParallelSortLimit] sort_fields={:?}, limit={}, offset={:?}",
            sort_fields,
            limit,
            offset
        );

        let partition_names = self.engine.list_partitions(table_name).await;

        // 计算每个 partition 需要返回的数据量
        let partition_limit = offset.unwrap_or(0) + limit;

        // 🚀 并行查询所有 partition
        let futures: Vec<_> = partition_names
            .iter()
            .map(|partition_name| {
                let table_name = table_name.to_string();
                let partition_name = partition_name.clone();
                let sql = sql.to_string();
                let sort_fields = sort_fields.clone();
                async move {
                    self.execute_on_partition(
                        &table_name,
                        &partition_name,
                        &sql,
                        Some(sort_fields),
                        Some(partition_limit),
                    )
                    .await
                }
            })
            .collect();

        let results = futures::future::join_all(futures).await;

        let mut all_batches = Vec::new();
        for (idx, result) in results.into_iter().enumerate() {
            match result {
                Ok(batches) => all_batches.extend(batches),
                Err(e) => {
                    // SQL syntax errors should fail immediately
                    if Self::is_sql_error(&e) {
                        return Err(e);
                    }
                    // Data errors can be skipped
                    log::warn!("⚠️  Partition {} failed: {}", partition_names[idx], e);
                }
            }
        }

        let total_rows_before: usize = all_batches.iter().map(|b| b.num_rows()).sum();
        let matched_docs = total_rows_before;

        // 应用 TopK 合并
        let final_batch = if all_batches.is_empty() {
            self.create_empty_batch(table_name).await?
        } else {
            self.apply_topk_merge(all_batches, sort_fields, limit, offset)?
        };

        Ok(QueryResult {
            batch: final_batch,
            matched_docs,
        })
    }

    /// 执行并行排序流式查询（ORDER BY 无 LIMIT）
    pub async fn execute_parallel_sort_streaming(
        &self,
        sql: &str,
        table_name: &str,
        sort_fields: Vec<(String, bool)>,
    ) -> CoreResult<QueryResult> {
        log::info!("🌊 [ParallelSortStreaming] sort_fields={:?}", sort_fields);

        let partition_names = self.engine.list_partitions(table_name).await;

        // 🚀 并行查询所有 partition
        let futures: Vec<_> = partition_names
            .iter()
            .map(|partition_name| {
                let table_name = table_name.to_string();
                let partition_name = partition_name.clone();
                let sql = sql.to_string();
                let sort_fields = sort_fields.clone();
                async move {
                    self.execute_on_partition(
                        &table_name,
                        &partition_name,
                        &sql,
                        Some(sort_fields),
                        None, // 无 limit
                    )
                    .await
                }
            })
            .collect();

        let results = futures::future::join_all(futures).await;

        let mut all_batches = Vec::new();
        for (idx, result) in results.into_iter().enumerate() {
            match result {
                Ok(batches) => all_batches.extend(batches),
                Err(e) => {
                    // SQL syntax errors should fail immediately
                    if Self::is_sql_error(&e) {
                        return Err(e);
                    }
                    // Data errors can be skipped
                    log::warn!("⚠️  Partition {} failed: {}", partition_names[idx], e);
                }
            }
        }

        let matched_docs = all_batches.iter().map(|b| b.num_rows()).sum();

        // 合并所有 batches（无 limit）
        let final_batch = if all_batches.is_empty() {
            self.create_empty_batch(table_name).await?
        } else {
            Self::concat_batches(all_batches)?
        };

        Ok(QueryResult {
            batch: final_batch,
            matched_docs,
        })
    }

    // ===== 私有方法 =====

    /// 在单个 partition 上执行查询
    async fn execute_on_partition(
        &self,
        table_name: &str,
        partition_name: &str,
        sql: &str,
        sort_hints: Option<Vec<(String, bool)>>,
        limit_hint: Option<usize>,
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

        let provider = Arc::new(
            crate::compute::PartitionTableProviderWithHints::new_with_hints(
                partition, sort_hints, limit_hint,
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

        Ok(batches)
    }

    /// 应用 TopK 合并
    fn apply_topk_merge(
        &self,
        batches: Vec<RecordBatch>,
        sort_fields: Vec<(String, bool)>,
        limit: usize,
        offset: Option<usize>,
    ) -> CoreResult<RecordBatch> {
        log::info!(
            "🔄 [TopK Merge] Merging {} batches, limit={}, offset={:?}",
            batches.len(),
            limit,
            offset
        );

        let merger = TopKMerger::new(sort_fields, limit, offset);

        let result = merger
            .merge(vec![batches])
            .map_err(|e| CoreError::Internal(format!("Failed to merge results: {}", e)))?;

        log::info!("✅ [TopK Merge] Result: {} rows", result.num_rows());

        Ok(result)
    }

    /// 创建空 RecordBatch
    async fn create_empty_batch(&self, table_name: &str) -> CoreResult<RecordBatch> {
        let partition_names = self.engine.list_partitions(table_name).await;
        let schema = self
            .engine
            .get_partition(table_name, &partition_names[0])
            .await
            .ok_or_else(|| CoreError::NotExisted("No partitions".to_string()))?
            .arrow_schema
            .clone();

        Self::create_empty_batch_with_schema(schema)
    }

    /// 使用给定 schema 创建空 RecordBatch
    fn create_empty_batch_with_schema(
        schema: Arc<datafusion::arrow::datatypes::Schema>,
    ) -> CoreResult<RecordBatch> {
        use datafusion::arrow::array::new_empty_array;

        let empty_columns: Vec<ArrayRef> = schema
            .fields()
            .iter()
            .map(|field| new_empty_array(field.data_type()))
            .collect();

        RecordBatch::try_new(schema, empty_columns)
            .map_err(|e| CoreError::Internal(format!("Failed to create empty RecordBatch: {}", e)))
    }

    /// 合并多个 RecordBatch
    fn concat_batches(batches: Vec<RecordBatch>) -> CoreResult<RecordBatch> {
        if batches.is_empty() {
            return Err(CoreError::Internal("No batches to concat".to_string()));
        }

        if batches.len() == 1 {
            return Ok(batches.into_iter().next().unwrap());
        }

        use datafusion::arrow::compute::concat_batches;
        let schema = batches[0].schema();
        concat_batches(&schema, &batches)
            .map_err(|e| CoreError::Internal(format!("Failed to concat batches: {}", e)))
    }

    /// 判断是否是 SQL 语法错误（应该立即返回给客户端）
    fn is_sql_error(error: &CoreError) -> bool {
        match error {
            CoreError::InvalidParam(msg) => {
                // SQL 语法错误关键词
                msg.contains("Schema error")
                    || msg.contains("No field named")
                    || msg.contains("Query parse error")
                    || msg.contains("parse error")
                    || msg.contains("SQL error")
            }
            _ => false,
        }
    }
}
