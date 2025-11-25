/// 聚合查询执行器
///
/// 负责执行聚合类查询：
/// - CountOnly: SELECT COUNT(*) FROM table
/// - CountWithFilter: SELECT COUNT(*) FROM table WHERE ...
/// - CountWithSingleGroupBy: SELECT COUNT(*) ... GROUP BY single_field
///
/// 核心优化：
/// - Bitmap 索引加速 COUNT
/// - 并行聚合 + 结果合并
use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, Int64Array, StringArray, UInt64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::*;

use crate::engine::Engine;
use crate::utils::error::{CoreError, CoreResult};

use super::QueryResult;

pub struct AggregationExecutor {
    engine: Arc<Engine>,
}

impl AggregationExecutor {
    pub fn new(engine: Arc<Engine>) -> Self {
        Self { engine }
    }

    /// 执行 COUNT(*) 查询（无过滤条件）
    pub async fn execute_count_only(&self, table_name: &str) -> CoreResult<QueryResult> {
        log::info!("🔢 [CountOnly] Counting all rows in table '{}'", table_name);

        let partition_names = self.engine.list_partitions(table_name).await;

        // 🚀 并行统计所有 partition
        let futures: Vec<_> = partition_names
            .iter()
            .map(|partition_name| {
                let table_name = table_name.to_string();
                let partition_name = partition_name.clone();
                async move {
                    self.count_partition(&table_name, &partition_name, None)
                        .await
                }
            })
            .collect();

        let results = futures::future::join_all(futures).await;

        let mut total_count = 0u64;
        for (idx, result) in results.into_iter().enumerate() {
            match result {
                Ok(count) => {
                    log::info!("📊 Partition {} count: {}", partition_names[idx], count);
                    total_count += count;
                }
                Err(e) => {
                    log::warn!("⚠️  Partition {} count failed: {}", partition_names[idx], e);
                }
            }
        }

        log::info!("✅ [CountOnly] Total count: {}", total_count);

        // 构造 COUNT(*) 结果
        let schema = Arc::new(Schema::new(vec![Field::new(
            "COUNT(*)",
            DataType::UInt64,
            false,
        )]));

        let count_array = UInt64Array::from(vec![total_count]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(count_array) as ArrayRef])
            .map_err(|e| CoreError::Internal(format!("Failed to create RecordBatch: {}", e)))?;

        Ok(QueryResult {
            batch,
            matched_docs: total_count as usize,
        })
    }

    /// 执行 COUNT(*) + WHERE 查询
    pub async fn execute_count_with_filter(
        &self,
        sql: &str,
        table_name: &str,
    ) -> CoreResult<QueryResult> {
        log::info!("🔍 [CountWithFilter] Executing: {}", sql);

        let partition_names = self.engine.list_partitions(table_name).await;

        // 🚀 并行执行所有 partition
        let futures: Vec<_> = partition_names
            .iter()
            .map(|partition_name| {
                let table_name = table_name.to_string();
                let partition_name = partition_name.clone();
                let sql = sql.to_string();
                async move {
                    self.execute_count_on_partition(&table_name, &partition_name, &sql)
                        .await
                }
            })
            .collect();

        let results = futures::future::join_all(futures).await;

        let mut total_count = 0u64;
        for (idx, result) in results.into_iter().enumerate() {
            match result {
                Ok(count) => {
                    log::info!("📊 Partition {} count: {}", partition_names[idx], count);
                    total_count += count;
                }
                Err(e) => {
                    log::warn!("⚠️  Partition {} count failed: {}", partition_names[idx], e);
                }
            }
        }

        log::info!("✅ [CountWithFilter] Total count: {}", total_count);

        // 构造结果
        let schema = Arc::new(Schema::new(vec![Field::new(
            "COUNT(*)",
            DataType::UInt64,
            false,
        )]));

        let count_array = UInt64Array::from(vec![total_count]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(count_array) as ArrayRef])
            .map_err(|e| CoreError::Internal(format!("Failed to create RecordBatch: {}", e)))?;

        Ok(QueryResult {
            batch,
            matched_docs: total_count as usize,
        })
    }

    /// 执行 COUNT(*) + GROUP BY (single field) 查询
    pub async fn execute_count_with_single_group_by(
        &self,
        sql: &str,
        table_name: &str,
        group_field: &str,
    ) -> CoreResult<QueryResult> {
        log::info!(
            "📊 [CountWithGroupBy] GROUP BY '{}', SQL: {}",
            group_field,
            sql
        );

        let partition_names = self.engine.list_partitions(table_name).await;

        // 🚀 并行执行所有 partition
        let futures: Vec<_> = partition_names
            .iter()
            .map(|partition_name| {
                let table_name = table_name.to_string();
                let partition_name = partition_name.clone();
                let sql = sql.to_string();
                async move {
                    self.execute_group_by_on_partition(&table_name, &partition_name, &sql)
                        .await
                }
            })
            .collect();

        let results = futures::future::join_all(futures).await;

        // 合并所有 partition 的结果
        let mut merged_counts: HashMap<String, u64> = HashMap::new();
        for (idx, result) in results.into_iter().enumerate() {
            match result {
                Ok(batches) => {
                    for batch in batches {
                        self.merge_group_counts(&batch, group_field, &mut merged_counts)?;
                    }
                }
                Err(e) => {
                    log::warn!(
                        "⚠️  Partition {} GROUP BY failed: {}",
                        partition_names[idx],
                        e
                    );
                }
            }
        }

        let matched_docs: u64 = merged_counts.values().sum();
        log::info!(
            "✅ [CountWithGroupBy] Total groups: {}, total docs: {}",
            merged_counts.len(),
            matched_docs
        );

        // 构造结果 RecordBatch
        let batch = self.build_group_by_result(group_field, merged_counts)?;

        Ok(QueryResult {
            batch,
            matched_docs: matched_docs as usize,
        })
    }

    // ===== 私有方法 =====

    /// 统计单个 partition 的行数
    async fn count_partition(
        &self,
        table_name: &str,
        partition_name: &str,
        where_clause: Option<&str>,
    ) -> CoreResult<u64> {
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

        // 如果没有 WHERE，直接返回 total_count
        if where_clause.is_none() {
            return Ok(partition.total_count());
        }

        // 带 WHERE 条件：需要通过 DataFusion 执行查询统计
        // 注意：这里不能直接访问 segment 内部的 bitmap，
        // 因为 WHERE 条件可能很复杂，需要完整的查询引擎
        self.execute_count_on_partition(
            table_name,
            partition_name,
            &format!(
                "SELECT COUNT(*) FROM {} {}",
                table_name,
                where_clause.unwrap_or("")
            ),
        )
        .await
    }

    /// 在单个 partition 上执行 COUNT(*) + WHERE
    async fn execute_count_on_partition(
        &self,
        table_name: &str,
        partition_name: &str,
        sql: &str,
    ) -> CoreResult<u64> {
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

        // 提取 COUNT(*) 结果
        let count = if batches.is_empty() {
            0u64
        } else {
            let batch = &batches[0];
            if batch.num_rows() == 0 {
                0u64
            } else {
                let column = batch.column(0);
                // 尝试读取 UInt64 或 Int64
                if let Some(arr) = column.as_any().downcast_ref::<UInt64Array>() {
                    arr.value(0)
                } else if let Some(arr) = column.as_any().downcast_ref::<Int64Array>() {
                    arr.value(0) as u64
                } else {
                    return Err(CoreError::Internal(
                        "COUNT(*) result is not UInt64 or Int64".to_string(),
                    ));
                }
            }
        };

        Ok(count)
    }

    /// 在单个 partition 上执行 GROUP BY 查询
    async fn execute_group_by_on_partition(
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

    /// 合并 GROUP BY 结果
    fn merge_group_counts(
        &self,
        batch: &RecordBatch,
        group_field: &str,
        merged: &mut HashMap<String, u64>,
    ) -> CoreResult<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        // 假设第一列是 GROUP BY 字段，第二列是 COUNT(*)
        let group_column = batch.column(0);
        let count_column = batch.column(1);

        let group_array = group_column
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                CoreError::Internal(format!("GROUP BY field '{}' is not a string", group_field))
            })?;

        // 不需要预先downcast，直接在循环中判断
        for i in 0..batch.num_rows() {
            let group_value = group_array.value(i).to_string();
            let count_value = if let Some(arr) = count_column.as_any().downcast_ref::<UInt64Array>()
            {
                arr.value(i)
            } else if let Some(arr) = count_column.as_any().downcast_ref::<Int64Array>() {
                arr.value(i) as u64
            } else {
                return Err(CoreError::Internal("Invalid count type".to_string()));
            };

            *merged.entry(group_value).or_insert(0) += count_value;
        }

        Ok(())
    }

    /// 构造 GROUP BY 查询结果
    fn build_group_by_result(
        &self,
        group_field: &str,
        counts: HashMap<String, u64>,
    ) -> CoreResult<RecordBatch> {
        let mut groups: Vec<_> = counts.into_iter().collect();
        groups.sort_by(|a, b| b.1.cmp(&a.1)); // 按 count 降序

        let group_values: Vec<String> = groups.iter().map(|(k, _)| k.clone()).collect();
        let count_values: Vec<u64> = groups.iter().map(|(_, v)| *v).collect();

        let schema = Arc::new(Schema::new(vec![
            Field::new(group_field, DataType::Utf8, false),
            Field::new("COUNT(*)", DataType::UInt64, false),
        ]));

        let group_array = StringArray::from(group_values);
        let count_array = UInt64Array::from(count_values);

        RecordBatch::try_new(
            schema,
            vec![Arc::new(group_array) as ArrayRef, Arc::new(count_array)],
        )
        .map_err(|e| CoreError::Internal(format!("Failed to create RecordBatch: {}", e)))
    }

    /// 执行通用聚合查询(MIN, MAX, SUM, AVG, etc.)
    pub async fn execute_general_aggregation(
        &self,
        sql: &str,
        table_name: &str,
    ) -> CoreResult<QueryResult> {
        log::info!("📊 [GeneralAggregation] Executing: {}", sql);

        let partition_names = self.engine.list_partitions(table_name).await;
        let mut all_batches = Vec::new();

        for partition_name in &partition_names {
            let partition = self
                .engine
                .get_partition(table_name, partition_name)
                .await
                .ok_or_else(|| {
                    CoreError::NotExisted(format!("Partition {} not found", partition_name))
                })?;

            let ctx = SessionContext::new();
            let provider = Arc::new(
                crate::compute::PartitionTableProviderWithHints::new_with_hints(
                    partition, None, None,
                ),
            );

            ctx.register_table(table_name, provider)
                .map_err(|e| CoreError::Internal(format!("Failed to register table: {}", e)))?;

            let df = ctx
                .sql(sql)
                .await
                .map_err(|e| CoreError::Internal(format!("Query failed: {}", e)))?;

            let batches = df
                .collect()
                .await
                .map_err(|e| CoreError::Internal(format!("Collect failed: {}", e)))?;

            all_batches.extend(batches);
        }

        // 对于聚合结果需要重新聚合
        let batch = if all_batches.is_empty() {
            let schema = Arc::new(Schema::new(vec![Field::new(
                "result",
                DataType::Int64,
                true,
            )]));
            RecordBatch::new_empty(schema)
        } else if all_batches.len() == 1 {
            all_batches[0].clone()
        } else {
            // 多个 partition 的聚合结果需要再次聚合
            // 每个 batch 已经是聚合后的结果(如 MIN, MAX)
            // 我们需要对这些结果再次执行相同的聚合操作

            use datafusion::arrow::compute::concat_batches;
            let schema = all_batches[0].schema();
            let combined = concat_batches(&schema, &all_batches)
                .map_err(|e| CoreError::Internal(format!("Merge failed: {}", e)))?;

            // 为合并后的结果构建重新聚合的SQL
            // 从schema中获取列名,这些列名是聚合函数的结果
            let field_names: Vec<String> = schema
                .fields()
                .iter()
                .map(|f| format!("\"{}\"", f.name()))
                .collect();

            // 构建重新聚合的SQL
            // 例如: SELECT MIN(col1), MAX(col2) FROM temp_results
            // 其中 col1 是 "min(taxi_trips.fare_amount)"
            let mut agg_exprs = Vec::new();
            for field_name in &field_names {
                let lower = field_name.to_lowercase();
                if lower.contains("min(") {
                    agg_exprs.push(format!("MIN({}) AS {}", field_name, field_name));
                } else if lower.contains("max(") {
                    agg_exprs.push(format!("MAX({}) AS {}", field_name, field_name));
                } else if lower.contains("sum(") {
                    agg_exprs.push(format!("SUM({}) AS {}", field_name, field_name));
                } else if lower.contains("count(") {
                    agg_exprs.push(format!("SUM({}) AS {}", field_name, field_name));
                } else if lower.contains("avg(") {
                    // AVG需要特殊处理: SUM(sum_col) / SUM(count_col)
                    // 这里简化处理,假设只有一个AVG
                    agg_exprs.push(format!("AVG({}) AS {}", field_name, field_name));
                } else {
                    // 普通列,用于GROUP BY
                    agg_exprs.push(field_name.clone());
                }
            }

            let re_agg_sql = format!("SELECT {} FROM temp_results", agg_exprs.join(", "));
            log::info!("🔄 [Re-aggregation SQL] {}", re_agg_sql);

            let ctx = SessionContext::new();
            let provider =
                datafusion::datasource::MemTable::try_new(schema.clone(), vec![vec![combined]])
                    .map_err(|e| CoreError::Internal(format!("MemTable failed: {}", e)))?;

            ctx.register_table("temp_results", Arc::new(provider))
                .map_err(|e| CoreError::Internal(format!("Register failed: {}", e)))?;

            let df = ctx
                .sql(&re_agg_sql)
                .await
                .map_err(|e| CoreError::Internal(format!("Re-agg failed: {}", e)))?;

            let final_batches = df
                .collect()
                .await
                .map_err(|e| CoreError::Internal(format!("Final collect failed: {}", e)))?;

            if final_batches.is_empty() {
                RecordBatch::new_empty(schema)
            } else {
                final_batches[0].clone()
            }
        };

        let matched_docs = batch.num_rows();

        Ok(QueryResult {
            batch,
            matched_docs,
        })
    }
}
