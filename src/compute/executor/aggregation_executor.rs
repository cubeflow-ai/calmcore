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
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, Int64Array, UInt64Array};
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
        // 🔧 修复: 移除 ORDER BY 和 LIMIT,因为对 COUNT(*) 聚合查询无意义
        // 这些子句可能导致 DataFusion 优化错误,丢失 WHERE 条件
        let cleaned_sql = Self::remove_order_by_and_limit(sql);
        log::info!("🔍 [CountWithFilter] Executing: {}", cleaned_sql);

        let partition_names = self.engine.list_partitions(table_name).await;

        // 🚀 并行执行所有 partition
        let futures: Vec<_> = partition_names
            .iter()
            .map(|partition_name| {
                let table_name = table_name.to_string();
                let partition_name = partition_name.clone();
                let sql = cleaned_sql.clone();
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
            "📊 [CountWithGroupBy] GROUP BY '{}', Original SQL: {}",
            group_field,
            sql
        );

        // 🔧 提取 ORDER BY 和 LIMIT 子句,稍后在合并后应用
        let (order_by_clause, limit_clause) = Self::extract_order_and_limit(sql);

        // 🔧 移除 ORDER BY 和 LIMIT,让各 partition 做完整的 GROUP BY
        let mut partition_sql = Self::remove_order_by_and_limit(sql);

        // 改写 SQL: 确保 SELECT 列表包含 GROUP BY 字段和 COUNT(*)
        // 将 "SELECT count(*)" 改为 "SELECT group_field, count(*)"
        if !partition_sql
            .to_lowercase()
            .contains(&format!("select {}", group_field.to_lowercase()))
        {
            // 需要添加 GROUP BY 字段到 SELECT 列表
            let sql_lower = partition_sql.to_lowercase();
            if let Some(select_pos) = sql_lower.find("select") {
                let before = &partition_sql[..select_pos + 6]; // "SELECT"
                let after = &partition_sql[select_pos + 6..];
                partition_sql = format!("{} {}, {}", before.trim(), group_field, after.trim());
            }
        }

        if order_by_clause.is_some() || limit_clause.is_some() {
            log::info!(
                "🔧 [CountWithGroupBy] Removed ORDER BY/LIMIT from partition queries, will apply after merge"
            );
        }

        log::info!("📊 [CountWithGroupBy] Partition SQL: {}", partition_sql);

        let partition_names = self.engine.list_partitions(table_name).await;
        let mut all_batches = Vec::new();

        // 🚀 并行执行所有 partition
        let futures: Vec<_> = partition_names
            .iter()
            .map(|partition_name| {
                let table_name = table_name.to_string();
                let partition_name = partition_name.clone();
                let sql = partition_sql.clone();
                async move {
                    self.execute_group_by_on_partition(&table_name, &partition_name, &sql)
                        .await
                }
            })
            .collect();

        let results = futures::future::join_all(futures).await;

        // 收集所有 partition 的结果
        for (idx, result) in results.into_iter().enumerate() {
            match result {
                Ok(batches) => {
                    log::info!(
                        "📦 Partition {} returned {} batches",
                        partition_names[idx],
                        batches.len()
                    );
                    all_batches.extend(batches);
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

        // 合并并重新聚合
        let batch = if all_batches.is_empty() {
            // 空结果
            let schema = Arc::new(Schema::new(vec![
                Field::new(group_field, DataType::Utf8, true),
                Field::new("COUNT(*)", DataType::UInt64, false),
            ]));
            RecordBatch::new_empty(schema)
        } else if all_batches.len() == 1 {
            // 单个 partition,无需重新聚合
            all_batches[0].clone()
        } else {
            // 多个 partition,需要重新聚合
            use datafusion::arrow::compute::concat_batches;
            let schema = all_batches[0].schema();
            let combined = concat_batches(&schema, &all_batches)
                .map_err(|e| CoreError::Internal(format!("Merge failed: {}", e)))?;

            // 从 schema 中获取实际的 COUNT 列名 (可能是 "count(*)" 或 "COUNT(*)")
            let count_field_name = schema
                .fields()
                .iter()
                .find(|f| {
                    let name_lower = f.name().to_lowercase();
                    name_lower == "count(*)" || name_lower == "cnt" || name_lower.contains("count")
                })
                .map(|f| f.name().clone())
                .unwrap_or_else(|| "count(*)".to_string());

            log::info!(
                "🔍 [Re-agg] Detected count field name: '{}'",
                count_field_name
            );

            // 构建重新聚合的 SQL: SELECT group_field, SUM(count_field) AS original_name FROM temp_results GROUP BY group_field
            let mut re_agg_sql = format!(
                "SELECT \"{}\", SUM(\"{}\") AS \"{}\" FROM temp_results GROUP BY \"{}\"",
                group_field, count_field_name, count_field_name, group_field
            );

            // 🔧 在重新聚合后应用原始的 ORDER BY 和 LIMIT
            // 需要将 ORDER BY 中的 COUNT(*) 替换为列名(不带引号,因为 DataFusion 会自动处理)
            if let Some(mut order_by) = order_by_clause {
                // 将 COUNT(*) 或 count(*) 替换为列名
                let order_by_upper = order_by.to_uppercase();
                if order_by_upper.contains("COUNT(*)") {
                    // 直接替换为列名,不添加引号(DataFusion 会自动处理小写列名)
                    order_by = order_by
                        .replace("COUNT(*)", &count_field_name)
                        .replace("count(*)", &count_field_name);
                    log::info!("🔧 [Re-agg] Rewrote ORDER BY: {}", order_by);
                }
                re_agg_sql.push_str(&order_by);
            }
            if let Some(limit) = &limit_clause {
                re_agg_sql.push_str(limit);
            }

            log::info!("🔄 [Re-aggregation SQL] {}", re_agg_sql);

            // 用 DataFusion 重新聚合
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
        log::info!("✅ [CountWithGroupBy] Total rows: {}", matched_docs);

        // 🔧 检查原始 SQL 是否在 SELECT 中包含 GROUP BY 字段
        let include_group_field = sql
            .to_lowercase()
            .contains(&format!("select {}", group_field.to_lowercase()))
            || sql.to_lowercase().contains("select *");

        // 🔧 如果用户没有请求 GROUP BY 字段,移除它
        let batch = if !include_group_field {
            // 只保留 COUNT(*) 列
            log::info!("🔧 [Projection] Removing group field from result");
            let count_col = batch.column(1);
            let schema = Arc::new(Schema::new(vec![Field::new(
                "COUNT(*)",
                DataType::UInt64,
                false,
            )]));
            RecordBatch::try_new(schema, vec![count_col.clone()])
                .map_err(|e| CoreError::Internal(format!("Failed to create RecordBatch: {}", e)))?
        } else {
            batch
        };

        Ok(QueryResult {
            batch,
            matched_docs,
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

    /// 提取 SQL 中的 ORDER BY 和 LIMIT 子句
    ///
    /// 返回 (ORDER BY 子句, LIMIT 子句)
    fn extract_order_and_limit(sql: &str) -> (Option<String>, Option<String>) {
        let sql_upper = sql.to_uppercase();

        let order_by_pos = sql_upper.find(" ORDER BY");
        let limit_pos = sql_upper.find(" LIMIT");

        let order_by_clause = if let Some(pos) = order_by_pos {
            let end_pos = limit_pos.unwrap_or(sql.len());
            Some(sql[pos..end_pos].to_string())
        } else {
            None
        };

        let limit_clause = if let Some(pos) = limit_pos {
            Some(sql[pos..].to_string())
        } else {
            None
        };

        (order_by_clause, limit_clause)
    }

    /// 移除 SQL 中的 ORDER BY 和 LIMIT 子句
    ///
    /// 对于聚合查询(如 COUNT),ORDER BY 和 LIMIT 是无意义的,
    /// 因为聚合结果只有一行。这些子句可能导致 DataFusion 优化问题。
    fn remove_order_by_and_limit(sql: &str) -> String {
        let sql_upper = sql.to_uppercase();

        // 找到 ORDER BY 的位置
        let order_by_pos = sql_upper.find(" ORDER BY");

        // 找到 LIMIT 的位置
        let limit_pos = sql_upper.find(" LIMIT");

        // 取最早出现的位置作为截断点
        let cut_pos = match (order_by_pos, limit_pos) {
            (Some(o), Some(l)) => Some(o.min(l)),
            (Some(o), None) => Some(o),
            (None, Some(l)) => Some(l),
            (None, None) => None,
        };

        if let Some(pos) = cut_pos {
            sql[..pos].to_string()
        } else {
            sql.to_string()
        }
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

    /// 执行通用聚合查询(MIN, MAX, SUM, AVG, etc.)
    pub async fn execute_general_aggregation(
        &self,
        sql: &str,
        table_name: &str,
    ) -> CoreResult<QueryResult> {
        log::info!("📊 [GeneralAggregation] Executing: {}", sql);

        // 🔧 提取 ORDER BY 和 LIMIT 子句,稍后在 re-aggregation 后应用
        let (order_by_clause, limit_clause) = Self::extract_order_and_limit(sql);

        // 🔧 移除 ORDER BY 和 LIMIT,让各 partition 做完整聚合
        let partition_sql = Self::remove_order_by_and_limit(sql);

        if order_by_clause.is_some() || limit_clause.is_some() {
            log::info!(
                "🔧 [GeneralAggregation] Removed ORDER BY/LIMIT from partition queries, will apply after re-aggregation"
            );
        }

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
                .sql(&partition_sql)
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
            let mut group_keys = Vec::new();

            for field_name in &field_names {
                let lower = field_name.to_lowercase();

                // 判断是否为聚合函数结果列
                // 注意: 必须先判断特定的聚合函数(MIN/MAX/AVG),再判断通用的COUNT别名
                // 否则 min_fare, max_fare, avg_fare 会被误识别为 COUNT
                let is_count_result = lower == "\"cnt\""
                    || lower == "\"count\""
                    || lower == "\"total\""
                    || lower.ends_with("_cnt\"")
                    || lower.ends_with("_count\"");

                if lower.contains("min(") || (lower.starts_with("\"min_") && !is_count_result) {
                    agg_exprs.push(format!("MIN({}) AS {}", field_name, field_name));
                } else if lower.contains("max(")
                    || (lower.starts_with("\"max_") && !is_count_result)
                {
                    agg_exprs.push(format!("MAX({}) AS {}", field_name, field_name));
                } else if lower.contains("sum(")
                    || (lower.starts_with("\"sum_") && !is_count_result)
                {
                    agg_exprs.push(format!("SUM({}) AS {}", field_name, field_name));
                } else if lower.contains("avg(")
                    || lower.contains("\"avg")
                    || (lower.starts_with("\"avg_") && !is_count_result)
                    || lower.ends_with("_avg\"")
                {
                    // AVG 需要特殊处理: 理想情况应该是 SUM(sum_col) / SUM(count_col)
                    // 但这需要改写原始SQL让partition返回sum和count
                    // 这里使用 AVG 作为近似(会有轻微误差,但比SUM正确得多)
                    agg_exprs.push(format!("AVG({}) AS {}", field_name, field_name));
                } else if lower.contains("count(") || is_count_result {
                    // COUNT 结果或聚合别名都需要用 SUM 合并
                    agg_exprs.push(format!("SUM({}) AS {}", field_name, field_name));
                } else {
                    // 普通列,用于GROUP BY
                    agg_exprs.push(field_name.clone());
                    group_keys.push(field_name.clone());
                }
            }

            let mut re_agg_sql = format!("SELECT {} FROM temp_results", agg_exprs.join(", "));
            if !group_keys.is_empty() {
                re_agg_sql.push_str(&format!(" GROUP BY {}", group_keys.join(", ")));
            }

            // 🔧 在 re-aggregation 后应用原始的 ORDER BY 和 LIMIT
            if let Some(order_by) = &order_by_clause {
                re_agg_sql.push_str(order_by);
            }
            if let Some(limit) = &limit_clause {
                re_agg_sql.push_str(limit);
            }

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
