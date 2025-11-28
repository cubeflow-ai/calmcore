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
use datafusion::logical_expr::LogicalPlan;
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
        log::info!("🔍 [CountWithFilter] Original SQL: {}", sql);

        let partition_names = self.engine.list_partitions(table_name).await;

        // 🚀 并行执行所有 partition
        let futures: Vec<_> = partition_names
            .iter()
            .map(|partition_name| {
                let table_name = table_name.to_string();
                let partition_name = partition_name.clone();
                let sql = sql.to_string();
                async move {
                    self.execute_count_on_partition_safe(&table_name, &partition_name, &sql)
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

        let partition_names = self.engine.list_partitions(table_name).await;
        let mut all_batches = Vec::new();

        // 🚀 并行执行所有 partition (使用原始 SQL,在 partition 内部用 LogicalPlan 处理)
        let futures: Vec<_> = partition_names
            .iter()
            .map(|partition_name| {
                let table_name = table_name.to_string();
                let partition_name = partition_name.clone();
                let sql = sql.to_string();
                async move {
                    self.execute_group_by_on_partition_safe(&table_name, &partition_name, &sql)
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

            log::info!("🔄 [Re-aggregation] Merging {} batches", all_batches.len());

            // 🔧 使用 DataFusion 重新聚合
            let ctx = SessionContext::new();
            let provider =
                datafusion::datasource::MemTable::try_new(schema.clone(), vec![vec![combined]])
                    .map_err(|e| CoreError::Internal(format!("MemTable failed: {}", e)))?;

            ctx.register_table("temp_results", Arc::new(provider))
                .map_err(|e| CoreError::Internal(format!("Register failed: {}", e)))?;

            // 获取字段名
            let field_names: Vec<String> =
                schema.fields().iter().map(|f| f.name().clone()).collect();

            if field_names.len() < 2 {
                return Err(CoreError::Internal(
                    "Schema must have at least 2 fields".to_string(),
                ));
            }

            let group_col = &field_names[0];
            let count_col = &field_names[1];

            // 构建基本的重新聚合 SQL
            let mut re_agg_sql = format!(
                "SELECT \"{}\" AS \"{}\", SUM(\"{}\") AS \"{}\" FROM temp_results GROUP BY \"{}\"",
                group_col, group_col, count_col, count_col, group_col
            );

            // 🔧 直接从原始 SQL 字符串提取 ORDER BY 和 LIMIT (简单且可靠)
            let sql_upper = sql.to_uppercase();
            if let Some(order_pos) = sql_upper.find(" ORDER BY") {
                let order_clause = &sql[order_pos..];
                // 找到 LIMIT 的位置
                if let Some(limit_pos) = order_clause.to_uppercase().find(" LIMIT") {
                    re_agg_sql.push_str(&order_clause[..limit_pos]);
                    re_agg_sql.push_str(&order_clause[limit_pos..]);
                } else {
                    re_agg_sql.push_str(order_clause);
                }
            } else if let Some(limit_pos) = sql_upper.find(" LIMIT") {
                re_agg_sql.push_str(&sql[limit_pos..]);
            }

            log::info!("🔄 [Re-aggregation SQL] {}", re_agg_sql);

            let re_agg_df = ctx
                .sql(&re_agg_sql)
                .await
                .map_err(|e| CoreError::Internal(format!("Re-agg SQL failed: {}", e)))?;

            let final_batches = re_agg_df
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

    /// 在单个 partition 上安全地执行 GROUP BY 查询
    ///
    /// 使用 LogicalPlan 操作移除 ORDER BY 和 LIMIT,避免字符串操作的风险。
    async fn execute_group_by_on_partition_safe(
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

        // 🔧 使用 LogicalPlan 安全地移除 ORDER BY 和 LIMIT
        let df = ctx
            .sql(sql)
            .await
            .map_err(|e| CoreError::InvalidParam(format!("Query parse error: {}", e)))?;

        let plan = df.logical_plan().clone();
        let cleaned_plan = Self::remove_sort_and_limit_from_plan(plan);

        let cleaned_df = DataFrame::new(ctx.state(), cleaned_plan);

        let batches = cleaned_df
            .collect()
            .await
            .map_err(|e| CoreError::Internal(format!("Query execution error: {}", e)))?;

        Ok(batches)
    }

    /// 递归移除 LogicalPlan 中的 Sort 和 Limit 节点
    ///
    /// 这个方法会递归遍历 LogicalPlan 树,移除顶层的 Sort 和 Limit 节点。
    /// 与字符串操作不同,这种方式不会影响子查询中的 ORDER BY/LIMIT。
    fn remove_sort_and_limit_from_plan(plan: LogicalPlan) -> LogicalPlan {
        match plan {
            LogicalPlan::Sort(sort) => {
                // 跳过 Sort 节点,继续处理其输入
                Self::remove_sort_and_limit_from_plan((*sort.input).clone())
            }
            LogicalPlan::Limit(limit) => {
                // 跳过 Limit 节点,继续处理其输入
                Self::remove_sort_and_limit_from_plan((*limit.input).clone())
            }
            // 其他节点保持不变
            _ => plan,
        }
    }

    /// 在单个 partition 上安全地执行 COUNT(*) 查询
    ///
    /// 使用 LogicalPlan 操作移除 ORDER BY 和 LIMIT,避免字符串操作的风险。
    /// 对于 COUNT(*) 聚合查询,ORDER BY 和 LIMIT 是无意义的。
    async fn execute_count_on_partition_safe(
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

        // 🔧 使用 LogicalPlan 安全地移除 ORDER BY 和 LIMIT
        let df = ctx
            .sql(sql)
            .await
            .map_err(|e| CoreError::InvalidParam(format!("Query parse error: {}", e)))?;

        let plan = df.logical_plan().clone();
        let cleaned_plan = Self::remove_sort_and_limit_from_plan(plan);

        let cleaned_df = DataFrame::new(ctx.state(), cleaned_plan);

        let batches = cleaned_df
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

    /// 执行通用聚合查询(MIN, MAX, SUM, AVG, etc.)
    ///
    /// 🚀 新架构：使用 UnionTableProvider + RecordHub + Stream
    /// - 完全交给 DataFusion 处理查询优化和执行
    /// - 自动并行扫描所有 partition
    /// - 流式处理 + 自动背压控制
    /// - 代码更简洁，性能更好
    pub async fn execute_general_aggregation(
        &self,
        sql: &str,
        table_name: &str,
        _info: &crate::compute::optimizer::AggregationInfo,
    ) -> CoreResult<QueryResult> {
        log::info!("📊 [GeneralAggregation] Executing with UnionTable: {}", sql);

        // 1. 获取所有 partition
        let partition_names = self.engine.list_partitions(table_name).await;
        let mut partitions = Vec::new();

        for partition_name in partition_names {
            if let Some(partition) = self.engine.get_partition(table_name, &partition_name).await {
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
            "📦 [GeneralAggregation] Found {} partitions",
            partitions.len()
        );

        // 2. 创建 UnionTableProvider (无需 RecordHub, 使用 DataFusion 内置的 UnionExec)
        let union_table = crate::compute::UnionTableProvider::new(partitions)?;

        // 3. 创建 DataFusion SessionContext 并注册表
        let ctx = SessionContext::new();
        ctx.register_table(table_name, Arc::new(union_table))
            .map_err(|e| CoreError::Internal(format!("Failed to register union table: {}", e)))?;

        log::info!("✅ [GeneralAggregation] UnionTable registered, executing SQL...");

        // 5. 直接执行原始 SQL，让 DataFusion 自动优化和执行
        // 不需要手动处理 ORDER BY/LIMIT/聚合重组
        // DataFusion 会自动：
        // - 优化查询计划
        // - 并行扫描数据
        // - 正确处理聚合、排序、限制
        let df = ctx
            .sql(sql)
            .await
            .map_err(|e| CoreError::Internal(format!("Query failed: {}", e)))?;

        let batches = df
            .collect()
            .await
            .map_err(|e| CoreError::Internal(format!("Collect failed: {}", e)))?;

        log::info!(
            "📊 [GeneralAggregation] Query returned {} batches",
            batches.len()
        );

        // 6. 返回结果
        let batch = if batches.is_empty() {
            // 空结果
            let schema = Arc::new(Schema::new(vec![Field::new(
                "result",
                DataType::Int64,
                true,
            )]));
            RecordBatch::new_empty(schema)
        } else if batches.len() == 1 {
            batches[0].clone()
        } else {
            // 多个 batch，需要合并
            use datafusion::arrow::compute::concat_batches;
            let schema = batches[0].schema();
            concat_batches(&schema, &batches)
                .map_err(|e| CoreError::Internal(format!("Merge failed: {}", e)))?
        };

        let matched_docs = batch.num_rows();

        log::info!("✅ [GeneralAggregation] Completed: {} rows", matched_docs);

        Ok(QueryResult {
            batch,
            matched_docs,
        })
    }
}
