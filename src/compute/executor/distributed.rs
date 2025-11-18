use std::sync::Arc;

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::*;

use crate::compute::optimizer::{analyze_query, QueryType};
use crate::compute::PartitionTableProvider;
use crate::engine::Engine;
use crate::utils::error::{CoreError, CoreResult};

use super::aggregation::AggregationMerger;
use super::query_builder::QueryBuilder;

/// 查询结果
#[derive(Debug)]
pub struct QueryResult {
    /// 查询结果的数据批次
    pub batches: Vec<RecordBatch>,
}

/// 分布式查询执行器
///
/// 负责协调多个 partition 的查询执行和结果合并
pub struct DistributedExecutor {
    engine: Arc<Engine>,
    query_builder: QueryBuilder,
    aggregation_merger: AggregationMerger,
}

impl DistributedExecutor {
    /// 创建新的分布式查询执行器
    pub fn new(engine: Arc<Engine>) -> Self {
        Self {
            engine: engine.clone(),
            query_builder: QueryBuilder::new(engine.clone()),
            aggregation_merger: AggregationMerger::new(),
        }
    }

    /// 执行 SQL 查询
    ///
    /// 根据查询类型分为两个分支：
    /// 1. 聚合查询 → execute_aggregation_query
    /// 2. 普通查询 → execute_query
    pub async fn execute_sql(&self, sql: &str) -> CoreResult<QueryResult> {
        log::info!(
            "📥 [DistributedExecutor::execute_sql] Received SQL: {}",
            sql
        );

        let plan = analyze_query(sql);

        if let Some(ref p) = &plan {
            log::info!(
                "🔍 [DistributedExecutor::execute_sql] Query type: {:?}",
                p.query_type
            );
        }

        // 分支 1：聚合查询（需要特殊合并）
        if let Some(ref p) = plan {
            if matches!(p.query_type, QueryType::Aggregation) {
                log::info!("📊 [DistributedExecutor::execute_sql] Executing aggregation query");
                return self.execute_aggregation_query(sql).await;
            }
        }

        // 分支 2：普通查询
        // 提取需要的参数：sort_fields 和 sort_limit_info
        let (sort_fields, sort_limit_info) = if let Some(p) = plan {
            match p.query_type {
                QueryType::SortLimit(info) => (Some(info.sort_fields.clone()), Some(info)),
                _ => (None, None),
            }
        } else {
            // 即使没有识别为 SortLimit,也尝试提取 ORDER BY 字段
            (self.extract_order_by_fields(sql), None)
        };

        self.execute_query(sql, sort_fields, sort_limit_info).await
    }

    /// 从 SQL 中提取 ORDER BY 字段 (简单解析,用于 hints 下发)
    fn extract_order_by_fields(&self, sql: &str) -> Option<Vec<(String, bool)>> {
        let sql_lower = sql.to_lowercase();

        // 查找 ORDER BY 子句
        if let Some(order_by_start) = sql_lower.find("order by") {
            // 🔧 修复：在小写版本中切片，避免字节索引错误
            let after_order_by = &sql_lower[order_by_start + 8..]; // "order by".len() == 8

            // 找到下一个 SQL 关键字或结束
            let order_clause = after_order_by
                .split_whitespace()
                .take_while(|w| !w.starts_with("limit") && !w.starts_with("offset"))
                .collect::<Vec<_>>()
                .join(" ");

            // 解析字段和方向
            let mut fields = Vec::new();
            for part in order_clause.split(',') {
                let tokens: Vec<&str> = part.trim().split_whitespace().collect();
                if !tokens.is_empty() {
                    let field_name = tokens[0].to_lowercase(); // 🔧 统一转小写
                    let ascending = tokens
                        .get(1)
                        .map(|s| *s != "desc") // 🔧 已经是小写，直接比较
                        .unwrap_or(true);
                    fields.push((field_name, ascending));
                }
            }

            if !fields.is_empty() {
                return Some(fields);
            }
        }

        None
    }

    /// 执行普通查询（非聚合）
    ///
    /// # 参数
    /// * `sql` - SQL 查询语句
    /// * `sort_fields` - ORDER BY 字段（用于下发给 SegmentScanner）
    /// * `sort_limit_info` - 完整的 sort + limit 信息（用于最终合并）
    async fn execute_query(
        &self,
        sql: &str,
        sort_fields: Option<Vec<(String, bool)>>,
        sort_limit_info: Option<crate::compute::optimizer::SortLimitInfo>,
    ) -> CoreResult<QueryResult> {
        let table_name = self.query_builder.extract_table_name(sql)?;
        let meta = self.engine.get_table_meta(&table_name)?;
        let num_partitions = meta.parallel_workers;

        // 🔧 提取 limit hint
        let limit_hint = sort_limit_info.as_ref().map(|info| info.limit);

        log::info!(
            "🔍 [execute_query] table='{}', partitions={}, has_sort={}, limit_hint={:?}",
            table_name,
            num_partitions,
            sort_fields.is_some(),
            limit_hint
        );

        // 并行查询所有 partition
        let mut all_batches = Vec::new();

        for partition_id in 0..num_partitions {
            match self
                .execute_on_partition(
                    &table_name,
                    partition_id as u64,
                    sql,
                    sort_fields.clone(),
                    limit_hint,
                )
                .await
            {
                Ok(batches) => {
                    all_batches.extend(batches);
                }
                Err(e) => {
                    log::warn!("⚠️  Partition {} failed: {}", partition_id, e);
                }
            }
        }

        let total_rows_before: usize = all_batches.iter().map(|b| b.num_rows()).sum();
        log::info!(
            "📊 [execute_query] Collected {} batches with {} total rows from {} partitions",
            all_batches.len(),
            total_rows_before,
            num_partitions
        );

        // 如果有 ORDER BY，做最终排序
        let final_batches = if let Some(info) = sort_limit_info {
            self.apply_final_sort_limit(all_batches, &info)?
        } else {
            all_batches
        };

        Ok(QueryResult {
            batches: final_batches,
        })
    }

    /// 在单个 partition 上执行查询
    async fn execute_on_partition(
        &self,
        table_name: &str,
        partition_id: u64,
        sql: &str,
        sort_hints: Option<Vec<(String, bool)>>,
        limit_hint: Option<usize>,
    ) -> CoreResult<Vec<RecordBatch>> {
        log::info!(
            "🔧 [execute_on_partition] partition_id={}, table='{}', sql='{}', sort_hints={:?}, limit_hint={:?}",
            partition_id,
            table_name,
            sql,
            sort_hints,
            limit_hint
        );

        let ctx = SessionContext::new();

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

        // 🔧 使用带 hints 的 Provider（sort 和 limit 都是 Option，自动传递）
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

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        log::info!(
            "✅ [execute_on_partition] partition_id={}, returned {} batches with {} total rows",
            partition_id,
            batches.len(),
            total_rows
        );

        Ok(batches)
    }
    /// 应用最终的排序和 LIMIT（在协调节点）
    /// 使用 DataFusion API 实现
    fn apply_final_sort_limit(
        &self,
        batches: Vec<RecordBatch>,
        info: &crate::compute::optimizer::SortLimitInfo,
    ) -> CoreResult<Vec<RecordBatch>> {
        if batches.is_empty() {
            return Ok(batches);
        }

        // 如果没有排序字段，这是纯 LIMIT 查询，直接应用 limit
        if info.sort_fields.is_empty() {
            log::info!(
                "🔍 [apply_final_sort_limit] Pure LIMIT query, applying limit={}, offset={:?} to {} batches",
                info.limit,
                info.offset,
                batches.len()
            );

            let offset = info.offset.unwrap_or(0);
            let limit = info.limit;

            // 跳过 offset 行，取 limit 行
            let mut result_batches = Vec::new();
            let mut rows_skipped = 0;
            let mut rows_collected = 0;

            for batch in batches {
                let batch_rows = batch.num_rows();

                // 跳过 offset 行
                if rows_skipped < offset {
                    let skip = (offset - rows_skipped).min(batch_rows);
                    rows_skipped += skip;

                    if skip >= batch_rows {
                        // 整个 batch 都被跳过
                        continue;
                    }

                    // 部分跳过，切片保留剩余部分
                    let remaining = batch.slice(skip, batch_rows - skip);
                    let take = (limit - rows_collected).min(remaining.num_rows());
                    let sliced = remaining.slice(0, take);
                    rows_collected += take;
                    result_batches.push(sliced);
                } else if rows_collected < limit {
                    // 收集数据
                    let take = (limit - rows_collected).min(batch_rows);
                    let sliced = batch.slice(0, take);
                    rows_collected += take;
                    result_batches.push(sliced);
                }

                // 已经收集够了
                if rows_collected >= limit {
                    break;
                }
            }

            log::info!(
                "✅ [apply_final_sort_limit] Pure LIMIT result: {} rows",
                rows_collected
            );

            return Ok(result_batches);
        }

        log::info!(
            "🔄 [apply_final_sort_limit] Sorting {} batches by {:?}, limit={}, offset={:?}",
            batches.len(),
            info.sort_fields,
            info.limit,
            info.offset
        );

        // 方案：使用 TopKMerger（已经实现好的）
        // 未来可以考虑使用 DataFusion 的 sort + limit API
        use crate::compute::TopKMerger;

        let merger = TopKMerger::new(info.sort_fields.clone(), info.limit, info.offset);

        // 将所有 batches 作为一个 partition 的结果
        let result = merger
            .merge(vec![batches])
            .map_err(|e| CoreError::Internal(format!("Failed to merge results: {}", e)))?;

        log::info!(
            "✅ [apply_final_sort_limit] Sorted and limited to {} rows",
            result.iter().map(|b| b.num_rows()).sum::<usize>()
        );

        Ok(result)
    }

    /// 在单个分区上执行 SQL 查询（旧实现，保留用于聚合查询）
    async fn execute_sql_on_partition_old(
        &self,
        table_name: &str,
        partition_id: u64,
        sql: &str,
    ) -> CoreResult<Vec<RecordBatch>> {
        eprintln!(
            "🔍 [execute_sql_on_partition] Starting query on partition {} for table '{}'",
            partition_id, table_name
        );
        eprintln!("🔍 [execute_sql_on_partition] SQL: {}", sql);

        let ctx = SessionContext::new();

        // 获取分区
        let partition = self
            .engine
            .get_partition(table_name, partition_id)
            .await
            .ok_or_else(|| {
                crate::utils::error::CoreError::NotExisted(format!(
                    "Partition {} not found for table '{}'",
                    partition_id, table_name
                ))
            })?;

        eprintln!(
            "🔍 [execute_sql_on_partition] Got partition {}, registering table...",
            partition_id
        );

        // 注册分区到 DataFusion 的默认 catalog 和 schema
        let provider = Arc::new(PartitionTableProvider::new(partition));

        // 使用 catalog API 注册表，这样可以在 datafusion.public.{table_name} 下找到
        if let Err(e) = ctx.register_table(table_name, provider) {
            eprintln!(
                "❌ [execute_sql_on_partition] Failed to register table: {}",
                e
            );
            return Err(crate::utils::error::CoreError::Internal(format!(
                "Failed to register table: {}",
                e
            )));
        }

        eprintln!(
            "🔍 [execute_sql_on_partition] Table '{}' registered, executing SQL...",
            table_name
        );

        // 验证表是否真的注册成功
        let catalog = ctx.catalog("datafusion").unwrap();
        let schema = catalog.schema("public").unwrap();
        let registered_tables: Vec<String> = schema.table_names();
        eprintln!(
            "🔍 [execute_sql_on_partition] Registered tables in datafusion.public: {:?}",
            registered_tables
        );

        // 执行查询
        let df = ctx.sql(sql).await.map_err(|e| {
            eprintln!("❌ [execute_sql_on_partition] SQL parse error: {}", e);
            eprintln!(
                "❌ [execute_sql_on_partition] Available tables: {:?}",
                registered_tables
            );
            crate::utils::error::CoreError::InvalidParam(format!("Query parse error: {}", e))
        })?;

        eprintln!("🔍 [execute_sql_on_partition] SQL parsed successfully, collecting results...");

        let batches = df.collect().await.map_err(|e| {
            eprintln!("❌ [execute_sql_on_partition] Query execution error: {}", e);
            crate::utils::error::CoreError::Internal(format!("Query execution error: {}", e))
        })?;

        eprintln!(
            "🔍 [execute_sql_on_partition] Partition {} returned {} batches",
            partition_id,
            batches.len()
        );

        Ok(batches)
    }

    /// 执行分布式聚合查询
    ///
    /// 支持的聚合函数: COUNT, SUM, AVG, MAX, MIN
    /// 支持 GROUP BY 子句
    async fn execute_aggregation_query(&self, sql: &str) -> CoreResult<QueryResult> {
        // 提取表名
        let table_name = self.query_builder.extract_table_name(sql)?;

        let meta = self.engine.get_table_meta(&table_name)?;
        let num_partitions = meta.parallel_workers;

        eprintln!(
            "🔍 [DistributedExecutor] Executing distributed aggregation on table '{}' with {} partitions",
            table_name, num_partitions
        );

        // 并行在所有分区上执行聚合
        let mut partition_results = Vec::new();

        for partition_id in 0..num_partitions {
            match self
                .execute_sql_on_partition_old(&table_name, partition_id as u64, sql)
                .await
            {
                Ok(batches) => {
                    partition_results.push(batches);
                }
                Err(e) => {
                    eprintln!(
                        "⚠️  [DistributedExecutor] Failed to execute aggregation on partition {}: {}",
                        partition_id, e
                    );
                }
            }
        }

        if partition_results.is_empty() {
            return Err(crate::utils::error::CoreError::Internal(
                "No partition returned results".to_string(),
            ));
        }

        // 合并各分区的聚合结果
        let merged_batch = self
            .aggregation_merger
            .merge_aggregation_results(&partition_results, sql)?;

        Ok(QueryResult {
            batches: vec![merged_batch],
        })
    }
}
