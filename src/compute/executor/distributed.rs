use std::sync::Arc;

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::*;

use crate::compute::PartitionTableProvider;
use crate::compute::optimizer::{analyze_query, QueryType};
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
        let plan = analyze_query(sql);
        
        // 分支 1：聚合查询（需要特殊合并）
        if let Some(ref p) = plan {
            if matches!(p.query_type, QueryType::Aggregation) {
                return self.execute_aggregation_query(sql).await;
            }
        }
        
        // 分支 2：普通查询
        // 提取需要的参数：sort_fields 和 sort_limit_info
        let (sort_fields, sort_limit_info) = if let Some(p) = plan {
            match p.query_type {
                QueryType::SortLimit(info) => {
                    (Some(info.sort_fields.clone()), Some(info))
                }
                _ => (None, None)
            }
        } else {
            (None, None)
        };
        
        self.execute_query(sql, sort_fields, sort_limit_info).await
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
        
        log::info!(
            "🔍 [execute_query] table='{}', partitions={}, has_sort={}",
            table_name,
            num_partitions,
            sort_fields.is_some()
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
    ) -> CoreResult<Vec<RecordBatch>> {
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
        
        // 使用带 hints 的 Provider（sort 和 limit 都是 Option，自动传递）
        let provider = Arc::new(crate::compute::PartitionTableProviderWithHints::new_with_sort_hints(
            partition,
            sort_hints,
        ));
        
        ctx.register_table(table_name, provider)
            .map_err(|e| CoreError::Internal(format!("Failed to register table: {}", e)))?;
        
        let df = ctx.sql(sql).await.map_err(|e| {
            CoreError::InvalidParam(format!("Query parse error: {}", e))
        })?;
        
        let batches = df.collect().await.map_err(|e| {
            CoreError::Internal(format!("Query execution error: {}", e))
        })?;
        
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
        
        let merger = TopKMerger::new(
            info.sort_fields.clone(),
            info.limit,
            info.offset,
        );
        
        // 将所有 batches 作为一个 partition 的结果
        let result = merger.merge(vec![batches]).map_err(|e| {
            CoreError::Internal(format!("Failed to merge results: {}", e))
        })?;
        
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
