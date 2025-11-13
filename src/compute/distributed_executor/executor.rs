use std::sync::Arc;

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::*;

use crate::compute::PartitionTableProvider;
use crate::engine::Engine;
use crate::utils::error::CoreResult;

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
    /// 这是一个通用的 SQL 查询接口，可被 MySQL、GraphQL、Elasticsearch 等协议层调用
    ///
    /// # 支持的查询类型
    /// - ✅ SELECT * WHERE ... (简单过滤查询)
    /// - ✅ 聚合函数 (COUNT/SUM/AVG/MAX/MIN) - 分布式聚合
    /// - ✅ GROUP BY - 分布式分组聚合
    /// - ⚠️  ORDER BY + LIMIT: 结果可能不完整（只取第一个分区）
    pub async fn execute_sql(&self, sql: &str) -> CoreResult<QueryResult> {
        let query_lower = sql.to_lowercase();

        // 检查是否包含聚合函数
        let has_aggregation = query_lower.contains("count(")
            || query_lower.contains("sum(")
            || query_lower.contains("avg(")
            || query_lower.contains("max(")
            || query_lower.contains("min(");

        // 检查是否包含 GROUP BY
        let has_group_by = query_lower.contains("group by");

        if has_aggregation || has_group_by {
            // 使用分布式聚合执行路径
            return self.execute_aggregation_query(sql).await;
        }

        // 提取表名
        let table_name = self.query_builder.extract_table_name(sql)?;

        // 获取表元数据
        let meta = self.engine.get_table_meta(&table_name)?;
        let num_partitions = meta.parallel_workers;

        eprintln!(
            "🔍 [DistributedExecutor] Executing query on table '{}' with {} partitions",
            table_name, num_partitions
        );

        // 并行查询所有分区
        let mut all_batches = Vec::new();

        for partition_id in 0..num_partitions {
            match self
                .execute_sql_on_partition(&table_name, partition_id as u64, sql)
                .await
            {
                Ok(batches) => {
                    let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
                    eprintln!(
                        "🔍 [DistributedExecutor] Partition {} returned {} rows",
                        partition_id, row_count
                    );
                    all_batches.extend(batches);
                }
                Err(e) => {
                    eprintln!(
                        "⚠️  [DistributedExecutor] Failed to query partition {}: {}",
                        partition_id, e
                    );
                    // 继续查询其他分区，不因为一个分区失败而整体失败
                }
            }
        }

        eprintln!(
            "🔍 [DistributedExecutor] Query completed: {} batches",
            all_batches.len()
        );

        Ok(QueryResult {
            batches: all_batches,
        })
    }

    /// 在单个分区上执行 SQL 查询
    async fn execute_sql_on_partition(
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
                .execute_sql_on_partition(&table_name, partition_id as u64, sql)
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
