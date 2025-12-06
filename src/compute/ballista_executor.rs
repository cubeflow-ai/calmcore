//! DataFusion-based executor with optimized parallelism
//!
//! 使用 DataFusion 作为查询执行引擎,简化代码:
//! - 利用 DataFusion 的查询优化器
//! - 自动处理并行执行
//! - 保留所有 SegmentScanner 的优化
//! - 为未来分布式执行做准备

use std::sync::Arc;

use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::prelude::*;

use super::natural_order_executor::NaturalOrderExecutor;
use crate::compute::information_schema_executor::InformationSchemaExecutor;
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
    information_schema_executor: InformationSchemaExecutor,
}

impl DataFusionExecutor {
    pub fn new(engine: Arc<Engine>) -> Self {
        Self {
            natural_order_executor: NaturalOrderExecutor::new(engine.clone()),
            information_schema_executor: InformationSchemaExecutor::new(engine.clone()),
            engine,
        }
    }

    /// 执行 SQL 查询（流式版本）
    ///
    /// 返回 DataFusion 的原生 Stream，避免全部加载到内存
    ///
    /// # 优势
    /// - DataFusion 本身就是流式的（迭代器模式）
    /// - 避免 collect() 带来的内存峰值
    /// - Ballista 分布式执行也是流式的
    pub async fn execute_sql_stream(&self, sql: &str) -> CoreResult<SendableRecordBatchStream> {
        log::info!("🚀 [DataFusion Executor] Executing SQL (stream): {}", sql);

        // 🔧 标准化 SQL 并提取分区过滤条件
        let normalized = SqlNormalizer::normalize(sql)?;

        if normalized.rewritten_sql != sql {
            log::info!(
                "🔄 [SQL Normalized] {} -> {}",
                sql,
                normalized.rewritten_sql
            );
        }

        // 🎯 显示分区过滤信息
        if normalized.partition_filters.has_filter {
            log::info!("🎯 [Partition Filter] Detected partition conditions");
            if !normalized.partition_filters.exact_matches.is_empty() {
                log::info!(
                    "  Exact matches: {:?}",
                    normalized.partition_filters.exact_matches
                );
            }
            if !normalized.partition_filters.like_patterns.is_empty() {
                log::info!(
                    "  LIKE patterns: {:?}",
                    normalized.partition_filters.like_patterns
                );
            }
        }

        // 🗂️ 特殊处理: INFORMATION_SCHEMA 查询
        let normalized_upper = normalized.rewritten_sql.to_uppercase();
        if normalized_upper.contains("INFORMATION_SCHEMA") {
            log::info!("🗂️ [INFORMATION_SCHEMA] Detected metadata query");
            return self
                .information_schema_executor
                .execute_stream(&normalized.rewritten_sql)
                .await;
        }

        // 🌿 特殊处理: ORDER BY _nature (深度分页优化)
        if normalized_upper.contains("ORDER BY _NATURE")
            || normalized_upper.contains("ORDER BY `_NATURE`")
        {
            log::info!("🌿 [Natural Order] Detected ORDER BY _nature, using streaming cursor");

            // 使用 execute_natural_cursor 获取流式结果
            let stream_result = self
                .natural_order_executor
                .execute_natural_cursor(&normalized.rewritten_sql)
                .await?;

            // 将 mpsc::Receiver<CoreResult<RecordBatch>> 转换为 SendableRecordBatchStream
            use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
            use futures::stream;

            let schema = stream_result.schema.clone();
            let mut receiver = stream_result.receiver;

            // 创建 futures Stream
            let stream = stream::poll_fn(move |cx| {
                use std::task::Poll;

                match receiver.poll_recv(cx) {
                    Poll::Ready(Some(Ok(batch))) => Poll::Ready(Some(Ok(batch))),
                    Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(
                        datafusion::error::DataFusionError::External(Box::new(e)),
                    ))),
                    Poll::Ready(None) => Poll::Ready(None),
                    Poll::Pending => Poll::Pending,
                }
            });

            // 包装成 RecordBatchStreamAdapter
            let adapter = RecordBatchStreamAdapter::new(schema, stream);
            return Ok(Box::pin(adapter));
        }

        //TODO:ANSJ

        // 创建 DataFusion SessionContext
        let config = SessionConfig::new().with_target_partitions(1);
        let ctx = SessionContext::new_with_config(config);

        // 提取表名
        let table_name = self.extract_table_name(&normalized.rewritten_sql)?;

        // 🎯 根据分区过滤条件确定要扫描的分区
        let all_partition_names = self.engine.list_partitions(&table_name).await;
        let target_partition_names = if normalized.partition_filters.has_filter {
            // 有分区过滤条件，只扫描匹配的分区
            let matched = normalized
                .partition_filters
                .resolve_partitions(&all_partition_names);
            log::info!(
                "📂 [Partitions] Scanning {} out of {} partitions (filtered)",
                matched.len(),
                all_partition_names.len()
            );
            matched
        } else {
            // 没有分区过滤条件，扫描所有分区
            log::info!(
                "📂 [Partitions] Scanning all {} partitions",
                all_partition_names.len()
            );
            all_partition_names
        };

        // 获取目标分区对象
        let mut partitions = Vec::new();
        for partition_name in target_partition_names {
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

        let union_table = UnionTableProvider::new(partitions)
            .map_err(|e| CoreError::Internal(format!("Failed to create UnionTable: {}", e)))?;

        ctx.register_table(&table_name, Arc::new(union_table))
            .map_err(|e| CoreError::Internal(format!("Failed to register table: {}", e)))?;

        // 执行查询，返回 Stream
        let df = ctx
            .sql(&normalized.rewritten_sql)
            .await
            .map_err(|e| CoreError::Internal(format!("Query execution error: {}", e)))?;

        // 🔑 关键：返回 Stream 而不是 collect()
        let stream = df
            .execute_stream()
            .await
            .map_err(|e| CoreError::Internal(format!("Failed to execute stream: {}", e)))?;

        log::info!("✅ [DataFusion Executor] Stream ready");
        Ok(stream)
    }

    /// 从 SQL 中提取表名 (简化版本)
    fn extract_table_name(&self, sql: &str) -> CoreResult<String> {
        log::debug!("[extract_table_name] Input SQL: {}", sql);

        let sql_upper = sql.to_uppercase();
        log::debug!("[extract_table_name] Uppercase SQL: {}", sql_upper);

        // 查找 FROM 关键字
        if let Some(from_pos) = sql_upper.find(" FROM ") {
            // 使用原始 SQL 的位置提取表名（保持原始大小写）
            let after_from = &sql[from_pos + 6..].trim();
            log::debug!("[extract_table_name] After FROM: {}", after_from);

            // 提取表名（可能包含 schema.table 格式）
            let table_name = after_from
                .split_whitespace()
                .next()
                .unwrap_or("")
                .trim_matches(|c: char| !c.is_alphanumeric() && c != '_' && c != '.');

            log::debug!("[extract_table_name] Extracted table name: {}", table_name);

            if !table_name.is_empty() {
                return Ok(table_name.to_string());
            }
        }

        log::error!(
            "[extract_table_name] Could not find FROM clause in SQL: {}",
            sql
        );
        Err(CoreError::InvalidParam(format!(
            "Could not extract table name from SQL: {}",
            sql
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_table_name() {
        use crate::engine::EngineConfig;
        let engine = Engine::new(EngineConfig::default()).unwrap();
        let executor = DataFusionExecutor::new(engine);

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
