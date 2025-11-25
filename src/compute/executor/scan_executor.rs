/// 扫描查询执行器
///
/// 处理串行扫描、分页查询，支持深度分页优化
use std::sync::Arc;

use datafusion::arrow::record_batch::RecordBatch;

use crate::compute::optimizer::{ExecutionHints, PureLimitInfo};
use crate::engine::Engine;
use crate::utils::error::{CoreError, CoreResult};

use super::partition_executor::PartitionExecutor;
use super::result_merger::ResultMerger;
use super::sql_utils::SqlUtils;

/// 查询结果
#[derive(Debug)]
pub struct QueryResult {
    pub batch: RecordBatch,
    pub matched_docs: usize,
}

/// 扫描执行器
pub struct ScanExecutor {
    engine: Arc<Engine>,
    partition_executor: PartitionExecutor,
    result_merger: ResultMerger,
}

impl ScanExecutor {
    pub fn new(engine: Arc<Engine>) -> Self {
        Self {
            engine: engine.clone(),
            partition_executor: PartitionExecutor::new(engine.clone()),
            result_merger: ResultMerger::new(),
        }
    }

    /// 执行串行全表扫描（无 ORDER BY，无 LIMIT）
    ///
    /// 🚀 优化点：
    /// 1. 支持 _partition 过滤：只扫描指定 partition
    /// 2. 支持 LIMIT 下推：只读取需要的行数
    /// 3. 移除虚拟字段：清理 SQL 避免 DataFusion 报错
    pub async fn execute_serial_full_scan(
        &self,
        sql: &str,
        table_name: &str,
        hints: &ExecutionHints,
    ) -> CoreResult<QueryResult> {
        // 🔧 从 SQL 中移除 _partition 和 _segment 条件
        let cleaned_sql = SqlUtils::remove_virtual_columns(sql)?;

        // 🔧 提取 LIMIT/OFFSET 信息用于下推
        let (limit_hint, offset_value) = SqlUtils::extract_limit_offset(sql);

        log::info!("🔧 [ScanExecutor] Original SQL: {}", sql);
        log::info!("🔧 [ScanExecutor] Cleaned SQL: {}", cleaned_sql);
        log::info!(
            "🔧 [ScanExecutor] LIMIT: {:?}, OFFSET: {:?}",
            limit_hint,
            offset_value
        );

        // 检查是否指定了 partition
        if let Some(ref partition_name) = hints.target_partition {
            log::info!(
                "🎯 [Partition Filter] Querying specific partition: {}",
                partition_name
            );

            return self
                .execute_on_single_partition(
                    table_name,
                    partition_name,
                    &cleaned_sql,
                    limit_hint,
                    offset_value,
                )
                .await;
        }

        // 没有指定 partition，执行全表扫描
        log::info!("📄 [Full Scan] Scanning all partitions");
        self.execute_on_all_partitions(table_name, &cleaned_sql, limit_hint, offset_value)
            .await
    }

    /// 执行串行 LIMIT（只有 LIMIT，无 ORDER BY）
    /// 优化：串行遍历 partition，达到 limit 就停止
    pub async fn execute_serial_limit(
        &self,
        sql: &str,
        table_name: &str,
        info: PureLimitInfo,
    ) -> CoreResult<QueryResult> {
        let cleaned_sql = SqlUtils::remove_virtual_columns(sql)?;
        let partition_names = self.engine.list_partitions(table_name).await;

        let mut all_batches = Vec::new();
        let mut collected_rows = 0;
        let target_rows = info.offset.unwrap_or(0) + info.limit;

        // 串行遍历分区，达到目标行数就停止
        for partition_name in &partition_names {
            if collected_rows >= target_rows {
                log::info!(
                    "✅ Early termination: collected {} >= target {}",
                    collected_rows,
                    target_rows
                );
                break;
            }

            match self
                .partition_executor
                .execute_on_partition(
                    table_name,
                    partition_name,
                    &cleaned_sql,
                    None,
                    Some(target_rows - collected_rows),
                )
                .await
            {
                Ok(batches) => {
                    for batch in batches {
                        collected_rows += batch.num_rows();
                        all_batches.push(batch);

                        if collected_rows >= target_rows {
                            break;
                        }
                    }
                }
                Err(e) => {
                    // SQL syntax errors should fail immediately
                    if Self::is_sql_error(&e) {
                        return Err(e);
                    }
                    // Data errors can be skipped
                    log::warn!("⚠️  Partition {} failed: {}", partition_name, e);
                }
            }
        }

        let matched_docs = collected_rows;

        // 应用 OFFSET（如果有）
        let final_batch = if all_batches.is_empty() {
            self.result_merger
                .create_empty_batch_from_sql(&cleaned_sql, table_name, &self.engine)
                .await?
        } else {
            let merged = self.result_merger.concat_batches(all_batches)?;
            if let Some(offset) = info.offset {
                if offset > 0 && merged.num_rows() > offset {
                    merged.slice(offset, (merged.num_rows() - offset).min(info.limit))
                } else if offset >= merged.num_rows() {
                    // offset 超出范围，返回空结果
                    self.result_merger
                        .create_empty_batch_with_schema(merged.schema())?
                } else {
                    merged.slice(0, info.limit.min(merged.num_rows()))
                }
            } else {
                merged.slice(0, info.limit.min(merged.num_rows()))
            }
        };

        Ok(QueryResult {
            batch: final_batch,
            matched_docs,
        })
    }

    /// 在单个 partition 上执行查询
    async fn execute_on_single_partition(
        &self,
        table_name: &str,
        partition_name: &str,
        sql: &str,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> CoreResult<QueryResult> {
        // 🚀 计算需要从 partition 拉取的行数：OFFSET + LIMIT
        let fetch_limit = if let Some(l) = limit {
            Some(offset.unwrap_or(0) + l)
        } else {
            None
        };

        let batches = self
            .partition_executor
            .execute_on_partition(table_name, partition_name, sql, None, fetch_limit)
            .await?;

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        let matched_docs = total_rows;

        let final_batch = if batches.is_empty() {
            self.result_merger
                .create_empty_batch_from_sql(sql, table_name, &self.engine)
                .await?
        } else {
            let merged = self.result_merger.concat_batches(batches)?;

            // 🔧 应用 OFFSET 和 LIMIT
            self.apply_offset_limit(merged, offset, limit)?
        };

        Ok(QueryResult {
            batch: final_batch,
            matched_docs,
        })
    }

    /// 在所有 partitions 上执行查询
    async fn execute_on_all_partitions(
        &self,
        table_name: &str,
        sql: &str,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> CoreResult<QueryResult> {
        let partition_names = self.engine.list_partitions(table_name).await;

        // 🚀 计算需要从每个 partition 拉取的行数
        let fetch_limit = if let Some(l) = limit {
            Some(offset.unwrap_or(0) + l)
        } else {
            None
        };

        let mut all_batches = Vec::new();

        for partition_name in &partition_names {
            match self
                .partition_executor
                .execute_on_partition(table_name, partition_name, sql, None, fetch_limit)
                .await
            {
                Ok(batches) => all_batches.extend(batches),
                Err(e) => {
                    // SQL syntax errors should fail immediately
                    if Self::is_sql_error(&e) {
                        return Err(e);
                    }
                    // Data errors can be skipped
                    log::warn!("⚠️  Partition {} failed: {}", partition_name, e);
                }
            }
        }

        let matched_docs = all_batches.iter().map(|b| b.num_rows()).sum();

        let final_batch = if all_batches.is_empty() {
            self.result_merger
                .create_empty_batch_from_sql(sql, table_name, &self.engine)
                .await?
        } else {
            let merged = self.result_merger.concat_batches(all_batches)?;

            // 🔧 应用 OFFSET 和 LIMIT
            self.apply_offset_limit(merged, offset, limit)?
        };

        Ok(QueryResult {
            batch: final_batch,
            matched_docs,
        })
    }

    /// 应用 OFFSET 和 LIMIT 切片
    fn apply_offset_limit(
        &self,
        batch: RecordBatch,
        offset: Option<usize>,
        limit: Option<usize>,
    ) -> CoreResult<RecordBatch> {
        if let Some(off) = offset {
            if off >= batch.num_rows() {
                // offset 超出范围，返回空结果
                return self
                    .result_merger
                    .create_empty_batch_with_schema(batch.schema());
            } else if let Some(lim) = limit {
                // 有 offset 和 limit
                let remaining = batch.num_rows() - off;
                return Ok(batch.slice(off, remaining.min(lim)));
            } else {
                // 只有 offset
                return Ok(batch.slice(off, batch.num_rows() - off));
            }
        } else if let Some(lim) = limit {
            // 只有 limit
            return Ok(batch.slice(0, lim.min(batch.num_rows())));
        }

        // 无 offset 和 limit
        Ok(batch)
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
