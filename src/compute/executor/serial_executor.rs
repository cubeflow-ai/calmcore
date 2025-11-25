/// 串行查询执行器
///
/// 负责执行串行扫描查询：
/// - SerialLimit: 只有 LIMIT，无 ORDER BY
/// - SerialFullScan: 无 ORDER BY，无 LIMIT
///
/// 核心优化：
/// - Partition 串行遍历，提前终止
/// - 无 WHERE 时 Segment 串行扫描（最激进优化）
/// - 有 WHERE 时 Segment 并行扫描（利用索引）
use std::sync::Arc;

use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::*;

use crate::engine::Engine;
use crate::utils::error::{CoreError, CoreResult};

use super::QueryResult;

pub struct SerialExecutor {
    engine: Arc<Engine>,
}

impl SerialExecutor {
    pub fn new(engine: Arc<Engine>) -> Self {
        Self { engine }
    }

    /// 执行串行 LIMIT（只有 LIMIT，无 ORDER BY）
    pub async fn execute_serial_limit(
        &self,
        sql: &str,
        table_name: &str,
        limit: usize,
        offset: Option<usize>,
        has_where: bool,
    ) -> CoreResult<QueryResult> {
        if has_where {
            // 有 WHERE：Partition 串行，Segment 并行（通过 DataFusion）
            log::info!("🔀 [SerialLimit with WHERE] Using parallel segment scan");
            self.execute_with_where(sql, table_name, limit, offset)
                .await
        } else {
            // 无 WHERE：Partition 串行，Segment 串行，直接读取原始数据
            log::info!(
                "➡️  [SerialLimit no WHERE] Using serial segment scan with early termination"
            );
            self.execute_no_where(table_name, limit, offset).await
        }
    }

    /// 执行串行全表扫描（无 ORDER BY，无 LIMIT）
    pub async fn execute_serial_full_scan(
        &self,
        sql: &str,
        table_name: &str,
        limit_hint: Option<usize>,
        offset_value: Option<usize>,
    ) -> CoreResult<QueryResult> {
        let partition_names = self.engine.list_partitions(table_name).await;

        let fetch_limit = if let Some(limit) = limit_hint {
            Some(offset_value.unwrap_or(0) + limit)
        } else {
            None
        };

        let mut all_batches = Vec::new();

        for partition_name in &partition_names {
            match self
                .execute_on_partition_via_datafusion(
                    table_name,
                    partition_name,
                    sql,
                    None,
                    fetch_limit,
                )
                .await
            {
                Ok(batches) => all_batches.extend(batches),
                Err(e) => {
                    // SQL syntax errors (field not found, parse error) should fail immediately
                    if Self::is_sql_error(&e) {
                        return Err(e);
                    }
                    // Data errors (partition corruption) can be skipped
                    log::warn!("⚠️  Partition {} failed: {}", partition_name, e);
                }
            }
        }

        let matched_docs = all_batches.iter().map(|b| b.num_rows()).sum();

        let final_batch = if all_batches.is_empty() {
            self.create_empty_batch(table_name).await?
        } else {
            let merged = Self::concat_batches(all_batches)?;

            // 应用 OFFSET 和 LIMIT
            if let Some(offset) = offset_value {
                if offset >= merged.num_rows() {
                    Self::create_empty_batch_with_schema(merged.schema())?
                } else if let Some(limit) = limit_hint {
                    let remaining = merged.num_rows() - offset;
                    merged.slice(offset, remaining.min(limit))
                } else {
                    merged.slice(offset, merged.num_rows() - offset)
                }
            } else if let Some(limit) = limit_hint {
                merged.slice(0, limit.min(merged.num_rows()))
            } else {
                merged
            }
        };

        Ok(QueryResult {
            batch: final_batch,
            matched_docs,
        })
    }

    // ===== 私有方法 =====

    /// 有 WHERE 条件的串行 LIMIT
    async fn execute_with_where(
        &self,
        sql: &str,
        table_name: &str,
        limit: usize,
        offset: Option<usize>,
    ) -> CoreResult<QueryResult> {
        let partition_names = self.engine.list_partitions(table_name).await;

        let mut all_batches = Vec::new();
        let mut collected_rows = 0;
        let target_rows = offset.unwrap_or(0) + limit;

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
                .execute_on_partition_via_datafusion(
                    table_name,
                    partition_name,
                    sql,
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

        let final_batch = if all_batches.is_empty() {
            self.create_empty_batch(table_name).await?
        } else {
            let merged = Self::concat_batches(all_batches)?;
            if let Some(off) = offset {
                if off > 0 && merged.num_rows() > off {
                    merged.slice(off, (merged.num_rows() - off).min(limit))
                } else if off >= merged.num_rows() {
                    Self::create_empty_batch_with_schema(merged.schema())?
                } else {
                    merged.slice(0, limit.min(merged.num_rows()))
                }
            } else {
                merged.slice(0, limit.min(merged.num_rows()))
            }
        };

        Ok(QueryResult {
            batch: final_batch,
            matched_docs,
        })
    }

    /// 无 WHERE 条件的串行 LIMIT
    async fn execute_no_where(
        &self,
        table_name: &str,
        limit: usize,
        offset: Option<usize>,
    ) -> CoreResult<QueryResult> {
        let partition_names = self.engine.list_partitions(table_name).await;

        let mut all_batches = Vec::new();
        let mut collected_rows = 0;
        let target_rows = offset.unwrap_or(0) + limit;

        // 串行遍历 Partitions
        'partition_loop: for partition_name in &partition_names {
            let partition = self
                .engine
                .get_partition(table_name, partition_name)
                .await
                .ok_or_else(|| {
                    CoreError::NotExisted(format!("Partition {} not found", partition_name))
                })?;

            // 串行遍历 Segments（current + frozen）
            // 1. 先扫描 current segment
            {
                let segment = partition.get_current_segment();
                if segment.doc_count() > 0 {
                    let remaining = target_rows - collected_rows;
                    if let Some(batch) = Self::read_segment_data(&segment, remaining)? {
                        collected_rows += batch.num_rows();
                        all_batches.push(batch);

                        if collected_rows >= target_rows {
                            log::info!(
                                "✅ Early termination at current segment: collected {} >= target {}",
                                collected_rows,
                                target_rows
                            );
                            break 'partition_loop;
                        }
                    }
                }
            }

            // 2. 再扫描 frozen segments
            {
                let frozen_segments = partition.get_frozen_segments();
                for (_seg_id, segment) in frozen_segments.iter() {
                    if collected_rows >= target_rows {
                        break 'partition_loop;
                    }

                    let remaining = target_rows - collected_rows;
                    if let Some(batch) = Self::read_segment_data(segment, remaining)? {
                        collected_rows += batch.num_rows();
                        all_batches.push(batch);

                        if collected_rows >= target_rows {
                            log::info!(
                                "✅ Early termination at frozen segment: collected {} >= target {}",
                                collected_rows,
                                target_rows
                            );
                            break 'partition_loop;
                        }
                    }
                }
            }
        }

        let matched_docs = collected_rows;

        let final_batch = if all_batches.is_empty() {
            self.create_empty_batch(table_name).await?
        } else {
            let merged = Self::concat_batches(all_batches)?;
            if let Some(off) = offset {
                if off > 0 && merged.num_rows() > off {
                    merged.slice(off, (merged.num_rows() - off).min(limit))
                } else if off >= merged.num_rows() {
                    Self::create_empty_batch_with_schema(merged.schema())?
                } else {
                    merged.slice(0, limit.min(merged.num_rows()))
                }
            } else {
                merged.slice(0, limit.min(merged.num_rows()))
            }
        };

        Ok(QueryResult {
            batch: final_batch,
            matched_docs,
        })
    }

    /// 通过 DataFusion 在单个 partition 上执行查询
    async fn execute_on_partition_via_datafusion(
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

    /// 从 segment 读取指定数量的原始数据（无过滤）
    fn read_segment_data(
        segment: &crate::segment::Segment,
        limit: usize,
    ) -> CoreResult<Option<RecordBatch>> {
        use datafusion::arrow::array::UInt32Array;
        use datafusion::arrow::compute::take;

        let row_data = segment.get_row_data();
        let deleted = segment.get_deleted();

        // 计算有效文档
        let mut valid_docs = roaring::RoaringBitmap::new();
        valid_docs.insert_range(0..segment.doc_count());
        valid_docs -= &deleted;

        if valid_docs.is_empty() {
            return Ok(None);
        }

        // 取前 limit 个有效文档
        let doc_ids: Vec<u32> = valid_docs.iter().take(limit).collect();

        if doc_ids.is_empty() {
            return Ok(None);
        }

        // 使用 batch_lookup 查找哪些 RecordBatch 包含这些 doc_ids
        let batch_groups = row_data.batch_lookup_doc_ids(&doc_ids);

        if batch_groups.is_empty() {
            return Ok(None);
        }

        // 批量读取所有需要的 RecordBatch
        let batch_keys: Vec<u32> = batch_groups.keys().copied().collect();
        let batches = row_data.get_batch_with_projection(&batch_keys, None);

        // 从每个 batch 中提取需要的行
        let mut result_batches = Vec::new();

        for (batch_key, batch) in batches {
            if let Some(target_doc_ids) = batch_groups.get(&batch_key) {
                // batch_key 是 batch 的起始 doc_id
                // 将 doc_id 转换为 batch 内的相对索引
                let batch_size = batch.num_rows() as u32;

                let mut indices = Vec::new();
                for &doc_id in target_doc_ids {
                    let relative_idx = doc_id.saturating_sub(batch_key);
                    // 边界检查
                    if relative_idx < batch_size {
                        indices.push(relative_idx);
                    }
                }

                if !indices.is_empty() {
                    // 使用 take 提取行
                    let indices_array = UInt32Array::from(indices);
                    let mut new_columns = Vec::new();

                    for column in batch.columns() {
                        let taken = take(column.as_ref(), &indices_array, None).map_err(|e| {
                            CoreError::Internal(format!("Failed to take rows: {}", e))
                        })?;
                        new_columns.push(taken);
                    }

                    let new_batch =
                        RecordBatch::try_new(batch.schema(), new_columns).map_err(|e| {
                            CoreError::Internal(format!("Failed to create RecordBatch: {}", e))
                        })?;

                    result_batches.push(new_batch);
                }
            }
        }

        if result_batches.is_empty() {
            return Ok(None);
        }

        // 合并所有 batches
        if result_batches.len() == 1 {
            Ok(Some(result_batches.into_iter().next().unwrap()))
        } else {
            use datafusion::arrow::compute::concat_batches;
            let schema = result_batches[0].schema();
            let merged = concat_batches(&schema, &result_batches)
                .map_err(|e| CoreError::Internal(format!("Failed to concat batches: {}", e)))?;
            Ok(Some(merged))
        }
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
}
