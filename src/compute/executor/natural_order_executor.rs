/// 自然顺序查询执行器
///
/// 处理 `ORDER BY _nature` 查询，使用元数据优化深度分页
///
/// ## 核心设计
///
/// 1. **元数据索引**：构建 (partition, segment, doc_count) 元数据
/// 2. **快速定位**：通过累加 doc_count 快速定位目标 segment
/// 3. **Bitmap 跳过**：在 segment 内部通过 bitmap 跳过不需要的行
///
/// ## 性能
///
/// - OFFSET 定位：O(N) segments（扫描元数据）
/// - 数据读取：O(LIMIT) rows（只读需要的数据）
/// - 内存占用：O(N) segments（元数据很小）
use std::sync::Arc;

use datafusion::arrow::datatypes::Schema as ArrowSchema;
use datafusion::arrow::record_batch::RecordBatch;
use roaring::RoaringBitmap;
use std::collections::HashMap;

use crate::engine::Engine;
use crate::segment::field_store::row_data::RowDataStore;
use crate::segment::field_store::IndexReader;
use crate::utils::error::{CoreError, CoreResult};

use super::result_merger::ResultMerger;

/// 查询结果
#[derive(Debug)]
pub struct QueryResult {
    pub batch: RecordBatch,
    pub matched_docs: usize,
}

/// Segment 元数据
#[derive(Debug, Clone)]
pub struct SegmentMeta {
    /// Partition 名称
    pub partition_name: String,
    /// Segment ID ("current" 或者数字 ID)
    pub segment_id: String,
    /// 文档数量
    pub doc_count: u64,
    /// 累计文档数（用于快速定位）
    pub cumulative_count: u64,
}

/// 自然顺序执行器
pub struct NaturalOrderExecutor {
    engine: Arc<Engine>,
    result_merger: ResultMerger,
}

impl NaturalOrderExecutor {
    pub fn new(engine: Arc<Engine>) -> Self {
        Self {
            engine,
            result_merger: ResultMerger::new(),
        }
    }

    /// 执行自然顺序查询
    ///
    /// # 参数
    /// * `sql` - 原始 SQL（包含 ORDER BY _nature）
    /// * `table_name` - 表名
    /// * `limit` - LIMIT 值
    /// * `offset` - OFFSET 值
    /// * `where_clause` - WHERE 条件的 SQL 文本（可选）
    ///
    /// # 示例
    /// ```sql
    /// SELECT * FROM table WHERE app_name='test' ORDER BY _nature LIMIT 1000 OFFSET 1000000;
    /// ```
    pub async fn execute_natural_order(
        &self,
        sql: &str,
        table_name: &str,
        limit: usize,
        offset: usize,
        where_clause: Option<&str>,
    ) -> CoreResult<QueryResult> {
        let total_start = std::time::Instant::now();

        log::info!(
            "🌿 [NaturalOrder] Executing natural order query: table={}, limit={}, offset={}, where={:?}",
            table_name,
            limit,
            offset,
            where_clause
        );

        // 第一步：构建元数据索引
        let step1_start = std::time::Instant::now();
        let segment_metas = self.build_segment_metadata(table_name).await?;
        log::info!(
            "⏱️  [NaturalOrder] Step 1 (metadata): {:?}",
            step1_start.elapsed()
        );

        log::info!(
            "📊 [NaturalOrder] Built metadata index: {} segments, total docs: {}",
            segment_metas.len(),
            segment_metas
                .last()
                .map(|m| m.cumulative_count)
                .unwrap_or(0)
        );

        // 第二步：计算需要读取的 segments
        let step2_start = std::time::Instant::now();
        let target_segments = self.calculate_target_segments(&segment_metas, offset, limit)?;
        log::info!(
            "⏱️  [NaturalOrder] Step 2 (calculate): {:?}",
            step2_start.elapsed()
        );

        log::info!(
            "🎯 [NaturalOrder] Target segments: {:?}",
            target_segments
                .iter()
                .map(|(p, s, _, _)| format!("{}/{}", p, s))
                .collect::<Vec<_>>()
        );

        // 第三步：从 segments 读取数据（带 skip）
        let step3_start = std::time::Instant::now();
        let batches = self
            .read_from_segments(table_name, sql, &target_segments, where_clause)
            .await?;
        log::info!(
            "⏱️  [NaturalOrder] Step 3 (read segments): {:?}",
            step3_start.elapsed()
        );

        let matched_docs = batches.iter().map(|b| b.num_rows()).sum();

        let step4_start = std::time::Instant::now();
        let final_batch = if batches.is_empty() {
            self.result_merger
                .create_empty_batch_from_sql(sql, table_name, &self.engine)
                .await?
        } else {
            self.result_merger.concat_batches(batches)?
        };
        log::info!(
            "⏱️  [NaturalOrder] Step 4 (merge batches): {:?}",
            step4_start.elapsed()
        );

        log::info!(
            "✅ [NaturalOrder] Total time: {:?}, Returned {} rows",
            total_start.elapsed(),
            final_batch.num_rows()
        );

        Ok(QueryResult {
            batch: final_batch,
            matched_docs,
        })
    }

    /// 构建 Segment 元数据索引
    ///
    /// 遍历所有 partition 和 segment，收集元数据并排序
    async fn build_segment_metadata(&self, table_name: &str) -> CoreResult<Vec<SegmentMeta>> {
        let partition_names = self.engine.list_partitions(table_name).await;

        let mut segment_metas = Vec::new();

        for partition_name in partition_names {
            let partition = self
                .engine
                .get_partition(table_name, &partition_name)
                .await
                .ok_or_else(|| {
                    CoreError::NotExisted(format!("Partition {} not found", partition_name))
                })?;

            // ⚠️ 跳过 current segment (Memory 类型)
            // Natural order 只适用于已持久化的 Parquet segments
            // current segment 的数据还在写入,顺序不稳定
            let current_segment = partition.get_current_segment();
            if current_segment.doc_count() > 0 {
                log::debug!(
                    "⏭️  [NaturalOrder] Skipping current segment in partition '{}' ({} docs, Memory type)",
                    partition_name,
                    current_segment.doc_count()
                );
            }

            // 收集 frozen segments (Parquet 类型)
            let frozen_segments = partition.get_frozen_segments();
            for (segment_id, segment) in frozen_segments.iter() {
                segment_metas.push(SegmentMeta {
                    partition_name: partition_name.clone(),
                    segment_id: segment_id.to_string(),
                    doc_count: segment.doc_count() as u64,
                    cumulative_count: 0, // 稍后填充
                });
            }
        }

        // 按照自然顺序排序：partition_name -> segment_id
        segment_metas.sort_by(|a, b| {
            a.partition_name
                .cmp(&b.partition_name)
                .then(a.segment_id.cmp(&b.segment_id))
        });

        // 计算累计文档数
        let mut cumulative = 0u64;
        for meta in &mut segment_metas {
            cumulative += meta.doc_count;
            meta.cumulative_count = cumulative;
        }

        Ok(segment_metas)
    }

    /// 计算需要读取的目标 segments
    ///
    /// # 返回
    /// Vec<(partition_name, segment_id, skip_in_segment, read_count)>
    ///
    /// # 示例
    /// ```
    /// offset = 1000000, limit = 1000
    /// segments = [
    ///   (p1, s1, 100000),  // cumulative: 100000
    ///   (p1, s2, 150000),  // cumulative: 250000
    ///   ...
    ///   (p2, s5, 200000),  // cumulative: 1000000 ← 命中起点
    ///   (p2, s6, 100000),  // cumulative: 1100000
    /// ]
    ///
    /// 结果：[
    ///   (p2, s5, 0, 200000),      // 从 s5 第 0 行开始读 200000 行
    ///   (p2, s6, 0, 1000),        // 从 s6 第 0 行开始读 1000 行（凑够 limit）
    /// ]
    /// ```
    fn calculate_target_segments(
        &self,
        segment_metas: &[SegmentMeta],
        offset: usize,
        limit: usize,
    ) -> CoreResult<Vec<(String, String, usize, usize)>> {
        if segment_metas.is_empty() {
            return Ok(Vec::new());
        }

        let total_docs = segment_metas.last().unwrap().cumulative_count as usize;

        // 检查 offset 是否超出范围
        if offset >= total_docs {
            log::warn!(
                "⚠️  [NaturalOrder] OFFSET {} exceeds total docs {}",
                offset,
                total_docs
            );
            return Ok(Vec::new());
        }

        let mut result = Vec::new();
        let mut remaining_skip = offset;
        let mut remaining_read = limit;

        for meta in segment_metas {
            let doc_count = meta.doc_count as usize;

            if remaining_skip > 0 {
                // 还在跳过阶段
                if remaining_skip >= doc_count {
                    // 整个 segment 都要跳过
                    remaining_skip -= doc_count;
                    continue;
                } else {
                    // 在这个 segment 中开始读取
                    let skip_in_segment = remaining_skip;
                    let available = doc_count - skip_in_segment;
                    let read_count = available.min(remaining_read);

                    result.push((
                        meta.partition_name.clone(),
                        meta.segment_id.clone(),
                        skip_in_segment,
                        read_count,
                    ));

                    remaining_skip = 0;
                    remaining_read -= read_count;

                    if remaining_read == 0 {
                        break;
                    }
                }
            } else {
                // 已经跳过完成，直接读取
                let read_count = doc_count.min(remaining_read);

                result.push((
                    meta.partition_name.clone(),
                    meta.segment_id.clone(),
                    0,
                    read_count,
                ));

                remaining_read -= read_count;

                if remaining_read == 0 {
                    break;
                }
            }
        }

        Ok(result)
    }

    /// 从指定的 segments 读取数据
    ///
    /// # 参数
    /// * `target_segments` - Vec<(partition_name, segment_id, skip, read_count)>
    /// * `where_clause` - WHERE 条件的 SQL 文本（可选）
    async fn read_from_segments(
        &self,
        table_name: &str,
        sql: &str,
        target_segments: &[(String, String, usize, usize)],
        where_clause: Option<&str>,
    ) -> CoreResult<Vec<RecordBatch>> {
        // 清理 SQL：移除 ORDER BY _nature 和 LIMIT/OFFSET
        let _cleaned_sql = self.clean_sql_for_segment_query(sql)?;

        let mut all_batches = Vec::new();

        for (partition_name, segment_id, skip, read_count) in target_segments {
            let segment_start = std::time::Instant::now();

            log::info!(
                "📖 [NaturalOrder] Reading {}/{}: skip={}, read={}",
                partition_name,
                segment_id,
                skip,
                read_count
            );

            let get_partition_start = std::time::Instant::now();
            let partition = self
                .engine
                .get_partition(table_name, partition_name)
                .await
                .ok_or_else(|| {
                    CoreError::NotExisted(format!("Partition {} not found", partition_name))
                })?;
            log::debug!("  ⏱️  get_partition: {:?}", get_partition_start.elapsed());

            // 获取 segment 数据并立即释放锁
            let get_segment_start = std::time::Instant::now();
            let (schema, index_readers, doc_count, deleted, row_data) = if segment_id == "current" {
                let current_segment = partition.get_current_segment();
                let schema = partition.arrow_schema.clone();
                let index_readers = current_segment.get_index_readers();
                let doc_count = current_segment.doc_count();
                let deleted = Arc::new(current_segment.get_deleted());
                let row_data = Arc::new(current_segment.get_row_data());
                // 锁在这里被释放
                drop(current_segment);
                (schema, index_readers, doc_count, deleted, row_data)
            } else {
                let segment_id_u64: u64 = segment_id.parse().map_err(|_| {
                    CoreError::Internal(format!("Invalid segment ID: {}", segment_id))
                })?;

                let frozen_segments = partition.get_frozen_segments();
                let segment_arc = frozen_segments
                    .iter()
                    .find(|(id, _)| *id == segment_id_u64)
                    .ok_or_else(|| {
                        CoreError::NotExisted(format!("Segment {} not found", segment_id))
                    })?
                    .1
                    .clone();
                // 释放 frozen_segments 的锁
                drop(frozen_segments);

                let schema = partition.arrow_schema.clone();
                let index_readers = segment_arc.get_index_readers();
                let doc_count = segment_arc.doc_count();
                let deleted = Arc::new(segment_arc.get_deleted());
                let row_data = Arc::new(segment_arc.get_row_data());

                // ⚠️ 检查 row_data 类型,只处理 Parquet
                match &*row_data {
                    RowDataStore::Memory(_) => {
                        log::warn!(
                            "⏭️  [NaturalOrder] Skipping frozen segment {}/{} (still Memory type)",
                            partition_name,
                            segment_id
                        );
                        continue; // 跳过这个 segment
                    }
                    RowDataStore::Parquet(_) => {
                        // OK, 继续处理
                    }
                }

                (schema, index_readers, doc_count, deleted, row_data)
            };

            log::debug!("  ⏱️  get_segment_data: {:?}", get_segment_start.elapsed());

            // 读取数据（已经没有锁）
            let read_data_start = std::time::Instant::now();
            let batch = self
                .read_from_segment_data(
                    schema,
                    index_readers,
                    doc_count,
                    deleted,
                    row_data,
                    *skip,
                    *read_count,
                    where_clause,
                )
                .await?;
            log::debug!(
                "  ⏱️  read_from_segment_data: {:?}",
                read_data_start.elapsed()
            );

            if batch.num_rows() > 0 {
                all_batches.push(batch);
            }

            log::info!(
                "  ⏱️  Total for segment {}/{}: {:?}",
                partition_name,
                segment_id,
                segment_start.elapsed()
            );
        }

        Ok(all_batches)
    }

    /// 从 segment 数据读取（带 skip）
    ///
    /// 核心优化：通过 bitmap 跳过不需要的行
    async fn read_from_segment_data(
        &self,
        schema: Arc<ArrowSchema>,
        index_readers: HashMap<String, Box<dyn IndexReader>>,
        doc_count: u32,
        deleted: Arc<RoaringBitmap>,
        row_data: Arc<RowDataStore>,
        skip: usize,
        read_count: usize,
        where_clause: Option<&str>,
    ) -> CoreResult<RecordBatch> {
        use crate::compute::segment_scanner::SegmentScanner;

        // SegmentScanner 需要拥有所有权，所以 clone
        let deleted_owned = (*deleted).clone();
        let row_data_owned = (*row_data).clone();

        let scanner = SegmentScanner::new(
            schema.clone(),
            row_data_owned,
            index_readers,
            doc_count,
            deleted_owned,
        );

        // 🚀 从 WHERE 条件解析过滤器
        let filters = if let Some(where_sql) = where_clause {
            self.parse_where_filters(where_sql, &schema)?
        } else {
            Vec::new()
        };

        // 🚀 核心优化：使用 SegmentScanner 的 scan_with_skip
        let batch = scanner.scan_with_skip_and_limit(&filters, skip, read_count)?;

        Ok(batch)
    }

    /// 从 WHERE SQL 文本解析成 DataFusion 过滤器表达式
    fn parse_where_filters(
        &self,
        _where_sql: &str,
        _schema: &Arc<ArrowSchema>,
    ) -> CoreResult<Vec<datafusion::logical_expr::Expr>> {
        // TODO: 实现 WHERE 条件解析
        // 暂时返回空，需要使用 DataFusion 的 SQL parser
        log::warn!("⚠️  WHERE condition parsing not yet implemented");
        Ok(Vec::new())
    }

    /// 清理 SQL：移除 ORDER BY _nature 和 LIMIT/OFFSET
    fn clean_sql_for_segment_query(&self, sql: &str) -> CoreResult<String> {
        let mut cleaned = sql.to_string();

        // 移除 ORDER BY _nature
        let sql_upper = cleaned.to_uppercase();
        if let Some(order_pos) = sql_upper.find("ORDER BY _NATURE") {
            // 查找 ORDER BY 后的下一个子句（LIMIT/OFFSET/分号）
            let after_order = &cleaned[order_pos..];
            if let Some(limit_pos) = after_order.to_uppercase().find("LIMIT") {
                // 保留 LIMIT 之后的部分（稍后会移除）
                cleaned = format!("{}{}", &cleaned[..order_pos], &after_order[limit_pos..]);
            } else {
                // 没有其他子句，直接截断
                cleaned = cleaned[..order_pos].to_string();
            }
        }

        // 移除 LIMIT 和 OFFSET（因为我们在代码中控制）
        let sql_upper = cleaned.to_uppercase();
        if let Some(limit_pos) = sql_upper.find("LIMIT") {
            cleaned = cleaned[..limit_pos].to_string();
        }

        Ok(cleaned.trim().to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_calculate_target_segments() {
        let metas = vec![
            SegmentMeta {
                partition_name: "p1".to_string(),
                segment_id: "s1".to_string(),
                doc_count: 100,
                cumulative_count: 100,
            },
            SegmentMeta {
                partition_name: "p1".to_string(),
                segment_id: "s2".to_string(),
                doc_count: 150,
                cumulative_count: 250,
            },
            SegmentMeta {
                partition_name: "p2".to_string(),
                segment_id: "s1".to_string(),
                doc_count: 200,
                cumulative_count: 450,
            },
        ];

        let executor = NaturalOrderExecutor {
            engine: Arc::new(Engine::new_in_memory()),
            result_merger: ResultMerger::new(),
        };

        // 测试 1：跳过前 100 行，读取 50 行
        let result = executor.calculate_target_segments(&metas, 100, 50).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], ("p1".to_string(), "s2".to_string(), 0, 50));

        // 测试 2：跳过前 200 行，读取 100 行
        let result = executor
            .calculate_target_segments(&metas, 200, 100)
            .unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], ("p1".to_string(), "s2".to_string(), 100, 50));
        assert_eq!(result[1], ("p2".to_string(), "s1".to_string(), 0, 50));

        // 测试 3：跨越多个 segment
        let result = executor.calculate_target_segments(&metas, 50, 300).unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result[0], ("p1".to_string(), "s1".to_string(), 50, 50));
        assert_eq!(result[1], ("p1".to_string(), "s2".to_string(), 0, 150));
        assert_eq!(result[2], ("p2".to_string(), "s1".to_string(), 0, 100));
    }
}
