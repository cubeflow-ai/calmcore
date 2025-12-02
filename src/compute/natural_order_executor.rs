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
use tokio::sync::mpsc;

use crate::engine::Engine;
use crate::utils::error::{CoreError, CoreResult};

/// 查询结果
#[derive(Debug)]
pub struct QueryResult {
    pub batch: RecordBatch,
    pub matched_docs: usize,
}

/// 流式查询结果 (通过 channel 传输)
pub struct StreamingQueryResult {
    /// 接收 RecordBatch 的 channel
    pub receiver: mpsc::Receiver<CoreResult<RecordBatch>>,
    /// Schema (第一个 batch 之前就知道)
    pub schema: Arc<ArrowSchema>,
    /// 总匹配文档数 (可能在流式传输过程中更新)
    pub matched_docs: Arc<std::sync::atomic::AtomicUsize>,
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
}

impl NaturalOrderExecutor {
    pub fn new(engine: Arc<Engine>) -> Self {
        Self { engine }
    }

    /// 从表达式中提取数字
    fn extract_number_from_expr(
        &self,
        expr: &datafusion::sql::sqlparser::ast::Expr,
    ) -> Option<usize> {
        use datafusion::sql::sqlparser::ast::{Expr, Value};

        match expr {
            Expr::Value(value_with_span) => match &value_with_span.value {
                Value::Number(n, _) => n.parse::<usize>().ok(),
                _ => None,
            },
            _ => None,
        }
    }

    /// 解析 ORDER BY _nature 查询的 SQL
    ///
    /// 返回: (table_name, limit, offset, where_clause, projection_fields, is_select_star)
    fn parse_sql(
        &self,
        sql: &str,
    ) -> CoreResult<(String, usize, usize, Option<String>, Vec<String>, bool)> {
        use datafusion::sql::sqlparser::ast::{Expr, SelectItem, SetExpr, Statement, TableFactor};
        use datafusion::sql::sqlparser::dialect::GenericDialect;
        use datafusion::sql::sqlparser::parser::Parser;

        log::debug!("🔍 [NaturalOrder] Parsing SQL: '{}'", sql);

        // 解析 SQL
        let dialect = GenericDialect {};
        let statements = Parser::parse_sql(&dialect, sql).map_err(|e| {
            log::error!("❌ [NaturalOrder] Failed to parse SQL '{}': {}", sql, e);
            CoreError::InvalidParam(format!("Failed to parse SQL: {}", e))
        })?;

        if statements.is_empty() {
            return Err(CoreError::InvalidParam("Empty SQL statement".into()));
        }

        let statement = &statements[0];

        if let Statement::Query(query) = statement {
            if let SetExpr::Select(select) = query.body.as_ref() {
                // 1. 提取表名
                let table_name = if let Some(table_with_joins) = select.from.first() {
                    match &table_with_joins.relation {
                        TableFactor::Table { name, .. } => name
                            .0
                            .iter()
                            .map(|ident| ident.to_string())
                            .collect::<Vec<_>>()
                            .join("."),
                        _ => {
                            return Err(CoreError::InvalidParam(
                                "Complex table expressions not supported".into(),
                            ))
                        }
                    }
                } else {
                    return Err(CoreError::InvalidParam(
                        "Cannot find table name in SQL".into(),
                    ));
                };
                // 2. 提取 SELECT 字段
                let (projection_fields, is_select_star) = if select.projection.len() == 1 {
                    match &select.projection[0] {
                        SelectItem::Wildcard(_) => (vec![], true),
                        SelectItem::UnnamedExpr(expr) => {
                            if let Expr::Identifier(ident) = expr {
                                let ident_str = ident.to_string();
                                if ident_str == "*" {
                                    (vec![], true)
                                } else {
                                    (vec![ident_str], false)
                                }
                            } else {
                                // 复杂表达式，暂时不支持
                                (vec![], true)
                            }
                        }
                        SelectItem::ExprWithAlias { alias, .. } => (vec![alias.to_string()], false),
                        SelectItem::QualifiedWildcard(_, _) => (vec![], true),
                    }
                } else if select.projection.iter().all(|item| {
                    matches!(
                        item,
                        SelectItem::UnnamedExpr(_) | SelectItem::ExprWithAlias { .. }
                    )
                }) {
                    // 多个字段
                    let fields: Result<Vec<String>, CoreError> = select
                        .projection
                        .iter()
                        .map(|item| match item {
                            SelectItem::UnnamedExpr(expr) => {
                                if let Expr::Identifier(ident) = expr {
                                    Ok(ident.to_string())
                                } else {
                                    Err(CoreError::InvalidParam(
                                        "Complex expressions in SELECT not supported".into(),
                                    ))
                                }
                            }
                            SelectItem::ExprWithAlias { alias, .. } => Ok(alias.to_string()),
                            _ => Err(CoreError::InvalidParam("Unsupported SELECT item".into())),
                        })
                        .collect();
                    (fields?, false)
                } else {
                    (vec![], true)
                };

                // 3. 提取 WHERE 子句
                let where_clause = if let Some(selection) = &select.selection {
                    Some(format!("{}", selection))
                } else {
                    None
                };

                // 4. 解析 LIMIT 和 OFFSET - 使用 sqlparser 的 LimitClause
                let (limit, offset) = if let Some(limit_clause) = &query.limit_clause {
                    use datafusion::sql::sqlparser::ast::LimitClause;

                    match limit_clause {
                        LimitClause::OffsetCommaLimit {
                            offset: offset_expr,
                            limit: limit_expr,
                        } => {
                            // MySQL 语法: LIMIT offset, limit
                            let offset_val =
                                self.extract_number_from_expr(offset_expr).ok_or_else(|| {
                                    CoreError::InvalidParam(
                                        "Invalid offset in MySQL LIMIT syntax".into(),
                                    )
                                })?;
                            let limit_val =
                                self.extract_number_from_expr(limit_expr).ok_or_else(|| {
                                    CoreError::InvalidParam(
                                        "Invalid limit in MySQL LIMIT syntax".into(),
                                    )
                                })?;
                            log::info!(
                                "✅ [NaturalOrder] MySQL LIMIT syntax parsed: offset={}, limit={}",
                                offset_val,
                                limit_val
                            );
                            (limit_val, offset_val)
                        }
                        LimitClause::LimitOffset { limit, offset, .. } => {
                            // 标准 SQL 语法: LIMIT count [OFFSET offset]
                            let limit_val = if let Some(limit_expr) = limit {
                                self.extract_number_from_expr(limit_expr).ok_or_else(|| {
                                    CoreError::InvalidParam("Invalid limit value".into())
                                })?
                            } else {
                                return Err(CoreError::InvalidParam(
                                    "ORDER BY _nature requires LIMIT".into(),
                                ));
                            };

                            let offset_val = if let Some(offset_obj) = offset {
                                self.extract_number_from_expr(&offset_obj.value)
                                    .ok_or_else(|| {
                                        CoreError::InvalidParam("Invalid offset value".into())
                                    })?
                            } else {
                                0
                            };
                            log::info!("✅ [NaturalOrder] Standard LIMIT syntax parsed: limit={}, offset={}", limit_val, offset_val);
                            (limit_val, offset_val)
                        }
                    }
                } else {
                    return Err(CoreError::InvalidParam(
                        "ORDER BY _nature requires LIMIT".into(),
                    ));
                };

                Ok((
                    table_name,
                    limit,
                    offset,
                    where_clause,
                    projection_fields,
                    is_select_star,
                ))
            } else {
                Err(CoreError::InvalidParam(
                    "Only SELECT statements are supported".into(),
                ))
            }
        } else {
            Err(CoreError::InvalidParam(
                "Only SELECT statements are supported".into(),
            ))
        }
    }

    /// 执行自然顺序查询 (流式版本，通过 channel 传输数据)
    ///
    /// # 参数
    /// * `sql` - 完整的 SQL 查询
    ///
    /// # 返回
    /// * `StreamingQueryResult` - 包含 receiver channel 和 schema
    ///
    /// # 示例
    /// ```rust
    /// let result = executor.execute_natural_cursor(sql).await?;
    /// while let Some(batch_result) = result.receiver.recv().await {
    ///     let batch = batch_result?;
    ///     // 处理每个 batch
    /// }
    /// ```
    pub async fn execute_natural_cursor(&self, sql: &str) -> CoreResult<StreamingQueryResult> {
        // 解析 SQL 提取参数
        let (table_name, limit, offset, _where_clause, projection_fields, is_select_star) =
            self.parse_sql(sql)?;

        log::info!(
            "🌊 [NaturalCursor] Streaming query: table={}, limit={}, offset={}",
            table_name,
            limit,
            offset
        );

        // 构建元数据索引
        let segment_metas = self.build_segment_metadata(&table_name).await?;

        // 计算需要读取的 segments
        let target_segments = self.calculate_target_segments(&segment_metas, offset, limit)?;

        log::info!(
            "🎯 [NaturalCursor] Will stream from {} segments",
            target_segments.len()
        );

        // 获取 schema
        let schema = self.get_table_schema(&table_name).await?;

        // 创建 channel (缓冲 10 个 batch，避免生产者过快)
        let (tx, rx) = mpsc::channel::<CoreResult<RecordBatch>>(10);
        let matched_docs = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let matched_docs_clone = matched_docs.clone();

        // 克隆需要的数据
        let engine = self.engine.clone();
        let table_name = table_name.clone();
        let sql = sql.to_string();
        let projection_fields = projection_fields.to_vec();

        // 在后台任务中流式发送数据
        tokio::spawn(async move {
            let executor = NaturalOrderExecutor::new(engine);

            for (partition_name, segment_id, skip, read_count) in target_segments {
                log::debug!(
                    "📦 [NaturalCursor] Streaming segment {}/{}: skip={}, read={}",
                    partition_name,
                    segment_id,
                    skip,
                    read_count
                );

                // 读取这个 segment 的数据
                match executor
                    .read_single_segment(
                        &table_name,
                        &sql,
                        &partition_name,
                        &segment_id,
                        skip,
                        read_count,
                        &projection_fields,
                        is_select_star,
                        false, // force_full_projection
                        &[],   // filter_exprs
                    )
                    .await
                {
                    Ok(batch) => {
                        let rows = batch.num_rows();
                        matched_docs_clone.fetch_add(rows, std::sync::atomic::Ordering::Relaxed);

                        // 发送 batch (如果 channel 满了会阻塞，实现背压)
                        if tx.send(Ok(batch)).await.is_err() {
                            log::warn!("🚫 [NaturalCursor] Receiver dropped, stopping stream");
                            break;
                        }
                    }
                    Err(e) => {
                        log::error!("❌ [NaturalCursor] Segment read failed: {}", e);
                        let _ = tx.send(Err(e)).await;
                        break;
                    }
                }
            }

            log::info!("✅ [NaturalCursor] Stream completed");
        });

        Ok(StreamingQueryResult {
            receiver: rx,
            schema,
            matched_docs,
        })
    }

    /// 执行自然顺序查询 (兼容旧版本，一次性返回所有数据)
    ///
    /// # 参数
    /// * `sql` - 完整的 SQL 查询 (包含 ORDER BY _nature)
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

    /// 获取表的 Arrow Schema
    async fn get_table_schema(&self, table_name: &str) -> CoreResult<Arc<ArrowSchema>> {
        let partition_names = self.engine.list_partitions(table_name).await;
        let first_partition_name = partition_names.into_iter().next().ok_or_else(|| {
            CoreError::NotExisted(format!("No partitions found for table '{}'", table_name))
        })?;

        let partition = self
            .engine
            .get_partition(table_name, &first_partition_name)
            .await
            .ok_or_else(|| {
                CoreError::NotExisted(format!("Partition {} not found", first_partition_name))
            })?;

        Ok(partition.schema().to_arrow_schema())
    }

    /// 读取单个 segment 的数据 (用于流式查询)
    async fn read_single_segment(
        &self,
        table_name: &str,
        _sql: &str,
        partition_name: &str,
        segment_id: &str,
        skip: usize,
        read_count: usize,
        projection_fields: &[String],
        is_select_star: bool,
        _force_full_projection: bool,
        _filter_exprs: &[datafusion::logical_expr::Expr],
    ) -> CoreResult<RecordBatch> {
        use crate::compute::table_provider::segment_scanner::SegmentScanner;

        let partition = self
            .engine
            .get_partition(table_name, partition_name)
            .await
            .ok_or_else(|| {
                CoreError::NotExisted(format!("Partition {} not found", partition_name))
            })?;

        // 获取 segment 数据
        let (schema, index_readers, doc_count, deleted, row_data) = if segment_id == "current" {
            let current_segment = partition.get_current_segment();
            let schema = partition.arrow_schema.clone();
            let index_readers = current_segment.get_index_readers();
            let doc_count = current_segment.doc_count();
            let deleted = current_segment.get_deleted();
            let row_data = current_segment.get_row_data();
            drop(current_segment);
            (schema, index_readers, doc_count, deleted, row_data)
        } else {
            let segment_id_u64: u64 = segment_id
                .parse()
                .map_err(|_| CoreError::Internal(format!("Invalid segment ID: {}", segment_id)))?;

            let frozen_segments = partition.get_frozen_segments();
            let segment = frozen_segments
                .iter()
                .find(|(id, _)| *id == segment_id_u64)
                .map(|(_, seg)| seg.clone())
                .ok_or_else(|| {
                    CoreError::NotExisted(format!("Segment {} not found", segment_id))
                })?;

            let schema = partition.arrow_schema.clone();
            let index_readers = segment.get_index_readers();
            let doc_count = segment.doc_count();
            let deleted = segment.get_deleted();
            let row_data = segment.get_row_data();
            drop(frozen_segments);
            (schema, index_readers, doc_count, deleted, row_data)
        };

        // 使用 SegmentScanner 直接扫描
        let scanner =
            SegmentScanner::new(schema.clone(), row_data, index_readers, doc_count, deleted);

        // 🚀 构建投影列索引
        let projection = if is_select_star {
            None // SELECT * 读取所有列
        } else {
            // 根据字段名找到列索引
            let mut indices = Vec::new();
            for field_name in projection_fields {
                if let Some((idx, _)) = schema
                    .fields()
                    .iter()
                    .enumerate()
                    .find(|(_, f)| f.name() == field_name)
                {
                    indices.push(idx);
                }
            }
            if indices.is_empty() {
                None // 如果没有找到任何列，读取所有列
            } else {
                Some(indices)
            }
        };

        // 扫描数据
        let batch = scanner.scan_with_skip_and_limit(&[], skip, read_count, projection.as_ref())?;

        Ok(batch)
    }
}

#[cfg(test)]
mod tests {
    // 注意：这些测试需要重构，因为 Engine 不再有 new_in_memory 方法
    // TODO: 使用正确的 Engine::new() 方法重写测试

    // 暂时注释掉所有测试
}
