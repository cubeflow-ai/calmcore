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
}

impl NaturalOrderExecutor {
    pub fn new(engine: Arc<Engine>) -> Self {
        Self { engine }
    }

    /// 解析 ORDER BY _nature 查询的 SQL
    ///
    /// 返回: (table_name, limit, offset, where_clause, projection_fields, is_select_star)
    fn parse_sql(
        &self,
        sql: &str,
    ) -> CoreResult<(String, usize, usize, Option<String>, Vec<String>, bool)> {
        use regex::Regex;

        let sql_upper = sql.to_uppercase();

        // 提取表名: FROM <table_name>
        let table_re = Regex::new(r"FROM\s+([a-zA-Z0-9_]+)").unwrap();
        let table_name = table_re
            .captures(sql)
            .and_then(|caps| caps.get(1))
            .map(|m| m.as_str().to_string())
            .ok_or_else(|| CoreError::InvalidParam("Cannot find table name in SQL".into()))?;

        // 提取 LIMIT
        let limit_re = Regex::new(r"LIMIT\s+(\d+)").unwrap();
        let limit = limit_re
            .captures(&sql_upper)
            .and_then(|caps| caps.get(1))
            .and_then(|m| m.as_str().parse::<usize>().ok())
            .ok_or_else(|| CoreError::InvalidParam("ORDER BY _nature requires LIMIT".into()))?;

        // 提取 OFFSET (可选)
        let offset_re = Regex::new(r"OFFSET\s+(\d+)").unwrap();
        let offset = offset_re
            .captures(&sql_upper)
            .and_then(|caps| caps.get(1))
            .and_then(|m| m.as_str().parse::<usize>().ok())
            .unwrap_or(0);

        // 提取 SELECT 字段
        let select_re = Regex::new(r"SELECT\s+(.+?)\s+FROM").unwrap();
        let (projection_fields, is_select_star) = if let Some(caps) = select_re.captures(sql) {
            let fields_str = caps.get(1).map(|m| m.as_str()).unwrap_or("*");
            if fields_str.trim() == "*" {
                (vec![], true)
            } else {
                let fields: Vec<String> = fields_str
                    .split(',')
                    .map(|f| f.trim().to_string())
                    .collect();
                (fields, false)
            }
        } else {
            (vec![], true)
        };

        // 提取 WHERE 子句 (可选)
        let where_re = Regex::new(r"WHERE\s+(.+?)(?:\s+ORDER\s+BY|\s+LIMIT|$)").unwrap();
        let where_clause = where_re
            .captures(sql)
            .and_then(|caps| caps.get(1))
            .map(|m| m.as_str().trim().to_string());

        Ok((
            table_name,
            limit,
            offset,
            where_clause,
            projection_fields,
            is_select_star,
        ))
    }

    /// 执行自然顺序查询
    ///
    /// # 参数
    /// * `sql` - 完整的 SQL 查询 (包含 ORDER BY _nature)
    ///
    /// # 示例
    /// ```sql
    /// SELECT app_name, timestamp FROM table WHERE app_name='test' ORDER BY _nature LIMIT 1000 OFFSET 1000000;
    /// ```
    pub async fn execute_natural_order(&self, sql: &str) -> CoreResult<QueryResult> {
        let total_start = std::time::Instant::now();

        // 解析 SQL 提取参数
        let (table_name, limit, offset, where_clause, projection_fields, is_select_star) =
            self.parse_sql(sql)?;

        log::info!(
            "🌿 [NaturalOrder] Executing natural order query: table={}, limit={}, offset={}, where={:?}, projection={:?}, is_select_star={}",
            table_name,
            limit,
            offset,
            where_clause,
            projection_fields,
            is_select_star
        );

        // 第一步：构建元数据索引
        let step1_start = std::time::Instant::now();
        let segment_metas = self.build_segment_metadata(&table_name).await?;
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
            .read_from_segments(
                &table_name,
                sql,
                &target_segments,
                where_clause.as_deref(),
                &projection_fields,
                is_select_star,
            )
            .await?;
        log::info!(
            "⏱️  [NaturalOrder] Step 3 (read segments): {:?}",
            step3_start.elapsed()
        );

        let matched_docs = batches.iter().map(|b| b.num_rows()).sum();

        let step4_start = std::time::Instant::now();
        let final_batch = if batches.is_empty() {
            self.create_empty_batch(&table_name).await?
        } else {
            self.concat_batches(batches)?
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
    /// * `projection_fields` - 投影字段列表
    /// * `is_select_star` - 是否是 SELECT *
    async fn read_from_segments(
        &self,
        table_name: &str,
        sql: &str,
        target_segments: &[(String, String, usize, usize)],
        where_clause: Option<&str>,
        projection_fields: &[String],
        is_select_star: bool,
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
                    projection_fields,
                    is_select_star,
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
    /// 核心优化：通过 bitmap 跳过不需要的行，支持列裁剪
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
        projection_fields: &[String],
        is_select_star: bool,
    ) -> CoreResult<RecordBatch> {
        use crate::compute::table_provider::segment_scanner::SegmentScanner;

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

        // 🚀 核心优化：使用 SegmentScanner 的 scan_with_skip 并应用投影下推
        let batch =
            scanner.scan_with_skip_and_limit(&filters, skip, read_count, projection.as_ref())?;

        Ok(batch)
    }

    /// 从 WHERE SQL 文本解析成 DataFusion 过滤器表达式
    fn parse_where_filters(
        &self,
        where_sql: &str,
        _schema: &Arc<ArrowSchema>,
    ) -> CoreResult<Vec<datafusion::logical_expr::Expr>> {
        use datafusion::sql::sqlparser::dialect::GenericDialect;
        use datafusion::sql::sqlparser::parser::Parser;

        // 构造一个临时的 SELECT 语句来解析 WHERE 条件
        let temp_sql = format!("SELECT * FROM dummy WHERE {}", where_sql);

        // 解析 SQL
        let dialect = GenericDialect {};
        let statements = Parser::parse_sql(&dialect, &temp_sql)
            .map_err(|e| CoreError::Internal(format!("Failed to parse WHERE clause: {}", e)))?;

        if statements.is_empty() {
            return Ok(Vec::new());
        }

        // 从 AST 提取 WHERE 表达式
        use datafusion::sql::sqlparser::ast::{SetExpr, Statement};
        if let Statement::Query(query) = &statements[0] {
            if let SetExpr::Select(select) = query.body.as_ref() {
                if let Some(selection) = &select.selection {
                    // 将 sqlparser 的 Expr 转换为 DataFusion 的 Expr
                    // 使用简单的转换策略
                    match self.convert_sql_expr_to_df_expr(selection) {
                        Ok(expr) => {
                            log::info!("✅ [NaturalOrder] Parsed WHERE filter: {:?}", expr);
                            return Ok(vec![expr]);
                        }
                        Err(e) => {
                            log::warn!("⚠️  Failed to convert WHERE expression: {}, returning empty filters", e);
                            return Ok(Vec::new());
                        }
                    }
                }
            }
        }

        Ok(Vec::new())
    }

    /// 将 sqlparser 的 Expr 转换为 DataFusion 的 Expr
    fn convert_sql_expr_to_df_expr(
        &self,
        sql_expr: &datafusion::sql::sqlparser::ast::Expr,
    ) -> CoreResult<datafusion::logical_expr::Expr> {
        use datafusion::logical_expr::{col, lit};
        use datafusion::sql::sqlparser::ast::Expr as SqlExpr;

        match sql_expr {
            // 二元操作: a = b, a > b, etc.
            SqlExpr::BinaryOp { left, op, right } => {
                let left_expr = self.convert_sql_expr_to_df_expr(left)?;
                let right_expr = self.convert_sql_expr_to_df_expr(right)?;

                use datafusion::sql::sqlparser::ast::BinaryOperator;
                let df_expr = match op {
                    BinaryOperator::Eq => left_expr.eq(right_expr),
                    BinaryOperator::NotEq => left_expr.not_eq(right_expr),
                    BinaryOperator::Lt => left_expr.lt(right_expr),
                    BinaryOperator::LtEq => left_expr.lt_eq(right_expr),
                    BinaryOperator::Gt => left_expr.gt(right_expr),
                    BinaryOperator::GtEq => left_expr.gt_eq(right_expr),
                    BinaryOperator::And => left_expr.and(right_expr),
                    BinaryOperator::Or => left_expr.or(right_expr),
                    _ => {
                        return Err(CoreError::Internal(format!(
                            "Unsupported binary operator: {:?}",
                            op
                        )))
                    }
                };
                Ok(df_expr)
            }

            // 列引用
            SqlExpr::Identifier(ident) => Ok(col(&ident.value)),

            // 值字面量（新版本使用 ValueWithSpan）
            SqlExpr::Value(value_with_span) => {
                use datafusion::sql::sqlparser::ast::Value;
                match &value_with_span.value {
                    Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => {
                        Ok(lit(s.clone()))
                    }
                    Value::Number(n, _) => {
                        // 尝试解析为 i64
                        if let Ok(num) = n.parse::<i64>() {
                            Ok(lit(num))
                        } else if let Ok(num) = n.parse::<f64>() {
                            Ok(lit(num))
                        } else {
                            Err(CoreError::Internal(format!("Invalid number: {}", n)))
                        }
                    }
                    _ => Err(CoreError::Internal(format!(
                        "Unsupported value type: {:?}",
                        value_with_span
                    ))),
                }
            }

            // 其他类型暂不支持
            _ => Err(CoreError::Internal(format!(
                "Unsupported SQL expression type: {:?}",
                sql_expr
            ))),
        }
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

    /// 合并多个 RecordBatch
    fn concat_batches(&self, batches: Vec<RecordBatch>) -> CoreResult<RecordBatch> {
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

    /// 创建空 RecordBatch（从第一个 segment 获取 schema）
    async fn create_empty_batch(&self, table_name: &str) -> CoreResult<RecordBatch> {
        use datafusion::arrow::array::{new_empty_array, ArrayRef};

        // 获取第一个 partition 和 segment
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

        // 获取 schema
        let arrow_schema = partition.schema().to_arrow_schema();

        // 创建空数组
        let empty_columns: Vec<ArrayRef> = arrow_schema
            .fields()
            .iter()
            .map(|field| new_empty_array(field.data_type()))
            .collect();

        RecordBatch::try_new(arrow_schema, empty_columns)
            .map_err(|e| CoreError::Internal(format!("Failed to create empty batch: {}", e)))
    }
}

#[cfg(test)]
mod tests {
    // 注意：这些测试需要重构，因为 Engine 不再有 new_in_memory 方法
    // TODO: 使用正确的 Engine::new() 方法重写测试

    // 暂时注释掉所有测试
}
