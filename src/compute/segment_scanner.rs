use std::{
    any::Any,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use crate::segment::{IndexReader, RowDataStore};
use ahash::HashMap;
use datafusion::scalar::ScalarValue;
use datafusion::{
    arrow::{datatypes::SchemaRef, record_batch::RecordBatch},
    error::Result as DFResult,
    execution::SendableRecordBatchStream,
    logical_expr::Expr,
    physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, RecordBatchStream},
};
use roaring::RoaringBitmap;

/// SegmentScanner - 内部工具类,用于扫描单个 Segment
///
/// 不实现 TableProvider - Segment 不直接对外暴露,最小查询单位是 Partition
pub(crate) struct SegmentScanner {
    schema: SchemaRef,
    raw_data: RowDataStore,
    index_readers: HashMap<String, Box<dyn IndexReader>>,
    doc_count: u32,
    /// 有效文档的 bitmap (doc_count - del)，预先计算避免每次都重新生成
    valid_docs: RoaringBitmap,
}

impl std::fmt::Debug for SegmentScanner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SegmentScanner")
            .field("doc_count", &self.doc_count)
            .finish()
    }
}

impl SegmentScanner {
    pub fn new(
        schema: SchemaRef,
        raw_data: RowDataStore,
        index_readers: HashMap<String, Box<dyn IndexReader>>,
        doc_count: u32,
        del: RoaringBitmap,
    ) -> Self {
        // Commented for cleaner logs
        // println!("[DEBUG] SegmentScanner::new() called");
        // println!("[DEBUG] Received {} index_readers: {:?}", index_readers.len(), index_readers.keys().collect::<Vec<_>>());

        // 预先计算有效文档的 bitmap (只计算一次)
        let mut valid_docs = RoaringBitmap::new();
        valid_docs.insert_range(0..doc_count);
        valid_docs -= &del;

        // 直接使用原始schema,不需要添加 _internal_id
        Self {
            schema: schema.clone(),
            raw_data,
            index_readers,
            doc_count,
            valid_docs,
        }
    }

    /// 创建执行计划 - 主入口方法
    ///
    /// 应用 filters，计算命中的文档，然后构建执行计划
    ///
    /// # 优化策略
    /// 1. 如果有 ORDER BY + LIMIT，尝试使用索引有序扫描
    /// 2. 否则使用普通的 bitmap 扫描
    pub fn create_plan(
        &self,
        filters: &[Expr],
        projection: Option<&Vec<usize>>,
        limit: Option<usize>,
        sort: Option<(String, bool)>,
    ) -> Option<Arc<dyn ExecutionPlan>> {
        log::info!(
            "🔍 [SegmentScanner::create_plan] filters={:?}, projection={:?}, limit={:?}, sort={:?}",
            filters,
            projection,
            limit,
            sort
        );

        // 应用 filters，计算命中的文档
        let result_bitmap = self.apply_filters(filters)?;

        log::info!(
            "🎯 [SegmentScanner] hit={}/{} ({:.1}%), limit={:?}, sort={:?}",
            result_bitmap.len(),
            self.valid_docs.len(),
            result_bitmap.len() as f64 / self.valid_docs.len() as f64 * 100.0,
            limit,
            sort
        );

        // 尝试使用索引有序扫描优化
        if let Some((field_name, ascending)) = &sort {
            if let Some(limit_val) = limit {
                // 判断是否应该使用有序扫描
                if self.should_use_ordered_scan(&result_bitmap, limit_val) {
                    // 尝试使用索引有序扫描
                    if let Some(ordered_plan) = self.try_ordered_scan(
                        field_name,
                        *ascending,
                        &result_bitmap,
                        projection,
                        limit_val,
                    ) {
                        log::info!(
                            "🚀 [SegmentScanner] Using index ordered scan for ORDER BY {} {}",
                            field_name,
                            if *ascending { "ASC" } else { "DESC" }
                        );
                        return Some(ordered_plan);
                    }
                } else {
                    log::info!(
                        "⏭️  [SegmentScanner] Skipping ordered scan: hit_ratio={:.1}%, hit_count={}, limit={}",
                        result_bitmap.len() as f64 / self.valid_docs.len() as f64 * 100.0,
                        result_bitmap.len(),
                        limit_val
                    );
                }
            }
        }

        // 否则使用普通的 bitmap 扫描
        self.build_exec_plan(result_bitmap, projection, sort, limit)
    }

    /// 应用过滤条件，返回命中的文档bitmap
    fn apply_filters(&self, filters: &[Expr]) -> Option<RoaringBitmap> {
        let mut result_bitmap = self.valid_docs.clone();

        for filter in filters {
            if let Some(bitmap) = self.expr_to_bitmap(filter) {
                result_bitmap &= bitmap;
            }
        }

        if result_bitmap.is_empty() {
            None
        } else {
            Some(result_bitmap)
        }
    }

    /// 判断是否应该使用有序扫描
    ///
    /// # 核心思想
    /// 有序扫描的优势：只需要读取 LIMIT 条数据，而不是所有命中的数据
    /// 但前提是：命中率不能太低，否则需要遍历太多索引 keys
    ///
    /// # 规则（3 条）
    /// 1. 命中率 >= 0.1%：避免遍历太多索引 keys
    /// 2. 命中数 >= 1000：确保排序开销足够大
    /// 3. LIMIT < 命中数 × 10%：确保有序扫描只读少量数据
    ///
    /// # Arguments
    /// * `result_bitmap` - 过滤后的文档 bitmap
    /// * `limit` - LIMIT 值
    fn should_use_ordered_scan(&self, result_bitmap: &RoaringBitmap, limit: usize) -> bool {
        let hit_count = result_bitmap.len() as usize;
        let total_count = self.valid_docs.len() as usize;
        let hit_ratio = hit_count as f64 / total_count as f64;

        // 规则 1：命中率必须 >= 0.1% (千分之一)
        // 原因：命中率太低时，有序扫描需要遍历太多索引 keys
        //
        // 例子：
        // ❌ 总数 1000万，命中 1万 (0.1%)，LIMIT 100
        //    如果索引有 100万 个不同值，每个值平均 10 个文档
        //    需要遍历 ~10000 个 keys 才能找到 100 条（太慢）
        //
        // ✅ 总数 1000万，命中 10万 (1%)，LIMIT 100
        //    需要遍历 ~1000 个 keys 就能找到 100 条（可接受）
        if hit_ratio < 0.001 {
            return false;
        }

        // 规则 2：命中数必须 >= 1000
        // 原因：命中数太少时，排序开销很小
        //       例如：1000 条数据排序只需要 ~10000 次比较，非常快
        if hit_count < 1000 {
            return false;
        }

        // 规则 3：LIMIT 必须 < 命中数的 10%
        // 原因：有序扫描的优势在于只读取 LIMIT 条数据
        //       如果 LIMIT 太大（接近命中数），优势不明显
        //
        // 例子：
        // ✅ 命中 10000 条，LIMIT 100 (1%)：读取 ~100 条 vs 10000 条（100x 提升）
        // ❌ 命中 10000 条，LIMIT 2000 (20%)：读取 ~2000 条 vs 10000 条（5x 提升，不值得）
        let limit_ratio = limit as f64 / hit_count as f64;
        if limit_ratio >= 0.1 {
            return false;
        }

        true
    }

    /// 尝试使用索引有序扫描（ORDER BY 优化）
    ///
    /// # Arguments
    /// * `field_name` - 排序字段名
    /// * `ascending` - 是否升序
    /// * `filter_bitmap` - 过滤后的文档 bitmap
    /// * `projection` - 投影列
    /// * `limit` - LIMIT 值
    ///
    /// # Returns
    /// 如果字段有索引且支持有序扫描，返回优化的执行计划；否则返回 None
    fn try_ordered_scan(
        &self,
        field_name: &str,
        ascending: bool,
        filter_bitmap: &RoaringBitmap,
        projection: Option<&Vec<usize>>,
        limit: usize,
    ) -> Option<Arc<dyn ExecutionPlan>> {
        // 检查字段是否有索引
        let index_reader = self.index_readers.get(field_name)?;

        // 尝试使用索引的有序扫描
        let ordered_doc_ids =
            index_reader.scan_ordered(ascending, Some(filter_bitmap), Some(limit))?;

        if ordered_doc_ids.is_empty() {
            return None;
        }

        log::info!(
            "🎯 [OrderedScan] Field '{}' returned {} docs (limit={})",
            field_name,
            ordered_doc_ids.len(),
            limit
        );

        // 将有序的 doc_ids 转换为 bitmap
        let ordered_bitmap = RoaringBitmap::from_sorted_iter(ordered_doc_ids.into_iter())
            .unwrap_or_else(|_| RoaringBitmap::new());

        // 构建执行计划
        let projected_schema = self.build_projected_schema(projection);
        let exec = SegmentExec::new(
            self.raw_data.clone(),
            self.schema.clone(),
            projected_schema,
            ordered_bitmap,
            projection.map(|p| p.to_vec()),
            Some((field_name.to_string(), ascending)),
            Some(limit),
        );

        Some(Arc::new(exec))
    }

    /// 构建执行计划（通用方法）
    fn build_exec_plan(
        &self,
        result_bitmap: RoaringBitmap,
        projection: Option<&Vec<usize>>,
        sort: Option<(String, bool)>,
        limit: Option<usize>,
    ) -> Option<Arc<dyn ExecutionPlan>> {
        let projected_schema = self.build_projected_schema(projection);

        let exec = SegmentExec::new(
            self.raw_data.clone(),
            self.schema.clone(),
            projected_schema,
            result_bitmap,
            projection.map(|p| p.to_vec()),
            sort,
            limit,
        );

        Some(Arc::new(exec))
    }

    /// 构建投影后的schema
    fn build_projected_schema(&self, projection: Option<&Vec<usize>>) -> SchemaRef {
        match projection {
            Some(indices) if !indices.is_empty() => {
                let fields: Vec<_> = indices
                    .iter()
                    .map(|i| self.schema.field(*i).clone())
                    .collect();
                Arc::new(datafusion::arrow::datatypes::Schema::new(fields))
            }
            Some(_) => Arc::new(datafusion::arrow::datatypes::Schema::empty()),
            None => self.schema.clone(),
        }
    }

    fn expr_to_bitmap(&self, expr: &Expr) -> Option<RoaringBitmap> {
        match expr {
            // BETWEEN 表达式: col BETWEEN start AND end
            Expr::Between(between) => {
                if let Expr::Column(column) = &*between.expr {
                    let field_name = &column.name;

                    if let (Expr::Literal(low, _), Expr::Literal(high, _)) =
                        (&*between.low, &*between.high)
                    {
                        // BETWEEN 是闭区间 [low, high]
                        // NOT BETWEEN 的话需要取反
                        let bitmap = self.query_range(field_name, low, true, high, true)?;
                        return Some(if between.negated {
                            // NOT BETWEEN: 返回补集
                            // 这需要知道全集，暂时返回 None
                            return None;
                        } else {
                            bitmap
                        });
                    }
                }
                None
            }

            // 二元表达式: col > value, col = value 等
            Expr::BinaryExpr(binary) => {
                use datafusion::logical_expr::Operator;

                // 先处理 AND/OR 逻辑运算
                match binary.op {
                    Operator::And => {
                        // AND: 两边都需要 bitmap
                        let left_opt = self.expr_to_bitmap(&binary.left);
                        let right_opt = self.expr_to_bitmap(&binary.right);

                        // 使用引用进行位运算，避免不必要的克隆
                        let result = match (left_opt.as_ref(), right_opt.as_ref()) {
                            (Some(left), Some(right)) => left & right,
                            (Some(left), None) => left & &self.valid_docs,
                            (None, Some(right)) => &self.valid_docs & right,
                            (None, None) => self.valid_docs.clone(),
                        };
                        return Some(result);
                    }
                    Operator::Or => {
                        // OR: 任何一边无法处理，返回全量
                        let left_opt = self.expr_to_bitmap(&binary.left);
                        let right_opt = self.expr_to_bitmap(&binary.right);

                        // 使用引用进行位运算，避免不必要的克隆
                        let result = match (left_opt.as_ref(), right_opt.as_ref()) {
                            (Some(left), Some(right)) => left | right,
                            (Some(left), None) => left | &self.valid_docs,
                            (None, Some(right)) => &self.valid_docs | right,
                            (None, None) => self.valid_docs.clone(),
                        };
                        return Some(result);
                    }
                    _ => {}
                }

                if let Expr::Column(column) = &*binary.left {
                    let field_name = &column.name;

                    match &*binary.right {
                        Expr::Literal(scalar_value, _) => {
                            match binary.op {
                                Operator::Eq => {
                                    return self.query_equal(field_name, scalar_value);
                                }
                                // col > value -> range(value, false, +∞, true)
                                Operator::Gt => {
                                    return self.query_range(
                                        field_name,
                                        scalar_value,
                                        false,
                                        &ScalarValue::Null,
                                        true,
                                    );
                                }
                                // col >= value -> range(value, true, +∞, true)
                                Operator::GtEq => {
                                    return self.query_range(
                                        field_name,
                                        scalar_value,
                                        true,
                                        &ScalarValue::Null,
                                        true,
                                    );
                                }
                                // col < value -> range(-∞, true, value, false)
                                Operator::Lt => {
                                    return self.query_range(
                                        field_name,
                                        &ScalarValue::Null,
                                        true,
                                        scalar_value,
                                        false,
                                    );
                                }
                                // col <= value -> range(-∞, true, value, true)
                                Operator::LtEq => {
                                    return self.query_range(
                                        field_name,
                                        &ScalarValue::Null,
                                        true,
                                        scalar_value,
                                        true,
                                    );
                                }
                                _ => {}
                            }
                        }
                        _ => {}
                    }
                }
                None
            }
            _ => None,
        }
    }
    /// 查询等值条件
    /// 返回 None 的情况：
    /// 1. 字段没有索引（index_readers 中不存在）- 返回空 bitmap
    /// 2. IndexReader 无法处理这个查询（如 Keyword 类型查询数字）- 返回空 bitmap
    ///
    /// 注意: 返回 None 会导致全表扫描,所以即使没有索引也应该返回空 bitmap
    fn query_equal(
        &self,
        field_name: &str,
        value: &datafusion::scalar::ScalarValue,
    ) -> Option<RoaringBitmap> {
        match self.index_readers.get(field_name) {
            Some(reader) => {
                // 有索引,调用 reader.query()
                // 如果查询失败(类型不匹配等),返回空 bitmap
                Some(reader.query(value).unwrap_or_else(RoaringBitmap::new))
            }
            None => Some(RoaringBitmap::new()),
        }
    }

    /// 查询范围条件
    /// 返回 None 的情况：
    /// 1. 字段没有索引（index_readers 中不存在）- 返回空 bitmap
    /// 2. 字段类型不支持范围查询（如 Keyword）- 返回空 bitmap
    ///
    /// 注意: 返回 None 会导致全表扫描,所以即使没有索引也应该返回空 bitmap
    fn query_range(
        &self,
        field_name: &str,
        start: &ScalarValue,
        start_inclusive: bool,
        end: &ScalarValue,
        end_inclusive: bool,
    ) -> Option<RoaringBitmap> {
        match self.index_readers.get(field_name) {
            Some(reader) => {
                log::info!(
                    "🔍 [SegmentScanner::query_range] field={}, trying range_union first",
                    field_name
                );

                // 优先尝试 range_union() - 100-200x faster for large ranges
                if let Some(bitmap) = reader.range_union(start, start_inclusive, end, end_inclusive)
                {
                    log::info!(
                        "🔍 [SegmentScanner::query_range] range_union returned {} docs",
                        bitmap.len()
                    );
                    return Some(bitmap);
                }

                log::info!("🔍 [SegmentScanner::query_range] range_union returned None, falling back to range()");

                // 回退到普通 range() - 兼容不支持 range_union 的索引类型
                Some(
                    reader
                        .range(start, start_inclusive, end, end_inclusive)
                        .unwrap_or_else(RoaringBitmap::new),
                )
            }
            None => Some(RoaringBitmap::new()),
        }
    }
}

/// SegmentExec - 自定义的 ExecutionPlan
///
/// 只扫描 matched_docs bitmap 中的 doc_ids
struct SegmentExec {
    raw_data: RowDataStore,
    #[allow(dead_code)]
    full_schema: SchemaRef, // 完整 schema (用于读取数据)
    projected_schema: SchemaRef, // 投影后的 schema (返回给 DataFusion)
    matched_docs: RoaringBitmap,
    projection: Option<Vec<usize>>, // 投影列索引
    /// 执行计划属性，在构造时创建并存储，避免每次调用 properties() 都创建
    properties: datafusion::physical_plan::PlanProperties,
    sort: Option<(String, bool)>,
    limit: Option<usize>,
}

impl SegmentExec {
    fn new(
        raw_data: RowDataStore,
        full_schema: SchemaRef,
        projected_schema: SchemaRef,
        matched_docs: RoaringBitmap,
        projection: Option<Vec<usize>>,
        sort: Option<(String, bool)>,
        limit: Option<usize>,
    ) -> Self {
        use datafusion::physical_expr::EquivalenceProperties;
        use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
        use datafusion::physical_plan::Partitioning;

        // 在构造时就创建 PlanProperties（只创建一次）
        // 注意: 使用 projected_schema 作为输出 schema
        let eq_properties = EquivalenceProperties::new(projected_schema.clone());
        let partitioning = Partitioning::UnknownPartitioning(1);
        // EmissionType::Final 表示 batch 之间没有重复的行，也没有更新关系
        // 每个 batch 包含不同的 doc_ids，是最终的、完整的数据
        let emission_type = EmissionType::Final;
        let boundedness = Boundedness::Bounded; // 有界数据（不是无限流）

        let properties = datafusion::physical_plan::PlanProperties::new(
            eq_properties,
            partitioning,
            emission_type,
            boundedness,
        );

        Self {
            raw_data,
            full_schema,
            projected_schema,
            matched_docs,
            projection,
            properties,
            sort,
            limit,
        }
    }
}
impl std::fmt::Debug for SegmentExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SegmentExec")
            .field("projected_schema", &self.projected_schema)
            .field("matched_docs_count", &self.matched_docs.len())
            .field("projection", &self.projection)
            .finish()
    }
}

impl std::fmt::Display for SegmentExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SegmentExec: matched_docs={}", self.matched_docs.len())
    }
}

impl DisplayAs for SegmentExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "SegmentExec: matched_docs={}", self.matched_docs.len())
    }
}

impl ExecutionPlan for SegmentExec {
    fn name(&self) -> &str {
        "SegmentExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
    }

    fn properties(&self) -> &datafusion::physical_plan::PlanProperties {
        // 直接返回存储在结构体中的 properties 引用
        // 没有内存泄漏，没有每次都创建新对象
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        // 这是叶子节点,没有子节点
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<datafusion::execution::TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        // 🚀 流式处理优化: 使用真正的异步流,按需分批生成数据
        // 避免一次性加载所有数据到内存,防止 OOM

        let projected_schema = self.projected_schema.clone();
        let raw_data = self.raw_data.clone();
        let matched_docs = self.matched_docs.clone();
        let projection = self.projection.clone();

        // LIMIT 下推：如果没有 SORT，在 Segment 层面应用 LIMIT
        let pushdown_limit = if self.sort.is_none() {
            self.limit
        } else {
            None
        };

        // 配置: 每次最多处理多少个 storage batch
        // 这控制了内存使用上限: CHUNK_SIZE × 1000行/batch × 列数 × 数据大小
        const CHUNK_SIZE: usize = 100; // 每次处理100个batch (约10万行)

        eprintln!(
            "🔍 [SegmentExec::execute] Starting streaming execution, total matched_docs={}, pushdown_limit={:?}",
            matched_docs.len(),
            pushdown_limit
        );

        Ok(Box::pin(SegmentStream::new(
            projected_schema,
            raw_data,
            matched_docs,
            projection,
            CHUNK_SIZE,
            pushdown_limit,
        )))
    }
}

/// 真正的流式 RecordBatchStream 实现
///
/// 按需分批读取数据,避免一次性加载全部到内存
struct SegmentStream {
    schema: SchemaRef,
    raw_data: RowDataStore,
    doc_ids_iter: roaring::bitmap::IntoIter, // 使用迭代器，避免一次性分配大 Vec
    projection: Option<Vec<usize>>,
    chunk_size: usize,
    limit: Option<usize>, // LIMIT 下推：如果设置，只返回这么多行

    // 迭代状态
    rows_returned: usize, // 已经返回的行数（用于 LIMIT）
    pending_batches: std::vec::IntoIter<RecordBatch>,
}

impl SegmentStream {
    fn new(
        schema: SchemaRef,
        raw_data: RowDataStore,
        matched_docs: RoaringBitmap,
        projection: Option<Vec<usize>>,
        chunk_size: usize,
        limit: Option<usize>,
    ) -> Self {
        let total_docs = matched_docs.len();

        eprintln!(
            "🔍 [SegmentStream::new] Total doc_ids={}, will process in chunks of {} storage batches, limit={:?}",
            total_docs,
            chunk_size,
            limit
        );

        Self {
            schema,
            raw_data,
            doc_ids_iter: matched_docs.into_iter(), // 使用迭代器，零额外内存
            projection,
            chunk_size,
            limit,
            rows_returned: 0,
            pending_batches: Vec::new().into_iter(),
        }
    }

    /// 生成下一批 RecordBatches
    fn generate_next_chunk(&mut self) -> DFResult<Vec<RecordBatch>> {
        use datafusion::arrow::array::UInt32Array;
        use datafusion::arrow::compute::take;
        use std::collections::HashMap;

        // LIMIT 下推：如果已经返回足够的行，停止生成
        if let Some(limit) = self.limit {
            if self.rows_returned >= limit {
                return Ok(Vec::new());
            }
        }

        // 1. 收集下一个 chunk 的 doc_ids
        // LIMIT 优化：只收集需要的 doc_ids
        let remaining_rows = self.limit.map(|l| l.saturating_sub(self.rows_returned));
        let mut batch_groups: HashMap<u32, Vec<u32>> = HashMap::new();
        let mut batch_keys_set = std::collections::HashSet::new();
        let mut collected_rows = 0;

        // 使用迭代器，按需读取 doc_ids
        for doc_id in self.doc_ids_iter.by_ref() {
            // LIMIT 优化：如果已经收集足够的行，停止
            if let Some(remaining) = remaining_rows {
                if collected_rows >= remaining {
                    break;
                }
            }

            if let Some(batch_start_id) = self.raw_data.get_batch_key_for_doc(doc_id) {
                batch_groups
                    .entry(batch_start_id)
                    .or_insert_with(Vec::new)
                    .push(doc_id);
                batch_keys_set.insert(batch_start_id);
                collected_rows += 1;
            }

            // 达到 chunk_size 个 storage batch,停止收集
            if batch_keys_set.len() >= self.chunk_size {
                break;
            }
        }

        // 没有更多数据
        if batch_groups.is_empty() {
            return Ok(Vec::new());
        }

        let _doc_count = batch_groups.values().map(|v| v.len()).sum::<usize>();

        // 2. 批量读取这一批的 storage batches
        let batch_keys: Vec<u32> = batch_keys_set.into_iter().collect();
        let is_empty_projection = self.projection.as_ref().map_or(false, |p| p.is_empty());

        let source_batches = if is_empty_projection {
            HashMap::new()
        } else {
            let proj_to_use = self.projection.as_ref().map(|p| p.as_slice());
            self.raw_data
                .get_batch_with_projection(&batch_keys, proj_to_use)
        };

        // 3. 生成 RecordBatches
        let mut result_batches = Vec::new();

        for (batch_start_id, doc_ids_in_batch) in batch_groups {
            // 空投影处理
            if is_empty_projection {
                let row_count = doc_ids_in_batch.len();
                if row_count > 0 {
                    if let Ok(batch) = RecordBatch::try_new_with_options(
                        self.schema.clone(),
                        vec![],
                        &datafusion::arrow::record_batch::RecordBatchOptions::new()
                            .with_row_count(Some(row_count)),
                    ) {
                        result_batches.push(batch);
                    }
                }
                continue;
            }

            // 正常投影处理
            if let Some(source_batch) = source_batches.get(&batch_start_id) {
                let mut row_indices: Vec<usize> = doc_ids_in_batch
                    .iter()
                    .filter_map(|&doc_id| {
                        let row_idx = (doc_id - batch_start_id) as usize;
                        if row_idx < source_batch.num_rows() {
                            Some(row_idx)
                        } else {
                            None
                        }
                    })
                    .collect();

                if row_indices.is_empty() {
                    continue;
                }

                row_indices.sort_unstable();

                let indices_array =
                    UInt32Array::from(row_indices.iter().map(|&i| i as u32).collect::<Vec<_>>());

                let final_columns: Vec<Arc<dyn datafusion::arrow::array::Array>> = source_batch
                    .columns()
                    .iter()
                    .filter_map(|col| take(col.as_ref(), &indices_array, None).ok())
                    .collect();

                if let Ok(batch) = RecordBatch::try_new(self.schema.clone(), final_columns) {
                    result_batches.push(batch);
                }
            }
        }

        // 更新已返回的行数（用于 LIMIT 下推）
        let total_rows: usize = result_batches.iter().map(|b| b.num_rows()).sum();
        self.rows_returned += total_rows;

        Ok(result_batches)
    }
}

impl RecordBatchStream for SegmentStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl futures::Stream for SegmentStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // 1. 如果有待处理的 batch,先返回
        if let Some(batch) = self.pending_batches.next() {
            return Poll::Ready(Some(Ok(batch)));
        }

        // 2. 生成下一批数据
        match self.generate_next_chunk() {
            Ok(batches) => {
                if batches.is_empty() {
                    // 没有更多数据
                    Poll::Ready(None)
                } else {
                    // 设置待处理队列
                    self.pending_batches = batches.into_iter();

                    // 立即返回第一个 batch
                    if let Some(batch) = self.pending_batches.next() {
                        Poll::Ready(Some(Ok(batch)))
                    } else {
                        // 理论上不会到这里
                        Poll::Ready(None)
                    }
                }
            }
            Err(e) => Poll::Ready(Some(Err(e))),
        }
    }
}
