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

        // 在 schema 前面添加 _internal_id 字段
        // 这样内部 schema 就与实际数据结构匹配了
        let schema_with_id = {
            use datafusion::arrow::datatypes::{DataType, Field, Schema};

            let mut fields = vec![Arc::new(Field::new(
                "_internal_id",
                DataType::UInt32,
                false,
            ))];
            fields.extend(schema.fields().iter().map(|f| Arc::clone(f)));
            Arc::new(Schema::new(fields))
        };
        Self {
            schema: schema_with_id,
            raw_data,
            index_readers,
            doc_count,
            valid_docs,
        }
    }

    /// 创建 ExecutionPlan - 对 Segment 应用过滤条件
    ///
    /// 这是核心方法,被 PartitionTableProvider 调用
    pub fn create_execution_plan(
        &self,
        filters: &[Expr],
        projection: Option<&Vec<usize>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        // 1. 从预计算的有效文档开始（已排除删除的文档）
        let mut result_bitmap = self.valid_docs.clone();

        // 2. 应用 filters，逐个与结果取交集
        for filter in filters {
            if let Some(bitmap) = self.expr_to_bitmap(filter) {
                result_bitmap &= bitmap;
            }
        }

        // 3. 处理投影
        // 注意: projection 的索引是基于外部 schema (不含 _internal_id)
        // 需要将其转换为内部 schema (含 _internal_id) 的索引
        let internal_projection = projection.and_then(|indices| {
            if indices.is_empty() {
                // 空投影表示不需要任何列(如 COUNT(*))
                None
            } else {
                // 将外部索引 [0, 1, 2] 转换为内部索引 [1, 2, 3]
                // 因为内部 schema 第 0 列是 _internal_id
                Some(indices.iter().map(|&i| i + 1).collect::<Vec<_>>())
            }
        });

        // 4. 计算投影后的 schema (基于内部 schema,但不包含 _internal_id)
        let projected_schema = match &internal_projection {
            Some(indices) => {
                let fields: Vec<_> = indices
                    .iter()
                    .map(|i| self.schema.field(*i).clone())
                    .collect();
                Arc::new(datafusion::arrow::datatypes::Schema::new(fields))
            }
            None => {
                if projection.map_or(false, |p| p.is_empty()) {
                    // 空投影(如 COUNT(*)),返回空 schema
                    Arc::new(datafusion::arrow::datatypes::Schema::empty())
                } else {
                    // 没有投影,返回所有列(但不包括 _internal_id)
                    let fields: Vec<_> = self.schema.fields()[1..].to_vec();
                    Arc::new(datafusion::arrow::datatypes::Schema::new(fields))
                }
            }
        };

        // 5. 创建 ExecutionPlan
        let exec = SegmentExec::new(
            self.raw_data.clone(),
            self.schema.clone(),
            projected_schema,
            result_bitmap,
            internal_projection,
        );

        Ok(Arc::new(exec))
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
    /// 1. 字段没有索引（index_readers 中不存在）
    /// 2. IndexReader 无法处理这个查询（如 Keyword 类型查询数字）
    fn query_equal(
        &self,
        field_name: &str,
        value: &datafusion::scalar::ScalarValue,
    ) -> Option<RoaringBitmap> {
        let result = self
            .index_readers
            .get(field_name)
            .and_then(|reader| reader.query(value));

        result
    }

    /// 查询范围条件
    /// 返回 None 的情况：
    /// 1. 字段没有索引（index_readers 中不存在）
    /// 2. 字段类型不支持范围查询（如 Keyword）
    fn query_range(
        &self,
        field_name: &str,
        start: &ScalarValue,
        start_inclusive: bool,
        end: &ScalarValue,
        end_inclusive: bool,
    ) -> Option<RoaringBitmap> {
        self.index_readers
            .get(field_name)
            .and_then(|reader| reader.range(start, start_inclusive, end, end_inclusive))
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
}

impl SegmentExec {
    fn new(
        raw_data: RowDataStore,
        full_schema: SchemaRef,
        projected_schema: SchemaRef,
        matched_docs: RoaringBitmap,
        projection: Option<Vec<usize>>,
    ) -> Self {
        use datafusion::physical_expr::EquivalenceProperties;
        use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
        use datafusion::physical_plan::Partitioning;

        // 在构造时就创建 PlanProperties（只创建一次）
        // 注意: 使用 projected_schema 作为输出 schema
        let eq_properties = EquivalenceProperties::new(projected_schema.clone());
        let partitioning = Partitioning::UnknownPartitioning(1);
        let emission_type = EmissionType::Final; // 最终输出，不是增量的
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
        use futures::stream;

        // 创建一个简单的流，从 matched_docs 中读取数据
        let projected_schema = self.projected_schema.clone();
        let raw_data = self.raw_data.clone();
        let doc_ids: Vec<u32> = self.matched_docs.iter().collect();
        let projection = self.projection.clone();

        // 将 doc_ids 按批次读取并应用投影
        // Note: RowDataStore 使用 batch 的 start_id 作为 key,
        // 所以需要使用 floor() 来找到包含某个 doc_id 的 batch
        let batches: Vec<RecordBatch> = doc_ids
            .into_iter()
            .filter_map(|doc_id| {
                // 使用 floor() 找到包含 doc_id 的 batch
                raw_data.floor(&doc_id).and_then(|(batch_start_id, batch)| {
                    if batch.num_columns() == 0 {
                        return None;
                    }

                    // 计算 doc_id 在 batch 中的行索引
                    let row_index = (doc_id - batch_start_id) as usize;
                    if row_index >= batch.num_rows() {
                        return None;
                    }

                    // 提取该行的数据并应用投影
                    // 现在 full_schema 包含 _internal_id,所以可以直接按索引投影
                    let final_columns = if projected_schema.fields().is_empty() {
                        // 空 schema (COUNT(*) 等),返回空列
                        vec![]
                    } else if let Some(ref indices) = projection {
                        // 根据投影索引选择列
                        indices
                            .iter()
                            .filter_map(|&i| {
                                batch.columns().get(i).map(|col| col.slice(row_index, 1))
                            })
                            .collect()
                    } else {
                        // 没有投影，返回所有列(跳过 _internal_id,即索引 0)
                        batch.columns()[1..]
                            .iter()
                            .map(|col| col.slice(row_index, 1))
                            .collect()
                    };

                    // 使用投影后的 schema 创建 RecordBatch (单行)
                    RecordBatch::try_new(projected_schema.clone(), final_columns).ok()
                })
            })
            .collect();

        // 创建流
        let stream = stream::iter(batches.into_iter().map(Ok));

        Ok(Box::pin(SegmentStream {
            schema: projected_schema,
            stream: Box::pin(stream),
        }))
    }
}

/// 简单的 RecordBatchStream 实现
struct SegmentStream {
    schema: SchemaRef,
    stream: Pin<Box<dyn futures::Stream<Item = DFResult<RecordBatch>> + Send>>,
}

impl RecordBatchStream for SegmentStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl futures::Stream for SegmentStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.stream.as_mut().poll_next(cx)
    }
}
