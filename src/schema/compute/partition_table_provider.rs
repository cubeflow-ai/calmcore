use std::{any::Any, sync::Arc};

use crate::segment::{IndexReader, Segment};
use ahash::HashMap;
use async_trait::async_trait;
use datafusion::scalar::ScalarValue;
use datafusion::{
    arrow::datatypes::SchemaRef,
    catalog::Session,
    datasource::{TableProvider, TableType},
    error::Result as DFResult,
    execution::SendableRecordBatchStream,
    logical_expr::{Expr, TableProviderFilterPushDown},
    physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan},
};
use roaring::RoaringBitmap;

type PartitionRef = Arc<crate::partition::Partition>;

pub struct SegmentTableProvider {
    partition: PartitionRef,
    schema: SchemaRef,
    index_readers: HashMap<String, Box<dyn IndexReader>>,
}

impl std::fmt::Debug for SegmentTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SegmentTableProvider")
            .field("partition", &self.partition.id)
            .finish()
    }
}
impl SegmentTableProvider {
    pub fn new(
        partition: PartitionRef,
        index_readers: HashMap<String, Box<dyn IndexReader>>,
    ) -> Self {
        Self {
            partition,
            index_readers,
            schema: todo!(),
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
                        let left = self.expr_to_bitmap(&binary.left)?;
                        let right = self.expr_to_bitmap(&binary.right)?;
                        return Some(left & right);
                    }
                    Operator::Or => {
                        let left = self.expr_to_bitmap(&binary.left)?;
                        let right = self.expr_to_bitmap(&binary.right)?;
                        return Some(left | right);
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
    fn query_equal(
        &self,
        field_name: &str,
        value: &datafusion::scalar::ScalarValue,
    ) -> Option<RoaringBitmap> {
        self.index_readers
            .get(field_name)
            .and_then(|reader| reader.query(value))
    }

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

#[async_trait]
impl TableProvider for SegmentTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    /// 关键方法: DataFusion 会调用这个方法,并传入 filters (WHERE 条件)
    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        // // 1. 解析 filters,构建 bitmap
        // let mut result_bitmap = RoaringBitmap::new();
        // result_bitmap.insert_range(0..self.segment.doc_count() as u32);

        // for filter in filters {
        //     if let Some(bitmap) = self.expr_to_bitmap(filter) {
        //         result_bitmap &= bitmap;
        //     }
        // }

        // println!("\n=== Filter Pushdown ===");
        // println!("Filters: {:#?}", filters);
        // println!("Matched doc_ids: {} docs", result_bitmap.len());

        // // 2. 创建 ExecutionPlan
        // let exec = SegmentExec::new(self.segment.clone(), self.schema.clone(), result_bitmap);

        // Ok(Arc::new(exec))
        todo!("实现 scan - 返回 ExecutionPlan")
    }

    /// 告诉 DataFusion 我们支持 filter pushdown
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Exact; filters.len()])
    }
}

/// SegmentExec - 自定义的 ExecutionPlan
///
/// 只扫描 matched_docs bitmap 中的 doc_ids
struct SegmentExec {
    segment: Arc<Segment>,
    schema: SchemaRef,
    matched_docs: RoaringBitmap,
}

impl SegmentExec {
    fn new(segment: Arc<Segment>, schema: SchemaRef, matched_docs: RoaringBitmap) -> Self {
        Self {
            segment,
            schema,
            matched_docs,
        }
    }
}

impl std::fmt::Debug for SegmentExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SegmentExec")
            .field("schema", &self.schema)
            .field("matched_docs_count", &self.matched_docs.len())
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
        self.schema.clone()
    }

    fn properties(&self) -> &datafusion::physical_plan::PlanProperties {
        todo!("实现 properties - 返回执行计划的属性")
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
        todo!("实现 execute - 返回 RecordBatchStream,只扫描 matched_docs")
    }
}
