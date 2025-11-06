use std::{any::Any, sync::Arc};

use datafusion::{
    arrow::datatypes::SchemaRef,
    catalog::Session,
    datasource::{TableProvider, TableType},
    error::{DataFusionError, Result},
    execution::{SendableRecordBatchStream, TaskContext},
    logical_expr::{Expr, TableProviderFilterPushDown},
    physical_expr::EquivalenceProperties,
    physical_plan::{
        execution_plan::{Boundedness, EmissionType},
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    },
};

use crate::partition::Partition;

use super::segment_scanner::SegmentScanner;

/// 判断表达式是否可以被索引精确处理
/// 返回 (can_handle, is_exact)
///
/// 被 PartitionTableProvider 用于 supports_filters_pushdown()
fn can_handle_expr(
    expr: &Expr,
    index_readers: &ahash::HashMap<String, Box<dyn crate::segment::IndexReader>>,
) -> (bool, bool) {
    match expr {
        // BETWEEN 表达式
        Expr::Between(between) => {
            if let Expr::Column(column) = &*between.expr {
                // 检查字段是否有索引
                if index_readers.contains_key(&column.name) {
                    // 检查 low 和 high 是否都是字面量
                    let low_is_literal = matches!(&*between.low, Expr::Literal(_, _));
                    let high_is_literal = matches!(&*between.high, Expr::Literal(_, _));

                    if low_is_literal && high_is_literal && !between.negated {
                        return (true, true); // 可以精确处理
                    }
                }
            }
            (false, false) // 无法处理
        }

        // 二元表达式
        Expr::BinaryExpr(binary) => {
            use datafusion::logical_expr::Operator;

            // AND/OR 的处理
            match binary.op {
                Operator::And => {
                    let (left_ok, left_exact) = can_handle_expr(&binary.left, index_readers);
                    let (right_ok, right_exact) = can_handle_expr(&binary.right, index_readers);

                    // AND: 两边都能处理才能处理，只要有一边不精确就不精确
                    return if left_ok && right_ok {
                        (true, left_exact && right_exact)
                    } else if left_ok || right_ok {
                        // 一边能处理，一边不能 → 可以处理但不精确
                        (true, false)
                    } else {
                        (false, false)
                    };
                }
                Operator::Or => {
                    let (left_ok, left_exact) = can_handle_expr(&binary.left, index_readers);
                    let (right_ok, right_exact) = can_handle_expr(&binary.right, index_readers);

                    // OR: 只要有一边不能处理，就不精确（需要返回全量）
                    return if left_ok && right_ok {
                        (true, left_exact && right_exact)
                    } else {
                        // 有一边无法处理 → 不精确（返回全量 + DataFusion 再过滤）
                        (true, false)
                    };
                }
                _ => {}
            }

            // 比较运算符: col op value
            if let Expr::Column(column) = &*binary.left {
                // 检查字段是否有索引
                if !index_readers.contains_key(&column.name) {
                    return (false, false);
                }

                // 检查右边是否是字面量
                if let Expr::Literal(_, _) = &*binary.right {
                    match binary.op {
                        Operator::Eq
                        | Operator::Gt
                        | Operator::GtEq
                        | Operator::Lt
                        | Operator::LtEq => {
                            return (true, true); // 可以精确处理
                        }
                        _ => return (false, false),
                    }
                }

                // 右边是列引用: col1 = col2 → 无法处理
                if matches!(&*binary.right, Expr::Column(_)) {
                    return (false, false);
                }
            }

            (false, false)
        }

        // 其他表达式暂不支持
        _ => (false, false),
    }
}

/// PartitionTableProvider: DataFusion TableProvider for a Partition
///
/// A Partition contains multiple segments (1 current + N frozen). This provider
/// creates a SegmentTableProvider for each segment and unions their results.
///
/// Strategy: Union All - no deduplication (per user requirement)
pub struct PartitionTableProvider {
    partition: Arc<Partition>,
    schema: SchemaRef,
}

impl std::fmt::Debug for PartitionTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PartitionTableProvider")
            .field("partition_id", &self.partition.id())
            .field("schema", &self.schema)
            .finish()
    }
}

impl PartitionTableProvider {
    pub fn new(partition: Arc<Partition>) -> Self {
        let schema = partition.arrow_schema.clone();
        Self { partition, schema }
    }

    /// Create SegmentScanner for a segment
    /// Simply extracts the needed data from Segment and constructs SegmentScanner
    fn create_segment_scanner(&self, segment: &crate::segment::Segment) -> Result<SegmentScanner> {
        // Get cloned index readers from segment (fast - Arc internally)
        let index_readers = segment.get_index_readers();

        // Commented for cleaner logs
        // println!("[DEBUG] create_segment_scanner: index_readers keys = {:?}", index_readers.keys().collect::<Vec<_>>());

        // Get doc_count
        let doc_count = segment.doc_count();

        // Get cloned deleted bitmap (fast - compressed bitmap)
        let deleted = segment.get_deleted();

        // Get cloned row_data (fast - either Arc or BTree with Arc values)
        let row_data = segment.get_row_data();

        // Create SegmentScanner with extracted data
        Ok(SegmentScanner::new(
            self.schema.clone(),
            row_data,
            index_readers,
            doc_count,
            deleted,
        ))
    }
}

#[async_trait::async_trait]
impl TableProvider for PartitionTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        // 获取 current segment 的索引能力来判断
        // 所有 segments 共享相同的 schema 和索引配置,所以检查一个即可
        let current_segment = self.partition.get_current_segment();
        let index_readers = current_segment.get_index_readers();

        Ok(filters
            .iter()
            .map(|expr| {
                let (can_handle, _is_exact) = can_handle_expr(expr, &index_readers);
                if !can_handle {
                    TableProviderFilterPushDown::Unsupported
                } else {
                    // 使用Inexact而不是Exact，这样DataFusion会将filter传递到scan()
                    // 我们需要filter来执行索引查询
                    TableProviderFilterPushDown::Inexact
                }
            })
            .collect())
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Collect execution plans from all segments (current + frozen)
        let mut segment_plans: Vec<Arc<dyn ExecutionPlan>> = Vec::new();

        // Add current segment (only if non-empty)
        {
            let current_segment = self.partition.get_current_segment();
            if current_segment.doc_count() > 0 {
                let scanner = self.create_segment_scanner(&*current_segment)?;
                let plan = scanner.create_execution_plan(filters, projection)?;
                segment_plans.push(plan);
            }
        }

        // Add frozen segments
        {
            let frozen_segments = self.partition.get_frozen_segments();

            for (_seg_id, segment) in frozen_segments.iter() {
                let scanner = self.create_segment_scanner(segment)?;
                let plan = scanner.create_execution_plan(filters, projection)?;
                segment_plans.push(plan);
            }
        }

        // Handle empty partition case
        if segment_plans.is_empty() {
            return Err(DataFusionError::Internal(
                "Partition has no data".to_string(),
            ));
        }

        // If only one segment, return its plan directly
        if segment_plans.len() == 1 {
            return Ok(segment_plans.into_iter().next().unwrap());
        }

        // Create Union plan for multiple segments
        // 使用DataFusion内置的UnionExec
        use datafusion::physical_plan::union::UnionExec;

        let union_plan = UnionExec::new(segment_plans);
        Ok(Arc::new(union_plan))
    }
}

/// PartitionUnionExec: ExecutionPlan that unions multiple segment execution plans
///
/// This is similar to DataFusion's UnionExec but specialized for our use case
struct PartitionUnionExec {
    inputs: Vec<Arc<dyn ExecutionPlan>>,
    schema: SchemaRef,
    properties: PlanProperties,
}

impl PartitionUnionExec {
    fn new(inputs: Vec<Arc<dyn ExecutionPlan>>, schema: SchemaRef) -> Self {
        // Create properties
        let eq_properties = EquivalenceProperties::new(schema.clone());
        // UnionExec应该有单个partition，因为我们要把所有输入合并成一个流
        let partitioning = Partitioning::UnknownPartitioning(1);
        let emission_type = EmissionType::Final;
        let boundedness = Boundedness::Bounded;

        let properties =
            PlanProperties::new(eq_properties, partitioning, emission_type, boundedness);

        Self {
            inputs,
            schema,
            properties,
        }
    }
}
impl std::fmt::Debug for PartitionUnionExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PartitionUnionExec")
            .field("num_segments", &self.inputs.len())
            .finish()
    }
}

impl DisplayAs for PartitionUnionExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "PartitionUnionExec: segments={}", self.inputs.len())
    }
}

impl ExecutionPlan for PartitionUnionExec {
    fn name(&self) -> &str {
        "PartitionUnionExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.inputs.iter().collect()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(PartitionUnionExec::new(
            children,
            self.schema.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // Union只有一个partition(partition 0)
        if partition != 0 {
            return Err(DataFusionError::Execution(format!(
                "Invalid partition index: {} (PartitionUnionExec has only 1 partition)",
                partition
            )));
        }

        // 执行所有input plans并合并它们的流
        use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
        use futures::stream::{self, StreamExt};

        println!(
            "[DEBUG] PartitionUnionExec::execute with {} inputs",
            self.inputs.len()
        );

        let mut streams = Vec::new();
        for (i, input) in self.inputs.iter().enumerate() {
            let stream = input.execute(0, context.clone())?;
            streams.push(stream);
            println!("[DEBUG] Added stream {}", i);
        }

        // 使用futures::stream::iter将所有流连接起来
        let schema = self.schema.clone();
        let combined_stream = stream::iter(streams).flat_map(|s| s);

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            combined_stream,
        )))
    }
}
