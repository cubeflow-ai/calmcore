use std::{any::Any, sync::Arc};

use datafusion::{
    arrow::datatypes::SchemaRef,
    catalog::Session,
    datasource::{TableProvider, TableType},
    error::Result,
    logical_expr::{Expr, TableProviderFilterPushDown},
    physical_plan::ExecutionPlan,
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

        eprintln!(
            "🔍 [PartitionTableProvider::scan] Starting scan for partition {}",
            self.partition.id()
        );
        eprintln!("🔍 [PartitionTableProvider::scan] Filters: {:?}", filters);

        // Add current segment (only if non-empty)
        {
            let current_segment = self.partition.get_current_segment();
            let doc_count = current_segment.doc_count();
            eprintln!(
                "🔍 [PartitionTableProvider::scan] Current segment doc_count: {}",
                doc_count
            );

            if doc_count > 0 {
                let scanner = self.create_segment_scanner(&*current_segment)?;
                if let Some(plan) = scanner.create_execution_plan(filters, projection) {
                    segment_plans.push(plan);
                }
            }
        }

        // Add frozen segments
        {
            let frozen_segments = self.partition.get_frozen_segments();
            eprintln!(
                "🔍 [PartitionTableProvider::scan] Frozen segments count: {}",
                frozen_segments.len()
            );

            for (seg_id, segment) in frozen_segments.iter() {
                eprintln!(
                    "🔍 [PartitionTableProvider::scan] Frozen segment {} doc_count: {}",
                    seg_id,
                    segment.doc_count()
                );
                let scanner = self.create_segment_scanner(segment)?;
                if let Some(plan) = scanner.create_execution_plan(filters, projection) {
                    segment_plans.push(plan);
                }
            }
        }

        eprintln!(
            "🔍 [PartitionTableProvider::scan] Total segment plans created: {}",
            segment_plans.len()
        );

        // Handle empty partition case - return empty plan instead of error
        if segment_plans.is_empty() {
            eprintln!("⚠️  [PartitionTableProvider::scan] No segments found, returning empty plan");
            use datafusion::physical_plan::empty::EmptyExec;

            // 使用投影后的schema(如果有),否则使用完整schema
            let empty_schema = if let Some(proj) = projection {
                let fields: Vec<_> = proj.iter().map(|i| self.schema.field(*i).clone()).collect();
                Arc::new(datafusion::arrow::datatypes::Schema::new(fields))
            } else {
                self.schema.clone()
            };

            return Ok(Arc::new(EmptyExec::new(empty_schema)));
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
