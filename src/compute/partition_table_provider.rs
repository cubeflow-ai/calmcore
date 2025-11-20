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

    // 如果是聚合查询
    //       查看是否有不支持的函数， 比如  age=birthday-2 这种的不支持。就算返回全量数据。
    // 如果非聚合查询
    //。     有 range 查询，但是range查询范围很大，此时反回全量数据 是
    //。     有 limit 有order by 此时 可以利用倒排索引按照order by 命中id 重新组织后排序 是精确的
    //。     有limit 没有 order 此时应该scan的时候值返回limit+ size 条数是不是就可以
    //       没有limit 只有order 数据量肯定不大 返回全量数据即可吧。
    // 所以这个函数是否返回全量是不好说的。我们可以在查询前有个预查询，或者其他方式来确定这个返回情况。你看看怎么做回更好呢？

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        // 策略：返回 Exact，让 DataFusion 把 filters 传给 scan()
        // 在 scan() 中，SegmentScanner 会根据完整的查询上下文（filters + limit + projection）
        // 做智能决策，选择最优的执行策略
        Ok(vec![TableProviderFilterPushDown::Exact; filters.len()])
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Collect execution plans from all segments (current + frozen)
        let mut segment_plans: Vec<Arc<dyn ExecutionPlan>> = Vec::new();

        log::info!(
            "🔍 [PartitionTableProvider::scan] Starting scan for partition {}, filters={:?}, limit={:?}",
            self.partition.id(),
            filters,
            limit
        );

        // Add current segment (only if non-empty)
        {
            let current_segment = self.partition.get_current_segment();
            let doc_count = current_segment.doc_count();

            if doc_count > 0 {
                let scanner = self.create_segment_scanner(&*current_segment)?;
                // 使用新的优化方法，传递完整的查询上下文
                if let Some(plan) = scanner.create_plan(filters, projection, limit, None) {
                    segment_plans.push(plan);
                }
            }
        }

        // Add frozen segments
        {
            let frozen_segments = self.partition.get_frozen_segments();

            for (_seg_id, segment) in frozen_segments.iter() {
                let scanner = self.create_segment_scanner(segment)?;
                // 使用新的优化方法，传递完整的查询上下文
                if let Some(plan) = scanner.create_plan(filters, projection, limit, None) {
                    segment_plans.push(plan);
                }
            }
        }

        log::info!(
            "🔍 [PartitionTableProvider::scan] Total segment plans created: {}",
            segment_plans.len()
        );

        // Handle empty partition case - return empty plan instead of error
        if segment_plans.is_empty() {
            log::warn!(
                "⚠️  [PartitionTableProvider::scan] No segments found, returning empty plan"
            );
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
        use datafusion::physical_plan::union::UnionExec;

        let union_plan = UnionExec::new(segment_plans);
        Ok(Arc::new(union_plan))
    }
}
