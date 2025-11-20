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

/// PartitionTableProvider with sort hints
///
/// 这是一个增强版的 PartitionTableProvider，支持传递 ORDER BY 信息给 SegmentScanner
/// 用于优化 ORDER BY + LIMIT 查询
pub struct PartitionTableProviderWithHints {
    partition: Arc<Partition>,
    schema: SchemaRef,
    /// ORDER BY 字段 [(field_name, ascending)]
    sort: Option<(String, bool)>,
    /// LIMIT hint (用于 ORDER BY + LIMIT 优化)
    limit_hint: Option<usize>,
}

impl std::fmt::Debug for PartitionTableProviderWithHints {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PartitionTableProviderWithHints")
            .field("partition_id", &self.partition.id())
            .field("schema", &self.schema)
            .field("sort", &self.sort)
            .finish()
    }
}

impl PartitionTableProviderWithHints {
    /// 创建带有 sort hints 的 TableProvider
    ///
    /// # Arguments
    /// * `partition` - Partition
    /// * `sort_hints` - ORDER BY 字段 [(field_name, ascending)]
    pub fn new_with_sort_hints(
        partition: Arc<Partition>,
        sort_hints: Option<Vec<(String, bool)>>,
    ) -> Self {
        let schema = partition.arrow_schema.clone();
        Self {
            partition,
            schema,
            sort: sort_hints.and_then(|l| l.into_iter().next()),
            limit_hint: None,
        }
    }

    /// 创建带有 sort 和 limit hints 的 TableProvider
    ///
    /// # Arguments
    /// * `partition` - Partition
    /// * `sort_hints` - ORDER BY 字段 [(field_name, ascending)]
    /// * `limit_hint` - LIMIT 提示
    pub fn new_with_hints(
        partition: Arc<Partition>,
        sort_hints: Option<Vec<(String, bool)>>,
        limit_hint: Option<usize>,
    ) -> Self {
        let schema = partition.arrow_schema.clone();
        Self {
            partition,
            schema,
            sort: sort_hints.and_then(|l| l.into_iter().next()),
            limit_hint,
        }
    }

    /// Create SegmentScanner for a segment
    fn create_segment_scanner(&self, segment: &crate::segment::Segment) -> Result<SegmentScanner> {
        let index_readers = segment.get_index_readers();
        let doc_count = segment.doc_count();
        let deleted = segment.get_deleted();
        let row_data = segment.get_row_data();

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
impl TableProvider for PartitionTableProviderWithHints {
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
        // 策略：返回 Exact，让 DataFusion 把 filters 传给 scan()
        Ok(vec![TableProviderFilterPushDown::Exact; filters.len()])
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut segment_plans: Vec<Arc<dyn ExecutionPlan>> = Vec::new();

        // 🔧 使用 limit_hint 而不是 DataFusion 传递的 limit（对于 ORDER BY + LIMIT，DataFusion 不会下推 limit）
        let effective_limit = self.limit_hint.or(limit);

        log::info!(
            "🔍 [PartitionTableProviderWithHints::scan] Starting scan for partition {}, filters={:?}, limit={:?}, limit_hint={:?}, effective_limit={:?}, sort={:?}",
            self.partition.id(),
            filters,
            limit,
            self.limit_hint,
            effective_limit,
            self.sort
        );

        // Add current segment (only if non-empty)
        {
            let current_segment = self.partition.get_current_segment();
            let doc_count = current_segment.doc_count();

            if doc_count > 0 {
                let scanner = self.create_segment_scanner(&*current_segment)?;
                // 🚀 关键：传递 sort 和 effective_limit 给 SegmentScanner
                if let Some(plan) =
                    scanner.create_plan(filters, projection, effective_limit, self.sort.clone())
                {
                    segment_plans.push(plan);
                }
            }
        }

        // Add frozen segments
        {
            let frozen_segments = self.partition.get_frozen_segments();

            for (_seg_id, segment) in frozen_segments.iter() {
                let scanner = self.create_segment_scanner(segment)?;
                // 🚀 关键：传递 sort 和 effective_limit 给 SegmentScanner
                if let Some(plan) =
                    scanner.create_plan(filters, projection, effective_limit, self.sort.clone())
                {
                    segment_plans.push(plan);
                }
            }
        }

        log::info!(
            "🔍 [PartitionTableProviderWithHints::scan] Total segment plans created: {}",
            segment_plans.len()
        );

        // Handle empty partition case
        if segment_plans.is_empty() {
            log::warn!("⚠️  [PartitionTableProviderWithHints::scan] No segments found, returning empty plan");
            use datafusion::physical_plan::empty::EmptyExec;

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
