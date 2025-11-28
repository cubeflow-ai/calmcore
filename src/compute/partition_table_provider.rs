use std::{any::Any, sync::Arc};

use datafusion::{
    arrow::datatypes::SchemaRef,
    catalog::Session,
    datasource::{TableProvider, TableType},
    error::Result,
    execution::{SendableRecordBatchStream, TaskContext},
    logical_expr::{Expr, TableProviderFilterPushDown},
    physical_expr::EquivalenceProperties,
    physical_plan::execution_plan::{Boundedness, EmissionType},
    physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties},
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
            .field("partition_name", &self.partition.name())
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

    // TODO： 优化过滤条件的推送下推策略
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        // 策略：全部返回 Inexact,更保险
        // 这样 DataFusion 不会过度优化(比如 COUNT 的空 projection)
        // 同时我们在 scan() 中仍然可以充分利用索引
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        log::info!(
            "🔍 [PartitionTableProvider::scan] Starting scan for partition {}, projection={:?}, filters={}, limit={:?}",
            self.partition.name(),
            projection,
            filters.len(),
            limit
        );

        // 🚀 新方案: 让每个segment成为一个独立的partition,实现真正的并行
        // 而不是使用UnionExec串行合并

        // 提前为每个segment创建SegmentScanner,存储扫描所需的数据
        let mut segment_scanners = Vec::new();

        // Add current segment (only if non-empty)
        {
            let current_segment = self.partition.get_current_segment();
            if current_segment.doc_count() > 0 {
                let scanner = SegmentScanner::new(
                    self.schema.clone(),
                    current_segment.get_row_data(),
                    current_segment.get_index_readers(),
                    current_segment.doc_count(),
                    current_segment.get_deleted(),
                );
                segment_scanners.push(scanner);
            }
        }

        // Add frozen segments
        {
            let frozen_segments = self.partition.get_frozen_segments();
            for (_seg_id, segment) in frozen_segments.iter() {
                let scanner = SegmentScanner::new(
                    self.schema.clone(),
                    segment.get_row_data(),
                    segment.get_index_readers(),
                    segment.doc_count(),
                    segment.get_deleted(),
                );
                segment_scanners.push(scanner);
            }
        }

        log::info!(
            "🔍 [PartitionTableProvider::scan] Found {} segments",
            segment_scanners.len()
        );

        // Handle empty partition case
        if segment_scanners.is_empty() {
            log::warn!(
                "⚠️  [PartitionTableProvider::scan] No segments found, returning empty plan"
            );
            use datafusion::physical_plan::empty::EmptyExec;

            let empty_schema = if let Some(proj) = projection {
                let fields: Vec<_> = proj.iter().map(|i| self.schema.field(*i).clone()).collect();
                Arc::new(datafusion::arrow::datatypes::Schema::new(fields))
            } else {
                self.schema.clone()
            };

            return Ok(Arc::new(EmptyExec::new(empty_schema)));
        }

        // 🔥 创建 MultiSegmentExec: 让DataFusion并行执行每个segment
        // 每个segment作为一个partition,DataFusion会自动并行调度
        Ok(Arc::new(MultiSegmentExec::new(
            self.schema.clone(),
            segment_scanners,
            filters.to_vec(),
            projection.cloned(),
            limit,
        )))
    }
}

/// MultiSegmentExec: 并行扫描多个segment的ExecutionPlan
///
/// **关键设计**:
/// - 每个segment作为一个DataFusion partition
/// - DataFusion会自动并行调度这些partition
/// - 每个partition内部使用SegmentScanner的优化能力(索引过滤、projection下推等)
/// - 流式返回数据,无需collect
pub struct MultiSegmentExec {
    schema: SchemaRef,
    segment_scanners: Vec<SegmentScanner>,
    filters: Vec<Expr>,
    projection: Option<Vec<usize>>,
    limit: Option<usize>,
    properties: PlanProperties,
}

/// 创建 MultiSegmentExec 的公开函数
/// 供 UnionTableProvider 使用,实现全局并行
pub fn create_multi_segment_exec(
    schema: SchemaRef,
    segment_scanners: Vec<SegmentScanner>,
    filters: Vec<Expr>,
    projection: Option<Vec<usize>>,
    limit: Option<usize>,
) -> MultiSegmentExec {
    MultiSegmentExec::new(schema, segment_scanners, filters, projection, limit)
}

impl MultiSegmentExec {
    fn new(
        schema: SchemaRef,
        segment_scanners: Vec<SegmentScanner>,
        filters: Vec<Expr>,
        projection: Option<Vec<usize>>,
        limit: Option<usize>,
    ) -> Self {
        let num_partitions = segment_scanners.len();

        // 应用projection到schema
        let output_schema = if let Some(ref proj) = projection {
            let fields: Vec<_> = proj.iter().map(|i| schema.field(*i).clone()).collect();
            Arc::new(datafusion::arrow::datatypes::Schema::new(fields))
        } else {
            schema.clone()
        };

        // 创建 PlanProperties
        let properties = PlanProperties::new(
            EquivalenceProperties::new(output_schema.clone()),
            Partitioning::UnknownPartitioning(num_partitions),
            EmissionType::Final,
            Boundedness::Bounded,
        );

        Self {
            schema: output_schema,
            segment_scanners,
            filters,
            projection,
            limit,
            properties,
        }
    }
}

impl std::fmt::Debug for MultiSegmentExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MultiSegmentExec")
            .field("num_segments", &self.segment_scanners.len())
            .field("filters", &self.filters.len())
            .field("projection", &self.projection)
            .field("limit", &self.limit)
            .finish()
    }
}

impl DisplayAs for MultiSegmentExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "MultiSegmentExec: segments={}, filters={}, projection={:?}, limit={:?}",
            self.segment_scanners.len(),
            self.filters.len(),
            self.projection,
            self.limit
        )
    }
}

impl ExecutionPlan for MultiSegmentExec {
    fn name(&self) -> &str {
        "MultiSegmentExec"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        log::info!(
            "🎯 [MultiSegmentExec::execute] Executing partition {} (segment)",
            partition
        );

        if partition >= self.segment_scanners.len() {
            return Err(datafusion::error::DataFusionError::Internal(format!(
                "Partition {} out of range (total: {})",
                partition,
                self.segment_scanners.len()
            )));
        }

        let scanner = &self.segment_scanners[partition];

        // 使用 SegmentScanner 的优化能力创建执行计划
        // TODO: 支持 ORDER BY 下推 (需要从LogicalPlan中提取)
        let plan = scanner.create_plan(
            &self.filters,
            self.projection.as_ref(),
            self.limit,
            None, // sort: 暂时不支持,可以后续从context中提取
        );

        match plan {
            Some(segment_plan) => {
                log::info!(
                    "✅ [MultiSegmentExec] Partition {} created execution plan",
                    partition
                );
                // 直接执行segment的plan并返回stream
                segment_plan.execute(0, _context)
            }
            None => {
                log::info!(
                    "📋 [MultiSegmentExec] Partition {} has no data (empty result)",
                    partition
                );
                // 返回空stream
                use datafusion::physical_plan::empty::EmptyExec;
                let empty_plan = EmptyExec::new(self.schema.clone());
                empty_plan.execute(0, _context)
            }
        }
    }
}
