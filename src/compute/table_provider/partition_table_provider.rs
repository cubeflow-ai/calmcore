use std::{any::Any, sync::Arc};

use datafusion::{
    arrow::{
        datatypes::{Field, SchemaRef},
        record_batch::RecordBatch,
    },
    catalog::Session,
    datasource::{TableProvider, TableType},
    error::Result,
    execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext},
    logical_expr::{Expr, TableProviderFilterPushDown},
    physical_expr::EquivalenceProperties,
    physical_plan::execution_plan::{Boundedness, EmissionType},
    physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties},
};
use tokio::sync::mpsc;

use crate::{catalog::schema::DOC_ID_FIELD, partition::Partition};

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
    emit_internal_id: bool,
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
    pub fn new(partition: Arc<Partition>, emit_internal_id: bool) -> Self {
        let base_schema = partition.arrow_schema.clone();
        let schema = if emit_internal_id {
            augment_schema_with_internal_id(base_schema)
        } else {
            base_schema
        };
        Self {
            partition,
            schema,
            emit_internal_id,
        }
    }

    pub fn scan_partition(
        &self,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Arc<dyn ExecutionPlan> {
        log::debug!(
            "[PartitionTableProvider::scan_partition] Starting scan for partition {}, projection={:?}, filters={}, limit={:?}",
            self.partition.name(),
            projection,
            filters.len(),
            limit
        );

        // 🚀 Partition 级别: 启动 tokio task，串行处理多个 Segments
        Arc::new(PartitionExec::new(
            self.partition.clone(),
            self.schema.clone(),
            filters.to_vec(),
            projection.cloned(),
            limit,
            self.emit_internal_id,
        ))
    }

    /// Create SegmentScanner for a segment
    /// Simply extracts the needed data from Segment and constructs SegmentScanner
    #[allow(dead_code)]
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
            self.emit_internal_id,
            segment.start,
        ))
    }
}

fn augment_schema_with_internal_id(schema: SchemaRef) -> SchemaRef {
    if schema.field_with_name(DOC_ID_FIELD).is_ok() {
        return schema;
    }

    let mut fields = schema.fields().to_vec();
    fields.push(Arc::new(Field::new(
        DOC_ID_FIELD,
        datafusion::arrow::datatypes::DataType::UInt32,
        false,
    )));
    Arc::new(datafusion::arrow::datatypes::Schema::new(fields))
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
        Ok(self.scan_partition(projection, filters, limit))
    }
}

/// PartitionExec: 单个 Partition 的执行计划
///
/// **架构设计**:
/// - 启动一个 tokio task
/// - 串行处理 current_segment + frozen_segments (按 start 倒序)
/// - 通过 bounded channel (容量 10) 推送数据
/// - 外部通过 RecvStream 包装成 SendableRecordBatchStream
pub struct PartitionExec {
    partition: Arc<Partition>,
    full_schema: SchemaRef,
    schema: SchemaRef,
    filters: Vec<Expr>,
    projection: Option<Vec<usize>>,
    limit: Option<usize>,
    emit_internal_id: bool,
    properties: PlanProperties,
}

impl PartitionExec {
    fn new(
        partition: Arc<Partition>,
        schema: SchemaRef,
        filters: Vec<Expr>,
        projection: Option<Vec<usize>>,
        limit: Option<usize>,
        emit_internal_id: bool,
    ) -> Self {
        // Debug logging for schema and projection
        if let Some(ref proj) = projection {
            log::debug!(
                "🔍 [PartitionExec::new] Creating with projection: {:?}, schema fields: {}",
                proj,
                schema.fields().len()
            );
            for idx in proj {
                if *idx >= schema.fields().len() {
                    log::error!(
                        "❌ [PartitionExec::new] Projection index {} out of bounds for schema with {} fields!",
                        idx,
                        schema.fields().len()
                    );
                    // Print schema fields for debugging
                    for (i, field) in schema.fields().iter().enumerate() {
                        log::error!("  Field {}: {}", i, field.name());
                    }
                }
            }
        }

        let full_schema = schema.clone();

        // 计算投影后的 schema
        let output_schema = if let Some(ref proj) = projection {
            if proj.is_empty() {
                Arc::new(datafusion::arrow::datatypes::Schema::empty())
            } else {
                let fields: Vec<_> = proj.iter().map(|i| schema.field(*i).clone()).collect();
                Arc::new(datafusion::arrow::datatypes::Schema::new(fields))
            }
        } else {
            schema.clone()
        };

        let properties = PlanProperties::new(
            EquivalenceProperties::new(output_schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        );

        Self {
            partition,
            full_schema,
            schema: output_schema,
            filters,
            projection,
            limit,
            emit_internal_id,
            properties,
        }
    }
}

impl std::fmt::Debug for PartitionExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PartitionExec: {}", self.partition.name())
    }
}

impl std::fmt::Display for PartitionExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PartitionExec: {}", self.partition.name())
    }
}

impl DisplayAs for PartitionExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "PartitionExec: {}", self.partition.name())
    }
}

impl ExecutionPlan for PartitionExec {
    fn name(&self) -> &str {
        "PartitionExec"
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
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        use datafusion::arrow::record_batch::RecordBatch;
        use tokio::sync::mpsc;

        let partition = self.partition.clone();
        let full_schema = self.full_schema.clone();
        let output_schema = self.schema.clone();
        let filters = self.filters.clone();
        let projection = self.projection.clone();
        let _limit = self.limit;
        let emit_internal_id = self.emit_internal_id;

        // 创建 bounded channel: 容量 10
        let (tx, rx) = mpsc::channel::<Result<RecordBatch>>(10);

        // 启动 tokio task 串行处理所有 Segments
        tokio::spawn(async move {
            if let Err(e) = process_partition_segments(
                partition,
                full_schema,
                filters,
                projection,
                emit_internal_id,
                tx,
            )
            .await
            {
                log::error!("❌ [PartitionExec] Error processing segments: {}", e);
            }
        });

        // 包装成 SendableRecordBatchStream
        Ok(Box::pin(RecvStream::new(output_schema, rx)))
    }
}

/// 串行处理单个 Partition 的所有 Segments
async fn process_partition_segments(
    partition: Arc<Partition>,
    schema: SchemaRef,
    filters: Vec<Expr>,
    projection: Option<Vec<usize>>,
    emit_internal_id: bool,
    tx: mpsc::Sender<Result<RecordBatch>>,
) -> Result<()> {
    // 1. 处理 current_segment (快照模式)
    {
        let scanner_opt = {
            let current_segment_arc = partition.get_current_segment();
            let current_segment = current_segment_arc.read();
            let doc_count = current_segment.doc_count();
            if doc_count == 0 {
                None
            } else {
                log::debug!(
                    "  [PartitionExec] Processing current_segment: {} docs",
                    doc_count
                );
                let start = std::time::Instant::now();
                let v = Some(SegmentScanner::new(
                    schema.clone(),
                    current_segment.get_row_data(),
                    current_segment.get_index_readers(),
                    doc_count,
                    current_segment.get_deleted(),
                    emit_internal_id,
                    current_segment.start,
                ));
                println!(
                    "    ⏱️ ================================== Created SegmentScanner for current_segment in {:?}",
                    start.elapsed()
                );

                v
            }
        };

        if let Some(scanner) = scanner_opt {
            let start = std::time::Instant::now();
            process_segment(&scanner, &filters, &projection, &tx).await?;
            println!(
                "    ⏱️ ================================== Processed current_segment in {:?}",
                start.elapsed()
            );
        }
    }

    // 2. 处理 frozen_segments (快照模式 - 已经是 clone 的 Vec)
    let mut scanners = {
        let frozen_segments = partition.get_frozen_segments(); // 已经是 Vec<(u64, Arc<Segment>)>
        let mut scanners = Vec::new();

        let start = std::time::Instant::now();
        for (seg_id, segment) in frozen_segments.iter() {
            let doc_count = segment.doc_count();
            if doc_count == 0 {
                continue;
            }

            log::debug!(
                "  [PartitionExec] Preparing segment {} (start={}): {} docs",
                seg_id,
                segment.start,
                doc_count
            );
            let scanner = SegmentScanner::new(
                schema.clone(),
                segment.get_row_data(),
                segment.get_index_readers(),
                doc_count,
                segment.get_deleted(),
                emit_internal_id,
                segment.start,
            );
            scanners.push((*seg_id, segment.start, scanner));
        }
        println!(
            "    ⏱️ ================================== Created SegmentScanners for {} frozen segments in {:?}",
            scanners.len(),
            start.elapsed()
        );
        scanners
    };

    // 按 start 倒序排序
    scanners.sort_by(|a, b| b.1.cmp(&a.1));

    // 串行处理
    for (seg_id, _start, scanner) in scanners {
        let start = std::time::Instant::now();
        process_segment(&scanner, &filters, &projection, &tx).await?;
        println!(
            "    ⏱️ ================================== Processed frozen segment {} in {:?}",
            seg_id,
            start.elapsed()
        );
    }

    Ok(())
}

/// 处理单个 Segment
async fn process_segment(
    scanner: &SegmentScanner,
    filters: &[Expr],
    projection: &Option<Vec<usize>>,
    tx: &mpsc::Sender<Result<RecordBatch>>,
) -> Result<()> {
    println!(
        "        🔍 [process_segment] Called with projection={:?}",
        projection
    );
    let start = std::time::Instant::now();
    let plan = match scanner.create_plan(filters, projection.as_ref(), None, None) {
        Some(p) => p,
        None => return Ok(()),
    };
    println!("        ⏱️ create_plan took {:?}", start.elapsed());

    let start = std::time::Instant::now();
    let context = Arc::new(TaskContext::default());
    let mut stream = plan.execute(0, context)?;
    println!("        ⏱️ execute took {:?}", start.elapsed());

    // 逐批发送数据到 channel
    let start = std::time::Instant::now();
    let mut batch_count = 0;
    let mut row_count = 0;
    while let Some(result) = futures::StreamExt::next(&mut stream).await {
        match &result {
            Ok(batch) => {
                batch_count += 1;
                row_count += batch.num_rows();
            }
            Err(_) => {}
        }
        if tx.send(result).await.is_err() {
            // Channel 关闭，停止发送
            return Ok(());
        }
    }
    println!(
        "        ⏱️ streaming {} batches ({} rows) took {:?}",
        batch_count,
        row_count,
        start.elapsed()
    );

    Ok(())
}

/// RecvStream: 包装 mpsc::Receiver 为 SendableRecordBatchStream
struct RecvStream {
    schema: SchemaRef,
    rx: mpsc::Receiver<Result<RecordBatch>>,
}

impl RecvStream {
    fn new(schema: SchemaRef, rx: mpsc::Receiver<Result<RecordBatch>>) -> Self {
        Self { schema, rx }
    }
}

impl RecordBatchStream for RecvStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl futures::Stream for RecvStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
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
    pub fn new(
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

    /// Get number of segments
    pub fn num_segments(&self) -> usize {
        self.segment_scanners.len()
    }

    /// Get projection
    pub fn projection(&self) -> Vec<usize> {
        self.projection
            .clone()
            .unwrap_or_else(|| (0..self.schema.fields().len()).collect())
    }

    /// Get limit
    pub fn limit(&self) -> Option<usize> {
        self.limit
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
        log::debug!(
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
                log::debug!(
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
