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
    count_only: bool,
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
    pub fn new(partition: Arc<Partition>, emit_internal_id: bool, count_only: bool) -> Self {
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
            count_only,
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
            self.count_only,
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
        // 🎯 COUNT(*) 优化: 如果是 count_only 查询，所有 filters 都已在索引层处理
        // 返回 Exact 告诉 DataFusion 不要再添加 FilterExec
        if self.count_only {
            return Ok(vec![TableProviderFilterPushDown::Exact; filters.len()]);
        }

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
    count_only: bool,
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
        count_only: bool,
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
        // 🎯 COUNT(*) 优化: 当 count_only=true 时，返回空 schema
        let output_schema = if count_only {
            Arc::new(datafusion::arrow::datatypes::Schema::empty())
        } else if let Some(ref proj) = projection {
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
            count_only,
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
        let limit = self.limit;
        let emit_internal_id = self.emit_internal_id;
        let count_only = self.count_only;

        // 创建 bounded channel: 容量 10
        let (tx, rx) = mpsc::channel::<Result<RecordBatch>>(10);

        // 启动 tokio task 串行处理所有 Segments
        tokio::spawn(async move {
            if let Err(e) = process_partition_segments(
                partition,
                full_schema,
                filters,
                projection,
                limit,
                emit_internal_id,
                count_only,
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
    limit: Option<usize>,
    emit_internal_id: bool,
    count_only: bool,
    tx: mpsc::Sender<Result<RecordBatch>>,
) -> Result<()> {
    let mut rows_sent = 0usize;
    {
        let scanner_opt = {
            let current_segment_arc = partition.get_current_segment();
            let current_segment = current_segment_arc.read();
            let doc_count = current_segment.doc_count();
            if doc_count == 0 {
                None
            } else {
                Some(SegmentScanner::new(
                    schema.clone(),
                    current_segment.get_row_data(),
                    current_segment.get_index_readers(),
                    doc_count,
                    current_segment.get_deleted(),
                    emit_internal_id,
                    count_only,
                    current_segment.start,
                ))
            }
        };

        if let Some(scanner) = scanner_opt {
            let _sent =
                process_segment(&scanner, &filters, &projection, limit, &mut rows_sent, &tx)
                    .await?;
            // 达到 limit 后提前返回
            if let Some(lim) = limit {
                if rows_sent >= lim {
                    return Ok(());
                }
            }
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
                count_only,
                segment.start,
            );
            scanners.push((*seg_id, segment.start, scanner));
        }
        scanners
    };

    // 按 start 倒序排序
    scanners.sort_by(|a, b| b.1.cmp(&a.1));

    // 串行处理
    for (seg_id, _start, scanner) in scanners {
        let start = std::time::Instant::now();
        let sent =
            process_segment(&scanner, &filters, &projection, limit, &mut rows_sent, &tx).await?;
        log::error!(
            "    ⏱️ ================================== Processed frozen segment {} in {:?}, sent {} rows (total={})",
            seg_id,
            start.elapsed(),
            sent,
            rows_sent
        );
        // 达到 limit 后提前返回
        if let Some(lim) = limit {
            if rows_sent >= lim {
                return Ok(());
            }
        }
    }

    Ok(())
}

/// 处理单个 Segment
async fn process_segment(
    scanner: &SegmentScanner,
    filters: &[Expr],
    projection: &Option<Vec<usize>>,
    limit: Option<usize>,
    rows_sent: &mut usize,
    tx: &mpsc::Sender<Result<RecordBatch>>,
) -> Result<usize> {
    let mut segment_rows = 0;
    log::error!(
        "        🔍 [process_segment] Called with projection={:?}",
        projection
    );
    let start = std::time::Instant::now();
    let plan = match scanner.create_plan(filters, projection.as_ref(), None, None) {
        Some(p) => p,
        None => return Ok(0),
    };
    log::error!("        ⏱️ create_plan took {:?}", start.elapsed());

    let start = std::time::Instant::now();
    let context = Arc::new(TaskContext::default());
    let mut stream = plan.execute(0, context)?;
    log::error!("        ⏱️ execute took {:?}", start.elapsed());

    // 逐批发送数据到 channel，应用 limit
    let start = std::time::Instant::now();
    let mut batch_count = 0;
    let mut row_count = 0;
    while let Some(result) = futures::StreamExt::next(&mut stream).await {
        let batch = match result {
            Ok(b) => {
                batch_count += 1;
                let batch_rows = b.num_rows();
                row_count += batch_rows;

                // 应用 limit：可能需要截断 batch
                if let Some(lim) = limit {
                    let remaining = lim.saturating_sub(*rows_sent);
                    if remaining == 0 {
                        // 已经达到 limit，停止发送
                        break;
                    } else if batch_rows > remaining {
                        // 需要截断 batch
                        let truncated = b.slice(0, remaining);
                        segment_rows += remaining;
                        *rows_sent += remaining;
                        if tx.send(Ok(truncated)).await.is_err() {
                            return Ok(segment_rows);
                        }
                        break;
                    }
                }

                segment_rows += batch_rows;
                *rows_sent += batch_rows;
                b
            }
            Err(e) => {
                if tx.send(Err(e)).await.is_err() {
                    return Ok(segment_rows);
                }
                continue;
            }
        };

        if tx.send(Ok(batch)).await.is_err() {
            // Channel 关闭，停止发送
            return Ok(segment_rows);
        }
    }
    log::error!(
        "        ⏱️ streaming {} batches ({} rows) took {:?}",
        batch_count,
        row_count,
        start.elapsed()
    );

    Ok(segment_rows)
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
