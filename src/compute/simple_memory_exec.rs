//! SimpleMemoryExec - 简单的内存执行计划
//! 
//! 只包含 RecordBatch，可以被序列化（通过 Arrow IPC）

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::Result as DataFusionResult;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use futures::stream;

/// SimpleMemoryExec: 包含已加载的 RecordBatch
#[derive(Clone)]
pub struct SimpleMemoryExec {
    /// 数据分区 (每个分区是一组 RecordBatch)
    partitions: Vec<Vec<RecordBatch>>,
    
    /// Schema
    schema: SchemaRef,
    
    /// Projection (已应用)
    projection: Option<Vec<usize>>,
    
    /// 计划属性
    properties: PlanProperties,
}

impl SimpleMemoryExec {
    pub fn try_new(
        partitions: &[Vec<RecordBatch>],
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
    ) -> DataFusionResult<Self> {
        let partitioning = Partitioning::UnknownPartitioning(partitions.len());
        let eq_properties = EquivalenceProperties::new(schema.clone());
        
        let properties = PlanProperties::new(
            eq_properties,
            partitioning,
            EmissionType::Final,
            Boundedness::Bounded,
        );
        
        Ok(Self {
            partitions: partitions.to_vec(),
            schema,
            projection,
            properties,
        })
    }
}

impl std::fmt::Debug for SimpleMemoryExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SimpleMemoryExec")
            .field("num_partitions", &self.partitions.len())
            .field("projection", &self.projection)
            .finish()
    }
}

impl DisplayAs for SimpleMemoryExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "SimpleMemoryExec: partitions={}",
            self.partitions.len()
        )
    }
}

impl ExecutionPlan for SimpleMemoryExec {
    fn name(&self) -> &str {
        "SimpleMemoryExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
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
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let batches = self.partitions.get(partition)
            .cloned()
            .unwrap_or_default();
        
        let schema = self.schema.clone();
        let stream = stream::iter(batches.into_iter().map(Ok));
        
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}
