use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::Result as DataFusionResult;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use std::any::Any;
use std::fmt;
use std::sync::Arc;

use crate::compute::federation::flight_executor::FlightExecutor;

/// 远程扫描执行计划
///
/// 通过 FlightExecutor 执行远程查询
pub struct RemoteScanExec {
    pub table_name: String,
    pub partition_ids: Vec<String>,
    pub schema: SchemaRef,
    pub sql: String,
    pub projection: Option<Vec<usize>>,
    pub executor: Arc<FlightExecutor>,
    properties: PlanProperties,
}

impl RemoteScanExec {
    /// 创建新的 RemoteScanExec
    pub fn new(
        table_name: String,
        partition_ids: Vec<String>,
        schema: SchemaRef,
        sql: String,
        projection: Option<Vec<usize>>,
        executor: Arc<FlightExecutor>,
    ) -> Self {
        // 创建 PlanProperties
        let partitioning = Partitioning::UnknownPartitioning(1);
        let boundedness = Boundedness::Bounded;
        let emission_type = EmissionType::Incremental;

        let properties = PlanProperties::new(
            datafusion::physical_expr::EquivalenceProperties::new(schema.clone()),
            partitioning,
            emission_type,
            boundedness,
        );

        Self {
            table_name,
            partition_ids,
            schema,
            sql,
            projection,
            executor,
            properties,
        }
    }
}

impl std::fmt::Debug for RemoteScanExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteScanExec")
            .field("table_name", &self.table_name)
            .field("partition_ids", &self.partition_ids)
            .field("sql", &self.sql)
            .finish()
    }
}

impl Clone for RemoteScanExec {
    fn clone(&self) -> Self {
        Self {
            table_name: self.table_name.clone(),
            partition_ids: self.partition_ids.clone(),
            schema: self.schema.clone(),
            sql: self.sql.clone(),
            projection: self.projection.clone(),
            executor: self.executor.clone(),
            properties: self.properties.clone(),
        }
    }
}

impl ExecutionPlan for RemoteScanExec {
    fn name(&self) -> &str {
        "RemoteScanExec"
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
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        log::debug!(
            "[RemoteScanExec] Execute called for table '{}', partitions: {:?}, SQL: {}",
            self.table_name,
            self.partition_ids,
            self.sql
        );

        // 调用 FlightExecutor 执行远程查询
        let executor = self.executor.clone();
        let sql = self.sql.clone();
        let schema = self.schema.clone();
        let partitions = self.partition_ids.clone();

        // 创建异步流
        let stream = futures::stream::once(async move {
            match executor
                .execute_sql_with_partitions(&sql, &partitions)
                .await
            {
                Ok(stream) => Ok(stream),
                Err(e) => {
                    let err = datafusion::error::DataFusionError::External(Box::new(e));
                    Err(err)
                }
            }
        });

        // 将 Stream<Result<SendableRecordBatchStream>> 展平为 Stream<Result<RecordBatch>>
        use futures::stream::TryStreamExt;
        let flattened = stream.try_flatten();

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, flattened)))
    }
}

impl DisplayAs for RemoteScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "RemoteScanExec: table={}, partitions={:?}",
            self.table_name, self.partition_ids
        )
    }
}
