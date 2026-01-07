use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::Result as DataFusionResult;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::Expr;
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
/// 通过 FlightExecutor 直接传递 projection/filters/limit 到远程节点
pub struct RemoteScanExec {
    pub table_name: String,
    pub partition_ids: Vec<String>,
    /// 完整的表 schema
    full_schema: SchemaRef,
    /// 输出 schema（应用 projection 后）
    output_schema: SchemaRef,
    pub projection: Option<Vec<usize>>,
    pub filters: Vec<Expr>,
    pub limit: Option<usize>,
    pub count_only: bool,
    pub executor: Arc<FlightExecutor>,
    properties: PlanProperties,
}

impl RemoteScanExec {
    /// 创建新的 RemoteScanExec
    ///
    /// 不生成 SQL，而是直接传递 projection/filters/limit 到远程节点
    pub fn new(
        table_name: String,
        partition_ids: Vec<String>,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
        filters: Vec<Expr>,
        limit: Option<usize>,
        count_only: bool,
        executor: Arc<FlightExecutor>,
    ) -> Self {
        // 计算输出 schema（应用 projection）
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

        // 创建 PlanProperties
        let partitioning = Partitioning::UnknownPartitioning(1);
        let boundedness = Boundedness::Bounded;
        let emission_type = EmissionType::Incremental;

        let properties = PlanProperties::new(
            datafusion::physical_expr::EquivalenceProperties::new(output_schema.clone()),
            partitioning,
            emission_type,
            boundedness,
        );

        Self {
            table_name,
            partition_ids,
            full_schema: schema,
            output_schema,
            projection,
            filters,
            limit,
            count_only,
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
            .field("projection", &self.projection)
            .field("filters", &self.filters.len())
            .field("limit", &self.limit)
            .finish()
    }
}

impl Clone for RemoteScanExec {
    fn clone(&self) -> Self {
        Self {
            table_name: self.table_name.clone(),
            partition_ids: self.partition_ids.clone(),
            full_schema: self.full_schema.clone(),
            output_schema: self.output_schema.clone(),
            projection: self.projection.clone(),
            filters: self.filters.clone(),
            limit: self.limit,
            count_only: self.count_only,
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
        self.output_schema.clone()
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
            "[RemoteScanExec] Execute called for table '{}', partitions: {:?}, projection: {:?}, filters: {}, limit: {:?}",
            self.table_name,
            self.partition_ids,
            self.projection,
            self.filters.len(),
            self.limit
        );

        // 调用 FlightExecutor 执行远程扫描（不是SQL）
        let executor = self.executor.clone();
        let table_name = self.table_name.clone();
        let schema = self.output_schema.clone();
        let partitions = self.partition_ids.clone();
        let projection = self.projection.clone();
        let filters = self.filters.clone();
        let limit = self.limit;
        let count_only = self.count_only;

        // 创建异步流
        let stream = futures::stream::once(async move {
            match executor
                .execute_scan(
                    &table_name,
                    &partitions,
                    projection,
                    filters,
                    limit,
                    count_only,
                )
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
