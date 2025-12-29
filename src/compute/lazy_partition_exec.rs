//! LazyPartitionExec - 延迟加载的分区执行计划
//!
//! 类似 ParquetExec，只存储元数据（partition 名称），在执行时才加载数据
//! 可以被序列化发送到远程节点

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
};
use futures::stream;
use futures::{StreamExt, TryStreamExt};

use crate::engine::Engine;
use crate::utils::error::CoreResult;

/// LazyPartitionExec: 延迟加载的分区执行计划
///
/// **关键设计**：
/// - 只存储元数据：table_name, partition_names, filters, projection, limit
/// - 可以序列化为 protobuf（所有字段都是简单类型或可序列化类型）
/// - 在 execute() 时才从 Engine 加载真实数据
/// - 类似 ParquetExec，但用于我们的内存 partition
#[derive(Clone)]
pub struct LazyPartitionExec {
    /// 表名
    table_name: String,

    /// Partition 名称列表（相当于 ParquetExec 的文件路径）
    partition_names: Vec<String>,

    /// 每个 partition 的 owner node_id（用于路由判断）
    /// Key: partition_name, Value: owner_node_id
    partition_owners: std::collections::HashMap<String, String>,

    /// 基础 Schema（原始完整表 schema，用于创建 PartitionTableProvider）
    base_schema: SchemaRef,

    /// 输出 Schema（应用 projection 后的 schema，返回给 DataFusion）
    output_schema: SchemaRef,

    /// Filter 表达式（下推）
    filters: Vec<Expr>,

    /// Projection（下推）
    projection: Option<Vec<usize>>,

    /// Limit（下推）
    limit: Option<usize>,

    /// 执行属性
    properties: PlanProperties,

    /// Engine 引用（用于加载数据）
    /// 注意：这个字段不会被序列化，远程节点会用自己的 Engine
    #[allow(dead_code)]
    engine: Option<Arc<Engine>>,
}

impl LazyPartitionExec {
    pub fn new(
        table_name: String,
        partition_names: Vec<String>,
        partition_owners: std::collections::HashMap<String, String>,
        schema: SchemaRef,
        filters: Vec<Expr>,
        projection: Option<Vec<usize>>,
        limit: Option<usize>,
        engine: Arc<Engine>,
    ) -> Self {
        let num_partitions = partition_names.len();

        // 🎯 关键区分：
        // - base_schema: 原始完整 schema，用于创建 PartitionTableProvider
        // - output_schema: 应用 projection 后的 schema，返回给 DataFusion
        let output_schema = if let Some(ref proj) = projection {
            let fields: Vec<_> = proj.iter().map(|i| schema.field(*i).clone()).collect();
            Arc::new(datafusion::arrow::datatypes::Schema::new(fields))
        } else {
            schema.clone()
        };

        let properties = PlanProperties::new(
            EquivalenceProperties::new(output_schema.clone()),
            Partitioning::UnknownPartitioning(num_partitions),
            EmissionType::Final,
            Boundedness::Bounded,
        );

        Self {
            table_name,
            partition_names,
            partition_owners,
            base_schema: schema, // 原始完整 schema
            output_schema,       // projection 后的 schema
            filters,
            projection,
            limit,
            properties,
            engine: Some(engine),
        }
    }

    /// 用于反序列化（远程节点）
    pub fn new_without_engine(
        table_name: String,
        partition_names: Vec<String>,
        partition_owners: std::collections::HashMap<String, String>,
        schema: SchemaRef,
        filters: Vec<Expr>,
        projection: Option<Vec<usize>>,
        limit: Option<usize>,
    ) -> Self {
        let num_partitions = partition_names.len();

        // 与 new() 保持一致：区分 base_schema 和 output_schema
        let output_schema = if let Some(ref proj) = projection {
            let fields: Vec<_> = proj.iter().map(|i| schema.field(*i).clone()).collect();
            Arc::new(datafusion::arrow::datatypes::Schema::new(fields))
        } else {
            schema.clone()
        };

        let properties = PlanProperties::new(
            EquivalenceProperties::new(output_schema.clone()),
            Partitioning::UnknownPartitioning(num_partitions),
            EmissionType::Final,
            Boundedness::Bounded,
        );

        Self {
            table_name,
            partition_names,
            partition_owners,
            base_schema: schema, // 原始完整 schema
            output_schema,       // projection 后的 schema
            filters,
            projection,
            limit,
            properties,
            engine: None,
        }
    }

    // Getters for serialization
    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    pub fn partition_names(&self) -> &[String] {
        &self.partition_names
    }

    pub fn partition_owners(&self) -> &std::collections::HashMap<String, String> {
        &self.partition_owners
    }

    pub fn base_schema(&self) -> SchemaRef {
        self.base_schema.clone()
    }

    pub fn filters(&self) -> &[Expr] {
        &self.filters
    }

    pub fn projection(&self) -> Option<&Vec<usize>> {
        self.projection.as_ref()
    }

    pub fn limit(&self) -> Option<usize> {
        self.limit
    }

    /// 从上下文获取 Engine（用于远程执行）
    fn get_engine_from_context(ctx: &Arc<TaskContext>) -> CoreResult<Arc<Engine>> {
        use crate::engine::Engine;

        log::info!("🔍 [LazyPartitionExec] Trying to get Engine from TaskContext...");

        // 从 SessionConfig 的 extensions 中获取 Engine
        let config = ctx.session_config();

        log::info!("🔍 [LazyPartitionExec] SessionConfig obtained, checking extensions...");

        // 🎯 关键：with_extension(Arc<Engine>) 会存储为 Arc<Arc<Engine>>
        // 所以 get_extension::<Engine>() 会返回 Arc<Engine>
        if let Some(engine) = config.get_extension::<Engine>() {
            log::info!("✅ [LazyPartitionExec] Found Engine in TaskContext extensions!");
            return Ok(engine);
        }

        log::error!("❌ [LazyPartitionExec] Engine NOT found in TaskContext extensions");

        Err(crate::utils::error::CoreError::Internal(
            "Engine not available in TaskContext. Need to inject Engine when creating distributed context.".to_string()
        ))
    }

    /// 加载 partition 并创建执行流
    async fn load_and_execute(
        &self,
        partition_idx: usize,
        engine: Arc<Engine>,
        context: Arc<TaskContext>,
    ) -> CoreResult<SendableRecordBatchStream> {
        use crate::compute::table_provider::partition_table_provider::PartitionTableProvider;
        use datafusion::datasource::TableProvider;

        if partition_idx >= self.partition_names.len() {
            return Err(crate::utils::error::CoreError::Internal(format!(
                "Partition index {} out of range (total: {})",
                partition_idx,
                self.partition_names.len()
            )));
        }

        let partition_name = &self.partition_names[partition_idx];

        log::info!(
            "📦 [LazyPartitionExec::load_and_execute] Starting for partition '{}' (index={})",
            partition_name,
            partition_idx
        );

        // 🎯 关键逻辑：判断这个 partition 是否属于当前节点
        // 1. 获取当前节点的 node_id（从 TaskContext Extension 中）
        let my_node_id = context
            .session_config()
            .get_extension::<String>()
            .map(|s| (*s).clone())
            .unwrap_or_else(|| "standalone".to_string());

        log::info!("🔍 [LazyPartitionExec] My node_id: '{}'", my_node_id);

        // 2. 获取这个 partition 的 owner_node_id
        if let Some(owner_node_id) = self.partition_owners.get(partition_name) {
            log::info!(
                "🏷️  [LazyPartitionExec] Partition '{}' owner: '{}', my_node_id: '{}'",
                partition_name,
                owner_node_id,
                my_node_id
            );

            // 3. 如果 owner 不是当前节点，跳过（返回空流）
            if owner_node_id != &my_node_id {
                log::info!(
                    "⏭️  [LazyPartitionExec] Partition '{}' belongs to node '{}', but I am '{}'. Skipping and returning empty stream.",
                    partition_name,
                    owner_node_id,
                    my_node_id
                );

                // 🔧 FIX: 返回真正的空流（不包含任何 batch），而不是包含一个空 batch
                // 这样 DataFusion 在合并多个节点的流时不会因为 schema 不匹配而出错
                use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
                use futures::stream;

                log::info!(
                    "⚠️  [LazyPartitionExec] Returning empty stream for partition '{}'. Output schema fields: {}, names: {:?}",
                    partition_name,
                    self.output_schema.fields().len(),
                    self.output_schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>()
                );

                let empty_stream = stream::empty();
                let adapter =
                    RecordBatchStreamAdapter::new(self.output_schema.clone(), empty_stream);
                return Ok(Box::pin(adapter));
            }
        } else {
            log::warn!(
                "⚠️  [LazyPartitionExec] Partition '{}' has no owner info, attempting to load",
                partition_name
            );
        }

        // 从 Engine 加载 partition
        let partition = engine.get_partition(&self.table_name, partition_name).await
            .ok_or_else(|| {
                crate::utils::error::CoreError::Internal(format!(
                    "Partition '{}' not found in table '{}'. This partition should be owned by this node but is missing!",
                    partition_name,
                    self.table_name
                ))
            })?;

        log::info!(
            "🔍 [LazyPartitionExec] Loaded partition '{}'. Base schema fields: {}, Output schema fields: {}",
            partition_name,
            partition.arrow_schema.fields().len(),
            self.output_schema.fields().len()
        );
        log::info!(
            "🔍 [LazyPartitionExec] Base schema: {:?}",
            partition
                .arrow_schema
                .fields()
                .iter()
                .map(|f| f.name())
                .collect::<Vec<_>>()
        );
        log::info!(
            "🔍 [LazyPartitionExec] Output schema: {:?}",
            self.output_schema
                .fields()
                .iter()
                .map(|f| f.name())
                .collect::<Vec<_>>()
        );
        if let Some(ref proj) = self.projection {
            log::info!("🔍 [LazyPartitionExec] Projection indices: {:?}", proj);
        }

        // 创建 PartitionTableProvider
        let provider = PartitionTableProvider::new(partition);

        // 直接调用 scan_partition，避免创建 SessionState (可能导致 runtime 嵌套问题)
        // 注意：self.schema 已经应用了 projection，所以这里需要传递原始 projection
        // 而不是 None，让 PartitionTableProvider 自己处理
        let exec = provider.scan_partition(self.projection.as_ref(), &self.filters, self.limit);

        log::info!(
            "✅ [LazyPartitionExec] Partition '{}' loaded, executing scan. Exec schema fields: {}",
            partition_name,
            exec.schema().fields().len()
        );
        log::info!(
            "🔍 [LazyPartitionExec] Exec schema: {:?}",
            exec.schema()
                .fields()
                .iter()
                .map(|f| f.name())
                .collect::<Vec<_>>()
        );

        // 执行计划（这里会创建 MultiSegmentExec 并执行，但只在本地）
        // 使用传入的 context，避免创建新的 RuntimeEnv
        let stream = exec.execute(0, context.clone()).map_err(|e| {
            crate::utils::error::CoreError::Internal(format!("Execute failed: {}", e))
        })?;

        log::info!(
            "🔍 [LazyPartitionExec] Stream schema fields: {}",
            stream.schema().fields().len()
        );
        log::info!(
            "🔍 [LazyPartitionExec] Stream schema: {:?}",
            stream
                .schema()
                .fields()
                .iter()
                .map(|f| f.name())
                .collect::<Vec<_>>()
        );

        Ok(stream)
    }
}

impl fmt::Debug for LazyPartitionExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LazyPartitionExec")
            .field("table", &self.table_name)
            .field("partitions", &self.partition_names.len())
            .field("filters", &self.filters.len())
            .field("projection", &self.projection)
            .field("limit", &self.limit)
            .finish()
    }
}

impl DisplayAs for LazyPartitionExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "LazyPartitionExec: table={}, partitions={}, filters={}, projection={:?}, limit={:?}",
            self.table_name,
            self.partition_names.len(),
            self.filters.len(),
            self.projection,
            self.limit
        )
    }
}

impl ExecutionPlan for LazyPartitionExec {
    fn name(&self) -> &str {
        "LazyPartitionExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        // 返回 output_schema（应用 projection 后的 schema）
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
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        log::info!(
            "🚀 [LazyPartitionExec::execute] Partition {} of table '{}'",
            partition,
            self.table_name
        );

        // 尝试使用已有的 Engine 或从上下文获取
        let engine = if let Some(ref eng) = self.engine {
            eng.clone()
        } else {
            // 远程执行：从上下文获取 Engine
            Self::get_engine_from_context(&_context)
                .map_err(|e| DataFusionError::Internal(format!("Failed to get Engine: {}", e)))?
        };

        // 异步执行 load_and_execute，避免在 runtime 中调用 block_on
        let exec = self.clone();
        let context_clone = _context.clone();
        let schema = self.output_schema.clone();
        let schema_for_err = schema.clone();

        let future_stream = async move {
            match exec
                .load_and_execute(partition, engine, context_clone)
                .await
            {
                Ok(stream) => stream,
                Err(e) => {
                    // 如果加载失败，返回一个包含错误的流
                    let err = DataFusionError::Internal(e.to_string());
                    let error_stream = stream::once(async move { Err(err) });
                    Box::pin(RecordBatchStreamAdapter::new(schema_for_err, error_stream))
                        as SendableRecordBatchStream
                }
            }
        };

        // 将 Future<Stream> 转换为 Stream<Item>
        let stream = stream::once(future_stream).flatten();

        // 使用 RecordBatchStreamAdapter 包装
        let adapter = RecordBatchStreamAdapter::new(schema, stream);

        Ok(Box::pin(adapter))
    }
}
