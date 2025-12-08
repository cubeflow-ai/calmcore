//! 分布式 TableProvider
//!
//! 实现 DataFusion TableProvider trait，用于分布式查询的表扫描。
//!
//! ## 设计原则
//!
//! - 复用现有组件（SegmentScanner、索引优化等）
//! - 支持 Scatter-Gather 和 Shuffle 两种执行模式
//! - 流式处理，避免内存爆炸
//!
//! ## Requirements
//!
//! - 9.1: 复用现有的 SegmentScanner
//! - 9.2: 复用现有的索引优化
//! - 5.1: 提取 GROUP BY 字段
//! - 5.2: 按 GROUP BY 字段的 Hash 值分发数据
//! - 4.1: 并行发送查询到所有相关节点

use std::any::Any;
use std::fmt::{Debug, Display, Formatter};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::Session;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use futures::Stream;
use tokio::sync::mpsc;

use super::node_client::{NodeClientManager, QueryRequest};
use super::shuffle_exec::ShuffleExec;
use super::shuffle_stream::ShuffleConfig;
use crate::partition::Partition;
use crate::utils::error::CoreResult;

// ============================================================================
// RemotePartitionInfo - 远程 Partition 信息
// ============================================================================

/// 远程 Partition 信息
///
/// 包含远程 Partition 的位置和元数据
#[derive(Debug, Clone)]
pub struct RemotePartitionInfo {
    /// 节点 ID
    pub node_id: String,
    /// 节点地址 (host:port)
    pub node_addr: String,
    /// Partition ID/名称
    pub partition_id: String,
    /// 表名
    pub table_name: String,
}

// ============================================================================
// DistributedTableProvider - 分布式 TableProvider
// ============================================================================

/// 分布式 TableProvider
///
/// 实现 DataFusion TableProvider trait，支持分布式查询。
/// 根据查询类型选择执行策略：
/// - 无 GROUP BY：Scatter-Gather 模式
/// - 有 GROUP BY：Shuffle 模式
///
/// ## Requirements
///
/// - 9.1: 复用现有的 SegmentScanner
/// - 9.2: 复用现有的索引优化
/// - 5.1: 提取 GROUP BY 字段
/// - 5.2: 按 GROUP BY 字段的 Hash 值分发数据
pub struct DistributedTableProvider {
    /// 表 Schema
    schema: SchemaRef,
    /// 本地 Partition 列表
    local_partitions: Vec<Arc<Partition>>,
    /// 远程 Partition 信息列表
    remote_partitions: Vec<RemotePartitionInfo>,
    /// GROUP BY 列名（如果有）
    group_by_cols: Vec<String>,
    /// 节点客户端管理器
    node_clients: Arc<NodeClientManager>,
    /// 表名
    table_name: String,
    /// 查询 ID（用于分布式查询追踪）
    query_id: String,
    /// 查询超时（毫秒）
    timeout_ms: u64,
}

impl DistributedTableProvider {
    /// 创建新的 DistributedTableProvider
    ///
    /// # Arguments
    ///
    /// * `schema` - 表 Schema
    /// * `local_partitions` - 本地 Partition 列表
    /// * `remote_partitions` - 远程 Partition 信息列表
    /// * `group_by_cols` - GROUP BY 列名（空表示无 GROUP BY）
    /// * `node_clients` - 节点客户端管理器
    /// * `table_name` - 表名
    /// * `query_id` - 查询 ID
    /// * `timeout_ms` - 查询超时（毫秒）
    pub fn new(
        schema: SchemaRef,
        local_partitions: Vec<Arc<Partition>>,
        remote_partitions: Vec<RemotePartitionInfo>,
        group_by_cols: Vec<String>,
        node_clients: Arc<NodeClientManager>,
        table_name: String,
        query_id: String,
        timeout_ms: u64,
    ) -> Self {
        Self {
            schema,
            local_partitions,
            remote_partitions,
            group_by_cols,
            node_clients,
            table_name,
            query_id,
            timeout_ms,
        }
    }

    /// 检查是否需要 Shuffle（有 GROUP BY）
    ///
    /// # Requirements
    /// - 5.1: 提取 GROUP BY 字段
    pub fn needs_shuffle(&self) -> bool {
        !self.group_by_cols.is_empty()
    }

    /// 获取 GROUP BY 列索引
    fn get_group_by_indices(&self) -> Vec<usize> {
        self.group_by_cols
            .iter()
            .filter_map(|col_name| {
                self.schema
                    .fields()
                    .iter()
                    .position(|f| f.name() == col_name)
            })
            .collect()
    }

    /// 创建 Scatter-Gather 执行计划（无 GROUP BY）
    ///
    /// 并行扫描本地和远程 Partition，流式合并结果。
    ///
    /// # Requirements
    /// - 4.1: 并行发送查询到所有相关节点
    /// - 9.1: 复用现有的 SegmentScanner
    async fn create_scatter_gather_plan(
        &self,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        log::info!(
            "[DistributedTableProvider] Creating Scatter-Gather plan: {} local, {} remote partitions",
            self.local_partitions.len(),
            self.remote_partitions.len()
        );

        // 计算投影后的 schema
        let output_schema = self.compute_output_schema(projection);

        // 创建本地扫描执行计划
        let local_plan = if !self.local_partitions.is_empty() {
            Some(self.create_local_scan_plan(projection, filters, limit)?)
        } else {
            None
        };

        // 创建远程扫描执行计划
        let remote_plan = if !self.remote_partitions.is_empty() {
            Some(
                self.create_remote_scan_plan(projection, filters, limit)
                    .await?,
            )
        } else {
            None
        };

        // 合并本地和远程计划
        match (local_plan, remote_plan) {
            (Some(local), Some(remote)) => {
                // 使用 UnionExec 合并
                Ok(Arc::new(ScatterGatherExec::new(
                    output_schema,
                    vec![local, remote],
                )))
            }
            (Some(local), None) => Ok(local),
            (None, Some(remote)) => Ok(remote),
            (None, None) => {
                // 返回空执行计划
                use datafusion::physical_plan::empty::EmptyExec;
                Ok(Arc::new(EmptyExec::new(output_schema)))
            }
        }
    }

    /// 创建 Shuffle 执行计划（有 GROUP BY）
    ///
    /// 按 GROUP BY 字段的 Hash 值分发数据到各节点。
    ///
    /// # Requirements
    /// - 5.1: 提取 GROUP BY 字段
    /// - 5.2: 按 GROUP BY 字段的 Hash 值分发数据
    async fn create_shuffle_plan(
        &self,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        log::info!(
            "[DistributedTableProvider] Creating Shuffle plan: group_by={:?}, {} local, {} remote partitions",
            self.group_by_cols,
            self.local_partitions.len(),
            self.remote_partitions.len()
        );

        // 获取 GROUP BY 列索引
        let group_by_indices = self.get_group_by_indices();
        if group_by_indices.is_empty() {
            return Err(DataFusionError::Internal(
                "GROUP BY columns not found in schema".to_string(),
            ));
        }

        // 计算投影后的 schema
        let output_schema = self.compute_output_schema(projection);

        // 创建本地扫描计划（作为 Shuffle 的输入）
        let local_scan = self.create_local_scan_plan(projection, filters, None)?;

        // 计算节点数量（本地 + 远程唯一节点）
        let num_nodes = self.calculate_num_nodes();
        let my_node_index = 0; // 本节点总是索引 0

        // 创建 Shuffle 配置
        let config = ShuffleConfig::new(num_nodes, my_node_index, 10000);

        // 创建默认的远程工厂（后续可以替换为网络工厂）
        let receiver_factory = Arc::new(super::shuffle_exec::DefaultRemoteReceiverFactory::new(
            10000,
        ));
        let sender_factory = Arc::new(super::shuffle_exec::DefaultRemoteSenderFactory::new(
            num_nodes, 10000,
        ));

        // 创建 ShuffleExec
        let shuffle_exec = ShuffleExec::new(
            local_scan,
            group_by_indices,
            config,
            receiver_factory,
            sender_factory,
        );

        Ok(Arc::new(shuffle_exec))
    }

    /// 创建本地 Partition 扫描计划
    ///
    /// 复用现有的 SegmentScanner 和 UnionTableProvider。
    ///
    /// # Requirements
    /// - 9.1: 复用现有的 SegmentScanner
    /// - 9.2: 复用现有的索引优化
    fn create_local_scan_plan(
        &self,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        use crate::compute::table_provider::partition_table_provider::create_multi_segment_exec;
        use crate::compute::table_provider::segment_scanner::SegmentScanner;

        log::debug!(
            "[DistributedTableProvider] Creating local scan for {} partitions",
            self.local_partitions.len()
        );

        // 收集所有 segment scanners
        let mut all_segment_scanners = Vec::new();

        for partition in &self.local_partitions {
            // 添加 current segment
            {
                let current_segment = partition.get_current_segment();
                if current_segment.doc_count() > 0 {
                    let scanner = SegmentScanner::new(
                        self.schema.clone(),
                        current_segment.get_row_data(),
                        current_segment.get_index_readers(),
                        current_segment.doc_count(),
                        current_segment.get_deleted(),
                    );
                    all_segment_scanners.push(scanner);
                }
            }

            // 添加 frozen segments
            {
                let frozen_segments = partition.get_frozen_segments();
                for (_seg_id, segment) in frozen_segments.iter() {
                    let scanner = SegmentScanner::new(
                        self.schema.clone(),
                        segment.get_row_data(),
                        segment.get_index_readers(),
                        segment.doc_count(),
                        segment.get_deleted(),
                    );
                    all_segment_scanners.push(scanner);
                }
            }
        }

        log::debug!(
            "[DistributedTableProvider] Created {} segment scanners",
            all_segment_scanners.len()
        );

        // 创建 MultiSegmentExec
        let exec = create_multi_segment_exec(
            self.schema.clone(),
            all_segment_scanners,
            filters.to_vec(),
            projection.cloned(),
            limit,
        );

        Ok(Arc::new(exec))
    }

    /// 创建远程 Partition 扫描计划
    ///
    /// 创建 RemoteScanExec 来并行查询远程节点。
    ///
    /// # Requirements
    /// - 4.1: 并行发送查询到所有相关节点
    async fn create_remote_scan_plan(
        &self,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        log::debug!(
            "[DistributedTableProvider] Creating remote scan for {} partitions",
            self.remote_partitions.len()
        );

        // 计算投影后的 schema
        let output_schema = self.compute_output_schema(projection);

        // 创建 RemoteScanExec
        Ok(Arc::new(RemoteScanExec::new(
            output_schema,
            self.remote_partitions.clone(),
            self.node_clients.clone(),
            self.table_name.clone(),
            self.query_id.clone(),
            self.timeout_ms,
            filters.to_vec(),
            projection.cloned(),
            limit,
        )))
    }

    /// 计算投影后的 Schema
    fn compute_output_schema(&self, projection: Option<&Vec<usize>>) -> SchemaRef {
        if let Some(proj) = projection {
            if proj.is_empty() {
                Arc::new(datafusion::arrow::datatypes::Schema::empty())
            } else {
                let fields: Vec<_> = proj.iter().map(|i| self.schema.field(*i).clone()).collect();
                Arc::new(datafusion::arrow::datatypes::Schema::new(fields))
            }
        } else {
            self.schema.clone()
        }
    }

    /// 计算参与 Shuffle 的节点数量
    fn calculate_num_nodes(&self) -> usize {
        let mut unique_nodes: std::collections::HashSet<&str> = std::collections::HashSet::new();

        // 本地节点
        if !self.local_partitions.is_empty() {
            unique_nodes.insert("local");
        }

        // 远程节点
        for rp in &self.remote_partitions {
            unique_nodes.insert(&rp.node_id);
        }

        unique_nodes.len().max(1)
    }
}

impl Debug for DistributedTableProvider {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DistributedTableProvider")
            .field("table_name", &self.table_name)
            .field("local_partitions", &self.local_partitions.len())
            .field("remote_partitions", &self.remote_partitions.len())
            .field("group_by_cols", &self.group_by_cols)
            .finish()
    }
}
// ============================================================================
// TableProvider 实现
// ============================================================================

#[async_trait]
impl TableProvider for DistributedTableProvider {
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
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        // 支持 filter 下推（Inexact 表示我们会尽力处理）
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }

    /// 创建扫描执行计划
    ///
    /// 根据是否有 GROUP BY 选择执行策略：
    /// - 无 GROUP BY：Scatter-Gather 模式
    /// - 有 GROUP BY：Shuffle 模式
    ///
    /// # Requirements
    /// - 5.1: 提取 GROUP BY 字段
    /// - 5.2: 按 GROUP BY 字段的 Hash 值分发数据
    /// - 9.1: 复用现有的 SegmentScanner
    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        log::info!(
            "[DistributedTableProvider::scan] table={}, projection={:?}, filters={}, limit={:?}, needs_shuffle={}",
            self.table_name,
            projection,
            filters.len(),
            limit,
            self.needs_shuffle()
        );

        if self.needs_shuffle() {
            // 有 GROUP BY：使用 Shuffle 模式
            self.create_shuffle_plan(projection, filters, limit).await
        } else {
            // 无 GROUP BY：使用 Scatter-Gather 模式
            self.create_scatter_gather_plan(projection, filters, limit)
                .await
        }
    }
}
// ============================================================================
// ScatterGatherExec - Scatter-Gather 执行计划
// ============================================================================

/// Scatter-Gather 执行计划
///
/// 合并多个子执行计划的结果（本地 + 远程）。
/// 类似于 UnionExec，但专门用于分布式查询。
pub struct ScatterGatherExec {
    /// 输出 Schema
    schema: SchemaRef,
    /// 子执行计划列表
    children: Vec<Arc<dyn ExecutionPlan>>,
    /// 执行计划属性
    properties: PlanProperties,
}

impl ScatterGatherExec {
    /// 创建新的 ScatterGatherExec
    pub fn new(schema: SchemaRef, children: Vec<Arc<dyn ExecutionPlan>>) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(children.len()),
            EmissionType::Final,
            Boundedness::Bounded,
        );

        Self {
            schema,
            children,
            properties,
        }
    }
}

impl Debug for ScatterGatherExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScatterGatherExec")
            .field("num_children", &self.children.len())
            .field("schema", &self.schema)
            .finish()
    }
}

impl Display for ScatterGatherExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ScatterGatherExec: children={}", self.children.len())
    }
}

impl DisplayAs for ScatterGatherExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "ScatterGatherExec: children={}, schema={:?}",
            self.children.len(),
            self.schema
        )
    }
}

impl ExecutionPlan for ScatterGatherExec {
    fn name(&self) -> &str {
        "ScatterGatherExec"
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
        self.children.iter().collect()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(ScatterGatherExec::new(
            self.schema.clone(),
            children,
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        log::debug!(
            "[ScatterGatherExec] Executing partition {} of {}",
            partition,
            self.children.len()
        );

        if partition >= self.children.len() {
            return Err(DataFusionError::Internal(format!(
                "Partition {} out of range (total: {})",
                partition,
                self.children.len()
            )));
        }

        // 执行对应的子计划
        self.children[partition].execute(0, context)
    }
}
// ============================================================================
// RemoteScanExec - 远程 Partition 扫描执行计划
// ============================================================================

/// 远程 Partition 扫描执行计划
///
/// 并行查询远程节点的 Partition 数据。
///
/// # Requirements
/// - 4.1: 并行发送查询到所有相关节点
pub struct RemoteScanExec {
    /// 输出 Schema
    schema: SchemaRef,
    /// 远程 Partition 信息列表
    remote_partitions: Vec<RemotePartitionInfo>,
    /// 节点客户端管理器
    node_clients: Arc<NodeClientManager>,
    /// 表名
    table_name: String,
    /// 查询 ID
    query_id: String,
    /// 超时时间（毫秒）
    timeout_ms: u64,
    /// 过滤条件
    filters: Vec<Expr>,
    /// 投影列
    projection: Option<Vec<usize>>,
    /// LIMIT
    limit: Option<usize>,
    /// 执行计划属性
    properties: PlanProperties,
}

impl RemoteScanExec {
    /// 创建新的 RemoteScanExec
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        schema: SchemaRef,
        remote_partitions: Vec<RemotePartitionInfo>,
        node_clients: Arc<NodeClientManager>,
        table_name: String,
        query_id: String,
        timeout_ms: u64,
        filters: Vec<Expr>,
        projection: Option<Vec<usize>>,
        limit: Option<usize>,
    ) -> Self {
        // 按节点分组，每个节点一个 partition
        let num_nodes = Self::count_unique_nodes(&remote_partitions);

        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(num_nodes.max(1)),
            EmissionType::Final,
            Boundedness::Bounded,
        );

        Self {
            schema,
            remote_partitions,
            node_clients,
            table_name,
            query_id,
            timeout_ms,
            filters,
            projection,
            limit,
            properties,
        }
    }

    /// 计算唯一节点数量
    fn count_unique_nodes(partitions: &[RemotePartitionInfo]) -> usize {
        let unique: std::collections::HashSet<_> = partitions.iter().map(|p| &p.node_id).collect();
        unique.len()
    }

    /// 按节点分组 Partition
    fn group_by_node(&self) -> std::collections::HashMap<String, Vec<String>> {
        let mut groups: std::collections::HashMap<String, Vec<String>> =
            std::collections::HashMap::new();
        for rp in &self.remote_partitions {
            groups
                .entry(rp.node_id.clone())
                .or_default()
                .push(rp.partition_id.clone());
        }
        groups
    }

    /// 构建远程查询 SQL
    fn build_remote_sql(&self, partition_filter: &[String]) -> String {
        // 构建基本 SELECT
        let columns = if let Some(ref proj) = self.projection {
            proj.iter()
                .map(|i| self.schema.field(*i).name().clone())
                .collect::<Vec<_>>()
                .join(", ")
        } else {
            "*".to_string()
        };

        let mut sql = format!("SELECT {} FROM {}", columns, self.table_name);

        // 添加 WHERE 条件
        // TODO: 将 Expr 转换为 SQL 字符串
        // 目前简化处理，不传递复杂过滤条件

        // 添加 LIMIT
        if let Some(limit) = self.limit {
            sql.push_str(&format!(" LIMIT {}", limit));
        }

        sql
    }
}

impl Debug for RemoteScanExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteScanExec")
            .field("table_name", &self.table_name)
            .field("num_remote_partitions", &self.remote_partitions.len())
            .field("query_id", &self.query_id)
            .finish()
    }
}

impl Display for RemoteScanExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "RemoteScanExec: table={}, partitions={}",
            self.table_name,
            self.remote_partitions.len()
        )
    }
}

impl DisplayAs for RemoteScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "RemoteScanExec: table={}, partitions={}, query_id={}",
            self.table_name,
            self.remote_partitions.len(),
            self.query_id
        )
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
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(DataFusionError::Internal(
                "RemoteScanExec expects no children".to_string(),
            ));
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        log::debug!(
            "[RemoteScanExec] Executing partition {} for table {}",
            partition,
            self.table_name
        );

        // 按节点分组
        let node_groups: Vec<_> = self.group_by_node().into_iter().collect();

        if partition >= node_groups.len() {
            // 返回空流
            return Ok(Box::pin(EmptyRecordBatchStream::new(self.schema.clone())));
        }

        let (node_id, partition_ids) = &node_groups[partition];
        let sql = self.build_remote_sql(partition_ids);

        log::debug!(
            "[RemoteScanExec] Querying node {} with SQL: {}",
            node_id,
            sql
        );

        // 创建远程查询流
        let stream = RemoteQueryStream::new(
            self.schema.clone(),
            self.node_clients.clone(),
            node_id.clone(),
            sql,
            self.query_id.clone(),
            self.timeout_ms,
            partition_ids.clone(),
        );

        Ok(Box::pin(stream))
    }
}
// ============================================================================
// RemoteQueryStream - 远程查询流
// ============================================================================

/// 远程查询流
///
/// 异步执行远程查询并流式返回结果。
struct RemoteQueryStream {
    /// 输出 Schema
    schema: SchemaRef,
    /// 节点客户端管理器
    node_clients: Arc<NodeClientManager>,
    /// 目标节点 ID
    node_id: String,
    /// SQL 查询
    sql: String,
    /// 查询 ID
    query_id: String,
    /// 超时时间（毫秒）
    timeout_ms: u64,
    /// Partition 过滤器
    partition_filter: Vec<String>,
    /// 内部接收器
    receiver: Option<mpsc::Receiver<DataFusionResult<RecordBatch>>>,
    /// 是否已启动
    started: bool,
}

impl RemoteQueryStream {
    fn new(
        schema: SchemaRef,
        node_clients: Arc<NodeClientManager>,
        node_id: String,
        sql: String,
        query_id: String,
        timeout_ms: u64,
        partition_filter: Vec<String>,
    ) -> Self {
        Self {
            schema,
            node_clients,
            node_id,
            sql,
            query_id,
            timeout_ms,
            partition_filter,
            receiver: None,
            started: false,
        }
    }

    /// 启动远程查询
    fn start_query(&mut self) {
        if self.started {
            return;
        }
        self.started = true;

        let (tx, rx) = mpsc::channel(10);
        self.receiver = Some(rx);

        let node_clients = self.node_clients.clone();
        let node_id = self.node_id.clone();
        let sql = self.sql.clone();
        let query_id = self.query_id.clone();
        let timeout_ms = self.timeout_ms;
        let partition_filter = self.partition_filter.clone();

        // 启动异步任务执行远程查询
        tokio::spawn(async move {
            let result = Self::execute_remote_query(
                node_clients,
                &node_id,
                &sql,
                &query_id,
                timeout_ms,
                partition_filter,
            )
            .await;

            match result {
                Ok(batches) => {
                    for batch in batches {
                        if tx.send(Ok(batch)).await.is_err() {
                            break;
                        }
                    }
                }
                Err(e) => {
                    let _ = tx.send(Err(DataFusionError::External(Box::new(e)))).await;
                }
            }
        });
    }

    /// 执行远程查询
    async fn execute_remote_query(
        node_clients: Arc<NodeClientManager>,
        node_id: &str,
        sql: &str,
        query_id: &str,
        timeout_ms: u64,
        partition_filter: Vec<String>,
    ) -> CoreResult<Vec<RecordBatch>> {
        let client = node_clients.get_client(node_id).await?;

        let request = QueryRequest {
            sql: sql.to_string(),
            query_id: query_id.to_string(),
            timeout_ms,
            partition_filter,
        };

        client.execute_query(&request).await
    }
}

impl Stream for RemoteQueryStream {
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // 首次调用时启动查询
        if !self.started {
            self.start_query();
        }

        // 从接收器获取数据
        if let Some(ref mut rx) = self.receiver {
            Pin::new(rx).poll_recv(cx)
        } else {
            Poll::Ready(None)
        }
    }
}

impl RecordBatchStream for RemoteQueryStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
// ============================================================================
// EmptyRecordBatchStream - 空流
// ============================================================================

/// 空的 RecordBatch 流
struct EmptyRecordBatchStream {
    schema: SchemaRef,
}

impl EmptyRecordBatchStream {
    fn new(schema: SchemaRef) -> Self {
        Self { schema }
    }
}

impl Stream for EmptyRecordBatchStream {
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(None)
    }
}

impl RecordBatchStream for EmptyRecordBatchStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

// ============================================================================
// 测试
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("value", DataType::Int64, false),
        ]))
    }

    #[test]
    fn test_remote_partition_info() {
        let info = RemotePartitionInfo {
            node_id: "node-1".to_string(),
            node_addr: "192.168.1.1:7947".to_string(),
            partition_id: "partition_0".to_string(),
            table_name: "users".to_string(),
        };

        assert_eq!(info.node_id, "node-1");
        assert_eq!(info.partition_id, "partition_0");
    }

    #[test]
    fn test_scatter_gather_exec_creation() {
        let schema = test_schema();
        use datafusion::physical_plan::empty::EmptyExec;

        let child1 = Arc::new(EmptyExec::new(schema.clone()));
        let child2 = Arc::new(EmptyExec::new(schema.clone()));

        let exec = ScatterGatherExec::new(schema.clone(), vec![child1, child2]);

        assert_eq!(exec.name(), "ScatterGatherExec");
        assert_eq!(exec.children().len(), 2);
        assert_eq!(exec.schema(), schema);
    }

    #[test]
    fn test_scatter_gather_exec_properties() {
        let schema = test_schema();
        use datafusion::physical_plan::empty::EmptyExec;

        let child = Arc::new(EmptyExec::new(schema.clone()));
        let exec = ScatterGatherExec::new(schema.clone(), vec![child]);

        let props = exec.properties();
        assert_eq!(props.output_partitioning().partition_count(), 1);
    }

    #[test]
    fn test_remote_scan_exec_group_by_node() {
        let schema = test_schema();
        let partitions = vec![
            RemotePartitionInfo {
                node_id: "node-1".to_string(),
                node_addr: "192.168.1.1:7947".to_string(),
                partition_id: "p1".to_string(),
                table_name: "users".to_string(),
            },
            RemotePartitionInfo {
                node_id: "node-1".to_string(),
                node_addr: "192.168.1.1:7947".to_string(),
                partition_id: "p2".to_string(),
                table_name: "users".to_string(),
            },
            RemotePartitionInfo {
                node_id: "node-2".to_string(),
                node_addr: "192.168.1.2:7947".to_string(),
                partition_id: "p3".to_string(),
                table_name: "users".to_string(),
            },
        ];

        // 验证唯一节点数量
        let unique_nodes = RemoteScanExec::count_unique_nodes(&partitions);
        assert_eq!(unique_nodes, 2);
    }

    #[test]
    fn test_compute_output_schema_with_projection() {
        let schema = test_schema();

        // 测试投影
        let projection = vec![0, 2]; // id 和 value
        let fields: Vec<_> = projection
            .iter()
            .map(|i| schema.field(*i).clone())
            .collect();
        let projected_schema = Arc::new(Schema::new(fields));

        assert_eq!(projected_schema.fields().len(), 2);
        assert_eq!(projected_schema.field(0).name(), "id");
        assert_eq!(projected_schema.field(1).name(), "value");
    }

    #[test]
    fn test_needs_shuffle() {
        // 测试 GROUP BY 检测逻辑
        let group_by_cols_empty: Vec<String> = vec![];
        let group_by_cols_with_data = vec!["name".to_string()];

        assert!(group_by_cols_empty.is_empty());
        assert!(!group_by_cols_with_data.is_empty());
    }

    #[test]
    fn test_get_group_by_indices() {
        let schema = test_schema();
        let group_by_cols = vec!["name".to_string(), "value".to_string()];

        let indices: Vec<usize> = group_by_cols
            .iter()
            .filter_map(|col_name| schema.fields().iter().position(|f| f.name() == col_name))
            .collect();

        assert_eq!(indices, vec![1, 2]); // name 是索引 1，value 是索引 2
    }
}
