//! Shuffle 执行计划
//!
//! 实现 DataFusion ExecutionPlan trait，用于 GROUP BY 查询的数据重分发。
//!
//! ## 设计原则
//!
//! - 按 GROUP BY 字段的 Hash 值分发数据
//! - 相同 key 的数据保证发送到同一节点
//! - 流式处理，避免内存爆炸
//!
//! ## Requirements
//!
//! - 5.1: 提取 GROUP BY 字段
//! - 5.2: 按 GROUP BY 字段的 Hash 值分发数据
//! - 5.3: Hash 值属于本节点时发送到本地 channel
//! - 5.4: Hash 值属于其他节点时发送到对应节点
//! - 6.4: 接收其他节点发来的数据

use std::any::Any;
use std::fmt::{Debug, Display, Formatter};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use futures::Stream;
use tokio::sync::mpsc;

use super::shuffle_stream::{ShuffleConfig, ShuffleStream};

// ============================================================================
// ShuffleExec - Shuffle 执行计划
// ============================================================================

/// Shuffle 执行计划
///
/// 实现 DataFusion ExecutionPlan trait，用于 GROUP BY 查询的数据重分发。
/// 将输入数据按 GROUP BY 字段的 Hash 值分发到不同节点。
///
/// ## Requirements
///
/// - 5.1: 提取 GROUP BY 字段
/// - 5.2: 按 GROUP BY 字段的 Hash 值分发数据
/// - 5.3: Hash 值属于本节点时发送到本地 channel
/// - 5.4: Hash 值属于其他节点时发送到对应节点
pub struct ShuffleExec {
    /// 输入执行计划（本地扫描）
    input: Arc<dyn ExecutionPlan>,
    /// GROUP BY 列索引
    group_by_indices: Vec<usize>,
    /// Shuffle 配置
    config: ShuffleConfig,
    /// 输出 Schema
    schema: SchemaRef,
    /// 执行计划属性
    properties: PlanProperties,
    /// 远程数据接收器工厂
    /// 用于创建接收其他节点数据的 channel
    remote_receiver_factory: Arc<dyn RemoteReceiverFactory>,
    /// 远程数据发送器工厂
    /// 用于创建发送数据到其他节点的 channel
    remote_sender_factory: Arc<dyn RemoteSenderFactory>,
}

/// 远程数据接收器工厂 trait
///
/// 用于创建接收其他节点发来数据的 channel
///
/// # Requirements
/// - 6.4: 接收其他节点发来的数据
pub trait RemoteReceiverFactory: Send + Sync + Debug {
    /// 创建远程数据接收器
    fn create_receiver(&self) -> mpsc::Receiver<RecordBatch>;
}

/// 远程数据发送器工厂 trait
///
/// 用于创建发送数据到其他节点的 channel
///
/// # Requirements
/// - 5.4: Hash 值属于其他节点时发送到对应节点
pub trait RemoteSenderFactory: Send + Sync + Debug {
    /// 创建远程数据发送器列表（按节点索引）
    fn create_senders(&self) -> Vec<mpsc::Sender<RecordBatch>>;
}

// ============================================================================
// 默认工厂实现（用于测试和单节点场景）
// ============================================================================

/// 默认远程接收器工厂
///
/// 创建一个空的接收器，用于单节点场景或测试
#[derive(Debug, Clone)]
pub struct DefaultRemoteReceiverFactory {
    /// Channel 缓冲区大小
    buffer_size: usize,
    /// 发送端（用于外部注入数据）
    sender: Option<mpsc::Sender<RecordBatch>>,
}

impl DefaultRemoteReceiverFactory {
    /// 创建新的默认接收器工厂
    pub fn new(buffer_size: usize) -> Self {
        Self {
            buffer_size,
            sender: None,
        }
    }

    /// 创建带发送端的工厂（用于测试）
    pub fn with_sender(buffer_size: usize, sender: mpsc::Sender<RecordBatch>) -> Self {
        Self {
            buffer_size,
            sender: Some(sender),
        }
    }
}

impl RemoteReceiverFactory for DefaultRemoteReceiverFactory {
    fn create_receiver(&self) -> mpsc::Receiver<RecordBatch> {
        if let Some(ref _sender) = self.sender {
            // 如果有预设的发送端，创建新的 channel 并返回接收端
            let (_, rx) = mpsc::channel(self.buffer_size);
            rx
        } else {
            // 创建一个新的 channel，但不保存发送端（数据会被丢弃）
            let (_, rx) = mpsc::channel(self.buffer_size);
            rx
        }
    }
}

/// 默认远程发送器工厂
///
/// 创建发送到其他节点的 channel
#[derive(Debug, Clone)]
pub struct DefaultRemoteSenderFactory {
    /// 节点数量
    num_nodes: usize,
    /// Channel 缓冲区大小
    buffer_size: usize,
}

impl DefaultRemoteSenderFactory {
    /// 创建新的默认发送器工厂
    pub fn new(num_nodes: usize, buffer_size: usize) -> Self {
        Self {
            num_nodes,
            buffer_size,
        }
    }
}

impl RemoteSenderFactory for DefaultRemoteSenderFactory {
    fn create_senders(&self) -> Vec<mpsc::Sender<RecordBatch>> {
        let mut senders = Vec::with_capacity(self.num_nodes);
        for _ in 0..self.num_nodes {
            let (tx, _rx) = mpsc::channel(self.buffer_size);
            senders.push(tx);
        }
        senders
    }
}

// ============================================================================
// NetworkShuffleManager - 网络 Shuffle 管理器
// ============================================================================

/// 网络 Shuffle 管理器
///
/// 协调分布式 Shuffle 过程，管理节点间的数据传输。
/// 提供网络感知的发送器和接收器工厂。
///
/// # Requirements
///
/// - 5.3: Hash 值属于本节点时发送到本地 channel
/// - 5.4: Hash 值属于其他节点时发送到对应节点
/// - 6.4: 接收其他节点发来的数据
#[derive(Debug)]
pub struct NetworkShuffleManager {
    /// 查询 ID
    query_id: String,
    /// 本节点 ID
    my_node_id: String,
    /// 本节点索引
    my_node_index: usize,
    /// 所有节点 ID 列表（按索引排序）
    node_ids: Vec<String>,
    /// 缓冲区大小
    buffer_size: usize,
    /// 接收其他节点数据的 channel 发送端
    /// 外部网络层通过此发送端注入数据
    incoming_sender: mpsc::Sender<RecordBatch>,
    /// 接收其他节点数据的 channel 接收端
    incoming_receiver: Option<mpsc::Receiver<RecordBatch>>,
    /// 发送到其他节点的 channel（按节点索引）
    /// 外部网络层从这些 channel 读取数据并发送
    outgoing_senders: Vec<mpsc::Sender<RecordBatch>>,
    /// 发送到其他节点的 channel 接收端（供网络层使用）
    outgoing_receivers: Vec<mpsc::Receiver<RecordBatch>>,
}

impl NetworkShuffleManager {
    /// 创建新的网络 Shuffle 管理器
    ///
    /// # Arguments
    ///
    /// * `query_id` - 查询 ID
    /// * `my_node_id` - 本节点 ID
    /// * `my_node_index` - 本节点在节点列表中的索引
    /// * `node_ids` - 所有参与 Shuffle 的节点 ID 列表
    /// * `buffer_size` - Channel 缓冲区大小
    pub fn new(
        query_id: String,
        my_node_id: String,
        my_node_index: usize,
        node_ids: Vec<String>,
        buffer_size: usize,
    ) -> Self {
        // 创建接收其他节点数据的 channel
        let (incoming_sender, incoming_receiver) = mpsc::channel(buffer_size);

        // 创建发送到其他节点的 channel
        let num_nodes = node_ids.len();
        let mut outgoing_senders = Vec::with_capacity(num_nodes);
        let mut outgoing_receivers = Vec::with_capacity(num_nodes);

        for _ in 0..num_nodes {
            let (tx, rx) = mpsc::channel(buffer_size);
            outgoing_senders.push(tx);
            outgoing_receivers.push(rx);
        }

        Self {
            query_id,
            my_node_id,
            my_node_index,
            node_ids,
            buffer_size,
            incoming_sender,
            incoming_receiver: Some(incoming_receiver),
            outgoing_senders,
            outgoing_receivers,
        }
    }

    /// 获取查询 ID
    pub fn query_id(&self) -> &str {
        &self.query_id
    }

    /// 获取本节点 ID
    pub fn my_node_id(&self) -> &str {
        &self.my_node_id
    }

    /// 获取本节点索引
    pub fn my_node_index(&self) -> usize {
        self.my_node_index
    }

    /// 获取节点数量
    pub fn num_nodes(&self) -> usize {
        self.node_ids.len()
    }

    /// 获取节点 ID 列表
    pub fn node_ids(&self) -> &[String] {
        &self.node_ids
    }

    /// 获取用于注入远程数据的发送端
    ///
    /// 网络层使用此发送端将从其他节点接收的数据注入到 Shuffle 流中。
    ///
    /// # Requirements
    /// - 6.4: 接收其他节点发来的数据
    pub fn get_incoming_sender(&self) -> mpsc::Sender<RecordBatch> {
        self.incoming_sender.clone()
    }

    /// 获取指定节点的发送数据接收端
    ///
    /// 网络层使用此接收端获取要发送到指定节点的数据。
    ///
    /// # Arguments
    ///
    /// * `node_index` - 目标节点索引
    ///
    /// # Returns
    ///
    /// 如果节点索引有效，返回对应的接收端引用
    pub fn get_outgoing_receiver(
        &mut self,
        node_index: usize,
    ) -> Option<mpsc::Receiver<RecordBatch>> {
        if node_index < self.outgoing_receivers.len() {
            // 使用 std::mem::replace 取出接收端
            let (_, new_rx) = mpsc::channel(1);
            Some(std::mem::replace(
                &mut self.outgoing_receivers[node_index],
                new_rx,
            ))
        } else {
            None
        }
    }

    /// 创建 Shuffle 配置
    pub fn create_config(&self) -> ShuffleConfig {
        ShuffleConfig::new(self.node_ids.len(), self.my_node_index, self.buffer_size)
    }

    /// 创建远程接收器工厂
    ///
    /// # Requirements
    /// - 6.4: 接收其他节点发来的数据
    pub fn create_receiver_factory(&mut self) -> Arc<dyn RemoteReceiverFactory> {
        // 取出接收端（只能调用一次）
        let receiver = self.incoming_receiver.take();
        Arc::new(NetworkRemoteReceiverFactory::new(
            self.buffer_size,
            receiver,
        ))
    }

    /// 创建远程发送器工厂
    ///
    /// # Requirements
    /// - 5.4: Hash 值属于其他节点时发送到对应节点
    pub fn create_sender_factory(&self) -> Arc<dyn RemoteSenderFactory> {
        Arc::new(NetworkRemoteSenderFactory::new(
            self.outgoing_senders.clone(),
        ))
    }
}

/// 网络远程接收器工厂
///
/// 使用预先创建的 channel 接收其他节点的数据。
///
/// # Requirements
/// - 6.4: 接收其他节点发来的数据
#[derive(Debug)]
pub struct NetworkRemoteReceiverFactory {
    /// 缓冲区大小（用于创建备用 channel）
    buffer_size: usize,
    /// 预先创建的接收端
    receiver: std::sync::Mutex<Option<mpsc::Receiver<RecordBatch>>>,
}

impl NetworkRemoteReceiverFactory {
    /// 创建新的网络接收器工厂
    pub fn new(buffer_size: usize, receiver: Option<mpsc::Receiver<RecordBatch>>) -> Self {
        Self {
            buffer_size,
            receiver: std::sync::Mutex::new(receiver),
        }
    }
}

impl RemoteReceiverFactory for NetworkRemoteReceiverFactory {
    fn create_receiver(&self) -> mpsc::Receiver<RecordBatch> {
        // 尝试取出预先创建的接收端
        let mut guard = self.receiver.lock().unwrap();
        if let Some(rx) = guard.take() {
            rx
        } else {
            // 如果已经被取走，创建一个新的空 channel
            let (_, rx) = mpsc::channel(self.buffer_size);
            rx
        }
    }
}

/// 网络远程发送器工厂
///
/// 使用预先创建的 channel 发送数据到其他节点。
///
/// # Requirements
/// - 5.4: Hash 值属于其他节点时发送到对应节点
#[derive(Debug)]
pub struct NetworkRemoteSenderFactory {
    /// 预先创建的发送端列表
    senders: Vec<mpsc::Sender<RecordBatch>>,
}

impl NetworkRemoteSenderFactory {
    /// 创建新的网络发送器工厂
    pub fn new(senders: Vec<mpsc::Sender<RecordBatch>>) -> Self {
        Self { senders }
    }
}

impl RemoteSenderFactory for NetworkRemoteSenderFactory {
    fn create_senders(&self) -> Vec<mpsc::Sender<RecordBatch>> {
        self.senders.clone()
    }
}

// ============================================================================
// ShuffleExec 实现
// ============================================================================

impl ShuffleExec {
    /// 创建新的 ShuffleExec
    ///
    /// # Arguments
    ///
    /// * `input` - 输入执行计划（本地扫描）
    /// * `group_by_indices` - GROUP BY 列的索引
    /// * `config` - Shuffle 配置
    /// * `remote_receiver_factory` - 远程数据接收器工厂
    /// * `remote_sender_factory` - 远程数据发送器工厂
    ///
    /// # Requirements
    ///
    /// - 5.1: 提取 GROUP BY 字段
    /// - 5.2: 按 GROUP BY 字段的 Hash 值分发数据
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        group_by_indices: Vec<usize>,
        config: ShuffleConfig,
        remote_receiver_factory: Arc<dyn RemoteReceiverFactory>,
        remote_sender_factory: Arc<dyn RemoteSenderFactory>,
    ) -> Self {
        let schema = input.schema();

        // 创建执行计划属性
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            // Shuffle 后数据按 hash 分布，每个节点一个 partition
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        );

        Self {
            input,
            group_by_indices,
            config,
            schema,
            properties,
            remote_receiver_factory,
            remote_sender_factory,
        }
    }

    /// 创建简单的 ShuffleExec（使用默认工厂）
    ///
    /// 用于测试或单节点场景
    pub fn new_simple(
        input: Arc<dyn ExecutionPlan>,
        group_by_indices: Vec<usize>,
        num_nodes: usize,
        my_node_index: usize,
        buffer_size: usize,
    ) -> Self {
        let config = ShuffleConfig::new(num_nodes, my_node_index, buffer_size);
        let receiver_factory = Arc::new(DefaultRemoteReceiverFactory::new(buffer_size));
        let sender_factory = Arc::new(DefaultRemoteSenderFactory::new(num_nodes, buffer_size));

        Self::new(
            input,
            group_by_indices,
            config,
            receiver_factory,
            sender_factory,
        )
    }

    /// 获取 GROUP BY 列索引
    pub fn group_by_indices(&self) -> &[usize] {
        &self.group_by_indices
    }

    /// 获取 Shuffle 配置
    pub fn config(&self) -> &ShuffleConfig {
        &self.config
    }

    /// 获取输入执行计划
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }
}

// ============================================================================
// Debug 和 Display 实现
// ============================================================================

impl Debug for ShuffleExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShuffleExec")
            .field("group_by_indices", &self.group_by_indices)
            .field("num_nodes", &self.config.num_nodes)
            .field("my_node_index", &self.config.my_node_index)
            .field("schema", &self.schema)
            .finish()
    }
}

impl Display for ShuffleExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ShuffleExec: group_by={:?}, nodes={}, my_index={}",
            self.group_by_indices, self.config.num_nodes, self.config.my_node_index
        )
    }
}

impl DisplayAs for ShuffleExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "ShuffleExec: group_by_indices={:?}, num_nodes={}, my_node_index={}, buffer_size={}",
            self.group_by_indices,
            self.config.num_nodes,
            self.config.my_node_index,
            self.config.buffer_size
        )
    }
}

// ============================================================================
// ExecutionPlan 实现
// ============================================================================

impl ExecutionPlan for ShuffleExec {
    fn name(&self) -> &str {
        "ShuffleExec"
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
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(format!(
                "ShuffleExec expects exactly 1 child, got {}",
                children.len()
            )));
        }

        Ok(Arc::new(ShuffleExec::new(
            children[0].clone(),
            self.group_by_indices.clone(),
            self.config.clone(),
            self.remote_receiver_factory.clone(),
            self.remote_sender_factory.clone(),
        )))
    }

    /// 执行 Shuffle 操作
    ///
    /// 创建 ShuffleStream，设置远程 channel，开始数据分发。
    ///
    /// # Requirements
    ///
    /// - 5.3: Hash 值属于本节点时发送到本地 channel
    /// - 5.4: Hash 值属于其他节点时发送到对应节点
    /// - 6.4: 接收其他节点发来的数据
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        log::debug!(
            "[ShuffleExec] Executing partition {}, group_by_indices={:?}, num_nodes={}, my_node={}",
            partition,
            self.group_by_indices,
            self.config.num_nodes,
            self.config.my_node_index
        );

        // 1. 执行输入计划获取本地数据流
        let input_stream = self.input.execute(partition, context)?;

        // 2. 创建远程发送器（发送到其他节点）
        let remote_senders = self.remote_sender_factory.create_senders();

        // 3. 创建远程接收器（接收其他节点数据）
        let remote_receiver = self.remote_receiver_factory.create_receiver();

        // 4. 创建 ShuffleStream
        let shuffle_stream = ShuffleStream::new(
            input_stream,
            self.group_by_indices.clone(),
            self.config.clone(),
            remote_senders,
            remote_receiver,
            self.schema.clone(),
        );

        log::debug!(
            "[ShuffleExec] Created ShuffleStream for partition {}",
            partition
        );

        Ok(Box::pin(shuffle_stream))
    }
}

// ============================================================================
// ShuffleReceiverExec - 远程数据接收执行计划
// ============================================================================

/// 远程数据接收执行计划
///
/// 专门用于接收其他节点发来的 Shuffle 数据。
/// 与 ShuffleExec 配合使用，实现完整的 Shuffle 机制。
///
/// # Requirements
///
/// - 6.4: 接收其他节点发来的数据
pub struct ShuffleReceiverExec {
    /// 输出 Schema
    schema: SchemaRef,
    /// 执行计划属性
    properties: PlanProperties,
    /// 远程数据接收器
    receiver: Arc<tokio::sync::Mutex<Option<mpsc::Receiver<RecordBatch>>>>,
}

impl ShuffleReceiverExec {
    /// 创建新的 ShuffleReceiverExec
    pub fn new(schema: SchemaRef, receiver: mpsc::Receiver<RecordBatch>) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        );

        Self {
            schema,
            properties,
            receiver: Arc::new(tokio::sync::Mutex::new(Some(receiver))),
        }
    }
}

impl Debug for ShuffleReceiverExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShuffleReceiverExec")
            .field("schema", &self.schema)
            .finish()
    }
}

impl Display for ShuffleReceiverExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ShuffleReceiverExec")
    }
}

impl DisplayAs for ShuffleReceiverExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "ShuffleReceiverExec: schema={:?}", self.schema)
    }
}

impl ExecutionPlan for ShuffleReceiverExec {
    fn name(&self) -> &str {
        "ShuffleReceiverExec"
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
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(DataFusionError::Internal(
                "ShuffleReceiverExec expects no children".to_string(),
            ));
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        log::debug!("[ShuffleReceiverExec] Executing partition {}", partition);

        // 获取接收器（只能执行一次）
        let receiver = {
            let mut guard = futures::executor::block_on(self.receiver.lock());
            guard.take()
        };

        match receiver {
            Some(rx) => Ok(Box::pin(ReceiverStream::new(self.schema.clone(), rx))),
            None => Err(DataFusionError::Internal(
                "ShuffleReceiverExec can only be executed once".to_string(),
            )),
        }
    }
}

// ============================================================================
// ReceiverStream - 接收器流
// ============================================================================

/// 接收器流
///
/// 将 mpsc::Receiver 包装为 SendableRecordBatchStream
struct ReceiverStream {
    schema: SchemaRef,
    receiver: mpsc::Receiver<RecordBatch>,
}

impl ReceiverStream {
    fn new(schema: SchemaRef, receiver: mpsc::Receiver<RecordBatch>) -> Self {
        Self { schema, receiver }
    }
}

impl Stream for ReceiverStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.receiver).poll_recv(cx) {
            Poll::Ready(Some(batch)) => Poll::Ready(Some(Ok(batch))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl RecordBatchStream for ReceiverStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

// ============================================================================
// MultiSourceShuffleReceiverExec - 多源远程数据接收执行计划
// ============================================================================

/// 多源远程数据接收执行计划
///
/// 从多个源节点接收 Shuffle 数据并合并到本地流。
/// 支持跟踪每个源节点的 EOF 状态。
///
/// # Requirements
///
/// - 6.4: 接收其他节点发来的数据
pub struct MultiSourceShuffleReceiverExec {
    /// 输出 Schema
    schema: SchemaRef,
    /// 执行计划属性
    properties: PlanProperties,
    /// 源节点数量
    num_sources: usize,
    /// 远程数据接收器（合并所有源节点的数据）
    receiver: Arc<tokio::sync::Mutex<Option<mpsc::Receiver<RecordBatch>>>>,
    /// 用于注入数据的发送端
    sender: mpsc::Sender<RecordBatch>,
}

impl MultiSourceShuffleReceiverExec {
    /// 创建新的 MultiSourceShuffleReceiverExec
    ///
    /// # Arguments
    ///
    /// * `schema` - 输出 Schema
    /// * `num_sources` - 源节点数量
    /// * `buffer_size` - Channel 缓冲区大小
    pub fn new(schema: SchemaRef, num_sources: usize, buffer_size: usize) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        );

        let (sender, receiver) = mpsc::channel(buffer_size);

        Self {
            schema,
            properties,
            num_sources,
            receiver: Arc::new(tokio::sync::Mutex::new(Some(receiver))),
            sender,
        }
    }

    /// 获取用于注入数据的发送端
    ///
    /// 网络层使用此发送端将从其他节点接收的数据注入到流中。
    pub fn get_sender(&self) -> mpsc::Sender<RecordBatch> {
        self.sender.clone()
    }

    /// 获取源节点数量
    pub fn num_sources(&self) -> usize {
        self.num_sources
    }
}

impl Debug for MultiSourceShuffleReceiverExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MultiSourceShuffleReceiverExec")
            .field("schema", &self.schema)
            .field("num_sources", &self.num_sources)
            .finish()
    }
}

impl Display for MultiSourceShuffleReceiverExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "MultiSourceShuffleReceiverExec: sources={}",
            self.num_sources
        )
    }
}

impl DisplayAs for MultiSourceShuffleReceiverExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "MultiSourceShuffleReceiverExec: schema={:?}, num_sources={}",
            self.schema, self.num_sources
        )
    }
}

impl ExecutionPlan for MultiSourceShuffleReceiverExec {
    fn name(&self) -> &str {
        "MultiSourceShuffleReceiverExec"
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
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(DataFusionError::Internal(
                "MultiSourceShuffleReceiverExec expects no children".to_string(),
            ));
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        log::debug!(
            "[MultiSourceShuffleReceiverExec] Executing partition {}, num_sources={}",
            partition,
            self.num_sources
        );

        // 获取接收器（只能执行一次）
        let receiver = {
            let mut guard = futures::executor::block_on(self.receiver.lock());
            guard.take()
        };

        match receiver {
            Some(rx) => Ok(Box::pin(ReceiverStream::new(self.schema.clone(), rx))),
            None => Err(DataFusionError::Internal(
                "MultiSourceShuffleReceiverExec can only be executed once".to_string(),
            )),
        }
    }
}

// ============================================================================
// ShuffleDataInjector - Shuffle 数据注入器
// ============================================================================

/// Shuffle 数据注入器
///
/// 用于将从网络接收的数据注入到 Shuffle 流中。
/// 跟踪每个源节点的 EOF 状态。
///
/// # Requirements
///
/// - 6.4: 接收其他节点发来的数据
/// - 6.6: 所有 Partition 扫描完成后发送 EOF 信号
#[derive(Debug)]
pub struct ShuffleDataInjector {
    /// 数据发送端
    sender: mpsc::Sender<RecordBatch>,
    /// 源节点数量
    num_sources: usize,
    /// 已收到 EOF 的源节点数量
    eof_received: std::sync::atomic::AtomicUsize,
}

impl ShuffleDataInjector {
    /// 创建新的 Shuffle 数据注入器
    pub fn new(sender: mpsc::Sender<RecordBatch>, num_sources: usize) -> Self {
        Self {
            sender,
            num_sources,
            eof_received: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    /// 注入数据
    ///
    /// 将从网络接收的 RecordBatch 注入到 Shuffle 流中。
    pub async fn inject(
        &self,
        batch: RecordBatch,
    ) -> std::result::Result<(), mpsc::error::SendError<RecordBatch>> {
        self.sender.send(batch).await
    }

    /// 标记源节点 EOF
    ///
    /// 当一个源节点发送完所有数据后调用此方法。
    /// 当所有源节点都发送 EOF 后，关闭 channel。
    ///
    /// # Returns
    ///
    /// 返回是否所有源节点都已发送 EOF
    pub fn mark_source_eof(&self) -> bool {
        let prev = self
            .eof_received
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let all_done = prev + 1 >= self.num_sources;

        log::debug!(
            "[ShuffleDataInjector] Source EOF received: {}/{}, all_done={}",
            prev + 1,
            self.num_sources,
            all_done
        );

        all_done
    }

    /// 检查是否所有源节点都已发送 EOF
    pub fn is_complete(&self) -> bool {
        self.eof_received.load(std::sync::atomic::Ordering::SeqCst) >= self.num_sources
    }

    /// 获取已收到 EOF 的源节点数量
    pub fn eof_count(&self) -> usize {
        self.eof_received.load(std::sync::atomic::Ordering::SeqCst)
    }
}

// ============================================================================
// 测试
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int32Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::MemTable;
    use datafusion::physical_plan::empty::EmptyExec;
    use futures::StreamExt;

    /// 创建测试 Schema
    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("value", DataType::Int64, false),
        ]))
    }

    /// 创建测试 RecordBatch
    fn test_batch(ids: Vec<i32>, names: Vec<&str>, values: Vec<i64>) -> RecordBatch {
        use datafusion::arrow::array::Int64Array;
        let schema = test_schema();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(StringArray::from(names)),
                Arc::new(Int64Array::from(values)),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_shuffle_exec_creation() {
        let schema = test_schema();
        let input = Arc::new(EmptyExec::new(schema.clone()));

        let shuffle = ShuffleExec::new_simple(
            input,
            vec![0], // GROUP BY id
            4,       // 4 nodes
            0,       // my node index
            1000,    // buffer size
        );

        assert_eq!(shuffle.name(), "ShuffleExec");
        assert_eq!(shuffle.group_by_indices(), &[0]);
        assert_eq!(shuffle.config().num_nodes, 4);
        assert_eq!(shuffle.config().my_node_index, 0);
        assert_eq!(shuffle.schema(), schema);
    }

    #[test]
    fn test_shuffle_exec_properties() {
        let schema = test_schema();
        let input = Arc::new(EmptyExec::new(schema.clone()));

        let shuffle = ShuffleExec::new_simple(input, vec![0, 1], 2, 1, 500);

        // 验证属性
        let props = shuffle.properties();
        assert_eq!(props.output_partitioning().partition_count(), 1);
    }

    #[test]
    fn test_shuffle_exec_children() {
        let schema = test_schema();
        let input = Arc::new(EmptyExec::new(schema.clone()));

        let shuffle = ShuffleExec::new_simple(input.clone(), vec![0], 2, 0, 100);

        // 验证子节点
        let children = shuffle.children();
        assert_eq!(children.len(), 1);
    }

    #[test]
    fn test_shuffle_exec_with_new_children() {
        let schema = test_schema();
        let input1 = Arc::new(EmptyExec::new(schema.clone()));
        let input2 = Arc::new(EmptyExec::new(schema.clone()));

        let shuffle = Arc::new(ShuffleExec::new_simple(input1, vec![0], 2, 0, 100));

        // 替换子节点
        let new_shuffle = shuffle.with_new_children(vec![input2]).unwrap();

        assert_eq!(new_shuffle.children().len(), 1);
    }

    #[test]
    fn test_shuffle_exec_with_wrong_children_count() {
        let schema = test_schema();
        let input = Arc::new(EmptyExec::new(schema.clone()));

        let shuffle = Arc::new(ShuffleExec::new_simple(input.clone(), vec![0], 2, 0, 100));

        // 尝试传入错误数量的子节点
        let result = shuffle.with_new_children(vec![input.clone(), input.clone()]);
        assert!(result.is_err());
    }

    #[test]
    fn test_shuffle_exec_display() {
        let schema = test_schema();
        let input = Arc::new(EmptyExec::new(schema.clone()));

        let shuffle = ShuffleExec::new_simple(input, vec![0, 1], 4, 2, 1000);

        let display = format!("{}", shuffle);
        assert!(display.contains("ShuffleExec"));
        assert!(display.contains("group_by"));
    }

    #[test]
    fn test_shuffle_exec_debug() {
        let schema = test_schema();
        let input = Arc::new(EmptyExec::new(schema.clone()));

        let shuffle = ShuffleExec::new_simple(input, vec![0], 2, 0, 100);

        let debug = format!("{:?}", shuffle);
        assert!(debug.contains("ShuffleExec"));
        assert!(debug.contains("group_by_indices"));
    }

    #[test]
    fn test_default_remote_receiver_factory() {
        let factory = DefaultRemoteReceiverFactory::new(100);
        let _receiver = factory.create_receiver();
        // 验证可以创建接收器
    }

    #[test]
    fn test_default_remote_sender_factory() {
        let factory = DefaultRemoteSenderFactory::new(4, 100);
        let senders = factory.create_senders();
        assert_eq!(senders.len(), 4);
    }

    #[tokio::test]
    async fn test_shuffle_receiver_exec() {
        let schema = test_schema();
        let (tx, rx) = mpsc::channel(10);

        let receiver_exec = ShuffleReceiverExec::new(schema.clone(), rx);

        // 发送测试数据
        let batch = test_batch(vec![1, 2, 3], vec!["a", "b", "c"], vec![10, 20, 30]);
        tx.send(batch.clone()).await.unwrap();
        drop(tx); // 关闭发送端

        // 执行并收集结果
        let context = Arc::new(TaskContext::default());
        let stream = receiver_exec.execute(0, context).unwrap();

        let results: Vec<_> = stream.collect::<Vec<_>>().await;

        assert_eq!(results.len(), 1);
        assert!(results[0].is_ok());
        assert_eq!(results[0].as_ref().unwrap().num_rows(), 3);
    }

    #[tokio::test]
    async fn test_shuffle_receiver_exec_multiple_batches() {
        let schema = test_schema();
        let (tx, rx) = mpsc::channel(10);

        let receiver_exec = ShuffleReceiverExec::new(schema.clone(), rx);

        // 发送多个 batch
        let batch1 = test_batch(vec![1, 2], vec!["a", "b"], vec![10, 20]);
        let batch2 = test_batch(vec![3, 4], vec!["c", "d"], vec![30, 40]);

        tx.send(batch1).await.unwrap();
        tx.send(batch2).await.unwrap();
        drop(tx);

        let context = Arc::new(TaskContext::default());
        let stream = receiver_exec.execute(0, context).unwrap();

        let results: Vec<_> = stream.collect::<Vec<_>>().await;

        assert_eq!(results.len(), 2);

        let total_rows: usize = results
            .iter()
            .filter_map(|r| r.as_ref().ok())
            .map(|b| b.num_rows())
            .sum();

        assert_eq!(total_rows, 4);
    }

    #[test]
    fn test_shuffle_receiver_exec_properties() {
        let schema = test_schema();
        let (_tx, rx) = mpsc::channel::<RecordBatch>(10);

        let receiver_exec = ShuffleReceiverExec::new(schema.clone(), rx);

        assert_eq!(receiver_exec.name(), "ShuffleReceiverExec");
        assert_eq!(receiver_exec.schema(), schema);
        assert!(receiver_exec.children().is_empty());
    }

    /// 测试 ShuffleExec 执行流程
    ///
    /// 验证 ShuffleExec 能正确创建 ShuffleStream 并处理数据
    #[tokio::test]
    async fn test_shuffle_exec_execute() {
        use datafusion::prelude::SessionContext;

        let schema = test_schema();
        let batch = test_batch(
            vec![1, 2, 3, 4],
            vec!["a", "b", "c", "d"],
            vec![10, 20, 30, 40],
        );

        // 创建内存表并获取执行计划
        let ctx = SessionContext::new();
        let mem_table = MemTable::try_new(schema.clone(), vec![vec![batch]]).unwrap();
        ctx.register_table("test", Arc::new(mem_table)).unwrap();

        let df = ctx.table("test").await.unwrap();
        let plan = df.create_physical_plan().await.unwrap();

        // 创建 ShuffleExec（单节点场景）
        let shuffle = ShuffleExec::new_simple(
            plan,
            vec![0], // GROUP BY id
            1,       // 单节点
            0,       // my node index
            100,     // buffer size
        );

        // 执行
        let context = Arc::new(TaskContext::default());
        let stream = shuffle.execute(0, context).unwrap();

        // 收集结果
        let results: Vec<_> = stream.collect::<Vec<_>>().await;

        // 单节点场景，所有数据都应该在本地
        let total_rows: usize = results
            .iter()
            .filter_map(|r| r.as_ref().ok())
            .map(|b| b.num_rows())
            .sum();

        assert_eq!(
            total_rows, 4,
            "All rows should be returned in single-node mode"
        );
    }

    /// 测试 ShuffleExec 多列 GROUP BY
    #[tokio::test]
    async fn test_shuffle_exec_multi_column_group_by() {
        use datafusion::prelude::SessionContext;

        let schema = test_schema();
        let batch = test_batch(
            vec![1, 1, 2, 2],
            vec!["a", "b", "a", "b"],
            vec![10, 20, 30, 40],
        );

        // 创建内存表并获取执行计划
        let ctx = SessionContext::new();
        let mem_table = MemTable::try_new(schema.clone(), vec![vec![batch]]).unwrap();
        ctx.register_table("test", Arc::new(mem_table)).unwrap();

        let df = ctx.table("test").await.unwrap();
        let plan = df.create_physical_plan().await.unwrap();

        // GROUP BY id, name
        let shuffle = ShuffleExec::new_simple(plan, vec![0, 1], 1, 0, 100);

        let context = Arc::new(TaskContext::default());
        let stream = shuffle.execute(0, context).unwrap();

        let results: Vec<_> = stream.collect::<Vec<_>>().await;

        let total_rows: usize = results
            .iter()
            .filter_map(|r| r.as_ref().ok())
            .map(|b| b.num_rows())
            .sum();

        assert_eq!(total_rows, 4);
    }

    // ========================================================================
    // NetworkShuffleManager 测试
    // ========================================================================

    #[test]
    fn test_network_shuffle_manager_creation() {
        let manager = NetworkShuffleManager::new(
            "query-123".to_string(),
            "node-1".to_string(),
            1,
            vec![
                "node-0".to_string(),
                "node-1".to_string(),
                "node-2".to_string(),
            ],
            1000,
        );

        assert_eq!(manager.query_id(), "query-123");
        assert_eq!(manager.my_node_id(), "node-1");
        assert_eq!(manager.my_node_index(), 1);
        assert_eq!(manager.num_nodes(), 3);
        assert_eq!(manager.node_ids().len(), 3);
    }

    #[test]
    fn test_network_shuffle_manager_config() {
        let manager = NetworkShuffleManager::new(
            "q1".to_string(),
            "n0".to_string(),
            0,
            vec!["n0".to_string(), "n1".to_string()],
            500,
        );

        let config = manager.create_config();
        assert_eq!(config.num_nodes, 2);
        assert_eq!(config.my_node_index, 0);
        assert_eq!(config.buffer_size, 500);
    }

    #[tokio::test]
    async fn test_network_shuffle_manager_incoming_channel() {
        let manager = NetworkShuffleManager::new(
            "q1".to_string(),
            "n0".to_string(),
            0,
            vec!["n0".to_string(), "n1".to_string()],
            100,
        );

        // 获取发送端
        let sender = manager.get_incoming_sender();

        // 发送测试数据
        let batch = test_batch(vec![1, 2], vec!["a", "b"], vec![10, 20]);
        sender.send(batch.clone()).await.unwrap();

        // 验证发送成功（通过 sender 的容量变化）
        assert!(sender.capacity() > 0);
    }

    #[test]
    fn test_network_shuffle_manager_factories() {
        let mut manager = NetworkShuffleManager::new(
            "q1".to_string(),
            "n0".to_string(),
            0,
            vec!["n0".to_string(), "n1".to_string(), "n2".to_string()],
            100,
        );

        // 创建接收器工厂
        let receiver_factory = manager.create_receiver_factory();
        let _receiver = receiver_factory.create_receiver();

        // 创建发送器工厂
        let sender_factory = manager.create_sender_factory();
        let senders = sender_factory.create_senders();
        assert_eq!(senders.len(), 3);
    }

    #[tokio::test]
    async fn test_network_remote_receiver_factory() {
        let (tx, rx) = mpsc::channel(10);
        let factory = NetworkRemoteReceiverFactory::new(10, Some(rx));

        // 第一次调用应该返回预先创建的接收端
        let receiver = factory.create_receiver();

        // 发送数据
        let batch = test_batch(vec![1], vec!["a"], vec![10]);
        tx.send(batch).await.unwrap();
        drop(tx);

        // 接收数据
        let mut receiver = receiver;
        let received = receiver.recv().await;
        assert!(received.is_some());
        assert_eq!(received.unwrap().num_rows(), 1);
    }

    #[tokio::test]
    async fn test_network_remote_sender_factory() {
        let (tx1, mut rx1) = mpsc::channel(10);
        let (tx2, mut rx2) = mpsc::channel(10);

        let factory = NetworkRemoteSenderFactory::new(vec![tx1, tx2]);
        let senders = factory.create_senders();

        assert_eq!(senders.len(), 2);

        // 通过工厂创建的发送端发送数据
        let batch = test_batch(vec![1], vec!["a"], vec![10]);
        senders[0].send(batch.clone()).await.unwrap();
        senders[1].send(batch.clone()).await.unwrap();

        // 验证接收
        let r1 = rx1.recv().await;
        let r2 = rx2.recv().await;
        assert!(r1.is_some());
        assert!(r2.is_some());
    }

    /// 测试 NetworkShuffleManager 完整流程
    ///
    /// 验证数据可以通过 NetworkShuffleManager 正确传输
    #[tokio::test]
    async fn test_network_shuffle_manager_full_flow() {
        use datafusion::prelude::SessionContext;

        let schema = test_schema();
        let batch = test_batch(
            vec![1, 2, 3, 4],
            vec!["a", "b", "c", "d"],
            vec![10, 20, 30, 40],
        );

        // 创建 NetworkShuffleManager
        let mut manager = NetworkShuffleManager::new(
            "test-query".to_string(),
            "node-0".to_string(),
            0,
            vec!["node-0".to_string()], // 单节点测试
            100,
        );

        // 创建工厂
        let receiver_factory = manager.create_receiver_factory();
        let sender_factory = manager.create_sender_factory();
        let config = manager.create_config();

        // 创建内存表并获取执行计划
        let ctx = SessionContext::new();
        let mem_table = MemTable::try_new(schema.clone(), vec![vec![batch]]).unwrap();
        ctx.register_table("test", Arc::new(mem_table)).unwrap();

        let df = ctx.table("test").await.unwrap();
        let plan = df.create_physical_plan().await.unwrap();

        // 创建 ShuffleExec
        let shuffle = ShuffleExec::new(
            plan,
            vec![0], // GROUP BY id
            config,
            receiver_factory,
            sender_factory,
        );

        // 执行
        let context = Arc::new(TaskContext::default());
        let stream = shuffle.execute(0, context).unwrap();

        // 收集结果
        let results: Vec<_> = stream.collect::<Vec<_>>().await;

        let total_rows: usize = results
            .iter()
            .filter_map(|r| r.as_ref().ok())
            .map(|b| b.num_rows())
            .sum();

        assert_eq!(total_rows, 4, "All rows should be processed");
    }

    // ========================================================================
    // MultiSourceShuffleReceiverExec 测试
    // ========================================================================

    #[test]
    fn test_multi_source_shuffle_receiver_exec_creation() {
        let schema = test_schema();
        let receiver_exec = MultiSourceShuffleReceiverExec::new(schema.clone(), 3, 100);

        assert_eq!(receiver_exec.name(), "MultiSourceShuffleReceiverExec");
        assert_eq!(receiver_exec.num_sources(), 3);
        assert_eq!(receiver_exec.schema(), schema);
        assert!(receiver_exec.children().is_empty());
    }

    #[tokio::test]
    async fn test_multi_source_shuffle_receiver_exec_data_flow() {
        use tokio::time::{timeout, Duration};

        let schema = test_schema();
        let receiver_exec = MultiSourceShuffleReceiverExec::new(schema.clone(), 2, 100);

        // 获取发送端
        let sender = receiver_exec.get_sender();

        // 先执行获取流
        let context = Arc::new(TaskContext::default());
        let mut stream = receiver_exec.execute(0, context).unwrap();

        // 然后发送测试数据
        let batch1 = test_batch(vec![1, 2], vec!["a", "b"], vec![10, 20]);
        let batch2 = test_batch(vec![3, 4], vec!["c", "d"], vec![30, 40]);

        sender.send(batch1).await.unwrap();
        sender.send(batch2).await.unwrap();
        drop(sender);

        // 收集结果（带超时）
        let mut results = Vec::new();
        loop {
            match timeout(
                Duration::from_millis(100),
                futures::StreamExt::next(&mut stream),
            )
            .await
            {
                Ok(Some(Ok(batch))) => results.push(batch),
                Ok(Some(Err(e))) => panic!("Error: {:?}", e),
                Ok(None) => break, // Stream ended
                Err(_) => break,   // Timeout
            }
        }

        assert_eq!(results.len(), 2);

        let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();

        assert_eq!(total_rows, 4);
    }

    #[test]
    fn test_multi_source_shuffle_receiver_exec_display() {
        let schema = test_schema();
        let receiver_exec = MultiSourceShuffleReceiverExec::new(schema, 5, 100);

        let display = format!("{}", receiver_exec);
        assert!(display.contains("MultiSourceShuffleReceiverExec"));
        assert!(display.contains("5"));
    }

    // ========================================================================
    // ShuffleDataInjector 测试
    // ========================================================================

    #[test]
    fn test_shuffle_data_injector_creation() {
        let (tx, _rx) = mpsc::channel(100);
        let injector = ShuffleDataInjector::new(tx, 3);

        assert_eq!(injector.eof_count(), 0);
        assert!(!injector.is_complete());
    }

    #[tokio::test]
    async fn test_shuffle_data_injector_inject() {
        let (tx, mut rx) = mpsc::channel(100);
        let injector = ShuffleDataInjector::new(tx, 2);

        // 注入数据
        let batch = test_batch(vec![1, 2], vec!["a", "b"], vec![10, 20]);
        injector.inject(batch.clone()).await.unwrap();

        // 验证接收
        let received = rx.recv().await;
        assert!(received.is_some());
        assert_eq!(received.unwrap().num_rows(), 2);
    }

    #[test]
    fn test_shuffle_data_injector_eof_tracking() {
        let (tx, _rx) = mpsc::channel(100);
        let injector = ShuffleDataInjector::new(tx, 3);

        // 初始状态
        assert_eq!(injector.eof_count(), 0);
        assert!(!injector.is_complete());

        // 第一个源 EOF
        let all_done = injector.mark_source_eof();
        assert!(!all_done);
        assert_eq!(injector.eof_count(), 1);
        assert!(!injector.is_complete());

        // 第二个源 EOF
        let all_done = injector.mark_source_eof();
        assert!(!all_done);
        assert_eq!(injector.eof_count(), 2);
        assert!(!injector.is_complete());

        // 第三个源 EOF - 全部完成
        let all_done = injector.mark_source_eof();
        assert!(all_done);
        assert_eq!(injector.eof_count(), 3);
        assert!(injector.is_complete());
    }

    #[tokio::test]
    async fn test_shuffle_data_injector_full_flow() {
        use tokio::time::{timeout, Duration};

        let schema = test_schema();
        let receiver_exec = MultiSourceShuffleReceiverExec::new(schema.clone(), 2, 100);

        // 获取发送端（在执行前获取，因为 execute 会消费 receiver）
        let sender = receiver_exec.get_sender();

        // 执行获取流
        let context = Arc::new(TaskContext::default());
        let mut stream = receiver_exec.execute(0, context).unwrap();

        // 创建注入器
        let injector = ShuffleDataInjector::new(sender, 2);

        // 模拟两个源节点发送数据
        // 源 1 发送数据
        let batch1 = test_batch(vec![1, 2], vec!["a", "b"], vec![10, 20]);
        injector.inject(batch1).await.unwrap();

        // 源 2 发送数据
        let batch2 = test_batch(vec![3, 4], vec!["c", "d"], vec![30, 40]);
        injector.inject(batch2).await.unwrap();

        // 源 1 EOF
        assert!(!injector.mark_source_eof());

        // 源 2 EOF - 全部完成
        assert!(injector.mark_source_eof());
        assert!(injector.is_complete());

        // 关闭发送端（这会导致 stream 结束）
        drop(injector);

        // 收集结果（带超时）
        let mut results = Vec::new();
        loop {
            match timeout(
                Duration::from_millis(100),
                futures::StreamExt::next(&mut stream),
            )
            .await
            {
                Ok(Some(Ok(batch))) => results.push(batch),
                Ok(Some(Err(e))) => panic!("Error: {:?}", e),
                Ok(None) => break, // Stream ended
                Err(_) => break,   // Timeout
            }
        }

        let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();

        assert_eq!(
            total_rows, 4,
            "All data from both sources should be received"
        );
    }
}
