//! Shuffle 网络传输层
//!
//! 负责将 Shuffle 数据通过网络发送到远程节点。
//!
//! ## Requirements
//!
//! - 6.5: 远程数据通过网络发送到目标节点
//! - 6.6: 所有 Partition 扫描完成后发送 EOF 信号

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use datafusion::arrow::record_batch::RecordBatch;
use tokio::sync::mpsc;

use super::node_client::{serialize_batch, NodeClient, ShuffleChunk};
use crate::utils::error::{CoreError, CoreResult};

/// Shuffle 发送器
///
/// 从本地 channel 读取数据并通过网络发送到远程节点。
///
/// # Requirements
/// - 6.5: 远程数据通过网络发送到目标节点
#[derive(Debug)]
pub struct ShuffleSender {
    /// 查询 ID
    query_id: String,
    /// 源节点 ID
    source_node: String,
    /// 目标节点 ID
    target_node: String,
    /// 节点客户端
    client: Arc<NodeClient>,
    /// 发送的块计数
    chunk_counter: AtomicU64,
    /// 是否已停止
    stopped: AtomicBool,
}

impl ShuffleSender {
    /// 创建新的 Shuffle 发送器
    pub fn new(
        query_id: String,
        source_node: String,
        target_node: String,
        client: Arc<NodeClient>,
    ) -> Self {
        Self {
            query_id,
            source_node,
            target_node,
            client,
            chunk_counter: AtomicU64::new(0),
            stopped: AtomicBool::new(false),
        }
    }

    /// 发送单个 RecordBatch
    ///
    /// # Requirements
    /// - 6.5: 远程数据通过网络发送到目标节点
    pub async fn send_batch(&self, batch: RecordBatch) -> CoreResult<()> {
        if self.stopped.load(Ordering::SeqCst) {
            return Err(CoreError::Internal("Shuffle sender is stopped".to_string()));
        }

        // 序列化 batch
        let data = serialize_batch(&batch)?;

        // 创建 ShuffleChunk
        let chunk_index = self.chunk_counter.fetch_add(1, Ordering::SeqCst);
        let chunk = ShuffleChunk {
            query_id: self.query_id.clone(),
            source_node: self.source_node.clone(),
            target_node: self.target_node.clone(),
            chunk_index,
            data,
            is_eof: false,
        };

        // 发送到远程节点
        self.client.send_shuffle_chunk(&chunk).await?;

        log::trace!(
            "[ShuffleSender] Sent chunk {} to node {} ({} rows)",
            chunk_index,
            self.target_node,
            batch.num_rows()
        );

        Ok(())
    }

    /// 发送 EOF 信号
    ///
    /// # Requirements
    /// - 6.6: 所有 Partition 扫描完成后发送 EOF 信号
    pub async fn send_eof(&self) -> CoreResult<()> {
        if self.stopped.load(Ordering::SeqCst) {
            return Ok(());
        }

        let chunk_index = self.chunk_counter.load(Ordering::SeqCst);
        let chunk = ShuffleChunk {
            query_id: self.query_id.clone(),
            source_node: self.source_node.clone(),
            target_node: self.target_node.clone(),
            chunk_index,
            data: Vec::new(),
            is_eof: true,
        };

        self.client.send_shuffle_chunk(&chunk).await?;

        log::debug!(
            "[ShuffleSender] Sent EOF to node {} (total chunks: {})",
            self.target_node,
            chunk_index
        );

        Ok(())
    }

    /// 停止发送器
    pub fn stop(&self) {
        self.stopped.store(true, Ordering::SeqCst);
    }

    /// 获取已发送的块数量
    pub fn chunks_sent(&self) -> u64 {
        self.chunk_counter.load(Ordering::SeqCst)
    }
}

/// Shuffle 传输任务
///
/// 后台任务，从 channel 读取数据并发送到远程节点。
///
/// # Requirements
/// - 6.5: 远程数据通过网络发送到目标节点
pub struct ShuffleTransportTask {
    /// Shuffle 发送器
    sender: Arc<ShuffleSender>,
    /// 数据接收 channel
    receiver: mpsc::Receiver<RecordBatch>,
}

impl ShuffleTransportTask {
    /// 创建新的传输任务
    pub fn new(sender: Arc<ShuffleSender>, receiver: mpsc::Receiver<RecordBatch>) -> Self {
        Self { sender, receiver }
    }

    /// 运行传输任务
    ///
    /// 持续从 channel 读取数据并发送到远程节点，直到 channel 关闭。
    pub async fn run(mut self) -> CoreResult<()> {
        log::debug!(
            "[ShuffleTransportTask] Starting transport to node {}",
            self.sender.target_node
        );

        let mut error_count = 0;
        const MAX_ERRORS: usize = 3;

        while let Some(batch) = self.receiver.recv().await {
            match self.sender.send_batch(batch).await {
                Ok(_) => {
                    error_count = 0; // Reset error count on success
                }
                Err(e) => {
                    error_count += 1;
                    log::error!(
                        "[ShuffleTransportTask] Failed to send batch to node {}: {} (error {}/{})",
                        self.sender.target_node,
                        e,
                        error_count,
                        MAX_ERRORS
                    );

                    if error_count >= MAX_ERRORS {
                        log::error!(
                            "[ShuffleTransportTask] Too many errors, stopping transport to node {}",
                            self.sender.target_node
                        );
                        self.sender.stop();
                        return Err(e);
                    }
                }
            }
        }

        // Channel 关闭，发送 EOF
        if let Err(e) = self.sender.send_eof().await {
            log::warn!(
                "[ShuffleTransportTask] Failed to send EOF to node {}: {}",
                self.sender.target_node,
                e
            );
        }

        log::debug!(
            "[ShuffleTransportTask] Transport to node {} completed ({} chunks sent)",
            self.sender.target_node,
            self.sender.chunks_sent()
        );

        Ok(())
    }
}

/// Shuffle 传输管理器
///
/// 管理多个节点的 Shuffle 传输任务。
///
/// # Requirements
/// - 6.5: 远程数据通过网络发送到目标节点
pub struct ShuffleTransportManager {
    /// 查询 ID
    query_id: String,
    /// 源节点 ID
    source_node: String,
    /// 传输任务句柄
    task_handles: Vec<tokio::task::JoinHandle<CoreResult<()>>>,
    /// 发送器列表（用于外部访问）
    senders: Vec<Arc<ShuffleSender>>,
}

impl ShuffleTransportManager {
    /// 创建新的传输管理器
    pub fn new(query_id: String, source_node: String) -> Self {
        Self {
            query_id,
            source_node,
            task_handles: Vec::new(),
            senders: Vec::new(),
        }
    }

    /// 添加传输目标
    ///
    /// 创建到指定节点的传输任务。
    ///
    /// # Arguments
    ///
    /// * `target_node` - 目标节点 ID
    /// * `client` - 节点客户端
    /// * `receiver` - 数据接收 channel
    pub fn add_target(
        &mut self,
        target_node: String,
        client: Arc<NodeClient>,
        receiver: mpsc::Receiver<RecordBatch>,
    ) {
        let sender = Arc::new(ShuffleSender::new(
            self.query_id.clone(),
            self.source_node.clone(),
            target_node.clone(),
            client,
        ));

        self.senders.push(sender.clone());

        let task = ShuffleTransportTask::new(sender, receiver);
        let handle = tokio::spawn(async move { task.run().await });

        self.task_handles.push(handle);

        log::debug!(
            "[ShuffleTransportManager] Added transport target: {}",
            target_node
        );
    }

    /// 等待所有传输任务完成
    pub async fn wait_all(&mut self) -> CoreResult<()> {
        let mut errors = Vec::new();

        for handle in self.task_handles.drain(..) {
            match handle.await {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => {
                    errors.push(e);
                }
                Err(e) => {
                    errors.push(CoreError::Internal(format!("Task join error: {}", e)));
                }
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(CoreError::Internal(format!(
                "Shuffle transport errors: {:?}",
                errors
            )))
        }
    }

    /// 停止所有传输任务
    pub fn stop_all(&self) {
        for sender in &self.senders {
            sender.stop();
        }
    }

    /// 获取总发送块数
    pub fn total_chunks_sent(&self) -> u64 {
        self.senders.iter().map(|s| s.chunks_sent()).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int32Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc as StdArc;

    fn test_schema() -> StdArc<Schema> {
        StdArc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]))
    }

    fn test_batch() -> RecordBatch {
        RecordBatch::try_new(
            test_schema(),
            vec![
                StdArc::new(Int32Array::from(vec![1, 2, 3])),
                StdArc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_shuffle_sender_creation() {
        use super::super::config::DistributedConfig;

        let config = DistributedConfig::default();
        let client = Arc::new(NodeClient::new(
            "node-1".to_string(),
            "127.0.0.1:7947".to_string(),
            config,
        ));

        let sender = ShuffleSender::new(
            "query-1".to_string(),
            "node-0".to_string(),
            "node-1".to_string(),
            client,
        );

        assert_eq!(sender.chunks_sent(), 0);
    }

    #[test]
    fn test_shuffle_transport_manager_creation() {
        let manager = ShuffleTransportManager::new("query-1".to_string(), "node-0".to_string());

        assert_eq!(manager.total_chunks_sent(), 0);
    }
}
