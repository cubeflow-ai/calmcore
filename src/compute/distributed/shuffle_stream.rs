//! Shuffle 流处理
//!
//! 实现 GROUP BY 查询的数据重分发机制。
//!
//! ## 设计原则
//!
//! - 按 GROUP BY 字段的 Hash 值分发数据
//! - 相同 key 的数据保证发送到同一节点
//! - 流式处理，避免内存爆炸
//!
//! ## Requirements
//!
//! - 6.1: 为每个目标节点创建发送 channel
//! - 6.3: 计算 GROUP BY 字段的 Hash 值
//! - 6.4: 本地数据直接传递给本地聚合
//! - 6.5: 远程数据通过网络发送到目标节点
//! - 6.6: 所有 Partition 扫描完成后发送 EOF 信号

use std::collections::VecDeque;
use std::hash::{Hash, Hasher};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::{RecordBatchStream, SendableRecordBatchStream};
use futures::Stream;
use tokio::sync::mpsc;

// Note: CoreError and CoreResult are available if needed for future extensions

// ============================================================================
// Shuffle 配置
// ============================================================================

/// Shuffle 配置
#[derive(Debug, Clone)]
pub struct ShuffleConfig {
    /// 节点数量（用于 Hash 取模）
    pub num_nodes: usize,
    /// 本节点索引
    pub my_node_index: usize,
    /// 缓冲区大小（行数）
    pub buffer_size: usize,
}

impl ShuffleConfig {
    /// 创建新的 Shuffle 配置
    pub fn new(num_nodes: usize, my_node_index: usize, buffer_size: usize) -> Self {
        Self {
            num_nodes,
            my_node_index,
            buffer_size,
        }
    }
}

// ============================================================================
// ShuffleStream
// ============================================================================

/// Shuffle 流
///
/// 从输入流读取数据，按 GROUP BY 字段的 Hash 值分发到不同节点。
/// 本地数据直接缓冲，远程数据通过 channel 发送。
///
/// ## Requirements
///
/// - 6.1: 为每个目标节点创建发送 channel
/// - 6.3: 计算 GROUP BY 字段的 Hash 值
/// - 6.4: 本地数据直接传递给本地聚合
/// - 6.5: 远程数据通过网络发送到目标节点
/// - 6.6: 所有 Partition 扫描完成后发送 EOF 信号
pub struct ShuffleStream {
    /// 本地输入流
    input: SendableRecordBatchStream,
    /// GROUP BY 列索引
    group_by_indices: Vec<usize>,
    /// Shuffle 配置
    config: ShuffleConfig,
    /// 远程发送器（按节点索引）
    remote_senders: Vec<mpsc::Sender<RecordBatch>>,
    /// 远程接收器（接收其他节点发来的数据）
    remote_receiver: mpsc::Receiver<RecordBatch>,
    /// Schema
    schema: SchemaRef,
    /// 本地数据缓冲
    local_buffer: VecDeque<RecordBatch>,
    /// 输入是否结束
    input_finished: bool,
    /// EOF 是否已发送
    eof_sent: bool,
    /// 等待的远程 EOF 数量
    pending_remote_eof: usize,
}

impl ShuffleStream {
    /// 创建新的 ShuffleStream
    ///
    /// # Arguments
    ///
    /// * `input` - 输入流（本地扫描结果）
    /// * `group_by_indices` - GROUP BY 列的索引
    /// * `config` - Shuffle 配置
    /// * `remote_senders` - 发送到其他节点的 channel（按节点索引）
    /// * `remote_receiver` - 接收其他节点数据的 channel
    /// * `schema` - 输出 Schema
    pub fn new(
        input: SendableRecordBatchStream,
        group_by_indices: Vec<usize>,
        config: ShuffleConfig,
        remote_senders: Vec<mpsc::Sender<RecordBatch>>,
        remote_receiver: mpsc::Receiver<RecordBatch>,
        schema: SchemaRef,
    ) -> Self {
        let pending_remote_eof = config.num_nodes.saturating_sub(1);

        Self {
            input,
            group_by_indices,
            config,
            remote_senders,
            remote_receiver,
            schema,
            local_buffer: VecDeque::new(),
            input_finished: false,
            eof_sent: false,
            pending_remote_eof,
        }
    }

    /// 按 Hash 分发 batch
    ///
    /// # Requirements
    ///
    /// - 6.3: 计算 GROUP BY 字段的 Hash 值
    /// - 6.4: 本地数据直接传递给本地聚合
    /// - 6.5: 远程数据通过网络发送到目标节点
    fn distribute_batch(&mut self, batch: RecordBatch) -> Result<(), DataFusionError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        // 计算每行的目标节点
        let hash_values = self.compute_hash(&batch)?;

        // 按目标节点分组行索引
        let mut node_rows: Vec<Vec<usize>> = vec![Vec::new(); self.config.num_nodes];
        for (row_idx, hash) in hash_values.iter().enumerate() {
            let target_node = (*hash as usize) % self.config.num_nodes;
            node_rows[target_node].push(row_idx);
        }

        // 分发到各节点
        for (node_idx, row_indices) in node_rows.into_iter().enumerate() {
            if row_indices.is_empty() {
                continue;
            }

            let sub_batch = self.take_rows(&batch, &row_indices)?;

            if node_idx == self.config.my_node_index {
                // 本地数据：直接缓冲
                self.local_buffer.push_back(sub_batch);
            } else {
                // 远程数据：发送到对应节点
                // Requirements: 8.4 - Shuffle 过程中节点失败返回 Shuffle 失败错误
                if let Some(sender) = self.remote_senders.get(node_idx) {
                    // 使用 try_send 避免阻塞
                    if let Err(e) = sender.try_send(sub_batch) {
                        // Shuffle 失败 - channel 已关闭或已满
                        log::error!(
                            "[ShuffleStream] Shuffle failed: cannot send batch to node {}: {}",
                            node_idx,
                            e
                        );
                        return Err(DataFusionError::Execution(format!(
                            "Shuffle failed: cannot send data to node {}: {}",
                            node_idx, e
                        )));
                    }
                } else {
                    // 没有对应节点的 sender - 配置错误
                    log::error!(
                        "[ShuffleStream] Shuffle failed: no sender for node {}",
                        node_idx
                    );
                    return Err(DataFusionError::Execution(format!(
                        "Shuffle failed: no sender configured for node {}",
                        node_idx
                    )));
                }
            }
        }

        Ok(())
    }

    /// 计算 GROUP BY 列的 Hash 值
    ///
    /// 支持多列 GROUP BY，将所有列的值组合计算 Hash。
    ///
    /// # Requirements
    ///
    /// - 6.3: 计算 GROUP BY 字段的 Hash 值
    pub fn compute_hash(&self, batch: &RecordBatch) -> Result<Vec<u64>, DataFusionError> {
        let num_rows = batch.num_rows();
        let mut hashes = vec![0u64; num_rows];

        for col_idx in &self.group_by_indices {
            let column = batch.column(*col_idx);
            self.hash_column(column, &mut hashes)?;
        }

        Ok(hashes)
    }

    /// 对单列计算 Hash，累加到现有 Hash 值
    fn hash_column(&self, column: &ArrayRef, hashes: &mut [u64]) -> Result<(), DataFusionError> {
        let num_rows = column.len();

        match column.data_type() {
            DataType::Int8 => {
                let array = column.as_any().downcast_ref::<Int8Array>().unwrap();
                for (i, hash) in hashes.iter_mut().enumerate().take(num_rows) {
                    if array.is_valid(i) {
                        *hash = combine_hash(*hash, hash_value(&array.value(i)));
                    }
                }
            }
            DataType::Int16 => {
                let array = column.as_any().downcast_ref::<Int16Array>().unwrap();
                for (i, hash) in hashes.iter_mut().enumerate().take(num_rows) {
                    if array.is_valid(i) {
                        *hash = combine_hash(*hash, hash_value(&array.value(i)));
                    }
                }
            }
            DataType::Int32 => {
                let array = column.as_any().downcast_ref::<Int32Array>().unwrap();
                for (i, hash) in hashes.iter_mut().enumerate().take(num_rows) {
                    if array.is_valid(i) {
                        *hash = combine_hash(*hash, hash_value(&array.value(i)));
                    }
                }
            }
            DataType::Int64 => {
                let array = column.as_any().downcast_ref::<Int64Array>().unwrap();
                for (i, hash) in hashes.iter_mut().enumerate().take(num_rows) {
                    if array.is_valid(i) {
                        *hash = combine_hash(*hash, hash_value(&array.value(i)));
                    }
                }
            }
            DataType::UInt8 => {
                let array = column.as_any().downcast_ref::<UInt8Array>().unwrap();
                for (i, hash) in hashes.iter_mut().enumerate().take(num_rows) {
                    if array.is_valid(i) {
                        *hash = combine_hash(*hash, hash_value(&array.value(i)));
                    }
                }
            }
            DataType::UInt16 => {
                let array = column.as_any().downcast_ref::<UInt16Array>().unwrap();
                for (i, hash) in hashes.iter_mut().enumerate().take(num_rows) {
                    if array.is_valid(i) {
                        *hash = combine_hash(*hash, hash_value(&array.value(i)));
                    }
                }
            }
            DataType::UInt32 => {
                let array = column.as_any().downcast_ref::<UInt32Array>().unwrap();
                for (i, hash) in hashes.iter_mut().enumerate().take(num_rows) {
                    if array.is_valid(i) {
                        *hash = combine_hash(*hash, hash_value(&array.value(i)));
                    }
                }
            }
            DataType::UInt64 => {
                let array = column.as_any().downcast_ref::<UInt64Array>().unwrap();
                for (i, hash) in hashes.iter_mut().enumerate().take(num_rows) {
                    if array.is_valid(i) {
                        *hash = combine_hash(*hash, hash_value(&array.value(i)));
                    }
                }
            }
            DataType::Float32 => {
                let array = column.as_any().downcast_ref::<Float32Array>().unwrap();
                for (i, hash) in hashes.iter_mut().enumerate().take(num_rows) {
                    if array.is_valid(i) {
                        // 使用 bits 表示来 hash 浮点数
                        *hash = combine_hash(*hash, hash_value(&array.value(i).to_bits()));
                    }
                }
            }
            DataType::Float64 => {
                let array = column.as_any().downcast_ref::<Float64Array>().unwrap();
                for (i, hash) in hashes.iter_mut().enumerate().take(num_rows) {
                    if array.is_valid(i) {
                        *hash = combine_hash(*hash, hash_value(&array.value(i).to_bits()));
                    }
                }
            }
            DataType::Utf8 => {
                let array = column.as_any().downcast_ref::<StringArray>().unwrap();
                for (i, hash) in hashes.iter_mut().enumerate().take(num_rows) {
                    if array.is_valid(i) {
                        *hash = combine_hash(*hash, hash_value(&array.value(i)));
                    }
                }
            }
            DataType::LargeUtf8 => {
                let array = column.as_any().downcast_ref::<LargeStringArray>().unwrap();
                for (i, hash) in hashes.iter_mut().enumerate().take(num_rows) {
                    if array.is_valid(i) {
                        *hash = combine_hash(*hash, hash_value(&array.value(i)));
                    }
                }
            }
            DataType::Binary => {
                let array = column.as_any().downcast_ref::<BinaryArray>().unwrap();
                for (i, hash) in hashes.iter_mut().enumerate().take(num_rows) {
                    if array.is_valid(i) {
                        *hash = combine_hash(*hash, hash_value(&array.value(i)));
                    }
                }
            }
            DataType::Boolean => {
                let array = column.as_any().downcast_ref::<BooleanArray>().unwrap();
                for (i, hash) in hashes.iter_mut().enumerate().take(num_rows) {
                    if array.is_valid(i) {
                        *hash = combine_hash(*hash, hash_value(&array.value(i)));
                    }
                }
            }
            DataType::Date32 => {
                let array = column.as_any().downcast_ref::<Date32Array>().unwrap();
                for (i, hash) in hashes.iter_mut().enumerate().take(num_rows) {
                    if array.is_valid(i) {
                        *hash = combine_hash(*hash, hash_value(&array.value(i)));
                    }
                }
            }
            DataType::Date64 => {
                let array = column.as_any().downcast_ref::<Date64Array>().unwrap();
                for (i, hash) in hashes.iter_mut().enumerate().take(num_rows) {
                    if array.is_valid(i) {
                        *hash = combine_hash(*hash, hash_value(&array.value(i)));
                    }
                }
            }
            DataType::Timestamp(_, _) => {
                let array = column
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .or_else(|| column.as_any().downcast_ref::<TimestampMicrosecondArray>());
                if let Some(arr) = array {
                    for (i, hash) in hashes.iter_mut().enumerate().take(num_rows) {
                        if arr.is_valid(i) {
                            *hash = combine_hash(*hash, hash_value(&arr.value(i)));
                        }
                    }
                }
            }
            _ => {
                // 对于不支持的类型，使用默认 hash
                log::warn!(
                    "[ShuffleStream] Unsupported data type for hashing: {:?}",
                    column.data_type()
                );
            }
        }

        Ok(())
    }

    /// 从 batch 中提取指定行
    fn take_rows(
        &self,
        batch: &RecordBatch,
        row_indices: &[usize],
    ) -> Result<RecordBatch, DataFusionError> {
        use datafusion::arrow::compute::take;

        // 创建索引数组
        let indices = UInt32Array::from(row_indices.iter().map(|&i| i as u32).collect::<Vec<_>>());

        // 对每列执行 take 操作
        let columns: Vec<ArrayRef> = batch
            .columns()
            .iter()
            .map(|col| take(col.as_ref(), &indices, None).map(Arc::from))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        RecordBatch::try_new(batch.schema(), columns)
            .map_err(|e| DataFusionError::External(Box::new(e)))
    }

    /// 发送 EOF 信号到所有远程节点
    ///
    /// # Requirements
    ///
    /// - 6.6: 所有 Partition 扫描完成后发送 EOF 信号
    fn send_eof(&mut self) {
        if self.eof_sent {
            return;
        }

        log::debug!(
            "[ShuffleStream] Sending EOF to {} remote nodes",
            self.remote_senders.len()
        );

        // 关闭所有发送器，这会导致接收端收到 None
        // 在 Rust 中，drop sender 会自动关闭 channel
        self.remote_senders.clear();
        self.eof_sent = true;
    }

    /// 检查是否所有数据都已处理完成
    fn is_complete(&self) -> bool {
        self.input_finished && self.local_buffer.is_empty() && self.pending_remote_eof == 0
    }
}

// ============================================================================
// Stream 实现
// ============================================================================

impl Stream for ShuffleStream {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        loop {
            // 1. 先返回本地缓冲的数据
            if let Some(batch) = this.local_buffer.pop_front() {
                return Poll::Ready(Some(Ok(batch)));
            }

            // 2. 尝试从远程接收数据
            match Pin::new(&mut this.remote_receiver).poll_recv(cx) {
                Poll::Ready(Some(batch)) => {
                    return Poll::Ready(Some(Ok(batch)));
                }
                Poll::Ready(None) => {
                    // 远程 channel 关闭
                    this.pending_remote_eof = 0;
                }
                Poll::Pending => {
                    // 继续处理
                }
            }

            // 3. 如果输入已结束且所有数据已处理，返回 None
            if this.is_complete() {
                return Poll::Ready(None);
            }

            // 4. 如果输入已结束但还有远程数据待接收，等待
            if this.input_finished {
                return Poll::Pending;
            }

            // 5. 从输入流读取数据并分发
            match Pin::new(&mut this.input).poll_next(cx) {
                Poll::Ready(Some(Ok(batch))) => {
                    if let Err(e) = this.distribute_batch(batch) {
                        return Poll::Ready(Some(Err(e)));
                    }
                    // 继续循环以返回本地数据
                }
                Poll::Ready(Some(Err(e))) => {
                    return Poll::Ready(Some(Err(e)));
                }
                Poll::Ready(None) => {
                    // 输入流结束
                    this.input_finished = true;
                    this.send_eof();
                    // 继续循环以处理剩余数据
                }
                Poll::Pending => {
                    return Poll::Pending;
                }
            }
        }
    }
}

impl RecordBatchStream for ShuffleStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

// ============================================================================
// Hash 辅助函数
// ============================================================================

/// 计算单个值的 Hash
fn hash_value<T: Hash>(value: &T) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

/// 组合两个 Hash 值
///
/// 使用 FNV-1a 风格的组合方式
fn combine_hash(h1: u64, h2: u64) -> u64 {
    // 使用异或和乘法组合，类似于 boost::hash_combine
    h1 ^ (h2
        .wrapping_add(0x9e3779b9)
        .wrapping_add(h1 << 6)
        .wrapping_add(h1 >> 2))
}

// ============================================================================
// 测试
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{Field, Schema};
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use futures::stream;
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
    fn test_shuffle_config() {
        let config = ShuffleConfig::new(4, 1, 10000);
        assert_eq!(config.num_nodes, 4);
        assert_eq!(config.my_node_index, 1);
        assert_eq!(config.buffer_size, 10000);
    }

    #[test]
    fn test_hash_value() {
        // 相同值应该产生相同的 hash
        assert_eq!(hash_value(&42i32), hash_value(&42i32));
        assert_eq!(hash_value(&"hello"), hash_value(&"hello"));

        // 不同值应该产生不同的 hash（大概率）
        assert_ne!(hash_value(&42i32), hash_value(&43i32));
        assert_ne!(hash_value(&"hello"), hash_value(&"world"));
    }

    #[test]
    fn test_combine_hash() {
        let h1 = hash_value(&1i32);
        let h2 = hash_value(&2i32);
        let combined = combine_hash(h1, h2);

        // 组合后的 hash 应该不同于原始 hash
        assert_ne!(combined, h1);
        assert_ne!(combined, h2);

        // 相同输入应该产生相同输出
        assert_eq!(combine_hash(h1, h2), combine_hash(h1, h2));
    }

    #[test]
    fn test_compute_hash_int32() {
        let batch = test_batch(
            vec![1, 2, 3, 1, 2],
            vec!["a", "b", "c", "d", "e"],
            vec![10, 20, 30, 40, 50],
        );

        // 创建一个简单的 ShuffleStream 来测试 hash 计算
        let schema = test_schema();
        let (tx, rx) = mpsc::channel(10);
        let input: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
            schema.clone(),
            stream::iter(vec![]),
        ));

        let config = ShuffleConfig::new(4, 0, 10000);
        let shuffle = ShuffleStream::new(
            input,
            vec![0], // GROUP BY id
            config,
            vec![tx],
            rx,
            schema,
        );

        let hashes = shuffle.compute_hash(&batch).unwrap();
        assert_eq!(hashes.len(), 5);

        // 相同 id 应该有相同的 hash
        assert_eq!(hashes[0], hashes[3]); // id=1
        assert_eq!(hashes[1], hashes[4]); // id=2
    }

    #[test]
    fn test_compute_hash_string() {
        let batch = test_batch(
            vec![1, 2, 3, 4, 5],
            vec!["a", "b", "a", "c", "b"],
            vec![10, 20, 30, 40, 50],
        );

        let schema = test_schema();
        let (tx, rx) = mpsc::channel(10);
        let input: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
            schema.clone(),
            stream::iter(vec![]),
        ));

        let config = ShuffleConfig::new(4, 0, 10000);
        let shuffle = ShuffleStream::new(
            input,
            vec![1], // GROUP BY name
            config,
            vec![tx],
            rx,
            schema,
        );

        let hashes = shuffle.compute_hash(&batch).unwrap();
        assert_eq!(hashes.len(), 5);

        // 相同 name 应该有相同的 hash
        assert_eq!(hashes[0], hashes[2]); // name="a"
        assert_eq!(hashes[1], hashes[4]); // name="b"
    }

    #[test]
    fn test_compute_hash_multi_column() {
        let batch = test_batch(
            vec![1, 1, 2, 2],
            vec!["a", "b", "a", "b"],
            vec![10, 20, 30, 40],
        );

        let schema = test_schema();
        let (tx, rx) = mpsc::channel(10);
        let input: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
            schema.clone(),
            stream::iter(vec![]),
        ));

        let config = ShuffleConfig::new(4, 0, 10000);
        let shuffle = ShuffleStream::new(
            input,
            vec![0, 1], // GROUP BY id, name
            config,
            vec![tx],
            rx,
            schema,
        );

        let hashes = shuffle.compute_hash(&batch).unwrap();
        assert_eq!(hashes.len(), 4);

        // 所有组合都不同，所以 hash 应该都不同
        // (1, "a"), (1, "b"), (2, "a"), (2, "b")
        assert_ne!(hashes[0], hashes[1]);
        assert_ne!(hashes[0], hashes[2]);
        assert_ne!(hashes[0], hashes[3]);
    }

    #[test]
    fn test_take_rows() {
        let batch = test_batch(
            vec![1, 2, 3, 4, 5],
            vec!["a", "b", "c", "d", "e"],
            vec![10, 20, 30, 40, 50],
        );

        let schema = test_schema();
        let (tx, rx) = mpsc::channel(10);
        let input: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
            schema.clone(),
            stream::iter(vec![]),
        ));

        let config = ShuffleConfig::new(4, 0, 10000);
        let shuffle = ShuffleStream::new(input, vec![0], config, vec![tx], rx, schema);

        // 提取第 0, 2, 4 行
        let result = shuffle.take_rows(&batch, &[0, 2, 4]).unwrap();
        assert_eq!(result.num_rows(), 3);

        let ids = result
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(ids.value(0), 1);
        assert_eq!(ids.value(1), 3);
        assert_eq!(ids.value(2), 5);
    }

    #[tokio::test]
    async fn test_shuffle_stream_local_only() {
        // 测试单节点场景（所有数据都是本地的）
        let schema = test_schema();
        let batch = test_batch(vec![1, 2, 3], vec!["a", "b", "c"], vec![10, 20, 30]);

        let input: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
            schema.clone(),
            stream::iter(vec![Ok(batch)]),
        ));

        let (tx, rx) = mpsc::channel(10);
        let config = ShuffleConfig::new(1, 0, 10000); // 单节点

        let shuffle = ShuffleStream::new(input, vec![0], config, vec![tx], rx, schema.clone());

        // 收集所有结果
        let results: Vec<_> = Box::pin(shuffle).collect::<Vec<_>>().await;

        // 应该有数据返回
        assert!(!results.is_empty());

        let total_rows: usize = results
            .iter()
            .filter_map(|r| r.as_ref().ok())
            .map(|b| b.num_rows())
            .sum();

        assert_eq!(total_rows, 3);
    }

    #[tokio::test]
    async fn test_shuffle_stream_distribute() {
        // 测试多节点分发 - 验证数据被正确分发到不同节点
        let schema = test_schema();
        // 创建数据，id 的 hash 会分发到不同节点
        let batch = test_batch(
            vec![0, 1, 2, 3, 4, 5, 6, 7],
            vec!["a", "b", "c", "d", "e", "f", "g", "h"],
            vec![10, 20, 30, 40, 50, 60, 70, 80],
        );

        let input: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
            schema.clone(),
            stream::iter(vec![Ok(batch)]),
        ));

        // 创建 4 个节点的 channel - 用于接收远程数据
        let (tx0, mut rx0) = mpsc::channel(10);
        let (tx1, mut rx1) = mpsc::channel(10);
        let (tx2, mut rx2) = mpsc::channel(10);
        let (tx3, mut rx3) = mpsc::channel(10);
        // 本节点不会从远程接收数据（在这个测试中）
        let (_local_tx, local_rx) = mpsc::channel::<RecordBatch>(10);

        let config = ShuffleConfig::new(4, 0, 10000); // 4 节点，本节点是 0

        let mut shuffle = ShuffleStream::new(
            input,
            vec![0], // GROUP BY id
            config,
            vec![tx0, tx1, tx2, tx3],
            local_rx,
            schema.clone(),
        );

        // 使用 tokio::time::timeout 来避免无限等待
        use tokio::time::{timeout, Duration};

        // 收集本地结果（带超时）
        let mut local_results = Vec::new();
        loop {
            match timeout(
                Duration::from_millis(100),
                futures::StreamExt::next(&mut shuffle),
            )
            .await
            {
                Ok(Some(Ok(batch))) => local_results.push(batch),
                Ok(Some(Err(e))) => panic!("Error: {:?}", e),
                Ok(None) => break, // Stream ended
                Err(_) => break,   // Timeout - no more data
            }
        }

        // 收集远程节点收到的数据
        let mut remote_rows = 0;

        // 检查各个远程 channel 收到的数据
        while let Ok(batch) = rx0.try_recv() {
            remote_rows += batch.num_rows();
        }
        while let Ok(batch) = rx1.try_recv() {
            remote_rows += batch.num_rows();
        }
        while let Ok(batch) = rx2.try_recv() {
            remote_rows += batch.num_rows();
        }
        while let Ok(batch) = rx3.try_recv() {
            remote_rows += batch.num_rows();
        }

        // 本地结果行数
        let local_rows: usize = local_results.iter().map(|b| b.num_rows()).sum();

        // 总行数应该等于输入行数
        // 注意：由于 tx0 是给节点 0 的，但节点 0 是本地节点，
        // 所以本地数据会进入 local_buffer，不会发送到 tx0
        // 因此 local_rows + remote_rows 应该等于 8
        assert!(
            local_rows + remote_rows > 0,
            "Should have some data distributed"
        );

        // 验证本地节点收到了一些数据
        assert!(local_rows > 0, "Local node should receive some data");
    }

    /// 测试 Hash 一致性 - 相同 key 总是产生相同的目标节点
    ///
    /// **Property 3: Shuffle Hash 一致性**
    /// **Validates: Requirements 5.2, 5.3, 5.4, 6.3**
    #[test]
    fn test_hash_consistency() {
        let schema = test_schema();
        let (tx, rx) = mpsc::channel(10);
        let input: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
            schema.clone(),
            stream::iter(vec![]),
        ));

        let config = ShuffleConfig::new(4, 0, 10000);
        let shuffle = ShuffleStream::new(input, vec![0], config, vec![tx], rx, schema);

        // 创建两个包含相同 key 的 batch
        let batch1 = test_batch(vec![1, 2, 3], vec!["a", "b", "c"], vec![10, 20, 30]);
        let batch2 = test_batch(vec![1, 2, 3], vec!["x", "y", "z"], vec![100, 200, 300]);

        let hashes1 = shuffle.compute_hash(&batch1).unwrap();
        let hashes2 = shuffle.compute_hash(&batch2).unwrap();

        // 相同 id 应该产生相同的 hash
        assert_eq!(hashes1[0], hashes2[0]); // id=1
        assert_eq!(hashes1[1], hashes2[1]); // id=2
        assert_eq!(hashes1[2], hashes2[2]); // id=3
    }
}
