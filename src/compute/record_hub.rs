//! RecordHub: 分布式数据路由和背压控制中心
//!
//! 核心职责:
//! 1. 接收来自 segment 的数据流
//! 2. 根据路由策略分发数据到不同的物理分区
//! 3. 实现背压控制,防止内存溢出
//! 4. 为未来的分布式扩展做准备
//!
//! 架构演进:
//! - 阶段 1 (当前): LocalOnly 模式,所有数据到单一 partition
//! - 阶段 2: HashPartition 模式,本地多 partition 模拟分布式
//! - 阶段 3: Remote 模式,真正的分布式路由

use crate::utils::error::{CoreError, CoreResult};
use datafusion::arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};

/// 路由策略：定义如何将数据分发到不同的 partition
#[derive(Debug, Clone)]
pub enum RoutingStrategy {
    /// 本地单 partition 模式：所有数据发送到 partition 0
    /// 适用场景：单机部署,简单快速
    LocalOnly,

    /// Hash 分区模式：根据指定列的 hash 值分桶
    /// 适用场景：本地多 partition 或分布式部署
    ///
    /// 例如: GROUP BY passenger_count 时,相同 passenger_count 的数据
    /// 会路由到同一个 partition,从而可以独立做 partial aggregation
    #[allow(dead_code)]
    HashPartition {
        /// 用于计算 hash 的列名
        columns: Vec<String>,
        /// partition 数量
        num_partitions: usize,
    },

    /// Range 分区模式：根据范围分桶
    /// 适用场景：时间序列数据,按日期/时间范围路由
    #[allow(dead_code)]
    RangePartition {
        column: String,
        ranges: Vec<(i64, i64)>,
    },
}

impl RoutingStrategy {
    /// 获取 partition 数量
    pub fn num_partitions(&self) -> usize {
        match self {
            RoutingStrategy::LocalOnly => 1,
            RoutingStrategy::HashPartition { num_partitions, .. } => *num_partitions,
            RoutingStrategy::RangePartition { ranges, .. } => ranges.len(),
        }
    }
}

/// RecordHub: 数据路由和背压控制中心
#[derive(Debug)]
pub struct RecordHub {
    /// 路由策略
    strategy: RoutingStrategy,

    /// 每个 partition 的发送端
    /// partition_id -> Sender<RecordBatch>
    senders: Arc<RwLock<HashMap<usize, mpsc::Sender<RecordBatch>>>>,

    /// 背压控制：每个 partition 的 buffer 大小
    /// buffer_size = 1: 最严格的背压,确保不会内存溢出
    /// buffer_size > 1: 允许一定的缓冲,提高吞吐量
    buffer_size: usize,
}

impl RecordHub {
    /// 创建新的 RecordHub
    ///
    /// # 参数
    /// - `strategy`: 路由策略
    /// - `buffer_size`: 每个 partition 的 buffer 大小
    pub fn new(strategy: RoutingStrategy, buffer_size: usize) -> Self {
        Self {
            strategy,
            senders: Arc::new(RwLock::new(HashMap::new())),
            buffer_size,
        }
    }

    /// 获取路由策略
    pub fn strategy(&self) -> &RoutingStrategy {
        &self.strategy
    }

    /// 获取 buffer 大小
    pub fn buffer_size(&self) -> usize {
        self.buffer_size
    }

    /// 注册 partition 的接收端
    ///
    /// 当 ExecutionPlan 创建 Stream 时调用,为每个 partition 创建 channel
    pub async fn register_partition(&self, partition_id: usize) -> mpsc::Receiver<RecordBatch> {
        log::info!(
            "📍 [RecordHub::register_partition] Registering partition {} with buffer_size {}",
            partition_id,
            self.buffer_size
        );
        let (tx, rx) = mpsc::channel(self.buffer_size);
        self.senders.write().await.insert(partition_id, tx);
        log::info!(
            "✅ [RecordHub::register_partition] Partition {} registered",
            partition_id
        );
        rx
    }

    /// 路由并发送一个 RecordBatch
    ///
    /// 根据路由策略决定发送到哪个 partition
    /// 如果 channel 满了,会自动阻塞(背压)
    pub async fn route_batch(&self, batch: RecordBatch) -> CoreResult<()> {
        log::info!(
            "📍 [RecordHub::route_batch] Routing batch with {} rows",
            batch.num_rows()
        );
        match &self.strategy {
            RoutingStrategy::LocalOnly => {
                // 所有数据发送到 partition 0
                self.send_to_partition(0, batch).await
            }

            RoutingStrategy::HashPartition {
                columns,
                num_partitions,
            } => {
                // 🔧 未来实现：根据 hash 分桶
                // 目前先简单发送到 partition 0
                log::warn!("HashPartition not fully implemented, fallback to partition 0");
                let _ = (columns, num_partitions); // 避免 unused warning
                self.send_to_partition(0, batch).await
            }

            RoutingStrategy::RangePartition { .. } => {
                // 🔧 未来实现：根据 range 分桶
                log::warn!("RangePartition not implemented, fallback to partition 0");
                self.send_to_partition(0, batch).await
            }
        }
    }

    /// 发送 RecordBatch 到指定 partition
    ///
    /// 如果 channel 满了,会阻塞直到有空间(自动背压)
    /// 如果 partition 不存在,返回错误
    async fn send_to_partition(&self, partition_id: usize, batch: RecordBatch) -> CoreResult<()> {
        log::info!(
            "📍 [RecordHub::send_to_partition] Sending batch ({} rows) to partition {}",
            batch.num_rows(),
            partition_id
        );
        let senders = self.senders.read().await;

        if let Some(sender) = senders.get(&partition_id) {
            sender.send(batch).await.map_err(|_| {
                CoreError::Internal(format!(
                    "Failed to send batch to partition {}: channel closed",
                    partition_id
                ))
            })?;
            log::info!(
                "✅ [RecordHub::send_to_partition] Batch sent to partition {} successfully",
                partition_id
            );
            Ok(())
        } else {
            Err(CoreError::Internal(format!(
                "Partition {} not registered in RecordHub",
                partition_id
            )))
        }
    }

    /// 通知所有 partition 数据发送完毕
    ///
    /// 关闭所有 sender,让 receiver 端的 Stream 自然结束
    pub async fn finish(&self) {
        log::info!("🔚 [RecordHub::finish] Closing all senders...");
        let mut senders = self.senders.write().await;
        let count = senders.len();
        senders.clear();
        log::info!("✅ [RecordHub::finish] Closed {} sender channels", count);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_record_hub_local_only() {
        let hub = RecordHub::new(RoutingStrategy::LocalOnly, 10);
        assert_eq!(hub.strategy().num_partitions(), 1);
        assert_eq!(hub.buffer_size(), 10);
    }

    #[tokio::test]
    async fn test_record_hub_hash_partition() {
        let hub = RecordHub::new(
            RoutingStrategy::HashPartition {
                columns: vec!["id".to_string()],
                num_partitions: 4,
            },
            10,
        );
        assert_eq!(hub.strategy().num_partitions(), 4);
    }
}
