//! Engine 模块 - 核心存储引擎实现
//!
//! ## 模块结构
//! - `config`: 配置和共享类型定义
//! - `metadata`: 表和分区的元数据管理
//! - `lifecycle`: 引擎的启动和关闭逻辑
//! - `persist`: 后台持久化任务
//! - `data_operations`: 数据插入、查询和加载操作

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::{mpsc, RwLock};

use crate::{config::Config, partition::Partition, utils::error::CoreResult};

// 子模块声明
pub mod data_handler;
pub mod partition_handler;
pub mod persist_handler;

/// 核心存储引擎
///
/// **职责**：只管本地数据操作
/// - 管理本地分区的加载和卸载
/// - 处理本地数据的插入和查询
/// - 协调后台持久化任务
///
/// **不包含**：
/// - Catalog（元数据管理）→ 由 CalmService 管理
/// - ClusterManager（集群通信）→ 由 CalmService 管理
#[derive(Clone)]
pub struct Engine {
    /// 分区存储：(table_name, partition_name) -> Partition
    ///
    /// 使用 RwLock<HashMap> 提供并发读写访问
    pub(crate) partitions: Arc<RwLock<HashMap<(String, String), Arc<Partition>>>>,

    /// 持久化请求通道（发送端）
    pub(crate) persist_tx: mpsc::UnboundedSender<PersistRequest>,

    /// 分区通知通道（发送端）
    ///
    /// 当分区有新数据时，通过此通道通知持久化任务
    /// (table_name, partition_name)
    pub(crate) partition_notify_tx: Arc<mpsc::UnboundedSender<(String, String)>>,

    /// 持久化任务句柄
    ///
    /// 用于在停止时等待后台持久化任务完成
    pub(crate) persist_task_handle: Arc<tokio::sync::Mutex<Option<tokio::task::JoinHandle<()>>>>,
}

impl Engine {
    /// 创建新的 Engine 实例
    ///
    /// **Engine 职责**：只管本地数据操作
    /// - 不依赖 Catalog（由外部传入）
    /// - 不依赖 ClusterManager（由外部传入）
    ///
    /// # 示例
    /// ```rust
    /// let engine = Engine::new(EngineConfig::default()).unwrap();
    /// ```
    pub fn new(config: &Config) -> CoreResult<Arc<Self>> {
        let (persist_tx, persist_rx) = mpsc::unbounded_channel();
        let (partition_notify_tx, partition_notify_rx) = mpsc::unbounded_channel();
        let partitions = Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new()));
        let persist_task_handle = Arc::new(tokio::sync::Mutex::new(None));

        let engine = Arc::new(Self {
            partitions,
            persist_tx,
            partition_notify_tx: Arc::new(partition_notify_tx),
            persist_task_handle: persist_task_handle.clone(),
        });

        // 启动后台持久化任务（需要 Arc<Self>）
        let handle = tokio::spawn(Self::persist_background_task(
            engine.clone(),
            persist_rx,
            partition_notify_rx,
            config.engine.persist_check_interval_secs,
        ));

        // 将 handle 存入 Mutex
        let handle_clone = persist_task_handle.clone();
        tokio::spawn(async move {
            *handle_clone.lock().await = Some(handle);
        });

        Ok(engine)
    }

    /// 获取统计信息
    pub async fn stats(&self) -> EngineStats {
        let partitions = self.partitions.read().await;

        let mut stats = EngineStats::default();
        stats.partition_count = partitions.len();

        for partition in partitions.values() {
            stats.total_doc_count += partition.total_count();
            stats.total_frozen_segments += partition.get_frozen_segments().len();
            stats.total_unpersisted_segments += partition.get_unpersisted_segments().len();
        }

        stats
    }

    /// 关闭 Engine（优雅停机）
    ///
    /// **重要**: 调用后 Engine 停止所有操作，无法再使用
    ///
    /// 执行步骤：
    /// 1. 停止后台持久化任务
    /// 2. 扫描所有 Partition 并全部持久化
    /// 3. 等待所有操作完成
    pub async fn stop(&self) -> CoreResult<()> {
        // 1. 停止后台持久化任务
        log::info!("Stopping background persist task...");
        let _ = self.persist_tx.send(PersistRequest::Shutdown);

        {
            let mut handle_opt = self.persist_task_handle.lock().await;
            if let Some(handle) = handle_opt.take() {
                let _ = handle.await;
            }
        }
        log::info!("Background task stopped");

        // 2. 获取所有 Partition Keys
        let partition_keys: Vec<(String, String)> = {
            let partitions = self.partitions.read().await;
            partitions
                .keys()
                .map(|k| (k.0.clone(), k.1.clone()))
                .collect()
        };

        if partition_keys.is_empty() {
            log::info!("No partitions to persist");
            return Ok(());
        }

        log::info!("Persisting {} partitions...", partition_keys.len());

        // 3. 同步持久化所有 Partition（串行执行，确保稳定）
        let mut success_count = 0;
        let mut failed_partitions = Vec::new();

        for (table_name, partition_name) in partition_keys {
            match self.persist_partition(&table_name, &partition_name).await {
                Ok(_) => success_count += 1,
                Err(e) => {
                    log::error!(
                        "Partition {} of table {} persist failed: {:?}",
                        partition_name,
                        table_name,
                        e
                    );
                    failed_partitions.push((table_name, partition_name));
                }
            }
        }

        // 4. 记录最终统计
        let stats = self.stats().await;

        log::info!("Engine stop summary:");
        log::info!("  Partitions persisted: {}", success_count);
        if !failed_partitions.is_empty() {
            log::warn!("  Failed partitions: {:?}", failed_partitions);
        }
        log::info!("  Total documents: {}", stats.total_doc_count);
        log::info!("  Total segments: {}", stats.total_frozen_segments);
        log::info!(
            "  Unpersisted segments: {}",
            stats.total_unpersisted_segments
        );

        if stats.total_unpersisted_segments == 0 && failed_partitions.is_empty() {
            log::info!("All data persisted successfully");
        } else {
            log::warn!("Some data may not be persisted");
        }

        log::info!("Engine stop completed");

        if !failed_partitions.is_empty() {
            return Err(crate::utils::error::CoreError::Internal(format!(
                "Failed to persist {} partitions",
                failed_partitions.len()
            )));
        }

        Ok(())
    }

    /// 关闭 Engine（向后兼容的别名）
    pub async fn shutdown(&self) {
        let _ = self.stop().await;
    }
}

impl Drop for Engine {
    fn drop(&mut self) {
        // 发送关闭信号
        let _ = self.persist_tx.send(PersistRequest::Shutdown);
        // 注意: Drop 是同步的，无法 await stop()
        // 建议用户在 drop 前显式调用 engine.stop().await
        log::info!("[Engine] Dropping - if data not saved, call engine.stop().await first!");
    }
}

/// Engine 统计信息
#[derive(Debug, Clone, Default)]
pub struct EngineStats {
    pub partition_count: usize,
    pub total_doc_count: u64,
    pub total_frozen_segments: usize,
    pub total_unpersisted_segments: usize,
    pub total_memory_bytes: u64,
}

/// 插入统计信息
#[derive(Debug, Clone, Default)]
pub struct InsertStats {
    /// 插入的总行数
    pub rows_inserted: usize,
    /// 影响的分区数量
    pub partitions_affected: usize,
}

/// 持久化请求
#[derive(Debug, Clone)]
pub(crate) enum PersistRequest {
    /// 检查并持久化特定 Partition
    /// 参数: (table_name, partition_name)
    CheckPartition((String, String)),

    /// 检查并持久化所有 Partition
    CheckAll,

    /// 关闭持久化任务
    Shutdown,
}
