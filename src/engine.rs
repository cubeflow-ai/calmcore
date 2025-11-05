use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use itertools::Itertools;
use tokio::sync::{mpsc, RwLock};
use tokio::task::JoinHandle;

use crate::partition::Partition;
use crate::schema::Schema;
use crate::utils::error::CoreResult;

/// Engine 配置
#[derive(Debug, Clone)]
pub struct EngineConfig {
    /// 数据根目录
    pub data_dir: PathBuf,

    /// 持久化检查间隔（秒）
    pub persist_check_interval_secs: u64,

    /// 最大并发持久化的 Partition 数量
    pub max_concurrent_persists: usize,

    /// 是否在 flush 后立即检查持久化
    pub check_after_flush: bool,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("./data"),
            persist_check_interval_secs: 60,
            max_concurrent_persists: 4,
            check_after_flush: true,
        }
    }
}

/// 持久化请求
#[derive(Debug)]
enum PersistRequest {
    /// 检查并持久化特定 Partition
    CheckPartition(u64),

    /// 检查并持久化所有 Partition
    CheckAll,

    /// 关闭持久化任务
    Shutdown,
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

/// 存储引擎 - 顶层管理器
pub struct Engine {
    config: EngineConfig,

    /// 所有 Partitions（partition_id -> Partition）
    partitions: Arc<RwLock<HashMap<u64, Arc<Partition>>>>,

    /// 持久化请求通道
    persist_tx: mpsc::UnboundedSender<PersistRequest>,

    /// Partition 通知通道（Partition → Engine）
    partition_notify_tx: mpsc::UnboundedSender<u64>,

    /// 后台任务句柄（使用 Mutex 以便在 Arc 中修改）
    persist_task_handle: Arc<tokio::sync::Mutex<Option<JoinHandle<()>>>>,
}

impl Engine {
    /// 创建新的 Engine 实例
    ///
    /// # 示例
    /// ```rust
    /// let engine = Engine::new(EngineConfig::default());
    /// ```
    pub fn new(config: EngineConfig) -> Arc<Self> {
        let (persist_tx, persist_rx) = mpsc::unbounded_channel();
        let (partition_notify_tx, partition_notify_rx) = mpsc::unbounded_channel();
        let partitions = Arc::new(RwLock::new(HashMap::new()));
        let persist_task_handle = Arc::new(tokio::sync::Mutex::new(None));

        let engine = Arc::new(Self {
            config,
            partitions,
            persist_tx,
            partition_notify_tx,
            persist_task_handle: persist_task_handle.clone(),
        });

        // 启动后台持久化任务（需要 Arc<Self>）
        let handle = tokio::spawn(Self::persist_background_task(
            engine.clone(),
            persist_rx,
            partition_notify_rx,
        ));

        // 将 handle 存入 Mutex
        let handle_clone = persist_task_handle.clone();
        tokio::spawn(async move {
            *handle_clone.lock().await = Some(handle);
        });

        engine
    }

    /// 创建新的 Partition
    pub async fn create_partition(&self, id: u32, schema: Schema) -> Arc<Partition> {
        let partition_dir = self.config.data_dir.join(format!("partition-{}", id));
        let partition = Partition::new(id, partition_dir, schema, self.partition_notify_tx.clone());

        let partition = Arc::new(partition);
        self.add_partition(partition.clone()).await;
        partition
    }

    /// 添加已存在的 Partition
    pub async fn add_partition(&self, partition: Arc<Partition>) {
        let partition_id = partition.id();
        let mut partitions = self.partitions.write().await;
        partitions.insert(partition_id, partition);

        println!("[Engine] Added partition {}", partition_id);
    }

    /// 加载 Partition（从磁盘恢复）
    pub async fn load_partition(&self, id: u32, schema: Schema) -> CoreResult<Arc<Partition>> {
        let partition_dir = self.config.data_dir.join(format!("partition-{}", id));
        let partition =
            Partition::load(id, partition_dir, schema, self.partition_notify_tx.clone())?;

        let partition = Arc::new(partition);
        self.add_partition(partition.clone()).await;
        Ok(partition)
    }

    /// 移除 Partition
    pub async fn remove_partition(&self, partition_id: u64) {
        let mut partitions = self.partitions.write().await;
        partitions.remove(&partition_id);

        println!("[Engine] Removed partition {}", partition_id);
    }

    /// 获取 Partition
    pub async fn get_partition(&self, partition_id: u64) -> Option<Arc<Partition>> {
        let partitions = self.partitions.read().await;
        partitions.get(&partition_id).cloned()
    }

    /// 列出所有 Partition
    pub async fn list_partitions(&self) -> Vec<u64> {
        let partitions = self.partitions.read().await;
        partitions.keys().copied().collect()
    }

    /// 触发特定 Partition 的持久化检查
    pub fn trigger_persist(&self, partition_id: u64) {
        let _ = self
            .persist_tx
            .send(PersistRequest::CheckPartition(partition_id));
    }

    /// 触发所有 Partition 的持久化检查
    pub fn trigger_persist_all(&self) {
        let _ = self.persist_tx.send(PersistRequest::CheckAll);
    }

    /// 获取统计信息
    pub async fn stats(&self) -> EngineStats {
        let partitions = self.partitions.read().await;

        let mut stats = EngineStats::default();
        stats.partition_count = partitions.len();

        for partition in partitions.values() {
            stats.total_doc_count += partition.total_count();
            stats.total_frozen_segments += partition.frozen_count();
            stats.total_unpersisted_segments += partition.get_unpersisted_segments().len();
        }

        stats
    }

    /// 打印统计信息
    pub async fn print_stats(&self) {
        let stats = self.stats().await;

        println!("\n╔══════════════════════════════════════════════════════════════╗");
        println!("║                     Engine Statistics                        ║");
        println!("╠══════════════════════════════════════════════════════════════╣");
        println!("║ Partitions          : {:>39} ║", stats.partition_count);
        println!("║ Total Documents     : {:>39} ║", stats.total_doc_count);
        println!(
            "║ Frozen Segments     : {:>39} ║",
            stats.total_frozen_segments
        );
        println!(
            "║ Unpersisted Segments: {:>39} ║",
            stats.total_unpersisted_segments
        );
        println!("╚══════════════════════════════════════════════════════════════╝\n");
    }

    /// 关闭 Engine
    pub async fn shutdown(&self) {
        println!("[Engine] Shutting down...");

        // 发送关闭信号
        let _ = self.persist_tx.send(PersistRequest::Shutdown);

        // 等待后台任务完成
        let mut handle_guard = self.persist_task_handle.lock().await;
        if let Some(handle) = handle_guard.take() {
            drop(handle_guard); // 释放锁
            let _ = handle.await;
        }

        println!("[Engine] Shutdown complete");
    }

    /// 后台持久化任务
    async fn persist_background_task(
        self: Arc<Self>,
        mut persist_rx: mpsc::UnboundedReceiver<PersistRequest>,
        mut partition_notify_rx: mpsc::UnboundedReceiver<u64>,
    ) {
        println!("[Engine] Persist background task started");

        // 定时器
        let mut interval =
            tokio::time::interval(Duration::from_secs(self.config.persist_check_interval_secs));

        // 当前正在持久化的任务
        let active_tasks: Arc<tokio::sync::Mutex<HashMap<u64, JoinHandle<()>>>> =
            Arc::new(tokio::sync::Mutex::new(HashMap::new()));

        loop {
            tokio::select! {
                // 接收手动持久化请求
                Some(request) = persist_rx.recv() => {
                    match request {
                        PersistRequest::CheckPartition(partition_id) => {
                            self.handle_partition_persist(
                                partition_id,
                                &active_tasks,
                            ).await;
                        }

                        PersistRequest::CheckAll => {
                            self.handle_all_persist(
                                &active_tasks,
                            ).await;
                        }

                        PersistRequest::Shutdown => {
                            println!("[Engine] Persist task shutting down...");

                            // 等待所有任务完成
                            let tasks = active_tasks.lock().await;
                            for (partition_id, _) in tasks.iter() {
                                println!("[Engine] Waiting for partition {} to finish persisting", partition_id);
                            }

                            break;
                        }
                    }
                }

                // 接收 Partition 的通知（write/flush 达到阈值）
                Some(partition_id) = partition_notify_rx.recv() => {
                    println!("[Engine] Received persist notification from partition {}", partition_id);
                    self.handle_partition_persist(
                        partition_id,
                        &active_tasks,
                    ).await;
                }

                // 定时检查（兜底保障）
                _ = interval.tick() => {
                    // println!("[Engine] Periodic persist check (fallback)");
                    self.handle_all_persist(
                        &active_tasks,
                    ).await;
                }
            }
        }

        println!("[Engine] Persist background task stopped");
    }

    /// 处理单个 Partition 的持久化
    async fn handle_partition_persist(
        &self,
        partition_id: u64,
        active_tasks: &Arc<tokio::sync::Mutex<HashMap<u64, JoinHandle<()>>>>,
    ) {
        // 检查是否已有任务在执行
        {
            let tasks = active_tasks.lock().await;
            if tasks.contains_key(&partition_id) {
                println!(
                    "[Engine] Partition {} is already persisting, skip",
                    partition_id
                );
                return;
            }
        }

        // 获取 Partition
        let partition = {
            let parts = self.partitions.read().await;
            match parts.get(&partition_id) {
                Some(p) => p.clone(),
                None => {
                    println!("[Engine] Partition {} not found", partition_id);
                    return;
                }
            }
        };

        // 检查是否需要持久化（基于 schema 的 PersistPolicy）
        let segments_to_persist = partition
            .get_unpersisted_segments()
            .into_iter()
            .filter(|(_, segment)| !segment.is_persisted())
            .collect_vec();

        if segments_to_persist.is_empty() {
            return;
        }

        println!(
            "[Engine] Partition {} has {} segments ready for persist (doc/time threshold)",
            partition_id,
            segments_to_persist.len()
        );

        // 启动持久化任务并异步等待完成
        let active_tasks_clone = active_tasks.clone();
        let handle = tokio::spawn(async move {
            // 在阻塞线程池中执行持久化
            let result =
                tokio::task::spawn_blocking(move || partition.persist_unpersisted_segments()).await;

            match result {
                Ok(Ok(persisted_ids)) => {
                    println!(
                        "[Engine] Partition {} persist completed: {} segments",
                        partition_id,
                        persisted_ids.len()
                    );
                }
                Ok(Err(e)) => {
                    eprintln!(
                        "[Engine] Partition {} persist failed: {:?}",
                        partition_id, e
                    );
                }
                Err(e) => {
                    eprintln!(
                        "[Engine] Partition {} persist task panicked: {:?}",
                        partition_id, e
                    );
                }
            }

            // 持久化完成后立即从 active_tasks 中移除
            let mut tasks = active_tasks_clone.lock().await;
            tasks.remove(&partition_id);
            println!(
                "[Engine] Partition {} removed from active tasks",
                partition_id
            );
        });

        // 记录任务
        {
            let mut tasks = active_tasks.lock().await;
            tasks.insert(partition_id, handle);
        }
    }

    /// 处理所有 Partition 的持久化
    async fn handle_all_persist(
        &self,
        active_tasks: &Arc<tokio::sync::Mutex<HashMap<u64, JoinHandle<()>>>>,
    ) {
        let partition_ids: Vec<u64> = {
            let parts = self.partitions.read().await;
            parts.keys().copied().collect()
        };

        for partition_id in partition_ids {
            self.handle_partition_persist(partition_id, active_tasks)
                .await;
        }
    }

    /// 优雅停止 Engine 并确保所有数据持久化
    ///
    /// 执行步骤：
    /// 1. 停止持久化后台任务
    /// 2. 对每个 Partition 调用 stop() 确保数据持久化
    /// 3. 等待所有持久化操作完成
    pub async fn stop(&self) {
        println!("\n╔════════════════════════════════════════════════════╗");
        println!("║           Engine Stopping - Saving Data           ║");
        println!("╚════════════════════════════════════════════════════╝");

        // 1. 发送关闭信号给持久化任务
        let _ = self.persist_tx.send(PersistRequest::Shutdown);

        // 2. 等待持久化任务完成
        {
            let mut handle_opt = self.persist_task_handle.lock().await;
            if let Some(handle) = handle_opt.take() {
                println!("[Engine] Waiting for persist task to shutdown...");
                let _ = handle.await;
                println!("[Engine] Persist task shutdown completed");
            }
        }

        // 3. 获取所有 Partition 并调用它们的 stop
        let partition_list: Vec<(u64, Arc<Partition>)> = {
            let parts = self.partitions.read().await;
            parts.iter().map(|(id, p)| (*id, p.clone())).collect()
        };

        let partition_count = partition_list.len();
        println!("[Engine] Stopping {} partitions...", partition_count);

        // 4. 并发停止所有 Partition（使用 blocking 线程池）
        let handles: Vec<_> = partition_list
            .into_iter()
            .map(|(id, partition)| {
                tokio::task::spawn_blocking(move || {
                    partition.stop();
                    id
                })
            })
            .collect();

        // 5. 等待所有 Partition 停止完成
        let mut success_count = 0;
        let mut failed_count = 0;

        for handle in handles {
            match handle.await {
                Ok(partition_id) => {
                    println!("[Engine] Partition {} stopped successfully", partition_id);
                    success_count += 1;
                }
                Err(e) => {
                    eprintln!("[Engine] Partition stop failed: {:?}", e);
                    failed_count += 1;
                }
            }
        }

        // 6. 统计信息
        let stats = self.stats().await;

        println!("\n╔════════════════════════════════════════════════════╗");
        println!("║              Engine Stop Summary                   ║");
        println!("╚════════════════════════════════════════════════════╝");
        println!(
            "  Partitions stopped: {}/{}",
            success_count, partition_count
        );
        if failed_count > 0 {
            println!("  ⚠️  Failed: {}", failed_count);
        }
        println!("  Total documents: {}", stats.total_doc_count);
        println!("  Total frozen segments: {}", stats.total_frozen_segments);
        println!(
            "  Unpersisted segments: {}",
            stats.total_unpersisted_segments
        );

        if stats.total_unpersisted_segments == 0 {
            println!("\n  ✅ All data persisted successfully!");
        } else {
            println!(
                "\n  ⚠️  Warning: {} segments not persisted",
                stats.total_unpersisted_segments
            );
        }

        println!("\n[Engine] Stop completed.");
    }
}

impl Drop for Engine {
    fn drop(&mut self) {
        // 发送关闭信号
        let _ = self.persist_tx.send(PersistRequest::Shutdown);
        // 注意: Drop 是同步的，无法 await stop()
        // 建议用户在 drop 前显式调用 engine.stop().await
        println!("[Engine] Dropping - if data not saved, call engine.stop().await first!");
    }
}
