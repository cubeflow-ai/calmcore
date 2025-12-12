//! 生命周期管理模块 - 处理Engine的启动和停止

use std::sync::Arc;
use tokio::sync::mpsc;

use crate::partition::Partition;
use crate::utils::error::CoreResult;

use super::config::{EngineConfig, EngineStats, PersistRequest};
use super::Engine;

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
    pub fn new(config: EngineConfig) -> CoreResult<Arc<Self>> {
        let (persist_tx, persist_rx) = mpsc::unbounded_channel();
        let (partition_notify_tx, partition_notify_rx) = mpsc::unbounded_channel();
        let partitions = Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new()));
        let persist_task_handle = Arc::new(tokio::sync::Mutex::new(None));

        let engine = Arc::new(Self {
            config,
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
        ));

        // 将 handle 存入 Mutex
        let handle_clone = persist_task_handle.clone();
        tokio::spawn(async move {
            *handle_clone.lock().await = Some(handle);
        });

        Ok(engine)
    }

    /// 加载所有已存在的表和它们的 partition
    pub async fn load_existing_tables_from_disk(
        self: &Arc<Self>,
        catalog: &crate::catalog::Catalog,
    ) -> CoreResult<()> {
        log::info!("🔍 [Engine] Loading existing tables...");
        let table_names = catalog.list_tables();
        log::info!("🔍 [Engine] Found {} tables", table_names.len());

        for table_name in table_names {
            log::info!("🔍 [Engine] Loading table: {}", table_name);
            let meta = match catalog.get_table(&table_name) {
                Ok(meta) => meta,
                Err(e) => {
                    log::warn!("⚠️  Failed to get metadata for table {}: {}", table_name, e);
                    continue;
                }
            };

            let table_dir = self.config.data_dir.join("tables").join(&table_name);
            let partitions_dir = table_dir.join("partitions");

            // 统一扫描 partitions 子目录，不依赖分区策略
            log::info!(
                "🔍 [Engine] Scanning partition directories for table {}...",
                table_name
            );

            let mut partition_names: Vec<String> = Vec::new();
            if partitions_dir.exists() {
                if let Ok(entries) = std::fs::read_dir(&partitions_dir) {
                    for entry in entries.flatten() {
                        let path = entry.path();
                        if path.is_dir() {
                            if let Some(dir_name) = path.file_name().and_then(|n| n.to_str()) {
                                if let Some(partition_name) =
                                    crate::catalog::PartitionStrategy::extract_partition_from_dir_name(
                                        dir_name,
                                    )
                                {
                                    partition_names.push(partition_name);
                                }
                            }
                        }
                    }
                }
            }

            // 如果没有找到任何 partition，根据策略创建默认的
            if partition_names.is_empty() {
                log::info!(
                    "🔍 [Engine] No existing partitions found, creating default partitions for table {}",
                    table_name
                );
                partition_names = meta
                    .partition_strategy
                    .generate_partitions()
                    .unwrap_or_else(Vec::new);
            } else {
                log::info!(
                    "🔍 [Engine] Found {} partitions for table {}",
                    partition_names.len(),
                    table_name
                );
            }

            // 加载所有 partition
            for partition_name in partition_names {
                let partition_dir = partitions_dir.join(
                    crate::catalog::PartitionStrategy::generate_partition_dir_name(&partition_name),
                );

                // 检查目录是否存在
                if !partition_dir.exists() {
                    log::error!(
                        "🔍 [Engine] Partition directory does not exist, creating new partition: {}",
                        partition_name
                    );
                    continue;
                }

                log::info!("🔍 [Engine] Loading existing partition: {}", partition_name);
                // 如果目录存在，从磁盘加载
                match Partition::load(
                    partition_name.clone(),
                    table_name.clone(),
                    partition_dir.clone(),
                    meta.schema.clone(),
                    (*self.partition_notify_tx).clone(),
                ) {
                    Ok(partition) => {
                        let partition = Arc::new(partition);
                        self.partitions
                            .write()
                            .await
                            .insert((table_name.clone(), partition_name.clone()), partition);
                    }
                    Err(e) => {
                        log::error!(
                            "⚠️  Failed to load partition {} for table {}: {}",
                            partition_name,
                            table_name,
                            e
                        );
                    }
                }
            }

            let loaded_count = self.list_partitions(&table_name).await.len();

            log::info!(
                "✅ Loaded table '{}' with {} partitions",
                table_name,
                loaded_count
            );
        }

        Ok(())
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
