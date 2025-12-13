//! 持久化任务模块 - 处理后台持久化逻辑

use itertools::Itertools;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::utils::error::CoreResult;

use super::domain::PersistRequest;
use super::Engine;

impl Engine {
    /// 触发特定 Partition 的持久化检查
    pub fn trigger_persist(&self, table_name: &str, partition_name: &str) {
        let key = (table_name.to_string(), partition_name.to_string());
        let _ = self.persist_tx.send(PersistRequest::CheckPartition(key));
    }

    /// 触发所有 Partition 的持久化检查
    pub fn trigger_persist_all(&self) {
        let _ = self.persist_tx.send(PersistRequest::CheckAll);
    }

    /// 同步持久化指定的 Partition
    ///
    /// 这是一个同步操作：调用后，该 Partition 的所有 segment 保证已持久化完毕
    pub async fn persist_partition(
        &self,
        table_name: &str,
        partition_name: &str,
    ) -> CoreResult<()> {
        log::info!(
            "Persisting partition {} of table {}...",
            partition_name,
            table_name
        );

        // 1. 获取 Partition
        let partition = {
            let key = (table_name.to_string(), partition_name.to_string());
            let partitions = self.partitions.read().await;
            partitions.get(&key).cloned().ok_or_else(|| {
                crate::utils::error::CoreError::Internal(format!(
                    "Partition {} of table {} not found",
                    partition_name, table_name
                ))
            })?
        };

        // 2. 在阻塞线程池中同步执行持久化（避免阻塞异步运行时）
        tokio::task::spawn_blocking(move || {
            // 使用 Partition 的 persist_all 方法（内部会 flush + persist）
            partition.persist_all()
        })
        .await
        .map_err(|e| {
            crate::utils::error::CoreError::Internal(format!("Persist task failed: {}", e))
        })?
    }

    /// 持久化整个表的所有 partition
    ///
    /// 这是一个同步操作：调用后，该表的所有 partition 的所有 segment 保证已持久化完毕
    pub async fn flush_table(
        &self,
        catalog: &crate::catalog::Catalog,
        table_name: &str,
    ) -> CoreResult<()> {
        log::info!("🔄 Flushing table '{}'...", table_name);

        // 1. 获取表的元数据以确定有多少个 partition
        let meta = catalog.get_table(table_name)?;
        let num_partitions = meta.num_partitions().unwrap_or(1);

        log::info!(
            "🔍 Table '{}' has {} partitions",
            table_name,
            num_partitions
        );

        // 2. 持久化所有 partition
        let mut success_count = 0;
        let mut error_count = 0;

        let partition_names = match meta.partition_strategy.generate_partitions() {
            Some(names) => names,
            None => vec!["partition_000000000000000000".to_string()],
        };

        for partition_name in partition_names {
            match self.persist_partition(table_name, &partition_name).await {
                Ok(_) => {
                    success_count += 1;
                    log::info!(
                        "✅ Flushed partition {} of table '{}'",
                        partition_name,
                        table_name
                    );
                }
                Err(e) => {
                    error_count += 1;
                    log::error!(
                        "❌ Failed to flush partition {} of table '{}': {}",
                        partition_name,
                        table_name,
                        e
                    );
                }
            }
        }

        if error_count > 0 {
            Err(crate::utils::error::CoreError::Internal(format!(
                "Failed to flush {} out of {} partitions for table '{}'",
                error_count, num_partitions, table_name
            )))
        } else {
            log::info!(
                "✅ Successfully flushed all {} partitions of table '{}'",
                success_count,
                table_name
            );
            Ok(())
        }
    }

    /// 后台持久化任务
    pub(super) async fn persist_background_task(
        self: Arc<Self>,
        mut persist_rx: mpsc::UnboundedReceiver<PersistRequest>,
        mut partition_notify_rx: mpsc::UnboundedReceiver<(String, String)>,
        persist_check_interval_secs: u64,
    ) {
        log::info!("[Engine] Persist background task started");

        // 定时器
        let mut interval = tokio::time::interval(Duration::from_secs(persist_check_interval_secs));

        // 当前正在持久化的任务
        let active_tasks: Arc<tokio::sync::Mutex<HashMap<(String, String), JoinHandle<()>>>> =
            Arc::new(tokio::sync::Mutex::new(HashMap::new()));

        loop {
            tokio::select! {
                // 接收手动持久化请求
                Some(request) = persist_rx.recv() => {
                    match request {
                        PersistRequest::CheckPartition(key) => {
                            self.handle_partition_persist(
                                key,
                                &active_tasks,
                            ).await;
                        }

                        PersistRequest::CheckAll => {
                            self.handle_all_persist(
                                &active_tasks,
                            ).await;
                        }

                        PersistRequest::Shutdown => {
                            log::info!("[Engine] Persist task shutting down...");

                            // 等待所有任务完成
                            let tasks = active_tasks.lock().await;
                            for (key, _) in tasks.iter() {
                                log::info!("[Engine] Waiting for partition {}/{} to finish persisting",
                                    key.0, key.1);
                            }

                            break;
                        }
                    }
                }

                // 接收 Partition 的通知（write/flush 达到阈值）
                Some((table_name, partition_name)) = partition_notify_rx.recv() => {
                    log::info!("[Engine] Received persist notification from partition {}/{}", table_name, partition_name);
                    let key = (table_name, partition_name);
                    self.handle_partition_persist(
                        key,
                        &active_tasks,
                    ).await;
                }

                // 定时检查（兜底保障）
                _ = interval.tick() => {
                    log::info!("[Engine] Periodic persist check (fallback)");
                    self.handle_all_persist(
                        &active_tasks,
                    ).await;
                }
            }
        }

        log::info!("[Engine] Persist background task stopped");
    }

    /// 处理单个 Partition 的持久化
    async fn handle_partition_persist(
        &self,
        key: (String, String),
        active_tasks: &Arc<tokio::sync::Mutex<HashMap<(String, String), JoinHandle<()>>>>,
    ) {
        // 检查是否已有任务在执行
        {
            let tasks = active_tasks.lock().await;
            if tasks.contains_key(&key) {
                log::info!(
                    "[Engine] Partition {}/{} is already persisting, skip",
                    key.0,
                    key.1
                );
                return;
            }
        }

        // 获取 Partition
        let partition = {
            let parts = self.partitions.read().await;
            match parts.get(&key) {
                Some(p) => p.clone(),
                None => {
                    log::info!("[Engine] Partition {}/{} not found", key.0, key.1);
                    return;
                }
            }
        };

        // 检查是否需要持久化（基于 schema 的 PersistPolicy）
        let max_age = partition.schema().persist_policy.max_segment_age;
        let segments_to_persist = partition
            .get_unpersisted_segments()
            .into_iter()
            .filter(|(_, segment)| {
                // 检查是否未持久化 AND (文档数超标 OR 年龄超标)
                if segment.is_persisted() {
                    return false;
                }

                // 检查文档数阈值
                let doc_threshold_met =
                    segment.doc_count() >= partition.schema().persist_policy.max_docs_per_segment;

                // 检查时间阈值
                let age_threshold_met = segment.age() >= max_age;

                doc_threshold_met || age_threshold_met
            })
            .collect_vec();

        if segments_to_persist.is_empty() {
            return;
        }

        log::info!(
            "[Engine] Partition {}/{} has {} segments ready for persist (doc/time threshold)",
            key.0,
            key.1,
            segments_to_persist.len()
        );

        // 启动持久化任务并异步等待完成
        let active_tasks_clone = active_tasks.clone();
        let key_clone = key.clone();
        let handle = tokio::spawn(async move {
            // 在阻塞线程池中执行持久化
            let result =
                tokio::task::spawn_blocking(move || partition.persist_unpersisted_segments()).await;

            match result {
                Ok(Ok(persisted_ids)) => {
                    log::info!(
                        "[Engine] Partition {}/{} persist completed: {} segments",
                        key_clone.0,
                        key_clone.1,
                        persisted_ids.len()
                    );
                }
                Ok(Err(e)) => {
                    log::error!(
                        "[Engine] Partition {}/{} persist failed: {:?}",
                        key_clone.0,
                        key_clone.1,
                        e
                    );
                }
                Err(e) => {
                    log::error!(
                        "[Engine] Partition {}/{} persist task panicked: {:?}",
                        key_clone.0,
                        key_clone.1,
                        e
                    );
                }
            }

            // 持久化完成后立即从 active_tasks 中移除
            let mut tasks = active_tasks_clone.lock().await;
            tasks.remove(&key_clone);
            log::info!(
                "[Engine] Partition {}/{} removed from active tasks",
                key_clone.0,
                key_clone.1
            );
        });

        // 记录任务
        {
            let mut tasks = active_tasks.lock().await;
            tasks.insert(key, handle);
        }
    }

    /// 处理所有 Partition 的持久化
    async fn handle_all_persist(
        &self,
        active_tasks: &Arc<tokio::sync::Mutex<HashMap<(String, String), JoinHandle<()>>>>,
    ) {
        let partition_keys: Vec<(String, String)> = {
            let parts = self.partitions.read().await;
            parts.keys().cloned().collect()
        };

        for key in partition_keys {
            self.handle_partition_persist(key, active_tasks).await;
        }
    }
}
