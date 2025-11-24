use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use itertools::Itertools;
use tokio::sync::{mpsc, RwLock};
use tokio::task::JoinHandle;

use crate::catalog::{Catalog, PartitionStrategy, TableMeta};
use crate::partition::Partition;
use crate::schema::Schema;
use crate::utils::error::CoreResult;

/// Partition 的唯一标识 (表名, partition_name)
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
struct PartitionKey {
    table_name: String,
    partition_name: String,
}

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
#[derive(Debug, Clone)]
enum PersistRequest {
    /// 检查并持久化特定 Partition
    CheckPartition(PartitionKey),

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

    /// Catalog - 表元数据管理
    catalog: Arc<Catalog>,

    /// 所有 Partitions（(table_name, partition_name) -> Partition）
    partitions: Arc<RwLock<HashMap<PartitionKey, Arc<Partition>>>>,

    /// 持久化请求通道
    persist_tx: mpsc::UnboundedSender<PersistRequest>,

    /// Partition 通知通道（Partition → Engine）
    /// 格式: (table_name, partition_name)
    partition_notify_tx: mpsc::UnboundedSender<(String, String)>,

    /// 后台任务句柄（使用 Mutex 以便在 Arc 中修改）
    persist_task_handle: Arc<tokio::sync::Mutex<Option<JoinHandle<()>>>>,
}

impl Engine {
    /// 创建新的 Engine 实例
    ///
    /// # 示例
    /// ```rust
    /// let engine = Engine::new(EngineConfig::default()).unwrap();
    /// ```
    pub fn new(config: EngineConfig) -> CoreResult<Arc<Self>> {
        let (persist_tx, persist_rx) = mpsc::unbounded_channel();
        let (partition_notify_tx, partition_notify_rx) = mpsc::unbounded_channel();
        let partitions = Arc::new(RwLock::new(HashMap::new()));
        let persist_task_handle = Arc::new(tokio::sync::Mutex::new(None));

        // 创建 Catalog
        let catalog = Arc::new(Catalog::new(config.data_dir.clone())?);

        let engine = Arc::new(Self {
            config,
            catalog,
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

        Ok(engine)
    }

    /// 加载所有已存在的表和它们的 partition
    pub async fn load_existing_tables(self: &Arc<Self>) -> CoreResult<()> {
        log::info!("🔍 [Engine] Loading existing tables...");
        let table_names = self.catalog.list_tables();
        log::info!("🔍 [Engine] Found {} tables", table_names.len());

        for table_name in table_names {
            log::info!("🔍 [Engine] Loading table: {}", table_name);
            let meta = match self.catalog.get_table(&table_name) {
                Ok(meta) => meta,
                Err(e) => {
                    log::warn!("⚠️  Failed to get metadata for table {}: {}", table_name, e);
                    continue;
                }
            };

            let table_dir = self.config.data_dir.join("tables").join(&table_name);
            let partitions_dir = table_dir.join("partitions");

            // 统一扫描 partitions 子目录，不依赖分区策略
            log::info!("🔍 [Engine] Scanning partition directories for table {}...", table_name);
            
            let mut partition_names: Vec<String> = Vec::new();
            if partitions_dir.exists() {
                if let Ok(entries) = std::fs::read_dir(&partitions_dir) {
                    for entry in entries.flatten() {
                        let path = entry.path();
                        if path.is_dir() {
                            if let Some(dir_name) = path.file_name().and_then(|n| n.to_str()) {
                                if let Some(partition_name) = 
                                    crate::catalog::PartitionStrategy::extract_partition_from_dir_name(dir_name) 
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
                log::info!("🔍 [Engine] No existing partitions found, creating default partitions for table {}", table_name);
                partition_names = meta.partition_strategy.generate_partitions(meta.parallel_workers);
            } else {
                log::info!("🔍 [Engine] Found {} partitions for table {}", partition_names.len(), table_name);
            }

            // 加载所有 partition
            for partition_name in partition_names {
                let partition_dir = partitions_dir.join(
                    crate::catalog::PartitionStrategy::generate_partition_dir_name(&partition_name),
                );

                // 检查目录是否存在
                if !partition_dir.exists() {
                    log::info!("🔍 [Engine] Partition directory does not exist, creating new partition: {}", partition_name);
                    // 如果目录不存在，创建新的 partition
                    let partition = Partition::new(
                        partition_name.clone(),
                        table_name.clone(),
                        partition_dir,
                        meta.schema.clone(),
                        self.partition_notify_tx.clone(),
                    );
                    let partition = Arc::new(partition);
                    self.add_partition_with_table(&table_name, partition).await;
                } else {
                    log::info!("🔍 [Engine] Loading existing partition: {}", partition_name);
                    // 如果目录存在，从磁盘加载
                    match Partition::load(
                        partition_name.clone(),
                        table_name.clone(),
                        partition_dir,
                        meta.schema.clone(),
                        self.partition_notify_tx.clone(),
                    ) {
                        Ok(partition) => {
                            let partition = Arc::new(partition);
                            self.add_partition_with_table(&table_name, partition).await;
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

    /// 创建新表
    ///
    /// # 参数
    /// - `table_name`: 表名
    /// - `schema`: 表的 Schema
    /// - `partition_strategy`: 分区策略
    /// - `num_partitions`: 分区数量
    ///
    /// # 示例
    /// ```rust
    /// use calm::catalog::PartitionStrategy;
    ///
    /// engine.create_table(
    ///     "my_table",
    ///     schema,
    ///     PartitionStrategy::Hash {
    ///         field: "id".to_string(),
    ///         num_partitions: 4,
    ///     },
    ///     4,
    /// ).await?;
    /// ```
    pub async fn create_table(
        &self,
        table_name: &str,
        schema: Schema,
        partition_strategy: PartitionStrategy,
        num_partitions: usize,
    ) -> CoreResult<()> {
        // 创建 TableMeta
        let meta = TableMeta::new(
            table_name.to_string(),
            schema.clone(),
            partition_strategy,
            num_partitions,
        );

        // 在 Catalog 中创建表（会创建目录结构和元数据）
        self.catalog.create_table(meta)?;

        // 加载所有 partition 到内存
        log::debug!(
            "🔍 [DEBUG create_table] Creating {} partitions for table '{}'",
            num_partitions,
            table_name
        );

        let table_meta = self.catalog.get_table(table_name)?;

        // 生成所有 partition 名称
        let partition_names = table_meta.partition_strategy.generate_partitions(num_partitions);

        for partition_name in partition_names {
            let partition_dir = self.config.data_dir
                .join("tables")
                .join(table_name)
                .join("partitions")
                .join(crate::catalog::PartitionStrategy::generate_partition_dir_name(&partition_name));

            log::debug!(
                "🔍 [DEBUG create_table] Creating partition {} at {:?}",
                partition_name,
                partition_dir
            );

            let partition = Partition::new(
                partition_name.clone(),
                table_name.to_string(),
                partition_dir,
                schema.clone(),
                self.partition_notify_tx.clone(),
            );

            let partition = Arc::new(partition);
            log::debug!(
                "🔍 [DEBUG create_table] About to add partition {} to map",
                partition_name
            );
            self.add_partition_with_table(table_name, partition).await;
            log::debug!(
                "🔍 [DEBUG create_table] Finished adding partition {}",
                partition_name
            );
        }

        log::debug!(
            "✅ Table '{}' created with {} partitions",
            table_name,
            num_partitions
        );
        Ok(())
    }

    /// 获取表的元数据
    pub fn get_table_meta(&self, table_name: &str) -> CoreResult<Arc<TableMeta>> {
        self.catalog.get_table(table_name)
    }

    /// 列出所有表
    pub fn list_tables(&self) -> Vec<String> {
        self.catalog.list_tables()
    }

    /// 删除表
    pub async fn drop_table(&self, table_name: &str) -> CoreResult<()> {
        // 直接扫描磁盘上的 partitions 子目录，不依赖分区策略
        let partitions_dir = self.config.data_dir.join("tables").join(table_name).join("partitions");
        
        let mut partition_names = Vec::new();
        if partitions_dir.exists() {
            if let Ok(entries) = std::fs::read_dir(&partitions_dir) {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if path.is_dir() {
                        if let Some(dir_name) = path.file_name().and_then(|n| n.to_str()) {
                            // 提取 partition-{name} 格式的目录
                            if let Some(partition_name) = 
                                crate::catalog::PartitionStrategy::extract_partition_from_dir_name(dir_name) 
                            {
                                partition_names.push(partition_name);
                            }
                        }
                    }
                }
            }
        }

        // 移除所有相关的 partition
        for partition_name in partition_names {
            self.remove_partition(table_name, &partition_name).await;
        }

        // 从 catalog 中删除
        self.catalog.drop_table(table_name)?;
        Ok(())
    }

    /// 根据分区策略路由到对应的 partition_name
    ///
    /// # 参数
    /// - `table_name`: 表名
    /// - `partition_value`: 分区字段的值
    ///
    /// # 返回
    /// 返回应该使用的 partition_name
    pub fn route_partition(&self, table_name: &str, partition_value: &str) -> CoreResult<String> {
        let meta = self.catalog.get_table(table_name)?;

        match &meta.partition_strategy {
            PartitionStrategy::Hash { num_partitions, .. } => {
                // Hash 分区：对值进行 hash 然后取模
                use std::collections::hash_map::DefaultHasher;
                use std::hash::{Hash, Hasher};

                let mut hasher = DefaultHasher::new();
                partition_value.hash(&mut hasher);
                let hash = hasher.finish();

                let index = (hash % (*num_partitions as u64)) as usize;
                
                // 生成所有 partition 名称并返回对应索引的
                let partitions = meta.partition_strategy.generate_partitions(meta.parallel_workers);
                Ok(partitions.get(index).cloned().unwrap_or_else(|| format!("{:019}", index)))
            }

            PartitionStrategy::Range { ranges, .. } => {
                // Range 分区：找到值所在的范围
                let value_enum = Self::parse_partition_value(partition_value);

                let partitions = meta.partition_strategy.generate_partitions(meta.parallel_workers);
                
                for (idx, range) in ranges.iter().enumerate() {
                    if value_enum >= range.start && value_enum < range.end {
                        return Ok(partitions.get(idx).cloned().unwrap_or_else(|| format!("{:019}", idx)));
                    }
                }
                // 如果没找到匹配的范围，返回最后一个 partition
                let last_idx = ranges.len().saturating_sub(1);
                Ok(partitions.get(last_idx).cloned().unwrap_or_else(|| format!("{:019}", last_idx)))
            }

            PartitionStrategy::Custom => {
                // Custom 分区：用户自定义，直接使用 partition_value 作为 partition_name
                Ok(PartitionStrategy::sanitize_filename(partition_value))
            }

            PartitionStrategy::None => {
                // 无分区策略，返回默认 partition
                let partitions = meta.partition_strategy.generate_partitions(1);
                Ok(partitions.get(0).cloned().unwrap_or_else(|| format!("{:019}", 0)))
            }
        }
    }

    /// 辅助方法：将字符串值解析为 PartitionValue
    fn parse_partition_value(value: &str) -> crate::catalog::PartitionValue {
        use crate::catalog::PartitionValue;

        // 尝试解析为整数
        if let Ok(val) = value.parse::<i64>() {
            return PartitionValue::Int64(val);
        }

        // 尝试解析为无符号整数
        if let Ok(val) = value.parse::<u64>() {
            return PartitionValue::UInt64(val);
        }

        // 默认作为字符串
        PartitionValue::String(value.to_string())
    }

    /// 创建新的 Partition（低级 API，通常不需要直接调用）
    /// 已废弃：使用带 table_name 的版本
    #[allow(dead_code)]
    #[deprecated(note = "需要提供 table_name")]
    pub async fn create_partition(&self, _id: u64, _schema: Schema) -> Arc<Partition> {
        panic!("create_partition is deprecated, use create_partition_with_table instead");
    }

    /// 添加已存在的 Partition (带表名)
    pub async fn add_partition_with_table(&self, table_name: &str, partition: Arc<Partition>) {
        let partition_name = partition.name().to_string();
        let key = PartitionKey {
            table_name: table_name.to_string(),
            partition_name: partition_name.clone(),
        };
        let mut partitions = self.partitions.write().await;
        partitions.insert(key.clone(), partition);

        log::debug!(
            "🔍 [DEBUG] Added partition: table={}, partition_name={}",
            table_name,
            partition_name
        );
        log::debug!("🔍 [DEBUG] Total partitions in map: {}", partitions.len());
        log::debug!("🔍 [DEBUG] Key inserted: {:?}", key);

        log::info!("Added partition {} for table {}", partition_name, table_name);
    }

    /// 添加已存在的 Partition (兼容旧接口,已废弃)
    #[deprecated(note = "使用 add_partition_with_table 代替")]
    pub async fn add_partition(&self, partition: Arc<Partition>) {
        let partition_name = partition.name().to_string();
        let mut partitions = self.partitions.write().await;
        // 使用 partition_name 作为 table_name (向后兼容)
        let key = PartitionKey {
            table_name: format!("__legacy_{}", partition_name),
            partition_name: partition_name.clone(),
        };
        partitions.insert(key, partition);

        log::info!("Added partition {} (legacy mode)", partition_name);
    }

    /// 加载 Partition（从磁盘恢复）
    pub async fn load_partition(
        &self,
        table_name: &str,
        id: String,
        schema: Schema,
    ) -> CoreResult<Arc<Partition>> {
        let partition_dir = self
            .config
            .data_dir
            .join("tables")
            .join(table_name)
            .join(crate::catalog::PartitionStrategy::generate_partition_dir_name(&id));
        let partition = Partition::load(
            id,
            table_name.to_string(),
            partition_dir,
            schema,
            self.partition_notify_tx.clone(),
        )?;

        let partition = Arc::new(partition);
        self.add_partition_with_table(table_name, partition.clone())
            .await;
        Ok(partition)
    }

    /// 移除 Partition
    pub async fn remove_partition(&self, table_name: &str, partition_name: &str) {
        let key = PartitionKey {
            table_name: table_name.to_string(),
            partition_name: partition_name.to_string(),
        };
        let mut partitions = self.partitions.write().await;
        partitions.remove(&key);

        log::info!(
            "Removed partition {} for table {}",
            partition_name,
            table_name
        );
    }

    /// 获取 Partition
    pub async fn get_partition(
        &self,
        table_name: &str,
        partition_name: &str,
    ) -> Option<Arc<Partition>> {
        let key = PartitionKey {
            table_name: table_name.to_string(),
            partition_name: partition_name.to_string(),
        };
        let partitions = self.partitions.read().await;
        let result = partitions.get(&key).cloned();
        result
    }

    /// 列出表的所有 Partition IDs
    pub async fn list_partitions(&self, table_name: &str) -> Vec<String> {
        let partitions = self.partitions.read().await;
        partitions
            .keys()
            .filter(|k| k.table_name == table_name)
            .map(|k| k.partition_name.clone())
            .collect()
    }

    /// 列出所有 Partition Keys
    pub async fn list_all_partition_keys(&self) -> Vec<(String, String)> {
        let partitions = self.partitions.read().await;
        partitions
            .keys()
            .map(|k| (k.table_name.clone(), k.partition_name.clone()))
            .collect()
    }

    /// 列出指定表/分区下的所有 segment 信息（包括 frozen 和 current）
    /// 返回: (segment_id, doc_count, created_ts_ms, is_current)
    pub async fn list_segments(
        &self,
        table_name: &str,
        partition_name: &str,
    ) -> Vec<(u64, u64, u64, bool)> {
        let key = PartitionKey {
            table_name: table_name.to_string(),
            partition_name: partition_name.to_string(),
        };
        let partitions = self.partitions.read().await;
        if let Some(partition) = partitions.get(&key) {
            let mut infos = Vec::new();

            // frozen segments
            let frozen = partition.get_frozen_segments();
            for (seg_id, seg) in frozen.iter() {
                // 估算绝对创建时间戳：当前时间戳减去 age
                let age = seg.created_since_start();
                let now_ts_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64;
                let created_ts_ms = now_ts_ms.saturating_sub(age.as_millis() as u64);
                infos.push((
                    *seg_id,
                    seg.doc_count() as u64,
                    created_ts_ms,
                    false, // is_current = false for frozen
                ));
            }
            drop(frozen);

            // current segment
            let current = partition.get_current_segment();
            let age = current.created_since_start();
            let now_ts_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            let created_ts_ms = now_ts_ms.saturating_sub(age.as_millis() as u64);
            let seg_id = current.start; // Use actual start ID
            infos.push((seg_id, current.doc_count() as u64, created_ts_ms, true));
            drop(current);

            infos
        } else {
            Vec::new()
        }
    }

    /// 触发特定 Partition 的持久化检查
    pub fn trigger_persist(&self, table_name: &str, partition_name: &str) {
        let key = PartitionKey {
            table_name: table_name.to_string(),
            partition_name: partition_name.to_string(),
        };
        let _ = self.persist_tx.send(PersistRequest::CheckPartition(key));
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
            stats.total_frozen_segments += partition.get_frozen_segments().len();
            stats.total_unpersisted_segments += partition.get_unpersisted_segments().len();
        }

        stats
    }

    /// 同步持久化指定的 Partition
    ///
    /// 这是一个同步操作：调用后，该 Partition 的所有 segment 保证已持久化完毕
    ///
    /// # 示例
    /// ```rust
    /// engine.persist_partition("users", "0000000000000000001").await;
    /// // 此时 users 表的 partition 1 的所有数据已写入磁盘
    /// ```
    pub async fn persist_partition(&self, table_name: &str, partition_name: &str) -> CoreResult<()> {
        log::info!(
            "Persisting partition {} of table {}...",
            partition_name,
            table_name
        );

        // 1. 获取 Partition
        let partition = {
            let key = PartitionKey {
                table_name: table_name.to_string(),
                partition_name: partition_name.to_string(),
            };
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
    ///
    /// # 示例
    /// ```rust
    /// engine.flush_table("users").await?;
    /// // 此时 users 表的所有数据已写入磁盘
    /// ```
    pub async fn flush_table(&self, table_name: &str) -> CoreResult<()> {
        log::info!("🔄 Flushing table '{}'...", table_name);

        // 1. 获取表的元数据以确定有多少个 partition
        let meta = self.catalog.get_table(table_name)?;
        let num_partitions = meta.parallel_workers;

        log::info!(
            "🔍 Table '{}' has {} partitions",
            table_name,
            num_partitions
        );

        // 2. 持久化所有 partition
        let mut success_count = 0;
        let mut error_count = 0;

        let partition_names = meta.partition_strategy.generate_partitions(num_partitions);
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

    /// 关闭 Engine（优雅停机）
    ///
    /// **重要**: 调用后 Engine 停止所有操作，无法再使用
    ///
    /// 执行步骤：
    /// 1. 停止后台持久化任务
    /// 2. 扫描所有 Partition 并全部持久化
    /// 3. 等待所有操作完成
    ///
    /// # 示例
    /// ```rust
    /// engine.stop().await;
    /// // 此时所有数据已安全写入磁盘，Engine 不可再用
    /// ```
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
                .map(|k| (k.table_name.clone(), k.partition_name.clone()))
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

    /// 后台持久化任务
    async fn persist_background_task(
        self: Arc<Self>,
        mut persist_rx: mpsc::UnboundedReceiver<PersistRequest>,
        mut partition_notify_rx: mpsc::UnboundedReceiver<(String, String)>,
    ) {
        log::info!("[Engine] Persist background task started");

        // 定时器
        let mut interval =
            tokio::time::interval(Duration::from_secs(self.config.persist_check_interval_secs));

        // 当前正在持久化的任务
        let active_tasks: Arc<tokio::sync::Mutex<HashMap<PartitionKey, JoinHandle<()>>>> =
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
                                    key.table_name, key.partition_name);
                            }

                            break;
                        }
                    }
                }

                // 接收 Partition 的通知（write/flush 达到阈值）
                Some((table_name, partition_name)) = partition_notify_rx.recv() => {
                    log::info!("[Engine] Received persist notification from partition {}/{}", table_name, partition_name);
                    let key = PartitionKey {
                        table_name,
                        partition_name,
                    };
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
        key: PartitionKey,
        active_tasks: &Arc<tokio::sync::Mutex<HashMap<PartitionKey, JoinHandle<()>>>>,
    ) {
        // 检查是否已有任务在执行
        {
            let tasks = active_tasks.lock().await;
            if tasks.contains_key(&key) {
                log::info!(
                    "[Engine] Partition {}/{} is already persisting, skip",
                    key.table_name,
                    key.partition_name
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
                    log::info!(
                        "[Engine] Partition {}/{} not found",
                        key.table_name,
                        key.partition_name
                    );
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
            key.table_name,
            key.partition_name,
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
                        key_clone.table_name,
                        key_clone.partition_name,
                        persisted_ids.len()
                    );
                }
                Ok(Err(e)) => {
                    log::error!(
                        "[Engine] Partition {}/{} persist failed: {:?}",
                        key_clone.table_name,
                        key_clone.partition_name,
                        e
                    );
                }
                Err(e) => {
                    log::error!(
                        "[Engine] Partition {}/{} persist task panicked: {:?}",
                        key_clone.table_name,
                        key_clone.partition_name,
                        e
                    );
                }
            }

            // 持久化完成后立即从 active_tasks 中移除
            let mut tasks = active_tasks_clone.lock().await;
            tasks.remove(&key_clone);
            log::info!(
                "[Engine] Partition {}/{} removed from active tasks",
                key_clone.table_name,
                key_clone.partition_name
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
        active_tasks: &Arc<tokio::sync::Mutex<HashMap<PartitionKey, JoinHandle<()>>>>,
    ) {
        let partition_keys: Vec<PartitionKey> = {
            let parts = self.partitions.read().await;
            parts.keys().cloned().collect()
        };

        for key in partition_keys {
            self.handle_partition_persist(key, active_tasks).await;
        }
    }

    /// 执行 SQL 查询 (向后兼容方法)
    ///
    /// **推荐**: 新代码请直接使用 `DistributedExecutor`
    ///
    /// # 参数
    /// - `sql`: SQL 查询语句
    ///
    /// # 返回
    /// - `batches`: 查询结果的数据批次
    ///
    /// # 注意
    /// 此方法现在委托给 `DistributedExecutor`，保持向后兼容。
    /// 由于需要 Arc<Engine>，建议协议层直接使用 DistributedExecutor。
    ///
    /// 如果协议层需要总行数（如 ES 分页），应该：
    /// 1. 先执行 COUNT 查询获取总数
    /// 2. 再执行实际查询获取数据
    pub async fn execute_sql(
        self: &Arc<Self>,
        sql: &str,
    ) -> CoreResult<crate::compute::QueryResult> {
        use crate::compute::DistributedExecutor;

        let executor = DistributedExecutor::new(self.clone());
        executor.execute_sql(sql).await
    }

    /// 加载外部文件到 segment
    ///
    /// # 参数
    /// - `table_name`: 表名
    /// - `partition_name`: 分区名称（必须提供且符合目录名称规范）
    /// - `file_path`: 外部文件路径
    /// - `handler_type`: 文件处理类型
    ///
    /// # 返回
    /// 返回加载的文档数量
    pub async fn load_segment(
        &self,
        table_name: &str,
        partition_name: String,
        file_path: PathBuf,
        handler_type: Option<crate::segment_loader::FileHandlerType>,
    ) -> CoreResult<usize> {
        // 获取表的元数据
        let meta = self.catalog.get_table(table_name)?;

        // 验证 partition_name 符合目录名称规范
        let sanitized_name = PartitionStrategy::sanitize_filename(&partition_name);
        if sanitized_name != partition_name {
            return Err(crate::utils::error::CoreError::InvalidParam(format!(
                "Invalid partition_name '{}'. Must not contain special characters. Suggested: '{}'",
                partition_name, sanitized_name
            )));
        }


        let partition_dir =
            self.config.data_dir
                .join("tables")
                .join(table_name)
                .join("partitions")
                .join(crate::catalog::PartitionStrategy::generate_partition_dir_name(&partition_name));

        // 1. 判断 partition 是否存在：先检查内存引用，再检查目录
        let partition = if let Some(existing_partition) = self.get_partition(table_name, &partition_name).await {
            log::info!(
                "Partition {} already exists in memory for table {}",
                partition_name,
                table_name
            );
            existing_partition
        } else if partition_dir.exists() {
            // 目录存在但内存中没有，从磁盘加载
            log::info!(
                "Loading existing partition {} from disk for table {}",
                partition_name,
                table_name
            );
            let loaded_partition = Partition::load(
                partition_name.clone(),
                table_name.to_string(),
                partition_dir.clone(),
                meta.schema.clone(),
                self.partition_notify_tx.clone(),
            )?;
            let partition = Arc::new(loaded_partition);
            self.add_partition_with_table(table_name, partition.clone()).await;
            partition
        } else {
            // 不存在，创建新的 partition
            log::info!(
                "Creating new partition {} for table {}",
                partition_name,
                table_name
            );
            let new_partition = Partition::new(
                partition_name.clone(),
                table_name.to_string(),
                partition_dir,
                meta.schema.clone(),
                self.partition_notify_tx.clone(),
            );
            let partition = Arc::new(new_partition);
            self.add_partition_with_table(table_name, partition.clone()).await;
            partition
        };

        // 2. 检查文件是否已经被加载过
        // 获取文件的规范路径用于比较
        let file_canonical_path = file_path.canonicalize().map_err(|e| {
            crate::utils::error::CoreError::IOError(format!(
                "Failed to canonicalize file path {:?}: {}",
                file_path, e
            ))
        })?;

        // 检查所有 frozen segments 是否已经引用了这个文件
        {
            let frozen_segments = partition.get_frozen_segments();
            for (seg_id, segment) in frozen_segments.iter() {
                if let Some(segment_parquet_path) = segment.get_parquet_path() {
                    // 尝试规范化 segment 的路径进行比较
                    if let Ok(segment_canonical_path) = std::path::Path::new(segment_parquet_path).canonicalize() {
                        if segment_canonical_path == file_canonical_path {
                            return Err(crate::utils::error::CoreError::InvalidParam(format!(
                                "File {:?} has already been loaded into partition {} as segment {}",
                                file_path, partition_name, seg_id
                            )));
                        }
                    }
                }
            }
            // frozen_segments 在这里自动释放
        }

        log::info!(
            "File {:?} not yet loaded, proceeding to load into partition {}",
            file_path,
            partition_name
        );

        // 3. 使用 SegmentLoader 加载文件
        use crate::segment_loader::SegmentLoader;
        let loader = SegmentLoader::new(self.config.data_dir.clone());

        let doc_count = loader
            .create_segment_from_file(&partition, &file_path, handler_type)
            .await?;

        log::info!(
            "Loaded {} documents from {:?} into partition {} of table {}",
            doc_count,
            file_path,
            partition_name,
            table_name
        );

        // 加载完成后，触发持久化
        self.trigger_persist(table_name, &partition_name);

        Ok(doc_count)
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
