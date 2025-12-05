//! 元数据管理模块 - 处理表和分区的元数据操作

use std::sync::Arc;

use crate::catalog::{PartitionStrategy, TableMeta};
use crate::partition::Partition;
use crate::schema::Schema;
use crate::utils::error::{CoreError, CoreResult};

use super::Engine;

impl Engine {
    /// 获取表的元数据
    pub fn get_table_meta(&self, table_name: &str) -> CoreResult<Arc<TableMeta>> {
        self.catalog.get_table(table_name)
    }

    /// 列出所有表
    pub fn list_tables(&self) -> Vec<String> {
        self.catalog.list_tables()
    }

    /// 获取表的 Schema（用于 INFORMATION_SCHEMA 查询）
    pub async fn get_table_schema(
        &self,
        table_name: &str,
    ) -> CoreResult<Arc<crate::schema::Schema>> {
        let table_meta = self.catalog.get_table(table_name)?;
        Ok(Arc::new(table_meta.schema.clone()))
    }

    /// 创建新表
    ///
    /// # 参数
    /// - `table_name`: 表名
    /// - `schema`: 表的 Schema
    /// - `partition_strategy`: 分区策略
    /// - `num_partitions`: 分区数量
    pub async fn create_table(
        &self,
        table_name: &str,
        schema: Schema,
        partition_strategy: PartitionStrategy,
        num_partitions: usize,
    ) -> CoreResult<()> {
        // 创建 TableMeta
        let meta = TableMeta::new(table_name.to_string(), schema.clone(), partition_strategy);

        // 在 Catalog 中创建表（会创建目录结构和元数据）
        self.catalog.create_table(meta)?;

        // 加载所有 partition 到内存
        log::debug!(
            "🔍 [DEBUG create_table] Creating {} partitions for table '{}'",
            num_partitions,
            table_name
        );

        let table_meta = self.catalog.get_table(table_name)?;

        // 生成所有 partition 名称（None 策略返回 None）
        let partition_names = match table_meta.partition_strategy.generate_partitions() {
            Some(names) => names,
            None => {
                // None 策略：单分区
                vec!["partition_000000000000000000".to_string()]
            }
        };

        for partition_name in partition_names {
            let partition_dir = self
                .config
                .data_dir
                .join("tables")
                .join(table_name)
                .join("partitions")
                .join(
                    crate::catalog::PartitionStrategy::generate_partition_dir_name(&partition_name),
                );

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
                (*self.partition_notify_tx).clone(),
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

    /// 删除表
    pub async fn drop_table(&self, table_name: &str) -> CoreResult<()> {
        // 直接扫描磁盘上的 partitions 子目录，不依赖分区策略
        let partitions_dir = self
            .config
            .data_dir
            .join("tables")
            .join(table_name)
            .join("partitions");

        let mut partition_names = Vec::new();
        if partitions_dir.exists() {
            if let Ok(entries) = std::fs::read_dir(&partitions_dir) {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if path.is_dir() {
                        if let Some(dir_name) = path.file_name().and_then(|n| n.to_str()) {
                            // 提取 partition-{name} 格式的目录
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

        // 移除所有相关的 partition
        for partition_name in partition_names {
            self.remove_partition(table_name, &partition_name).await;
        }

        // 从 catalog 中删除
        self.catalog.drop_table(table_name)?;
        Ok(())
    }

    /// 添加已存在的 Partition (带表名)
    pub async fn add_partition_with_table(&self, table_name: &str, partition: Arc<Partition>) {
        let partition_name = partition.name().to_string();
        let key = (table_name.to_string(), partition_name.clone());
        let mut partitions = self.partitions.write().await;
        partitions.insert(key.clone(), partition);

        log::debug!(
            "🔍 [DEBUG] Added partition: table={}, partition_name={}",
            table_name,
            partition_name
        );
        log::debug!("🔍 [DEBUG] Total partitions in map: {}", partitions.len());
        log::debug!("🔍 [DEBUG] Key inserted: {:?}", key);

        log::info!(
            "Added partition {} for table {}",
            partition_name,
            table_name
        );
    }

    /// 添加已存在的 Partition (兼容旧接口,已废弃)
    #[deprecated(note = "使用 add_partition_with_table 代替")]
    pub async fn add_partition(&self, partition: Arc<Partition>) {
        let partition_name = partition.name().to_string();
        let mut partitions = self.partitions.write().await;
        // 使用 partition_name 作为 table_name (向后兼容)
        let key = (
            format!("__legacy_{}", partition_name),
            partition_name.clone(),
        );
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
            (*self.partition_notify_tx).clone(),
        )?;

        let partition = Arc::new(partition);
        self.add_partition_with_table(table_name, partition.clone())
            .await;
        Ok(partition)
    }

    /// 移除 Partition
    pub async fn remove_partition(&self, table_name: &str, partition_name: &str) {
        let key = (table_name.to_string(), partition_name.to_string());
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
        let key = (table_name.to_string(), partition_name.to_string());
        let partitions = self.partitions.read().await;
        let result = partitions.get(&key).cloned();
        result
    }

    /// 列出表的所有 Partition IDs
    pub async fn list_partitions(&self, table_name: &str) -> Vec<String> {
        let partitions = self.partitions.read().await;
        partitions
            .keys()
            .filter(|k| k.0 == table_name)
            .map(|k| k.1.clone())
            .collect()
    }

    /// 列出所有 Partition Keys
    pub async fn list_all_partition_keys(&self) -> Vec<(String, String)> {
        let partitions = self.partitions.read().await;
        partitions
            .keys()
            .map(|k| (k.0.clone(), k.1.clone()))
            .collect()
    }

    /// 列出指定表/分区下的所有 segment 信息（包括 frozen 和 current）
    /// 返回: (segment_id, doc_count, created_ts_ms, is_current)
    pub async fn list_segments(
        &self,
        table_name: &str,
        partition_name: &str,
    ) -> Vec<(u64, u64, u64, bool)> {
        let key = (table_name.to_string(), partition_name.to_string());
        let partitions = self.partitions.read().await;
        if let Some(partition) = partitions.get(&key) {
            let mut infos = Vec::new();

            // frozen segments
            let frozen = partition.get_frozen_segments();
            for (_, seg) in frozen.iter() {
                // 估算绝对创建时间戳：当前时间戳减去 age
                let age = seg.created_since_start();
                let now_ts_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64;
                let created_ts_ms = now_ts_ms.saturating_sub(age.as_millis() as u64);
                infos.push((
                    seg.start,
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

    /// 创建新的 Partition（低级 API，通常不需要直接调用）
    /// 已废弃：使用带 table_name 的版本
    #[allow(dead_code)]
    #[deprecated(note = "需要提供 table_name")]
    pub async fn create_partition(&self, _id: u64, _schema: Schema) -> Arc<Partition> {
        panic!("create_partition is deprecated, use create_partition_with_table instead");
    }

    /// 根据分区策略路由到对应的 partition_name
    ///
    /// # 参数
    /// - `table_name`: 表名
    /// - `partition_value`: 分区字段的值（字符串格式）
    ///
    /// # 返回
    /// 返回应该使用的 partition_name
    ///
    /// # 注意
    /// 这是一个兼容性方法，新代码应该使用 Router::route_batch
    pub fn route_partition(&self, table_name: &str, partition_value: &str) -> CoreResult<String> {
        let meta = self.catalog.get_table(table_name)?;

        match &meta.partition_strategy {
            PartitionStrategy::PKHash { num_partitions }
            | PartitionStrategy::Hash { num_partitions, .. } => {
                // Hash 分区：对值进行 hash 然后取模
                use std::collections::hash_map::DefaultHasher;
                use std::hash::{Hash, Hasher};

                let mut hasher = DefaultHasher::new();
                partition_value.hash(&mut hasher);
                let hash = hasher.finish();

                let index = (hash % (*num_partitions as u64)) as usize;
                Ok(PartitionStrategy::format_partition_id(index as i64))
            }

            PartitionStrategy::Range { start, step, .. } => {
                // Range 分区：根据值计算所在的 partition_start
                let value = partition_value.parse::<i64>().map_err(|e| {
                    CoreError::Internal(format!(
                        "Failed to parse range value '{}': {}",
                        partition_value, e
                    ))
                })?;

                let offset = value - start;
                let partition_index = offset / step;
                let partition_start = start + (partition_index * step);

                Ok(PartitionStrategy::format_partition_id(partition_start))
            }

            PartitionStrategy::Custom => {
                // Custom 分区：用户自定义，直接使用 partition_value 作为 partition_name
                Ok(partition_value.to_string())
            }

            PartitionStrategy::None => {
                // 无分区策略，返回默认 partition
                Ok("partition_000000000000000000".to_string())
            }
        }
    }
}
