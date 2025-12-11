//! 元数据管理模块 - 处理表和分区的元数据操作

use std::sync::Arc;

use datafusion::arrow::compute::kernels::partition;

use crate::catalog::{dir, PartitionStrategy, TableMeta};
use crate::partition::Partition;
use crate::schema::Schema;
use crate::utils::error::{CoreError, CoreResult};

use super::Engine;

impl Engine {
    /// 获取表的元数据
    pub async fn get_table_meta(&self, table_name: &str) -> CoreResult<Arc<TableMeta>> {
        self.catalog.get_table(table_name)
    }

    /// 列出所有表
    pub fn list_tables(&self) -> Vec<String> {
        self.catalog.list_tables()
    }

    /// ==============================================================local methods ==================================================

    pub async fn local_load_partition(
        &self,
        table_name: &str,
        partition_name: &str,
    ) -> CoreResult<Arc<Partition>> {
        // 从文件系统读取表的元数据
        let table_meta = self.catalog.get_table(table_name)?;
        let schema = table_meta.schema.clone();

        let partition_dir = dir::partition_dir(&self.config.data_dir, table_name, partition_name);

        // 从磁盘加载分区
        let partition = Arc::new(Partition::load(
            partition_name.to_string(),
            table_name.to_string(),
            partition_dir,
            schema,
            (*self.partition_notify_tx).clone(),
        )?);

        // 添加到内存中
        self.partitions.write().await.insert(
            (table_name.to_string(), partition.name().to_string()),
            partition.clone(),
        );

        log::info!(
            "Added partition {} for table {}",
            partition_name,
            table_name
        );

        Ok(partition)
    }

    /// 移除 Partition
    pub async fn local_drop_partition(&self, table_name: &str, partition_name: &str) {
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

            PartitionStrategy::DatetimeRange { .. } => {
                // DatetimeRange 分区：期望传入时间戳（毫秒）
                let timestamp_ms = partition_value.parse::<i64>().map_err(|e| {
                    CoreError::Internal(format!(
                        "Failed to parse datetime value '{}' as timestamp: {}",
                        partition_value, e
                    ))
                })?;

                meta.partition_strategy
                    .calculate_datetime_partition(timestamp_ms)
                    .ok_or_else(|| {
                        CoreError::Internal(format!(
                            "Failed to calculate datetime partition for timestamp {}",
                            timestamp_ms
                        ))
                    })
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
