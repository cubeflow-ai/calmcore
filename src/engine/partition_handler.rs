//! 元数据管理模块 - 处理表和分区的元数据操作

use std::path::{Path, PathBuf};
use std::sync::Arc;

use datafusion::arrow::compute::kernels::partition;

use crate::catalog::{dir, table_meta, Catalog, PartitionStrategy, TableMeta};
use crate::partition::Partition;
use crate::schema::Schema;
use crate::utils::error::{CoreError, CoreResult};

use super::Engine;

impl Engine {
    /// ==============================================================local methods ==================================================

    pub async fn load_partition(
        &self,
        table_name: &str,
        partition_name: &str,
        partition_dir: PathBuf,
        schema: Schema,
    ) -> CoreResult<Arc<Partition>> {
        // 从文件系统读取表的元数据

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

    /// unload Partition
    pub async fn unload_partition(&self, table_name: &str, partition_name: &str) {
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

    pub async fn list_all_spartitions(&self) -> Vec<(String, String)> {
        let partitions = self.partitions.read().await;
        partitions.keys().cloned().collect()
    }

    /// 移除分区（用于删除表时清理本地数据）
    pub async fn remove_partition(&self, table_name: &str, partition_name: &str) {
        let key = (table_name.to_string(), partition_name.to_string());
        let mut partitions = self.partitions.write().await;
        if let Some(partition) = partitions.remove(&key) {
            drop(partitions); // 释放写锁
            log::info!(
                "🗑️  Removed partition '{}/{}' from memory",
                table_name,
                partition_name
            );
            // Partition 的 Drop trait 会自动清理资源
            drop(partition);
        }
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
}
