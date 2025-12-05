//! Engine 配置模块

use std::path::PathBuf;

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
