/// Table 元数据定义
use crate::schema::Schema;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;

/// 表的元数据
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableMeta {
    /// 表名
    pub table_name: String,

    /// Schema 信息
    pub schema: Schema,

    /// 分区策略
    pub partition_strategy: PartitionStrategy,

    /// 并行工作数（partition 数量）
    pub parallel_workers: usize,

    /// 工作目录
    pub work_dir: PathBuf,

    /// 创建时间（Unix 时间戳）
    pub created_at: u64,

    /// 更新时间（Unix 时间戳）
    pub updated_at: u64,
}

/// 分区策略
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PartitionStrategy {
    /// 哈希分区 - 根据字段值的哈希分配 partition
    Hash {
        /// 分区字段名
        field: String,
        /// partition 数量
        num_partitions: usize,
    },

    /// 范围分区 - 适合时序数据或有序数据
    Range {
        /// 分区字段名
        field: String,
        /// 范围列表 [(起始值, 结束值, partition_id)]
        ranges: Vec<RangePartition>,
    },

    /// 列表分区 - 适合枚举值
    List {
        /// 分区字段名
        field: String,
        /// 值到 partition 的映射
        values: HashMap<String, usize>,
    },

    /// 无分区 - 所有数据在一个 partition
    None,
}

/// 范围分区配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RangePartition {
    /// 起始值（包含）
    pub start: PartitionValue,
    /// 结束值（不包含）
    pub end: PartitionValue,
    /// partition ID
    pub partition_id: usize,
}

/// 分区值（支持常见类型）
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum PartitionValue {
    Int64(i64),
    UInt64(u64),
    String(String),
    MinValue, // 负无穷
    MaxValue, // 正无穷
}

/// Partition 的元数据
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionMeta {
    /// partition ID
    pub partition_id: usize,

    /// 包含的 segment 列表
    pub segments: Vec<SegmentInfo>,

    /// 创建时间
    pub created_at: u64,

    /// 最后更新时间
    pub updated_at: u64,

    /// 当前活跃的 segment ID
    pub active_segment_id: Option<u64>,
}

/// Segment 信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentInfo {
    /// Segment ID 范围起始
    pub start: u64,

    /// Segment ID 范围结束
    pub end: u64,

    /// 状态
    pub status: SegmentStatus,

    /// 大小（字节）
    pub size_bytes: u64,

    /// 文档数量
    pub doc_count: u64,

    /// 创建时间
    pub created_at: u64,

    /// 冻结时间（如果已冻结）
    pub frozen_at: Option<u64>,
}

/// Segment 状态
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SegmentStatus {
    /// 活跃的（可写入）
    Active,

    /// 已冻结（只读）
    Frozen,

    /// 已持久化到磁盘
    Persisted,
}

impl TableMeta {
    /// 创建新的表元数据
    pub fn new(
        table_name: String,
        schema: Schema,
        partition_strategy: PartitionStrategy,
        parallel_workers: usize,
        work_dir: PathBuf,
    ) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        Self {
            table_name,
            schema,
            partition_strategy,
            parallel_workers,
            work_dir,
            created_at: now,
            updated_at: now,
        }
    }

    /// 获取表的目录路径
    pub fn table_dir(&self) -> PathBuf {
        self.work_dir.join("tables").join(&self.table_name)
    }

    /// 获取 partition 目录路径
    pub fn partition_dir(&self, partition_id: usize) -> PathBuf {
        self.table_dir()
            .join("partitions")
            .join(format!("partition-{}", partition_id))
    }

    /// 获取 segment 目录路径
    pub fn segment_dir(&self, partition_id: usize, start: u64, end: u64) -> PathBuf {
        self.partition_dir(partition_id)
            .join("segments")
            .join(format!("segment-{}-{}", start, end))
    }
}

impl PartitionMeta {
    /// 创建新的 partition 元数据
    pub fn new(partition_id: usize) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        Self {
            partition_id,
            segments: Vec::new(),
            created_at: now,
            updated_at: now,
            active_segment_id: None,
        }
    }

    /// 添加新的 segment
    pub fn add_segment(&mut self, segment: SegmentInfo) {
        self.segments.push(segment);
        self.updated_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
    }

    /// 更新 segment 状态
    pub fn update_segment_status(&mut self, start: u64, end: u64, status: SegmentStatus) {
        if let Some(segment) = self
            .segments
            .iter_mut()
            .find(|s| s.start == start && s.end == end)
        {
            segment.status = status;
            if status == SegmentStatus::Frozen || status == SegmentStatus::Persisted {
                segment.frozen_at = Some(
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs(),
                );
            }
            self.updated_at = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();
        }
    }
}

impl SegmentInfo {
    /// 创建新的 segment 信息
    pub fn new(start: u64, end: u64) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        Self {
            start,
            end,
            status: SegmentStatus::Active,
            size_bytes: 0,
            doc_count: 0,
            created_at: now,
            frozen_at: None,
        }
    }
}
