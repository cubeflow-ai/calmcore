/// Table 元数据定义
use crate::schema::Schema;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// 表的元数据
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableMeta {
    /// 表名
    pub table_name: String,

    /// Schema 信息
    pub schema: Schema,

    /// 分区策略
    pub partition_strategy: PartitionStrategy,

    /// 创建时间（Unix 时间戳）
    pub created_at: u64,

    /// 更新时间（Unix 时间戳）
    pub updated_at: u64,
}

/// 分区策略
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PartitionStrategy {
    /// 主键哈希分区（预创建，仅用于有主键的表）
    ///
    /// **约束**：只能用于有主键的表
    /// **语义**：按主键字段进行哈希分区
    ///
    /// partition 会按照 num_partitions 提前创建
    /// partition 命名：partition_0000000000000000000, partition_0000000000000000001, ...
    PKHash {
        /// partition 数量（预创建）
        num_partitions: usize,
    },

    /// 哈希分区（预创建，用于无主键的表）
    ///
    /// **约束**：只能用于无主键的表
    /// **语义**：按指定字段进行哈希分区
    ///
    /// partition 会按照 num_partitions 提前创建
    /// partition 命名：partition_0000000000000000000, partition_0000000000000000001, ...
    Hash {
        /// 分区字段名
        field: String,
        /// partition 数量（预创建）
        num_partitions: usize,
    },

    /// 范围分区（按需创建，适合数值范围数据）
    ///
    /// partition 不会提前创建，插入时按需创建
    /// partition 命名格式：partition_{start:019} 或 partition_{start:019}_{parallelism_index}
    ///
    /// 例如：start=0, step=1000000, parallelism=None
    ///   → partition_0000000000000000000 (范围: [0, 1000000))
    ///   → partition_0000000000001000000 (范围: [1000000, 2000000))
    ///
    /// 例如：start=0, step=1000000, parallelism=Some(2)
    ///   → partition_0000000000000000000_0 (范围: [0, 1000000), 并行索引 0)
    ///   → partition_0000000000000000000_1 (范围: [0, 1000000), 并行索引 1)
    ///   → partition_0000000000001000000_0 (范围: [1000000, 2000000), 并行索引 0)
    ///   → partition_0000000000001000000_1 (范围: [1000000, 2000000), 并行索引 1)
    Range {
        /// 分区字段名
        field: String,
        /// 起始位置（支持负数）
        start: i64,
        /// 步长（每个分区的范围大小）
        step: i64,
        /// 并行度（可选，用于提高单个 range 内的写入并行度）
        /// None 表示不启用并行，Some(n) 表示每个 range 创建 n 个子分区
        parallelism: Option<usize>,
    },

    /// 时间范围分区（按需创建，专为时序数据优化）
    ///
    /// partition 不会提前创建，插入时按需创建
    /// partition 命名格式：紧凑的数字格式，便于排序和识别
    ///
    /// 例如：granularity=Day, timezone=None
    ///   → partition_20240101 (本地时区 2024-01-01)
    ///   → partition_20240102 (本地时区 2024-01-02)
    ///
    /// 例如：granularity=Hour, timezone=Some("Asia/Shanghai"), parallelism=Some(2)
    ///   → partition_2024010108_0 (东八区 2024-01-01 08:00, 并行索引 0)
    ///   → partition_2024010108_1 (东八区 2024-01-01 08:00, 并行索引 1)
    ///
    /// 例如：granularity=Month, timezone=Some("UTC")
    ///   → partition_202401 (UTC 2024-01)
    ///   → partition_202402 (UTC 2024-02)
    DatetimeRange {
        /// 分区字段名（必须是 Timestamp 类型）
        field: String,
        /// 时间粒度
        granularity: TimeGranularity,
        /// 时区（可选，默认使用机器本地时区）
        /// 例如："UTC", "Asia/Shanghai", "America/New_York"
        /// None 表示使用本地时区
        timezone: Option<String>,
        /// 并行度（可选，用于提高单个时间段内的写入并行度）
        parallelism: Option<usize>,
    },

    /// 自定义分区（按需创建）
    ///
    /// partition 不会提前创建
    /// - 用户通过 LOAD DATA ... PARTITION(name) 指定
    /// - 或者 INSERT INTO ... PARTITION(name) 指定
    Custom,

    /// 无分区（单分区表，适合小表）
    ///
    /// 所有数据在一个分区：partition_0000000000000000000
    None,
}

/// 时间分区粒度
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TimeGranularity {
    /// 按年分区
    /// 格式：2024
    Year,
    /// 按月分区
    /// 格式：202401
    Month,
    /// 按周分区（ISO 8601 周编号）
    /// 格式：2024W01
    Week,
    /// 按天分区
    /// 格式：20240101
    Day,
    /// 按小时分区
    /// 格式：2024010108
    Hour,
}

impl PartitionStrategy {
    /// 获取路由字段名
    pub fn router_field(&self) -> Option<&String> {
        match self {
            PartitionStrategy::PKHash { .. } => None, // PKHash 使用主键，由调用方决定
            PartitionStrategy::Hash { field, .. } => Some(field),
            PartitionStrategy::Range { field, .. } => Some(field),
            PartitionStrategy::DatetimeRange { field, .. } => Some(field),
            PartitionStrategy::Custom => None,
            PartitionStrategy::None => None,
        }
    }

    /// 获取分区数量（仅用于预创建的策略）
    pub fn num_partitions(&self) -> Option<usize> {
        match self {
            PartitionStrategy::PKHash { num_partitions, .. } => Some(*num_partitions),
            PartitionStrategy::Hash { num_partitions, .. } => Some(*num_partitions),
            PartitionStrategy::Range { .. } => None, // 按需创建
            PartitionStrategy::DatetimeRange { .. } => None, // 按需创建
            PartitionStrategy::Custom => None,       // 按需创建
            PartitionStrategy::None => Some(1),      // 单分区
        }
    }

    /// 判断是否需要预创建分区
    pub fn should_precreate_partitions(&self) -> bool {
        matches!(
            self,
            PartitionStrategy::PKHash { .. }
                | PartitionStrategy::Hash { .. }
                | PartitionStrategy::None
        )
    }

    /// 生成需要预创建的 partition_name 列表
    ///
    /// 返回 None 表示按需创建（Range, Custom）
    pub fn generate_partitions(&self) -> Option<Vec<String>> {
        match self {
            PartitionStrategy::PKHash { num_partitions, .. } => {
                // PKHash 分区：预创建 num_partitions 个分区
                // 格式：0000000000000000000, 0000000000000000001, ...
                Some((0..*num_partitions).map(|i| format!("{:019}", i)).collect())
            }
            PartitionStrategy::Hash { num_partitions, .. } => {
                // Hash 分区：预创建 num_partitions 个分区
                // 格式：0000000000000000000, 0000000000000000001, ...
                Some((0..*num_partitions).map(|i| format!("{:019}", i)).collect())
            }
            PartitionStrategy::Range { .. } => {
                // Range 分区：按需创建
                None
            }
            PartitionStrategy::DatetimeRange { .. } => {
                // DatetimeRange 分区：按需创建
                None
            }
            PartitionStrategy::Custom => {
                // Custom 分区：按需创建
                None
            }
            PartitionStrategy::None => {
                // None 分区：单分区
                Some(vec![format!("{:019}", 0)])
            }
        }
    }

    /// 根据时间戳计算 DatetimeRange 分区名
    ///
    /// # 参数
    /// - `timestamp_millis`: Unix 时间戳（毫秒）
    ///
    /// # 返回
    /// 分区名，例如：20240101, 2024010108_0 等
    ///
    /// # 例子
    /// - granularity=Day, timezone=None → "20240101"
    /// - granularity=Hour, timezone=Some("UTC"), parallelism=Some(2) → "2024010108_0" 或 "2024010108_1"
    pub fn calculate_datetime_partition(&self, timestamp_millis: i64) -> Option<String> {
        match self {
            PartitionStrategy::DatetimeRange {
                granularity,
                timezone,
                parallelism,
                ..
            } => {
                use chrono::{Local, TimeZone, Utc};

                // 1. 将毫秒时间戳转换为秒和纳秒
                let secs = timestamp_millis / 1000;
                let nsecs = ((timestamp_millis % 1000) * 1_000_000) as u32;

                // 2. 根据时区创建 DateTime
                let partition_key = if let Some(tz_str) = timezone {
                    // 使用指定时区（目前只支持 UTC，未来可扩展 chrono-tz）
                    if tz_str == "UTC" {
                        let dt = Utc.timestamp_opt(secs, nsecs).single()?;
                        Self::format_datetime_partition(*granularity, &dt)
                    } else {
                        // 暂不支持其他时区，使用本地时区
                        log::warn!(
                            "Timezone '{}' not yet supported, using local timezone",
                            tz_str
                        );
                        let dt = Local.timestamp_opt(secs, nsecs).single()?;
                        Self::format_datetime_partition(*granularity, &dt)
                    }
                } else {
                    // 使用本地时区
                    let dt = Local.timestamp_opt(secs, nsecs).single()?;
                    Self::format_datetime_partition(*granularity, &dt)
                };

                // 3. 处理并行度
                if let Some(p) = parallelism {
                    if *p > 1 {
                        let parallel_idx = Self::hash_for_parallel(timestamp_millis, *p);
                        return Some(format!("{}_{}", partition_key, parallel_idx));
                    }
                }

                Some(partition_key)
            }
            _ => None,
        }
    }

    /// 格式化时间分区名（紧凑数字格式）
    fn format_datetime_partition<Tz: chrono::TimeZone>(
        granularity: TimeGranularity,
        dt: &chrono::DateTime<Tz>,
    ) -> String
    where
        Tz::Offset: std::fmt::Display,
    {
        use chrono::{Datelike, Timelike};
        match granularity {
            TimeGranularity::Year => {
                format!("{:04}", dt.year())
            }
            TimeGranularity::Month => {
                format!("{:04}{:02}", dt.year(), dt.month())
            }
            TimeGranularity::Week => {
                // ISO 8601 周编号
                format!("{:04}W{:02}", dt.iso_week().year(), dt.iso_week().week())
            }
            TimeGranularity::Day => {
                format!("{:04}{:02}{:02}", dt.year(), dt.month(), dt.day())
            }
            TimeGranularity::Hour => {
                format!(
                    "{:04}{:02}{:02}{:02}",
                    dt.year(),
                    dt.month(),
                    dt.day(),
                    dt.hour()
                )
            }
        }
    }

    /// 根据值计算并行索引（使用哈希）
    fn hash_for_parallel(value: i64, parallelism: usize) -> usize {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        // 结合时间戳和纳秒来生成随机性
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        now.hash(&mut hasher);
        value.hash(&mut hasher);

        let hash = hasher.finish();
        (hash as usize) % parallelism
    }

    /// 根据字段值计算 Range 分区名（随机选择并行分区）
    ///
    /// 例如：value=1500000, start=0, step=1000000, parallelism=None
    ///   → partition_0000000000001000000 (范围 [1000000, 2000000))
    ///
    /// 例如：value=1500000, start=0, step=1000000, parallelism=Some(2)
    ///   → partition_0000000000001000000_0 或 partition_0000000000001000000_1 (随机选择)
    pub fn calculate_range_partition(&self, value: i64) -> Option<String> {
        match self {
            PartitionStrategy::Range {
                start,
                step,
                parallelism,
                ..
            } => {
                if *step <= 0 {
                    return None;
                }

                // 计算 partition 的起始位置
                let offset = value - start;
                let partition_index = offset / step;
                let partition_start = start + (partition_index * step);

                // 格式化基础分区名
                let base_name = Self::format_partition_id(partition_start);

                // 如果启用了并行度，随机选择一个子分区
                if let Some(p) = parallelism {
                    if *p > 1 {
                        let parallel_index = Self::hash_for_parallel(value, *p);
                        return Some(format!("{}_{}", base_name, parallel_index));
                    }
                }

                // 未启用并行度，返回基础分区名
                Some(base_name)
            }
            _ => None,
        }
    }

    /// 格式化 partition ID（支持负数）
    ///
    /// 正数：0000000000000001000
    /// 负数：-000000000001000000
    pub fn format_partition_id(value: i64) -> String {
        if value >= 0 {
            format!("{:019}", value)
        } else {
            // 负数：符号 + 018 位数字
            format!("-{:018}", value.abs())
        }
    }

    /// 生成 partition 目录名
    ///
    /// 格式：partition_{partition_name}
    /// 例如：partition_0000000000000000001
    pub fn generate_partition_dir_name(partition_name: &str) -> String {
        format!("partition_{}", partition_name)
    }

    /// 从 partition 目录名中提取 partition_name
    ///
    /// 例如：从 "partition_0000000000000000001" 提取 "0000000000000000001"
    pub fn extract_partition_from_dir_name(dir_name: &str) -> Option<String> {
        dir_name.strip_prefix("partition_").map(|s| s.to_string())
    }
}

/// Partition 的元数据
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionMeta {
    /// partition ID
    pub partition_name: String,

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
    pub fn new(table_name: String, schema: Schema, partition_strategy: PartitionStrategy) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        Self {
            table_name,
            schema,
            partition_strategy,
            created_at: now,
            updated_at: now,
        }
    }

    /// 获取分区数量（如果可预知）
    pub fn num_partitions(&self) -> Option<usize> {
        self.partition_strategy.num_partitions()
    }

    /// 获取表的目录路径
    pub fn table_dir(&self, work_dir: &Path) -> PathBuf {
        work_dir.join("tables").join(&self.table_name)
    }

    /// 获取 partition 目录路径（使用 partition_name 字符串）
    pub fn partition_dir_by_name(&self, work_dir: &Path, partition_name: &str) -> PathBuf {
        let dir_name = PartitionStrategy::generate_partition_dir_name(partition_name);
        self.table_dir(work_dir).join("partitions").join(dir_name)
    }

    /// 获取 partition 目录路径（使用 partition_name 字符串）- 别名方法
    pub fn partition_dir_by_id(&self, work_dir: &Path, partition_name: &str) -> PathBuf {
        self.partition_dir_by_name(work_dir, partition_name)
    }

    /// 获取 segment 目录路径（使用字符串 partition_name
    pub fn segment_dir_by_id(
        &self,
        work_dir: &PathBuf,
        partition_name: &str,
        start: u64,
        end: u64,
    ) -> PathBuf {
        self.partition_dir_by_id(work_dir, partition_name)
            .join(format!("segment-{}-{}", start, end))
    }
}

impl PartitionMeta {
    /// 创建新的 partition 元数据
    pub fn new(partition_name: String) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        Self {
            partition_name,
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
