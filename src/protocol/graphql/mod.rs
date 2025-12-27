use std::sync::Arc;

use async_graphql::{Context, EmptySubscription, Json, Object, Result, Schema, SimpleObject};
use poem::{
    get, handler, listener::TcpListener, middleware::Cors, post, EndpointExt, Route, Server,
};
use serde_json::Value as JsonValue;

use crate::{
    calm::{CalmRpcService, CalmService, TableDetail},
    catalog::PartitionStrategy,
    schema::{field::FieldOption, PersistPolicy, Schema as CalmSchema},
};

/// GraphQL Schema for database operations
pub type CalmGraphQLSchema = Schema<QueryRoot, MutationRoot, EmptySubscription>;

/// 创建 GraphQL Schema
pub fn create_schema(service: Arc<CalmService>) -> CalmGraphQLSchema {
    Schema::build(QueryRoot, MutationRoot, EmptySubscription)
        .data(service)
        .finish()
}

// ===== 类型定义 =====

/// 字段类型枚举
///
/// Calm 支持 14 种数据类型,包括有符号/无符号整数、浮点数、布尔值、关键字和时间戳。
///
/// # MCP 提示
///
/// **推荐用法:**
/// - `KEYWORD` / `TEXT`: 文本、关键字、ID (支持数组和大小写敏感配置)
/// - `I32` / `INTEGER`: 标准整数 (-2^31 ~ 2^31-1)
/// - `I64` / `LONG`: 长整数 (-2^63 ~ 2^63-1)
/// - `U64`: 无符号长整数 (0 ~ 2^64-1), 适合 ID
/// - `F64` / `DOUBLE`: 浮点数,适合金额、比率
/// - `BOOLEAN` / `BOOL`: 布尔值
/// - `TIMESTAMP` / `DATETIME`: 时间戳(毫秒), 支持 iso8601 格式
///
/// **小整数优化:**
/// - `I8`: -128 ~ 127 (如年龄、评分)
/// - `U8`: 0 ~ 255 (如百分比、状态码)
/// - `I16`: -32768 ~ 32767 (如年份)
/// - `U16`: 0 ~ 65535 (如端口号)
///
/// **示例:**
/// ```graphql
/// fields: [
///   { name: "user_id", field_type: U64 }      # 用户 ID
///   { name: "age", field_type: I8 }           # 年龄 0-120
///   { name: "balance", field_type: F64 }      # 金额
///   { name: "active", field_type: BOOLEAN }   # 是否激活
///   { name: "created_at", field_type: TIMESTAMP, format: "iso8601" }
/// ]
/// ```
#[derive(async_graphql::Enum, Copy, Clone, Eq, PartialEq)]
pub enum FieldTypeEnum {
    /// 关键字/文本类型 - 适合短文本、ID、标签。支持数组和大小写配置
    Keyword,
    /// 文本类型 (Keyword 的别名) - 同 Keyword
    Text,
    /// 8位有符号整数 (-128 ~ 127) - 适合年龄、评分、百分比
    I8,
    /// 16位有符号整数 (-32768 ~ 32767) - 适合年份、数量
    I16,
    /// 32位有符号整数 (-2^31 ~ 2^31-1) - 标准整数类型
    I32,
    /// 整数 (I32 的别名) - 同 I32
    Integer,
    /// 64位有符号整数 (-2^63 ~ 2^63-1) - 适合大数值
    I64,
    /// 长整数 (I64 的别名) - 同 I64
    Long,
    /// 8位无符号整数 (0 ~ 255) - 适合百分比、状态码、端口范围
    U8,
    /// 16位无符号整数 (0 ~ 65535) - 适合端口号、计数
    U16,
    /// 32位无符号整数 (0 ~ 2^32-1) - 适合大计数
    U32,
    /// 64位无符号整数 (0 ~ 2^64-1) - 适合 ID、时间戳
    U64,
    /// 32位浮点数 - 适合精度要求不高的小数
    F32,
    /// 浮点数 (F32 的别名) - 同 F32
    Float,
    /// 64位浮点数 - 适合金额、比率、科学计算
    F64,
    /// 双精度浮点数 (F64 的别名) - 同 F64
    Double,
    /// 布尔类型 - true/false
    Boolean,
    /// 布尔类型别名 - 同 Boolean
    Bool,
    /// 时间戳类型 (毫秒级 Unix 时间戳) - 支持 iso8601/rfc3339 格式
    Timestamp,
    /// 时间类型 (Timestamp 的别名) - 同 Timestamp
    Datetime,
}

/// 表元数据(查询返回)
///
/// `table(name: String)` 查询的返回类型,包含表的基本信息。
///
/// # MCP 提示
///
/// **用途:**
/// - 查看表的基本结构和配置
/// - 检查表是否存在
/// - 获取字段列表和类型
///
/// **查询示例:**
/// ```graphql
/// query {
///   table(name: "users") {
///     name              # 表名
///     partition_count   # 分区数
///     primary_key       # 主键字段
///     fields {          # 字段列表
///       name
///       field_type
///       indexed
///     }
///   }
/// }
/// ```
#[derive(SimpleObject)]
pub struct Table {
    /// 表名
    pub name: String,
    /// 分区数量
    pub partition_count: u64,
    /// 字段列表
    pub fields: Vec<Field>,
    /// 主键字段名
    pub primary_key: Option<String>,
}

/// 字段元数据(查询返回)
///
/// 表示表中的单个字段及其配置信息。
///
/// # MCP 提示
///
/// **字段说明:**
/// - `name`: 字段名
/// - `field_type`: 字段类型字符串 ("keyword", "i64", "f64", "timestamp" 等)
/// - `indexed`: 是否建立索引 (true=可查询/排序, false=仅存储)
#[derive(SimpleObject)]
pub struct Field {
    /// 字段名
    pub name: String,
    /// 字段类型 ("keyword", "i64", "u64", "f64", "timestamp", "boolean" 等)
    pub field_type: String,
    /// 是否建立索引
    pub indexed: bool,
}

/// 分区策略类型枚举
///
/// Calm 支持 4 种分区策略,用于数据的物理分布和查询优化。
///
/// # MCP 提示
///
/// **选择建议:**
/// - `HASH`: 默认选择,适合 ID 类字段,数据均衡分布
/// - `RANGE`: 适合时序数据(时间戳)、有序数据(日期、金额)
/// - `NONE`: 小表(<100万行)、测试环境
/// - `CUSTOM`: 特殊业务需求,需手动管理
///
/// **示例:**
/// ```graphql
/// # Hash 分区 - 推荐用于 ID
/// partition_strategy: {
///   strategy_type: HASH
///   field: "user_id"
///   num_partitions: 4
/// }
///
/// # Range 分区 - 推荐用于时间
/// partition_strategy: {
///   strategy_type: RANGE
///   field: "timestamp"
///   ranges: [
///     { partition_id: 0, start: {int_value: 0}, end: {int_value: 1704067200000} }
///     { partition_id: 1, start: {int_value: 1704067200000}, end: {int_value: 1735689600000} }
///   ]
/// }
///
/// # 无分区 - 小表
/// partition_strategy: {
///   strategy_type: NONE
/// }
/// ```
#[derive(async_graphql::Enum, Copy, Clone, Eq, PartialEq)]
pub enum PartitionStrategyType {
    /// 主键哈希分区 - 仅用于有主键的表，按主键哈希分区。需要 num_partitions
    PKHash,
    /// 哈希分区 - 基于字段哈希值均匀分布,适合 ID 类字段。需要 field 和 num_partitions
    Hash,
    /// 范围分区 - 基于数值范围划分,适合数值类数据。需要 field、range_start 和 range_step
    Range,
    /// 时间范围分区 - 专为时序数据优化,按时间粒度划分。需要 field、time_granularity 和可选的 timezone
    DatetimeRange,
    /// 自定义分区 - 用户手动管理分区元数据,适合特殊需求
    Custom,
    /// 无分区 - 所有数据在单分区,适合小表(<100万行)
    None,
}

/// 时间分区粒度
///
/// 定义时间分区的时间粒度,用于 DatetimeRange 策略。
///
/// # 说明
/// - Year: 按年分区 (格式: 2024)
/// - Month: 按月分区 (格式: 202401)
/// - Week: 按周分区,使用 ISO 8601 周编号 (格式: 2024W01)
/// - Day: 按天分区 (格式: 20240101)
/// - Hour: 按小时分区 (格式: 2024010108)
#[derive(async_graphql::Enum, Copy, Clone, Eq, PartialEq)]
pub enum TimeGranularityType {
    /// 按年分区 - 格式: 2024
    Year,
    /// 按月分区 - 格式: 202401
    Month,
    /// 按周分区 - 格式: 2024W01 (ISO 8601 周编号)
    Week,
    /// 按天分区 - 格式: 20240101
    Day,
    /// 按小时分区 - 格式: 2024010108
    Hour,
}

/// 分区值枚举(用于 Range 分区)
///
/// 定义 Range 分区的边界值,支持整数、无符号整数和字符串。
///
/// # MCP 提示
///
/// **三选一填写:**
/// - `int_value`: 有符号整数 (如负数、普通整数)
/// - `uint_value`: 无符号整数 (如时间戳、ID)
/// - `string_value`: 字符串 (如日期字符串)
///
/// **时间戳示例(毫秒):**
/// ```graphql
/// start: { int_value: 1704067200000 }  # 2024-01-01 00:00:00 UTC
/// end: { int_value: 1735689600000 }    # 2025-01-01 00:00:00 UTC
/// ```
///
/// **字符串示例:**
/// ```graphql
/// start: { string_value: "2024-01-01" }
/// end: { string_value: "2025-01-01" }
/// 分区策略配置
///
/// 定义数据如何在多个分区中分布,影响查询性能和并行度。
///
/// # MCP 提示
///
/// **Hash 分区(推荐):**
/// ```graphql
/// partition_strategy: {
///   strategy_type: HASH
///   field: "user_id"        # 分区字段(通常是主键)
///   num_partitions: 4       # 分区数量(建议 2-16)
/// }
/// ```
/// - 适合: ID 类字段(user_id, order_id)
/// - 优点: 数据均衡,查询并行度高
/// - 建议: 分区数 = CPU 核心数 或 2^n
///
/// **Range 分区:**
/// ```graphql
/// partition_strategy: {
///   strategy_type: RANGE
///   field: "created_at"     # 时间戳字段
///   ranges: [
///     {
///       partition_id: 0
///       start: { int_value: 0 }
///       end: { int_value: 1704067200000 }  # 2024-01-01
///     }
///     {
///       partition_id: 1
///       start: { int_value: 1704067200000 }
///       end: { int_value: 9999999999999 }
///     }
///   ]
/// }
/// ```
/// - 适合: 时序数据(日志、订单)
/// - 优点: 范围查询快(WHERE date BETWEEN ...)
/// - 建议: 按月/周/日划分
///
/// **DatetimeRange 分区(时序数据优化):**
/// ```graphql
/// partition_strategy: {
///   strategy_type: DATETIMERANGE
///   field: "event_time"           # Timestamp 类型字段
///   time_granularity: DAY          # 按天分区
///   timezone: "UTC"                # 可选: UTC 或本地时区
///   datetime_parallelism: 2        # 可选: 每天创建 2 个并行分区
/// }
/// ```
/// - 适合: 时间序列数据(日志、监控、IoT)
/// - 优点: 按需创建、紧凑命名(20240101)、时区支持
/// - 粒度: YEAR/MONTH/WEEK/DAY/HOUR
/// - 建议: 根据数据量选择粒度(大数据量用 HOUR, 小数据量用 DAY/MONTH)
///
/// **None 分区(小表):**
/// ```graphql
/// partition_strategy: {
///   strategy_type: NONE
/// }
/// # 或直接使用: partition_count: 1
/// ```
/// - 适合: <100万行的表
/// - 优点: 简单,无分区开销

// ===== OneofObject 分区策略配置 =====

/// PKHash 分区配置 - 基于主键哈希分区
///
/// 使用主键的哈希值均匀分布数据到多个分区。
///
/// # 示例
/// ```graphql
/// partitionStrategy: {
///   pkHash: {
///     numPartitions: 4
///   }
/// }
/// ```
#[derive(async_graphql::InputObject)]
pub struct PKHashPartitionConfig {
    /// 分区数量 - 建议 2-16,推荐 CPU 核心数或 2 的幂次
    pub num_partitions: u64,
}

/// Hash 分区配置 - 基于指定字段哈希分区
///
/// 使用指定字段的哈希值均匀分布数据,适合 ID 类字段。
///
/// # 示例
/// ```graphql
/// partitionStrategy: {
///   hash: {
///     field: "user_id"
///     numPartitions: 8
///   }
/// }
/// ```
#[derive(async_graphql::InputObject)]
pub struct HashPartitionConfig {
    /// 用于分区的字段名 - 通常是 ID 类字段
    pub field: String,
    /// 分区数量 - 建议 2-16,推荐 CPU 核心数或 2 的幂次
    pub num_partitions: u64,
}

/// Range 分区配置 - 基于数值范围分区
///
/// 将数值字段按范围划分到不同分区,适合时序数据。
///
/// # 示例
/// ```graphql
/// partitionStrategy: {
///   range: {
///     field: "timestamp"
///     start: 0
///     step: 86400000  # 1天的毫秒数
///     numPartitions: 30
///     parallelism: 2   # 可选,每个范围内2个子分区
///   }
/// }
/// ```
#[derive(async_graphql::InputObject)]
pub struct RangePartitionConfig {
    /// 用于分区的字段名 - 通常是时间戳或递增ID
    pub field: String,
    /// 起始值 - 支持负数,默认 0
    pub start: i64,
    /// 步长 - 每个分区的范围大小
    pub step: i64,
    /// 分区数量
    pub num_partitions: u64,
    /// 并行度 - 每个 range 内创建多个子分区以提高并行写入性能。默认 1
    #[graphql(default = 1)]
    pub parallelism: u64,
}

/// DatetimeRange 分区配置 - 时序数据优化分区
///
/// 专为时间序列数据设计,按时间粒度自动创建分区。
///
/// # 示例
/// ```graphql
/// partitionStrategy: {
///   datetimeRange: {
///     field: "event_time"
///     granularity: DAY
///     timezone: "UTC"       # 可选,默认 UTC
///     parallelism: 2        # 可选,默认 1
///   }
/// }
/// ```
#[derive(async_graphql::InputObject)]
pub struct DatetimeRangePartitionConfig {
    /// 用于分区的时间字段名 - 必须是 Timestamp 类型
    pub field: String,
    /// 时间粒度 - YEAR(2024)/MONTH(202401)/WEEK(2024W01)/DAY(20240101)/HOUR(2024010108)
    pub granularity: TimeGranularityType,
    /// 时区 - 支持 "UTC"(默认) 或 "Local"
    #[graphql(default = "UTC")]
    pub timezone: String,
    /// 并行度 - 每个时间段内创建多个子分区以提高并行写入性能。默认 1
    #[graphql(default = 1)]
    pub parallelism: u64,
}

/// Custom 分区配置 - 用户自定义分区逻辑
///
/// 允许用户手动管理分区元数据,适合特殊需求。
///
/// # 示例
/// ```graphql
/// partitionStrategy: {
///   custom: {
///     expression: "custom_logic"
///   }
/// }
/// ```
#[derive(async_graphql::InputObject)]
pub struct CustomPartitionConfig {
    /// 自定义分区表达式
    pub expression: String,
}

/// None 分区配置 - 无分区(所有数据在单分区)
///
/// 适合小表(<100万行),无分区开销。
///
/// # 示例
/// ```graphql
/// partitionStrategy: {
///   none: { enabled: true }
/// }
/// ```
#[derive(async_graphql::InputObject)]
pub struct NonePartitionConfig {
    /// 是否启用 None 分区 - 必须为 true
    #[graphql(default = true)]
    pub enabled: bool,
}

/// 分区策略配置 (OneofObject)
///
/// 使用 GraphQL OneOf 模式,确保只能指定一种分区策略。
/// 每种策略都有明确的必填字段,避免配置错误。
///
/// # MCP 提示
///
/// **PKHash 分区(推荐 - 有主键):**
/// ```graphql
/// partitionStrategy: {
///   pkHash: { numPartitions: 4 }
/// }
/// ```
/// - 适合: 有主键的表,自动基于主键哈希分区
/// - 优点: 配置最简单,数据均衡
/// - 建议: 分区数 = CPU 核心数 或 2^n
///
/// **Hash 分区(推荐 - 无主键):**
/// ```graphql
/// partitionStrategy: {
///   hash: {
///     field: "user_id"
///     numPartitions: 8
///   }
/// }
/// ```
/// - 适合: 无主键,基于某个 ID 字段分区
/// - 优点: 数据均衡,查询并行度高
/// - 建议: 选择基数高的字段(如 user_id, order_id)
///
/// **Range 分区:**
/// ```graphql
/// partitionStrategy: {
///   range: {
///     field: "created_at"
///     start: 1704067200000  # 2024-01-01 00:00:00 UTC
///     step: 86400000        # 1天
///     numPartitions: 365
///     parallelism: 1
///   }
/// }
/// ```
/// - 适合: 时序数据,需要手动控制范围
/// - 优点: 范围查询快(WHERE date BETWEEN ...)
/// - 建议: 按月/周/日划分,根据查询模式调整
///
/// **DatetimeRange 分区(时序数据优化):**
/// ```graphql
/// partitionStrategy: {
///   datetimeRange: {
///     field: "event_time"
///     granularity: DAY
///     timezone: "UTC"
///     parallelism: 2
///   }
/// }
/// ```
/// - 适合: 时间序列数据(日志、监控、IoT)
/// - 优点: 按需创建、紧凑命名(20240101)、时区支持
/// - 粒度: YEAR/MONTH/WEEK/DAY/HOUR
/// - 建议: 根据数据量选择粒度(大数据量用 HOUR, 小数据量用 DAY/MONTH)
///
/// **None 分区(小表):**
/// ```graphql
/// partitionStrategy: {
///   none: {}
/// }
/// ```
/// - 适合: <100万行的表
/// - 优点: 简单,无分区开销
#[derive(async_graphql::OneofObject)]
pub enum PartitionStrategyInput {
    /// PKHash 分区 - 基于主键哈希,适合有主键的表
    PkHash(PKHashPartitionConfig),
    /// Hash 分区 - 基于指定字段哈希,适合 ID 类字段
    Hash(HashPartitionConfig),
    /// Range 分区 - 基于数值范围,适合时序数据
    Range(RangePartitionConfig),
    /// DatetimeRange 分区 - 时序数据优化,按时间粒度自动创建
    DatetimeRange(DatetimeRangePartitionConfig),
    /// Custom 分区 - 用户自定义分区逻辑
    Custom(CustomPartitionConfig),
    /// None 分区 - 无分区,所有数据在单分区
    None(NonePartitionConfig),
}
/// 持久化策略配置
///
/// 控制内存段(Segment)何时刷新到磁盘,影响性能和数据持久性。
///
/// # MCP 提示
///
/// **默认值(适合大多数场景):**
/// - `max_docs_per_segment`: 100,000 文档
/// - `max_segment_age_secs`: 300 秒 (5分钟)
///
/// **场景建议:**
/// - **高吞吐写入**(日志、监控): max_docs=500000, max_age=600(10分钟)
/// - **实时查询**(订单、交易): max_docs=50000, max_age=60(1分钟)
/// - **小数据量**(配置表): max_docs=10000, max_age=300
///
/// **示例:**
/// ```graphql
/// persist_policy: {
///   max_docs_per_segment: 100000    # 10万文档持久化
///   max_segment_age_secs: 300       # 5分钟持久化
/// }
/// ```
#[derive(async_graphql::InputObject)]
pub struct PersistPolicyInput {
    /// 文档数阈值 - 段内文档数达到此值触发持久化。默认 100000。增大可提高写入吞吐,但占用更多内存
    pub max_docs_per_segment: Option<u32>,

    /// 时间阈值(秒) - 段存活超过此时间触发持久化。默认 300(5分钟)。减小可降低数据丢失风险
    pub max_segment_age_secs: Option<u64>,
}

/// 创建表输入参数
///
/// 用于 `createTable` mutation 的输入对象,定义表的完整结构。
///
/// # MCP 提示
///
/// **必填字段:**
/// - `name`: 表名 (小写字母、数字、下划线)
/// - `fields`: 字段列表 (至少 1 个字段)
///
/// **推荐配置:**
/// - `description`: 表描述,帮助团队理解
/// - `primary_key`: 主键字段,用于去重和分区
/// - `partition_strategy`: 分区配置（不指定则默认 None）
/// - `store_source`: 是否存储原始 JSON (默认 true)
///
/// **快速开始模板 (PKHash 分区):**
/// ```graphql
/// mutation {
///   createTable(input: {
///     name: "users"                          # 表名
///     description: "用户信息表"               # 表描述
///     primary_key: "user_id"                 # 主键
///     partitionStrategy: {                   # PKHash 分区 (最简单)
///       pkHash: { numPartitions: 4 }
///     }
///     
///     fields: [                              # 字段定义
///       {
///         name: "user_id"
///         field_type: U64
///         description: "用户 ID"
///         nullable: false                    # 必填
///       }
///       {
///         name: "username"
///         field_type: KEYWORD
///         description: "用户名"
///         case_sensitive: false              # 不区分大小写
///         nullable: false
///       }
///       {
///         name: "age"
///         field_type: I8
///         description: "年龄"
///         default_value: "18"                # 默认值
///         nullable: true                     # 可选
///       }
///     ]
///   }) {
///     name
///   }
/// }
/// ```
///
/// **Hash 分区模板 (无主键表):**
/// ```graphql
/// mutation {
///   createTable(input: {
///     name: "events"
///     description: "事件表,按 user_id 哈希分区"
///     
///     partitionStrategy: {
///       hash: {
///         field: "user_id"      # 分区字段
///         numPartitions: 8      # 分区数量
///       }
///     }
///     
///     fields: [...]
///   }) { name }
/// }
/// ```
///
/// **时序数据模板 (DatetimeRange 分区):**
/// ```graphql
/// mutation {
///   createTable(input: {
///     name: "logs"
///     description: "日志表,按天分区"
///     primary_key: "log_id"
///     
///     partitionStrategy: {
///       datetimeRange: {
///         field: "timestamp"       # 时间字段
///         granularity: DAY         # 按天分区
///         timezone: "UTC"          # 可选,默认 UTC
///         parallelism: 2           # 可选,默认 1
///       }
///     }
///     
///     persist_policy: {
///       max_docs_per_segment: 500000         # 50万文档持久化
///       max_segment_age_secs: 600            # 10分钟持久化
///     }
///     
///     fields: [...]
///   }) { name }
/// }
/// ```
///
/// **Range 分区模板 (手动控制范围):**
/// ```graphql
/// mutation {
///   createTable(input: {
///     name: "orders"
///     description: "订单表,按时间戳范围分区"
///     primary_key: "order_id"
///     
///     partitionStrategy: {
///       range: {
///         field: "created_at"
///         start: 1704067200000       # 2024-01-01 00:00:00 UTC
///         step: 86400000             # 1天 (毫秒)
///         numPartitions: 365         # 365 个分区
///         parallelism: 1             # 可选,默认 1
///       }
///     }
///     
///     fields: [...]
///   }) { name }
/// }
/// ```
#[derive(async_graphql::InputObject)]
pub struct CreateTableInput {
    /// 表名 - 只能包含小写字母、数字和下划线,建议使用蛇形命名(如 user_orders)
    pub name: String,

    /// 主键字段名 - 用于去重和作为默认分区键。如果不指定,第一个字段将作为主键
    pub primary_key: Option<String>,

    /// 表描述/注释 - 帮助团队理解表用途和业务含义,强烈建议填写
    pub description: Option<String>,

    /// 分区策略 - 定义数据如何分布。不指定则默认为 None (无分区)
    pub partition_strategy: Option<PartitionStrategyInput>,

    /// 字段列表 - 定义表的列结构,至少需要 1 个字段
    pub fields: Vec<FieldInput>,

    /// 是否存储原始 JSON 数据 - true 则可以用 SELECT * 返回完整文档。默认 true
    pub store_source: Option<bool>,

    /// 持久化策略配置 - 控制何时将内存段刷新到磁盘。不指定则使用默认值
    pub persist_policy: Option<PersistPolicyInput>,
}

/// 字段输入参数
///
/// 定义表中的单个字段(列)及其配置。
///
/// # MCP 提示
///
/// **必填字段:**
/// - `name`: 字段名 (小写字母、数字、下划线)
/// - `field_type`: 字段类型 (参考 FieldTypeEnum)
///
/// **推荐配置:**
/// - `description`: 字段含义和用途
/// - `nullable`: 是否必填 (false=必填, true=可选)
/// - `indexed`: 是否索引 (true=可查询, false=仅存储)
/// - `default_value`: 默认值,插入时未提供则自动填充
///
/// **字段模板:**
/// ```graphql
/// # ID 字段
/// {
///   name: "user_id"
///   field_type: U64
///   description: "用户唯一标识符"
///   indexed: true      # 必须索引,用于查询
///   nullable: false    # 必填
/// }
///
/// # 文本字段(不区分大小写)
/// {
///   name: "email"
///   field_type: KEYWORD
///   description: "用户邮箱"
///   indexed: true
///   case_sensitive: false   # 不区分大小写
///   nullable: false
/// }
///
/// # 数组字段
/// {
///   name: "tags"
///   field_type: KEYWORD
///   description: "用户标签列表"
///   indexed: true
///   is_array: true          # 数组类型
///   nullable: true          # 可选
/// }
///
/// # 整数字段(带默认值)
/// {
///   name: "age"
///   field_type: I8
///   description: "用户年龄"
///   indexed: true
///   default_value: "18"     # 默认 18
///   nullable: true
/// }
///
/// # 时间戳字段
/// {
///   name: "created_at"
///   field_type: TIMESTAMP
///   description: "创建时间"
///   indexed: true
///   format: "iso8601"       # ISO 8601 格式
///   nullable: false
/// }
/// ```
#[derive(async_graphql::InputObject)]
pub struct FieldInput {
    /// 字段名 - 只能包含小写字母、数字和下划线。系统会自动转为小写
    pub name: String,

    /// 字段类型 - 参考 FieldTypeEnum 选择合适的类型
    pub field_type: FieldTypeEnum,

    /// 是否建立索引 - true=可查询(WHERE/ORDER BY), false=仅存储。默认 true
    pub indexed: Option<bool>,

    /// 字段描述/注释 - 解释字段含义、单位、范围等,帮助团队理解
    pub description: Option<String>,

    /// 默认值(JSON 字符串格式) - INSERT 时未提供值则使用默认值。示例: "0", "\"unknown\"", "true"
    pub default_value: Option<String>,

    /// 是否可为空 - false=必填(NOT NULL), true=可选。默认 true
    pub nullable: Option<bool>,

    // ===== Keyword 特定配置 =====
    /// 是否区分大小写(仅 Keyword) - false=查询和存储都转小写, true=保持原样。默认 true
    pub case_sensitive: Option<bool>,

    /// 是否为数组类型(仅 Keyword) - true=存储字符串数组, false=单个字符串。默认 false
    pub is_array: Option<bool>,

    // ===== Timestamp 特定配置 =====
    /// 时间格式(仅 Timestamp) - "iso8601"(推荐), "rfc3339" 或自定义格式。默认接受数值时间戳
    pub format: Option<String>,
}

/// 插入结果(返回)
///
/// `insertData` mutation 的返回类型,指示插入操作的结果。
///
/// # MCP 提示
///
/// **字段说明:**
/// - `success`: 是否成功 (true=全部成功, false=全部或部分失败)
/// - `rows_inserted`: 实际插入的文档数
/// - `message`: 详细消息或错误信息
///
/// **示例:**
/// ```graphql
/// mutation {
///   insertData(input: {
///     table: "users"
///     data: [{"user_id": 1, "name": "Alice"}]
///   }) {
///     success
///     rows_inserted
///     message
///   }
/// }
/// # 返回: { success: true, rows_inserted: 1, message: "OK" }
/// ```
#[derive(SimpleObject)]
pub struct InsertResult {
    /// 是否成功
    pub success: bool,
    /// 实际插入的行数
    pub rows_inserted: usize,
    /// 详细消息
    pub message: String,
}

/// SQL 查询结果(返回)
///
/// `query(sql: String)` 查询的返回类型,包含查询结果集。
///
/// # MCP 提示
///
/// **字段说明:**
/// - `columns`: 列名数组 ([“user_id”, “name”, “age”])
/// - `rows`: 数据行数组 (JSON 对象数组)
/// - `total_rows`: 总行数
///
/// **查询示例:**
/// ```graphql
/// query {
///   query(sql: "SELECT * FROM users WHERE age > 18 LIMIT 10") {
///     columns
///     rows
///     total_rows
///   }
/// }
/// # 返回: {
/// #   columns: ["user_id", "name", "age"],
/// #   rows: [{"user_id": 1, "name": "Alice", "age": 25}],
/// #   total_rows: 1
/// # }
/// ```
#[derive(SimpleObject)]
pub struct QueryResult {
    /// 列名数组
    pub columns: Vec<String>,
    /// 数据行 (JSON 数组)
    pub rows: Vec<JsonValue>,
    /// 总行数
    pub total_rows: usize,
}

/// 分区信息(返回)
///
/// 包含单个分区的详细状态,用于 `partitions` 查询和 `tableDetail` 查询。
///
/// # MCP 提示
///
/// **字段说明:**
/// - `partition_id`: 分区标识符 (Hash: "0", "1"; Range: "2024-01")
/// - `segment_count`: 该分区下的段数量 (frozen + current)
/// - `segments`: 段的详细信息数组
///
/// **使用场景:**
/// - 诊断分区数据分布是否均衡
/// - 检查分区下有多少段
/// - 查看各段的持久化状态
#[derive(SimpleObject)]
pub struct PartitionInfo {
    /// 分区 ID
    pub partition_id: String,
    /// 段数量
    pub segment_count: usize,
    /// 段详情列表
    pub segments: Vec<SegmentInfo>,
}

/// 段信息(返回)
///
/// 包含单个 Segment 的详细状态和统计信息。
///
/// # MCP 提示
///
/// **字段说明:**
/// - `segment_id`: 段 ID (0=当前活跃段, >0=冻结段)
/// - `doc_count`: 文档总数 (包含已删除)
/// - `deleted_count`: 已删除文档数
/// - `is_persisted`: 是否已持久化到磁盘
/// - `base_path`: 段的磁盘目录 (已持久化的段)
/// - `is_external_reference`: 是否外部 Parquet 文件 (loadSegment 导入)
/// - `external_data_path`: 外部 Parquet 文件路径
///
/// **实际文档数 = doc_count - deleted_count**
#[derive(SimpleObject)]
pub struct SegmentInfo {
    /// 段 ID (0=当前段)
    pub segment_id: u64,
    /// 文档总数
    pub doc_count: u32,
    /// 已删除文档数
    pub deleted_count: u64,
    /// 是否已持久化
    pub is_persisted: bool,
    /// 持久化目录
    pub base_path: Option<String>,
    /// 是否外部 Parquet
    pub is_external_reference: bool,
    /// 外部文件路径
    pub external_data_path: Option<String>,
}

#[derive(async_graphql::InputObject)]
pub struct InsertDataInput {
    /// 表名
    pub table: String,
    /// JSON 数据数组
    pub data: Vec<JsonValue>,
    /// 可选:指定分区名称(跳过路由策略),如果分区不存在则自动创建
    pub partition: Option<String>,
}

/// 文件处理类型枚举
#[derive(async_graphql::Enum, Copy, Clone, Eq, PartialEq)]
pub enum FileHandlerTypeEnum {
    /// 引用文件路径（不移动原文件）
    Reference,
    /// 移动文件
    Move,
    /// 拷贝文件
    Copy,
}

impl From<FileHandlerTypeEnum> for crate::segment_loader::FileHandlerType {
    fn from(val: FileHandlerTypeEnum) -> Self {
        match val {
            FileHandlerTypeEnum::Reference => crate::segment_loader::FileHandlerType::Reference,
            FileHandlerTypeEnum::Move => crate::segment_loader::FileHandlerType::Move,
            FileHandlerTypeEnum::Copy => crate::segment_loader::FileHandlerType::Copy,
        }
    }
}

#[derive(async_graphql::InputObject)]
pub struct LoadSegmentInput {
    /// 表名
    pub table: String,
    /// 分区名称（必须提供，且符合目录名称规范）
    pub partition_name: String,
    /// 文件路径（支持 .parquet 和 .jsonl 格式）
    pub file_path: String,
    /// 文件处理类型（Parquet 必需：REFERENCE/MOVE/COPY，JSONL 不需要）
    pub handler_type: Option<FileHandlerTypeEnum>,
}

/// 加载段结果(返回)
///
/// `loadSegment` mutation 的返回类型,指示 Parquet/JSONL 文件加载结果。
///
/// # MCP 提示
///
/// **字段说明:**
/// - `success`: 是否成功加载
/// - `documents_loaded`: 加载的文档数量
/// - `partition_name`: 目标分区名
/// - `message`: 详细消息或错误信息
///
/// **使用场景:**
/// - 批量导入 Parquet 文件
/// - 加载历史数据
/// - 数据迁移
///
/// **示例:**
/// ```graphql
/// mutation {
///   loadSegment(input: {
///     table: "taxi_trips"
///     partition_name: "p0"
///     file_path: "/data/trips_2024.parquet"
///     handler_type: REFERENCE
///   }) {
///     success
///     documents_loaded
///     message
///   }
/// }
/// # 返回: { success: true, documents_loaded: 1000000, message: "OK" }
/// ```
#[derive(SimpleObject)]
pub struct LoadSegmentResult {
    /// 是否成功
    pub success: bool,
    /// 加载的文档数
    pub documents_loaded: usize,
    /// 分区名称
    pub partition_name: String,
    /// 详细消息
    pub message: String,
}

// ===== Query Root =====

pub struct QueryRoot;

#[Object]
impl QueryRoot {
    /// 列出所有表
    ///
    /// 返回当前实例中的所有表名列表。
    ///
    /// # MCP 提示
    ///
    /// **用途:** 发现数据库中有哪些表
    ///
    /// **示例:**
    /// ```graphql
    /// query {
    ///   tables
    /// }
    /// # 返回: ["users", "orders", "products"]
    /// ```
    async fn tables(&self, ctx: &Context<'_>) -> Result<Vec<String>> {
        let service = ctx.data::<Arc<CalmService>>()?.clone();
        Ok(
            CalmRpcService::list_tables(Arc::as_ref(&service).clone(), tarpc::context::current())
                .await?,
        )
    }

    /// 获取表的完整详情(包括分区和段)
    ///
    /// 查询表的完整结构和所有分区/段的详细信息,包括文档统计、持久化状态等。
    ///
    /// # MCP 提示
    ///
    /// **参数:** `name` - 表名
    /// **返回:** `TableDetail` 对象或 `null`(表不存在)
    ///
    /// **用途:**
    /// - 诊断表的整体健康状况
    /// - 查看数据分布和持久化状态
    /// - 统计总文档数和段数
    ///
    /// **示例:**
    /// ```graphql
    /// query {
    ///   tableDetail(name: "users") {
    ///     total_documents
    ///     total_segments
    ///     partitions {
    ///       partition_id
    ///       segments {
    ///         doc_count
    ///         is_persisted
    ///       }
    ///     }
    ///   }
    /// }
    /// ```
    async fn table_detail(
        &self,
        ctx: &Context<'_>,
        name: String,
    ) -> Result<Option<Json<TableDetail>>> {
        let service = ctx.data::<Arc<CalmService>>()?;

        let table_detail = match CalmRpcService::get_table_detail(
            Arc::as_ref(&service).clone(),
            tarpc::context::current(),
            name.clone(),
        )
        .await
        {
            Ok(meta) => meta,
            Err(_) => return Ok(None),
        };
        Ok(Some(Json(table_detail)))
    }

    /// 执行 SQL 查询
    ///
    /// 执行标准 SQL 查询语句并返回结果集。
    ///
    /// # MCP 提示
    ///
    /// **参数:** `sql` - SQL 查询语句
    /// **返回:** `QueryResult` 对象(columns, rows, total_rows)
    ///
    /// **支持的 SQL:**
    /// - SELECT 查询 (支持 WHERE, ORDER BY, LIMIT, 聚合等)
    /// - 支持全文检索语法 (MATCH, FUZZY, WILDCARD)
    ///
    /// **示例:**
    /// ```graphql
    /// query {
    ///   query(sql: "SELECT user_id, name, age FROM users WHERE age > 18 LIMIT 10") {
    ///     columns
    ///     rows
    ///     total_rows
    ///   }
    /// }
    ///
    /// # 全文检索示例:
    /// query {
    ///   query(sql: "SELECT * FROM articles WHERE MATCH(content, 'rust programming')") {
    ///     rows
    ///   }
    /// }
    /// ```
    async fn query(&self, ctx: &Context<'_>, sql: String) -> Result<QueryResult> {
        use crate::utils::arrow_utils;
        use futures::StreamExt;

        log::info!("📊 [GraphQL Query] SQL: {}", sql);

        // 获取 CalmService
        let service = ctx.data::<Arc<CalmService>>()?;

        // 使用分布式查询执行器
        let mut stream = service
            .execute_query_stream(&sql)
            .await
            .map_err(|e| async_graphql::Error::new(format!("Query failed: {}", e)))?;

        // 收集所有 RecordBatch
        let mut batches = Vec::new();
        while let Some(batch_result) = stream.next().await {
            let batch = batch_result
                .map_err(|e| async_graphql::Error::new(format!("Stream error: {}", e)))?;
            batches.push(batch);
        }

        if batches.is_empty() {
            log::info!("✅ [GraphQL Query] No results");
            return Ok(QueryResult {
                columns: vec![],
                rows: vec![],
                total_rows: 0,
            });
        }

        // 合并 batches
        let result = if batches.len() == 1 {
            batches.into_iter().next().unwrap()
        } else {
            use datafusion::arrow::compute::concat_batches;
            let schema = batches[0].schema();
            concat_batches(&schema, &batches).map_err(|e| {
                async_graphql::Error::new(format!("Failed to concat batches: {}", e))
            })?
        };

        if result.num_rows() == 0 {
            log::info!("✅ [GraphQL Query] Empty result set");
            return Ok(QueryResult {
                columns: vec![],
                rows: vec![],
                total_rows: 0,
            });
        }

        // 获取列名
        let columns: Vec<String> = result
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();

        // 转换为 JSON
        let rows = arrow_utils::record_batch_to_json(&result)
            .map_err(|e| async_graphql::Error::new(format!("Failed to convert to JSON: {}", e)))?;

        let total_rows = rows.len();

        log::info!(
            "✅ [GraphQL Query] Returned {} rows, {} columns",
            total_rows,
            columns.len()
        );

        Ok(QueryResult {
            columns,
            rows,
            total_rows,
        })
    }

    /// 获取当前节点的状态信息
    ///
    /// 返回当前节点的系统资源使用情况和分区负载信息。
    ///
    /// # MCP 提示
    ///
    /// **返回:** JSON 对象(包含 CPU、内存、负载等信息)
    ///
    /// **字段说明:**
    /// - `node_id`: 节点唯一标识符
    /// - `partition_count`: 当前节点上的分区数量
    /// - `cpu_usage`: CPU 使用率 (0-100)
    /// - `memory_usage`: 内存使用率 (0-100)
    /// - `total_memory`: 总内存 (bytes)
    /// - `used_memory`: 已使用内存 (bytes)
    /// - `load_avg_1min`: 系统 1 分钟平均负载
    ///
    /// **用途:**
    /// - 监控节点健康状态
    /// - 查看分区分布
    /// - 诊断性能问题
    ///
    /// **示例:**
    /// ```graphql
    /// query {
    ///   nodeInfo
    /// }
    /// # 返回示例:
    /// # {
    /// #   "node_id": "20231225120530123_127.0.0.1_52000_52001",
    /// #   "partition_count": 16,
    /// #   "cpu_usage": 45.3,
    /// #   "memory_usage": 62.1,
    /// #   "total_memory": 17179869184,
    /// #   "used_memory": 10672529408,
    /// #   "load_avg_1min": 2.5
    /// # }
    /// ```
    async fn node_info(&self, ctx: &Context<'_>) -> Result<Json<crate::calm::NodeInfo>> {
        let service = ctx.data::<Arc<CalmService>>()?;

        let node_info =
            CalmRpcService::node_info(Arc::as_ref(&service).clone(), tarpc::context::current())
                .await
                .map_err(|e| {
                    async_graphql::Error::new(format!("Failed to get node info: {}", e))
                })?;

        Ok(Json(node_info))
    }

    /// 获取所有存活节点的状态信息（仅协调节点）
    ///
    /// 返回集群中所有存活节点的系统资源使用情况和分区负载信息。
    ///
    /// # MCP 提示
    ///
    /// **返回:** JSON 数组，每个元素包含一个节点的状态信息
    ///
    /// **用途:**
    /// - 集群整体健康监控
    /// - 负载均衡决策
    /// - 容量规划
    ///
    /// **示例:**
    /// ```graphql
    /// query {
    ///   listNode
    /// }
    /// # 返回示例:
    /// # [
    /// #   {
    /// #     "node_id": "20231225120530123_127.0.0.1_52000_52001",
    /// #     "partition_count": 16,
    /// #     "cpu_usage": 45.3,
    /// #     "memory_usage": 62.1,
    /// #     "total_memory": 17179869184,
    /// #     "used_memory": 10672529408,
    /// #     "load_avg_1min": 2.5
    /// #   },
    /// #   {
    /// #     "node_id": "20231225120530124_127.0.0.1_52100_52101",
    /// #     "partition_count": 12,
    /// #     "cpu_usage": 38.7,
    /// #     "memory_usage": 55.2,
    /// #     "total_memory": 17179869184,
    /// #     "used_memory": 9486323712,
    /// #     "load_avg_1min": 1.8
    /// #   }
    /// # ]
    /// ```
    async fn list_node(&self, ctx: &Context<'_>) -> Result<Json<Vec<crate::calm::NodeInfo>>> {
        let service = ctx.data::<Arc<CalmService>>()?;

        let node_infos =
            CalmRpcService::list_node(Arc::as_ref(&service).clone(), tarpc::context::current())
                .await
                .map_err(|e| {
                    async_graphql::Error::new(format!("Failed to get node list: {}", e))
                })?;

        Ok(Json(node_infos))
    }
}

// ===== Mutation Root =====

pub struct MutationRoot;

#[Object]
impl MutationRoot {
    /// 创建表
    ///
    /// 根据输入参数创建新表,包括字段定义、分区策略、持久化配置等。
    ///
    /// # MCP 提示
    ///
    /// **参数:** `input: CreateTableInput` - 表的完整定义
    /// **返回:** `Table` 对象(新创建的表)
    ///
    /// **最佳实践:**
    /// - 表名使用小写字母+下划线(user_orders)
    /// - 至少定义 1 个字段
    /// - 指定 primary_key 用于去重和分区
    /// - 设置合理的分区策略(Hash 推荐)
    /// - 添加 description 帮助团队理解
    ///
    /// **快速示例(推荐):**
    /// ```graphql
    /// mutation {
    ///   createTable(input: {
    ///     name: "users"
    ///     description: "用户信息表"
    ///     primary_key: "user_id"
    ///     partitionStrategy: {
    ///       strategyType: PKHASH
    ///       numPartitions: 4
    ///     }
    ///     fields: [
    ///       { name: "user_id", field_type: U64, nullable: false }
    ///       { name: "username", field_type: KEYWORD, nullable: false }
    ///       { name: "age", field_type: I8, nullable: true }
    ///     ]
    ///   }) {
    ///     name
    ///   }
    /// }
    /// ```
    ///
    /// 详细配置参考 `CreateTableInput` 类型文档。
    async fn create_table(&self, ctx: &Context<'_>, input: CreateTableInput) -> Result<Table> {
        let service = ctx.data::<Arc<CalmService>>()?;

        // 构建字段
        let mut fields = Vec::new();
        for field_input in input.fields {
            let indexed = field_input.indexed.unwrap_or(true);
            let nullable = field_input.nullable.unwrap_or(true);

            let field = match field_input.field_type {
                FieldTypeEnum::Keyword | FieldTypeEnum::Text => {
                    let case_sensitive = field_input.case_sensitive.unwrap_or(true);
                    let is_array = field_input.is_array.unwrap_or(false);
                    FieldOption::Keyword {
                        name: field_input.name.clone(),
                        index: indexed,
                        is_array,
                        persist_option: None,
                        case_sensitive,
                        description: field_input.description.clone(),
                        default_value: field_input.default_value.clone(),
                        nullable,
                    }
                }
                FieldTypeEnum::I8 => FieldOption::I8 {
                    name: field_input.name.clone(),
                    index: indexed,
                    description: field_input.description.clone(),
                    default_value: field_input.default_value.clone(),
                    nullable,
                },
                FieldTypeEnum::I16 => FieldOption::I16 {
                    name: field_input.name.clone(),
                    index: indexed,
                    description: field_input.description.clone(),
                    default_value: field_input.default_value.clone(),
                    nullable,
                },
                FieldTypeEnum::I32 | FieldTypeEnum::Integer => FieldOption::I32 {
                    name: field_input.name.clone(),
                    index: indexed,
                    description: field_input.description.clone(),
                    default_value: field_input.default_value.clone(),
                    nullable,
                },
                FieldTypeEnum::I64 | FieldTypeEnum::Long => FieldOption::I64 {
                    name: field_input.name.clone(),
                    index: indexed,
                    description: field_input.description.clone(),
                    default_value: field_input.default_value.clone(),
                    nullable,
                },
                FieldTypeEnum::U8 => FieldOption::U8 {
                    name: field_input.name.clone(),
                    index: indexed,
                    description: field_input.description.clone(),
                    default_value: field_input.default_value.clone(),
                    nullable,
                },
                FieldTypeEnum::U16 => FieldOption::U16 {
                    name: field_input.name.clone(),
                    index: indexed,
                    description: field_input.description.clone(),
                    default_value: field_input.default_value.clone(),
                    nullable,
                },
                FieldTypeEnum::U32 => FieldOption::U32 {
                    name: field_input.name.clone(),
                    index: indexed,
                    description: field_input.description.clone(),
                    default_value: field_input.default_value.clone(),
                    nullable,
                },
                FieldTypeEnum::U64 => FieldOption::U64 {
                    name: field_input.name.clone(),
                    index: indexed,
                    description: field_input.description.clone(),
                    default_value: field_input.default_value.clone(),
                    nullable,
                },
                FieldTypeEnum::F32 | FieldTypeEnum::Float => FieldOption::F32 {
                    name: field_input.name.clone(),
                    index: indexed,
                    description: field_input.description.clone(),
                    default_value: field_input.default_value.clone(),
                    nullable,
                },
                FieldTypeEnum::F64 | FieldTypeEnum::Double => FieldOption::F64 {
                    name: field_input.name.clone(),
                    index: indexed,
                    description: field_input.description.clone(),
                    default_value: field_input.default_value.clone(),
                    nullable,
                },
                FieldTypeEnum::Boolean | FieldTypeEnum::Bool => FieldOption::Boolean {
                    name: field_input.name.clone(),
                    index: indexed,
                    description: field_input.description.clone(),
                    default_value: field_input.default_value.clone(),
                    nullable,
                },
                FieldTypeEnum::Timestamp | FieldTypeEnum::Datetime => FieldOption::Timestamp {
                    name: field_input.name.clone(),
                    index: indexed,
                    format: field_input.format.clone(),
                    description: field_input.description.clone(),
                    default_value: field_input.default_value.clone(),
                    nullable,
                },
            };

            fields.push(field);
        }

        // 构建持久化策略
        let persist_policy = if let Some(policy_input) = input.persist_policy {
            PersistPolicy {
                max_docs_per_segment: policy_input.max_docs_per_segment.unwrap_or(100_000),
                max_segment_age: std::time::Duration::from_secs(
                    policy_input.max_segment_age_secs.unwrap_or(300),
                ),
            }
        } else {
            PersistPolicy::default()
        };

        // 创建 Schema
        let schema = CalmSchema::new(
            input.name.clone(),
            input.primary_key.clone(),
            input.store_source.unwrap_or(true),
            fields.clone(),
            persist_policy,
            input.description.clone(),
        );

        // 构建分区策略 (使用 OneofObject 模式)
        let (partition_strategy, num_partitions) =
            if let Some(strategy_input) = input.partition_strategy {
                match strategy_input {
                    PartitionStrategyInput::PkHash(config) => {
                        let num_partitions = config.num_partitions as usize;
                        (PartitionStrategy::PKHash { num_partitions }, num_partitions)
                    }
                    PartitionStrategyInput::Hash(config) => {
                        let num_partitions = config.num_partitions as usize;
                        (
                            PartitionStrategy::Hash {
                                field: config.field.to_lowercase(), // 自动转换为小写
                                num_partitions,
                            },
                            num_partitions,
                        )
                    }
                    PartitionStrategyInput::Range(config) => {
                        // 可选的并行度参数
                        let parallelism = if config.parallelism > 1 {
                            Some(config.parallelism as usize)
                        } else {
                            None
                        };

                        // Range 分区按需创建,不需要预先指定 num_partitions
                        (
                            PartitionStrategy::Range {
                                field: config.field.to_lowercase(), // 自动转换为小写
                                start: config.start,
                                step: config.step,
                                parallelism,
                            },
                            0,
                        )
                    }
                    PartitionStrategyInput::DatetimeRange(config) => {
                        use crate::catalog::TimeGranularity;

                        let granularity = match config.granularity {
                            TimeGranularityType::Year => TimeGranularity::Year,
                            TimeGranularityType::Month => TimeGranularity::Month,
                            TimeGranularityType::Week => TimeGranularity::Week,
                            TimeGranularityType::Day => TimeGranularity::Day,
                            TimeGranularityType::Hour => TimeGranularity::Hour,
                        };

                        // 可选的并行度参数
                        let parallelism = if config.parallelism > 1 {
                            Some(config.parallelism as usize)
                        } else {
                            None
                        };

                        // DatetimeRange 分区按需创建,不需要预先指定 num_partitions
                        (
                            PartitionStrategy::DatetimeRange {
                                field: config.field.to_lowercase(), // 自动转换为小写
                                granularity,
                                timezone: Some(config.timezone),
                                parallelism,
                            },
                            0,
                        )
                    }
                    PartitionStrategyInput::Custom(_config) => {
                        // Custom 分区不需要其他参数
                        (PartitionStrategy::Custom, 0)
                    }
                    PartitionStrategyInput::None(_) => {
                        // None 分区策略，所有数据在一个 partition
                        (PartitionStrategy::None, 1)
                    }
                }
            } else {
                // 未指定分区策略，默认使用 None（无分区）
                (PartitionStrategy::None, 1)
            };

        // 直接调用 CalmService 的 create_table 方法
        CalmRpcService::create_table(
            Arc::as_ref(&service).clone(),
            tarpc::context::current(),
            schema,
            partition_strategy,
        )
        .await
        .map_err(|e| async_graphql::Error::new(format!("Failed to create table: {}", e)))?;

        // 返回创建的表信息
        let field_info: Vec<Field> = fields
            .iter()
            .map(|f| {
                let field_type = match f {
                    FieldOption::Keyword { .. } => "keyword",
                    FieldOption::I64 { .. } => "i64",
                    FieldOption::F64 { .. } => "f64",
                    FieldOption::Boolean { .. } => "boolean",
                    FieldOption::I32 { .. } => "i32",
                    FieldOption::F32 { .. } => "f32",
                    FieldOption::Timestamp { .. } => "timestamp",
                    _ => "unknown",
                };

                Field {
                    name: f.name().to_string(),
                    field_type: field_type.to_string(),
                    indexed: f.is_index(),
                }
            })
            .collect();

        Ok(Table {
            name: input.name,
            partition_count: num_partitions as u64,
            fields: field_info,
            primary_key: input.primary_key,
        })
    }

    /// 删除表
    ///
    /// 永久删除指定的表及其所有数据。
    ///
    /// # MCP 提示
    ///
    /// **参数:** `name` - 表名
    /// **返回:** `true`(成功) 或错误
    ///
    /// **警告:** 此操作不可逆,所有数据将被删除！
    ///
    /// **示例:**
    /// ```graphql
    /// mutation {
    ///   dropTable(name: "old_users")
    /// }
    /// ```
    async fn drop_table(&self, ctx: &Context<'_>, name: String) -> Result<bool> {
        let service = ctx.data::<Arc<CalmService>>()?;
        CalmRpcService::drop_table(
            Arc::as_ref(service).clone(),
            tarpc::context::current(),
            name,
        )
        .await
        .map_err(|e| async_graphql::Error::new(format!("Failed to drop table: {}", e)))?;
        Ok(true)
    }

    /// 持久化表(强制将表的所有数据写入磁盘)
    ///
    /// 强制将表的所有内存数据刺入磁盘,确保数据持久化。
    ///
    /// # MCP 提示
    ///
    /// **参数:** `name` - 表名
    /// **返回:** `true`(成功) 或错误
    ///
    /// **场景:**
    /// - 批量导入后立刻持久化
    /// - 重启前保证数据安全
    /// - 性能测试时确保数据已写盘
    ///
    /// **示例:**
    /// ```graphql
    /// mutation {
    ///   flushTable(name: "users")
    /// }
    /// ```
    async fn flush_table(&self, ctx: &Context<'_>, name: String) -> Result<bool> {
        let service = ctx.data::<Arc<CalmService>>()?;

        CalmRpcService::flush_table(
            Arc::as_ref(service).clone(),
            tarpc::context::current(),
            name,
        )
        .await
        .map_err(|e| async_graphql::Error::new(format!("Failed to flush table: {}", e)))?;

        Ok(true)
    }

    /// 插入数据
    ///
    /// 向指定表插入一批文档(JSON 对象数组)。
    ///
    /// # MCP 提示
    ///
    /// **参数:** `input: InsertDataInput` (table, data, partition?)
    /// **返回:** `InsertResult` (success, rows_inserted, message)
    ///
    /// **数据格式:**
    /// - JSON 对象数组
    /// - 字段名必须与 schema 匹配
    /// - 缺失字段使用 default_value 或 null
    /// - 主键重复会覆盖旧数据(upsert)
    ///
    /// **分区选项:**
    /// - 不指定 `partition`: 使用表的路由策略自动计算分区
    /// - 指定 `partition`: 直接插入到指定分区,跳过路由策略,分区不存在则自动创建
    ///
    /// **示例 1(自动路由):**
    /// ```graphql
    /// mutation {
    ///   insertData(input: {
    ///     table: "users"
    ///     data: [
    ///       {"user_id": 1, "username": "alice", "age": 25}
    ///       {"user_id": 2, "username": "bob", "age": 30}
    ///     ]
    ///   }) {
    ///     success
    ///     rows_inserted
    ///     message
    ///   }
    /// }
    /// ```
    ///
    /// **示例 2(指定分区):**
    /// ```graphql
    /// mutation {
    ///   insertData(input: {
    ///     table: "users"
    ///     partition: "custom_p0"  # 直接插入到 custom_p0,不存在则创建
    ///     data: [{"user_id": 1, "username": "alice"}]
    ///   }) {
    ///     success
    ///     rows_inserted
    ///   }
    /// }
    /// ```
    async fn insert_data(&self, ctx: &Context<'_>, input: InsertDataInput) -> Result<InsertResult> {
        log::info!(
            "📝 [GraphQL Insert] Table: {}, Rows: {}",
            input.table,
            input.data.len()
        );

        // 获取 CalmService
        let service = ctx.data::<Arc<CalmService>>()?;

        // 1. 获取表元数据以获取 Arrow Schema
        let table_info = service
            .catalog
            .get_or_load_table(&input.table)
            .await
            .map_err(|e| async_graphql::Error::new(format!("Failed to load table: {}", e)))?;

        // 2. 标准化 JSON 字段名为小写（与 Arrow Schema 保持一致）
        let normalized_data: Vec<serde_json::Value> = input
            .data
            .iter()
            .map(|value| {
                if let serde_json::Value::Object(map) = value {
                    let mut new_map = serde_json::Map::new();
                    for (key, val) in map {
                        new_map.insert(key.to_lowercase(), val.clone());
                    }
                    serde_json::Value::Object(new_map)
                } else {
                    value.clone()
                }
            })
            .collect();

        // 3. 将 JSON 数据转换为 RecordBatch
        use crate::utils::arrow_utils;
        let batch = arrow_utils::json_to_record_batch(
            &normalized_data,
            table_info.table.schema.to_arrow_schema(),
        )
        .map_err(|e| {
            async_graphql::Error::new(format!("Failed to convert JSON to RecordBatch: {}", e))
        })?;

        // 4. 调用 CalmService 插入数据（内部使用 Router 路由）
        let rows_inserted = service
            .insert_data(&input.table, batch)
            .await
            .map_err(|e| async_graphql::Error::new(format!("Insert failed: {}", e)))?;

        log::info!(
            "✅ [GraphQL Insert] Successfully inserted {} rows to table '{}'",
            rows_inserted,
            input.table
        );

        Ok(InsertResult {
            success: true,
            rows_inserted,
            message: format!(
                "Successfully inserted {} rows to table '{}'",
                rows_inserted, input.table
            ),
        })
    }

    /// 加载外部文件到 segment
    ///
    /// 直接加载 Parquet 或 JSONL 文件到指定分区,无需先创建表再插入。
    ///
    /// # MCP 提示
    ///
    /// **参数:** `input: LoadSegmentInput` (table, partition_name, file_path, handler_type)
    /// **返回:** `LoadSegmentResult` (success, documents_loaded, message)
    ///
    /// **文件类型:**
    /// - `.parquet`: 需要指定 handler_type (REFERENCE/MOVE/COPY)
    /// - `.jsonl`: 不需要 handler_type,直接导入
    ///
    /// **handler_type 说明:**
    /// - `REFERENCE`: 引用原文件,不复制(推荐,零拷贝)
    /// - `MOVE`: 移动文件到数据目录
    /// - `COPY`: 复制文件到数据目录
    ///
    /// **使用场景:**
    /// - 批量导入大文件
    /// - 加载历史数据
    /// - 数据迁移
    ///
    /// **示例:**
    /// ```graphql
    /// mutation {
    ///   loadSegment(input: {
    ///     table: "taxi_trips"
    ///     partition_name: "p0"
    ///     file_path: "/data/trips_2024.parquet"
    ///     handler_type: REFERENCE
    ///   }) {
    ///     success
    ///     documents_loaded
    ///     message
    ///   }
    /// }
    /// ```
    async fn load_segment(
        &self,
        ctx: &Context<'_>,
        input: LoadSegmentInput,
    ) -> Result<LoadSegmentResult> {
        todo!()
        // let engine = ctx.data::<Arc<Engine>>()?;

        // // 验证文件路径
        // let file_path = std::path::PathBuf::from(&input.file_path);
        // if !file_path.exists() {
        //     return Err(async_graphql::Error::new(format!(
        //         "File does not exist: {}",
        //         input.file_path
        //     )));
        // }

        // // 转换 handler_type（可选）
        // let handler_type = input.handler_type.map(|ht| ht.into());

        // // 调用 engine 的 load_segment 方法
        // let doc_count = engine
        //     .load_segment(
        //         &input.table,
        //         input.partition_name.clone(),
        //         file_path,
        //         handler_type,
        //     )
        //     .await
        //     .map_err(|e| async_graphql::Error::new(format!("Load segment failed: {}", e)))?;

        // Ok(LoadSegmentResult {
        //     success: true,
        //     documents_loaded: doc_count,
        //     partition_name: input.partition_name,
        //     message: format!(
        //         "Successfully loaded {} documents from {}",
        //         doc_count, input.file_path
        //     ),
        // })
    }
}

// ===== GraphQL Server =====

/// GraphQL 服务器
pub struct GraphQLServer {
    service: Arc<CalmService>,
}

impl GraphQLServer {
    pub fn new(service: Arc<CalmService>) -> Self {
        Self { service }
    }

    /// 启动 GraphQL 服务器
    pub async fn start(self, addr: &str) -> Result<(), std::io::Error> {
        // 创建 GraphQL Schema
        let graphql_schema = create_schema(self.service);

        // GraphQL endpoint
        let graphql_endpoint = async_graphql_poem::GraphQL::new(graphql_schema);

        // 构建路由
        let app = Route::new()
            .at("/", get(root))
            .at("/health", get(health))
            .at("/graphql", post(graphql_endpoint))
            .with(Cors::new());

        Server::new(TcpListener::bind(addr)).run(app).await
    }
}

/// 健康检查
#[handler]
async fn health() -> &'static str {
    "OK"
}

/// 根路径
#[handler]
async fn root() -> poem::web::Json<serde_json::Value> {
    poem::web::Json(serde_json::json!({
        "name": "Calm Database - GraphQL Server",
        "version": "0.1.0",
        "graphql_endpoint": "/graphql",
    }))
}
