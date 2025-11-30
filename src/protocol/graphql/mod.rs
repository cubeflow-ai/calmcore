use std::sync::Arc;

use async_graphql::{Context, EmptySubscription, Object, Result, Schema, SimpleObject};
use poem::{
    get, handler, listener::TcpListener, middleware::Cors, post, EndpointExt, Route, Server,
};
use serde_json::Value as JsonValue;

use crate::{
    catalog::{PartitionStrategy, PartitionValue, RangePartition},
    engine::Engine,
    schema::{field::FieldOption, PersistPolicy, Schema as CalmSchema},
    utils::arrow_utils,
};

/// GraphQL Schema for database operations
pub type CalmGraphQLSchema = Schema<QueryRoot, MutationRoot, EmptySubscription>;

/// 创建 GraphQL Schema
pub fn create_schema(engine: Arc<Engine>) -> CalmGraphQLSchema {
    Schema::build(QueryRoot, MutationRoot, EmptySubscription)
        .data(engine)
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
/// partition_count: 1  # 等价于 strategy_type: NONE
/// ```
#[derive(async_graphql::Enum, Copy, Clone, Eq, PartialEq)]
pub enum PartitionStrategyType {
    /// 哈希分区 - 基于字段哈希值均匀分布,适合 ID 类字段。需要 field 和 num_partitions
    Hash,
    /// 范围分区 - 基于字段值范围划分,适合时序数据。需要 field 和 ranges
    Range,
    /// 自定义分区 - 用户手动管理分区元数据,适合特殊需求
    Custom,
    /// 无分区 - 所有数据在单分区,适合小表(<100万行)
    None,
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
/// ```
#[derive(async_graphql::InputObject)]
pub struct PartitionValueInput {
    /// 有符号整数值 - 用于普通整数或负数。与 uint_value、string_value 三选一
    pub int_value: Option<i64>,

    /// 无符号整数值 - 用于时间戳(毫秒)、ID 等。与 int_value、string_value 三选一
    pub uint_value: Option<u64>,

    /// 字符串值 - 用于日期字符串、分类值等。与 int_value、uint_value 三选一
    pub string_value: Option<String>,
}

impl From<PartitionValueInput> for PartitionValue {
    fn from(val: PartitionValueInput) -> Self {
        if let Some(i) = val.int_value {
            PartitionValue::Int64(i)
        } else if let Some(u) = val.uint_value {
            PartitionValue::UInt64(u)
        } else if let Some(s) = val.string_value {
            PartitionValue::String(s)
        } else {
            PartitionValue::Int64(0) // 默认值
        }
    }
}

/// 范围分区定义
///
/// 定义单个 Range 分区的边界:[start, end)。
///
/// # MCP 提示
///
/// **规则:**
/// - 区间左闭右开: start <= value < end
/// - 分区不能重叠
/// - 分区应连续覆盖所有可能值
///
/// **时序数据示例(按月分区):**
/// ```graphql
/// ranges: [
///   {
///     partition_id: 0
///     start: { int_value: 1704067200000 }  # 2024-01-01
///     end: { int_value: 1706745600000 }    # 2024-02-01
///   }
///   {
///     partition_id: 1
///     start: { int_value: 1706745600000 }  # 2024-02-01
///     end: { int_value: 1709251200000 }    # 2024-03-01
///   }
/// ]
/// ```
#[derive(async_graphql::InputObject)]
pub struct RangePartitionInput {
    /// 起始值(包含) - 分区范围的下界,该值属于此分区
    pub start: PartitionValueInput,

    /// 结束值(不包含) - 分区范围的上界,该值不属于此分区(属于下一分区)
    pub end: PartitionValueInput,

    /// 分区 ID - 唯一标识符,从 0 开始递增
    pub partition_id: u64,
}

impl From<RangePartitionInput> for RangePartition {
    fn from(val: RangePartitionInput) -> Self {
        RangePartition {
            start: val.start.into(),
            end: val.end.into(),
        }
    }
}

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
/// **None 分区(小表):**
/// ```graphql
/// partition_strategy: {
///   strategy_type: NONE
/// }
/// # 或直接使用: partition_count: 1
/// ```
/// - 适合: <100万行的表
/// - 优点: 简单,无分区开销
#[derive(async_graphql::InputObject)]
pub struct PartitionStrategyInput {
    /// 分区策略类型 - HASH(推荐), RANGE(时序), CUSTOM(高级), NONE(小表)
    pub strategy_type: PartitionStrategyType,

    /// 分区字段名 - Hash/Range 策略必需。通常选择主键(Hash)或时间戳(Range)
    pub field: Option<String>,

    /// 分区数量 - Hash 策略必需。建议 2-16,推荐 CPU 核心数或 2 的幂次
    pub num_partitions: Option<u64>,

    /// 范围定义 - Range 策略必需。定义每个分区的值范围(start <= value < end)
    pub ranges: Option<Vec<RangePartitionInput>>,

    /// 值映射 - List 策略使用(暂不推荐)。格式: "value1:0,value2:1,value3:2"
    pub value_mapping: Option<String>,
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
/// - `partition_strategy` 或 `partition_count`: 分区配置
/// - `store_source`: 是否存储原始 JSON (默认 true)
///
/// **快速开始模板:**
/// ```graphql
/// mutation {
///   createTable(input: {
///     name: "users"                          # 表名
///     description: "用户信息表"               # 表描述
///     primary_key: "user_id"                 # 主键
///     partition_count: 4                     # 简单分区(Hash on user_id)
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
///     partition_count
///   }
/// }
/// ```
///
/// **高级配置模板(带分区策略):**
/// ```graphql
/// mutation {
///   createTable(input: {
///     name: "orders"
///     description: "订单表,按时间范围分区"
///     primary_key: "order_id"
///     
///     # 自定义 Range 分区
///     partition_strategy: {
///       strategy_type: RANGE
///       field: "created_at"
///       ranges: [
///         { partition_id: 0, start: {int_value: 0}, end: {int_value: 1704067200000} }
///         { partition_id: 1, start: {int_value: 1704067200000}, end: {int_value: 9999999999999} }
///       ]
///     }
///     
///     # 持久化策略
///     persist_policy: {
///       max_docs_per_segment: 500000         # 50万文档持久化
///       max_segment_age_secs: 600            # 10分钟持久化
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

    /// 分区策略 - 定义数据如何分布。与 partition_count 二选一,不指定则默认 Hash 分区 1 个
    pub partition_strategy: Option<PartitionStrategyInput>,

    /// 分区数量 - 简化配置,仅在未指定 partition_strategy 时使用。默认为 1(无分区)
    pub partition_count: Option<u64>,

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

/// 表详细信息(返回)
///
/// `tableDetail(name: String)` 查询的返回类型,包含表的完整结构和所有分区/段信息。
///
/// # MCP 提示
///
/// **字段说明:**
/// - `name`: 表名
/// - `partition_count`: 分区数量
/// - `total_segments`: 所有分区的段总数
/// - `total_documents`: 所有分区的有效文档总数 (doc_count - deleted_count)
/// - `fields`: 字段列表
/// - `primary_key`: 主键字段
/// - `partitions`: 所有分区的详细信息
///
/// **使用场景:**
/// - 诊断表的整体健康状况
/// - 查看数据分布和持久化状态
/// - 计算存储空间使用
///
/// **查询示例:**
/// ```graphql
/// query {
///   tableDetail(name: "users") {
///     name
///     total_documents
///     partitions {
///       partition_id
///       segments {
///         segment_id
///         doc_count
///         is_persisted
///       }
///     }
///   }
/// }
/// ```
#[derive(SimpleObject)]
pub struct TableDetail {
    /// 表名
    pub name: String,
    /// 分区数
    pub partition_count: usize,
    /// 段总数
    pub total_segments: usize,
    /// 有效文档总数
    pub total_documents: u64,
    /// 字段列表
    pub fields: Vec<Field>,
    /// 主键字段
    pub primary_key: Option<String>,
    /// 分区详情
    pub partitions: Vec<PartitionInfo>,
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
        let engine = ctx.data::<Arc<Engine>>()?;
        Ok(engine.list_tables())
    }

    /// 获取表的基本信息
    ///
    /// 根据表名查询表的基本结构,包括字段列表、分区数等。
    ///
    /// # MCP 提示
    ///
    /// **参数:** `name` - 表名
    /// **返回:** `Table` 对象或 `null`(表不存在)
    ///
    /// **示例:**
    /// ```graphql
    /// query {
    ///   table(name: "users") {
    ///     name
    ///     partition_count
    ///     primary_key
    ///     fields {
    ///       name
    ///       field_type
    ///       indexed
    ///     }
    ///   }
    /// }
    /// ```
    async fn table(&self, ctx: &Context<'_>, name: String) -> Result<Option<Table>> {
        let engine = ctx.data::<Arc<Engine>>()?;

        let meta = match engine.get_table_meta(&name) {
            Ok(meta) => meta,
            Err(_) => return Ok(None),
        };

        let fields = meta
            .schema
            .fields
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

        Ok(Some(Table {
            name: meta.schema.name.clone(),
            partition_count: meta.parallel_workers as u64,
            fields,
            primary_key: meta.schema.primary_key.clone(),
        }))
    }

    /// 获取表的所有分区信息
    ///
    /// 查询表的所有分区及其段的详细状态。
    ///
    /// # MCP 提示
    ///
    /// **参数:** `table` - 表名
    /// **返回:** 分区信息数组
    ///
    /// **用途:** 诊断数据分布、查看持久化状态
    ///
    /// **示例:**
    /// ```graphql
    /// query {
    ///   partitions(table: "users") {
    ///     partition_id
    ///     segment_count
    ///     segments {
    ///       segment_id
    ///       doc_count
    ///       is_persisted
    ///     }
    ///   }
    /// }
    /// ```
    async fn partitions(&self, ctx: &Context<'_>, table: String) -> Result<Vec<PartitionInfo>> {
        let engine = ctx.data::<Arc<Engine>>()?;

        // 获取表的所有分区 ID
        let partition_ids = engine.list_partitions(&table).await;

        let mut partition_infos = Vec::new();

        for partition_id in partition_ids {
            if let Some(partition) = engine.get_partition(&table, &partition_id).await {
                // 获取 frozen segments
                let frozen_segments = partition.get_frozen_segments();
                let mut segments = Vec::new();

                // 收集 frozen segments 信息
                for (seg_id, segment) in frozen_segments.iter() {
                    segments.push(SegmentInfo {
                        segment_id: *seg_id,
                        doc_count: segment.doc_count(),
                        deleted_count: segment.deleted_count(),
                        is_persisted: segment.is_persisted(),
                        base_path: segment.base_path(),
                        is_external_reference: segment.is_external_reference(),
                        external_data_path: segment.get_external_data_path(),
                    });
                }

                // 释放读锁
                drop(frozen_segments);

                // 获取当前 segment（ID = 0 表示当前活跃 segment）
                let current_segment = partition.get_current_segment();
                segments.push(SegmentInfo {
                    segment_id: 0,
                    doc_count: current_segment.doc_count(),
                    deleted_count: current_segment.deleted_count(),
                    is_persisted: current_segment.is_persisted(),
                    base_path: current_segment.base_path(),
                    is_external_reference: current_segment.is_external_reference(),
                    external_data_path: current_segment.get_external_data_path(),
                });

                partition_infos.push(PartitionInfo {
                    partition_id,
                    segment_count: segments.len(),
                    segments,
                });
            }
        }

        Ok(partition_infos)
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
    async fn table_detail(&self, ctx: &Context<'_>, name: String) -> Result<Option<TableDetail>> {
        let engine = ctx.data::<Arc<Engine>>()?;

        let meta = match engine.get_table_meta(&name) {
            Ok(meta) => meta,
            Err(_) => return Ok(None),
        };

        let fields = meta
            .schema
            .fields
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

        // 获取所有分区信息
        let partition_ids = engine.list_partitions(&name).await;
        let mut partition_infos = Vec::new();
        let mut total_segments = 0;
        let mut total_documents = 0u64;

        for partition_id in partition_ids {
            if let Some(partition) = engine.get_partition(&name, &partition_id).await {
                // 获取 frozen segments
                let frozen_segments = partition.get_frozen_segments();
                let mut segments = Vec::new();

                // 收集 frozen segments 信息
                for (seg_id, segment) in frozen_segments.iter() {
                    let doc_count = segment.doc_count();
                    let deleted_count = segment.deleted_count();
                    total_documents += doc_count as u64 - deleted_count;

                    segments.push(SegmentInfo {
                        segment_id: *seg_id,
                        doc_count,
                        deleted_count,
                        is_persisted: segment.is_persisted(),
                        base_path: segment.base_path(),
                        is_external_reference: segment.is_external_reference(),
                        external_data_path: segment.get_external_data_path(),
                    });
                }

                // 释放读锁
                drop(frozen_segments);

                // 获取当前 segment
                let current_segment = partition.get_current_segment();
                let doc_count = current_segment.doc_count();
                let deleted_count = current_segment.deleted_count();
                total_documents += doc_count as u64 - deleted_count;

                segments.push(SegmentInfo {
                    segment_id: 0,
                    doc_count,
                    deleted_count,
                    is_persisted: current_segment.is_persisted(),
                    base_path: current_segment.base_path(),
                    is_external_reference: current_segment.is_external_reference(),
                    external_data_path: current_segment.get_external_data_path(),
                });

                total_segments += segments.len();

                partition_infos.push(PartitionInfo {
                    partition_id,
                    segment_count: segments.len(),
                    segments,
                });
            }
        }

        Ok(Some(TableDetail {
            name: meta.schema.name.clone(),
            partition_count: partition_infos.len(),
            total_segments,
            total_documents,
            fields,
            primary_key: meta.schema.primary_key.clone(),
            partitions: partition_infos,
        }))
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
        let engine = ctx.data::<Arc<Engine>>()?;

        // 使用 Engine 的 execute_sql 方法
        let result = engine
            .execute_sql(&sql)
            .await
            .map_err(|e| async_graphql::Error::new(format!("Query failed: {}", e)))?;

        if result.batch.num_rows() == 0 {
            return Ok(QueryResult {
                columns: vec![],
                rows: vec![],
                total_rows: 0,
            });
        }

        // 获取列名
        let columns: Vec<String> = result
            .batch
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();

        // 转换为 JSON
        let rows = arrow_utils::record_batch_to_json(&result.batch)
            .map_err(|e| async_graphql::Error::new(format!("Failed to convert to JSON: {}", e)))?;

        let total_rows = rows.len();

        Ok(QueryResult {
            columns,
            rows,
            total_rows,
        })
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
    ///     partition_count: 4  # 简化配置,Hash 分区
    ///     fields: [
    ///       { name: "user_id", field_type: U64, nullable: false }
    ///       { name: "username", field_type: KEYWORD, nullable: false }
    ///       { name: "age", field_type: I8, nullable: true }
    ///     ]
    ///   }) {
    ///     name
    ///     partition_count
    ///   }
    /// }
    /// ```
    ///
    /// 详细配置参考 `CreateTableInput` 类型文档。
    async fn create_table(&self, ctx: &Context<'_>, input: CreateTableInput) -> Result<Table> {
        let engine = ctx.data::<Arc<Engine>>()?;

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

        // 构建分区策略
        let (partition_strategy, num_partitions) =
            if let Some(strategy_input) = input.partition_strategy {
                match strategy_input.strategy_type {
                    PartitionStrategyType::Hash => {
                        let field = strategy_input.field.ok_or_else(|| {
                            async_graphql::Error::new("Hash strategy requires 'field' parameter")
                        })?;
                        let num_partitions = strategy_input.num_partitions.ok_or_else(|| {
                            async_graphql::Error::new(
                                "Hash strategy requires 'num_partitions' parameter",
                            )
                        })? as usize;

                        (
                            PartitionStrategy::Hash {
                                field,
                                num_partitions,
                            },
                            num_partitions,
                        )
                    }
                    PartitionStrategyType::Range => {
                        let field = strategy_input.field.ok_or_else(|| {
                            async_graphql::Error::new("Range strategy requires 'field' parameter")
                        })?;
                        let ranges_input = strategy_input.ranges.ok_or_else(|| {
                            async_graphql::Error::new("Range strategy requires 'ranges' parameter")
                        })?;

                        let ranges: Vec<RangePartition> =
                            ranges_input.into_iter().map(|r| r.into()).collect();

                        let num_partitions = ranges.len();

                        (PartitionStrategy::Range { field, ranges }, num_partitions)
                    }
                    PartitionStrategyType::Custom => {
                        // Custom 分区不需要其他参数
                        (PartitionStrategy::Custom, 0)
                    }
                    PartitionStrategyType::None => {
                        // None 分区策略，所有数据在一个 partition
                        (PartitionStrategy::None, 1)
                    }
                }
            } else {
                // 如果未指定分区策略，使用默认的 Hash 策略
                let partition_field = input.primary_key.clone().unwrap_or_else(|| {
                    fields
                        .first()
                        .map(|f| f.name().to_string())
                        .unwrap_or_default()
                });

                let partition_count = input.partition_count.unwrap_or(1) as usize;

                (
                    PartitionStrategy::Hash {
                        field: partition_field,
                        num_partitions: partition_count,
                    },
                    partition_count,
                )
            };

        // 创建表
        engine
            .create_table(&input.name, schema, partition_strategy, num_partitions)
            .await
            .map_err(|e| async_graphql::Error::new(e.to_string()))?;

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
        let engine = ctx.data::<Arc<Engine>>()?;

        engine
            .drop_table(&name)
            .await
            .map_err(|e| async_graphql::Error::new(e.to_string()))?;

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
        let engine = ctx.data::<Arc<Engine>>()?;

        engine
            .flush_table(&name)
            .await
            .map_err(|e| async_graphql::Error::new(format!("Failed to flush table: {}", e)))?;

        Ok(true)
    }

    /// 持久化表(别名,与 flushTable 功能相同)
    ///
    /// `flushTable` 的别名,功能完全相同。
    ///
    /// # MCP 提示
    ///
    /// **建议使用 `flushTable` 以保持一致性。**
    async fn table_persist(&self, ctx: &Context<'_>, name: String) -> Result<bool> {
        self.flush_table(ctx, name).await
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
        let engine = ctx.data::<Arc<Engine>>()?;

        // 获取表的元数据
        let meta = engine
            .get_table_meta(&input.table)
            .map_err(|e| async_graphql::Error::new(format!("Table not found: {}", e)))?;

        let mut total_inserted = 0;

        // 情况 1: 用户指定了 partition,直接插入到该分区
        if let Some(partition_name) = &input.partition {
            // 尝试获取分区,如果不存在则创建
            let partition = match engine.get_partition(&input.table, partition_name).await {
                Some(p) => p,
                None => {
                    // 分区不存在,自动创建新分区
                    log::info!(
                        "Partition '{}' not found for table '{}', creating new partition",
                        partition_name,
                        input.table
                    );

                    // 使用 load_partition 创建新分区
                    engine
                        .load_partition(&input.table, partition_name.clone(), meta.schema.clone())
                        .await
                        .map_err(|e| {
                            async_graphql::Error::new(format!(
                                "Failed to create partition '{}': {}",
                                partition_name, e
                            ))
                        })?
                }
            };

            // 批量插入所有数据到指定分区
            partition
                .upsert_json(&input.data)
                .map_err(|e| async_graphql::Error::new(format!("Insert failed: {}", e)))?;

            total_inserted = input.data.len();

            return Ok(InsertResult {
                success: true,
                rows_inserted: total_inserted,
                message: format!(
                    "Successfully inserted {} rows to partition '{}'",
                    total_inserted, partition_name
                ),
            });
        }

        // 情况 2: 未指定 partition,使用路由策略
        // 获取主键字段
        let pk_field = meta
            .schema
            .primary_key
            .as_ref()
            .ok_or_else(|| async_graphql::Error::new("Table has no primary key"))?;

        // 对每条数据进行路由和插入
        for doc in &input.data {
            // 1. 提取主键值
            let pk_value = doc.get(pk_field).and_then(|v| v.as_str()).ok_or_else(|| {
                async_graphql::Error::new(format!("Missing or invalid primary key: {}", pk_field))
            })?;

            // 2. 根据路由策略计算 partition_id
            let partition_id = engine
                .route_partition(&input.table, pk_value)
                .map_err(|e| async_graphql::Error::new(format!("Route failed: {}", e)))?;

            // 3. 获取 partition
            let partition = engine
                .get_partition(&input.table, &partition_id)
                .await
                .ok_or_else(|| {
                    async_graphql::Error::new(format!(
                        "Partition {} not found for table {}",
                        partition_id, input.table
                    ))
                })?;

            // 4. 插入单条数据
            partition
                .upsert_json(&[doc.clone()])
                .map_err(|e| async_graphql::Error::new(format!("Insert failed: {}", e)))?;

            total_inserted += 1;
        }

        Ok(InsertResult {
            success: true,
            rows_inserted: total_inserted,
            message: format!("Successfully inserted {} rows", total_inserted),
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
        let engine = ctx.data::<Arc<Engine>>()?;

        // 验证文件路径
        let file_path = std::path::PathBuf::from(&input.file_path);
        if !file_path.exists() {
            return Err(async_graphql::Error::new(format!(
                "File does not exist: {}",
                input.file_path
            )));
        }

        // 转换 handler_type（可选）
        let handler_type = input.handler_type.map(|ht| ht.into());

        // 调用 engine 的 load_segment 方法
        let doc_count = engine
            .load_segment(
                &input.table,
                input.partition_name.clone(),
                file_path,
                handler_type,
            )
            .await
            .map_err(|e| async_graphql::Error::new(format!("Load segment failed: {}", e)))?;

        Ok(LoadSegmentResult {
            success: true,
            documents_loaded: doc_count,
            partition_name: input.partition_name,
            message: format!(
                "Successfully loaded {} documents from {}",
                doc_count, input.file_path
            ),
        })
    }
}

// ===== GraphQL Server =====

/// GraphQL 服务器
pub struct GraphQLServer {
    engine: Arc<Engine>,
}

impl GraphQLServer {
    pub fn new(engine: Arc<Engine>) -> Self {
        Self { engine }
    }

    /// 启动 GraphQL 服务器
    pub async fn start(self, addr: &str) -> Result<(), std::io::Error> {
        // 创建 GraphQL Schema
        let graphql_schema = create_schema(self.engine.clone());

        // GraphQL endpoint
        let graphql_endpoint = async_graphql_poem::GraphQL::new(graphql_schema);

        // 构建路由
        let app = Route::new()
            .at("/", get(root))
            .at("/health", get(health))
            .at("/graphql", post(graphql_endpoint))
            .at(
                "/playground",
                get(poem::endpoint::make_sync(move |_| {
                    poem::web::Html(
                        r#"
                        <!DOCTYPE html>
                        <html>
                        <head>
                            <title>GraphQL Playground</title>
                            <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/graphql-playground-react/build/static/css/index.css" />
                            <script src="https://cdn.jsdelivr.net/npm/graphql-playground-react/build/static/js/middleware.js"></script>
                        </head>
                        <body>
                            <div id="root"></div>
                            <script>
                                window.addEventListener('load', function() {
                                    GraphQLPlayground.init(document.getElementById('root'), {
                                        endpoint: '/graphql'
                                    })
                                })
                            </script>
                        </body>
                        </html>
                        "#.to_string()
                    )
                })),
            )
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
        "playground": "/playground"
    }))
}
