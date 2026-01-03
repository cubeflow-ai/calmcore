# GraphQL createTable 完整使用指南

## 概览

`createTable` 是 Calm 的核心 mutation,用于创建新表。本指南提供完整的使用说明、最佳实践和常见场景示例。

## 目录

1. [快速开始](#快速开始)
2. [参数说明](#参数说明)
3. [常见场景](#常见场景)
4. [分区策略选择](#分区策略选择)
5. [字段类型选择](#字段类型选择)
6. [最佳实践](#最佳实践)
7. [故障排查](#故障排查)

---

## 快速开始

### 最简单的表

```graphql
mutation {
  createTable(input: {
    name: "users"
    fields: [
      { name: "user_id", fieldType: U64 }
      { name: "username", fieldType: KEYWORD }
    ]
  }) {
    name
    partitionCount
  }
}
```

**说明:**
- 表名: `users`
- 字段: `user_id` (U64), `username` (KEYWORD)
- 分区: 默认 1 个(无分区)
- 主键: 默认第一个字段(`user_id`)

### 推荐配置(生产环境)

```graphql
mutation {
  createTable(input: {
    name: "users"
    description: "用户信息表"
    primaryKey: "user_id"
    partitionCount: 4
    
    fields: [
      {
        name: "user_id"
        fieldType: U64
        description: "用户唯一标识符"
        indexed: true
        nullable: false
      }
      {
        name: "username"
        fieldType: KEYWORD
        description: "用户名(不区分大小写)"
        indexed: true
        caseSensitive: false
        nullable: false
      }
      {
        name: "email"
        fieldType: KEYWORD
        description: "用户邮箱"
        indexed: true
        nullable: true
      }
      {
        name: "age"
        fieldType: I8
        description: "用户年龄(0-127)"
        indexed: true
        defaultValue: "18"
        nullable: true
      }
      {
        name: "created_at"
        fieldType: TIMESTAMP
        description: "创建时间"
        indexed: true
        nullable: false
      }
    ]
  }) {
    name
    partitionCount
    primaryKey
    fields {
      name
      fieldType
      indexed
    }
  }
}
```

---

## 参数说明

### CreateTableInput 字段

| 字段 | 类型 | 必填 | 默认值 | 说明 |
|------|------|------|--------|------|
| `name` | String | ✅ | - | 表名(小写字母+数字+下划线) |
| `fields` | [FieldInput] | ✅ | - | 字段列表(至少 1 个) |
| `primaryKey` | String | ❌ | 第一个字段 | 主键字段名 |
| `description` | String | ❌ | null | 表描述(强烈推荐) |
| `partitionStrategy` | PartitionStrategyInput | ❌ | null | 高级分区配置 |
| `storeSource` | bool | ❌ | true | 是否存储原始 JSON |
| `persistPolicy` | PersistPolicyInput | ❌ | 默认 | 持久化策略 |

### FieldInput 字段

| 字段 | 类型 | 必填 | 默认值 | 说明 |
|------|------|------|--------|------|
| `name` | String | ✅ | - | 字段名(小写字母+数字+下划线) |
| `fieldType` | FieldTypeEnum | ✅ | - | 字段类型(见下方) |
| `indexed` | bool | ❌ | true | 是否索引(true=可查询) |
| `description` | String | ❌ | null | 字段描述(推荐填写) |
| `nullable` | bool | ❌ | true | 是否可为空(false=必填) |
| `defaultValue` | String | ❌ | null | 默认值(JSON 字符串) |
| `caseSensitive` | bool | ❌ | true | 是否区分大小写(仅 KEYWORD) |
| `isArray` | bool | ❌ | false | 是否数组(仅 KEYWORD) |
| `format` | String | ❌ | null | 时间格式(仅 TIMESTAMP) |

---

## 常见场景

### 场景 1: 用户表

```graphql
mutation {
  createTable(input: {
    name: "users"
    description: "用户信息表"
    primaryKey: "user_id"
    partitionStrategy: {
      pkHash: { numPartitions: 4 }
    }
    
    fields: [
      { name: "user_id", fieldType: U64, nullable: false }
      { name: "username", fieldType: KEYWORD, caseSensitive: false, nullable: false }
      { name: "email", fieldType: KEYWORD, nullable: true }
      { name: "age", fieldType: I8, defaultValue: "18", nullable: true }
      { name: "tags", fieldType: KEYWORD, isArray: true, nullable: true }
      { name: "created_at", fieldType: TIMESTAMP, nullable: false }
    ]
  }) { name }
}
```

**特点:**
- Hash 分区(user_id)
- 用户名不区分大小写
- 年龄有默认值 18
- 标签字段支持数组

### 场景 2: 订单表(时序数据)

```graphql
mutation {
  createTable(input: {
    name: "orders"
    description: "订单表"
    primaryKey: "order_id"
    
    partitionStrategy: {
      range: {
        field: "order_time"
        start: 1704067200000        # 起始毫秒
        step: 2592000000            # 30 天 (毫秒)
        numPartitions: 2            # 创建 2 个范围分区
        parallelism: 1              # 可选
      }
    }
    
    fields: [
      { name: "order_id", fieldType: U64, nullable: false }
      { name: "user_id", fieldType: U64, indexed: true, nullable: false }
      { name: "amount", fieldType: F64, nullable: false }
      { name: "status", fieldType: KEYWORD, nullable: false }
      { name: "order_time", fieldType: TIMESTAMP, nullable: false }
    ]
  }) { name }
}
```

**特点:**
- Range 分区(按月划分)
- 适合时间范围查询
- 金额使用 F64

### 场景 3: 日志表(高吞吐)

```graphql
mutation {
  createTable(input: {
    name: "logs"
    description: "应用日志表"
    primaryKey: "log_id"
    partitionStrategy: {
      hash: {
        field: "log_id"
        numPartitions: 8
      }
    }  # 高并发,更多分区
    
    persistPolicy: {
      maxDocsPerSegment: 500000  # 50 万文档持久化
      maxSegmentAgeSecs: 600      # 10 分钟持久化
    }
    
    fields: [
      { name: "log_id", fieldType: U64, nullable: false }
      { name: "level", fieldType: KEYWORD, nullable: false }
      { name: "message", fieldType: TEXT, nullable: false }
      { name: "service", fieldType: KEYWORD, nullable: false }
      { name: "timestamp", fieldType: TIMESTAMP, nullable: false }
    ]
  }) { name }
}
```

**特点:**
- 8 个分区(高并发)
- 持久化阈值提高(降低 I/O 频率)
- message 使用 TEXT(支持全文检索)

### 场景 4: 配置表(小表)

```graphql
mutation {
  createTable(input: {
    name: "config"
    description: "系统配置表"
    primaryKey: "config_key"
    partitionStrategy: { none: {} }  # 小表无需分区
    
    persistPolicy: {
      maxDocsPerSegment: 10000
      maxSegmentAgeSecs: 300
    }
    
    fields: [
      { name: "config_key", fieldType: KEYWORD, nullable: false }
      { name: "config_value", fieldType: KEYWORD, nullable: false }
      { name: "description", fieldType: TEXT, nullable: true }
      { name: "updated_at", fieldType: TIMESTAMP, nullable: false }
    ]
  }) { name }
}
```

**特点:**
- 单分区(数据量小)
- 持久化阈值降低(快速持久化)

---

## 分区策略选择

### Hash 分区(推荐)

**适用场景:**
- ID 类字段(user_id, order_id)
- 均匀分布的数据
- 随机查询

**推荐配置:**
```graphql
partitionStrategy: {
  hash: {
    field: "user_id"
    numPartitions: 4
  }
}
```

**分区数建议:**
- 小表(<100 万): 1-2
- 中表(100 万-1000 万): 4-8
- 大表(>1000 万): 8-16
- 推荐 CPU 核心数或 2^n

### Range 分区

**适用场景:**
- 时序数据(日志、订单)
- 时间范围查询
- 按日期归档

**配置:**
```graphql
partitionStrategy: {
  range: {
    field: "timestamp"
    start: 0
    step: 86400000
    numPartitions: 8
    parallelism: 1
  }
}
```

**注意:**
- 区间左闭右开: [start, end)
- 分区不能重叠
- 分区应连续覆盖所有值

### 无分区(None)

**适用场景:**
- 小表(<100 万行)
- 测试环境
- 配置表

**配置:**
```graphql
partitionStrategy: {
  none: {}
}
```

---

## 字段类型选择

### 整数类型

| 类型 | 范围 | 适用场景 |
|------|------|----------|
| `I8` | -128 ~ 127 | 年龄、百分比 |
| `I16` | -32768 ~ 32767 | 数量、计数 |
| `I32` | -2^31 ~ 2^31-1 | 金额(分)、数量 |
| `I64` | -2^63 ~ 2^63-1 | 时间戳、大数值 |
| `U8` | 0 ~ 255 | 状态码、枚举 |
| `U16` | 0 ~ 65535 | 端口、ID |
| `U32` | 0 ~ 2^32-1 | ID、计数 |
| `U64` | 0 ~ 2^64-1 | 主键 ID、时间戳(毫秒) |

### 浮点类型

| 类型 | 精度 | 适用场景 |
|------|------|----------|
| `F32` | 单精度 | 坐标、比率 |
| `F64` | 双精度 | 金额、精确计算 |

### 文本类型

| 类型 | 索引 | 适用场景 |
|------|------|----------|
| `KEYWORD` | 精确匹配 | 用户名、邮箱、标签 |
| `TEXT` | 全文检索 | 文章内容、评论 |

**KEYWORD 配置:**
```graphql
{
  name: "username"
  fieldType: KEYWORD
  caseSensitive: false  # 不区分大小写
  isArray: false        # 单个值
}
```

**TEXT 配置:**
```graphql
{
  name: "content"
  fieldType: TEXT
  indexed: true  # 支持全文检索
}
```

### 其他类型

| 类型 | 说明 | 示例 |
|------|------|------|
| `BOOLEAN` | 布尔值 | is_active |
| `TIMESTAMP` | 时间戳 | created_at |
| `BYTES` | 二进制 | 文件内容 |

---

## 最佳实践

### 1. 命名规范

✅ **推荐:**
```
users
user_orders
product_reviews
```

❌ **不推荐:**
```
Users          # 大写
user-orders    # 短横线
UserOrders     # 驼峰
```

### 2. 必填字段

```graphql
{
  name: "user_id"
  fieldType: U64
  nullable: false  # 必填
}
```

### 3. 添加描述

```graphql
{
  name: "users"
  description: "用户信息表,存储注册用户的基本信息"
}
```

### 4. 设置默认值

```graphql
{
  name: "age"
  fieldType: I8
  defaultValue: "18"  # JSON 字符串
  nullable: true
}
```

### 5. 选择合适的主键

```graphql
primaryKey: "user_id"  # 唯一、不变、索引
```

### 6. 合理配置分区

```graphql
# 小表
partitionStrategy: { none: {} }

# 中表
partitionStrategy: {
  hash: { field: "user_id", numPartitions: 4 }
}

# 大表
partitionStrategy: {
  hash: { field: "user_id", numPartitions: 8 }
}
```

### 7. 调整持久化策略

```graphql
# 实时查询(低延迟)
persistPolicy: {
  maxDocsPerSegment: 50000
  maxSegmentAgeSecs: 60
}

# 高吞吐写入(高延迟)
persistPolicy: {
  maxDocsPerSegment: 500000
  maxSegmentAgeSecs: 600
}
```

---

## 故障排查

### 错误 1: 表已存在

```
Error: Table 'users' already exists
```

**解决:**
```graphql
mutation {
  dropTable(name: "users")
}
```

### 错误 2: 字段名重复

```
Error: Duplicate field name: 'user_id'
```

**解决:** 检查 `fields` 数组,确保每个字段名唯一。

### 错误 3: 分区数为 0

```
Error: num_partitions must be > 0
```

**解决:**
```graphql
partitionStrategy: {
  hash: { field: "user_id", numPartitions: 4 }
}
```

### 错误 4: Range 分区重叠

```
Error: Range partitions overlap
```

**解决:** 确保区间不重叠,且连续:
```graphql
ranges: [
  { partition_id: 0, start: {intValue: 0}, end: {intValue: 1000} }
  { partition_id: 1, start: {intValue: 1000}, end: {intValue: 2000} }  # 连续
]
```

### 错误 5: 默认值格式错误

```
Error: Invalid defaultValue JSON
```

**解决:** 使用 JSON 字符串格式:
```graphql
defaultValue: "18"        # 整数
defaultValue: "\"text\""  # 字符串(需要转义引号)
defaultValue: "true"      # 布尔
defaultValue: "1.23"      # 浮点
```

---

## MCP 集成提示

本指南的所有示例都可以直接在 GraphQL Playground 中运行。MCP(Model Context Protocol)可以自动识别这些注释和示例,帮助 AI 生成正确的 GraphQL 查询。

**MCP 优化建议:**
1. 使用内联注释描述字段用途
2. 提供完整的代码示例
3. 包含常见场景的模板
4. 说明参数的可选性和默认值

---

## 参考文档

- [GraphQL DDL 完整示例](./graphql_ddl_complete_example.md)
- [DDL 设计分析](./DDL_DESIGN_ANALYSIS.md)
- [GraphQL DDL 增强总结](../GRAPHQL_DDL_ENHANCEMENT_SUMMARY.md)

---

**最后更新:** 2024
**版本:** v1.0
