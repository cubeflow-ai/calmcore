# GraphQL DDL 完整示例

## 概述

Calm 使用 **GraphQL 作为唯一的 DDL (数据定义语言) 接口**。这样设计是因为:

1. **分区策略复杂**: Calm 的分区系统 (Hash/Range/Custom/None) 无法映射到标准 SQL DDL
2. **段管理配置丰富**: 持久化策略、段大小等是 Calm 特有的
3. **字段类型完整**: 支持 14 种数值类型,标准 SQL 难以完全表达
4. **避免兼容性陷阱**: SQL/ES DDL 语法差异大,维护多套实现得不偿失

## 完整功能列表

### 表级配置

| 字段 | 类型 | 必填 | 默认值 | 说明 |
|------|------|------|--------|------|
| `name` | String | ✅ | - | 表名 |
| `primaryKey` | String | ❌ | null | 主键字段名 |
| `description` | String | ❌ | null | **✨ 新增** 表描述/注释 |
| `partitionStrategy` | PartitionStrategyInput | ❌ | Hash(1 partition) | 分区策略 |
| `fields` | [FieldInput!]! | ✅ | - | 字段列表 |
| `storeSource` | Boolean | ❌ | true | 是否存储原始 JSON |
| `persistPolicy` | PersistPolicyInput | ❌ | default | 持久化策略 |

### 字段级配置

| 字段 | 类型 | 必填 | 默认值 | 说明 |
|------|------|------|--------|------|
| `name` | String | ✅ | - | 字段名 |
| `fieldType` | FieldTypeEnum | ✅ | - | 字段类型 |
| `indexed` | Boolean | ❌ | true | 是否建立索引 |
| `description` | String | ❌ | null | **✨ 新增** 字段描述/注释 |
| `defaultValue` | String | ❌ | null | **✨ 新增** 默认值(JSON 格式) |
| `nullable` | Boolean | ❌ | true | **✨ 新增** 是否可为空 |
| `caseSensitive` | Boolean | ❌ | true | 是否区分大小写(仅 Keyword) |
| `isArray` | Boolean | ❌ | false | **✨ 新增** 是否为数组类型(仅 Keyword) |
| `format` | String | ❌ | null | 时间格式(仅 Timestamp) |

### 支持的字段类型

| 类型 | 别名 | 说明 |
|------|------|------|
| `I8` | - | 8位有符号整数 (-128 ~ 127) |
| `I16` | - | 16位有符号整数 |
| `I32` | `INTEGER` | 32位有符号整数 |
| `I64` | `LONG` | 64位有符号整数 |
| `U8` | - | 8位无符号整数 (0 ~ 255) |
| `U16` | - | 16位无符号整数 |
| `U32` | - | 32位无符号整数 |
| `U64` | - | 64位无符号整数 |
| `F32` | `FLOAT` | 32位浮点数 |
| `F64` | `DOUBLE` | 64位浮点数 |
| `BOOLEAN` | `BOOL` | 布尔类型 |
| `KEYWORD` | `TEXT` | 关键字/文本类型 |
| `TIMESTAMP` | `DATETIME` | 时间戳类型(毫秒) |

### 分区策略

| 策略 | 参数 | 说明 | 使用场景 |
|------|------|------|----------|
| `PKHash` | numPartitions | 基于主键哈希分配 | 适合有主键的表,自动负载均衡 |
| `HASH` | field, numPartitions | 基于指定字段哈希分配 | 无主键表,均衡负载 |
| `RANGE` | field, start, step, numPartitions | 基于范围划分(按需创建) | 时序数据,支持 Int64/Timestamp 字段 |
| `CUSTOM` | - | 用户自定义分区名 | 特殊业务需求,手动指定分区 |
| `NONE` | - | 单分区 | 小表,测试环境 |

- **Range 分区说明:**
- `start`: 起始值(毫秒时间戳或整数)
- `step`: 步长(例如: 86400000 = 1天)
- `numPartitions`: 预创建的分区数量
- 分区按需创建,命名格式: `partition_{start_value}`
- 支持字段类型: Int64, Timestamp(毫秒/秒/微秒/纳秒)

### 持久化策略

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `maxDocsPerSegment` | Int | 100000 | 段内文档数阈值 |
| `maxSegmentAgeSecs` | Int | 300 | 段存活时间阈值(秒) |

---

## 完整示例

### 示例 1: 用户表 (带描述和默认值)

```graphql
mutation {
  createTable(input: {
    name: "users"
    description: "用户信息表,存储所有注册用户的基本信息"
    primaryKey: "user_id"
    
    # Hash 分区,基于 user_id,4 个分区
    partitionStrategy: {
      hash: {
        field: "user_id"
        numPartitions: 4
      }
    }
    
    fields: [
      { 
        name: "user_id"
        fieldType: U64
        indexed: true
        description: "用户唯一标识符"
        nullable: false
      }
      { 
        name: "username"
        fieldType: KEYWORD
        indexed: true
        caseSensitive: false
        description: "用户名,不区分大小写"
        nullable: false
      }
      { 
        name: "email"
        fieldType: KEYWORD
        indexed: true
        caseSensitive: false
        description: "用户邮箱"
        nullable: false
      }
      { 
        name: "age"
        fieldType: I8
        indexed: true
        description: "用户年龄"
        defaultValue: "18"
        nullable: true
      }
      { 
        name: "balance"
        fieldType: F64
        indexed: false
        description: "账户余额"
        defaultValue: "0.0"
        nullable: false
      }
      { 
        name: "is_active"
        fieldType: BOOLEAN
        indexed: true
        description: "账户是否激活"
        defaultValue: "true"
        nullable: false
      }
      { 
        name: "tags"
        fieldType: KEYWORD
        indexed: true
        isArray: true
        description: "用户标签列表"
        nullable: true
      }
      { 
        name: "created_at"
        fieldType: TIMESTAMP
        indexed: true
        format: "iso8601"
        description: "账户创建时间"
        nullable: false
      }
      { 
        name: "last_login_at"
        fieldType: TIMESTAMP
        indexed: true
        format: "iso8601"
        description: "最后登录时间"
        nullable: true
      }
    ]
    
    storeSource: true
    
    persistPolicy: {
      maxDocsPerSegment: 100000
      maxSegmentAgeSecs: 300
    }
  }) {
    name
    partitionCount
    fields {
      name
      fieldType
      indexed
    }
  }
}
```

**返回:**
```json
{
  "data": {
    "createTable": {
      "name": "users",
      "partitionCount": 4,
      "fields": [
        { "name": "user_id", "fieldType": "U64", "indexed": true },
        { "name": "username", "fieldType": "Keyword", "indexed": true },
        { "name": "email", "fieldType": "Keyword", "indexed": true },
        { "name": "age", "fieldType": "I8", "indexed": true },
        { "name": "balance", "fieldType": "F64", "indexed": false },
        { "name": "is_active", "fieldType": "Boolean", "indexed": true },
        { "name": "tags", "fieldType": "Keyword", "indexed": true },
        { "name": "created_at", "fieldType": "Timestamp", "indexed": true },
        { "name": "last_login_at", "fieldType": "Timestamp", "indexed": true }
      ]
    }
  }
}
```

---

### 示例 2: 日志表 (Range 分区按时间)

```graphql
mutation {
  createTable(input: {
    name: "access_logs"
    description: "访问日志表,按天分区"
    primaryKey: "log_id"
    
    # Range 分区,按时间戳分区
    partitionStrategy: {
      range: {
        field: "timestamp"
        start: 1704067200000         # 2024-01-01 00:00:00 UTC
        step: 86400000               # 1 天(毫秒)
        numPartitions: 30            # 按需创建 30 个分区
        parallelism: 1
      }
    }
    
    fields: [
      { 
        name: "log_id"
        fieldType: U64
        indexed: true
        description: "日志唯一 ID"
        nullable: false
      }
      { 
        name: "timestamp"
        fieldType: TIMESTAMP
        indexed: true
        format: "iso8601"
        description: "访问时间"
        nullable: false
      }
      { 
        name: "user_id"
        fieldType: U64
        indexed: true
        description: "访问用户 ID"
        nullable: true
      }
      { 
        name: "path"
        fieldType: KEYWORD
        indexed: true
        description: "访问路径"
        nullable: false
      }
      { 
        name: "status_code"
        fieldType: I16
        indexed: true
        description: "HTTP 状态码"
        nullable: false
      }
      { 
        name: "response_time_ms"
        fieldType: F32
        indexed: false
        description: "响应时间(毫秒)"
        nullable: true
      }
    ]
    
    storeSource: true
    
    persistPolicy: {
      maxDocsPerSegment: 500000  # 日志量大,增加段大小
      maxSegmentAgeSecs: 600     # 10 分钟持久化
    }
  }) {
    name
    partitionCount
  }
}
```

---

### 示例 3: 商品表 (PKHash 分区)

```graphql
mutation {
  createTable(input: {
    name: "products"
    description: "商品信息表,按主键自动分区"
    primaryKey: "product_id"
    
    # PKHash 分区,基于主键 product_id 自动哈希
    partitionStrategy: {
      pkHash: {
        numPartitions: 4
      }
    }
    
    fields: [
      { 
        name: "product_id"
        fieldType: U64
        description: "商品 ID"
      }
      { 
        name: "name"
        fieldType: KEYWORD
        description: "商品名称"
      }
      { 
        name: "price"
        fieldType: F64
        description: "商品价格"
        defaultValue: "0.0"
      }
      { 
        name: "stock"
        fieldType: I32
        description: "库存数量"
        defaultValue: "0"
      }
      { 
        name: "on_sale"
        fieldType: BOOLEAN
        description: "是否在售"
        defaultValue: "true"
      }
    ]
  }) {
    name
    partitionCount
  }
}
```

---

### 示例 4: 性能监控表 (所有数值类型)

```graphql
mutation {
  createTable(input: {
    name: "metrics"
    description: "系统性能指标表,展示所有数值类型"
    primaryKey: "metric_id"
    partitionStrategy: {
      hash: {
        field: "metric_id"
        numPartitions: 2
      }
    }
    
    fields: [
      { name: "metric_id", fieldType: U64, description: "指标 ID" }
      { name: "cpu_usage_i8", fieldType: I8, description: "CPU 使用率(0-100)" }
      { name: "memory_usage_i16", fieldType: I16, description: "内存使用(MB)" }
      { name: "disk_usage_i32", fieldType: I32, description: "磁盘使用(MB)" }
      { name: "network_bytes_i64", fieldType: I64, description: "网络传输字节数" }
      { name: "port_u8", fieldType: U8, description: "端口号(0-255)" }
      { name: "process_id_u16", fieldType: U16, description: "进程 ID" }
      { name: "session_id_u32", fieldType: U32, description: "会话 ID" }
      { name: "transaction_id_u64", fieldType: U64, description: "事务 ID" }
      { name: "latency_f32", fieldType: F32, description: "延迟(ms)" }
      { name: "throughput_f64", fieldType: F64, description: "吞吐量(MB/s)" }
      { name: "is_healthy", fieldType: BOOLEAN, description: "健康状态" }
      { name: "timestamp", fieldType: TIMESTAMP, format: "iso8601", description: "采集时间" }
    ]
  }) {
    name
  }
}
```

---

## 字段描述 (description) 的用途

新增的 `description` 字段非常有用:

1. **自文档化**: 表和字段含义清晰,无需额外文档
2. **团队协作**: 新成员快速理解数据模型
3. **工具支持**: 可以生成 API 文档、ER 图
4. **运维友好**: SHOW CREATE TABLE 时显示注释

**示例: 查询表结构时显示描述**

```graphql
query {
  table(name: "users") {
    name
    description  # 显示: "用户信息表,存储所有注册用户的基本信息"
    fields {
      name
      fieldType
      indexed
      # description  # 后续可以添加到 Field 类型中
    }
  }
}
```

---

## 默认值 (defaultValue) 的用途

新增的 `defaultValue` 字段实现:

1. **插入时自动填充**: INSERT 时未提供值,自动使用默认值
2. **业务逻辑**: balance 默认 0.0, is_active 默认 true
3. **数据完整性**: 避免插入 null 导致的查询问题

**示例: 插入数据时使用默认值**

```sql
-- 只插入必填字段,其他字段使用默认值
INSERT INTO users (user_id, username, email) VALUES (1, 'alice', 'alice@example.com');

-- age 自动设为 18
-- balance 自动设为 0.0
-- is_active 自动设为 true
```

---

## 可空性 (nullable) 的用途

新增的 `nullable` 字段实现:

1. **数据约束**: nullable=false 时,插入 null 值会报错
2. **查询优化**: 非空字段可以优化查询计划
3. **类型安全**: 配合默认值使用,确保数据完整性

**示例: 约束检查**

```sql
-- ❌ 错误: user_id 是 nullable=false
INSERT INTO users (username, email) VALUES ('bob', 'bob@example.com');
-- Error: Field 'user_id' cannot be null

-- ✅ 正确
INSERT INTO users (user_id, username, email) VALUES (2, 'bob', 'bob@example.com');
```

---

## 数组字段 (isArray) 的用途

新增的 `isArray` 字段 (仅 Keyword 类型) 实现:

1. **标签系统**: 用户标签、商品分类
2. **权限管理**: 用户角色列表
3. **多值属性**: 一个字段存储多个值

**示例: 数组字段使用**

```graphql
# 创建带数组字段的表
mutation {
  createTable(input: {
    name: "posts"
    fields: [
      { name: "post_id", fieldType: U64 }
      { name: "title", fieldType: KEYWORD }
      { 
        name: "tags"
        fieldType: KEYWORD
        isArray: true      # ✨ 标记为数组
        description: "文章标签列表"
      }
    ]
  }) { name }
}

# 插入数据
INSERT INTO posts VALUES (1, 'Hello World', ['rust', 'database', 'tutorial']);

# 查询
SELECT * FROM posts WHERE ARRAY_CONTAINS(tags, 'rust');
```

---

## 与 SQL/ES 的对比

### SQL CREATE TABLE (不支持 ❌)

```sql
-- ❌ Calm 不支持 SQL 建表
CREATE TABLE users (
    user_id BIGINT UNSIGNED PRIMARY KEY,
    username VARCHAR(255),
    age TINYINT DEFAULT 18
);

-- 返回错误提示:
-- Error: CREATE TABLE is not supported in SQL.
-- Please use GraphQL API: http://localhost:9567/playground
```

### ES PUT /<index> (不支持 ❌)

```bash
# ❌ Calm 不支持 ES 建索引
PUT /users
{
  "mappings": {
    "properties": {
      "user_id": { "type": "long" },
      "username": { "type": "keyword" }
    }
  }
}

# 返回错误提示:
# Error: PUT /<index> is not supported.
# Please use GraphQL API or POST /<index>/_doc to insert directly.
```

### GraphQL (唯一支持 ✅)

```graphql
# ✅ 只有 GraphQL 支持建表
mutation {
  createTable(input: {
    name: "users"
    description: "用户表"
    fields: [
      { name: "user_id", fieldType: U64, description: "用户 ID" }
      { name: "username", fieldType: KEYWORD, description: "用户名" }
      { name: "age", fieldType: I8, defaultValue: "18", description: "年龄" }
    ]
  }) { name }
}
```

---

## 推荐工作流

```
┌─────────────────────────────────────────────────────────┐
│                   Calm 数据操作流程                       │
└─────────────────────────────────────────────────────────┘

1️⃣  建表 (DDL)
   ├─ ✅ GraphQL mutation createTable
   └─ ❌ SQL CREATE TABLE (不支持)

2️⃣  插入数据 (DML)
   ├─ ✅ SQL INSERT INTO
   ├─ ✅ ES POST /<index>/_doc
   └─ ✅ GraphQL mutation insertData

3️⃣  查询数据 (DQL)
   ├─ ✅ SQL SELECT
   ├─ ✅ ES GET /<index>/_search
   └─ ✅ GraphQL query

4️⃣  管理操作 (DDL)
   ├─ ✅ GraphQL mutation dropTable
   ├─ ✅ GraphQL mutation flushTable
   └─ ✅ GraphQL query tables
```

---

## 总结

### ✨ 新增功能对比

| 功能 | 之前 | 现在 (v0.2) |
|------|------|------------|
| 表描述 | ❌ | ✅ description |
| 字段描述 | ❌ | ✅ description |
| 默认值 | ❌ | ✅ defaultValue |
| 可空性 | ❌ | ✅ nullable |
| 数组字段 | ❌ | ✅ isArray (Keyword) |
| 字段类型 | 13 种 | 13 种 (支持别名) |
| 分区策略 | ✅ | ✅ 完整支持 |
| 持久化策略 | ✅ | ✅ 完整支持 |

### 🎯 设计原则

1. **单一入口**: GraphQL 是唯一的 DDL 接口
2. **完整性**: 支持所有 Calm 特性(分区、持久化、14 种类型)
3. **自文档化**: description 字段使数据模型清晰易懂
4. **业务友好**: defaultValue、nullable 满足实际需求
5. **灵活性**: isArray 支持多值属性

### 📚 相关文档

- [DDL 设计分析](./DDL_DESIGN_ANALYSIS.md) - 详细的设计思路
- [GraphQL Playground](http://localhost:9567/playground) - 交互式测试
- [快速开始指南](../README.md) - 快速上手

---

**版本**: v0.2.0  
**更新时间**: 2025-11-30  
**维护**: Calm Team
