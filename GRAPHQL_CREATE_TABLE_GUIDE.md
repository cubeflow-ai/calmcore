# GraphQL 创建表完整指南

本文档说明如何使用 GraphQL API 创建表，包括完整的分区策略支持。

## 目录

- [基本用法](#基本用法)
- [分区策略详解](#分区策略详解)
  - [Hash 分区](#hash-分区)
  - [Range 分区](#range-分区)
  - [List 分区](#list-分区)
  - [Custom 分区](#custom-分区)
  - [None 分区](#none-分区)
- [字段类型](#字段类型)
- [完整示例](#完整示例)

## 基本用法

### 简单创建表（使用默认 Hash 分区）

```graphql
mutation {
  createTable(input: {
    name: "users"
    primaryKey: "user_id"
    partitionCount: 4
    fields: [
      { name: "user_id", fieldType: KEYWORD }
      { name: "name", fieldType: KEYWORD }
      { name: "age", fieldType: I32 }
      { name: "email", fieldType: KEYWORD, caseSensitive: false }
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

## 分区策略详解

### Hash 分区

根据字段值的哈希值分配分区，适合均匀分布的数据。

**特点：**
- 数据自动均匀分布
- 适合高并发写入
- Partition ID 格式：19 位零填充数字（如 `0000000000000000000`）

**示例：**

```graphql
mutation {
  createTable(input: {
    name: "orders"
    primaryKey: "order_id"
    fields: [
      { name: "order_id", fieldType: KEYWORD }
      { name: "user_id", fieldType: KEYWORD }
      { name: "amount", fieldType: F64 }
      { name: "status", fieldType: KEYWORD }
    ]
    partitionStrategy: {
      strategyType: HASH
      field: "user_id"
      numPartitions: 8
    }
  }) {
    name
    partitionCount
  }
}
```

### Range 分区

根据字段值的范围分配分区，适合时序数据或有序数据。

**特点：**
- 按值范围划分
- 适合范围查询
- Partition ID 格式：`start_end`（如 `0_100`、`100_200`）

**示例 1：整数范围分区**

```graphql
mutation {
  createTable(input: {
    name: "events"
    primaryKey: "event_id"
    fields: [
      { name: "event_id", fieldType: I64 }
      { name: "timestamp", fieldType: TIMESTAMP }
      { name: "event_type", fieldType: KEYWORD }
    ]
    partitionStrategy: {
      strategyType: RANGE
      field: "event_id"
      ranges: [
        { start: { intValue: 0 }, end: { intValue: 1000 }, partitionId: 0 }
        { start: { intValue: 1000 }, end: { intValue: 2000 }, partitionId: 1 }
        { start: { intValue: 2000 }, end: { intValue: 3000 }, partitionId: 2 }
      ]
    }
  }) {
    name
    partitionCount
  }
}
```

**示例 2：时间戳范围分区**

```graphql
mutation {
  createTable(input: {
    name: "logs"
    primaryKey: "log_id"
    fields: [
      { name: "log_id", fieldType: KEYWORD }
      { name: "timestamp", fieldType: I64 }
      { name: "level", fieldType: KEYWORD }
      { name: "message", fieldType: KEYWORD }
    ]
    partitionStrategy: {
      strategyType: RANGE
      field: "timestamp"
      ranges: [
        { 
          start: { intValue: 1700000000000 }, 
          end: { intValue: 1700086400000 }, 
          partitionId: 0 
        }
        { 
          start: { intValue: 1700086400000 }, 
          end: { intValue: 1700172800000 }, 
          partitionId: 1 
        }
      ]
    }
  }) {
    name
    partitionCount
  }
}
```

**示例 3：字符串范围分区**

```graphql
mutation {
  createTable(input: {
    name: "products"
    primaryKey: "product_id"
    fields: [
      { name: "product_id", fieldType: KEYWORD }
      { name: "category", fieldType: KEYWORD }
      { name: "name", fieldType: KEYWORD }
    ]
    partitionStrategy: {
      strategyType: RANGE
      field: "category"
      ranges: [
        { 
          start: { stringValue: "A" }, 
          end: { stringValue: "M" }, 
          partitionId: 0 
        }
        { 
          start: { stringValue: "M" }, 
          end: { stringValue: "Z" }, 
          partitionId: 1 
        }
      ]
    }
  }) {
    name
    partitionCount
  }
}
```

### List 分区

根据字段的具体值分配分区，适合枚举值或分类数据。

**特点：**
- 明确指定值到分区的映射
- 适合分类数据
- Partition ID 格式：清理后的值（如 `active`、`pending`）

**示例：**

```graphql
mutation {
  createTable(input: {
    name: "tasks"
    primaryKey: "task_id"
    fields: [
      { name: "task_id", fieldType: KEYWORD }
      { name: "status", fieldType: KEYWORD }
      { name: "priority", fieldType: I32 }
    ]
    partitionStrategy: {
      strategyType: LIST
      field: "status"
      valueMapping: "pending:0,active:1,completed:2,failed:3"
    }
  }) {
    name
    partitionCount
  }
}
```

**值映射格式：** `"value1:partition_id,value2:partition_id,value3:partition_id"`

### Custom 分区

用户自定义分区，需要通过 `loadSegment` 手动加载数据。

**特点：**
- 完全自定义分区逻辑
- 适合复杂业务场景
- 需要手动管理分区和数据加载

**示例：**

```graphql
mutation {
  createTable(input: {
    name: "external_data"
    primaryKey: "id"
    fields: [
      { name: "id", fieldType: KEYWORD }
      { name: "value", fieldType: I64 }
      { name: "metadata", fieldType: KEYWORD }
    ]
    partitionStrategy: {
      strategyType: CUSTOM
    }
  }) {
    name
    partitionCount
  }
}
```

**加载数据：**

```graphql
mutation {
  loadSegment(input: {
    table: "external_data"
    partitionId: "custom_partition_1"
    filePath: "/path/to/data.parquet"
    handlerType: REFERENCE
  }) {
    success
    documentsLoaded
    message
  }
}
```

### None 分区

所有数据存储在单个分区，适合小型表。

**特点：**
- 无分区开销
- 适合小数据量
- Partition ID：表名

**示例：**

```graphql
mutation {
  createTable(input: {
    name: "config"
    primaryKey: "key"
    fields: [
      { name: "key", fieldType: KEYWORD }
      { name: "value", fieldType: KEYWORD }
    ]
    partitionStrategy: {
      strategyType: NONE
    }
  }) {
    name
    partitionCount
  }
}
```

## 字段类型

### 支持的字段类型

| GraphQL 类型 | Rust 类型 | 说明 | 别名 |
|-------------|----------|------|-----|
| `KEYWORD` | String | 关键字/文本 | `TEXT` |
| `I8` | i8 | 8位有符号整数 | - |
| `I16` | i16 | 16位有符号整数 | - |
| `I32` | i32 | 32位有符号整数 | `INTEGER` |
| `I64` | i64 | 64位有符号整数 | `LONG` |
| `U8` | u8 | 8位无符号整数 | - |
| `U16` | u16 | 16位无符号整数 | - |
| `U32` | u32 | 32位无符号整数 | - |
| `U64` | u64 | 64位无符号整数 | - |
| `F32` | f32 | 32位浮点数 | `FLOAT` |
| `F64` | f64 | 64位浮点数 | `DOUBLE` |
| `BOOLEAN` | bool | 布尔类型 | `BOOL` |
| `TIMESTAMP` | i64 | 时间戳(毫秒) | `DATETIME` |

### 字段特定配置

#### KEYWORD 字段

```graphql
fields: [
  { 
    name: "email"
    fieldType: KEYWORD
    indexed: true
    caseSensitive: false  # 大小写敏感
  }
]
```

#### TIMESTAMP 字段

```graphql
fields: [
  { 
    name: "created_at"
    fieldType: TIMESTAMP
    indexed: true
    format: "%Y-%m-%d %H:%M:%S"  # 时间格式
  }
]
```

## 完整示例

### 电商订单表（Hash 分区）

```graphql
mutation {
  createTable(input: {
    name: "orders"
    primaryKey: "order_id"
    fields: [
      { name: "order_id", fieldType: KEYWORD }
      { name: "user_id", fieldType: KEYWORD }
      { name: "product_id", fieldType: KEYWORD }
      { name: "quantity", fieldType: I32 }
      { name: "price", fieldType: F64 }
      { name: "status", fieldType: KEYWORD }
      { name: "created_at", fieldType: TIMESTAMP }
    ]
    partitionStrategy: {
      strategyType: HASH
      field: "user_id"
      numPartitions: 16
    }
  }) {
    name
    partitionCount
    primaryKey
    fields {
      name
      fieldType
    }
  }
}
```

### 日志表（Range 分区按时间）

```graphql
mutation {
  createTable(input: {
    name: "application_logs"
    primaryKey: "log_id"
    fields: [
      { name: "log_id", fieldType: KEYWORD }
      { name: "timestamp", fieldType: I64 }
      { name: "level", fieldType: KEYWORD }
      { name: "service", fieldType: KEYWORD }
      { name: "message", fieldType: KEYWORD }
    ]
    partitionStrategy: {
      strategyType: RANGE
      field: "timestamp"
      ranges: [
        # 2024-01-01 to 2024-01-02
        { 
          start: { intValue: 1704067200000 }, 
          end: { intValue: 1704153600000 }, 
          partitionId: 0 
        }
        # 2024-01-02 to 2024-01-03
        { 
          start: { intValue: 1704153600000 }, 
          end: { intValue: 1704240000000 }, 
          partitionId: 1 
        }
        # 2024-01-03 to 2024-01-04
        { 
          start: { intValue: 1704240000000 }, 
          end: { intValue: 1704326400000 }, 
          partitionId: 2 
        }
      ]
    }
  }) {
    name
    partitionCount
  }
}
```

### 用户表（List 分区按地区）

```graphql
mutation {
  createTable(input: {
    name: "users_by_region"
    primaryKey: "user_id"
    fields: [
      { name: "user_id", fieldType: KEYWORD }
      { name: "name", fieldType: KEYWORD }
      { name: "region", fieldType: KEYWORD }
      { name: "email", fieldType: KEYWORD, caseSensitive: false }
      { name: "age", fieldType: I32 }
    ]
    partitionStrategy: {
      strategyType: LIST
      field: "region"
      valueMapping: "us-west:0,us-east:1,eu-west:2,ap-southeast:3"
    }
  }) {
    name
    partitionCount
  }
}
```

### 配置表（None 分区）

```graphql
mutation {
  createTable(input: {
    name: "system_config"
    primaryKey: "config_key"
    fields: [
      { name: "config_key", fieldType: KEYWORD }
      { name: "config_value", fieldType: KEYWORD }
      { name: "updated_at", fieldType: TIMESTAMP }
    ]
    partitionStrategy: {
      strategyType: NONE
    }
  }) {
    name
    partitionCount
  }
}
```

## 分区策略选择指南

| 场景 | 推荐策略 | 原因 |
|-----|---------|------|
| 高并发写入 | Hash | 数据均匀分布，避免热点 |
| 时序数据 | Range | 方便按时间范围查询 |
| 分类数据 | List | 明确的分类边界 |
| 地理数据 | List/Range | 按地区或经纬度分区 |
| 外部数据导入 | Custom | 灵活的数据加载 |
| 小型表 | None | 减少分区开销 |

## 注意事项

1. **Hash 分区**：
   - `field` 和 `numPartitions` 必需
   - 建议分区数为 2 的幂（如 4、8、16）

2. **Range 分区**：
   - `field` 和 `ranges` 必需
   - 范围不能重叠
   - 起始值包含，结束值不包含（左闭右开）

3. **List 分区**：
   - `field` 和 `valueMapping` 必需
   - 值映射格式：`"value1:0,value2:1"`
   - 未映射的值无法插入

4. **Custom 分区**：
   - 需要手动使用 `loadSegment` 加载数据
   - 适合批量导入场景

5. **None 分区**：
   - 所有数据在一个分区
   - 不适合大数据量

## 查看表信息

创建表后，可以查看详细信息：

```graphql
query {
  tableDetail(name: "orders") {
    name
    partitionCount
    totalSegments
    totalDocuments
    primaryKey
    fields {
      name
      fieldType
      indexed
    }
    partitions {
      partitionId
      segmentCount
    }
  }
}
```

## 相关文档

- [GraphQL 查询接口指南](./GRAPHQL_QUERY_GUIDE.md)
- [Custom 分区加载指南](./CUSTOM_PARTITION_LOAD_GUIDE.md)
- [GraphQL API 参考](./GRAPHQL_QUERY_REFERENCE.md)
