# GraphQL API 使用指南

## 字段类型及其特定配置

### 基础字段类型
每种字段类型都有自己特定的配置选项:

| 字段类型 | 类型别名 | 通用配置 | 特定配置 |
|---------|---------|---------|---------|
| keyword | text | indexed | case_sensitive (是否区分大小写) |
| timestamp | datetime | indexed | format (时间格式) |
| i32 | integer | indexed | - |
| i64 | long | indexed | - |
| f32 | float | indexed | - |
| f64 | double | indexed | - |
| boolean | bool | indexed | - |

## 创建表示例

### 1. 创建简单表

```graphql
mutation {
  createTable(input: {
    name: "users"
    primaryKey: "id"
    partitionCount: 4
    fields: [
      {
        name: "id"
        fieldType: "keyword"
        indexed: true
      }
      {
        name: "age"
        fieldType: "i32"
        indexed: true
      }
    ]
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

### 2. 创建带字符串配置的表 (不区分大小写)

```graphql
mutation {
  createTable(input: {
    name: "products"
    primaryKey: "sku"
    fields: [
      {
        name: "sku"
        fieldType: "keyword"
        indexed: true
        caseSensitive: true  # 区分大小写 (默认)
      }
      {
        name: "name"
        fieldType: "text"
        indexed: true
        caseSensitive: false  # 不区分大小写,搜索时忽略大小写
      }
      {
        name: "price"
        fieldType: "f64"
        indexed: true
      }
    ]
  }) {
    name
    fields {
      name
      fieldType
    }
  }
}
```

### 3. 创建带时间戳字段的表

```graphql
mutation {
  createTable(input: {
    name: "events"
    primaryKey: "id"
    partitionCount: 8
    fields: [
      {
        name: "id"
        fieldType: "keyword"
        indexed: true
      }
      {
        name: "created_at"
        fieldType: "timestamp"
        indexed: true
        format: null  # 接受任意时间格式 (默认)
      }
      {
        name: "event_time"
        fieldType: "datetime"  # timestamp 的别名
        indexed: true
        format: "iso8601"  # 指定特定格式 (可选)
      }
      {
        name: "message"
        fieldType: "text"
        indexed: true
        caseSensitive: false
      }
      {
        name: "status_code"
        fieldType: "i32"
        indexed: true
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

### 4. 完整的日志分析表

```graphql
mutation {
  createTable(input: {
    name: "app_logs"
    primaryKey: "log_id"
    partitionCount: 16
    fields: [
      {
        name: "log_id"
        fieldType: "keyword"
        indexed: true
      }
      {
        name: "app_name"
        fieldType: "keyword"
        indexed: true
        caseSensitive: false  # 应用名不区分大小写
      }
      {
        name: "level"
        fieldType: "keyword"
        indexed: true
        caseSensitive: false  # ERROR, error, Error 都一样
      }
      {
        name: "timestamp"
        fieldType: "timestamp"
        indexed: true
      }
      {
        name: "message"
        fieldType: "text"
        indexed: true
        caseSensitive: false  # 日志内容不区分大小写搜索
      }
      {
        name: "duration_ms"
        fieldType: "i64"
        indexed: true
      }
      {
        name: "success"
        fieldType: "boolean"
        indexed: true
      }
    ]
  }) {
    name
    fields {
      name
      fieldType
      indexed
    }
  }
}
```

## 插入数据

### 插入时间戳数据 (支持多种格式)

```graphql
mutation {
  insertData(input: {
    table: "events"
    data: [
      {
        id: "evt1"
        created_at: 1704067200000  # 毫秒时间戳
        event_time: "2024-01-01T00:00:00Z"  # ISO8601
        message: "System Started"
        status_code: 200
      }
      {
        id: "evt2"
        created_at: 1704067200  # 秒时间戳 (自动转换为毫秒)
        event_time: "2024-01-01 12:00:00"  # MySQL 格式
        message: "User Login"
        status_code: 200
      }
      {
        id: "evt3"
        created_at: "2024-01-02T00:00:00+08:00"  # RFC3339 带时区
        event_time: 1704153600000
        message: "Data Synced"
        status_code: 201
      }
    ]
  }) {
    success
    rowsInserted
    message
  }
}
```

### 插入不区分大小写的数据

```graphql
mutation {
  insertData(input: {
    table: "app_logs"
    data: [
      {
        log_id: "log1"
        app_name: "MyApp"  # 存储为 "myapp"
        level: "ERROR"  # 存储为 "error"
        timestamp: 1704067200000
        message: "Database Connection Failed"
        duration_ms: 5000
        success: false
      }
      {
        log_id: "log2"
        app_name: "myapp"  # 和 "MyApp" 等价
        level: "info"
        timestamp: 1704070800000
        message: "Request Processed Successfully"
        duration_ms: 150
        success: true
      }
    ]
  }) {
    success
    rowsInserted
  }
}
```

## 查询数据

### SQL 查询 (时间范围)

```graphql
query {
  query(sql: """
    SELECT * FROM events 
    WHERE created_at >= 1704067200000 
      AND created_at < 1704153600000 
    ORDER BY created_at DESC 
    LIMIT 100
  """) {
    columns
    totalRows
    rows
  }
}
```

### SQL 查询 (不区分大小写)

```graphql
query {
  query(sql: """
    SELECT * FROM app_logs 
    WHERE level = 'error'
      AND app_name = 'MYAPP'
    ORDER BY timestamp DESC
  """) {
    columns
    totalRows
    rows
  }
}
```

## 字段配置详解

### caseSensitive (keyword/text 类型)
- **默认值**: `true` (区分大小写)
- **设置为 false**: 
  - 存储时自动转换为小写
  - 查询时自动转换为小写进行匹配
  - 适用场景: 应用名、标签、状态码等不需要区分大小写的字段

### format (timestamp/datetime 类型)
- **默认值**: `null` (接受任意格式)
- **可选值**:
  - `"iso8601"`: ISO 8601 格式 (2024-01-01T10:00:00Z)
  - `"rfc3339"`: RFC 3339 格式
  - 自定义格式字符串
- **插入时支持的格式** (无论 format 设置为何):
  - 毫秒时间戳: `1704067200000`
  - 秒时间戳: `1704067200` (自动转为毫秒)
  - ISO8601: `"2024-01-01T00:00:00Z"`
  - MySQL: `"2024-01-01 12:00:00"`
  - RFC3339: `"2024-01-01T00:00:00+08:00"`

### indexed
- **默认值**: `true`
- **设置为 false**: 字段不建立索引,节省存储空间但无法高效查询

## 最佳实践

### 1. 选择正确的字段类型
- 状态、标签、分类 → `keyword` + `caseSensitive: false`
- ID、唯一标识 → `keyword` + `caseSensitive: true`
- 时间戳 → `timestamp`
- 数值 → `i32`, `i64`, `f32`, `f64`
- 开关标志 → `boolean`

### 2. 合理使用不区分大小写
```graphql
{
  name: "status"
  fieldType: "keyword"
  caseSensitive: false  # ✅ 状态码通常不区分大小写
}

{
  name: "user_id"
  fieldType: "keyword"
  caseSensitive: true  # ✅ ID 应该区分大小写
}
```

### 3. 时间戳字段建议
- 总是建立索引 (`indexed: true`)
- format 保持为 `null` 以支持多种输入格式
- 存储时统一为毫秒时间戳,查询时也用毫秒

### 4. 分区策略
- 高并发写入: `partitionCount: 16` 或更高
- 小数据集: `partitionCount: 1` 或 `4`
- 根据主键分布选择合适的分区数

## 常见场景示例

### 场景 1: 用户行为日志
```graphql
mutation {
  createTable(input: {
    name: "user_actions"
    primaryKey: "action_id"
    partitionCount: 8
    fields: [
      { name: "action_id", fieldType: "keyword", indexed: true }
      { name: "user_id", fieldType: "keyword", indexed: true }
      { name: "action", fieldType: "keyword", indexed: true, caseSensitive: false }
      { name: "timestamp", fieldType: "timestamp", indexed: true }
      { name: "duration_ms", fieldType: "i64", indexed: true }
    ]
  }) { name }
}
```

### 场景 2: 商品目录
```graphql
mutation {
  createTable(input: {
    name: "products"
    primaryKey: "sku"
    fields: [
      { name: "sku", fieldType: "keyword", indexed: true, caseSensitive: true }
      { name: "name", fieldType: "text", indexed: true, caseSensitive: false }
      { name: "category", fieldType: "keyword", indexed: true, caseSensitive: false }
      { name: "price", fieldType: "f64", indexed: true }
      { name: "in_stock", fieldType: "boolean", indexed: true }
      { name: "created_at", fieldType: "timestamp", indexed: true }
    ]
  }) { name }
}
```

### 场景 3: IoT 传感器数据
```graphql
mutation {
  createTable(input: {
    name: "sensor_data"
    primaryKey: "data_id"
    partitionCount: 32
    fields: [
      { name: "data_id", fieldType: "keyword", indexed: true }
      { name: "sensor_id", fieldType: "keyword", indexed: true }
      { name: "timestamp", fieldType: "timestamp", indexed: true }
      { name: "temperature", fieldType: "f32", indexed: true }
      { name: "humidity", fieldType: "f32", indexed: true }
      { name: "battery_level", fieldType: "i32", indexed: true }
    ]
  }) { name }
}
```
