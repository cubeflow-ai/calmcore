# GraphQL 创建表快速参考

## 基本语法

```graphql
mutation {
  createTable(input: {
    name: "表名"
    primaryKey: "主键字段"
    fields: [字段定义]
    partitionStrategy: {分区策略}  # 可选
  }) {
    name
    partitionCount
  }
}
```

## 五种分区策略

### 1. Hash（默认）- 哈希分区
```graphql
partitionStrategy: {
  strategyType: HASH
  field: "user_id"
  numPartitions: 8
}
```

### 2. Range - 范围分区
```graphql
partitionStrategy: {
  strategyType: RANGE
  field: "timestamp"
  ranges: [
    { start: { intValue: 0 }, end: { intValue: 1000 }, partitionId: 0 }
    { start: { intValue: 1000 }, end: { intValue: 2000 }, partitionId: 1 }
  ]
}
```

### 3. List - 列表分区
```graphql
partitionStrategy: {
  strategyType: LIST
  field: "status"
  valueMapping: "pending:0,active:1,completed:2"
}
```

### 4. Custom - 自定义分区
```graphql
partitionStrategy: {
  strategyType: CUSTOM
}
```

### 5. None - 无分区
```graphql
partitionStrategy: {
  strategyType: NONE
}
```

## 字段类型

| 类型 | 说明 | 别名 |
|-----|------|-----|
| `KEYWORD` | 字符串 | `TEXT` |
| `I32` | 32位整数 | `INTEGER` |
| `I64` | 64位整数 | `LONG` |
| `F32` | 32位浮点 | `FLOAT` |
| `F64` | 64位浮点 | `DOUBLE` |
| `BOOLEAN` | 布尔值 | `BOOL` |
| `TIMESTAMP` | 时间戳 | `DATETIME` |
| `U8/U16/U32/U64` | 无符号整数 | - |
| `I8/I16` | 小整数 | - |

## 完整示例

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
      { name: "created_at", fieldType: TIMESTAMP }
    ]
    partitionStrategy: {
      strategyType: HASH
      field: "user_id"
      numPartitions: 8
    }
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

## 分区策略选择

| 场景 | 推荐 |
|-----|------|
| 高并发写入 | Hash |
| 时序数据 | Range |
| 分类数据 | List |
| 外部导入 | Custom |
| 小表 | None |

## PartitionValue 类型

Range 分区支持三种值类型：

```graphql
# 整数
{ intValue: 1000 }

# 无符号整数
{ uintValue: 1000 }

# 字符串
{ stringValue: "2024-01-01" }
```

## 字段配置选项

```graphql
fields: [
  { 
    name: "email"
    fieldType: KEYWORD
    indexed: true           # 是否建立索引
    caseSensitive: false    # 大小写敏感（仅 KEYWORD）
  }
  {
    name: "created_at"
    fieldType: TIMESTAMP
    format: "%Y-%m-%d"      # 时间格式（仅 TIMESTAMP）
  }
]
```

## 默认行为

不指定 `partitionStrategy` 时：
- 使用 Hash 策略
- 分区字段：主键字段
- 分区数量：`partitionCount`（默认 1）

```graphql
mutation {
  createTable(input: {
    name: "simple_table"
    primaryKey: "id"
    partitionCount: 4  # 使用默认 Hash 策略
    fields: [...]
  })
}
```
