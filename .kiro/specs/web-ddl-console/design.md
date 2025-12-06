# Design Document

## Overview

本设计为 CalmCore Data Explorer Web 界面添加 DDL 功能和双语言控制台支持。主要涉及前端 Vue.js 组件的扩展，通过现有的 GraphQL API 与后端交互。

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Web Browser (Vue.js)                      │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
│  │ Table List  │  │ Data Show   │  │    AI Chat          │  │
│  │ + Create Btn│  │ (Results)   │  │                     │  │
│  │ + Delete Btn│  │             │  │                     │  │
│  └─────────────┘  └─────────────┘  └─────────────────────┘  │
│  ┌─────────────────────────────────────────────────────────┐│
│  │              Console (SQL / GraphQL)                    ││
│  │  [Language Selector] [Query Input] [Execute Button]     ││
│  └─────────────────────────────────────────────────────────┘│
├─────────────────────────────────────────────────────────────┤
│                    GraphQL API Layer                         │
│  POST /graphql                                               │
│  - query { query(sql: "...") }  ← SQL 模式                   │
│  - mutation { createTable(...) } ← 创建表                    │
│  - mutation { dropTable(...) }   ← 删除表                    │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                 CalmCore Engine (Rust)                       │
│  - Catalog (表元数据管理)                                     │
│  - Engine (数据操作)                                          │
└─────────────────────────────────────────────────────────────┘
```

## Components and Interfaces

### 1. Create Table Modal Component

**数据模型:**
```javascript
newTable: {
  name: '',           // 表名
  description: '',    // 描述
  primaryKey: '',     // 主键字段
  storeSource: true,  // 是否存储原始 JSON
  partitionStrategy: {
    type: 'NONE',     // PKHash | Hash | Range | DatetimeRange | Custom | None
    // PKHash 配置
    pkHashPartitions: 4,
    // Hash 配置
    hashField: '',
    hashPartitions: 4,
    // Range 配置
    rangeField: '',
    rangeStart: 0,
    rangeStep: 1000000,
    rangePartitions: 10,
    rangeParallelism: 1,
    // DatetimeRange 配置
    datetimeField: '',
    datetimeGranularity: 'DAY',
    datetimeTimezone: 'UTC',
    datetimeParallelism: 1
  },
  fields: [
    { name: '', type: 'KEYWORD', indexed: true, nullable: true, description: '' }
  ]
}
```

**GraphQL Mutation 构建:**
```javascript
function buildCreateTableMutation(tableConfig) {
  // 根据 partitionStrategy.type 构建对应的 GraphQL mutation
  // 返回完整的 mutation 字符串
}
```

### 2. Delete Table Confirmation Component

**数据模型:**
```javascript
deleteConfirm: {
  show: false,
  tableName: '',
  loading: false
}
```

### 3. Console Language Selector

**数据模型:**
```javascript
console: {
  language: 'graphql',  // 'sql' | 'graphql'
  query: '',
  executing: false
}
```

**SQL 到 GraphQL 转换:**
```javascript
function wrapSqlInGraphQL(sql) {
  return `query { query(sql: "${escapeSql(sql)}") { columns rows } }`;
}
```

## Data Models

### CreateTableInput (GraphQL)

```graphql
input CreateTableInput {
  name: String!
  description: String
  primary_key: String
  store_source: Boolean
  partition_strategy: PartitionStrategyInput
  fields: [FieldInput!]!
  persist_policy: PersistPolicyInput
}

input FieldInput {
  name: String!
  field_type: FieldTypeEnum!
  indexed: Boolean
  description: String
  default_value: String
  nullable: Boolean
  case_sensitive: Boolean
  is_array: Boolean
  format: String
}
```

### PartitionStrategyInput (GraphQL OneOf)

```graphql
input PartitionStrategyInput @oneOf {
  pkHash: PKHashPartitionConfig
  hash: HashPartitionConfig
  range: RangePartitionConfig
  datetimeRange: DatetimeRangePartitionConfig
  custom: CustomPartitionConfig
  none: NonePartitionConfig
}
```

## Correctness Properties

*A property is a characteristic or behavior that should hold true across all valid executions of a system-essentially, a formal statement about what the system should do. Properties serve as the bridge between human-readable specifications and machine-verifiable correctness guarantees.*

### Property 1: SQL to GraphQL Wrapping Correctness

*For any* valid SQL query string, when the console is in SQL mode, the system SHALL wrap it in a GraphQL query structure that:
- Contains the SQL string properly escaped
- Has the correct GraphQL query format: `query { query(sql: "...") { columns rows } }`
- Preserves the original SQL semantics

**Validates: Requirements 3.2**

### Property 2: GraphQL Passthrough Integrity

*For any* valid GraphQL query string, when the console is in GraphQL mode, the system SHALL send the query unchanged to the GraphQL endpoint without modification.

**Validates: Requirements 3.3**

### Property 3: CreateTable Mutation Generation

*For any* valid table configuration (name, fields, partition strategy), the `buildCreateTableMutation` function SHALL generate a syntactically valid GraphQL mutation that matches the CalmCore GraphQL schema.

**Validates: Requirements 1.2**

## Error Handling

| Error Scenario | User Feedback | System Behavior |
|----------------|---------------|-----------------|
| Network failure | "无法连接到服务器" | Keep modal open, allow retry |
| Table already exists | "表 'xxx' 已存在" | Keep modal open, highlight name field |
| Invalid table name | "表名只能包含小写字母、数字和下划线" | Prevent submission, show validation error |
| Table not found (delete) | "表 'xxx' 不存在" | Close dialog, refresh table list |
| GraphQL syntax error | Display server error message | Keep console content, show error |

## Testing Strategy

### Unit Tests

1. **SQL Escaping**: Test that special characters in SQL are properly escaped
2. **Mutation Building**: Test that different partition strategies generate correct mutations
3. **Form Validation**: Test that invalid inputs are rejected before submission

### Property-Based Tests

使用 JavaScript 的 fast-check 库进行属性测试：

1. **SQL Wrapping Property**: Generate random SQL strings and verify the wrapped GraphQL is valid
2. **GraphQL Passthrough Property**: Generate random GraphQL queries and verify they are sent unchanged
3. **Mutation Generation Property**: Generate random table configurations and verify the mutation is syntactically valid

每个属性测试配置运行至少 100 次迭代。

测试标注格式: `**Feature: web-ddl-console, Property {number}: {property_text}**`

