# GraphQL 查询接口指南

本文档介绍 CalmCore GraphQL API 中用于查看表、分区和段的查询接口。

## 概述

CalmCore 提供了三个层级的查询接口：
1. **tables** - 列出所有表名
2. **table** - 获取单个表的基本信息
3. **partitions** - 获取表的所有分区和段信息
4. **tableDetail** - 获取表的完整详情（包括分区、段和统计信息）

## 查询接口详解

### 1. 列出所有表

查询所有表的名称：

```graphql
query {
  tables
}
```

返回示例：
```json
{
  "data": {
    "tables": ["users", "orders", "products"]
  }
}
```

### 2. 获取表的基本信息

查询单个表的基本信息（不包括分区详情）：

```graphql
query {
  table(name: "users") {
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

返回示例：
```json
{
  "data": {
    "table": {
      "name": "users",
      "partitionCount": 4,
      "primaryKey": "user_id",
      "fields": [
        {
          "name": "user_id",
          "fieldType": "keyword",
          "indexed": true
        },
        {
          "name": "age",
          "fieldType": "i32",
          "indexed": true
        },
        {
          "name": "email",
          "fieldType": "keyword",
          "indexed": true
        }
      ]
    }
  }
}
```

### 3. 获取表的所有分区信息

查询表的所有分区及其段信息：

```graphql
query {
  partitions(table: "users") {
    partitionId
    segmentCount
    segments {
      segmentId
      docCount
      deletedCount
      isPersisted
      basePath
    }
  }
}
```

返回示例：
```json
{
  "data": {
    "partitions": [
      {
        "partitionId": "0000000000000000000",
        "segmentCount": 3,
        "segments": [
          {
            "segmentId": 1,
            "docCount": 1000,
            "deletedCount": 10,
            "isPersisted": true,
            "basePath": "/data/tables/users/partition_0000000000000000000/segment_1"
          },
          {
            "segmentId": 2,
            "docCount": 800,
            "deletedCount": 5,
            "isPersisted": true,
            "basePath": "/data/tables/users/partition_0000000000000000000/segment_2"
          },
          {
            "segmentId": 0,
            "docCount": 250,
            "deletedCount": 0,
            "isPersisted": false,
            "basePath": null
          }
        ]
      },
      {
        "partitionId": "0000000000000000001",
        "segmentCount": 2,
        "segments": [
          {
            "segmentId": 1,
            "docCount": 950,
            "deletedCount": 8,
            "isPersisted": true,
            "basePath": "/data/tables/users/partition_0000000000000000001/segment_1"
          },
          {
            "segmentId": 0,
            "docCount": 300,
            "deletedCount": 0,
            "isPersisted": false,
            "basePath": null
          }
        ]
      }
    ]
  }
}
```

**字段说明：**
- `partitionId`: 分区 ID（根据分区策略生成）
- `segmentCount`: 该分区中的段数量
- `segments`: 段列表
  - `segmentId`: 段 ID（0 表示当前活跃段，其他为已冻结段）
  - `docCount`: 文档数量
  - `deletedCount`: 已删除文档数量
  - `isPersisted`: 是否已持久化到磁盘
  - `basePath`: 持久化路径（null 表示未持久化）

### 4. 获取表的完整详情

查询表的完整信息，包括统计数据和所有分区/段详情：

```graphql
query {
  tableDetail(name: "users") {
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
      segments {
        segmentId
        docCount
        deletedCount
        isPersisted
        basePath
      }
    }
  }
}
```

返回示例：
```json
{
  "data": {
    "tableDetail": {
      "name": "users",
      "partitionCount": 4,
      "totalSegments": 10,
      "totalDocuments": 5000,
      "primaryKey": "user_id",
      "fields": [
        {
          "name": "user_id",
          "fieldType": "keyword",
          "indexed": true
        },
        {
          "name": "age",
          "fieldType": "i32",
          "indexed": true
        }
      ],
      "partitions": [
        {
          "partitionId": "0000000000000000000",
          "segmentCount": 3,
          "segments": [...]
        },
        {
          "partitionId": "0000000000000000001",
          "segmentCount": 2,
          "segments": [...]
        }
      ]
    }
  }
}
```

**统计字段说明：**
- `partitionCount`: 分区总数
- `totalSegments`: 所有分区中的段总数
- `totalDocuments`: 有效文档总数（总文档数 - 已删除文档数）

## 使用场景

### 场景 1: 监控表的存储情况

查看表的分区和段分布，评估是否需要合并或重平衡：

```graphql
query {
  tableDetail(name: "orders") {
    name
    partitionCount
    totalSegments
    totalDocuments
    partitions {
      partitionId
      segmentCount
      segments {
        docCount
        isPersisted
      }
    }
  }
}
```

### 场景 2: 检查未持久化的数据

查找所有未持久化的段，判断是否需要触发持久化：

```graphql
query {
  partitions(table: "products") {
    partitionId
    segments {
      segmentId
      docCount
      isPersisted
    }
  }
}
```

### 场景 3: 调试 Custom 分区

查看 Custom 分区策略下的分区和段分布：

```graphql
query {
  partitions(table: "custom_data") {
    partitionId
    segmentCount
    segments {
      segmentId
      docCount
      basePath
    }
  }
}
```

### 场景 4: 性能分析

分析每个分区的数据量，识别热点分区：

```graphql
query {
  tableDetail(name: "logs") {
    partitionCount
    totalDocuments
    partitions {
      partitionId
      segments {
        docCount
        deletedCount
      }
    }
  }
}
```

## 与其他 GraphQL 操作结合

### 创建表后查看详情

```graphql
mutation {
  createTable(input: {
    name: "test_table"
    primaryKey: "id"
    partitionCount: 4
    fields: [
      { name: "id", fieldType: KEYWORD }
      { name: "value", fieldType: I64 }
    ]
  }) {
    name
    partitionCount
  }
}

query {
  tableDetail(name: "test_table") {
    partitionCount
    totalSegments
    partitions {
      partitionId
      segmentCount
    }
  }
}
```

### 插入数据后检查分布

```graphql
mutation {
  insertData(input: {
    table: "test_table"
    data: [
      { id: "user1", value: 100 }
      { id: "user2", value: 200 }
    ]
  }) {
    success
    rowsInserted
  }
}

query {
  partitions(table: "test_table") {
    partitionId
    segments {
      docCount
      isPersisted
    }
  }
}
```

## 注意事项

1. **性能考虑**：`tableDetail` 查询会遍历所有分区和段，对于大型表可能较慢
2. **实时性**：查询结果反映查询时刻的状态，可能在查询过程中发生变化
3. **Segment ID**：
   - `segmentId = 0` 表示当前活跃段（正在写入）
   - `segmentId > 0` 表示已冻结段（只读）
4. **文档计数**：`totalDocuments` 已经扣除了已删除的文档
5. **持久化状态**：未持久化的段数据仅存在于内存中，重启后会丢失

## GraphQL Playground

访问 GraphQL Playground 进行交互式查询：

```
http://localhost:8080/playground
```

在 Playground 中可以：
- 查看完整的 Schema 文档
- 自动补全查询
- 查看查询历史
- 测试查询和变更操作
