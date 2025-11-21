# GraphQL 查询接口快速参考

## 新增的查询接口

CalmCore 的 GraphQL API 新增了三个查询接口，用于查看表、分区和段的详细信息。

### 接口列表

| 查询接口 | 参数 | 返回类型 | 说明 |
|---------|------|---------|------|
| `tables` | 无 | `[String!]!` | 列出所有表名 |
| `table` | `name: String!` | `Table` | 获取表的基本信息 |
| `partitions` | `table: String!` | `[PartitionInfo!]!` | 获取表的所有分区和段信息 |
| `tableDetail` | `name: String!` | `TableDetail` | 获取表的完整详情（包括统计信息） |

### 类型定义

#### PartitionInfo
```graphql
type PartitionInfo {
  partitionId: String!      # 分区 ID
  segmentCount: Int!        # 段数量
  segments: [SegmentInfo!]! # 段列表
}
```

#### SegmentInfo
```graphql
type SegmentInfo {
  segmentId: Int!          # 段 ID（0 表示当前活跃段）
  docCount: Int!           # 文档数量
  deletedCount: Int!       # 已删除文档数量
  isPersisted: Boolean!    # 是否已持久化
  basePath: String         # 持久化路径（null 表示未持久化）
}
```

#### TableDetail
```graphql
type TableDetail {
  name: String!            # 表名
  partitionCount: Int!     # 分区总数
  totalSegments: Int!      # 段总数
  totalDocuments: Int!     # 有效文档总数
  fields: [Field!]!        # 字段列表
  primaryKey: String       # 主键字段名
  partitions: [PartitionInfo!]! # 分区详情
}
```

## 快速示例

### 1. 查看所有表
```graphql
query {
  tables
}
```

### 2. 查看表结构
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

### 3. 查看分区和段
```graphql
query {
  partitions(table: "users") {
    partitionId
    segmentCount
    segments {
      segmentId
      docCount
      isPersisted
    }
  }
}
```

### 4. 完整表信息
```graphql
query {
  tableDetail(name: "users") {
    name
    partitionCount
    totalSegments
    totalDocuments
    partitions {
      partitionId
      segmentCount
    }
  }
}
```

## 运行演示

### 启动 GraphQL 服务器
```bash
cargo run --example graphql_query_demo
```

### 访问 Playground
打开浏览器访问：http://localhost:8080/playground

### 测试查询
在 Playground 中可以直接执行上述查询，支持：
- ✅ 自动补全
- ✅ Schema 文档
- ✅ 查询验证
- ✅ 结果格式化

## 使用场景

| 场景 | 推荐查询 |
|------|---------|
| 监控表存储情况 | `tableDetail` |
| 检查数据分布 | `partitions` |
| 查找未持久化数据 | `partitions` + `isPersisted` 字段 |
| 性能分析 | `tableDetail` + `totalDocuments` |
| 调试分区策略 | `partitions` + `partitionId` |

## 相关文档

- [GraphQL 查询接口详细指南](./GRAPHQL_QUERY_GUIDE.md)
- [GraphQL 服务器示例](./examples/graphql_query_demo.rs)
- [GraphQL API 完整文档](./src/protocol/graphql/mod.rs)
