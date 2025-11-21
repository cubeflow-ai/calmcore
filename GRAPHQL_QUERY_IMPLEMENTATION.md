# GraphQL 查询接口实现报告

## 概述

为 CalmCore 的 GraphQL API 新增了查看表、分区和段（Segment）详细信息的查询接口，方便用户通过 GraphQL 查询数据库的存储结构和统计信息。

## 实现内容

### 1. 新增类型定义

在 `src/protocol/graphql/mod.rs` 中添加了以下类型：

#### PartitionInfo
```rust
pub struct PartitionInfo {
    pub partition_id: String,
    pub segment_count: usize,
    pub segments: Vec<SegmentInfo>,
}
```

#### SegmentInfo
```rust
pub struct SegmentInfo {
    pub segment_id: u64,
    pub doc_count: u32,
    pub deleted_count: u64,
    pub is_persisted: bool,
    pub base_path: Option<String>,
}
```

#### TableDetail
```rust
pub struct TableDetail {
    pub name: String,
    pub partition_count: usize,
    pub total_segments: usize,
    pub total_documents: u64,
    pub fields: Vec<Field>,
    pub primary_key: Option<String>,
    pub partitions: Vec<PartitionInfo>,
}
```

### 2. 新增查询接口

在 `QueryRoot` 中添加了三个新查询：

#### partitions
- **功能**: 查询表的所有分区和段信息
- **参数**: `table: String`
- **返回**: `Vec<PartitionInfo>`
- **用途**: 查看表的分区分布和每个分区的段详情

#### tableDetail
- **功能**: 查询表的完整详情，包括统计信息
- **参数**: `name: String`
- **返回**: `Option<TableDetail>`
- **用途**: 获取表的全面视图，包括文档总数、分区数、段数等

#### table (已存在，未修改)
- **功能**: 查询表的基本信息
- **参数**: `name: String`
- **返回**: `Option<Table>`

### 3. 实现细节

#### 分区信息获取
- 使用 `Engine::list_partitions()` 获取所有分区 ID
- 使用 `Engine::get_partition()` 获取每个分区实例
- 通过 `Partition::get_frozen_segments()` 获取已冻结的段
- 通过 `Partition::get_current_segment()` 获取当前活跃段

#### 段信息收集
从 `Segment` 结构中提取以下信息：
- `doc_count()`: 文档数量
- `deleted_count()`: 已删除文档数量
- `is_persisted()`: 持久化状态
- `base_path()`: 持久化路径

#### 统计计算
- `totalSegments`: 累加所有分区的段数量
- `totalDocuments`: 累加所有段的有效文档数（doc_count - deleted_count）
- `partitionCount`: 返回分区列表的长度

## 文档

### 创建的文档文件

1. **GRAPHQL_QUERY_GUIDE.md**
   - 完整的使用指南
   - 详细的查询示例
   - 使用场景说明
   - 返回结果示例

2. **GRAPHQL_QUERY_REFERENCE.md**
   - 快速参考文档
   - 类型定义表格
   - 常用查询模板
   - 使用场景对照表

3. **examples/graphql_query_demo.rs**
   - 完整的可运行示例
   - 创建测试表（Hash 和 Range 分区策略）
   - 插入测试数据
   - 演示查询用法
   - 启动 GraphQL 服务器

## 使用示例

### 启动演示服务器
```bash
cargo run --example graphql_query_demo
```

### 访问 GraphQL Playground
```
http://localhost:8080/playground
```

### 基本查询示例

#### 查看所有表
```graphql
query {
  tables
}
```

#### 查看分区和段
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

#### 查看完整表信息
```graphql
query {
  tableDetail(name: "users") {
    name
    partitionCount
    totalSegments
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

## 支持的分区策略

新的查询接口支持所有分区策略：

| 分区策略 | Partition ID 格式 | 示例 |
|---------|------------------|------|
| Hash | 19位零填充数字 | `0000000000000000000` |
| Range | `start_end` | `0_100`, `100_200` |
| List | 经过清理的值 | `active`, `pending` |
| None | 表名 | `users` |
| Custom | 清理后的值或索引 | `custom_partition_1` |

## 使用场景

### 1. 监控和运维
- 查看表的存储分布
- 检查未持久化的数据
- 识别需要合并的段
- 监控文档删除情况

### 2. 性能分析
- 分析数据倾斜情况
- 评估分区策略效果
- 查找热点分区
- 统计总文档数

### 3. 调试和开发
- 验证分区路由逻辑
- 检查 Custom 分区加载
- 确认数据持久化状态
- 跟踪段的生命周期

### 4. 容量规划
- 评估存储使用情况
- 预测数据增长趋势
- 规划分区调整
- 优化段合并策略

## 技术实现亮点

### 1. 高效的读锁管理
```rust
// 获取 frozen segments
let frozen_segments = partition.get_frozen_segments();
// ... 处理数据 ...
drop(frozen_segments);  // 及时释放读锁

// 获取 current segment
let current_segment = partition.get_current_segment();
// ... 处理数据 ...
drop(current_segment);  // 及时释放读锁
```

### 2. 异步查询支持
所有查询方法都是异步的，支持高并发访问：
```rust
async fn partitions(&self, ctx: &Context<'_>, table: String) -> Result<Vec<PartitionInfo>>
```

### 3. 完整的类型安全
使用 `async-graphql` 的类型系统确保查询的类型安全：
- 自动生成 GraphQL Schema
- 编译时类型检查
- 自动序列化/反序列化

### 4. 零拷贝统计
直接访问原子变量和内部状态，无需额外的数据复制：
```rust
segment.doc_count()      // AtomicU32::load()
segment.deleted_count()  // RoaringBitmap::len()
segment.is_persisted()   // AtomicBool::load()
```

## 编译验证

所有代码已通过编译验证：

```bash
# 验证库编译
cargo check --lib
# ✅ 通过

# 验证示例编译
cargo check --example graphql_query_demo
# ✅ 通过
```

## 后续可能的增强

1. **分页支持**: 为大表的分区列表添加分页
2. **过滤功能**: 只返回满足条件的分区（如：未持久化的分区）
3. **聚合统计**: 添加更多统计信息（如：平均段大小、持久化比例）
4. **性能指标**: 添加查询响应时间、吞吐量等性能指标
5. **历史数据**: 记录和查询历史统计信息
6. **实时订阅**: 使用 GraphQL Subscription 实时推送变化

## 相关文件

### 修改的文件
- `src/protocol/graphql/mod.rs` - 添加查询接口和类型定义

### 新增的文件
- `GRAPHQL_QUERY_GUIDE.md` - 详细使用指南
- `GRAPHQL_QUERY_REFERENCE.md` - 快速参考文档
- `examples/graphql_query_demo.rs` - 演示示例

### 依赖的核心接口
- `Engine::list_partitions()` - 列出分区
- `Engine::get_partition()` - 获取分区实例
- `Partition::get_frozen_segments()` - 获取已冻结段
- `Partition::get_current_segment()` - 获取当前段
- `Segment::doc_count()` - 文档计数
- `Segment::deleted_count()` - 删除计数
- `Segment::is_persisted()` - 持久化状态
- `Segment::base_path()` - 持久化路径

## 总结

本次实现为 CalmCore 的 GraphQL API 提供了完整的表、分区和段查询能力，使用户可以：

1. ✅ 通过 GraphQL 查看所有表
2. ✅ 查询表的分区分布
3. ✅ 查看每个分区的段详情
4. ✅ 获取完整的表统计信息
5. ✅ 监控数据持久化状态
6. ✅ 分析存储和性能

所有功能已实现并通过编译验证，配套文档和示例完整，可以立即使用。
