# Range 分区并行度使用指南

## 功能说明

Range 分区策略现在支持**并行度参数** (`parallelism`),可以在同一个 range 内创建多个子分区,提高并行写入性能。

## 使用场景

- **高并发写入**: 时序数据大量并发写入同一时间段
- **热点分区**: 避免单个 range 分区成为写入瓶颈
- **提升吞吐**: 多个子分区可以并行写入,充分利用多核 CPU

## 分区命名规则

### 不启用并行度 (parallelism = None)
```
partition_0000000000000000000  (range: [0, 1000000))
partition_0000000000001000000  (range: [1000000, 2000000))
```

### 启用并行度 (parallelism = Some(2))
```
partition_0000000000000000000_0  (range: [0, 1000000), 并行索引 0)
partition_0000000000000000000_1  (range: [0, 1000000), 并行索引 1)
partition_0000000000001000000_0  (range: [1000000, 2000000), 并行索引 0)
partition_0000000000001000000_1  (range: [1000000, 2000000), 并行索引 1)
```

## GraphQL 建表示例

### 示例 1: 时序日志表 (2路并行)

```graphql
mutation {
  createTable(input: {
    table_name: "access_logs"
    fields: [
      { name: "_id", field_type: KEYWORD, index: true, nullable: false }
      { name: "timestamp", field_type: TIMESTAMP_MS, index: true, nullable: false }
      { name: "user_id", field_type: I64, index: true }
      { name: "action", field_type: KEYWORD, index: true }
    ]
    primary_key: "_id"
    partition_strategy: {
      strategy_type: RANGE
      field: "timestamp"
      range_start: 0
      range_step: 86400000          # 1天 = 86400000 毫秒
      range_parallelism: 2          # 每天2个子分区
    }
  }) {
    success
    message
  }
}
```

**效果**: 每天的数据会自动分布到 2 个子分区,提高并发写入能力

### 示例 2: 高吞吐监控表 (4路并行)

```graphql
mutation {
  createTable(input: {
    table_name: "metrics"
    fields: [
      { name: "_id", field_type: KEYWORD, index: true, nullable: false }
      { name: "timestamp", field_type: TIMESTAMP_MS, index: true, nullable: false }
      { name: "metric_name", field_type: KEYWORD, index: true }
      { name: "value", field_type: F64 }
    ]
    primary_key: "_id"
    partition_strategy: {
      strategy_type: RANGE
      field: "timestamp"
      range_start: 0
      range_step: 3600000           # 1小时 = 3600000 毫秒
      range_parallelism: 4          # 每小时4个子分区
    }
  }) {
    success
    message
  }
}
```

**效果**: 每小时的数据分布到 4 个子分区,支持更高的并发写入

### 示例 3: 不启用并行度 (默认行为)

```graphql
mutation {
  createTable(input: {
    table_name: "events"
    fields: [
      { name: "_id", field_type: KEYWORD, index: true, nullable: false }
      { name: "timestamp", field_type: TIMESTAMP_MS, index: true, nullable: false }
      { name: "event_type", field_type: KEYWORD, index: true }
    ]
    primary_key: "_id"
    partition_strategy: {
      strategy_type: RANGE
      field: "timestamp"
      range_start: 0
      range_step: 86400000
      # range_parallelism 不指定或设为 1
    }
  }) {
    success
    message
  }
}
```

**效果**: 传统的单分区模式,每天一个分区

## 写入行为

### 随机分配策略

插入数据时,系统会**随机选择**一个子分区进行写入:

```rust
// 伪代码示例
let parallel_index = random(0, parallelism);
partition_name = format!("{}_{}", base_name, parallel_index);
```

这种随机策略确保:
- ✅ 负载均衡: 数据均匀分布到各个子分区
- ✅ 避免热点: 防止某个子分区过载
- ✅ 简单高效: 无需额外的路由逻辑

## 性能建议

### 并行度选择

| 写入 QPS | 推荐并行度 | 说明 |
|---------|----------|------|
| < 1000 | 1 (不启用) | 单分区足够 |
| 1000 - 5000 | 2 | 2路并行 |
| 5000 - 20000 | 4 | 4路并行 |
| > 20000 | 8 | 8路并行 |

### 注意事项

1. **CPU 核心数**: 并行度建议不超过 CPU 核心数
2. **查询影响**: 并行度越高,查询时需要扫描的分区越多
3. **磁盘 IO**: 确保磁盘能支撑多分区并发写入
4. **权衡**: 在写入性能和查询性能之间找到平衡

## ES Bulk 操作支持

ES bulk 操作自动支持并行度,无需额外配置:

```bash
# 批量插入会自动分布到子分区
POST /access_logs/_bulk
{"index":{"_id":"1"}}
{"timestamp":1609459200000,"user_id":1,"action":"login"}
{"index":{"_id":"2"}}
{"timestamp":1609459200100,"user_id":2,"action":"view"}
```

系统会自动:
1. 计算每条记录的 base partition (基于 timestamp 和 range)
2. 随机选择子分区索引 (0, 1, ...)
3. 路由到对应的子分区

## 查询行为

查询时会自动聚合所有子分区的结果:

```sql
-- 查询某天的数据,自动扫描该天的所有子分区
SELECT * FROM access_logs 
WHERE timestamp >= 1609459200000 AND timestamp < 1609545600000;
```

引擎会自动:
1. 识别涉及的 base partitions
2. 扫描每个 base partition 的所有子分区
3. 合并结果返回

## 总结

Range 并行度功能让你可以:
- ✅ **提升写入性能**: 多分区并行写入
- ✅ **避免热点**: 负载均衡分布
- ✅ **灵活配置**: 可选参数,按需启用
- ✅ **透明查询**: 查询逻辑无需改变

根据实际写入负载选择合适的并行度,在吞吐量和查询效率之间找到最佳平衡!
