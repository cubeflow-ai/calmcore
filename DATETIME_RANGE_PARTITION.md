# DatetimeRange 分区策略 - 分布式实现说明

## 概述

DatetimeRange 是一种**按需创建**的时序数据分区策略，适用于时间序列数据（如日志、事件、监控指标等）。

## 分区策略配置

### GraphQL 创建表示例

```graphql
mutation {
  createTable(input: {
    name: "events"
    description: "事件日志表"
    primary_key: "event_id"
    
    partitionStrategy: {
      strategyType: DATETIMERANGE
      datetimeRange: {
        field: "updatetime"      # 时间戳字段名
        granularity: Day         # 时间粒度：Year/Month/Week/Day/Hour
        timezone: "UTC"          # 时区（可选）："UTC", "Asia/Shanghai" 等
        parallelism: 2           # 并行度（可选）：每个时间段的并行分区数
      }
    }
    
    fields: [
      { name: "event_id", fieldType: U64, nullable: false }
      { name: "updatetime", fieldType: TIMESTAMP, nullable: false }
      { name: "event_type", fieldType: KEYWORD, nullable: false }
    ]
  }) {
    name
    partitionCount
  }
}
```

## 工作原理

### 1. 表创建阶段

- ✅ **不预创建分区**：只在 Catalog 中记录表元数据和分区策略
- ✅ **分布式协调**：通过 `#[coordinator_route]` 宏自动路由到 coordinator 节点执行

```rust
// 分区列表为空，不预创建
let partitions = table_info
    .table
    .partition_strategy
    .generate_partitions()  // 返回 None（按需创建）
    .unwrap_or_else(Vec::new);  // 结果：[]
```

### 2. 数据插入阶段（自动创建分区）

#### 流程图

```
插入数据
  ↓
Router 计算分区名（如 20240101, 20240102_0）
  ↓
检查分区是否存在？
  ├─ 是 → 直接插入数据
  └─ 否 → 自动创建分区 → 插入数据
```

#### 代码实现（本地分区）

```rust
// src/calm/mod.rs - insert_data()

// 检查分区是否存在
if engine.get_partition(&table_name, &partition_name).await.is_none() {
    // 检查是否是按需创建的分区策略
    if !table_meta.table.partition_strategy.should_precreate_partitions() {
        log::info!("📦 Auto-creating on-demand partition '{}/{}'", table_name, partition_name);
        
        // 通过 tarpc 调用 create_partition（自动路由到 coordinator 或本地）
        CalmRpcService::create_partition(
            calm_service.as_ref().clone(),
            tarpc::context::current(),
            table_name.clone(),
            partition_name.clone(),
        ).await?;
    }
}

// 插入数据
engine.insert_batch(&table_name, &partition_name, partition_batch).await?;
```

#### 代码实现（远程分区 - Flight do_put）

```rust
// src/calm/flight_service.rs - do_put()

// 检查分区是否存在
if engine.get_partition(&table_name, &partition_name).await.is_none() {
    if !table_info.table.partition_strategy.should_precreate_partitions() {
        // 自动创建分区
        CalmRpcService::create_partition(
            Arc::as_ref(&calm_service).clone(),
            tarpc::context::current(),
            table_name,
            partition_name,
        ).await?;
    }
}

// 插入数据
engine.insert_batch(&table_name, &partition_name, batch).await?;
```

### 3. 分区命名规则

根据 `granularity` 和 `parallelism` 自动生成分区名：

| Granularity | Timezone | Parallelism | 示例分区名 | 说明 |
|------------|----------|-------------|----------|------|
| Day | None | None | `20240101` | 本地时区，2024-01-01 |
| Day | UTC | None | `20240101` | UTC 时区，2024-01-01 |
| Hour | UTC | None | `2024010108` | UTC 时区，2024-01-01 08:00 |
| Hour | UTC | Some(2) | `2024010108_0`, `2024010108_1` | UTC 时区，2024-01-01 08:00，2个并行分区 |
| Month | None | None | `202401` | 本地时区，2024-01 |
| Year | None | None | `2024` | 本地时区，2024年 |

### 4. 分区创建流程（分布式）

```
插入时检测分区不存在
  ↓
调用 create_partition(table_name, partition_name)
  ↓
#[coordinator_route] 宏自动路由
  ├─ 如果是 coordinator 节点 → 直接执行
  └─ 如果不是 → tarpc 转发到 coordinator
  ↓
Coordinator 执行：
  1. 负载均衡选择节点（最低负载）
  2. 通过 tarpc 调用目标节点的 create_partition
  3. 目标节点创建分区并通过 Gossip 发布
  4. Coordinator 等待 Gossip 同步完成
  ↓
分区创建完成，继续插入数据
```

## 关键特性

### ✅ 按需创建（On-Demand）

- **节省资源**：只在需要时创建分区，不浪费内存和磁盘空间
- **自动扩展**：随着时间推移自动创建新的时间分区
- **适合时序数据**：无需预先知道时间范围

### ✅ 负载均衡

- **动态选择节点**：综合考虑 partition 数量、CPU、内存、系统负载
- **评分算法**：
  ```
  score = partition_count * 0.4 + cpu_usage * 0.3 + 
          memory_usage * 0.2 + load_avg_1min * 10.0 * 0.1
  ```
- **优先低负载**：每次创建分区时选择负载最低的节点

### ✅ 并行度支持

- **提高写入吞吐**：同一时间段可创建多个并行分区（parallelism）
- **示例**：`granularity=Hour, parallelism=2`
  - 2024-01-01 08:00 有 2 个分区：`2024010108_0`, `2024010108_1`
  - Router 随机或轮询分配数据到这两个分区
  - 写入吞吐翻倍

### ✅ Gossip 同步

- **分区信息传播**：新创建的分区通过 Gossip 协议广播到所有节点
- **最终一致性**：所有节点最终都知道新分区的 owner 和 version
- **查询路由**：其他节点可以正确路由查询到分区所在节点

## 与预创建策略的对比

| 特性 | DatetimeRange（按需） | PKHash/Hash（预创建） |
|------|---------------------|---------------------|
| 创建时机 | 插入时按需创建 | 建表时预创建所有分区 |
| 分区数量 | 动态增长（随时间） | 固定（num_partitions） |
| 资源占用 | 低（按需分配） | 高（预先分配） |
| 负载均衡 | 创建时动态选择节点 | 建表时轮询分配 |
| 适用场景 | 时序数据、日志、事件 | 高频访问、固定数据集 |
| 查询性能 | 时间范围裁剪优秀 | 哈希查询优秀 |

## 注意事项

### 1. 时区处理

- **当前支持**：`UTC` 和本地时区（`None`）
- **未来扩展**：可集成 `chrono-tz` 支持更多时区（如 `Asia/Shanghai`, `America/New_York`）

### 2. 分区粒度选择建议

| 数据特征 | 推荐粒度 | 理由 |
|---------|---------|------|
| 高频日志（>100万/天） | Hour | 避免单个分区过大 |
| 中频事件（10万-100万/天） | Day | 平衡分区数量和大小 |
| 低频数据（<10万/天） | Week/Month | 减少分区数量 |
| 归档数据 | Month/Year | 长期存储优化 |

### 3. 并行度设置建议

```graphql
# 高并发写入场景
parallelism: 4  # 每个时间段 4 个分区，提高写入吞吐

# 中等并发场景
parallelism: 2  # 每个时间段 2 个分区

# 低并发或查询优化场景
parallelism: null  # 不启用并行度，每个时间段 1 个分区
```

### 4. Gossip 同步延迟

- **正常延迟**：100-500ms
- **超时设置**：30秒（`max_wait_secs`）
- **失败重试**：create_table 会等待 Gossip 同步完成

## 测试示例

### 创建 DatetimeRange 表

```bash
# test_datetime_range_partition.sh

curl -X POST http://localhost:9567/graphql \
  -H "Content-Type: application/json" \
  -d '{
    "query": "mutation { createTable(input: { name: \"logs\", primary_key: \"log_id\", partitionStrategy: { strategyType: DATETIMERANGE, datetimeRange: { field: \"timestamp\", granularity: Day, timezone: \"UTC\", parallelism: 2 } }, fields: [{ name: \"log_id\", fieldType: U64, nullable: false }, { name: \"timestamp\", fieldType: TIMESTAMP, nullable: false }, { name: \"message\", fieldType: TEXT }] }) { name partitionCount } }"
  }'
```

### 插入数据（自动创建分区）

```bash
# 插入不同日期的数据，观察分区自动创建

# 2024-01-01 的数据 → 自动创建 partition_20240101_0 或 partition_20240101_1
curl -X POST http://localhost:9567/graphql \
  -H "Content-Type: application/json" \
  -d '{
    "query": "mutation { insertData(input: { table: \"logs\", data: [{\"log_id\": 1, \"timestamp\": 1704067200000, \"message\": \"test\"}] }) { success rowsInserted } }"
  }'

# 2024-01-02 的数据 → 自动创建 partition_20240102_0 或 partition_20240102_1
curl -X POST http://localhost:9567/graphql \
  -H "Content-Type: application/json" \
  -d '{
    "query": "mutation { insertData(input: { table: \"logs\", data: [{\"log_id\": 2, \"timestamp\": 1704153600000, \"message\": \"test2\"}] }) { success rowsInserted } }"
  }'
```

### 查看分区列表

```bash
curl -X POST http://localhost:9567/graphql \
  -H "Content-Type: application/json" \
  -d '{
    "query": "{ listPartitions(tableName: \"logs\") }"
  }'

# 预期输出（假设 parallelism=2）：
# {
#   "data": {
#     "listPartitions": [
#       "20240101_0",
#       "20240101_1",
#       "20240102_0",
#       "20240102_1"
#     ]
#   }
# }
```

## 总结

✅ **已实现功能**：
1. DatetimeRange 分区策略的路由逻辑（Router）
2. 分区名称计算（按时间粒度和并行度）
3. 按需创建分区（插入时自动创建）
4. 分布式负载均衡（创建时选择最低负载节点）
5. Gossip 同步机制（分区信息广播）
6. 本地和远程插入支持（Flight do_put）

✅ **分布式支持**：
- 单机版逻辑完全兼容
- 分布式环境下自动路由到 coordinator
- 按需创建时自动负载均衡
- Gossip 协议保证最终一致性

🎯 **适用场景**：
- 日志存储系统（按天/小时分区）
- 事件追踪系统（按天分区）
- 监控指标存储（按小时分区）
- 时序数据库（按自定义粒度分区）

📌 **未来优化方向**：
1. 支持更多时区（集成 chrono-tz）
2. 分区自动合并（老旧分区合并减少碎片）
3. 分区自动删除（TTL 策略）
4. 更智能的并行度自适应算法
