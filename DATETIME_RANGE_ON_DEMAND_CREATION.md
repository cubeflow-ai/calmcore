# DatetimeRange 分区按需创建 - 完整流程说明

## 问题分析

DatetimeRange 是按需创建的分区策略，在分布式环境下需要处理：
1. **分区不存在时自动创建**
2. **并发创建的幂等性**（多个节点可能同时尝试创建同一分区）
3. **正确的创建时机**（在数据插入前，而不是在远程接收端）

## 解决方案

### 架构设计

```
客户端插入数据
    ↓
insert_data() - 数据路由到各个分区
    ↓
检查分区是否存在？
    ├─ 本地分区
    │   ├─ 存在 → 直接插入
    │   └─ 不存在 → create_partition(tarpc) → 插入
    │
    └─ 远程分区
        └─ flight_do_put 发送到远程节点
            ↓
        远程节点 do_put 接收
            ├─ 分区存在 → 直接插入
            └─ 分区不存在 → 返回错误（不应该发生）
```

### 关键点

1. **创建分区的责任在 insert_data**
   - `insert_data()` 检测分区不存在时，调用 `create_partition()`
   - 远程插入前分区已创建，`do_put` 只负责接收数据

2. **create_partition 的幂等性**
   - 检查分区是否已存在，存在则直接返回成功
   - 处理并发创建的场景

3. **远程插入流程**
   - `insert_data` → 发现是远程分区 → `flight_do_put` 发送数据
   - 远程节点 `do_put` → 检查分区存在性 → 插入数据

## 代码实现

### 1. insert_data() - 按需创建分区

**文件**: `src/calm/mod.rs`

```rust
pub async fn insert_data(
    &self,
    table_name: &str,
    batch: RecordBatch,
) -> CoreResult<usize> {
    // ... 路由到各个分区 ...
    
    // 并发处理所有分区
    let tasks: Vec<_> = routed_batches
        .into_iter()
        .map(|(partition_name, partition_batch)| {
            tokio::spawn(async move {
                // 检查分区归属
                if is_remote {
                    // 远程分区：通过 Flight do_put 发送
                    calm_service
                        .flight_do_put(&owner, &table_name, &partition_name, partition_batch)
                        .await?;
                } else {
                    // 本地分区：检查是否存在，不存在则按需创建
                    if engine.get_partition(&table_name, &partition_name).await.is_none() {
                        // 检查是否是按需创建的分区策略
                        let table_meta = catalog.get_or_load_table(&table_name).await?;
                        if !table_meta.table.partition_strategy.should_precreate_partitions() {
                            log::info!("📦 Auto-creating on-demand partition '{}/{}'", 
                                table_name, partition_name);
                            
                            // ✅ 通过 tarpc 调用 create_partition（自动路由到 coordinator 或本地）
                            CalmRpcService::create_partition(
                                calm_service.as_ref().clone(),
                                tarpc::context::current(),
                                table_name.clone(),
                                partition_name.clone(),
                            ).await?;
                            
                            log::info!("✅ Partition '{}/{}' created", table_name, partition_name);
                        } else {
                            // 预创建策略的分区不存在是错误
                            return Err(CoreError::NotExisted(
                                format!("Partition should be pre-created")
                            ));
                        }
                    }

                    // 插入数据到本地分区
                    engine.insert_batch(&table_name, &partition_name, partition_batch).await?;
                }

                Ok::<usize, CoreError>(rows)
            })
        })
        .collect();

    // 等待所有任务完成
    // ...
}
```

**关键点**：
- ✅ **只在本地分区检查并创建**
- ✅ **远程分区由远程节点的 insert_data 负责创建**
- ✅ **通过 tarpc 调用 create_partition，自动路由到正确节点**

### 2. create_partition() - 幂等性处理

**文件**: `src/calm/service.rs`

```rust
async fn create_partition(
    self,
    _context: Context,
    table_name: String,
    partition_name: String,
) -> CoreResult<()> {
    log::info!("📦 [DataNode] Creating partition '{}/{}'", table_name, partition_name);

    // ✅ 0. 检查分区是否已经存在（处理并发创建）
    if self.engine.get_partition(&table_name, &partition_name).await.is_some() {
        log::info!("✅ [DataNode] Partition '{}/{}' already exists, skipping creation",
            table_name, partition_name);
        return Ok(());  // 幂等性：已存在则返回成功
    }

    // 1. 从 Catalog 加载表信息
    let table_info = self.catalog.load_table(&table_name).await?;

    // 2. 将分区 owner 设置为当前节点
    let owner = self.cluster_manager.node_id().unwrap_or("standalone");
    self.catalog
        .create_partition(&table_name, &partition_name, owner)
        .await?;

    // 3. 使用 Engine 加载分区
    let partition_path = self.catalog.partition_dir(&table_name, &partition_name);
    self.engine
        .load_partition(
            &table_name,
            &partition_name,
            partition_path,
            table_info.table.schema,
        )
        .await?;

    // 4. 通过 gossip 发布分区信息
    if let Some(cm) = self.cluster_manager.as_ref() {
        let version = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        cm.put_partition(&table_name, &partition_name, version).await;
    }

    Ok(())
}
```

**关键点**：
- ✅ **幂等性**：分区已存在直接返回成功
- ✅ **并发安全**：多个节点同时创建同一分区不会出错
- ✅ **Gossip 广播**：分区信息传播到所有节点

### 3. do_put() - 仅接收数据

**文件**: `src/calm/flight_service.rs`

```rust
async fn do_put(
    &self,
    request: Request<Streaming<FlightData>>,
) -> Result<Response<Self::DoPutStream>, Status> {
    // ... 解析 FlightDescriptor，收集 RecordBatch ...

    for batch in batches {
        // ✅ 检查分区是否存在（由 insert_data 负责按需创建）
        if self.calm_service.engine
            .get_partition(&table_name, &partition_name).await.is_none()
        {
            // ❌ 不应该到达这里，因为 insert_data 已经创建了分区
            return Err(Status::not_found(format!(
                "Partition '{}/{}' does not exist. \
                 Partition should be created by insert_data before sending to remote.",
                table_name, partition_name
            )));
        }

        // ✅ 直接插入 RecordBatch
        self.calm_service.engine
            .insert_batch(&table_name, &partition_name, batch).await
            .map_err(|e| Status::internal(format!("Insert failed: {}", e)))?;
    }

    Ok(response)
}
```

**关键点**：
- ✅ **不创建分区**：do_put 只负责接收数据
- ✅ **分区必须存在**：不存在则返回错误
- ✅ **简单可靠**：职责单一，易于调试

## 完整流程示例

### 场景：插入数据到 DatetimeRange 分区表

```graphql
# 1. 创建表（不预创建分区）
mutation {
  createTable(input: {
    name: "events"
    partitionStrategy: {
      strategyType: DATETIMERANGE
      datetimeRange: {
        field: "timestamp"
        granularity: Day
        timezone: "UTC"
        parallelism: 2
      }
    }
    fields: [...]
  })
}

# 2. 插入数据（自动创建分区）
mutation {
  insertData(input: {
    table: "events"
    data: [
      {"event_id": 1, "timestamp": 1704067200000}  # 2024-01-01
    ]
  })
}
```

### 执行流程

```
1. insertData → CalmService::insert_data()
   ↓
2. Router 计算分区名：20240101_0 或 20240101_1
   ↓
3. 检查分区归属（通过 Gossip）
   ├─ 本地分区？
   │   ↓
   │   检查分区是否存在？
   │   ├─ 存在 → insert_batch()
   │   └─ 不存在 → create_partition(tarpc) → insert_batch()
   │        ↓
   │        create_partition 执行：
   │        - 检查已存在？返回成功
   │        - 不存在？创建 → Gossip 广播
   │
   └─ 远程分区？
       ↓
       flight_do_put() → 发送数据到远程节点
       ↓
       远程节点 do_put 接收：
       - 检查分区存在
       - insert_batch()
```

## 并发场景处理

### 场景1：多个客户端同时插入同一时间段数据

```
客户端A: INSERT 2024-01-01 数据 → 计算分区 20240101_0
客户端B: INSERT 2024-01-01 数据 → 计算分区 20240101_0
    ↓                                    ↓
检测分区不存在                       检测分区不存在
    ↓                                    ↓
create_partition(20240101_0)        create_partition(20240101_0)
    ↓                                    ↓
    ├─ A先到达：创建分区成功
    └─ B后到达：检测到已存在 → 返回成功（幂等）
```

**结果**：
- ✅ 两个客户端都成功插入数据
- ✅ 分区只创建一次
- ✅ 无并发冲突

### 场景2：分布式环境下的并发创建

```
Node1: 路由数据到本地 → 检测分区不存在 → create_partition
Node2: 路由数据到本地 → 检测分区不存在 → create_partition
    ↓                                           ↓
两个节点都调用 tarpc create_partition
    ↓
通过 #[coordinator_route] 路由到 Coordinator
    ↓
Coordinator 串行处理：
    - 第一个请求：创建分区 → Gossip 广播
    - 第二个请求：检测到已存在 → 返回成功
```

**结果**：
- ✅ 分区只在一个节点创建
- ✅ Gossip 广播到所有节点
- ✅ 所有节点最终一致

## 错误处理

### 1. 预创建策略的分区不存在

```rust
if !table_meta.table.partition_strategy.should_precreate_partitions() {
    // 按需创建
} else {
    // 预创建策略（PKHash, Hash, None）
    return Err(CoreError::NotExisted(
        format!("Partition should be pre-created")
    ));
}
```

### 2. 远程节点分区不存在

```rust
// do_put 中
if engine.get_partition(&table_name, &partition_name).await.is_none() {
    return Err(Status::not_found(
        "Partition should be created by insert_data before sending to remote."
    ));
}
```

**原因**：这通常表示 bug，因为 insert_data 应该在发送前创建分区。

### 3. create_partition 失败

可能原因：
- Catalog 写入失败
- Engine 加载失败
- Gossip 广播失败（警告，不影响创建）

## 优点

1. **职责清晰**
   - `insert_data`: 路由数据 + 按需创建分区
   - `create_partition`: 创建分区（幂等）
   - `do_put`: 仅接收数据

2. **并发安全**
   - 幂等性设计
   - Coordinator 串行化处理
   - Gossip 最终一致性

3. **性能优化**
   - 按需创建，节省资源
   - 并发插入不同分区
   - 本地插入无网络开销

4. **易于调试**
   - 清晰的日志输出
   - 明确的错误信息
   - 简单的调用链

## 测试建议

### 1. 基本功能测试

```bash
# 测试按需创建
./test_datetime_range_basic.sh

# 测试多时间段
./test_datetime_range_multi_day.sh

# 测试并行度
./test_datetime_range_parallelism.sh
```

### 2. 并发测试

```bash
# 多客户端同时插入同一时间段
./test_datetime_range_concurrent.sh

# 多节点同时创建分区
./test_datetime_range_distributed_concurrent.sh
```

### 3. 故障恢复测试

```bash
# 节点宕机后重启
./test_datetime_range_recovery.sh

# Gossip 延迟场景
./test_datetime_range_gossip_delay.sh
```

## 总结

✅ **已实现**：
1. insert_data 中按需创建分区
2. create_partition 幂等性处理
3. do_put 简化为纯数据接收
4. 并发创建的安全性
5. 分布式协调（通过 tarpc + Gossip）

🎯 **适用于**：
- DatetimeRange 分区（时序数据）
- Range 分区（范围数据）
- Custom 分区（自定义分区）

⚠️ **注意**：
- PKHash、Hash、None 策略仍然是预创建
- 预创建策略的分区不存在是错误，不会自动创建
