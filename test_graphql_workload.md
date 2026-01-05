# 死锁诊断测试 - GraphQL 查询版本

## 目的
排除 MySQL 协议的 `block_in_place` 死锁问题，使用 GraphQL 查询来验证核心锁机制。

## 测试脚本对比

### mixed_workload.py（原版）
- 写入：MySQL INSERT
- 查询：MySQL SELECT
- 问题：MySQL 协议使用 `block_in_place + block_on(stream.next().await)` 导致死锁

### mixed_workload_graphql.py（新版）
- 写入：MySQL INSERT（保持不变）
- 查询：GraphQL Query（避免 MySQL 协议）
- 目的：验证是否是 MySQL 协议特有问题

## 使用方法

### 1. 启动服务（TRACE 日志）
```bash
# 编辑 calm.toml，设置日志级别
[log]
level = "trace"
target = "console"

# 启动服务
cargo run --release --bin calm -- --config calm.toml
```

### 2. 运行测试
```bash
python3 scripts/mixed_workload_graphql.py \
    --graphql-endpoint http://127.0.0.1:9567/graphql \
    --mysql-host 127.0.0.1 \
    --mysql-port 3307 \
    --mysql-user root \
    --table events \
    --insert-threads 4 \
    --query-threads 2 \
    --duration 60
```

### 3. 观察结果

#### 如果仍然死锁：
- **说明问题在核心锁机制**（Partition/Segment 的 parking_lot 锁）
- 日志应该能看到：
  ```
  🔒 [upsert] Acquiring write_lock for events/partition_0
  (长时间停顿...)
  ```

#### 如果不死锁：
- **说明问题是 MySQL 协议特有的**（`block_in_place` 死锁）
- 解决方案：
  1. 增加 tokio worker 线程数（已设置 4 个）
  2. 改用 `spawn_blocking` 而不是 `block_in_place`
  3. 重构 MySQL 协议为完全异步

## 关键日志监控

### 写入路径（TRACE 级别）
```
🔒 [upsert] Acquiring write_lock for events/partition_0
✅ [upsert] Acquired write_lock after 50µs
📋 [upsert] Cloned 3 frozen segments
🔍 [upsert] Found 0 segments with duplicates, took 2ms
✅ [upsert] Completed write, total time 5ms
```

### 查询路径（TRACE 级别）
```
🔒 [process_partition_segments] Acquiring current_segment read lock
✅ [process_partition_segments] Got current_segment guard after 10µs
🔓 [process_partition_segments] Released current_segment guard
🔒 [process_partition_segments] Acquiring frozen_segments read lock
✅ [process_partition_segments] Got frozen_segments guard (3 segments) after 15µs
🔓 [process_partition_segments] Released frozen_segments guard
```

### 死锁检测器（每 5 秒检查一次）
```
✅ Deadlock detector started (checking every 5 seconds)
🔍 Deadlock detector: 12 checks completed, no deadlocks detected
```

或者检测到死锁时：
```
╔══════════════════════════════════════════════════════════════╗
║  🚨 DEADLOCK DETECTED! 检测到死锁！                          ║
╚══════════════════════════════════════════════════════════════╝
```

## 已知问题总结

### 问题 1：MySQL 协议 block_in_place 死锁
**原因**：
```rust
tokio::task::block_in_place(|| {
    tokio::runtime::Handle::current().block_on(async {
        while let Some(batch) = stream.next().await {  // ← 阻塞等待
            // 但生产者任务（tokio::spawn）无法运行！
        }
    })
})
```

**触发条件**：
- tokio runtime 线程不足
- `block_in_place` 占用所有工作线程
- 生产者任务无法被调度
- 消费者永远等待数据

**解决方案**：
1. ✅ 增加 tokio worker 线程：`#[tokio::main(worker_threads = 4)]`
2. ⏳ 测试 GraphQL 查询是否避免此问题

### 问题 2：Parking Lot 锁在 async 中跨 await 点
**原因**：
```rust
let guard = partition.get_current_segment();  // parking_lot::RwLockReadGuard
process_segment(...).await;  // ❌ 跨 await 点持有锁
```

**解决方案**：
✅ 已修复：立即提取数据并释放 guard

### 问题 3：upsert 中 write_lock 持有时间过长
**原因**：
```rust
let _write_guard = self.write_lock.lock();  // 获取锁
let segments = self.frozen_segments.read().clone();  // 克隆可能慢
let dels = segments.par_iter()...;  // 并行查询耗时
self.write(&data, ...);  // 写入
// 锁在这里才释放
```

**影响**：
- 如果有多个写入线程，会串行等待
- 但不应该影响读取（读取用 read guard）

**待优化**：
- 缩短 write_lock 持有时间
- 先查询，再加锁写入

## 测试预期

### 成功场景（不死锁）
```
📊 [5s] Insert: 2000 batches (400.0 QPS, 2.5ms p50) | Query: 100 reqs (20.0 QPS, 15.0ms p50)
📊 [10s] Insert: 4000 batches (400.0 QPS, 2.6ms p50) | Query: 200 reqs (20.0 QPS, 16.2ms p50)
...
```

### 失败场景（死锁）
```
📊 [5s] Insert: 2000 batches (400.0 QPS, 2.5ms p50) | Query: 100 reqs (20.0 QPS, 15.0ms p50)
📊 [10s] Insert: 2050 batches (10.0 QPS, 5000ms p50) | Query: 100 reqs (0 QPS, ∞ms p50)
(系统停止响应...)
```

## 下一步

1. **运行测试**：`python3 scripts/mixed_workload_graphql.py`
2. **观察日志**：查找长时间的 "Acquiring" 日志
3. **分析结果**：
   - 如果还死锁 → 核心锁问题
   - 如果不死锁 → MySQL 协议问题
