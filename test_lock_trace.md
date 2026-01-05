# 锁追踪与死锁排查指南

## 已添加的追踪功能

### 1. 死锁检测器增强
- **检测间隔**：5 秒（原 10 秒）
- **启动确认**：服务启动时会打印 "✅ Deadlock detector started"
- **健康检查**：每 1 分钟打印一次检测状态（DEBUG 级别）

### 2. 写入路径锁追踪
在 `Partition::upsert()` 中添加了详细的 TRACE 级别日志：
```
🔒 Acquiring write_lock
✅ Acquired write_lock after Xms
📋 Cloned N frozen segments
🔍 Found N segments with duplicates, took Xms
✅ Completed write, total time Xms
```

### 3. 查询路径锁追踪
在 `process_partition_segments()` 中添加了 TRACE 级别日志：
```
🔒 Acquiring current_segment read lock
✅ Got current_segment guard after Xms
🔓 Released current_segment guard
🔒 Acquiring frozen_segments read lock
✅ Got frozen_segments guard (N segments) after Xms
🔓 Released frozen_segments guard, prepared N scanners
```

## 测试步骤

### 1. 启用 TRACE 日志
编辑 `calm.toml`：
```toml
[log]
level = "trace"  # 或者设置环境变量 RUST_LOG=trace
target = "console"
```

### 2. 启动服务
```bash
cargo run --release --bin calm -- --config calm.toml
```

应该看到：
```
✅ Deadlock detector started (checking every 5 seconds)
```

### 3. 并发测试

**终端 1 - 持续写入：**
```bash
while true; do
  mysql -h127.0.0.1 -P3307 -uroot -e "
    INSERT INTO events (event_id, user_id, event_name, created_at)
    VALUES (FLOOR(RAND()*1000000), FLOOR(RAND()*10000), 'test', NOW());
  "
  sleep 0.01
done
```

**终端 2 - 持续查询：**
```bash
while true; do
  mysql -h127.0.0.1 -P3307 -uroot -e "
    SELECT COUNT(*) FROM events WHERE user_id < 5000;
  "
  sleep 0.01
done
```

### 4. 观察日志

查看服务日志，重点关注：

#### 正常情况：
```
TRACE [upsert] Acquiring write_lock for events/partition_0
TRACE [upsert] Acquired write_lock after 50µs
TRACE [upsert] Completed write, total time 2ms
TRACE [process_partition_segments] Acquiring current_segment read lock
TRACE [process_partition_segments] Got current_segment guard after 10µs
TRACE [process_partition_segments] Released current_segment guard
```

#### 异常情况：
```
TRACE [upsert] Acquiring write_lock for events/partition_0
TRACE [upsert] Acquired write_lock after 5.2s  ⚠️ 等待时间过长！
```

或者：
```
TRACE [process_partition_segments] Acquiring frozen_segments read lock
... (长时间没有后续日志) ⚠️ 可能被阻塞！
```

或者死锁检测器输出：
```
╔══════════════════════════════════════════════════════════════╗
║  🚨 DEADLOCK DETECTED! 检测到死锁！                          ║
╚══════════════════════════════════════════════════════════════╝
```

## Parking Lot 死锁检测的局限性

### ✅ 能检测到的死锁类型：
1. **循环等待**：线程 A 等待线程 B 持有的锁，线程 B 等待线程 A 持有的锁
2. **多线程同步锁死锁**：多个线程之间形成锁依赖环

### ❌ 检测不到的问题：
1. **Async 中跨 await 持锁**：
   ```rust
   let guard = lock.read();  // 持有锁
   some_async_call().await;  // 等待时仍持有锁！
   ```
   
2. **Channel 满导致的阻塞**：
   ```rust
   let guard = lock.read();
   tx.send(data).await;  // Channel 满，等待消费者
   ```

3. **单线程自旋等待**：线程在持有锁的情况下等待自己

## 当前架构的潜在问题点

### 1. upsert 中的 write_lock 持有时间
```rust
let _write_guard = self.write_lock.lock();  // ⚠️ 获取锁
let segments = self.frozen_segments.read().clone();  // 克隆可能很慢

// ⚠️ 并行查询所有 segment（耗时操作）
let dels: Vec<_> = segments.par_iter()
    .filter_map(|segment| segment.mget_internal_id(...))
    .collect();

self.write(&data, ...)  // ⚠️ 写入操作
// 锁在这里才释放
```

**影响**：
- 如果有大量 frozen segments，并行查询会很慢
- 在此期间，任何需要 write_lock 的操作都会被阻塞
- 查询操作可能不受影响（使用 read guard）

### 2. 查询路径的潜在阻塞
虽然我们已经修复了跨 await 点持锁的问题，但如果：
- Channel 容量不足（当前 10）
- 数据生产速度 > 消费速度
- 仍然可能在 `tx.send(batch).await` 处阻塞

## 优化建议

### 短期方案（追踪问题）：
1. ✅ 启用 TRACE 日志，观察锁持有时间
2. ✅ 使用死锁检测器监控
3. 增加 Channel 容量（如果是 backpressure 问题）

### 中期方案（减少锁争用）：
1. **缩短 write_lock 持有时间**：
   ```rust
   // 先不持锁查询
   let dels = self.find_duplicates_without_lock(&pk_hash, column);
   
   // 再获取锁执行写入
   let _guard = self.write_lock.lock();
   self.write(&data, Some(pk_hash), Some(dels))
   ```

2. **使用更细粒度的锁**：分离读写路径的锁

### 长期方案（架构改进）：
1. **使用 tokio::sync::RwLock**：支持 async 的锁，可以跨 await 点
2. **无锁数据结构**：使用 Arc + CopyOnWrite
3. **Actor 模型**：每个 Partition 一个 actor，串行处理请求

## 下一步

运行测试并发送日志，重点关注：
1. 是否有 "Acquired write_lock after XXs" 等待时间超过 1 秒的日志？
2. 是否有死锁检测器报告？
3. 日志中是否有长时间停顿（某个 "Acquiring" 后没有对应的 "Acquired"）？
