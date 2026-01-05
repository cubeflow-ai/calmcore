# Parking Lot 死锁检测使用说明

## 已完成的修改

### 1. 添加 parking_lot 依赖
在 `Cargo.toml` 中添加：
```toml
parking_lot = { version = "0.12", features = ["deadlock_detection"] }
```

### 2. 创建死锁检测器模块
文件：`src/utils/deadlock_detector.rs`

该模块启动一个后台线程，每 10 秒检测一次是否存在死锁。如果检测到死锁，会打印详细的线程 ID 和堆栈信息。

### 3. 替换核心锁为 parking_lot

已替换的文件：
- `src/storage/partition.rs`: 将 `std::sync::{Mutex, RwLock}` 替换为 `parking_lot::{Mutex, RwLock}`
- `src/storage/segment/mod.rs`: 将 `std::sync::RwLock` 替换为 `parking_lot::RwLock`

### 4. 启动死锁检测
在 `src/bin/calm/main.rs` 中，main 函数开头调用：
```rust
deadlock_detector::start_deadlock_detector();
```

## 工作原理

### Parking Lot 死锁检测机制
`parking_lot` 的 `deadlock_detection` 功能会追踪所有线程持有的锁和等待的锁，并检测是否形成循环依赖：

```
线程 A: 持有锁 X，等待锁 Y
线程 B: 持有锁 Y，等待锁 X
→ 检测到死锁！
```

### 输出格式
当检测到死锁时，会输出：
```
Deadlock detected!
The following threads are blocked on each other:
Thread 123456:
  - Holding: ...
  - Waiting: ...
  Stack trace:
    at function_a (file.rs:42)
    at function_b (file.rs:78)
    ...

Thread 789012:
  - Holding: ...
  - Waiting: ...
  Stack trace:
    ...
```

## 使用方法

### 1. 编译项目
```bash
cargo build --release
```

### 2. 启动服务
```bash
cargo run --bin calm -- --config calm.toml
```

### 3. 触发并发写入/读取
使用之前的混合负载脚本：
```bash
python3 scripts/mixed_workload.py
```

或者使用 MySQL 客户端并发操作：

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
如果存在死锁，会在控制台或日志文件中看到详细的死锁报告，包括：
- 涉及的线程 ID
- 每个线程持有和等待的锁
- 完整的调用堆栈

## 优势对比

### 手动日志方式 (已放弃)
- ❌ 需要在所有锁操作前后添加日志
- ❌ 日志量巨大，难以分析
- ❌ 需要手动推断死锁关系
- ❌ 侵入性强，影响性能

### Parking Lot 自动检测
- ✅ 零侵入，无需修改业务逻辑
- ✅ 自动追踪所有锁依赖关系
- ✅ 只在检测到死锁时输出
- ✅ 提供完整的堆栈追踪
- ✅ 业界标准方案，Rust 社区广泛使用

## 注意事项

### 1. 性能影响
`deadlock_detection` 会有轻微的性能开销（追踪锁依赖关系），建议：
- 开发/测试环境：始终启用
- 生产环境：可以通过 feature flag 控制

### 2. 异步锁 (tokio::sync)
`parking_lot` 只能检测**同步锁**的死锁（`Mutex`, `RwLock`），不能检测 `tokio::sync::Mutex` 等异步锁。

当前代码中：
- `Partition`, `Segment`: 使用同步锁（已替换为 parking_lot）✅
- `CalmService`, `Locker`: 使用 tokio 异步锁（无法检测）⚠️

如果异步锁也存在死锁，需要使用 `tokio-console`。

### 3. 与 tokio-console 配合
如果需要检测异步任务死锁：
```toml
[dependencies]
console-subscriber = "0.2"
```

```rust
// 在 main 函数开头
console_subscriber::init();
```

然后运行：
```bash
tokio-console http://localhost:6669
```

## 解决方案

一旦检测到死锁，根据堆栈信息定位问题后，可以采取以下措施：

### 1. 缩短锁持有时间
```rust
// ❌ 错误：在持有 read guard 期间做大量计算
let guard = partition.get_frozen_segments();
let result = expensive_operation(&guard);  // 持锁时间过长

// ✅ 正确：先克隆数据再释放锁
let segments = {
    let guard = partition.get_frozen_segments();
    guard.iter().map(|s| s.clone()).collect::<Vec<_>>()
};  // guard 在此处被释放
let result = expensive_operation(&segments);
```

### 2. 统一锁顺序
```rust
// ❌ 错误：不同线程以不同顺序获取锁
// 线程 A: lock_x → lock_y
// 线程 B: lock_y → lock_x

// ✅ 正确：所有线程按相同顺序获取锁
// 所有线程: 始终先 lock_x，再 lock_y
```

### 3. 使用更细粒度的锁
```rust
// ❌ 错误：一个大锁保护所有数据
struct Partition {
    data: RwLock<PartitionData>,  // 包含所有字段
}

// ✅ 正确：每个字段独立锁
struct Partition {
    current_segment: RwLock<Segment>,
    frozen_segments: RwLock<Vec<Arc<Segment>>>,
    // ...
}
```

## 下一步

1. **运行并发测试**：使用 `scripts/mixed_workload.py` 或手动并发操作
2. **观察死锁检测输出**：查看是否有死锁报告
3. **根据堆栈定位问题**：找到涉及的代码路径
4. **应用修复方案**：缩短锁持有时间或调整锁顺序
