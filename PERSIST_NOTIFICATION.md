# 持久化通知机制 - 改进版

## 🎯 设计改进

根据反馈，原方案只是打印提示，没有实际通知 Engine。现在改进为：

### ✅ 改进后的架构

```
┌─────────────────────────────────────────────────────────────────┐
│                      Partition 写入流程                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  1. Partition.write()                                           │
│       ├─ 写入数据到 current_segment                             │
│       └─ 轻量级检查: doc_count >= max_docs_per_segment?        │
│          ├─ 是 → 发送通知到 Engine (非阻塞)  ✅                │
│          └─ 否 → 继续                                           │
│                                                                 │
│  2. Partition.flush()                                           │
│       ├─ 将 current_segment 移到 frozen_segments                │
│       └─ 检查: segment.should_persist()?                        │
│          ├─ 是 → 发送通知到 Engine (非阻塞)  ✅                │
│          └─ 否 → 继续                                           │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                      Engine 后台任务                             │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  tokio::select! {                                               │
│      // 1. 接收 Partition 通知（主要触发）✅                    │
│      Some(partition_id) = partition_notify_rx.recv() => {       │
│          handle_partition_persist(partition_id);                │
│      }                                                           │
│                                                                 │
│      // 2. 接收手动持久化请求                                   │
│      Some(request) = persist_rx.recv() => {                     │
│          match request { ... }                                  │
│      }                                                           │
│                                                                 │
│      // 3. 定时检查（兜底保障）                                 │
│      _ = interval.tick() => {                                   │
│          handle_all_persist();  // 防止遗漏                     │
│      }                                                           │
│  }                                                               │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## 🔧 实现细节

### 1. Partition 通知机制

```rust
pub struct Partition {
    // ...
    /// 持久化通知通道（通知 Engine）
    persist_notify: Option<mpsc::UnboundedSender<u64>>,
}

impl Partition {
    /// 由 Engine 在创建/加载时设置
    pub(crate) fn set_persist_notify(&mut self, tx: mpsc::UnboundedSender<u64>) {
        self.persist_notify = Some(tx);
    }

    fn write(&self, data: &RecordBatch, ...) -> CoreResult<Vec<u64>> {
        // 写入数据
        let result = current_segment.write(...);

        // 轻量级检查
        if current_segment.doc_count() >= max_docs_per_segment {
            // 通知 Engine ✅
            if let Some(ref tx) = self.persist_notify {
                let _ = tx.send(self.id);  // 非阻塞发送
            }
        }

        result
    }

    pub fn flush(&self) -> CoreResult<u64> {
        // flush 操作...

        // 检查并通知
        if segment.should_persist() {
            if let Some(ref tx) = self.persist_notify {
                let _ = tx.send(self.id);  // 非阻塞发送
            }
        }

        Ok(seg_id)
    }
}
```

### 2. Engine 接收通知

```rust
pub struct Engine {
    /// 持久化请求通道（手动触发）
    persist_tx: mpsc::UnboundedSender<PersistRequest>,
    
    /// Partition 通知通道（自动触发）✅
    partition_notify_tx: mpsc::UnboundedSender<u64>,
}

impl Engine {
    fn new_internal(config: EngineConfig) -> Self {
        let (persist_tx, persist_rx) = mpsc::unbounded_channel();
        let (partition_notify_tx, partition_notify_rx) = mpsc::unbounded_channel();

        // 启动后台任务，传入两个接收端
        let handle = tokio::spawn(Self::persist_background_task(
            partitions,
            persist_rx,
            partition_notify_rx,  // ✅ 接收 Partition 通知
            config,
        ));

        Self {
            persist_tx,
            partition_notify_tx,  // ✅ 给 Partition 用
            // ...
        }
    }

    pub async fn create_partition(&self, id: u32, schema: Schema) -> Arc<Partition> {
        let mut partition = Partition::new(id, partition_dir, schema);
        
        // 设置通知回调 ✅
        partition.set_persist_notify(self.partition_notify_tx.clone());
        
        Arc::new(partition)
    }
}
```

### 3. 后台任务处理

```rust
async fn persist_background_task(
    partitions: Arc<RwLock<HashMap<u64, Arc<Partition>>>>,
    mut persist_rx: mpsc::UnboundedReceiver<PersistRequest>,
    mut partition_notify_rx: mpsc::UnboundedReceiver<u64>,  // ✅ 新增
    config: EngineConfig,
) {
    let mut interval = tokio::time::interval(...);

    loop {
        tokio::select! {
            // 1. Partition 主动通知（最常见）✅
            Some(partition_id) = partition_notify_rx.recv() => {
                println!("[Engine] Received persist notification from partition {}", partition_id);
                Self::handle_partition_persist(
                    &partitions,
                    partition_id,
                    &config,
                    &active_tasks,
                ).await;
            }

            // 2. 手动触发
            Some(request) = persist_rx.recv() => {
                match request {
                    PersistRequest::CheckPartition(id) => { ... }
                    PersistRequest::CheckAll => { ... }
                    PersistRequest::Shutdown => { break; }
                }
            }

            // 3. 定时检查（兜底）
            _ = interval.tick() => {
                // 不打印日志，静默检查
                Self::handle_all_persist(...).await;
            }
        }
    }
}
```

## 📊 工作流程对比

### ❌ 改进前（只打印）

```
write() → 检查阈值 → println!("达到限制") → 无操作
                        ↓
                  Engine 必须定期扫描才能发现
                        ↓
                  最坏延迟 = 检查间隔（60秒）
```

### ✅ 改进后（主动通知）

```
write() → 检查阈值 → tx.send(partition_id) → Engine 立即收到
                        ↓                        ↓
                  非阻塞发送            handle_partition_persist()
                        ↓                        ↓
                  继续写入                  异步持久化
                        
延迟 ≈ 0（立即响应）✨
```

## 🎯 三种触发方式

### 1. 主动通知（Primary）✅

**触发条件**：

- `write()` 检测到 `doc_count >= max_docs_per_segment`
- `flush()` 检测到 `segment.should_persist()`

**优点**：

- ⚡ 实时响应（毫秒级）
- 🎯 精确触发（知道哪个 Partition）
- 🚀 无轮询开销

**代码**：

```rust
if current_segment.doc_count() >= max_docs_per_segment {
    if let Some(ref tx) = self.persist_notify {
        let _ = tx.send(self.id);  // 非阻塞
    }
}
```

### 2. 手动触发（Manual）

**触发方式**：

```rust
// 触发特定 Partition
engine.trigger_persist(partition_id);

// 触发所有 Partition
engine.trigger_persist_all();
```

**使用场景**：

- 批量导入完成后
- 用户手动触发
- 关闭前强制持久化

### 3. 定期检查（Fallback）

**触发间隔**：`persist_check_interval_secs`（默认 60秒）

**作用**：

- 🛡️ 兜底保障（防止通知丢失）
- ⏰ 检查时间阈值（`max_segment_age`）
- 📊 全局扫描（发现遗漏的 segments）

**优化**：不打印日志（静默检查）

## 📈 性能分析

### 通知开销

| 操作 | 开销 | 说明 |
|------|------|------|
| `tx.send()` | ~100ns | mpsc unbounded channel |
| 阻塞？ | ❌ 否 | 异步通知，不阻塞写入 |
| 内存 | 8 bytes | 只发送 partition_id (u64) |

### 响应时间

| 方式 | 延迟 | 说明 |
|------|------|------|
| 主动通知 | < 1ms | 立即发送并处理 |
| 定期检查 | 最多 60s | 取决于检查间隔 |

### 对比

```
改进前（纯定期）:
write() → [等待 60s] → Engine 检查 → 发现 → 持久化
延迟：0-60秒（平均 30秒）

改进后（主动通知）:
write() → [< 1ms] → Engine 收到 → 持久化
延迟：< 1ms ✨
```

## 🎛️ 配置建议

### 高频写入场景

```rust
Schema {
    persist_policy: PersistPolicy {
        max_docs_per_segment: 100_000,     // 10万条
        max_segment_age: Duration::from_secs(600),  // 10分钟（不重要）
        check_on_flush: true,               // flush 时通知
    }
}

EngineConfig {
    persist_check_interval_secs: 300,      // 5分钟检查（兜底）
    min_unpersisted_segments: 1,
    max_concurrent_persists: 8,
}
```

**效果**：

- 写满 10万条 → 立即通知 → 快速持久化
- 定期检查不重要（主要靠通知）

### 低频写入场景

```rust
Schema {
    persist_policy: PersistPolicy {
        max_docs_per_segment: 10_000,      // 1万条
        max_segment_age: Duration::from_secs(180),  // 3分钟
        check_on_flush: true,
    }
}

EngineConfig {
    persist_check_interval_secs: 60,       // 1分钟检查
    min_unpersisted_segments: 1,
    max_concurrent_persists: 2,
}
```

**效果**：

- 写满 1万条 → 立即通知
- 未写满但超过 3分钟 → 定期检查发现 → 持久化

## 🚀 使用示例

```rust
use calm::engine::{Engine, EngineConfig};
use calm::schema::{Schema, PersistPolicy};
use std::time::Duration;

#[tokio::main]
async fn main() {
    // 1. 初始化 Engine
    Engine::initialize(EngineConfig {
        data_dir: "/data".into(),
        persist_check_interval_secs: 60,  // 1分钟兜底检查
        min_unpersisted_segments: 1,
        max_concurrent_persists: 4,
        check_after_flush: true,
    });

    // 2. 创建 Partition（自动设置通知回调）
    let engine = Engine::global();
    let schema = Schema {
        name: "logs".into(),
        persist_policy: PersistPolicy {
            max_docs_per_segment: 50_000,
            max_segment_age: Duration::from_secs(300),
            check_on_flush: true,
        },
        // ...
    };
    
    let partition = engine.create_partition(1, schema).await;

    // 3. 写入数据（自动检查并通知）
    for i in 0..100 {
        let batch = create_batch(i * 1000, 1000);
        partition.upsert(batch)?;
        
        // 写满 50K 后自动通知 Engine ✅
        // Engine 立即开始持久化 ✅
    }

    // 4. 手动 flush（自动检查并通知）
    partition.flush()?;
    
    // 5. Engine 后台自动处理
    // 无需手动调用，一切自动化！✨
}
```

## 💡 优势总结

### ✅ 相比纯定期检查

| 维度 | 纯定期 | **改进后** |
|------|--------|-----------|
| 响应延迟 | 0-60秒 | **< 1ms** ✨ |
| CPU 开销 | 定期扫描 | **按需触发** |
| 实时性 | ❌ 差 | **✅ 优秀** |
| 资源利用 | 浪费 | **高效** |

### ✅ 相比纯通知

| 维度 | 纯通知 | **改进后（通知+兜底）** |
|------|--------|----------------------|
| 通知丢失 | ❌ 无保障 | **✅ 定期检查兜底** |
| 时间阈值 | ❌ 难实现 | **✅ 定期检查时间** |
| 可靠性 | ⚠️ 一般 | **✅ 高** |

## 🎯 核心改进点

1. **write() 不再只是打印** ✅
   - 现在：发送通知到 Engine
   - 效果：立即触发持久化

2. **Engine 多路监听** ✅
   - Partition 通知（主要）
   - 手动触发（辅助）
   - 定期检查（兜底）

3. **三重保障机制** ✅
   - 主动通知（实时）
   - 手动触发（灵活）
   - 定期扫描（可靠）

---

**就是这样！主动通知 + 定期兜底 = 完美方案！** 🎉
