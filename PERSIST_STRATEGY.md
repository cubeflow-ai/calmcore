# Segment 持久化策略

## 📋 概述

本文档说明 Segment 的持久化策略，采用**混合方案**：

- ✅ **write() 中轻量级检查**（文档数）
- ✅ **Engine 定期检查**（文档数 + 时间）

## 🎯 设计目标

1. **不阻塞写入**：write() 只做轻量级检查，不执行实际持久化
2. **实时响应**：写满立即提示，不需要等待定期检查
3. **时间保证**：即使文档数未达标，超时也会持久化
4. **全局优化**：Engine 可以协调多个 Partition 的持久化优先级

## 📐 架构设计

```
┌─────────────────────────────────────────────────────────────────┐
│                      写入流程 (Partition)                        │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  1. Partition.upsert()                                          │
│       ↓                                                         │
│  2. Partition.write()                                           │
│       ├─ 写入 current_segment                                   │
│       └─ 轻量级检查: doc_count >= max_docs_per_segment?        │
│          ├─ 是 → 打印提示 (不阻塞)                             │
│          └─ 否 → 继续                                           │
│       ↓                                                         │
│  3. 用户调用 flush() (可选)                                     │
│       ├─ 将 current_segment 移到 frozen_segments                │
│       └─ 检查: segment.should_persist()?                        │
│          ├─ 是 → 打印提示 (不阻塞)                             │
│          └─ 否 → 继续                                           │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                  定期检查流程 (Engine)                           │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  1. Engine.persist_background_task()                            │
│       ├─ 每隔 persist_check_interval_secs 秒                    │
│       └─ 或接收到手动触发请求                                   │
│       ↓                                                         │
│  2. 遍历所有 Partition                                          │
│       ↓                                                         │
│  3. 对每个 Partition:                                           │
│       ├─ 获取 unpersisted_segments                              │
│       ├─ 过滤: segment.should_persist()?                        │
│       │   ├─ doc_count >= max_docs_per_segment?  ✓             │
│       │   └─ age >= max_segment_age?              ✓             │
│       ├─ 检查: 达到 min_unpersisted_segments?                   │
│       └─ 启动异步持久化任务                                     │
│           └─ tokio::spawn_blocking(persist)                     │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## ⚙️ 配置参数

### Schema.PersistPolicy

在 Schema 级别配置，控制 Segment 何时应该被持久化：

```rust
pub struct PersistPolicy {
    /// 文档数阈值（达到此数量触发持久化）
    pub max_docs_per_segment: u64,
    
    /// 时间阈值（segment 存活超过此时间触发持久化）
    pub max_segment_age: Duration,
    
    /// 是否在 flush 时立即检查持久化
    pub check_on_flush: bool,
}

impl Default for PersistPolicy {
    fn default() -> Self {
        Self {
            max_docs_per_segment: 100_000,          // 10万条文档
            max_segment_age: Duration::from_secs(300), // 5分钟
            check_on_flush: true,
        }
    }
}
```

### EngineConfig

在 Engine 级别配置，控制持久化调度策略：

```rust
pub struct EngineConfig {
    pub persist_check_interval_secs: u64,    // 定期检查间隔（默认 60秒）
    pub min_unpersisted_segments: usize,     // 最小持久化数量（默认 1）
    pub max_concurrent_persists: usize,      // 最大并发持久化（默认 4）
    pub check_after_flush: bool,             // flush 后立即检查（默认 true）
}
```

## 🔍 检查逻辑详解

### 1. Segment.should_persist()

每个 Segment 根据自身状态判断是否需要持久化：

```rust
impl Segment {
    pub fn should_persist(&self) -> bool {
        if self.is_persisted() {
            return false; // 已持久化
        }

        let policy = &self.schema.persist_policy;

        // 检查文档数阈值
        let doc_count = self.doc_count() as u64;
        if doc_count >= policy.max_docs_per_segment {
            return true;  // 达到文档数限制
        }

        // 检查时间阈值
        if self.age() >= policy.max_segment_age {
            return true;  // 超过存活时间
        }

        false
    }
}
```

### 2. Partition.write() - 轻量级检查

只检查 current_segment 的文档数（不检查时间）：

```rust
fn write(&self, data: &RecordBatch, ...) -> CoreResult<Vec<u64>> {
    let current_segment = self.current_segment.read().unwrap();
    
    // 写入数据
    let result = current_segment.write(...);
    
    // 轻量级检查（只看文档数）
    if current_segment.doc_count() as u64 >= 
       self.schema.persist_policy.max_docs_per_segment {
        println!("[Partition {}] Current segment reached doc limit", self.id);
        // 不阻塞，只打印提示
    }
    
    result
}
```

### 3. Engine.handle_partition_persist() - 完整检查

Engine 定期检查所有维度：

```rust
async fn handle_partition_persist(...) {
    let unpersisted = partition.get_unpersisted_segments();
    
    // 过滤出真正需要持久化的 segments
    let segments_to_persist: Vec<_> = unpersisted
        .iter()
        .filter(|(_, segment)| segment.should_persist())  // 检查文档数 + 时间
        .collect();
    
    if segments_to_persist.len() < config.min_unpersisted_segments {
        return; // 数量不够，等下次
    }
    
    // 启动持久化任务...
}
```

## 📊 工作流程示例

### 场景 1: 高频写入，快速达到文档数阈值

```
Time    | Action                              | Check
--------|-------------------------------------|---------------------------
T0      | write 50K docs                      | write: 50K < 100K ✗
T1      | write 50K docs                      | write: 100K >= 100K ✓ (提示)
T2      | flush()                             | flush: should_persist=true ✓
T3      | Engine check (60s)                  | Engine: 1 segment ready → persist
```

**结果**：1-2 分钟内持久化（实时响应）

### 场景 2: 低频写入，依赖时间阈值

```
Time    | Action                              | Check
--------|-------------------------------------|---------------------------
T0      | write 10K docs                      | write: 10K < 100K ✗
T60     | Engine check                        | age: 60s < 300s ✗
T120    | Engine check                        | age: 120s < 300s ✗
T180    | Engine check                        | age: 180s < 300s ✗
T240    | Engine check                        | age: 240s < 300s ✗
T300    | Engine check                        | age: 300s >= 300s ✓ → persist
```

**结果**：5 分钟后持久化（时间保证）

### 场景 3: 中频写入，混合触发

```
Time    | Action                              | Check
--------|-------------------------------------|---------------------------
T0      | write 30K docs                      | write: 30K < 100K ✗
T60     | Engine check                        | age: 60s < 300s ✗
T90     | write 30K docs                      | write: 60K < 100K ✗
T150    | write 30K docs                      | write: 90K < 100K ✗
T180    | Engine check                        | age: 180s < 300s ✗
T200    | write 30K docs                      | write: 120K >= 100K ✓ (提示)
T210    | flush()                             | flush: should_persist=true ✓
T240    | Engine check                        | Engine: 1 segment ready → persist
```

**结果**：3-4 分钟内持久化（文档数优先）

## 🎛️ 配置建议

### 高吞吐场景（每秒上千条）

```rust
Schema {
    persist_policy: PersistPolicy {
        max_docs_per_segment: 100_000,     // 10万条
        max_segment_age: Duration::from_secs(600), // 10分钟
        check_on_flush: true,
    }
}

EngineConfig {
    persist_check_interval_secs: 30,       // 30秒检查
    min_unpersisted_segments: 2,           // 积累2个再持久化
    max_concurrent_persists: 8,            // 高并发
}
```

**效果**：

- 写入 10万条立即持久化
- 或者 10 分钟没写满也持久化
- 批量持久化提高效率

### 低吞吐场景（每秒几十条）

```rust
Schema {
    persist_policy: PersistPolicy {
        max_docs_per_segment: 10_000,      // 1万条
        max_segment_age: Duration::from_secs(180), // 3分钟
        check_on_flush: true,
    }
}

EngineConfig {
    persist_check_interval_secs: 60,       // 1分钟检查
    min_unpersisted_segments: 1,           // 1个就持久化
    max_concurrent_persists: 2,            // 低并发
}
```

**效果**：

- 写入 1万条立即持久化
- 或者 3 分钟没写满也持久化
- 快速释放内存

### 实时场景（秒级延迟）

```rust
Schema {
    persist_policy: PersistPolicy {
        max_docs_per_segment: 50_000,      // 5万条
        max_segment_age: Duration::from_secs(60), // 1分钟
        check_on_flush: true,
    }
}

EngineConfig {
    persist_check_interval_secs: 10,       // 10秒检查
    min_unpersisted_segments: 1,
    max_concurrent_persists: 4,
}
```

**效果**：

- 文档数或时间任一达标立即持久化
- 10秒级响应

## 🚀 使用示例

### 创建带自定义策略的 Schema

```rust
use std::time::Duration;
use calm::schema::{Schema, PersistPolicy};

let schema = Schema {
    name: "logs".into(),
    primary_key: Some("id".into()),
    store_source: true,
    fields: vec![/* ... */],
    persist_policy: PersistPolicy {
        max_docs_per_segment: 50_000,
        max_segment_age: Duration::from_secs(120), // 2分钟
        check_on_flush: true,
    },
};
```

### 初始化 Engine

```rust
use calm::engine::{Engine, EngineConfig};

Engine::initialize(EngineConfig {
    data_dir: "/data".into(),
    persist_check_interval_secs: 30,    // 30秒检查一次
    min_unpersisted_segments: 1,        // 1个就持久化
    max_concurrent_persists: 4,
    check_after_flush: true,
});
```

### 正常使用

```rust
let engine = Engine::global();
let partition = engine.create_partition(1, schema).await;

// 写入数据（自动检查）
partition.upsert(batch)?;

// Flush（自动检查）
partition.flush()?;

// Engine 后台自动定期检查并持久化
```

## 💡 优势总结

### ✅ 方案优势

1. **写入性能优秀**
   - write() 只做轻量级检查（一次原子读取）
   - 无锁竞争，无阻塞

2. **实时响应**
   - 文档数达标立即提示
   - 不需要等待定期检查

3. **时间保证**
   - 即使文档数未达标
   - 超过 max_segment_age 也会持久化

4. **全局优化**
   - Engine 可以看到所有 Partition
   - 可以协调持久化优先级
   - 可以批量持久化

5. **灵活配置**
   - Schema 级别配置（每个表不同策略）
   - Engine 级别配置（全局调度）
   - 多维度阈值（文档数 + 时间）

### 📈 性能分析

| 操作 | 开销 | 影响 |
|------|------|------|
| write() 文档数检查 | 1次原子读 | 可忽略 (~1ns) |
| flush() 持久化检查 | 1次原子读 + 1次时间计算 | 极小 (~100ns) |
| Engine 定期检查 | 遍历所有 Partition/Segment | 异步，不阻塞写入 |
| 实际持久化 | 磁盘 I/O | spawn_blocking，不阻塞 |

## 🎯 最佳实践

1. **根据写入速度调整 max_docs_per_segment**
   - 高吞吐：10万 - 100万
   - 中吞吐：1万 - 10万
   - 低吞吐：1千 - 1万

2. **根据内存限制调整 max_segment_age**
   - 内存充足：10分钟 - 1小时
   - 内存紧张：1分钟 - 5分钟

3. **定期检查间隔设置**
   - 实时场景：10秒 - 30秒
   - 批处理：1分钟 - 5分钟

4. **主动 flush**
   - 批量导入完成后主动 flush()
   - 配合 `check_on_flush=true` 立即检查

5. **监控统计**

   ```rust
   engine.print_stats().await;  // 查看未持久化的 segments 数量
   ```

---

**就是这样！智能持久化，高效可靠！** 🎉
