# Engine 使用文档

## 📋 概述

`Engine` 是存储引擎的顶层管理器，负责：

- 管理所有 Partition 的生命周期
- 自动持久化后台任务
- 统一配置和监控
- 统计信息上报

## 🏗️ 架构

```
Engine (顶层管理器)
  ├── Partition 管理（创建、加载、删除）
  ├── 自动持久化任务（后台线程）
  ├── 配置管理
  └── 统计信息

Partition-1 ──┐
Partition-2 ──┼──→ Engine
Partition-3 ──┘
```

## 🚀 快速开始

### 1. 添加依赖

```toml
[dependencies]
tokio = { version = "1", features = ["rt-multi-thread", "sync", "time", "macros"] }
```

### 2. 创建 Engine

```rust
use calm::engine::{Engine, EngineConfig};

#[tokio::main]
async fn main() {
    // 使用默认配置
    let engine = Engine::new(EngineConfig::default());
    
    // 或自定义配置
    let config = EngineConfig {
        data_dir: "/data".into(),
        persist_check_interval_secs: 60,
        min_unpersisted_segments: 1,
        max_concurrent_persists: 4,
        check_after_flush: true,
    };
    let engine = Engine::new(config);
}
```

### 3. 创建 Partition

```rust
// 创建新 Partition
let schema = create_test_schema();
let partition1 = engine.create_partition(1, schema.clone()).await;

// 加载已存在的 Partition
let partition2 = engine.load_partition(2, schema.clone()).await?;
```

### 4. 使用 Partition

```rust
// 正常使用 Partition
partition1.upsert(batch1)?;
partition1.flush()?;

// Engine 会在后台自动检查并持久化
```

### 5. 手动触发持久化

```rust
// 触发特定 Partition
engine.trigger_persist(1);

// 触发所有 Partition
engine.trigger_persist_all();
```

### 6. 查看统计信息

```rust
// 获取统计
let stats = engine.stats().await;
println!("Total documents: {}", stats.total_doc_count);

// 打印格式化统计
engine.print_stats().await;
```

### 7. 关闭 Engine

```rust
// 优雅关闭（等待所有持久化任务完成）
engine.shutdown().await;
```

## 📝 完整示例

```rust
use std::sync::Arc;
use calm::engine::{Engine, EngineConfig};
use calm::schema::Schema;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. 创建 Engine
    let config = EngineConfig {
        data_dir: "/tmp/data".into(),
        persist_check_interval_secs: 30,  // 30秒检查一次
        min_unpersisted_segments: 2,      // 有2个未持久化就触发
        max_concurrent_persists: 4,       // 最多4个并行
        check_after_flush: true,
    };
    let engine = Engine::new(config);
    
    // 2. 创建 Partitions
    let schema = create_test_schema();
    let partition1 = engine.create_partition(1, schema.clone()).await;
    let partition2 = engine.create_partition(2, schema.clone()).await;
    
    // 3. 写入数据
    for i in 0..10 {
        let batch = generate_batch(i * 10000, 10000);
        partition1.upsert(batch)?;
        
        if (i + 1) % 3 == 0 {
            partition1.flush()?;  // Engine 自动检查持久化
        }
    }
    
    // 4. 查看统计
    engine.print_stats().await;
    
    // 5. 手动触发持久化（如果需要）
    engine.trigger_persist_all();
    
    // 等待一下让持久化完成
    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
    
    // 6. 再次查看统计
    engine.print_stats().await;
    
    // 7. 关闭
    engine.shutdown().await;
    
    Ok(())
}
```

## ⚙️ 配置说明

### EngineConfig

```rust
pub struct EngineConfig {
    /// 数据根目录
    pub data_dir: PathBuf,
    
    /// 持久化检查间隔（秒）
    /// 建议：30-120秒
    pub persist_check_interval_secs: u64,
    
    /// 最小未持久化 segment 数量
    /// 达到这个数量才触发持久化
    /// 建议：1-5
    pub min_unpersisted_segments: usize,
    
    /// 最大并发持久化的 Partition 数量
    /// 建议：2-8，根据CPU和磁盘IO调整
    pub max_concurrent_persists: usize,
    
    /// 是否在 flush 后立即检查持久化
    /// 建议：true
    pub check_after_flush: bool,
}
```

### 默认配置

```rust
EngineConfig::default()  // {
    data_dir: "./data",
    persist_check_interval_secs: 60,
    min_unpersisted_segments: 1,
    max_concurrent_persists: 4,
    check_after_flush: true,
}
```

### 场景配置

#### 高吞吐写入

```rust
EngineConfig {
    persist_check_interval_secs: 30,
    min_unpersisted_segments: 3,
    max_concurrent_persists: 8,
    ..Default::default()
}
```

#### 低延迟

```rust
EngineConfig {
    persist_check_interval_secs: 10,
    min_unpersisted_segments: 1,
    max_concurrent_persists: 2,
    ..Default::default()
}
```

#### 批处理

```rust
EngineConfig {
    persist_check_interval_secs: 300,
    min_unpersisted_segments: 10,
    max_concurrent_persists: 16,
    ..Default::default()
}
```

## 📊 API 文档

### Engine 创建

```rust
// 创建 Engine
Engine::new(config: EngineConfig) -> Self
```

### Partition 管理

```rust
// 创建新 Partition
async fn create_partition(&self, id: u32, schema: Schema) -> Arc<Partition>

// 加载已存在的 Partition
async fn load_partition(&self, id: u32, schema: Schema) -> CoreResult<Arc<Partition>>

// 添加已存在的 Partition
async fn add_partition(&self, partition: Arc<Partition>)

// 移除 Partition
async fn remove_partition(&self, partition_id: u64)

// 获取 Partition
async fn get_partition(&self, partition_id: u64) -> Option<Arc<Partition>>

// 列出所有 Partition
async fn list_partitions(&self) -> Vec<u64>
```

### 持久化控制

```rust
// 触发特定 Partition 的持久化
fn trigger_persist(&self, partition_id: u64)

// 触发所有 Partition 的持久化
fn trigger_persist_all(&self)
```

### 统计信息

```rust
// 获取统计信息
async fn stats(&self) -> EngineStats

// 打印格式化统计
async fn print_stats(&self)
```

### 关闭

```rust
// 关闭 Engine（等待持久化完成）
async fn shutdown(self)
```

## 📈 统计信息

### EngineStats

```rust
pub struct EngineStats {
    pub partition_count: usize,
    pub total_doc_count: u64,
    pub total_frozen_segments: usize,
    pub total_unpersisted_segments: usize,
    pub total_memory_bytes: u64,
}
```

### 输出示例

```
╔══════════════════════════════════════════════════════════════╗
║                     Engine Statistics                        ║
╠══════════════════════════════════════════════════════════════╣
║ Partitions          :                                      3 ║
║ Total Documents     :                               1000000 ║
║ Frozen Segments     :                                     10 ║
║ Unpersisted Segments:                                      2 ║
╚══════════════════════════════════════════════════════════════╝
```

## 🔄 工作流程

```
1. Engine 启动后台任务
        ↓
2. 定时检查（每 N 秒）或接收触发请求
        ↓
3. 遍历所有 Partition
        ↓
4. 对每个 Partition：
   - 检查是否已有任务在执行
   - 检查未持久化 segment 数量
   - 检查并发限制
        ↓
5. 启动 tokio::task::spawn_blocking
        ↓
6. 调用 partition.persist_unpersisted_segments()
        ↓
7. 顺序持久化该 Partition 的所有未持久化 segments
        ↓
8. 完成后自动清理任务记录
```

## 🎯 核心特性

### 1. 单个 Partition 顺序持久化

每个 Partition 内部顺序持久化（调用 `persist_unpersisted_segments()`），保证数据准确性。

```rust
// 在 Partition 内部
for (seg_id, _) in unpersisted {
    self.persist_segment(seg_id)?;  // 一个接一个
}
```

### 2. 多个 Partition 并行持久化

不同 Partition 之间并行执行，充分利用多核CPU。

```rust
// 每个 Partition 在独立的 tokio task 中
for partition_id in partition_ids {
    tokio::task::spawn_blocking(move || {
        partition.persist_unpersisted_segments();
    });
}
```

### 3. 自动检查和触发

- 定时检查（configurable）
- flush 后立即检查（可选）
- 手动触发

### 4. 并发控制

- 限制同时持久化的 Partition 数量
- 防止资源耗尽
- 避免磁盘IO竞争

## 🧪 测试示例

```rust
#[tokio::test]
async fn test_engine() {
    let config = EngineConfig {
        data_dir: "/tmp/test".into(),
        persist_check_interval_secs: 5,
        min_unpersisted_segments: 1,
        max_concurrent_persists: 2,
        check_after_flush: true,
    };
    
    let engine = Engine::new(config);
    
    // 创建 Partition
    let schema = create_test_schema();
    let partition = engine.create_partition(1, schema).await;
    
    // 写入数据
    for i in 0..10 {
        let batch = generate_batch(i * 10000, 10000);
        partition.upsert(batch).unwrap();
    }
    
    // Flush
    partition.flush().unwrap();
    
    // 等待持久化
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
    
    // 验证
    let stats = engine.stats().await;
    assert_eq!(stats.total_unpersisted_segments, 0);
    
    engine.shutdown().await;
}
```

## 💡 最佳实践

1. **应用启动时创建 Engine**

   ```rust
   let engine = Arc::new(Engine::new(config));
   ```

2. **通过 Engine 创建所有 Partition**

   ```rust
   let partition = engine.create_partition(id, schema).await;
   ```

3. **合理配置检查间隔**
   - 高写入：30秒
   - 中等：60秒
   - 低写入：120秒

4. **设置合理的并发限制**
   - SSD：4-8
   - HDD：2-4

5. **应用关闭时调用 shutdown()**

   ```rust
   engine.shutdown().await;
   ```

## 🔍 与 PersistService 的对比

| 特性 | PersistService | Engine |
|------|---------------|--------|
| 定位 | 独立服务 | 顶层管理器 |
| Partition 管理 | ❌ | ✅ |
| 创建 Partition | ❌ | ✅ |
| 加载 Partition | ❌ | ✅ |
| 统计信息 | ❌ | ✅ |
| 配置管理 | ✅ | ✅ |
| 自动持久化 | ✅ | ✅ |
| 使用复杂度 | 高 | 低 |

**结论**：`Engine` 更符合顶层管理器的定位，提供完整的生命周期管理。

---

**这就是 Engine 的完整使用方式！简单、统一、强大！** 🎉
