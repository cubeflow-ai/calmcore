# Engine 实例使用文档

## 📋 概述

`Engine` 是存储引擎的顶层管理器，现在采用**实例模式**（非单例），每次调用 `Engine::new()` 都会创建一个新的独立实例。

## 🏗️ 架构特点

- **实例模式**：支持创建多个独立的 Engine 实例
- **自包含**：每个实例管理自己的 Partitions 和后台任务
- **Arc 包装**：`Engine::new()` 返回 `Arc<Engine>`，便于多处共享

## 🚀 快速开始

### 1. 创建 Engine 实例

```rust
use calm::engine::{Engine, EngineConfig};
use std::sync::Arc;

#[tokio::main]
async fn main() {
    // 使用默认配置创建
    let engine = Engine::new(EngineConfig::default());
    
    // 或自定义配置
    let engine = Engine::new(EngineConfig {
        data_dir: "/data".into(),
        persist_check_interval_secs: 60,
        max_concurrent_persists: 4,
        check_after_flush: true,
    });
    
    // engine 是 Arc<Engine> 类型，可以 clone 后在多处使用
    let engine_clone = engine.clone();
}
```

### 2. 创建和管理 Partition

```rust
use calm::schema::{Schema, Field, DataType, PersistPolicy};
use std::time::Duration;

// 创建 Schema
let schema = Schema {
    fields: vec![
        Field::new("id", DataType::U32, true),
        Field::new("name", DataType::Keyword, false),
        Field::new("age", DataType::I32, false),
    ],
    persist_policy: PersistPolicy {
        max_docs_per_segment: 100_000,
        max_segment_age: Duration::from_secs(300),
        check_on_flush: true,
    },
};

// 创建 Partition
let partition = engine.create_partition(1, schema).await;

// 写入数据
partition.write(vec![row1, row2, row3]).unwrap();
```

### 3. 多个 Engine 实例

```rust
// 可以创建多个独立的 Engine 实例
let engine1 = Engine::new(EngineConfig {
    data_dir: "/data/engine1".into(),
    ..Default::default()
});

let engine2 = Engine::new(EngineConfig {
    data_dir: "/data/engine2".into(),
    ..Default::default()
});

// 每个实例独立管理自己的 Partitions
let partition1 = engine1.create_partition(1, schema.clone()).await;
let partition2 = engine2.create_partition(1, schema.clone()).await;
```

## 📊 监控和管理

### 查看统计信息

```rust
// 获取统计信息
let stats = engine.stats().await;
println!("Partitions: {}", stats.partition_count);
println!("Total docs: {}", stats.total_doc_count);
println!("Frozen segments: {}", stats.total_frozen_segments);
println!("Unpersisted segments: {}", stats.total_unpersisted_segments);

// 或直接打印格式化的统计信息
engine.print_stats().await;
```

### 手动触发持久化

```rust
// 触发特定 Partition 的持久化检查
engine.trigger_persist(partition_id);

// 触发所有 Partition 的持久化检查
engine.trigger_persist_all();
```

### 列出所有 Partition

```rust
let partition_ids = engine.list_partitions().await;
for id in partition_ids {
    println!("Partition: {}", id);
}
```

## 🔧 生命周期管理

### 正常关闭

```rust
// 优雅关闭 Engine（等待后台任务完成）
engine.shutdown().await;
```

### 自动清理

```rust
// Engine 实现了 Drop trait
// 当 Arc<Engine> 的所有引用都被释放时，会自动发送关闭信号
{
    let engine = Engine::new(EngineConfig::default());
    // 使用 engine...
} // engine 超出作用域，自动清理
```

## 🎯 完整示例

```rust
use calm::engine::{Engine, EngineConfig};
use calm::schema::{Schema, Field, DataType, PersistPolicy};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. 创建 Engine
    let engine = Engine::new(EngineConfig {
        data_dir: "./data".into(),
        persist_check_interval_secs: 60,
        max_concurrent_persists: 4,
        check_after_flush: true,
    });

    // 2. 创建 Schema
    let schema = Schema {
        fields: vec![
            Field::new("id", DataType::U32, true),
            Field::new("name", DataType::Keyword, false),
            Field::new("score", DataType::I32, false),
        ],
        persist_policy: PersistPolicy {
            max_docs_per_segment: 100_000,
            max_segment_age: Duration::from_secs(300),
            check_on_flush: true,
        },
    };

    // 3. 创建 Partition
    let partition = engine.create_partition(1, schema).await;

    // 4. 写入数据
    for i in 0..200_000 {
        let row = vec![
            Value::U32(i),
            Value::String(format!("user_{}", i)),
            Value::I32((i % 100) as i32),
        ];
        partition.write(vec![row])?;
    }

    // 5. 手动触发持久化
    engine.trigger_persist(partition.id());

    // 6. 等待一段时间让持久化完成
    tokio::time::sleep(Duration::from_secs(5)).await;

    // 7. 查看统计
    engine.print_stats().await;

    // 8. 关闭
    engine.shutdown().await;

    Ok(())
}
```

## 🆚 对比：实例模式 vs 单例模式

### 实例模式（当前）

```rust
// 创建多个独立实例
let engine1 = Engine::new(config1);
let engine2 = Engine::new(config2);

// 每个实例有独立的配置和状态
engine1.create_partition(1, schema).await;
engine2.create_partition(1, schema).await; // 不冲突
```

**优点**：

- ✅ 灵活性高，支持多实例场景
- ✅ 测试友好，每个测试可以独立的 Engine
- ✅ 可以并存多个配置不同的 Engine
- ✅ 更符合依赖注入模式

**缺点**：

- ⚠️ 需要手动管理 Engine 实例的生命周期
- ⚠️ 多处使用需要传递 `Arc<Engine>`

### 单例模式（已移除）

```rust
// 全局初始化一次
Engine::initialize(config);

// 到处都能获取
let engine = Engine::global();
```

**优点**：

- ✅ 使用简单，到处都能访问
- ✅ 保证全局只有一个实例

**缺点**：

- ❌ 灵活性差，无法支持多实例
- ❌ 测试困难，多个测试共享状态
- ❌ 全局状态，容易产生隐式依赖

## 💡 最佳实践

### 1. 在应用入口创建

```rust
#[tokio::main]
async fn main() {
    let engine = Engine::new(load_config());
    
    // 传递给需要的模块
    run_server(engine.clone()).await;
    run_background_tasks(engine.clone()).await;
}
```

### 2. 使用依赖注入

```rust
struct ApiServer {
    engine: Arc<Engine>,
}

impl ApiServer {
    fn new(engine: Arc<Engine>) -> Self {
        Self { engine }
    }
    
    async fn handle_write(&self, data: Vec<Row>) {
        let partition = self.engine.get_partition(1).await.unwrap();
        partition.write(data).unwrap();
    }
}
```

### 3. 测试中独立创建

```rust
#[tokio::test]
async fn test_write_and_persist() {
    // 每个测试有自己的 Engine
    let engine = Engine::new(EngineConfig {
        data_dir: "/tmp/test1".into(),
        ..Default::default()
    });
    
    // 测试逻辑...
}

#[tokio::test]
async fn test_multiple_partitions() {
    // 另一个独立的 Engine
    let engine = Engine::new(EngineConfig {
        data_dir: "/tmp/test2".into(),
        ..Default::default()
    });
    
    // 测试逻辑...
}
```

## 🔄 迁移指南（从单例到实例）

### 旧代码（单例模式）

```rust
// 初始化
Engine::initialize(config);

// 使用
let engine = Engine::global();
let partition = engine.create_partition(1, schema).await;
```

### 新代码（实例模式）

```rust
// 创建实例
let engine = Engine::new(config);

// 使用（需要持有或传递 engine）
let partition = engine.create_partition(1, schema).await;

// 如果需要在多处使用，clone Arc
let engine_clone = engine.clone();
```

## 📝 注意事项

1. **生命周期**：确保 Engine 在所有 Partition 使用期间保持存活
2. **Arc 共享**：`Engine::new()` 返回 `Arc<Engine>`，可以安全地 clone 和共享
3. **后台任务**：Engine 创建时会自动启动后台持久化任务
4. **关闭顺序**：先关闭 Engine，再让 Partition 超出作用域
5. **多实例**：多个 Engine 实例应使用不同的 `data_dir`
