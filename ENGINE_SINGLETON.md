# Engine 单例模式使用文档

## 📋 概述

`Engine` 现在支持全局单例模式，确保整个应用只有一个 Engine 实例。

## 🎯 为什么使用单例？

1. **全局唯一**：整个应用只需要一个 Engine
2. **统一管理**：所有 Partition 集中管理
3. **简化使用**：无需到处传递 Engine 引用
4. **资源高效**：避免创建多个后台任务

## 🚀 快速开始

### 1. 初始化全局 Engine

```rust
use calm::engine::{Engine, EngineConfig};

#[tokio::main]
async fn main() {
    // 在应用启动时初始化一次
    let engine = Engine::initialize(EngineConfig::default());
    
    // 或者使用自定义配置
    let config = EngineConfig {
        data_dir: "/data".into(),
        persist_check_interval_secs: 30,
        min_unpersisted_segments: 2,
        max_concurrent_persists: 8,
        check_after_flush: true,
    };
    let engine = Engine::initialize(config);
    
    println!("Engine initialized!");
}
```

### 2. 在任何地方获取 Engine

```rust
use calm::engine::Engine;

async fn create_partition() {
    // 直接获取全局 Engine
    let engine = Engine::global();
    
    let partition = engine.create_partition(1, schema).await;
    // ...
}

async fn write_data() {
    let engine = Engine::global();
    
    let partition = engine.get_partition(1).await.unwrap();
    partition.upsert(batch)?;
}

async fn check_stats() {
    let engine = Engine::global();
    
    engine.print_stats().await;
}
```

### 3. 安全获取（不 panic）

```rust
use calm::engine::Engine;

async fn optional_operation() {
    // 如果 Engine 未初始化，返回 None
    if let Some(engine) = Engine::try_global() {
        engine.print_stats().await;
    } else {
        println!("Engine not initialized yet");
    }
}
```

## 📝 完整示例

```rust
use calm::engine::{Engine, EngineConfig};
use calm::schema::Schema;

// 应用入口
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. 初始化全局 Engine（只需一次）
    Engine::initialize(EngineConfig {
        data_dir: "/tmp/data".into(),
        persist_check_interval_secs: 30,
        min_unpersisted_segments: 2,
        max_concurrent_persists: 4,
        check_after_flush: true,
    });
    
    // 2. 创建 Partitions
    create_partitions().await?;
    
    // 3. 写入数据
    write_data().await?;
    
    // 4. 查看统计
    check_stats().await;
    
    // 5. 关闭（可选）
    // Engine 会在 Drop 时自动关闭
    
    Ok(())
}

// 创建 Partitions（可在任何模块）
async fn create_partitions() -> Result<(), Box<dyn std::error::Error>> {
    let engine = Engine::global();
    let schema = create_test_schema();
    
    engine.create_partition(1, schema.clone()).await;
    engine.create_partition(2, schema.clone()).await;
    engine.create_partition(3, schema.clone()).await;
    
    println!("Created 3 partitions");
    Ok(())
}

// 写入数据（可在任何模块）
async fn write_data() -> Result<(), Box<dyn std::error::Error>> {
    let engine = Engine::global();
    
    for partition_id in 1..=3 {
        if let Some(partition) = engine.get_partition(partition_id).await {
            for i in 0..10 {
                let batch = generate_batch(i * 10000, 10000);
                partition.upsert(batch)?;
                
                if (i + 1) % 3 == 0 {
                    partition.flush()?;
                }
            }
        }
    }
    
    println!("Data written");
    Ok(())
}

// 查看统计（可在任何模块）
async fn check_stats() {
    let engine = Engine::global();
    engine.print_stats().await;
}
```

## 🔄 与非单例模式的对比

### 非单例模式（旧方式）

```rust
// 需要到处传递 engine
async fn main() {
    let engine = Engine::new(config);
    create_partitions(&engine).await;
    write_data(&engine).await;
    check_stats(&engine).await;
}

async fn create_partitions(engine: &Engine) { /* ... */ }
async fn write_data(engine: &Engine) { /* ... */ }
async fn check_stats(engine: &Engine) { /* ... */ }
```

### 单例模式（新方式）

```rust
// 无需传递，直接获取
async fn main() {
    Engine::initialize(config);
    create_partitions().await;
    write_data().await;
    check_stats().await;
}

async fn create_partitions() {
    let engine = Engine::global();
    // ...
}

async fn write_data() {
    let engine = Engine::global();
    // ...
}

async fn check_stats() {
    let engine = Engine::global();
    // ...
}
```

## 📚 API 文档

### 初始化

```rust
/// 初始化全局 Engine 单例
/// 如果已经初始化，返回已存在的实例
pub fn initialize(config: EngineConfig) -> Arc<Engine>
```

### 获取

```rust
/// 获取全局 Engine 单例
/// Panics: 如果未初始化会 panic
pub fn global() -> Arc<Engine>

/// 尝试获取全局 Engine 单例
/// 返回 None 如果未初始化
pub fn try_global() -> Option<Arc<Engine>>
```

### 非单例创建（高级用法）

```rust
/// 创建新的 Engine 实例（不是全局单例）
/// 通常不需要使用，除非需要多个独立的 Engine
pub fn new(config: EngineConfig) -> Self
```

## ⚙️ 最佳实践

### 1. 应用启动时初始化

```rust
#[tokio::main]
async fn main() {
    // 第一行就初始化
    Engine::initialize(load_config());
    
    // 启动应用...
}
```

### 2. 使用全局访问

```rust
// ✅ 推荐：直接获取
let engine = Engine::global();

// ❌ 不推荐：传递引用
fn some_function(engine: &Engine) { }
```

### 3. 安全检查（可选）

```rust
// 在不确定是否初始化的地方
if let Some(engine) = Engine::try_global() {
    // 使用 engine
} else {
    // 处理未初始化情况
}
```

### 4. 配置管理

```rust
// 从配置文件加载
fn load_config() -> EngineConfig {
    EngineConfig {
        data_dir: env::var("DATA_DIR").unwrap_or("./data".into()).into(),
        persist_check_interval_secs: 60,
        // ...
    }
}

#[tokio::main]
async fn main() {
    Engine::initialize(load_config());
}
```

## 🔍 常见问题

### Q: 可以多次调用 initialize() 吗？

A: 可以，但只有第一次调用会真正初始化。后续调用返回已存在的实例。

```rust
let engine1 = Engine::initialize(config1);
let engine2 = Engine::initialize(config2);  // config2 被忽略
// engine1 和 engine2 指向同一个实例
```

### Q: 如何关闭全局 Engine？

A: Engine 会在程序退出时自动关闭（Drop）。如果需要手动关闭：

```rust
// 注意：这会消耗掉 Arc，需要确保没有其他引用
if let Some(engine) = Engine::try_global() {
    // 如果 engine 是唯一的引用
    if Arc::strong_count(&engine) == 1 {
        Arc::try_unwrap(engine).ok().unwrap().shutdown().await;
    }
}
```

### Q: 单例模式线程安全吗？

A: 是的，使用 `once_cell::OnceCell` 保证线程安全的初始化，`Arc` 保证共享访问安全。

### Q: 可以创建多个独立的 Engine 吗？

A: 可以，使用 `Engine::new()` 创建非全局实例：

```rust
let engine1 = Engine::new(config1);  // 非全局
let engine2 = Engine::new(config2);  // 另一个非全局

// 全局单例仍然独立存在
let global = Engine::global();
```

## 🎯 使用场景

### 场景 1: Web 应用

```rust
// main.rs
#[tokio::main]
async fn main() {
    Engine::initialize(EngineConfig::default());
    
    let app = axum::Router::new()
        .route("/create", post(create_partition_handler))
        .route("/write", post(write_data_handler))
        .route("/stats", get(stats_handler));
    
    // ...
}

// handlers.rs
async fn create_partition_handler() -> Result<Json<Response>> {
    let engine = Engine::global();
    let partition = engine.create_partition(1, schema).await;
    Ok(Json(Response { id: partition.id() }))
}

async fn stats_handler() -> Result<Json<EngineStats>> {
    let engine = Engine::global();
    Ok(Json(engine.stats().await))
}
```

### 场景 2: CLI 工具

```rust
// main.rs
#[tokio::main]
async fn main() {
    Engine::initialize(EngineConfig::default());
    
    match args.command {
        Command::Create => create_command().await,
        Command::Write => write_command().await,
        Command::Stats => stats_command().await,
    }
}

// commands.rs
async fn create_command() {
    let engine = Engine::global();
    // ...
}

async fn stats_command() {
    let engine = Engine::global();
    engine.print_stats().await;
}
```

### 场景 3: 后台服务

```rust
#[tokio::main]
async fn main() {
    Engine::initialize(EngineConfig::default());
    
    // 启动多个后台任务
    tokio::spawn(data_ingestion_task());
    tokio::spawn(monitoring_task());
    tokio::spawn(cleanup_task());
    
    // ...
}

async fn data_ingestion_task() {
    loop {
        let engine = Engine::global();
        // 接收数据并写入
    }
}

async fn monitoring_task() {
    loop {
        let engine = Engine::global();
        engine.print_stats().await;
        tokio::time::sleep(Duration::from_secs(60)).await;
    }
}
```

## 📊 性能影响

单例模式的性能影响：

- **初始化开销**: 一次性，可忽略
- **访问开销**: `Arc::clone()` 非常轻量（原子操作）
- **内存开销**: 无额外开销

## 🔒 线程安全保证

- `once_cell::OnceCell`: 保证初始化只执行一次
- `Arc`: 提供线程安全的共享所有权
- `tokio::sync::RwLock`: Engine 内部的并发控制

---

**就是这样！全局单例，简单强大！** 🎉
