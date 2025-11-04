# Engine Demo Examples

本目录包含了 Engine 使用的示例代码。

## 示例列表

### 1. simple_engine_demo.rs

简单的 Engine 使用示例，演示了完整的工作流程：

1. 创建 Engine 实例
2. 定义 Schema
3. 创建 Partition
4. 写入数据（使用 `upsert` 方法）
5. 查看统计信息
6. 手动 Flush
7. 触发持久化
8. 从磁盘加载数据
9. 关闭 Engine

**运行方式：**

```bash
cargo run --example simple_engine_demo
```

### 2. engine_demo.rs

更完整的 Engine 示例，包含：

- 大批量数据写入（500 条记录）
- 数据查询和读取
- 按 ID 过滤查询
- 完整的加载流程演示

**运行方式：**

```bash
cargo run --example engine_demo
```

## 关键 API

### Engine 创建

```rust
use calm::engine::{Engine, EngineConfig};

let engine = Engine::new(EngineConfig {
    data_dir: "./data".into(),
    persist_check_interval_secs: 5,
    max_concurrent_persists: 2,
    check_after_flush: true,
});
```

### Schema 定义

```rust
use calm::schema::{Schema, field::FieldOption, PersistPolicy};
use std::time::Duration;

let schema = Schema {
    name: "my_schema".to_string(),
    primary_key: Some("id".to_string()),
    store_source: false,
    fields: vec![
        FieldOption::U32 {
            name: "id".to_string(),
            index: true,
        },
        FieldOption::Keyword {
            name: "name".to_string(),
            index: true,
            is_array: false,
            persist_option: None,
            case_sensitive: true,
        },
    ],
    persist_policy: PersistPolicy {
        max_docs_per_segment: 100,
        max_segment_age: Duration::from_secs(30),
        check_on_flush: true,
    },
};
```

### 数据写入

```rust
use arrow::array::{RecordBatch, UInt32Array, StringArray};
use arrow::datatypes::{Schema as ArrowSchema, Field, DataType};
use std::sync::Arc;

// 构造 Arrow RecordBatch
let arrow_schema = ArrowSchema::new(vec![
    Field::new("id", DataType::UInt32, false),
    Field::new("name", DataType::Utf8, false),
]);

let batch = RecordBatch::try_new(
    Arc::new(arrow_schema),
    vec![
        Arc::new(UInt32Array::from(vec![1, 2, 3])),
        Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
    ],
)?;

// 写入数据
let row_ids = partition.upsert(batch)?;
```

### Partition 管理

```rust
// 创建 Partition
let partition = engine.create_partition(1, schema.clone()).await;

// 加载已有的 Partition
let loaded = engine.load_partition(1, schema).await?;

// 获取统计信息
println!("Total docs: {}", partition.total_count());
println!("Frozen segments: {}", partition.frozen_count());
```

### 持久化控制

```rust
// 手动 flush（移动到 frozen segments）
partition.flush(false)?;

// 触发持久化检查
engine.trigger_persist(partition_id);

// 触发所有 Partition 的持久化
engine.trigger_persist_all();
```

## 数据目录

运行示例后会在当前目录下创建数据目录：

- `simple_demo_data/` - simple_engine_demo 的数据
- `demo_data/` - engine_demo 的数据

可以手动删除这些目录来清理测试数据：

```bash
rm -rf simple_demo_data demo_data
```

## 注意事项

1. **持久化是异步的**：调用 `trigger_persist()` 后需要等待一段时间让持久化完成
2. **加载需要持久化数据**：只有已持久化的数据才能通过 `load_partition()` 加载
3. **Schema 必须匹配**：加载 Partition 时提供的 Schema 必须与创建时一致
4. **Engine 生命周期**：确保 Engine 在所有 Partition 使用期间保持存活

## 更多文档

详见项目根目录下的文档：

- `ENGINE_INSTANCE_USAGE.md` - Engine 实例使用详解
- `PERSIST_STRATEGY.md` - 持久化策略说明
- `PERSIST_NOTIFICATION.md` - 通知机制说明
