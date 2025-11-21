# Custom 分区加载外部文件功能使用指南

## 概述

Custom 分区策略允许用户通过 `load_segment` 接口加载外部文件到数据库中。这个功能特别适合批量导入数据的场景。

## 功能特性

1. **自动分区管理**：如果指定的 partition 不存在，系统会自动创建
2. **灵活的文件处理**：支持三种文件处理方式
   - `Reference`：引用文件（创建符号链接）
   - `Move`：移动文件到引擎管理目录
   - `Copy`：复制文件到引擎管理目录
3. **JSONL 格式支持**：每行一个 JSON 对象
4. **自动索引构建**：数据加载后自动建立索引
5. **自动持久化**：加载完成后自动触发持久化

## 使用步骤

### 1. 创建使用 Custom 分区策略的表

首先需要创建一个使用 Custom 分区策略的表：

```rust
use calm::catalog::PartitionStrategy;
use calm::schema::{Schema, field::FieldOption, PersistPolicy};

let schema = Schema {
    name: "custom_table".to_string(),
    primary_key: Some("id".to_string()),
    store_source: true,
    fields: vec![
        FieldOption::Keyword {
            name: "id".to_string(),
            index: true,
            is_array: false,
            persist_option: None,
            case_sensitive: true,
        },
        FieldOption::I64 {
            name: "timestamp".to_string(),
            index: true,
        },
        FieldOption::Keyword {
            name: "message".to_string(),
            index: true,
            is_array: false,
            persist_option: None,
            case_sensitive: false,
        },
    ],
    persist_policy: PersistPolicy::default(),
};

engine.create_table(
    "custom_table",
    schema,
    PartitionStrategy::Custom,
    1, // Custom 分区不需要预定义数量
).await?;
```

### 2. 准备数据文件

创建一个 JSONL 格式的数据文件（每行一个 JSON 对象）：

**data.jsonl**
```jsonl
{"id": "log001", "timestamp": 1700000000, "message": "System started"}
{"id": "log002", "timestamp": 1700000001, "message": "User logged in"}
{"id": "log003", "timestamp": 1700000002, "message": "Database connected"}
{"id": "log004", "timestamp": 1700000003, "message": "API request received"}
```

### 3. 使用 Rust API 加载数据

```rust
use calm::segment_loader::FileHandlerType;
use std::path::PathBuf;

let file_path = PathBuf::from("/path/to/data.jsonl");

// 方式1: 指定 partition_id
let doc_count = engine.load_segment(
    "custom_table",
    Some("partition_logs".to_string()),  // 指定 partition_id
    None,                                 // 不使用 partition_value
    file_path.clone(),
    FileHandlerType::Copy,               // 拷贝文件
).await?;

println!("Loaded {} documents", doc_count);

// 方式2: 使用 partition_value 自动生成 partition_id
let doc_count = engine.load_segment(
    "custom_table",
    None,                                 // 不指定 partition_id
    Some("2024-01".to_string()),         // 使用值生成 partition_id
    file_path,
    FileHandlerType::Move,               // 移动文件
).await?;
```

### 4. 使用 GraphQL API 加载数据

#### GraphQL Mutation

```graphql
mutation LoadSegment {
  loadSegment(input: {
    table: "custom_table"
    partitionValue: "2024-01"
    filePath: "/path/to/data.jsonl"
    handlerType: COPY
  }) {
    success
    documentsLoaded
    partitionId
    message
  }
}
```

#### 使用 curl

```bash
curl -X POST http://localhost:8080/graphql \
  -H "Content-Type: application/json" \
  -d '{
    "query": "mutation LoadSegment($input: LoadSegmentInput!) { loadSegment(input: $input) { success documentsLoaded partitionId message } }",
    "variables": {
      "input": {
        "table": "custom_table",
        "partitionValue": "2024-01",
        "filePath": "/path/to/data.jsonl",
        "handlerType": "COPY"
      }
    }
  }'
```

## 文件处理类型说明

### Reference（引用）
- **用途**：当源文件很大，不想复制时使用
- **特点**：创建符号链接，不占用额外空间
- **注意**：源文件不能移动或删除

### Move（移动）
- **用途**：将文件完全交给引擎管理
- **特点**：源文件会被移动到引擎目录
- **注意**：源文件位置的文件会消失

### Copy（拷贝）
- **用途**：保留源文件的同时导入数据
- **特点**：创建文件副本
- **注意**：需要额外的磁盘空间

## 分区命名规则

对于 Custom 分区：

1. **指定 partition_id**：直接使用提供的名称
2. **使用 partition_value**：使用 `sanitize_filename` 清理后的值
   - 特殊字符（`/`, `\`, `:`, `*`, `?`, `"`, `<`, `>`, `|`）会被替换为 `_`
   - 例如：`2024/01` → `2024_01`

## 完整示例

```rust
use calm::engine::{Engine, EngineConfig};
use calm::catalog::PartitionStrategy;
use calm::schema::{Schema, field::FieldOption, PersistPolicy};
use calm::segment_loader::FileHandlerType;
use std::sync::Arc;
use std::path::PathBuf;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. 创建 Engine
    let config = EngineConfig::default();
    let engine = Engine::new(config)?;
    
    // 2. 创建使用 Custom 分区的表
    let schema = Schema {
        name: "logs".to_string(),
        primary_key: Some("id".to_string()),
        store_source: true,
        fields: vec![
            FieldOption::Keyword {
                name: "id".to_string(),
                index: true,
                is_array: false,
                persist_option: None,
                case_sensitive: true,
            },
            FieldOption::I64 {
                name: "timestamp".to_string(),
                index: true,
            },
            FieldOption::Keyword {
                name: "level".to_string(),
                index: true,
                is_array: false,
                persist_option: None,
                case_sensitive: false,
            },
            FieldOption::Keyword {
                name: "message".to_string(),
                index: true,
                is_array: false,
                persist_option: None,
                case_sensitive: false,
            },
        ],
        persist_policy: PersistPolicy::default(),
    };
    
    engine.create_table(
        "logs",
        schema,
        PartitionStrategy::Custom,
        1,
    ).await?;
    
    // 3. 加载数据文件
    let file_path = PathBuf::from("logs/2024-01.jsonl");
    
    let doc_count = engine.load_segment(
        "logs",
        None,
        Some("2024-01".to_string()),
        file_path,
        FileHandlerType::Copy,
    ).await?;
    
    println!("Successfully loaded {} documents", doc_count);
    
    // 4. 查询数据
    let result = engine.execute_sql("SELECT * FROM logs WHERE level = 'ERROR'").await?;
    println!("Found {} error logs", result.batch.num_rows());
    
    // 5. 优雅关闭
    engine.stop().await?;
    
    Ok(())
}
```

## 注意事项

1. **文件格式**：目前只支持 JSONL 格式（每行一个 JSON 对象）
2. **字段匹配**：JSON 中的字段需要与表的 schema 匹配
3. **主键必需**：如果表定义了主键，JSON 数据中必须包含主键字段
4. **并发加载**：可以并发加载多个文件到不同的 partition
5. **错误处理**：加载失败时，临时文件会被清理
6. **持久化**：加载完成后会自动触发持久化，确保数据安全

## 性能建议

1. **文件大小**：建议单个文件不超过 100MB
2. **批量加载**：对于大量数据，分成多个文件并行加载
3. **分区策略**：按照时间或其他维度合理划分 partition
4. **索引优化**：加载前确保只对必要的字段建立索引

## 故障排查

### 常见错误

1. **"File does not exist"**
   - 检查文件路径是否正确
   - 确保文件有读取权限

2. **"Table not found"**
   - 确保表已创建
   - 检查表名拼写

3. **"Custom partition strategy required"**
   - 只有 Custom 分区策略的表才能使用 load_segment
   - 重新创建表时指定 `PartitionStrategy::Custom`

4. **"Failed to parse JSON"**
   - 检查 JSONL 文件格式
   - 确保每行都是有效的 JSON 对象
   - 字段类型需要与 schema 匹配
