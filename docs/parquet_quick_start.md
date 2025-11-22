# Parquet 持久化快速使用指南

## 基础用法

### 1. 持久化索引

```rust
use calm::schema::field::FieldOption;
use calm::segment::field_store::text::FullTextField;
use calm::segment::field_store::IndexWriter;

// 创建索引
let field = FieldOption::Keyword {
    name: "content".to_string(),
    index: true,
    is_array: false,
    persist_option: None,
    case_sensitive: true,
};
let index = FullTextField::new(&field);

// 索引文档
index.write(&batch, 0)?;

// 持久化到 Parquet
index.persist("/path/to/index")?;
```

**生成的文件**:
- `/path/to/index/posting_lists.parquet` - 主索引
- `/path/to/index/field_stats.json` - 字段统计

---

### 2. 加载索引

```rust
// 从磁盘加载（完整加载到内存）
let index = FullTextField::from_disk(&field, "/path/to/index")?;
let docs = index.term_query("rust")?;
```

**适用**: 小索引，高频查询

---

## 优化查询方法

### 方法 1: Row Group 统计（推荐默认）

```rust
use calm::segment::field_store::text::search_term;

let result = search_term("/path/to/index/posting_lists.parquet", "rust")?;
if let Some(row) = result {
    println!("Found {} docs: {:?}", row.doc_ids.len(), row.doc_ids);
}
```

**优势**:
- ✅ 零额外存储
- ✅ 只读取相关 Row Group
- ✅ 早期终止（词项不存在时）

**性能**: ~815µs（中等数据集）

---

### 方法 2: 列式读取（只要 doc_ids）

```rust
use calm::segment::field_store::text::search_term_docids_only;

let doc_ids = search_term_docids_only(
    "/path/to/index/posting_lists.parquet", 
    "programming"
)?;

if let Some(ids) = doc_ids {
    println!("Found docs: {:?}", ids);
}
```

**优势**:
- ✅ 跳过 positions 列（减少 50% I/O）
- ✅ 更快的查询速度
- ✅ 适合简单词项查询

**性能**: ~460µs（5x 提升）

---

### 方法 3: 批量查询

```rust
use calm::segment::field_store::text::search_terms_batch;

let terms = vec!["rust", "python", "javascript"];
let results = search_terms_batch(
    "/path/to/index/posting_lists.parquet",
    &terms
)?;

for (term, result) in terms.iter().zip(results.iter()) {
    match result {
        Some(row) => println!("{}: {} docs", term, row.doc_ids.len()),
        None => println!("{}: not found", term),
    }
}
```

**优势**:
- ✅ 一次文件打开，多次查询
- ✅ 摊销 I/O 成本
- ✅ 适合 Boolean 查询

**性能**: ~217µs/词（批量 4 词）

---

### 方法 4: Term Index（大索引 + 高频查询）

```rust
use calm::segment::field_store::text::{
    build_term_index, save_term_index, load_term_index, search_with_term_index
};

// 一次性构建索引（部署时）
let term_index = build_term_index("/path/to/index/posting_lists.parquet")?;
save_term_index(&term_index, "/path/to/index/term_index.json")?;

// 启动时加载索引
let term_index = load_term_index("/path/to/index/term_index.json")?;

// 使用索引查询
let result = search_with_term_index(
    "/path/to/index/posting_lists.parquet",
    "rust",
    &term_index
)?;
```

**优势**:
- ✅ O(1) 查找复杂度
- ✅ 索引可常驻内存
- ✅ 最快的单词查询

**劣势**:
- ❌ 额外 ~100% 存储开销

**性能**: ~685µs + ~85µs 加载

---

## 决策树

```
索引大小？
├─ < 1MB ────────────> 完全加载（FullTextField::from_disk）
├─ 1-100MB ──────────> Row Group 统计（search_term）
│   └─ 只需doc_ids? ─> 列式读取（search_term_docids_only）
│   └─ 多词查询? ────> 批量查询（search_terms_batch）
└─ > 100MB + 高频 ──> Term Index（search_with_term_index）
```

---

## 常见场景

### 场景 1: REST API 单词查询

```rust
// 使用 Row Group 统计（推荐）
#[get("/search/{term}")]
async fn search(term: String) -> Result<Json<Vec<u32>>> {
    let result = search_term("posting_lists.parquet", &term)?;
    Ok(Json(result.map(|r| r.doc_ids).unwrap_or_default()))
}
```

---

### 场景 2: Boolean 查询 (AND/OR)

```rust
// 批量查询多个词项
let terms = vec!["rust", "programming"];
let results = search_terms_batch("posting_lists.parquet", &terms)?;

// 取交集 (AND)
let rust_docs: HashSet<_> = results[0].as_ref().unwrap().doc_ids.iter().collect();
let prog_docs: HashSet<_> = results[1].as_ref().unwrap().doc_ids.iter().collect();
let intersection: Vec<_> = rust_docs.intersection(&prog_docs).collect();
```

---

### 场景 3: 自动补全（前缀查询）

```rust
// 读取所有 terms 并过滤
let all_rows = read_posting_lists("posting_lists.parquet")?;
let suggestions: Vec<_> = all_rows
    .iter()
    .filter(|row| row.term.starts_with("rus"))
    .take(10)
    .map(|row| row.term.clone())
    .collect();
```

---

### 场景 4: 统计分析

```rust
// 获取元数据
let metadata = get_parquet_metadata("posting_lists.parquet")?;
println!("Total terms: {}", metadata.num_rows);
println!("Row Groups: {}", metadata.num_row_groups);
println!("File size: {} bytes", metadata.file_size);

// 读取所有数据进行分析
let all_rows = read_posting_lists("posting_lists.parquet")?;
let total_postings: usize = all_rows.iter().map(|r| r.doc_ids.len()).sum();
println!("Total postings: {}", total_postings);
```

---

## 性能对比

| 场景 | 方法 | 性能 |
|------|------|------|
| 小索引频繁查询 | 完全加载 | ~9µs（内存） |
| 中等索引单词查询 | Row Group 统计 | ~815µs |
| 只需 doc_ids | 列式读取 | ~460µs |
| 多词 Boolean 查询 | 批量查询 | ~217µs/词 |
| 大索引高频查询 | Term Index | ~685µs |

---

## 注意事项

### 1. 索引更新

当前实现是 **immutable**（不可变）：
- ✅ 简单、安全
- ❌ 更新需要重建整个索引

**更新方式**:
```rust
// 重新索引
let new_index = FullTextField::new(&field);
new_index.write(&new_batch, 0)?;
new_index.persist("/path/to/index")?;  // 覆盖旧索引
```

### 2. 并发访问

Parquet 文件支持并发读取：
- ✅ 多个进程/线程可同时读
- ❌ 写入需要独占访问

### 3. 文件路径

- 使用绝对路径避免歧义
- 确保目录存在（persist 会自动创建）

### 4. 错误处理

```rust
use calm::utils::error::CoreResult;

fn search_safe(term: &str) -> CoreResult<Vec<u32>> {
    let result = search_term("posting_lists.parquet", term)?;
    Ok(result.map(|r| r.doc_ids).unwrap_or_default())
}
```

---

## 完整示例

```rust
use calm::schema::field::FieldOption;
use calm::segment::field_store::text::{FullTextField, search_term};
use calm::segment::field_store::IndexWriter;
use datafusion::arrow::array::StringArray;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use std::sync::Arc;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. 创建索引
    let field = FieldOption::Keyword {
        name: "content".to_string(),
        index: true,
        is_array: false,
        persist_option: None,
        case_sensitive: true,
    };
    let index = FullTextField::new(&field);

    // 2. 索引文档
    let texts = vec![
        "Rust programming is fast",
        "Python programming is easy",
    ];
    let schema = Arc::new(Schema::new(vec![
        Field::new("content", DataType::Utf8, false)
    ]));
    let array = StringArray::from(texts);
    let batch = RecordBatch::try_new(schema, vec![Arc::new(array)])?;
    index.write(&batch, 0)?;

    // 3. 持久化
    index.persist("./my_index")?;
    println!("✅ Index persisted");

    // 4. 查询（使用 Row Group 统计）
    let result = search_term("./my_index/posting_lists.parquet", "rust")?;
    if let Some(row) = result {
        println!("Found {} docs: {:?}", row.doc_ids.len(), row.doc_ids);
    }

    Ok(())
}
```

---

## 相关文档

- [`parquet_optimizations.md`](./parquet_optimizations.md) - 优化详解
- [`parquet_performance_report.md`](./parquet_performance_report.md) - 性能报告
- [examples/test_parquet_optimizations.rs](../examples/test_parquet_optimizations.rs) - 完整测试
- [examples/test_term_index.rs](../examples/test_term_index.rs) - Term Index 示例

---

*最后更新: 2025-11-22*
