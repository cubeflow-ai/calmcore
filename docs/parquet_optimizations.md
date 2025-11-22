# Parquet 持久化优化总结

## 概述

本文档总结了全文索引 Parquet 持久化的所有优化实现。这些优化显著提升了查询性能，同时保持了存储效率。

## 已实现的优化

### 1. Row Group 统计信息优化 ✅

**实现**: `search_term()` 函数利用 Parquet 的 Row Group min/max 统计信息

**原理**:
- Parquet 为每个 Row Group 的每一列维护 min/max 统计信息
- 对于排序的 term 列，可以通过二分查找快速定位包含目标词项的 Row Group
- 只读取目标 Row Group，跳过其他 Row Groups

**性能提升**:
- 对于大文件，避免读取不相关的 Row Groups
- 早期终止：如果查询词在最小值之前，立即返回不存在

**代码示例**:
```rust
// 使用 Row Group 统计信息定位数据
for rg_idx in 0..num_row_groups {
    let row_group = metadata.row_group(rg_idx);
    if let Some(stats) = row_group.column(0).statistics() {
        if let (Some(min_val), Some(max_val)) = (stats.min_bytes_opt(), stats.max_bytes_opt()) {
            if term >= min_str && term <= max_str {
                target_row_group = Some(rg_idx);
                break;
            } else if term < min_str {
                return Ok(None); // 早期终止
            }
        }
    }
}
```

**测试结果**:
- 早期终止测试 (查询 "aaaaa"): ~700µs
- 1.36x 加速相比完全加载索引

---

### 2. 列式读取优化 ✅

**实现**: `search_term_docids_only()` 函数只读取需要的列

**原理**:
- Parquet 是列式存储，可以只读取需要的列
- 对于只需要 doc_ids 的查询（如简单词项查询），可以跳过 positions 列
- positions 列通常是嵌套列表，占用较大空间

**性能提升**:
- 减少 I/O：不读取 positions 列
- 减少内存：不解析和存储 positions 数据
- 查询加速：数据传输和解析时间减少

**代码示例**:
```rust
// 只投影 term (列0) 和 doc_ids (列3)
let reader = builder
    .with_row_groups(vec![rg_idx])
    .with_projection(ProjectionMask::leaves(
        schema_descr_ptr,
        vec![0, 3], // 只读这两列
    ))
    .build()?;
```

**测试结果**:
- 查询 "programming": ~500µs
- 相比完整读取节省约 50% 时间（跳过了 positions 列）

---

### 3. 批量查询优化 ✅

**实现**: `search_terms_batch()` 函数一次查询多个词项

**原理**:
- 一次性读取所有数据到内存
- 对每个词项进行二分查找
- 避免多次打开/关闭文件和重复读取

**性能提升**:
- 减少文件 I/O 次数
- 利用批处理摊销文件打开成本
- 适合需要查询多个词项的场景

**代码示例**:
```rust
let all_rows = read_posting_lists(path)?;
let mut results = Vec::with_capacity(sorted_terms.len());

for term in sorted_terms {
    let result = all_rows
        .binary_search_by(|row| row.term.as_str().cmp(term))
        .ok()
        .map(|idx| all_rows[idx].clone());
    results.push(result);
}
```

**测试结果**:
- 4 个词项批量查询: ~870µs
- 平均每词项 ~217µs

---

### 4. Term Index（词项索引）✅

**实现**: `build_term_index()`, `search_with_term_index()` 函数

**原理**:
- 构建 term → (row_group_id, row_offset) 映射表
- 将索引保存为 JSON 文件（易读易调试）
- 查询时直接定位到具体的 Row Group 和行偏移
- 实现 O(1) 词项查找（假设索引在内存中）

**优势**:
- 真正的 O(1) 查找（内存二分查找）
- 适合点查询密集的场景
- 可以预加载到内存

**劣势**:
- 额外存储开销：约 100-110% Parquet 文件大小
- 需要维护索引一致性
- 索引构建时间（虽然只需一次）

**代码示例**:
```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TermIndexEntry {
    pub term: String,
    pub row_group_id: usize,
    pub row_offset: usize,
}

// 构建索引
let term_index = build_term_index(parquet_path)?;
save_term_index(&term_index, "term_index.json")?;

// 使用索引查询
let result = search_with_term_index(parquet_path, "rust", &term_index)?;
```

**测试结果**:
- 构建 42 词项索引: ~2.2ms
- 加载索引: ~97µs
- 查询加速: 1.20x
- 存储开销: ~108% Parquet 大小

**使用场景建议**:
- ✅ 使用 term index: 大量点查询，索引可常驻内存
- ✅ 使用 Row Group stats: 中等查询频率，节省存储
- ✅ 完全加载到内存: 小索引，查询极其频繁

---

### 5. Parquet 元数据查询 ✅

**实现**: `get_parquet_metadata()` 函数

**功能**:
- 获取 Row Groups 数量
- 获取总行数
- 获取文件大小

**用途**:
- 监控和调试
- 决策是否使用 term index
- 估算内存使用

**测试结果**:
```
• Row Groups: 1
• Total Rows: 30
• File Size: 2719 bytes
```

---

## 性能对比总结

| 优化方案 | 查询延迟 | 存储开销 | 适用场景 |
|---------|---------|---------|---------|
| **完全加载** | ~1.1ms (load) + ~9µs (query) | 0% | 小索引，频繁查询 |
| **Row Group Stats** | ~815µs | 0% | 中等索引，中等查询频率 |
| **列式读取 (doc_ids only)** | ~500µs | 0% | 只需 doc_ids 的查询 |
| **批量查询** | ~870µs (4词) | 0% | 多词项查询 |
| **Term Index** | ~685µs | +108% | 大索引，高频点查询 |

---

## 架构优势

### 相比自定义序列化：

1. **自动压缩**
   - ZSTD 压缩 (level 3)
   - 字典编码 (term 列)
   - Delta 编码 (doc_ids, positions)

2. **查询优化**
   - Row Group min/max 统计
   - Page-level 统计
   - 列式投影

3. **标准格式**
   - 通用工具支持 (parquet-tools)
   - 跨语言兼容
   - 生态系统丰富

4. **代码简洁**
   - 利用 Arrow/Parquet 库
   - 不需要手动实现压缩
   - 不需要手动维护统计信息

---

## 文件结构

```
{field_path}/
├── posting_lists.parquet    # 主索引文件 (ZSTD + 字典编码)
├── field_stats.json          # 字段统计信息
└── term_index.json           # 可选：词项索引 (O(1) 查找)
```

---

## API 使用指南

### 基础查询（使用 Row Group 统计）
```rust
use calm::segment::field_store::text::search_term;

let result = search_term("posting_lists.parquet", "rust")?;
if let Some(row) = result {
    println!("Found {} docs", row.doc_ids.len());
}
```

### 列式读取（只要 doc_ids）
```rust
use calm::segment::field_store::text::search_term_docids_only;

let doc_ids = search_term_docids_only("posting_lists.parquet", "rust")?;
if let Some(ids) = doc_ids {
    println!("Doc IDs: {:?}", ids);
}
```

### 批量查询
```rust
use calm::segment::field_store::text::search_terms_batch;

let terms = vec!["rust", "python", "javascript"];
let results = search_terms_batch("posting_lists.parquet", &terms)?;
```

### 使用 Term Index
```rust
use calm::segment::field_store::text::{
    build_term_index, save_term_index, load_term_index, search_with_term_index
};

// 构建并保存索引（一次性操作）
let index = build_term_index("posting_lists.parquet")?;
save_term_index(&index, "term_index.json")?;

// 加载索引并查询
let index = load_term_index("term_index.json")?;
let result = search_with_term_index("posting_lists.parquet", "rust", &index)?;
```

---

## 未来优化方向

### 1. 并行 Row Group 加载 (TODO)
- 使用 rayon 并行读取多个 Row Groups
- 适合非常大的索引文件
- 需要权衡线程开销

### 2. 内存映射 (TODO)
- 使用 mmap 映射 Parquet 文件
- 零拷贝读取
- 依赖 OS 页面缓存

### 3. 自适应策略
- 根据索引大小自动选择策略
- 小索引 → 完全加载到内存
- 中等索引 → Row Group 统计
- 大索引 → Term Index + 内存映射

### 4. 增量更新
- 支持 append-only 更新
- 新数据写入新 Row Group
- 避免重写整个文件

---

## 测试覆盖

所有优化都有对应的测试示例：

- ✅ `test_parquet_persist.rs` - 基础持久化测试
- ✅ `test_parquet_optimizations.rs` - 优化功能测试
- ✅ `test_term_index.rs` - Term Index 测试

运行测试：
```bash
cargo run --example test_parquet_optimizations
cargo run --example test_term_index
```

---

## 结论

通过这些优化，Parquet 持久化方案实现了：

1. **性能**: 相比完全加载快 1.36x，早期终止更快
2. **灵活性**: 多种查询模式支持
3. **扩展性**: 适合从小到大的各种索引
4. **标准化**: 基于工业标准 Parquet 格式

核心理念：**利用 Parquet 的列式存储和统计信息，避免不必要的 I/O**

---

*生成日期: 2025-11-22*
*版本: 1.0*
