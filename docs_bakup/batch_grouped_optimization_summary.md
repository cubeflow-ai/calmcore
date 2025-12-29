# Batch-Grouped Query 优化实现总结

## ✅ 已完成的优化

### 1. IndexReader Trait 新增方法

**文件**: `src/segment/field_store/mod.rs`

**新增方法**:
```rust
fn range_grouped_by_batch(
    &self,
    start: &ScalarValue,
    start_inclusive: bool,
    end: &ScalarValue,
    end_inclusive: bool,
    batch_ranges: &[(u32, u32, u32)],
) -> Option<HashMap<u32, Vec<u32>>>
```

**功能**: 
- 范围查询并直接返回按 batch_key 分组的 doc_ids
- 避免先收集全部 doc_ids 再逐个查找 batch_key 的开销

**实现原理**:
1. 调用 `range_union()` 或 `range()` 获取完整的 RoaringBitmap
2. 对每个 batch_range 使用 `RoaringBitmap::range()` 方法
3. 只迭代该 batch 区间内的 doc_ids: O(log m + k) per batch

**性能分析**:
- 传统方法: O(m * log b) where m = matched docs, b = num batches
- 新方法: O(m + b * log m)
- 对于 m=1M, b=1K: 约 10x 性能提升

### 2. SegmentScanner 批量查找优化

**文件**: `src/compute/segment_scanner.rs`

**修改**: `generate_next_chunk()` 方法

**优化前**:
```rust
for doc_id in self.doc_ids_iter.by_ref() {
    let batch_key = self.raw_data.get_batch_key_for_doc(doc_id)?;
    batch_groups.entry(batch_key).or_default().push(doc_id);
}
```
- 每个 doc_id 调用一次 `get_batch_key_for_doc()`
- 即使使用了 binary search，对 100万 doc_ids 仍需 100万次调用

**优化后**:
```rust
// 1. 先收集一批 doc_ids
let mut doc_ids_to_process: Vec<u32> = Vec::new();
for doc_id in self.doc_ids_iter.by_ref() {
    doc_ids_to_process.push(doc_id);
    if doc_ids_to_process.len() >= target_chunk_size {
        break;
    }
}

// 2. 批量查找 batch_key
let batch_groups = self.raw_data.batch_lookup_doc_ids(&doc_ids_to_process);
```
- **一次性批量查找** 所有 doc_ids 的 batch_key
- 利用了之前实现的 `batch_lookup_doc_ids()` 方法
- 显著减少函数调用开销

**性能提升**:
- 函数调用次数: 100万次 → 1次
- 预期性能提升: 5-10x

### 3. 其他已有优化 (之前实现)

**文件**: `src/segment/field_store/row_data.rs`

#### 3.1 预计算 Batch Ranges
```rust
pub struct ParquetRowDataReader {
    ranges: Arc<Vec<(u32, u32, usize)>>,  // (start, end, rowgroup_idx)
    // ...
}
```
- 在 Parquet 文件加载时预计算所有 RowGroup 的 doc_id 边界
- `get_batch_key_for_doc()` 使用 binary search: O(log n)

#### 3.2 批量查找方法
```rust
pub fn batch_lookup_doc_ids(&self, doc_ids: &[u32]) -> HashMap<u32, Vec<u32>>
```
- 一次性查找多个 doc_ids 的 batch_key
- 返回按 batch_key 分组的结果
- 利用 doc_ids 的有序性,优化查找效率

## 🎯 性能对比

### 场景: 100万 doc_ids, 1000 个 Parquet RowGroups

| 方法 | 时间复杂��� | 估算操作数 | 相对性能 |
|------|-----------|-----------|---------|
| 原始方法 (逐个查找) | O(m * log b) | 1M * log(1K) ≈ 10M | 1x |
| 批量查找 | O(m + b) | 1M + 1K ≈ 1M | 10x |
| range_grouped_by_batch | O(m + b * log m) | 1M + 1K * log(1M) ≈ 1M + 20K | 8-10x |

## 📊 代码统计

### 修改的文件
- `src/segment/field_store/mod.rs`: +75 行 (新增 trait 方法)
- `src/compute/segment_scanner.rs`: ~30 行 (重构批量查找逻辑)
- `docs/batch_grouped_query_design.md`: 新建设计文档

### 编译状态
✅ `cargo build --release` 成功
✅ `cargo check --lib` 无警告

## 🚀 下一步优化方向

### 1. 在 Index Query 阶段就分组 (深度优化)
**当前**: Query index → Bitmap → 迭代 → 批量查找 batch_key
**目标**: Query index → 直接返回 HashMap<batch_key, Vec<doc_id>>

**挑战**:
- 需要在 IndexReader 调用时就传入 batch_ranges
- 需要架构调整: SegmentScanner 在调用 index 时就知道要分组
- 涉及 `expr_to_bitmap()` → `query_range()` → IndexReader 整个调用链

**预期收益**: 
- 再提升 2-3x (避免完整 bitmap 迭代)
- 特别适合低命中率场景 (如 1% 命中率)

### 2. 并行化 Batch 处理
```rust
batch_ranges
    .par_iter()  // Rayon parallel iterator
    .map(|&(start, end, key)| {
        let docs = full_bitmap.range(start..end).collect();
        (key, docs)
    })
    .collect()
```

**预期收益**: 
- 多核场景下 2-4x 提升
- 适合大范围查询 (100+ batches)

### 3. Early Termination for LIMIT
当前 `range_grouped_by_batch` 会扫描所有匹配的 doc_ids,
可以添加 LIMIT 参数,提前终止:

```rust
fn range_grouped_by_batch(
    // ...
    limit: Option<usize>,  // 新增参数
) -> Option<HashMap<u32, Vec<u32>>> {
    let mut total_docs = 0;
    for &(start, end, key) in batch_ranges {
        let docs = full_bitmap.range(start..end).collect();
        total_docs += docs.len();
        
        if let Some(lim) = limit {
            if total_docs >= lim {
                break;  // 提前终止
            }
        }
    }
}
```

**预期收益**:
- LIMIT 1000 查询 100万 docs: 1000x 提升
- 特别适合 Web 分页场景

## 📝 测试验证

### 验证方法
1. **功能测试**: 运行现有的 range query 测试
   ```bash
   cargo run --example test_range_query --release
   ```

2. **性能基准测试**:
   - 测试场景: 1000万 docs, 1000 batches, 查询 100万匹配
   - 对比指标: 查询耗时, CPU 使用率
   - 预期: 5-10x 性能提升

3. **压力测试**:
   - 大范围查询: 1000万 docs, 匹配 500万
   - 小 LIMIT: LIMIT 100
   - 多并发: 10 个并发查询

### 测试数据集
使用 `test_suite/` 中的 taxi_trips 数据集:
- 约 500万条记录
- pickup_datetime 字段有索引
- 可以测试时间范围查询

## 🎉 总结

### 核心成果
1. ✅ IndexReader trait 新增 `range_grouped_by_batch()` 方法
2. ✅ SegmentScanner 使用批量查找优化
3. ✅ 编译通过,无警告
4. ✅ 设计文档完整

### 性能提升
- **预期**: 5-10x (对于典型的范围查询场景)
- **场景**: 100万+ doc_ids, 1000+ batches
- **适用**: 时间范围查询, 数值范围查询

### 代码质量
- 向后兼容: 保留了原有的查询路径
- 可扩展: 新方法有默认实现,子类可覆盖优化
- 文档完善: 详细的注释和设计文档

### 后续工作
1. 运行实际测试验证性能提升
2. 考虑实现"Index Query 阶段就分组"的深度优化
3. 添加并行化支持
4. 实现 Early Termination for LIMIT

