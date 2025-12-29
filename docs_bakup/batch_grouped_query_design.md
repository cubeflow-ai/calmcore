# Batch-Grouped Query 优化设计

## 背景

当前查询流程存在性能瓶颈:
```
1. query index → RoaringBitmap (100万 doc_ids)
2. 迭代 bitmap → 对每个 doc_id 调用 get_batch_key_for_doc()
3. 按 batch_key 分组
```

即使 `get_batch_key_for_doc()` 使用了二分查找 O(log n),但对 100万个 doc_id 调用 100万次仍然很慢。

## 核心思路

**在 index query 阶段就利用 batch_ranges 信息,直接输出按 batch_key 分组的结果**

### 数据结构

```rust
// ParquetRowDataReader 已有的 batch_ranges
ranges: Arc<Vec<(u32, u32, usize)>>  // (start_doc_id, end_doc_id, rowgroup_idx)
// 其中 start_doc_id 就是 batch_key
```

### 新增 IndexReader 方法

```rust
pub trait IndexReader {
    // ... 现有方法 ...
    
    /// 范围查询并按 batch 分组返回
    /// 
    /// 这个方法在遍历倒排索引时,同时利用 batch_ranges 将 doc_ids 分组
    /// 避免了先收集全部 doc_ids 再逐个查找 batch_key 的开销
    fn range_grouped_by_batch(
        &self,
        start: &ScalarValue,
        start_inclusive: bool,
        end: &ScalarValue,
        end_inclusive: bool,
        batch_ranges: &[(u32, u32, u32)],  // (start, end, batch_key)
    ) -> Option<HashMap<u32, Vec<u32>>>;
}
```

### 实现思路

#### 方案 A: RoaringBitmap 后处理 (简单但不完美)

```rust
fn range_grouped_by_batch(...) -> Option<HashMap<u32, Vec<u32>>> {
    // 1. 获取 RoaringBitmap
    let bitmap = self.range_union(start, start_inclusive, end, end_inclusive)?;
    
    // 2. 按 batch_ranges 分组
    let mut result: HashMap<u32, Vec<u32>> = HashMap::new();
    let mut range_idx = 0;
    
    for doc_id in bitmap.iter() {
        // 找到对应的 batch_range
        while range_idx < batch_ranges.len() 
            && doc_id >= batch_ranges[range_idx].1 {
            range_idx += 1;
        }
        
        if range_idx < batch_ranges.len() 
            && doc_id >= batch_ranges[range_idx].0 {
            let batch_key = batch_ranges[range_idx].2;
            result.entry(batch_key).or_default().push(doc_id);
        }
    }
    
    Some(result)
}
```

**优点:**
- 实现简单
- 只需要一次遍历 bitmap
- 利用了 doc_id 和 batch_ranges 都是有序的特性

**缺点:**
- 仍然需要遍历完整的 bitmap
- 对于 100万 doc_ids 还是有 100万次循环

#### 方案 B: Batch 级别的 Bitmap 操作 (更高效)

核心思路: 利用 RoaringBitmap 的区间操作

```rust
fn range_grouped_by_batch(...) -> Option<HashMap<u32, Vec<u32>>> {
    // 1. 获取完整的 RoaringBitmap
    let full_bitmap = self.range_union(start, start_inclusive, end, end_inclusive)?;
    
    // 2. 对每个 batch_range 做 bitmap 区间截取
    let mut result: HashMap<u32, Vec<u32>> = HashMap::new();
    
    for &(start_doc, end_doc, batch_key) in batch_ranges {
        // 创建这个 batch 的 doc_id 范围的 bitmap
        let batch_range_bitmap = RoaringBitmap::from_iter(start_doc..end_doc);
        
        // 与查询结果求交集
        let intersect = &full_bitmap & &batch_range_bitmap;
        
        if !intersect.is_empty() {
            result.insert(batch_key, intersect.iter().collect());
        }
    }
    
    Some(result)
}
```

**优点:**
- 利用了 RoaringBitmap 的高效区间操作
- 代码清晰易懂

**缺点:**
- 需要创建多个临时 bitmap
- 对于大量小 batch 可能不如直接遍历

#### 方案 C: RoaringBitmap 区间迭代器 (最优)

利用 RoaringBitmap 的 `range()` 方法

```rust
fn range_grouped_by_batch(...) -> Option<HashMap<u32, Vec<u32>>> {
    // 1. 获取完整的 RoaringBitmap
    let full_bitmap = self.range_union(start, start_inclusive, end, end_inclusive)?;
    
    // 2. 对每个 batch 使用 range 迭代器
    let mut result: HashMap<u32, Vec<u32>> = HashMap::new();
    
    for &(start_doc, end_doc, batch_key) in batch_ranges {
        // 使用 RoaringBitmap::range() 只迭代这个区间
        let doc_ids: Vec<u32> = full_bitmap
            .range(start_doc..end_doc)
            .collect();
        
        if !doc_ids.is_empty() {
            result.insert(batch_key, doc_ids);
        }
    }
    
    Some(result)
}
```

**优点:**
- **最高效!** RoaringBitmap 的 range() 是 O(log n) 查找 + O(k) 迭代 (k = 区间内元素数)
- 不需要遍历完整 bitmap
- 不需要创建临时 bitmap
- 代码简洁

**这就是最优解!**

## 使用示例

### 修改前 (SegmentScanner)

```rust
fn generate_next_chunk(&mut self) -> DFResult<Vec<RecordBatch>> {
    let mut batch_groups: HashMap<u32, Vec<u32>> = HashMap::new();
    
    // 遍历 bitmap,逐个查找 batch_key
    for doc_id in self.doc_ids_iter.by_ref() {
        let batch_key = self.raw_data.get_batch_key_for_doc(doc_id)?;
        batch_groups.entry(batch_key).or_default().push(doc_id);
    }
    
    // ...
}
```

### 修改后

```rust
fn generate_next_chunk(&mut self) -> DFResult<Vec<RecordBatch>> {
    // 直接获取按 batch 分组的结果!
    let batch_groups = self.index_reader.range_grouped_by_batch(
        start,
        start_inclusive,
        end,
        end_inclusive,
        &self.raw_data.ranges,  // 传入预计算的 batch_ranges
    )?;
    
    // ...
}
```

## 性能分析

### 当前实现
- Query index: O(m) where m = matched doc_ids
- Binary search batch_key: O(m * log b) where b = num batches
- **总复杂度: O(m * log b)**

### 方案 C (RoaringBitmap range)
- Query index: O(m)
- Per-batch range iteration: O(b * (log m + k)) where k = avg docs per batch
- **总复杂度: O(m + b * log m)**

对于 m = 1,000,000, b = 1,000:
- 当前: 1M * log(1000) ≈ 10M 次操作
- 方案 C: 1M + 1000 * log(1M) ≈ 1M + 20K ≈ 1M 次操作

**预期提升: 10x 性能改进!**

## 实现计划

1. ✅ 设计文档 (本文)
2. ⬜ 在 `IndexReader` trait 中新增 `range_grouped_by_batch()` 方法
3. ⬜ 在 `GenericIndexedField` 中实现该方法 (方案 C)
4. ⬜ 修改 `SegmentScanner::generate_next_chunk()` 使用新方法
5. ⬜ 测试验证性能提升

## 注意事项

1. **只对 range query 有效**: 这个优化主要针对范围查询(如 timestamp range)
2. **需要 batch_ranges**: 必须从 ParquetRowDataReader 传入预计算的 batch 边界
3. **向后兼容**: 保留原有的 `range()` 方法,新方法是可选的优化路径

## 进一步优化空间

如果还想更快,可以考虑:
1. **并行化**: 对每个 batch 的 range 迭代可以并行
2. **Early termination**: 如果有 LIMIT,达到后立即停止
3. **Chunk batches**: 每次只处理 N 个 batch,避免一次性处理太多

