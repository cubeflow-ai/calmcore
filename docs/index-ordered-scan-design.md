# 倒排索引有序扫描设计

## 背景

当前的 ORDER BY 查询流程：
1. 读取所有匹配的文档
2. 在内存中排序
3. 应用 LIMIT

这在大数据集 + 小 LIMIT 的场景下非常低效。

## 优化目标

利用倒排索引的有序性，直接按顺序读取文档，避免全表扫描和排序。

## 倒排索引结构

### Disk 版本（Parquet）
```rust
InvertedIndex::Disk(Arc<TreeReader<K, RoaringBitmap>>)
```
- 基于 B-Tree 结构
- Key 是有序的
- 支持 `seek()` 定位
- 支持 `iter()` 顺序迭代

### Memory 版本（BTree）
```rust
InvertedIndex::Memory(BTree<K, Arc<RwLock<Vec<u32>>>>)
```
- 基于内存 BTree
- Key 是有序的
- 支持 `seek()` 定位
- 支持 `iter()` 顺序迭代

## 设计方案

### 1. 在 IndexReader trait 中添加新方法

```rust
pub trait IndexReader: Send + Sync + 'static {
    // ... 现有方法 ...
    
    /// 按顺序扫描索引，返回有序的 doc_ids
    ///
    /// # Arguments
    /// * `ascending` - true 表示升序，false 表示降序
    /// * `filter_bitmap` - 可选的过滤 bitmap（只返回在这个 bitmap 中的 doc_ids）
    /// * `limit` - 可选的限制返回的文档数量
    ///
    /// # Returns
    /// 返回一个迭代器，按顺序产生 doc_ids
    fn scan_ordered(
        &self,
        ascending: bool,
        filter_bitmap: Option<&RoaringBitmap>,
        limit: Option<usize>,
    ) -> Box<dyn Iterator<Item = u32> + Send>;
}
```

### 2. 在 InvertedIndex 中实现有序扫描

```rust
impl<K: Clone + PartialOrd + Ord> InvertedIndex<K> {
    /// 按顺序扫描索引，返回有序的 doc_ids
    pub(crate) fn scan_ordered(
        &self,
        ascending: bool,
        filter_bitmap: Option<&RoaringBitmap>,
        limit: Option<usize>,
    ) -> Box<dyn Iterator<Item = u32> + Send> {
        match self {
            InvertedIndex::Disk(reader) => {
                // 使用 TreeReader 的迭代器
                let iter = if ascending {
                    reader.iter() // 正向迭代
                } else {
                    reader.iter_reverse() // 反向迭代（如果支持）
                };
                
                // 展开每个 key 对应的 doc_ids
                // 应用 filter_bitmap 和 limit
                Box::new(OrderedDocIterator::new(iter, filter_bitmap, limit))
            }
            InvertedIndex::Memory(btree) => {
                // 使用 BTree 的迭代器
                let iter = if ascending {
                    btree.iter()
                } else {
                    btree.iter_reverse() // 反向迭代（如果支持）
                };
                
                Box::new(OrderedDocIterator::new(iter, filter_bitmap, limit))
            }
        }
    }
}
```

### 3. 实现 OrderedDocIterator

```rust
struct OrderedDocIterator<I> {
    index_iter: I,                      // 索引迭代器
    current_docs: Option<Vec<u32>>,     // 当前 key 对应的 doc_ids
    current_offset: usize,              // 当前 doc_ids 的偏移量
    filter_bitmap: Option<RoaringBitmap>, // 过滤 bitmap
    limit: Option<usize>,               // 限制数量
    returned: usize,                    // 已返回的数量
}

impl<I> Iterator for OrderedDocIterator<I>
where
    I: Iterator<Item = (K, RoaringBitmap)>,
{
    type Item = u32;
    
    fn next(&mut self) -> Option<u32> {
        loop {
            // 如果达到 limit，停止
            if let Some(limit) = self.limit {
                if self.returned >= limit {
                    return None;
                }
            }
            
            // 如果当前 key 的 doc_ids 还没遍历完
            if let Some(docs) = &self.current_docs {
                while self.current_offset < docs.len() {
                    let doc_id = docs[self.current_offset];
                    self.current_offset += 1;
                    
                    // 应用 filter_bitmap
                    if let Some(filter) = &self.filter_bitmap {
                        if !filter.contains(doc_id) {
                            continue;
                        }
                    }
                    
                    self.returned += 1;
                    return Some(doc_id);
                }
            }
            
            // 移动到下一个 key
            if let Some((key, bitmap)) = self.index_iter.next() {
                self.current_docs = Some(bitmap.iter().collect());
                self.current_offset = 0;
            } else {
                return None;
            }
        }
    }
}
```

## 使用场景

### 场景 1：ORDER BY + LIMIT（最常见）

```sql
SELECT * FROM table WHERE age > 18 ORDER BY created_at DESC LIMIT 10;
```

**优化前**：
1. 读取所有 age > 18 的文档（可能 100万条）
2. 在内存中按 created_at 排序
3. 取前 10 条

**优化后**：
1. 从 created_at 索引中按降序迭代
2. 过滤 age > 18 的文档
3. 返回前 10 条
4. 可能只需要读取几百条文档

### 场景 2：ORDER BY 无 LIMIT

```sql
SELECT * FROM table WHERE age > 18 ORDER BY created_at DESC;
```

**分析**：
- 如果结果集很大，有序扫描可能不如全表扫描 + 排序
- 因为需要逐个检查每个 doc_id 是否满足 filter
- 建议：只在结果集较小或有 LIMIT 时使用

## 性能分析

### 何时使用有序扫描？

建议的启发式规则：
```rust
let use_ordered_scan = 
    has_order_by && 
    has_index_on_sort_field &&
    (
        has_limit ||  // 有 LIMIT 总是值得尝试
        hit_ratio < 0.5  // 或者命中率 < 50%
    );
```

### 性能对比

| 场景 | 全表扫描 + 排序 | 有序扫描 |
|------|----------------|----------|
| 100万条，LIMIT 10 | 读取 100万 + 排序 | 读取 ~100 条 |
| 100万条，LIMIT 1000 | 读取 100万 + 排序 | 读取 ~10000 条 |
| 100万条，无 LIMIT | 读取 100万 + 排序 | 读取 100万（逐个检查）|

## 实现步骤

1. **Phase 1**：在 `InvertedIndex` 中实现 `scan_ordered` 方法
2. **Phase 2**：在 `IndexReader` trait 中添加 `scan_ordered` 方法
3. **Phase 3**：在 `SegmentScanner` 中使用有序扫描
4. **Phase 4**：添加启发式规则，决定何时使用有序扫描
5. **Phase 5**：性能测试和调优

## 注意事项

1. **反向迭代**：需要确认 TreeReader 和 BTree 是否支持反向迭代
2. **内存使用**：每个 key 的 doc_ids 需要临时存储在内存中
3. **并发安全**：Memory 版本使用 RwLock，需要注意锁的粒度
4. **分布式环境**：每个 Segment 独立有序扫描，需要在上层做归并排序

## 未来优化

1. **Top-K 优化**：使用优先队列，避免存储所有中间结果
2. **并行扫描**：多个 Segment 并行有序扫描，然后归并
3. **索引统计**：记录每个 key 的 doc_count，用于更精确的成本估算
