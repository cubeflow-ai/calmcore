# ORDER BY 优化实现状态

## 你的问题

> "这个地方 order 必要是不是没有下发给 scan。如果下发了未来 order scan 的时候使用索引来做。"

**回答**：是的！你说得对。ORDER BY 信息确实没有下发给 `scan()`，这是一个很好的优化点！

## 已完成的工作

### 1. 代码实现

#### 新增方法

```rust
// SegmentScanner
pub fn create_optimized_plan_with_sort(
    &self,
    filters: &[Expr],
    projection: Option<&Vec<usize>>,
    limit: Option<usize>,
    sort_fields: Option<&[(String, bool)]>,  // ← 新增：ORDER BY 信息
) -> Option<Arc<dyn ExecutionPlan>>
```

#### 新增策略

```rust
enum ScanStrategy {
    IndexScan,
    FullScan,
    EarlyLimit,
    IndexOrderedScan,  // ← 新增：使用倒排索引做有序扫描
}
```

#### 策略选择逻辑

```rust
fn choose_strategy_with_sort(...) -> ScanStrategy {
    // 策略 0: ORDER BY + LIMIT + 单字段排序 + 有索引 → Index Ordered Scan
    if let (Some(limit_val), Some(sort_fields_val)) = (limit, sort_fields) {
        if sort_fields_val.len() == 1 {
            let (field_name, _ascending) = &sort_fields_val[0];
            if self.index_readers.contains_key(field_name) {
                return ScanStrategy::IndexOrderedScan;
            }
        }
    }
    // ... 其他策略
}
```

### 2. 文档

- ✅ `docs/order-by-optimization.md` - 详细的优化设计文档
- ✅ `docs/order-by-implementation-status.md` - 本文档

### 3. 编译验证

```bash
✅ cargo check - 编译通过
```

## 当前状态

### 已实现（占位）

```rust
fn create_index_ordered_scan_plan(...) -> Option<Arc<dyn ExecutionPlan>> {
    // 1. 应用 filters 获取候选文档
    let mut candidate_docs = self.valid_docs.clone();
    for filter in filters {
        if let Some(bitmap) = self.expr_to_bitmap(filter) {
            candidate_docs &= bitmap;
        }
    }
    
    // 2. 使用倒排索引获取有序的 doc_ids
    // TODO: 这里需要 IndexReader 提供一个 `scan_ordered()` 方法
    // 目前先 fallback 到普通的 IndexScan
    log::warn!("🎯 [IndexOrderedScan] IndexReader::scan_ordered() not implemented yet");
    
    // 临时方案：使用普通的 IndexScan
    self.create_execution_plan(filters, projection)
}
```

### 待完成

#### 短期（1-2 周）

1. **修改 SortLimitOptimizer 传递 sort hints**

```rust
// 在 SortLimitOptimizer::execute_on_partition 中
let provider = Arc::new(PartitionTableProviderWithHints::new_with_sort_hints(
    partition,
    Some(info.sort_fields.clone()),  // ← 传递 ORDER BY 信息
));
```

2. **创建 PartitionTableProviderWithHints**

```rust
pub struct PartitionTableProviderWithHints {
    partition: Arc<Partition>,
    schema: SchemaRef,
    sort_hints: Option<Vec<(String, bool)>>,
}

impl TableProvider for PartitionTableProviderWithHints {
    async fn scan(...) -> Result<Arc<dyn ExecutionPlan>> {
        scanner.create_optimized_plan_with_sort(
            filters,
            projection,
            limit,
            self.sort_hints.as_deref(),  // ← 传递给 SegmentScanner
        )
    }
}
```

#### 中期（1-2 月）

3. **为 IndexReader trait 添加 scan_ordered() 方法**

```rust
pub trait IndexReader: Send + Sync + 'static {
    // ... 现有方法 ...
    
    /// 有序扫描：按照索引的顺序返回 doc_ids
    fn scan_ordered(
        &self,
        ascending: bool,
        limit: Option<usize>,
        filter: Option<&RoaringBitmap>,
    ) -> Option<Vec<u32>> {
        None  // 默认不支持
    }
}
```

4. **为各种索引实现 scan_ordered()**

- B-Tree 索引
- 倒排索引（如果支持有序遍历）
- Range 索引

#### 长期（3-6 月）

5. **性能测试和调优**
6. **支持多字段排序的优化**
7. **支持复合索引**

## 为什么 ORDER BY 没有下发？

### DataFusion 的设计

DataFusion 的执行计划是这样的：

```
LimitExec(10)
    ↓
SortExec(age ASC)  ← ORDER BY 在这里
    ↓
FilterExec(age > 20)
    ↓
TableScan::scan()  ← 这里看不到 ORDER BY
```

**原因**：
- `TableProvider::scan()` 是一个通用接口
- ORDER BY 是在 scan 之后才添加的（`SortExec` 节点）
- DataFusion 不知道你的存储引擎是否支持有序扫描

### 解决方案

我们需要**绕过** DataFusion 的标准流程，在更高层（`SortLimitOptimizer`）传递 ORDER BY 信息：

```
SortLimitOptimizer (我们的代码)
    ↓ 传递 sort_hints
PartitionTableProviderWithHints (我们的代码)
    ↓ 传递给 SegmentScanner
SegmentScanner (我们的代码)
    ↓ 使用倒排索引有序扫描
ExecutionPlan
```

## 性能预期

### 场景 1：单字段排序 + LIMIT

```sql
SELECT * FROM users WHERE age > 20 ORDER BY age LIMIT 10
```

| 实现阶段 | 执行方式 | 性能 |
|---------|---------|------|
| 当前 | 全表扫描 + 排序 | 1x（基准） |
| 短期（sort hints） | 索引扫描 + 排序 | ~10x |
| 长期（scan_ordered） | 索引有序扫描 | ~100x |

### 场景 2：多字段排序 + LIMIT

```sql
SELECT * FROM users WHERE age > 20 ORDER BY age, name LIMIT 10
```

| 实现阶段 | 执行方式 | 性能 |
|---------|---------|------|
| 当前 | 全表扫描 + 排序 | 1x（基准） |
| 短期（sort hints） | 索引扫描 + 排序 | ~2x |
| 长期（复合索引） | 复合索引有序扫描 | ~50x |

## 下一步行动

### 立即可做

1. **创建 PartitionTableProviderWithHints**
   - 复制 `PartitionTableProvider`
   - 添加 `sort_hints` 字段
   - 在 `scan()` 中传递给 `SegmentScanner`

2. **修改 SortLimitOptimizer**
   - 使用 `PartitionTableProviderWithHints`
   - 传递 `info.sort_fields`

3. **测试验证**
   - 查看日志，确认策略选择为 `IndexOrderedScan`
   - 验证性能提升

### 需要更多时间

4. **实现 IndexReader::scan_ordered()**
   - 为 B-Tree 索引实现
   - 为倒排索引实现（如果支持）

5. **完善 create_index_ordered_scan_plan()**
   - 使用 `reader.scan_ordered()` 获取有序 doc_ids
   - 生成优化的 ExecutionPlan

## 代码示例

### 使用示例（未来）

```rust
// 在 SortLimitOptimizer 中
let provider = Arc::new(PartitionTableProviderWithHints::new_with_sort_hints(
    partition,
    Some(vec![("age".to_string(), true)]),  // ORDER BY age ASC
));

ctx.register_table(table_name, provider)?;
let df = ctx.sql(sql).await?;
let batches = df.collect().await?;
```

### 日志输出（预期）

```
🎯 [Strategy] IndexOrderedScan: ORDER BY age with index, limit=10
🎯 [SegmentScanner] Strategy=IndexOrderedScan, selectivity=30.00%, can_use_index=true, limit=Some(10), sort=Some([("age", true)])
🎯 [IndexOrderedScan] Using index on 'age' for ordered scan, ascending=true, limit=10
🎯 [IndexOrderedScan] After filters: 1000 candidate docs
🎯 [IndexOrderedScan] Using scan_ordered() to get top 10 docs
✅ [IndexOrderedScan] Returned 10 docs in order, no sorting needed
```

## 总结

1. ✅ **你的观察是对的**：ORDER BY 信息确实没有下发给 `scan()`
2. ✅ **已经做了准备工作**：
   - 添加了 `create_optimized_plan_with_sort()` 方法
   - 添加了 `IndexOrderedScan` 策略
   - 添加了策略选择逻辑
3. ⏳ **下一步**：
   - 创建 `PartitionTableProviderWithHints`
   - 修改 `SortLimitOptimizer` 传递 sort hints
   - 实现 `IndexReader::scan_ordered()`
4. 🚀 **预期性能提升**：~10x（短期）到 ~100x（长期）

这是一个非常好的优化方向，值得投入时间实现！
