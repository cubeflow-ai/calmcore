# ORDER BY 优化设计

## 问题

当前实现中，ORDER BY 信息没有下发到 `SegmentScanner`，导致无法利用倒排索引做有序扫描。

## 现状分析

### DataFusion 的执行流程

```
SQL: SELECT * FROM users WHERE age > 20 ORDER BY age LIMIT 10
    ↓
LogicalPlan:
    Limit(10)
      ↓
    Sort(age ASC)
      ↓
    Filter(age > 20)
      ↓
    TableScan(users)
    ↓
PhysicalPlan:
    LimitExec(10)
      ↓
    SortExec(age ASC)  ← ORDER BY 在这里
      ↓
    FilterExec(age > 20)
      ↓
    TableScan::scan()  ← 这里看不到 ORDER BY
```

**问题**：`TableProvider::scan()` 方法看不到 ORDER BY 信息，因为 ORDER BY 是在 scan 之后才添加的。

## 解决方案

### 方案 1：在 DistributedExecutor 层处理（当前实现）

**优点**：
- ✅ 已经实现（`SortLimitOptimizer`）
- ✅ 可以跨 Partition 优化
- ✅ 使用 TOP-K 合并算法

**缺点**：
- ❌ 每个 Partition 仍然需要全表扫描 + 排序
- ❌ 无法利用倒排索引的有序性

**适用场景**：
- 多字段排序
- 跨 Partition 的 TOP-K 查询

### 方案 2：扩展 PartitionTableProvider 支持 sort hints（推荐）

创建一个自定义的 TableProvider，支持传递 sort hints：

```rust
pub struct PartitionTableProviderWithHints {
    partition: Arc<Partition>,
    schema: SchemaRef,
    sort_hints: Option<Vec<(String, bool)>>,  // ← 新增
}

impl PartitionTableProviderWithHints {
    pub fn new_with_sort_hints(
        partition: Arc<Partition>,
        sort_hints: Option<Vec<(String, bool)>>,
    ) -> Self {
        // ...
    }
}

impl TableProvider for PartitionTableProviderWithHints {
    async fn scan(...) -> Result<Arc<dyn ExecutionPlan>> {
        // 传递 sort_hints 给 SegmentScanner
        scanner.create_optimized_plan_with_sort(
            filters,
            projection,
            limit,
            self.sort_hints.as_deref(),  // ← 传递 ORDER BY 信息
        )
    }
}
```

**优点**：
- ✅ 可以利用倒排索引的有序性
- ✅ 避免全表扫描和排序
- ✅ 性能提升显著（单字段排序场景）

**缺点**：
- ❌ 需要修改 SortLimitOptimizer 的调用方式
- ❌ 只适用于单字段排序

**适用场景**：
- 单字段排序 + LIMIT
- 排序字段有倒排索引

### 方案 3：实现 IndexReader::scan_ordered()（未来优化）

为 IndexReader trait 添加有序扫描接口：

```rust
pub trait IndexReader: Send + Sync + 'static {
    // ... 现有方法 ...
    
    /// 有序扫描：按照索引的顺序返回 doc_ids
    ///
    /// # Arguments
    /// * `ascending` - 是否升序
    /// * `limit` - 最多返回多少个 doc_ids
    /// * `filter` - 可选的过滤条件（只返回满足条件的 doc_ids）
    ///
    /// # Returns
    /// 有序的 doc_ids 列表
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

**实现示例（B-Tree 索引）**：

```rust
impl IndexReader for BTreeIndexReader {
    fn scan_ordered(
        &self,
        ascending: bool,
        limit: Option<usize>,
        filter: Option<&RoaringBitmap>,
    ) -> Option<Vec<u32>> {
        let mut result = Vec::new();
        let limit_val = limit.unwrap_or(usize::MAX);
        
        // 按照 B-Tree 的顺序遍历
        let iter = if ascending {
            self.tree.iter()  // 升序
        } else {
            self.tree.iter().rev()  // 降序
        };
        
        for (_key, doc_ids) in iter {
            for doc_id in doc_ids.iter() {
                // 应用 filter
                if let Some(filter_bitmap) = filter {
                    if !filter_bitmap.contains(doc_id) {
                        continue;
                    }
                }
                
                result.push(doc_id);
                
                if result.len() >= limit_val {
                    return Some(result);
                }
            }
        }
        
        Some(result)
    }
}
```

**优点**：
- ✅ 最优性能（直接从索引读取有序数据）
- ✅ 避免全表扫描
- ✅ 避免排序

**缺点**：
- ❌ 需要为每种索引类型实现
- ❌ 实现复杂度较高

## 推荐实现路径

### 阶段 1：当前实现（已完成）

- ✅ `SortLimitOptimizer` 在 DistributedExecutor 层处理
- ✅ 使用 TOP-K 合并算法
- ✅ 适用于所有场景

**性能**：
- 每个 Partition 需要全表扫描 + 排序
- 协调节点做 TOP-K 合并

### 阶段 2：添加 sort hints 支持（短期）

1. 扩展 `PartitionTableProvider` 支持 sort hints
2. 修改 `SortLimitOptimizer` 传递 sort hints
3. `SegmentScanner` 根据 sort hints 选择策略

**性能提升**：
- 单字段排序场景：~10x（避免排序）
- 多字段排序场景：无提升（仍然需要排序）

### 阶段 3：实现 IndexReader::scan_ordered()（长期）

1. 为 IndexReader trait 添加 `scan_ordered()` 方法
2. 为 B-Tree 索引实现有序扫描
3. 为倒排索引实现有序扫描（如果支持）

**性能提升**：
- 单字段排序 + LIMIT 场景：~100x（直接从索引读取）
- 避免全表扫描和排序

## 当前实现状态

### 已完成

- ✅ `SegmentScanner::create_optimized_plan_with_sort()` - 支持传递 sort hints
- ✅ `ScanStrategy::IndexOrderedScan` - 新的扫描策略
- ✅ `choose_strategy_with_sort()` - 考虑 ORDER BY 的策略选择
- ✅ `create_index_ordered_scan_plan()` - 有序扫描计划（占位实现）

### 待完成

- ⏳ 修改 `SortLimitOptimizer` 传递 sort hints
- ⏳ 实现 `IndexReader::scan_ordered()`
- ⏳ 性能测试和调优

## 使用示例

### 场景 1：单字段排序 + LIMIT（最优）

```sql
SELECT * FROM users WHERE age > 20 ORDER BY age LIMIT 10
```

**执行流程**：
```
DistributedExecutor
    ↓ 识别为 SortLimit 查询
SortLimitOptimizer
    ↓ 传递 sort_hints=[(age, true)]
PartitionTableProvider
    ↓ 传递给 SegmentScanner
SegmentScanner
    ↓ 选择 IndexOrderedScan 策略
    ↓ 使用倒排索引有序扫描
    ↓ 只读取前 10 条数据
ExecutionPlan
```

**性能**：
- 避免全表扫描
- 避免排序
- 只读取需要的数据

### 场景 2：多字段排序 + LIMIT

```sql
SELECT * FROM users WHERE age > 20 ORDER BY age, name LIMIT 10
```

**执行流程**：
```
DistributedExecutor
    ↓ 识别为 SortLimit 查询
SortLimitOptimizer
    ↓ 传递 sort_hints=[(age, true), (name, true)]
PartitionTableProvider
    ↓ 传递给 SegmentScanner
SegmentScanner
    ↓ 多字段排序，无法使用 IndexOrderedScan
    ↓ 选择 IndexScan 或 FullScan
    ↓ 返回数据给 DataFusion
DataFusion
    ↓ 应用 SortExec 排序
    ↓ 应用 LimitExec
ExecutionPlan
```

**性能**：
- 仍然需要排序
- 但可以利用索引过滤

### 场景 3：只有 LIMIT，没有 ORDER BY

```sql
SELECT * FROM users LIMIT 10
```

**执行流程**：
```
DistributedExecutor
    ↓ 识别为 Simple 查询
PartitionTableProvider
    ↓ 传递 limit=10
SegmentScanner
    ↓ 选择 EarlyLimit 策略
    ↓ 只读取前 10 条数据
ExecutionPlan
```

**性能**：
- 提前终止扫描
- 最快的策略

## 性能对比

| 场景 | 当前实现 | 优化后（sort hints） | 优化后（scan_ordered） |
|------|---------|---------------------|----------------------|
| 单字段排序 + LIMIT | 全表扫描 + 排序 | 索引扫描 + 排序 | 索引有序扫描 |
| 多字段排序 + LIMIT | 全表扫描 + 排序 | 索引扫描 + 排序 | 索引扫描 + 排序 |
| 只有 LIMIT | 全表扫描 | 提前终止 | 提前终止 |
| 性能提升 | 1x（基准） | ~10x | ~100x |

## 总结

1. ✅ **当前实现**：在 DistributedExecutor 层处理，适用于所有场景
2. 🚀 **短期优化**：添加 sort hints 支持，单字段排序场景提升 ~10x
3. 🚀🚀 **长期优化**：实现 `scan_ordered()`，单字段排序场景提升 ~100x

通过分阶段实现，我们可以在不破坏现有架构的前提下，逐步提升 ORDER BY 查询的性能！
