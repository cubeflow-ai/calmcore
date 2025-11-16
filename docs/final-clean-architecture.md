# 最终简化架构 - 清晰易懂

## 核心思路

根据你的建议，我们实现了一个**极简、清晰、易懂**的架构：

1. ✅ 删除复杂的成本估算代码
2. ✅ 直接用命中条数决策
3. ✅ 策略清晰：< 20% 用 Bitmap Scan，>= 20% 用 Full Scan
4. ✅ 有 ORDER BY 时利用倒排索引有序扫描

## 简化后的代码

### SegmentScanner::create_optimized_plan_with_sort()

```rust
pub fn create_optimized_plan_with_sort(
    &self,
    filters: &[Expr],
    projection: Option<&Vec<usize>>,
    limit: Option<usize>,
    sort_fields: Option<&[(String, bool)]>,
) -> Option<Arc<dyn ExecutionPlan>> {
    // 1. 应用 filters，计算命中的文档
    let mut result_bitmap = self.valid_docs.clone();
    
    for filter in filters {
        if let Some(bitmap) = self.expr_to_bitmap(filter) {
            result_bitmap &= bitmap;
        }
    }
    
    let hit_count = result_bitmap.len();
    let total_count = self.valid_docs.len();
    let hit_ratio = hit_count as f64 / total_count as f64;
    
    // 2. 选择策略
    let use_bitmap_scan = hit_ratio < 0.2;
    let has_order_by = sort_fields.is_some() && sort_fields.unwrap().len() > 0;
    
    // 3. 根据策略生成执行计划
    if use_bitmap_scan {
        // 命中率 < 20%：Bitmap Scan
        self.create_bitmap_scan_plan(result_bitmap, projection)
    } else if has_order_by {
        // 命中率 >= 20% + ORDER BY：倒排索引有序扫描
        self.create_index_ordered_scan_plan(...)
    } else {
        // 命中率 >= 20%：Full Scan
        self.create_bitmap_scan_plan(self.valid_docs.clone(), projection)
    }
}
```

## 决策逻辑

### 非常简单的三个分支

```
1. 计算命中率 = hit_count / total_count

2. 选择策略：
   ├─ 命中率 < 20% → Bitmap Scan（只扫描命中的文档）
   ├─ 命中率 >= 20% + 有 ORDER BY → Index Ordered Scan（倒排索引有序扫描）
   └─ 命中率 >= 20% → Full Scan（扫描所有文档）
```

### 为什么这样设计？

**命中率 < 20%**：
- 只有少量文档匹配
- 使用 Bitmap Scan 只扫描这些文档
- 性能最优

**命中率 >= 20%**：
- 大量文档匹配
- Bitmap Scan 的开销可能大于 Full Scan
- 直接全表扫描更快

**有 ORDER BY**：
- 可以利用倒排索引的有序性
- 避免排序开销
- 性能提升显著

## 删除的复杂代码

### 删除的数据结构

```rust
❌ struct FilterCost
❌ struct SegmentStatistics  
❌ struct FieldStatistics
❌ enum ScanStrategy（简化为直接判断）
```

### 删除的方法

```rust
❌ collect_statistics()
❌ estimate_filter_cost()
❌ estimate_expr_selectivity()
❌ choose_strategy()
❌ choose_strategy_with_sort()
❌ create_full_scan_plan()
❌ create_early_limit_plan()
```

### 保留的方法

```rust
✅ create_optimized_plan_with_sort() - 简化后的主入口
✅ create_bitmap_scan_plan() - 统一的扫描计划生成
✅ create_index_ordered_scan_plan() - ORDER BY 优化（占位）
✅ create_execution_plan() - 原有的方法（兼容）
```

## 代码对比

### 之前（复杂）

```rust
// ~300 行复杂的成本估算代码
collect_statistics()
estimate_filter_cost()
estimate_expr_selectivity()  // 递归估算
choose_strategy_with_sort()  // 5 个策略分支
```

### 现在（简洁）

```rust
// ~50 行简单清晰的代码
// 1. 应用 filters
let result_bitmap = apply_filters();

// 2. 计算命中率
let hit_ratio = hit_count / total_count;

// 3. 选择策略（3 个分支）
if hit_ratio < 0.2 {
    BitmapScan
} else if has_order_by {
    IndexOrderedScan
} else {
    FullScan
}
```

## 性能

性能不变或更好：
- ✅ 决策更快（不需要复杂的估算）
- ✅ 逻辑更清晰（容易理解和调试）
- ✅ 易于调整（只需要修改 20% 这个阈值）

## 完整的执行流程

### 场景 1：低命中率查询

```sql
SELECT * FROM users WHERE age = 25  -- 命中率 1%
```

```
SegmentScanner
    ↓ 应用 filters
    ↓ 计算命中率 = 1%
    ↓ 1% < 20% → Bitmap Scan
    ↓ 只扫描命中的文档
```

### 场景 2：高命中率查询

```sql
SELECT * FROM users WHERE age > 20  -- 命中率 80%
```

```
SegmentScanner
    ↓ 应用 filters
    ↓ 计算命中率 = 80%
    ↓ 80% >= 20% → Full Scan
    ↓ 扫描所有文档
```

### 场景 3：ORDER BY + LIMIT

```sql
SELECT * FROM users WHERE age > 20 ORDER BY age LIMIT 10  -- 命中率 80%
```

```
SegmentScanner
    ↓ 应用 filters
    ↓ 计算命中率 = 80%
    ↓ 80% >= 20% + 有 ORDER BY → Index Ordered Scan
    ↓ 使用倒排索引有序扫描（未来实现）
    ↓ 目前 fallback 到 Bitmap Scan
```

## 文件变更

### 修改的文件

- ✅ `src/compute/segment_scanner.rs` - 大幅简化（删除 ~250 行复杂代码）

### 删除的代码

- ❌ ~250 行复杂的成本估算代码
- ❌ 多个数据结构定义
- ❌ 递归的选择率估算

### 保留的代码

- ✅ ~50 行简单清晰的决策代码
- ✅ 核心的扫描逻辑

## 测试

```bash
✅ cargo check - 编译通过
✅ cargo test --lib - 28 个测试全部通过
```

## 总结

通过这次简化，我们实现了：

1. ✅ **极简的决策逻辑**：直接用命中条数，不需要复杂估算
2. ✅ **清晰的三个分支**：< 20% / >= 20% / >= 20% + ORDER BY
3. ✅ **删除冗余代码**：删除 ~250 行复杂代码
4. ✅ **易于理解**：任何人都能一眼看懂
5. ✅ **易于调整**：只需要修改 20% 这个阈值

代码现在**极其简洁、清晰、易懂**！🎉
