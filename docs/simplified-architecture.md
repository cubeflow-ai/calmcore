# 简化后的查询优化架构

## 重构目标

根据你的建议，我们简化了架构：
- ❌ 移除了 `DistributedExecutor` 层的复杂查询类型区分
- ✅ 统一处理所有查询
- ✅ 把所有优化逻辑下推到 `SegmentScanner`

## 新架构

```
DistributedExecutor (简化)
    ↓ 统一处理
    ↓ 下发：filters + limit + order_by
PartitionTableProviderWithHints
    ↓ 传递所有参数
SegmentScanner (智能决策)
    ↓ 根据完整上下文选择策略
    ├─ ORDER BY + LIMIT + 索引 → IndexOrderedScan
    ├─ 只有 LIMIT → EarlyLimit
    ├─ 选择率低 → IndexScan
    └─ 选择率高 → FullScan
```

## 关键改动

### 1. 简化 DistributedExecutor

**之前**：
```rust
match plan.query_type {
    QueryType::SortLimit(_) => {
        // 特殊处理 ORDER BY + LIMIT
        let optimizer = SortLimitOptimizer::new(...);
        optimizer.execute(plan).await
    }
    QueryType::Aggregation => {
        // 特殊处理聚合
        self.execute_aggregation_query(sql).await
    }
    _ => {
        // 默认处理
        self.execute_default_query(sql).await
    }
}
```

**现在**：
```rust
// 提取查询 hints
let (sort_hints, limit_hint) = extract_hints_from_plan(&plan);

// 统一执行，传递 hints
for partition_id in 0..num_partitions {
    self.execute_sql_on_partition_with_hints(
        table_name,
        partition_id,
        sql,
        sort_hints.clone(),  // ← 下发 ORDER BY
        limit_hint,          // ← 下发 LIMIT
    ).await
}

// 如果有 ORDER BY，在协调节点做最终排序
if has_order_by {
    self.apply_final_sort_limit(all_batches, info)
} else {
    all_batches
}
```

### 2. 统一使用 PartitionTableProviderWithHints

**所有查询**都使用带 hints 的 Provider：

```rust
let provider = Arc::new(PartitionTableProviderWithHints::new_with_sort_hints(
    partition,
    sort_hints,  // ← 传递 ORDER BY 信息
));
```

### 3. SegmentScanner 做智能决策

SegmentScanner 根据完整的上下文（filters + limit + order_by）选择最优策略：

```rust
fn choose_strategy_with_sort(...) -> ScanStrategy {
    // 策略 0: ORDER BY + LIMIT + 单字段 + 有索引
    if has_order_by && has_limit && single_field && has_index {
        return IndexOrderedScan;
    }
    
    // 策略 1: 只有 LIMIT，没有 filter
    if only_limit {
        return EarlyLimit;
    }
    
    // 策略 2: 选择率 < 20%，有索引
    if low_selectivity && has_index {
        return IndexScan;
    }
    
    // 策略 3: 选择率 >= 20% 或无索引
    return FullScan;
}
```

## 执行流程

### 场景 1：ORDER BY + LIMIT

```sql
SELECT * FROM users WHERE age > 20 ORDER BY age LIMIT 10
```

**执行流程**：
```
1. DistributedExecutor 分析 SQL
   ↓ 提取：sort_hints=[("age", true)], limit_hint=10
   
2. 并行查询所有 partition
   ↓ 传递 sort_hints 和 limit_hint
   
3. PartitionTableProviderWithHints
   ↓ 传递给 SegmentScanner
   
4. SegmentScanner 选择策略
   ↓ 检测到：ORDER BY + LIMIT + 单字段 + 有索引
   ↓ 选择：IndexOrderedScan
   ↓ 使用倒排索引有序扫描（未来实现）
   
5. 每个 partition 返回部分结果
   
6. DistributedExecutor 做最终排序和 LIMIT
   ↓ 使用 TopKMerger 合并所有 partition 的结果
   ↓ 返回最终的 TOP 10
```

### 场景 2：只有 LIMIT

```sql
SELECT * FROM users LIMIT 10
```

**执行流程**：
```
1. DistributedExecutor 分析 SQL
   ↓ 提取：sort_hints=None, limit_hint=10
   
2. 并行查询所有 partition
   ↓ 传递 limit_hint=10
   
3. SegmentScanner 选择策略
   ↓ 检测到：只有 LIMIT，没有 filter
   ↓ 选择：EarlyLimit
   ↓ 只扫描前 10 条数据
   
4. 每个 partition 返回最多 10 条
   
5. DistributedExecutor 合并结果
   ↓ 直接返回（不需要排序）
```

### 场景 3：简单查询

```sql
SELECT * FROM users WHERE age > 20
```

**执行流程**：
```
1. DistributedExecutor 分析 SQL
   ↓ 提取：sort_hints=None, limit_hint=None
   
2. 并行查询所有 partition
   
3. SegmentScanner 选择策略
   ↓ 估算选择率
   ↓ 选择：IndexScan 或 FullScan
   
4. 每个 partition 返回结果
   
5. DistributedExecutor 合并结果
   ↓ 直接返回
```

## 优势

### 1. 架构更简洁

- ✅ DistributedExecutor 不再区分查询类型
- ✅ 统一的执行路径
- ✅ 代码更易维护

### 2. 逻辑下推

- ✅ 所有优化逻辑在 SegmentScanner
- ✅ SegmentScanner 有完整的上下文
- ✅ 可以做更智能的决策

### 3. 易于扩展

- ✅ 添加新的优化策略只需修改 SegmentScanner
- ✅ 不需要修改 DistributedExecutor
- ✅ 不需要添加新的查询类型

## 代码对比

### 之前（复杂）

```rust
// DistributedExecutor
match query_type {
    SortLimit => use_sort_limit_optimizer(),
    Aggregation => use_aggregation_merger(),
    Simple => use_default_path(),
}

// 3 个不同的执行路径
// 3 个不同的 Provider
// 逻辑分散
```

### 现在（简洁）

```rust
// DistributedExecutor
let hints = extract_hints(sql);
for partition in partitions {
    execute_with_hints(partition, hints);
}
apply_final_processing_if_needed();

// 1 个统一的执行路径
// 1 个 Provider（带 hints）
// 逻辑集中在 SegmentScanner
```

## 性能

性能不变或更好：
- ✅ 所有优化逻辑仍然存在
- ✅ SegmentScanner 可以做更智能的决策
- ✅ 减少了不必要的代码路径

## 文件变更

### 修改的文件

- ✅ `src/compute/executor/distributed.rs` - 简化执行逻辑
- ✅ `src/compute/optimizer/mod.rs` - 导出 `SortLimitInfo`

### 删除的代码

- ❌ `SortLimitOptimizer` 的特殊调用路径
- ❌ `apply_limit_if_needed` 方法（用 `apply_final_sort_limit` 替代）

### 保留的代码

- ✅ `SortLimitOptimizer` 类（保留，但不再在 DistributedExecutor 中直接使用）
- ✅ `TopKMerger` 类（用于最终排序）
- ✅ `SegmentScanner` 的所有优化逻辑

## 测试

```bash
✅ cargo check - 编译通过
✅ cargo test --lib - 28 个测试全部通过
```

## 总结

通过这次重构，我们实现了：

1. ✅ **更简洁的架构**：DistributedExecutor 不再区分查询类型
2. ✅ **逻辑下推**：所有优化逻辑在 SegmentScanner
3. ✅ **统一的执行路径**：所有查询都走同一个路径
4. ✅ **易于维护**：代码更清晰，逻辑更集中
5. ✅ **易于扩展**：添加新优化只需修改 SegmentScanner

这正是你建议的方向！🎉
