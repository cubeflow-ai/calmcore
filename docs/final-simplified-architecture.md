# 最终简化架构

## 根据你的建议完成的优化

### 1. 简化 QueryType

**之前**：
```rust
pub enum QueryType {
    Simple,           // 简单查询
    SortLimit(...),   // ORDER BY + LIMIT
    Aggregation,      // 聚合查询
    Complex,          // 复杂查询
}
```

**现在**：
```rust
pub enum QueryType {
    Aggregation,      // 聚合查询（需要特殊合并）
    SortLimit(...),   // ORDER BY + LIMIT（需要最终排序）
}
```

**原因**：
- ✅ 只需要区分需要特殊处理的查询类型
- ✅ 其他查询返回 `None`，由 SegmentScanner 自行优化
- ✅ 代码更简洁

### 2. 逻辑下推到 SegmentScanner

**DistributedExecutor 的职责**：
- 只负责协调多个 partition
- 提取 sort_hints 并下发
- 对于 ORDER BY + LIMIT，做最终排序

**SegmentScanner 的职责**：
- 根据完整上下文（filters + limit + order_by）做智能决策
- 选择最优的扫描策略
- 所有底层优化逻辑

### 3. 保留 sort_hints 和 limit info

**为什么需要保留**：
- ✅ 合并多个 partition 的结果时需要
- ✅ 做最终排序和 LIMIT 时需要

**传递路径**：
```
DistributedExecutor
    ↓ 提取 sort_hints
PartitionTableProviderWithHints
    ↓ 传递给 SegmentScanner
SegmentScanner
    ↓ 用于策略选择
    ↓ 返回部分结果
DistributedExecutor
    ↓ 使用 sort_hints 和 limit info
    ↓ 做最终排序（TopKMerger）
```

### 4. 使用 DataFusion API 做最终排序

**当前实现**：
```rust
fn apply_final_sort_limit(...) -> CoreResult<Vec<RecordBatch>> {
    // 使用 TopKMerger（已经实现好的）
    let merger = TopKMerger::new(
        info.sort_fields.clone(),
        info.limit,
        info.offset,
    );
    
    merger.merge(vec![batches])
}
```

**未来优化**：
可以考虑使用 DataFusion 的原生 API：
```rust
// 创建 DataFrame
let ctx = SessionContext::new();
let df = ctx.read_batches(batches)?;

// 应用 sort + limit
let sorted = df
    .sort(sort_exprs)?
    .limit(offset, Some(limit))?;

// 收集结果
sorted.collect().await
```

## 最终架构

```
SQL 查询
    ↓
DistributedExecutor::execute_sql()
    ↓
analyze_query(sql)
    ├─ Aggregation → 特殊处理（分布式聚合）
    ├─ SortLimit → 提取 sort_hints
    └─ None → 普通查询
    ↓
并行查询所有 partition
    ↓ 传递 sort_hints
PartitionTableProviderWithHints
    ↓ 传递给 SegmentScanner
SegmentScanner（智能决策）
    ├─ ORDER BY + LIMIT + 索引 → IndexOrderedScan
    ├─ 只有 LIMIT → EarlyLimit
    ├─ 选择率低 → IndexScan
    └─ 选择率高 → FullScan
    ↓
返回部分结果
    ↓
DistributedExecutor 合并结果
    ├─ 如果有 ORDER BY → apply_final_sort_limit()
    └─ 否则 → 直接返回
```

## 关键改动

### 1. QueryType 简化

```rust
// plan_analyzer.rs

pub enum QueryType {
    Aggregation,
    SortLimit(SortLimitInfo),
}

pub fn analyze_query(sql: &str) -> Option<QueryPlan> {
    // 只识别需要特殊处理的查询
    if is_aggregation_query(&sql_lower) {
        return Some(QueryPlan { query_type: Aggregation, ... });
    }
    
    if let Some(info) = analyze_sort_limit(&sql_lower) {
        return Some(QueryPlan { query_type: SortLimit(info), ... });
    }
    
    // 其他查询返回 None
    None
}
```

### 2. DistributedExecutor 简化

```rust
// distributed.rs

pub async fn execute_sql(&self, sql: &str) -> CoreResult<QueryResult> {
    let plan = analyze_query(sql);
    
    // 特殊处理：聚合查询
    if let Some(ref p) = plan {
        if matches!(p.query_type, QueryType::Aggregation) {
            return self.execute_aggregation_query(sql).await;
        }
    }
    
    // 提取 sort_hints
    let sort_hints = if let Some(ref p) = plan {
        match &p.query_type {
            QueryType::SortLimit(info) => Some(info.sort_fields.clone()),
            _ => None
        }
    } else {
        None
    };
    
    // 统一执行，传递 sort_hints
    for partition_id in 0..num_partitions {
        self.execute_sql_on_partition_with_hints(
            table_name,
            partition_id,
            sql,
            sort_hints.clone(),
        ).await
    }
    
    // 如果有 ORDER BY，做最终排序
    if let Some(ref p) = plan {
        if let QueryType::SortLimit(ref info) = p.query_type {
            return self.apply_final_sort_limit(all_batches, info);
        }
    }
    
    Ok(QueryResult { batches: all_batches })
}
```

### 3. 测试用例更新

```rust
#[test]
fn test_simple_query() {
    let sql = "SELECT * FROM users WHERE age > 20";
    let plan = analyze_query(sql);
    
    // 简单查询返回 None
    assert!(plan.is_none());
}
```

## 优势

### 1. 更简洁

- ✅ QueryType 只有 2 种（Aggregation, SortLimit）
- ✅ 其他查询不需要特殊标记
- ✅ 代码更清晰

### 2. 职责分明

- ✅ DistributedExecutor：协调 + 最终处理
- ✅ SegmentScanner：底层优化决策
- ✅ 各司其职

### 3. 易于扩展

- ✅ 添加新的优化策略只需修改 SegmentScanner
- ✅ 不需要修改 QueryType
- ✅ 不需要修改 DistributedExecutor 的主流程

## 性能

### 两个优化过程

#### 过程 1：Segment 级别优化（SegmentScanner）

```
每个 Segment 根据自己的情况选择最优策略：
- 有索引 + 低选择率 → IndexScan
- 有索引 + ORDER BY + LIMIT → IndexOrderedScan（未来）
- 只有 LIMIT → EarlyLimit
- 高选择率 → FullScan
```

#### 过程 2：Partition 级别合并（DistributedExecutor）

```
合并多个 partition 的结果：
- 如果有 ORDER BY → 使用 TopKMerger 排序 + LIMIT
- 否则 → 直接合并返回
```

### 性能特点

- ✅ Segment 级别：充分利用索引，减少扫描
- ✅ Partition 级别：只做必要的排序和合并
- ✅ 两层优化，性能最优

## 验证

```bash
✅ cargo check - 编译通过
✅ cargo test --lib - 28 个测试全部通过
```

## 总结

根据你的建议，我们完成了：

1. ✅ **简化 QueryType**：只保留 Aggregation 和 SortLimit
2. ✅ **逻辑下推**：底层优化交给 SegmentScanner
3. ✅ **保留 sort_hints**：用于最终合并
4. ✅ **使用 TopKMerger**：做最终排序（未来可以用 DataFusion API）

架构更简洁，职责更清晰，性能更好！🎉
