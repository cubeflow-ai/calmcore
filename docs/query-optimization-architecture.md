# 查询优化架构设计

## 概述

本文档描述了 CalmCore 的查询优化架构，该架构采用分层设计，在不同层次做不同的优化决策。

## 架构层次

```
┌─────────────────────────────────────────────────────────────┐
│                    DistributedExecutor                       │
│                    (跨 Partition 优化)                       │
│                                                              │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐     │
│  │ ORDER BY +   │  │ Aggregation  │  │ Simple Query │     │
│  │ LIMIT        │  │              │  │              │     │
│  │ (SortLimit   │  │ (Aggregation │  │ (Default)    │     │
│  │  Optimizer)  │  │  Merger)     │  │              │     │
│  └──────────────┘  └──────────────┘  └──────────────┘     │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│                 PartitionTableProvider                       │
│                 (Partition 级别协调)                         │
│                                                              │
│  • 返回 Unsupported (让 DataFusion 传递 filters)           │
│  • 将完整查询上下文传递给 SegmentScanner                    │
│  • Union 多个 Segment 的结果                                │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│                    SegmentScanner                            │
│                    (Segment 级别优化)                        │
│                                                              │
│  1. 收集统计信息 (doc_count, cardinality, has_index)       │
│  2. 估算 filter 成本 (selectivity, can_use_index)          │
│  3. 选择执行策略:                                           │
│     ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  │
│     │ Index Scan   │  │ Full Scan    │  │ Early Limit  │  │
│     │ (选择率<20%) │  │ (选择率≥20%) │  │ (只有LIMIT)  │  │
│     └──────────────┘  └──────────────┘  └──────────────┘  │
│  4. 生成 ExecutionPlan                                      │
└─────────────────────────────────────────────────────────────┘
```

## 核心设计原则

### 1. 分层优化

**高层（DistributedExecutor）**：
- 处理跨 Partition 的优化
- 识别查询类型（ORDER BY + LIMIT、Aggregation、Simple）
- 路由到专门的优化器

**中层（PartitionTableProvider）**：
- 协调单个 Partition 内的多个 Segment
- 传递完整的查询上下文（filters + limit + projection）
- 不做复杂决策，只做数据组织

**底层（SegmentScanner）**：
- 处理单个 Segment 的优化
- 根据统计信息做成本估算
- 选择最优的扫描策略

### 2. 上下文传递

```rust
// PartitionTableProvider 返回 Unsupported
fn supports_filters_pushdown(&self, filters: &[&Expr]) -> Result<Vec<TableProviderFilterPushDown>> {
    Ok(vec![TableProviderFilterPushDown::Unsupported; filters.len()])
}

// DataFusion 会把 filters 传给 scan()
async fn scan(
    &self,
    _state: &dyn Session,
    projection: Option<&Vec<usize>>,
    filters: &[Expr],           // ← filters 传进来了
    limit: Option<usize>,       // ← limit 也有了
) -> Result<Arc<dyn ExecutionPlan>> {
    // 传递给 SegmentScanner
    scanner.create_optimized_plan(filters, projection, limit)
}
```

### 3. 智能决策

SegmentScanner 根据以下信息做决策：

**输入**：
- `filters`: 过滤条件
- `projection`: 投影列
- `limit`: LIMIT 值
- `statistics`: Segment 统计信息

**决策流程**：
```rust
1. 收集统计信息
   - doc_count: 文档总数
   - valid_doc_count: 有效文档数
   - field_stats: 字段统计（cardinality, has_index）

2. 估算 filter 成本
   - total_selectivity: 总选择率（0.0 ~ 1.0）
   - can_use_index: 是否可以使用索引
   - has_unsupported_filter: 是否有不支持的 filter

3. 选择策略
   - 只有 LIMIT，没有 filter → EarlyLimit
   - 有不支持的 filter → FullScan
   - 选择率 >= 20% → FullScan
   - 有索引且选择率 < 20% → IndexScan
   - 没有索引 → FullScan
```

## 扫描策略详解

### 1. IndexScan（索引扫描）

**适用场景**：
- 选择率 < 20%
- 有可用的索引

**实现**：
```rust
// 使用倒排索引过滤
let mut result_bitmap = self.valid_docs.clone();
for filter in filters {
    if let Some(bitmap) = self.expr_to_bitmap(filter) {
        result_bitmap &= bitmap;
    }
}
```

**优点**：
- 只扫描匹配的文档
- 减少 I/O 和 CPU 开销

**缺点**：
- 索引查询有开销
- 选择率高时不如全表扫描

### 2. FullScan（全表扫描）

**适用场景**：
- 选择率 >= 20%
- 没有可用的索引
- 有不支持的 filter

**实现**：
```rust
// 直接返回所有有效文档
let result_bitmap = self.valid_docs.clone();
```

**优点**：
- 简单高效
- 选择率高时比索引扫描快

**缺点**：
- 需要扫描所有文档
- 选择率低时浪费资源

### 3. EarlyLimit（提前终止）

**适用场景**：
- 只有 LIMIT，没有复杂 filter
- 例如：`SELECT * FROM users LIMIT 10`

**实现**：
```rust
// 只取前 limit 个有效文档
let mut result_bitmap = RoaringBitmap::new();
let mut count = 0;
for doc_id in self.valid_docs.iter() {
    if count >= limit {
        break;
    }
    result_bitmap.insert(doc_id);
    count += 1;
}
```

**优点**：
- 最快的策略
- 只扫描需要的文档数

**缺点**：
- 只适用于没有 filter 的场景

## 选择率估算

### 等值查询

```rust
// age = 25
selectivity = 1.0 / cardinality
```

例如：
- `cardinality = 100` → `selectivity = 1%`
- `cardinality = 1000` → `selectivity = 0.1%`

### Range 查询

```rust
// age > 25 或 age BETWEEN 20 AND 30
selectivity = 0.3  // 假设 30%
```

**改进方向**：
- 使用 min/max 统计信息
- 使用 histogram 更精确估算

### AND/OR 逻辑

```rust
// AND: 选择率相乘
selectivity(A AND B) = selectivity(A) * selectivity(B)

// OR: 选择率相加（上限 1.0）
selectivity(A OR B) = min(selectivity(A) + selectivity(B), 1.0)
```

## 性能优化点

### 1. 统计信息收集

```rust
fn collect_statistics(&self) -> SegmentStatistics {
    let mut field_stats = StdHashMap::new();
    
    for (field_name, reader) in &self.index_readers {
        field_stats.insert(
            field_name.clone(),
            FieldStatistics {
                cardinality: reader.estimate_cardinality(),  // ← 关键
                has_index: true,
            },
        );
    }
    
    SegmentStatistics {
        doc_count: self.doc_count,
        valid_doc_count: self.valid_docs.len(),
        field_stats,
    }
}
```

**改进方向**：
- 缓存统计信息（避免每次查询都重新计算）
- 增加 min/max 统计
- 增加 histogram 统计

### 2. 成本估算

```rust
fn estimate_filter_cost(&self, filters: &[Expr], stats: &SegmentStatistics) -> FilterCost {
    let mut cost = FilterCost {
        total_selectivity: 1.0,
        can_use_index: false,
        has_unsupported_filter: false,
    };
    
    for filter in filters {
        let (selectivity, has_index, is_supported) = self.estimate_expr_selectivity(filter, stats);
        cost.total_selectivity *= selectivity;  // AND 语义
        
        if has_index {
            cost.can_use_index = true;
        }
        
        if !is_supported {
            cost.has_unsupported_filter = true;
        }
    }
    
    cost
}
```

### 3. 策略选择

```rust
fn choose_strategy(
    &self,
    filters: &[Expr],
    limit: Option<usize>,
    cost: &FilterCost,
    _stats: &SegmentStatistics,
) -> ScanStrategy {
    // 策略 1: 只有 LIMIT，没有 filter → Early Limit
    if filters.is_empty() && limit.is_some() {
        return ScanStrategy::EarlyLimit;
    }
    
    // 策略 2: 有不支持的 filter → Full Scan
    if cost.has_unsupported_filter {
        return ScanStrategy::FullScan;
    }
    
    // 策略 3: 选择率 >= 20% → Full Scan
    if cost.total_selectivity >= 0.2 {
        return ScanStrategy::FullScan;
    }
    
    // 策略 4: 有索引且选择率低 → Index Scan
    if cost.can_use_index {
        return ScanStrategy::IndexScan;
    }
    
    // 策略 5: 没有索引 → Full Scan
    ScanStrategy::FullScan
}
```

## 与现有优化的配合

### ORDER BY + LIMIT 优化

```rust
// 在 DistributedExecutor 层处理
match plan.query_type {
    QueryType::SortLimit(info) => {
        // 使用倒排索引优化
        let optimizer = SortLimitOptimizer::new(self.engine.clone());
        optimizer.execute(plan).await
    }
    _ => {
        // 走默认路径，SegmentScanner 会做底层优化
        self.execute_default_query(sql).await
    }
}
```

### 聚合查询优化

```rust
QueryType::Aggregation => {
    // 分布式聚合
    self.execute_aggregation_query(sql).await
}
```

## 日志和监控

```rust
log::info!(
    "🎯 [SegmentScanner] Strategy={:?}, selectivity={:.2}%, can_use_index={}, limit={:?}",
    strategy,
    filter_cost.total_selectivity * 100.0,
    filter_cost.can_use_index,
    limit
);
```

**输出示例**：
```
🎯 [SegmentScanner] Strategy=IndexScan, selectivity=5.00%, can_use_index=true, limit=None
🎯 [SegmentScanner] Strategy=FullScan, selectivity=45.00%, can_use_index=true, limit=None
🎯 [SegmentScanner] Strategy=EarlyLimit, selectivity=100.00%, can_use_index=false, limit=Some(10)
```

## 未来改进方向

### 1. 更精确的统计信息

- **Histogram**：更精确的选择率估算
- **Min/Max**：Range 查询优化
- **Bloom Filter**：快速排除不存在的值

### 2. 自适应优化

- **运行时统计**：收集实际查询的选择率
- **动态调整阈值**：根据实际情况调整 20% 的阈值
- **学习查询模式**：识别常见查询，预先优化

### 3. 并行优化

- **Segment 并行扫描**：多个 Segment 并行处理
- **Pipeline 执行**：流式处理，减少内存占用

### 4. 缓存优化

- **统计信息缓存**：避免重复计算
- **查询结果缓存**：缓存热点查询

## 总结

这个架构的核心优势：

1. ✅ **分层清晰**：高层做路由，底层做优化
2. ✅ **上下文完整**：SegmentScanner 可以看到完整的查询信息
3. ✅ **智能决策**：根据统计信息选择最优策略
4. ✅ **易于扩展**：可以轻松添加新的优化策略
5. ✅ **生产可用**：经过充分测试，性能可靠

通过这个架构，我们可以在不破坏现有设计的前提下，实现精细的查询优化！
