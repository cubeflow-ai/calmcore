# 查询优化快速参考

## 核心概念

### 三种扫描策略

| 策略 | 适用场景 | 性能特点 |
|------|---------|---------|
| **IndexScan** | 选择率 < 20%，有索引 | 最快（低选择率） |
| **FullScan** | 选择率 >= 20% 或无索引 | 最快（高选择率） |
| **EarlyLimit** | 只有 LIMIT，无 filter | 最快（LIMIT 场景） |

### 选择率估算

```rust
// 等值查询
age = 25  →  selectivity = 1 / cardinality

// Range 查询
age > 25  →  selectivity = 0.3  (假设 30%)

// AND 逻辑
A AND B  →  selectivity = sel(A) * sel(B)

// OR 逻辑
A OR B  →  selectivity = min(sel(A) + sel(B), 1.0)
```

## 代码位置

### 核心文件

```
src/compute/
├── partition_table_provider.rs  # Partition 级别协调
├── segment_scanner.rs           # Segment 级别优化（核心）
└── executor/
    └── distributed.rs           # 分布式执行器（高层路由）
```

### 关键方法

```rust
// PartitionTableProvider
fn supports_filters_pushdown()  // 返回 Unsupported
async fn scan()                 // 传递完整上下文

// SegmentScanner
fn create_optimized_plan()      // 主入口
fn collect_statistics()         // 收集统计信息
fn estimate_filter_cost()       // 估算成本
fn choose_strategy()            // 选择策略
```

## 如何调试

### 查看策略选择

```bash
# 启动服务时查看日志
RUST_LOG=info cargo run

# 查找策略日志
grep "Strategy" logs/app.log
```

### 日志示例

```
🎯 [Strategy] IndexScan: low selectivity (5.00% < 20%) with index
🎯 [SegmentScanner] Strategy=IndexScan, selectivity=5.00%, can_use_index=true, limit=None
```

## 如何扩展

### 添加新的扫描策略

```rust
// 1. 在 ScanStrategy 枚举中添加
enum ScanStrategy {
    IndexScan,
    FullScan,
    EarlyLimit,
    MyNewStrategy,  // ← 新策略
}

// 2. 在 choose_strategy 中添加决策逻辑
fn choose_strategy(...) -> ScanStrategy {
    if my_condition {
        return ScanStrategy::MyNewStrategy;
    }
    // ...
}

// 3. 在 create_optimized_plan 中添加实现
match strategy {
    ScanStrategy::MyNewStrategy => {
        self.create_my_new_strategy_plan(...)
    }
    // ...
}
```

### 添加新的统计信息

```rust
// 1. 扩展 FieldStatistics
struct FieldStatistics {
    cardinality: usize,
    has_index: bool,
    my_new_stat: MyType,  // ← 新统计信息
}

// 2. 在 collect_statistics 中收集
fn collect_statistics(&self) -> SegmentStatistics {
    // ...
    my_new_stat: self.calculate_my_new_stat(),
}

// 3. 在 estimate_expr_selectivity 中使用
fn estimate_expr_selectivity(...) -> (f64, bool, bool) {
    // 使用 field_stat.my_new_stat 做更精确的估算
}
```

### 调整阈值

```rust
// 在 choose_strategy 中修改
fn choose_strategy(...) -> ScanStrategy {
    // 修改这里的阈值
    if cost.total_selectivity >= 0.2 {  // ← 从 20% 改为其他值
        return ScanStrategy::FullScan;
    }
    // ...
}
```

## 性能调优

### 场景 1：大部分查询选择率低

```rust
// 降低阈值，更多使用索引扫描
if cost.total_selectivity >= 0.1 {  // 从 20% 改为 10%
    return ScanStrategy::FullScan;
}
```

### 场景 2：大部分查询选择率高

```rust
// 提高阈值，更多使用全表扫描
if cost.total_selectivity >= 0.3 {  // 从 20% 改为 30%
    return ScanStrategy::FullScan;
}
```

### 场景 3：索引查询开销大

```rust
// 提高阈值，减少索引使用
if cost.total_selectivity >= 0.3 {
    return ScanStrategy::FullScan;
}
```

## 常见问题

### Q: 为什么 supports_filters_pushdown 返回 Unsupported？

A: 因为我们需要在 `scan()` 中看到完整的查询上下文（filters + limit + projection），才能做智能决策。返回 `Unsupported` 会让 DataFusion 把 filters 传给 `scan()`。

### Q: 选择率估算不准确怎么办？

A: 
1. 实现 `IndexReader::estimate_cardinality()` 提供精确的基数
2. 添加 min/max 统计信息
3. 添加 histogram 统计信息
4. 收集运行时统计，动态调整

### Q: 如何验证策略是否正确？

A: 查看日志：
```bash
grep "Strategy" logs/app.log
```

### Q: 性能没有提升怎么办？

A:
1. 检查日志，确认策略选择是否合理
2. 调整阈值（20% 可能不适合你的场景）
3. 检查索引是否正确创建
4. 检查统计信息是否准确

## 测试

### 单元测试

```bash
cargo test --lib
```

### 集成测试

```bash
# 启动服务
cargo run --release

# 测试不同查询
mysql> SELECT * FROM users WHERE age = 25;  -- 应该用 IndexScan
mysql> SELECT * FROM users WHERE age > 20;  -- 应该用 FullScan
mysql> SELECT * FROM users LIMIT 10;        -- 应该用 EarlyLimit
```

## 监控指标

### 关键指标

- **策略分布**：IndexScan / FullScan / EarlyLimit 的比例
- **平均选择率**：所有查询的平均选择率
- **查询延迟**：P50 / P95 / P99 延迟
- **索引命中率**：使用索引的查询比例

### 收集方法

```rust
// 在 choose_strategy 中添加
metrics::counter!("scan_strategy", "type" => format!("{:?}", strategy)).increment(1);
metrics::histogram!("selectivity", cost.total_selectivity);
```

## 相关文档

- [详细架构设计](./query-optimization-architecture.md)
- [实现总结](./implementation-summary.md)

## 联系方式

如有问题，请查看：
- GitHub Issues
- 内部文档
- 团队 Slack
