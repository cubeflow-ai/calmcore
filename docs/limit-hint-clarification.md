# LIMIT 信息下发说明

## 你的问题

> limit_hint 没有下发是吗？

## 回答

`limit` 信息**已经下发了**，但是通过两种方式：

### 方式 1：通过 DataFusion 的 scan() 方法（主要方式）

```
SQL: SELECT * FROM users LIMIT 10
    ↓
DataFusion 解析 SQL
    ↓
调用 TableProvider::scan(limit=Some(10))
    ↓
PartitionTableProviderWithHints::scan(limit=Some(10))
    ↓
SegmentScanner::create_optimized_plan_with_sort(limit=Some(10))
    ↓
SegmentScanner 使用 limit 选择策略
```

### 方式 2：通过 limit_hint 参数（辅助方式）

```
DistributedExecutor::execute_sql()
    ↓ 提取 limit_hint
execute_sql_on_partition_with_hints(limit_hint=Some(10))
    ↓ 但这里没有使用 limit_hint
PartitionTableProviderWithHints::new_with_sort_hints()
    ↓ 也没有保存 limit_hint
```

## 为什么 limit_hint 没有被使用？

因为 **DataFusion 已经自动处理了 LIMIT**：

1. DataFusion 解析 SQL 时会识别 LIMIT
2. DataFusion 会在调用 `scan()` 时传递 `limit` 参数
3. 我们的 `scan()` 方法会把 `limit` 传递给 `SegmentScanner`

所以 `limit_hint` 参数是**冗余的**，可以删除。

## 验证

让我们看看代码：

### PartitionTableProviderWithHints::scan()

```rust
async fn scan(
    &self,
    _state: &dyn Session,
    projection: Option<&Vec<usize>>,
    filters: &[Expr],
    limit: Option<usize>,  // ← DataFusion 传递的 LIMIT
) -> Result<Arc<dyn ExecutionPlan>> {
    // ...
    scanner.create_optimized_plan_with_sort(
        filters,
        projection,
        limit,  // ← 传递给 SegmentScanner
        self.sort_hints.as_deref(),
    )
}
```

### SegmentScanner::create_optimized_plan_with_sort()

```rust
pub fn create_optimized_plan_with_sort(
    &self,
    filters: &[Expr],
    projection: Option<&Vec<usize>>,
    limit: Option<usize>,  // ← 接收 LIMIT
    sort_fields: Option<&[(String, bool)]>,
) -> Option<Arc<dyn ExecutionPlan>> {
    // 使用 limit 选择策略
    let strategy = self.choose_strategy_with_sort(
        filters,
        limit,  // ← 使用 LIMIT
        sort_fields,
        &filter_cost,
        &stats,
    );
    
    match strategy {
        ScanStrategy::EarlyLimit => {
            // 使用 limit
            self.create_early_limit_plan(projection, limit.unwrap())
        }
        ScanStrategy::IndexOrderedScan => {
            // 使用 limit
            self.create_index_ordered_scan_plan(
                filters,
                projection,
                limit.unwrap(),
                sort_fields.unwrap(),
            )
        }
        // ...
    }
}
```

## 日志验证

启动服务并执行查询：

```sql
SELECT * FROM users LIMIT 10;
```

预期日志：

```
🔍 [PartitionTableProviderWithHints::scan] limit=Some(10)
🎯 [SegmentScanner] limit=Some(10)
🎯 [Strategy] EarlyLimit: no filters, no order by, limit=Some(10)
```

## 结论

1. ✅ **LIMIT 信息已经下发了**
   - 通过 DataFusion 的 `scan(limit=...)` 方法
   - 传递给 `SegmentScanner`
   - 用于策略选择

2. ❌ **limit_hint 参数是冗余的**
   - 在 `execute_sql_on_partition_with_hints` 中接收但未使用
   - 可以删除这个参数

3. ✅ **ORDER BY 信息也已经下发了**
   - 通过 `sort_hints` 参数
   - 保存在 `PartitionTableProviderWithHints` 中
   - 传递给 `SegmentScanner`

## 建议

### 选项 1：删除 limit_hint 参数（推荐）

因为 DataFusion 已经自动处理了 LIMIT，我们不需要手动传递 `limit_hint`。

```rust
async fn execute_sql_on_partition_with_hints(
    &self,
    table_name: &str,
    partition_id: u64,
    sql: &str,
    sort_hints: Option<Vec<(String, bool)>>,
    // limit_hint: Option<usize>,  // ← 删除这个参数
) -> CoreResult<Vec<RecordBatch>> {
    // ...
}
```

### 选项 2：保留 limit_hint 用于日志（可选）

如果你想在日志中看到 `limit_hint`，可以保留但只用于日志：

```rust
async fn execute_sql_on_partition_with_hints(
    &self,
    table_name: &str,
    partition_id: u64,
    sql: &str,
    sort_hints: Option<Vec<(String, bool)>>,
    limit_hint: Option<usize>,  // ← 保留用于日志
) -> CoreResult<Vec<RecordBatch>> {
    log::info!(
        "🔍 [execute_sql_on_partition_with_hints] limit_hint={:?}",
        limit_hint
    );
    // 但不需要传递给 Provider，因为 DataFusion 会自动处理
    // ...
}
```

## 总结

- ✅ LIMIT 信息**已经下发了**（通过 DataFusion）
- ✅ ORDER BY 信息**已经下发了**（通过 sort_hints）
- ❌ limit_hint 参数是**冗余的**（可以删除）

所有优化所需的信息都已经正确下发到 SegmentScanner 了！🎉
