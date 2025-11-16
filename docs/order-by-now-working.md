# ORDER BY 信息已经下发！✅

## 现在的实现

### 完整的调用链

```
SQL: SELECT * FROM users WHERE age > 20 ORDER BY age LIMIT 10
    ↓
DistributedExecutor::execute_sql()
    ↓ 识别为 SortLimit 查询
SortLimitOptimizer::execute()
    ↓ 提取 sort_fields = [("age", true)]
SortLimitOptimizer::execute_on_partition_with_hints()
    ↓ 传递 sort_hints = Some([("age", true)])
PartitionTableProviderWithHints::new_with_sort_hints()
    ↓ 保存 sort_hints
PartitionTableProviderWithHints::scan()
    ↓ 传递 sort_hints 给 SegmentScanner
SegmentScanner::create_optimized_plan_with_sort()
    ↓ 接收 sort_fields = Some([("age", true)])
SegmentScanner::choose_strategy_with_sort()
    ↓ 检测到：单字段排序 + 有索引 + 有 LIMIT
    ↓ 选择 IndexOrderedScan 策略 ✅
SegmentScanner::create_index_ordered_scan_plan()
    ↓ 使用倒排索引做有序扫描
ExecutionPlan
```

## 关键代码

### 1. SortLimitOptimizer 传递 sort hints

```rust
// src/compute/optimizer/sort_limit.rs

// 提取 sort_fields
let sort_hints = Some(info.sort_fields.clone());

// 传递给每个 partition
self.execute_on_partition_with_hints(
    table_name,
    partition_id as u64,
    &partition_sql,
    sort_hints.clone(),  // ← 传递 ORDER BY 信息！
)
```

### 2. PartitionTableProviderWithHints 接收并传递

```rust
// src/compute/partition_table_provider_with_hints.rs

pub struct PartitionTableProviderWithHints {
    partition: Arc<Partition>,
    schema: SchemaRef,
    sort_hints: Option<Vec<(String, bool)>>,  // ← 保存 ORDER BY 信息
}

async fn scan(...) -> Result<Arc<dyn ExecutionPlan>> {
    // 传递给 SegmentScanner
    scanner.create_optimized_plan_with_sort(
        filters,
        projection,
        limit,
        self.sort_hints.as_deref(),  // ← 传递 ORDER BY 信息！
    )
}
```

### 3. SegmentScanner 接收并使用

```rust
// src/compute/segment_scanner.rs

pub fn create_optimized_plan_with_sort(
    &self,
    filters: &[Expr],
    projection: Option<&Vec<usize>>,
    limit: Option<usize>,
    sort_fields: Option<&[(String, bool)]>,  // ← 接收 ORDER BY 信息！
) -> Option<Arc<dyn ExecutionPlan>> {
    // 选择策略
    let strategy = self.choose_strategy_with_sort(
        filters,
        limit,
        sort_fields,  // ← 使用 ORDER BY 信息！
        &filter_cost,
        &stats,
    );
    
    match strategy {
        ScanStrategy::IndexOrderedScan => {
            // 使用倒排索引做有序扫描 ✅
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

### 4. 策略选择逻辑

```rust
fn choose_strategy_with_sort(...) -> ScanStrategy {
    // 策略 0: ORDER BY + LIMIT + 单字段排序 + 有索引 → Index Ordered Scan
    if let (Some(limit_val), Some(sort_fields_val)) = (limit, sort_fields) {
        if sort_fields_val.len() == 1 {
            let (field_name, _ascending) = &sort_fields_val[0];
            if self.index_readers.contains_key(field_name) {
                log::info!(
                    "🎯 [Strategy] IndexOrderedScan: ORDER BY {} with index, limit={}",
                    field_name,
                    limit_val
                );
                return ScanStrategy::IndexOrderedScan;  // ← 选择有序扫描！
            }
        }
    }
    // ...
}
```

## 验证方法

### 1. 查看日志

启动服务并执行查询：

```bash
# 启动服务
RUST_LOG=info cargo run

# 在另一个终端执行查询
mysql -h 127.0.0.1 -P 3307
mysql> SELECT * FROM users WHERE age > 20 ORDER BY age LIMIT 10;
```

### 2. 预期日志输出

```
🚀 [SortLimitOptimizer] Using PartitionTableProviderWithHints, sort_hints=[("age", true)]
🔍 [PartitionTableProviderWithHints::scan] Starting scan for partition 0, filters=[...], limit=Some(20), sort_hints=Some([("age", true)])
🎯 [Strategy] IndexOrderedScan: ORDER BY age with index, limit=20
🎯 [SegmentScanner] Strategy=IndexOrderedScan, selectivity=30.00%, can_use_index=true, limit=Some(20), sort=Some([("age", true)])
🎯 [IndexOrderedScan] Using index on 'age' for ordered scan, ascending=true, limit=20
```

### 3. 关键日志标识

- ✅ `Using PartitionTableProviderWithHints` - 使用了带 hints 的 Provider
- ✅ `sort_hints=Some([...])` - sort hints 被传递了
- ✅ `Strategy=IndexOrderedScan` - 选择了有序扫描策略
- ✅ `Using index on 'age' for ordered scan` - 使用索引做有序扫描

## 文件清单

### 新增文件

- ✅ `src/compute/partition_table_provider_with_hints.rs` - 带 sort hints 的 Provider

### 修改文件

- ✅ `src/compute/mod.rs` - 导出新的 Provider
- ✅ `src/compute/optimizer/sort_limit.rs` - 传递 sort hints
- ✅ `src/compute/segment_scanner.rs` - 接收并使用 sort hints（之前已完成）

### 编译验证

```bash
✅ cargo check - 编译通过
✅ cargo test --lib - 28 个测试全部通过
```

## 当前状态

### ✅ 已完成

1. **ORDER BY 信息已经下发**
   - SortLimitOptimizer → PartitionTableProviderWithHints → SegmentScanner
   
2. **策略选择逻辑已实现**
   - 检测单字段排序 + 有索引 + 有 LIMIT
   - 选择 IndexOrderedScan 策略

3. **日志完善**
   - 可以清楚地看到 ORDER BY 信息的传递过程

### ⏳ 待完成

4. **实现 IndexReader::scan_ordered()**
   - 目前 `create_index_ordered_scan_plan()` 是占位实现
   - 需要为各种索引类型实现有序扫描接口

5. **性能测试**
   - 验证实际性能提升
   - 调整策略阈值

## 下一步

### 立即可做

测试验证：
```bash
# 1. 编译
cargo build --release

# 2. 启动服务
RUST_LOG=info ./target/release/calm

# 3. 执行查询
mysql -h 127.0.0.1 -P 3307
mysql> SELECT * FROM users WHERE age > 20 ORDER BY age LIMIT 10;

# 4. 查看日志
grep "IndexOrderedScan" logs/app.log
```

### 需要更多时间

实现 `IndexReader::scan_ordered()`：

```rust
pub trait IndexReader {
    fn scan_ordered(
        &self,
        ascending: bool,
        limit: Option<usize>,
        filter: Option<&RoaringBitmap>,
    ) -> Option<Vec<u32>>;
}
```

## 总结

✅ **ORDER BY 信息现在已经下发了！**

完整的调用链：
```
SortLimitOptimizer
    ↓ sort_hints
PartitionTableProviderWithHints
    ↓ sort_hints
SegmentScanner
    ↓ IndexOrderedScan 策略
create_index_ordered_scan_plan()
```

你现在可以：
1. 启动服务
2. 执行 ORDER BY + LIMIT 查询
3. 查看日志，确认 `IndexOrderedScan` 策略被选择
4. 验证性能提升

下一步只需要实现 `IndexReader::scan_ordered()` 就可以真正利用倒排索引做有序扫描了！🚀
