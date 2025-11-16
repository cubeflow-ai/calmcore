# LIMIT 下推优化

## 优化日期
2024-11-16

## 问题背景

之前的实现中，即使查询有 `LIMIT 10`，SegmentScanner 也会读取所有匹配的文档，然后由 DataFusion 在上层应用 LIMIT。这导致了不必要的数据读取。

## 优化策略

### 何时可以下推 LIMIT？

只有在**没有 ORDER BY** 的情况下，才能在 Segment 层面应用 LIMIT：

```sql
-- ✅ 可以下推 LIMIT
SELECT * FROM table WHERE age > 18 LIMIT 10;

-- ❌ 不能下推 LIMIT（有 ORDER BY）
SELECT * FROM table WHERE age > 18 ORDER BY name LIMIT 10;
```

### 为什么有 ORDER BY 不能下推？

因为：
1. 每个 Segment 只能看到部分数据
2. 如果在 Segment 层面应用 LIMIT，可能会丢失全局排序后应该返回的数据
3. 必须先收集所有 Segment 的数据，全局排序后，再应用 LIMIT

## 实现细节

### 1. 在 `build_exec_plan` 中判断

```rust
// LIMIT 下推优化：如果没有 SORT，可以在 Segment 层面就应用 LIMIT
let pushdown_limit = if sort.is_none() { 
    if let Some(l) = limit {
        log::info!("🚀 [LIMIT Pushdown] Applying LIMIT {} at segment level (no sort)", l);
    }
    limit 
} else { 
    None 
};
```

### 2. 在 `SegmentStream` 中实现

添加了两个字段：
- `limit: Option<usize>` - 要应用的 LIMIT 值
- `rows_returned: usize` - 已经返回的行数

在 `generate_next_chunk` 中：
1. 检查是否已经返回足够的行
2. 只收集需要的 doc_ids（不超过 remaining_rows）
3. 更新 `rows_returned` 计数器

```rust
// LIMIT 下推：如果已经返回足够的行，停止生成
if let Some(limit) = self.limit {
    if self.rows_returned >= limit {
        return Ok(Vec::new());
    }
}

// 只收集需要的 doc_ids
let remaining_rows = self.limit.map(|l| l.saturating_sub(self.rows_returned));

while self.current_offset < self.doc_ids.len() {
    // LIMIT 优化：如果已经收集足够的行，停止
    if let Some(remaining) = remaining_rows {
        if collected_rows >= remaining {
            break;
        }
    }
    // ... 收集 doc_ids
}

// 更新已返回的行数
let total_rows: usize = result_batches.iter().map(|b| b.num_rows()).sum();
self.rows_returned += total_rows;
```

## 性能提升

### 场景 1：小 LIMIT，大结果集
```sql
SELECT * FROM table WHERE status = 'active' LIMIT 10;
-- 假设有 100万 条 active 记录
```

**优化前**：
- 读取 100万 条记录
- 在内存中处理 100万 条记录
- DataFusion 应用 LIMIT，只返回 10 条

**优化后**：
- 只读取 10 条记录（或稍多一点，取决于 chunk 边界）
- 节省了 99.999% 的数据读取和处理

### 场景 2：有 ORDER BY
```sql
SELECT * FROM table WHERE status = 'active' ORDER BY created_at LIMIT 10;
```

**行为**：
- 不应用 LIMIT 下推（因为有 ORDER BY）
- 读取所有匹配的记录
- 在上层排序后应用 LIMIT
- 这是正确的行为，保证了结果的正确性

## 测试验证

可以通过日志验证优化是否生效：

```
🚀 [LIMIT Pushdown] Applying LIMIT 10 at segment level (no sort)
🔍 [SegmentExec::execute] Starting streaming execution, total matched_docs=1000000, pushdown_limit=Some(10)
🔍 [SegmentStream::new] Total doc_ids=1000000, will process in chunks of 100 storage batches, limit=Some(10)
```

## 注意事项

1. **分布式环境**：在多个 Partition 的情况下，每个 Partition 都会应用 LIMIT，所以最终可能返回 `LIMIT × Partition数` 条记录，然后在 DistributedExecutor 层面再次应用 LIMIT
2. **Chunk 边界**：由于是按 chunk 处理，实际读取的数据可能略多于 LIMIT，但不会超过一个 chunk 的大小
3. **空投影**：对于 `COUNT(*)` 等空投影查询，LIMIT 下推同样有效

## 未来优化

1. **更精确的 LIMIT**：可以在 batch 级别应用 LIMIT，而不是 chunk 级别
2. **与索引结合**：如果有索引，可以直接从索引中读取前 N 个文档
3. **动态调整 chunk_size**：根据 LIMIT 值动态调整 chunk_size，避免读取过多数据
