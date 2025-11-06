# 性能对比 Benchmark 总结

## 测试环境

- 数据量: 10,000,000 条记录
- Segment 数量: 100 个 (每个 100,000 条记录)
- 字段: id (I64), category (Keyword), value (I64), timestamp (I64)
- 所有字段都建立了索引

## 测试结果

# Calm Query Performance Benchmark Results

## Test Configuration

- **Date**: 2025-01-XX
- **System**: macOS (Release build)
- **Dataset**: 10,000,000 records
- **Storage**: Disk-based comparison
  - Calm: 100 frozen segments (inverted indexes)
  - DataFusion: Single Parquet file (173.51 MB)
- **Record Structure**:

  ```
  - id: i64
  - category: String (5 distinct values: A-E, 20% each)
  - value: i64 (range: 0-999, cyclic pattern)
  - timestamp: i64 (sequential)
  ```

## Disk-Based Benchmark Results

All times in milliseconds (ms).

| Query | Description | Calm (ms) | Native (ms) | Slowdown | Notes |
|-------|-------------|-----------|-------------|----------|-------|
| Q1 | `SELECT COUNT(*) FROM data` | 205.95 | 0.39 | **523.66x** | Full scan - DataFusion highly optimized |
| Q2 | `SELECT * FROM data WHERE category = 'A'` | 53.92 | 11.15 | **4.84x** | Index helps but still slower |
| Q3 | `SELECT * FROM data WHERE value >= 500` | 199.66 | 6.29 | **31.76x** | Range query - vectorization wins |
| Q4 | `SELECT * FROM data WHERE category = 'B' AND value < 300` | 97.23 | 17.02 | **5.71x** | Multi-condition filter |
| Q5 | `SELECT category, AVG(value) FROM data WHERE value > 100 GROUP BY category` | 6073.63 | 37.71 | **161.07x** | Aggregation - huge gap |
| Q6 | `SELECT * FROM data WHERE timestamp BETWEEN 1700000000 AND 1700001000` | 3049.49 | 2.26 | **1349.44x** | Small range - DataFusion predicate pushdown |
| Q7 | `SELECT * FROM data WHERE category = 'A' OR category = 'C'` | 110.83 | 19.04 | **5.82x** | Multi-value filter |

### Key Observations

1. **Full Scans (Q1, Q3)**: DataFusion's vectorized execution is 30-500x faster
2. **Selective Filters (Q2, Q7)**: Calm's inverted indexes reduce gap to 5-6x
3. **Small Range Query (Q6)**: DataFusion's Parquet predicate pushdown is extremely efficient (1349x faster!)
4. **Aggregations (Q5)**: DataFusion's columnar processing dominates (161x faster)

### Previous Memory-Based Results (For Reference)

When both systems used in-memory data (MemTable vs Memory segments):

| Query | Calm (ms) | Native (ms) | Slowdown |
|-------|-----------|-------------|----------|
| Q1 | 207.15 | 0.50 | 413.46x |
| Q2 | 42.69 | 11.18 | 3.82x |
| Q3 | 199.30 | 5.87 | 33.97x |
| Q4 | 95.13 | 16.34 | 5.82x |
| Q5 | 6049.97 | 37.44 | 161.61x |
| Q6 | 3073.21 | 2.11 | 1458.14x |
| Q7 | 110.01 | 18.88 | 5.83x |

**Conclusion**: Disk-based results are very similar to memory-based, suggesting both systems are I/O efficient. The performance gap is primarily due to query processing architecture rather than storage layer.

## 性能瓶颈分析

### 1. 数据分片导致的 Union 开销

- **现状**: 100 万条记录自动分成 100 个 segments
- **问题**: 创建 100 个 SegmentExec plan,然后用 PartitionUnionExec 合并
- **影响**: Union 操作本身有固定开销,并且难以利用向量化

### 2. 逐行数据处理

- **现状**: execute() 方法逐行读取,每行创建一个单行 RecordBatch
- **问题**:
  - 大量小 RecordBatch 创建/销毁开销
  - 无法利用 Arrow 的向量化执行
  - CPU cache 命中率低

### 3. RowDataStore 的 floor() 查找

- **现状**: 每个 doc_id 都需要调用 `raw_data.floor()` 找到包含它的 batch
- **问题**: 重复查找同一个 batch (一个 batch 可能包含数千行)

### 4. 原生 DataFusion 的优势

- **连续内存**: 所有数据在 MemTable 中连续存储
- **向量化执行**: 使用 SIMD 指令并行处理
- **批处理**: 一次处理整个 RecordBatch (数千行)
- **零拷贝**: Arrow 格式支持零拷贝切片

## 优化建议

### 短期优化 (性能提升 5-10x)

1. **批量读取数据**

   ```rust
   // 当前: 逐行读取
   for doc_id in doc_ids {
       let batch = raw_data.floor(&doc_id);
       // 提取单行...
   }

   // 优化: 按 batch 分组
   let batches_map = group_doc_ids_by_batch(doc_ids);
   for (batch_id, row_indices) in batches_map {
       let batch = raw_data.get(&batch_id);
       // 一次提取多行,创建单个 RecordBatch
   }
   ```

2. **减少 RecordBatch 创建次数**
   - 当前: 每个匹配的 doc 创建一个单行 RecordBatch
   - 优化: 合并同一个 source batch 的多行到一个 RecordBatch

3. **缓存 floor() 查找结果**
   - 记住上一个查找的 batch
   - 检查当前 doc_id 是否在同一个 batch 中

### 中期优化 (性能提升 10-50x)

4. **向量化过滤**

   ```rust
   // 当前: 根据 bitmap 选择行
   for doc_id in matched_docs {
       let row = get_row(doc_id);
       results.push(row);
   }

   // 优化: 使用 Arrow compute kernels
   let mask = bitmap_to_boolean_array(matched_docs);
   let filtered = arrow::compute::filter(&batch, &mask)?;
   ```

5. **Segment 级别的并行执行**
   - 使用 Rayon 并行执行多个 Segments
   - 减少 Union 的串行开销

6. **智能 Segment 合并**
   - 合并小 segments 减少数量
   - 目标: 每个 segment 1-10MB

### 长期优化 (性能提升 50-100x)

7. **列式存储优化**
   - 压缩列存储减少 I/O
   - 使用 Parquet 格式持久化

8. **查询下推到 Segment 级别**
   - 在 Segment 内部使用向量化过滤
   - 避免逐行处理

9. **自适应执行**
   - 小结果集: 使用索引
   - 大结果集 (>10%): 全表扫描 + 向量化过滤

## 当前实现的优势

尽管性能不如原生 DataFusion,但 Calm 有以下优势:

1. **倒排索引**: 等值查询 O(1),范围查询 O(log n)
2. **实时写入**: 支持高吞吐写入的同时查询
3. **内存效率**: 使用 RoaringBitmap 压缩 docid 集合
4. **灵活架构**: Segment 分片便于分布式扩展

## 性能目标

### 选择性查询 (< 1% 数据)

- **目标**: 比 DataFusion 快 10-100x
- **关键**: 利用索引避免全表扫描

### 中等选择性 (1-10% 数据)

- **目标**: 与 DataFusion 相当或快 2-5x
- **关键**: 索引 + 向量化过滤结合

### 低选择性查询 (> 10% 数据)

- **目标**: 不超过 DataFusion 的 2x 慢
- **关键**: 全表扫描 + 向量化执行

## 下一步行动

1. ✅ 完成基本功能和 SQL 集成
2. ✅ 实现 projection 支持
3. ✅ 完成性能 benchmark
4. 🔄 实施短期优化 (批量读取)
5. ⏳ 实施中期优化 (向量化过滤)
6. ⏳ 实施长期优化 (列式存储)

## 结论

当前实现成功验证了架构可行性,SQL 查询功能完整。性能瓶颈主要在于:

1. 逐行处理 vs 批量向量化
2. Union 多个 segments 的开销

通过优化批量读取和向量化执行,预计可以将性能提升 50-100x,使选择性查询超越原生 DataFusion。
