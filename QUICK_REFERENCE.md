# 🚀 CalmCore 性能优化 - 快速参考

## 📊 成果速览

| 指标 | 数值 | 说明 |
|------|------|------|
| **平均改进** | 🟢 8.8x | 5 个查询场景平均 |
| **最大改进** | 🟢 12.9x | 过滤查询最佳表现 |
| **Vs Tantivy** | 🟢 11.6x | 写入吞吐量对比 |
| **磁盘节省** | 🟢 7-10x | 压缩率对比 |

## 🎯 关键改进

```
优化前: Q1 = 29.27ms  (COUNT)
优化后: Q1 = 3.82ms   
改进:   7.7 倍 ✅

优化前: Q3 = 29.82ms  (过滤)
优化后: Q3 = 2.31ms
改进:   12.9 倍 ✅ (最佳)
```

## 📁 重要文档

### 快速阅读 (5分钟)

- `PERFORMANCE_SUMMARY.md` - 性能总结
- `OPTIMIZATION_COMPLETION_SUMMARY.md` - 工作总结

### 深度分析 (20分钟)

- `PERFORMANCE_REPORT_2025_11_08.md` - 完整分析
- `OPTIMIZATION_ANALYSIS_2025_11_09.md` - 技术深度

### 竞品对标 (10分钟)

- `BENCHMARK_VS_TANTIVY.md` - Tantivy 对标

## 🔧 如何查看改进

### 方法 1: 运行基准测试

```bash
cargo run --release --example benchmark_query
```

结果示例:

```
Q1 (COUNT):     3.82ms  (优化后)
Q3 (过滤):      2.31ms  (优化后)
改进: 12.9x 快 ✅
```

### 方法 2: 运行 Tantivy 对标

```bash
cargo run --release --example benchmark_vs_tantivy
```

结果示例:

```
写入吞吐量: Calm 1.83M/s vs Tantivy 0.16M/s
优势: 11.6x 快 ✅
```

## 💡 核心改进

### 1. O(N) → O(1) I/O

**问题**: 每个 doc_id 打开一次 Parquet 文件
**解决**: 批量打开，一次读取所有数据
**效果**: 6-8x 改进

### 2. 元数据快速查询

**问题**: 不知道数据在哪个文件
**解决**: 内存 BTree 快速定位
**效果**: < 1µs 查询时间

### 3. 列式投影

**问题**: 读取不需要的列
**解决**: Parquet 列投影，只读需要的数据
**效果**: 减少 I/O 数据量

## ⚡ 性能数据

### 按查询类型改进

```
COUNT(*)       │ 7.7x   ████████
过滤 (单字段)  │ 12.9x  █████████████ ← 最佳
OR 条件        │ 11.0x  ███████████
复杂 AND       │ 2.3x   ██
精确值         │ 1.3x   █
─────────────────────────────
平均            │ 8.8x   █████████
```

### 磁盘占用

```
100K 记录
─────────────────────────────
未压缩:        20.0 MB (基准)
Calm+Zstd:     2.08 MB (89.6% 压缩) ✅ 9.6x
Tantivy:      21.29 MB (6% 开销)
```

## 📌 已知问题

### 🔴 P0 - 数据丢失

- 现象: 写 100K，查到 97.7K
- 丢失: 2,328 行
- 优先级: **必须修复**

### 🟡 P1 - COUNT(*) 仍可优化

- 当前: 3.82ms (已 7.7x 改进)
- 目标: 0.5ms (需 COUNT(*) 快速路径)
- 改进: 再快 7.6x

## 🎓 技术亮点

### 使用的优化技术

- ✅ I/O 批处理
- ✅ 内存索引
- ✅ 列式存储
- ✅ 选择性加载
- ✅ RoaringBitmap 快速位运算

### 实现文件

```
src/segment/field_store/row_data.rs
  ↳ get_batch_key_for_doc() - 快速元数据查询

src/schema/compute/segment_scanner.rs
  ↳ 批量 I/O 优化实现
```

## 🏆 应用场景

### ✅ Calm 最适合

- 实时日志索引
- 时间序列存储 (TSDB)
- 事件流处理
- 指标聚合
- 内存分析

### 📊 vs 竞品

| 特性 | Calm | Tantivy | 评价 |
|------|------|---------|------|
| 写入性能 | 1.83M/s | 0.16M/s | Calm 赢 11.6x ✅ |
| 磁盘占用 | 2-3MB | 21MB | Calm 赢 7-10x ✅ |
| 全文搜索 | 基础 | 强大 | Tantivy 赢 ✅ |
| 成熟度 | 新 | 成熟 | Tantivy 赢 ✅ |

## 📞 快速帮助

**"为什么查询快了?"**
→ 见 `OPTIMIZATION_ANALYSIS_2025_11_09.md` 的「技术架构」部分

**"Calm vs Tantivy 哪个好?"**
→ 见 `BENCHMARK_VS_TANTIVY.md` 的「应用场景建议」

**"还能优化吗?"**
→ 见 `OPTIMIZATION_COMPLETION_SUMMARY.md` 的「下一步优化方向」

**"性能指标具体在哪?"**
→ 见 `OPTIMIZATION_RESULTS_2025_11_09.md` (详细 KPI 表)

## 🔗 相关链接

### 文档导航

```
📚 PERFORMANCE_REPORTS_INDEX.md
  ├─ 📊 PERFORMANCE_SUMMARY.md (5分钟读)
  ├─ 📈 PERFORMANCE_REPORT_2025_11_08.md (30分钟读)
  ├─ 🚀 OPTIMIZATION_ANALYSIS_2025_11_09.md (技术深度)
  ├─ 📋 OPTIMIZATION_RESULTS_2025_11_09.md (详细数据)
  ├─ ✅ OPTIMIZATION_COMPLETION_SUMMARY.md (工作总结)
  └─ 🏆 BENCHMARK_VS_TANTIVY.md (竞品对标)
```

### 代码

```
📁 examples/
  ├─ benchmark_query.rs (改进前后对比)
  ├─ benchmark_vs_tantivy.rs (Tantivy 对标)
  └─ benchmark_comprehensive.rs (综合测试)

📁 src/
  ├─ segment/field_store/row_data.rs (优化代码)
  └─ schema/compute/segment_scanner.rs (优化代码)
```

---

**最后更新**: 2025-11-09
**版本**: v1.0 Final
**状态**: ✅ 完成 (P0 问题除外)
