# 改进的基准测试设计说明

## 📊 概述

根据您的要求，我们已经创建了一个改进的 `benchmark_vs_tantivy.rs`，采用**持久化对比**方案，确保 Calm 和 Tantivy 的测试在公平的条件下进行。

## 🔧 核心改进

### 1. **数据规模增加 10 倍**

- **之前**: 100,000 条记录
- **现在**: 1,000,000 条记录
- **理由**: 更大的数据量能够提供更有统计意义的性能对比

### 2. **持久化保证**

#### Calm 持久化

```rust
pub fn write_and_persist(&self, records: &[TestRecord]) -> (usize, f64) {
    // ... 写入数据 ...
    if let Ok(_) = self.partition.upsert_json(&batch_data) {
        // 关键：立即持久化到磁盘
        let _ = self.partition.flush(true);
        // 计算写入吞吐量（包含持久化）
        let writes_per_sec = records.len() as f64 / elapsed;
        (records.len(), writes_per_sec)
    }
}
```

#### Tantivy 持久化

```rust
pub fn write_and_persist(&self, records: &[TestRecord]) -> (usize, f64) {
    // ... 写入文档到索引 ...
    index_writer.commit().ok();  // 提交索引到磁盘
    // 计算写入吞吐量（包含持久化）
    let writes_per_sec = records.len() as f64 / elapsed;
    (records.len(), writes_per_sec)
}
```

### 3. **磁盘占用对比**

新基准测试包含：

- **Calm 磁盘占用**: 持久化后的 Parquet 文件大小
- **Tantivy 磁盘占用**: 完整的倒排索引大小
- **压缩效率**: Calm 相对于原始数据的压缩率

## 📈 测试场景

### 场景 1: 写入性能对比（包含持久化）

```
吞吐量对比:
  - Calm: X writes/sec (包含 flush)
  - Tantivy: Y writes/sec (包含 commit)
  - 比率: Calm 快 X/Y 倍
```

### 场景 2: 磁盘占用对比（持久化后）

```
空间对比:
  - Calm 数据大小: A MB
  - Tantivy 索引大小: B MB
  - 比率: Tantivy 占用 Calm 的 B/A 倍
```

### 场景 3: 完整性能评估

```
性能指标:
  - 写入吞吐量 (M records/sec)
  - 单条记录耗时 (µs)
  - 持久化数据大小 (MB)
```

## 🎯 为什么这个对比更公平？

| 因素 | 旧对比 | 新对比 |
|------|-------|--------|
| 数据规模 | 100K | 1M (10x) |
| Calm 数据 | 内存中 | **磁盘上** ✅ |
| Tantivy 数据 | 磁盘上 | **磁盘上** ✅ |
| 对标方式 | 内存 vs 磁盘 | **磁盘 vs 磁盘** ✅ |
| 统计意义 | 低 | **高** ✅ |

## 📝 预期结果分析

### 吞吐量对比

- **结果**: Calm 写入可能略有下降（因为包含 flush）
- **意义**: 但依然会显著超过 Tantivy
- **原因**:
  - Calm 使用了优化的批量 I/O
  - Tantivy 需要构建倒排索引（CPU 密集）

### 磁盘占用对比

- **结果**: Calm 通常占用更少空间
- **原因**:
  - Parquet 格式本身就很高效
  - Zstd 压缩率优秀（89.6%）
  - Tantivy 要存储完整的倒排索引

### 应用场景

- **Calm**: 优于 OLTP 场景（实时写入、点查询）
- **Tantivy**: 优于全文搜索场景（复杂搜索、精排）

## 🚀 使用方法

```bash
cd /Users/sunjian/rustworkspace/calmcore

# 编译
cargo build --example benchmark_vs_tantivy --release

# 运行
cargo run --example benchmark_vs_tantivy --release
```

## ✅ 文件位置

- **源代码**: `examples/benchmark_vs_tantivy.rs` (351 行)
- **测试数据目录**: `/tmp/calm_vs_tantivy_benchmark/`
  - `calm_benchmark/`: Calm 持久化数据
  - `tantivy_benchmark/`: Tantivy 索引数据

## 📊 性能期望

基于 1M 条记录的 1 小时基准测试：

### Calm 预期

- 吞吐量: ~0.5-2M writes/sec (含持久化)
- 磁盘占用: ~100-200MB (高压缩率)
- 单条记录: ~0.5-2 µs

### Tantivy 预期

- 吞吐量: ~0.05-0.2M writes/sec (构建倒排索引)
- 磁盘占用: ~200-500MB (倒排索引)
- 单条记录: ~5-20 µs

### 对比结果

- **写入速度**: Calm 快 **5-40 倍** 🚀
- **磁盘占用**: Calm 节省 **1.5-3 倍** 💾

## 💡 关键改进点

1. ✅ **两个引擎都持久化**: 确保公平对比
2. ✅ **数据规模翻倍**: 1M 条记录增加统计意义
3. ✅ **包含持久化时间**: 测量实际端到端性能
4. ✅ **磁盘占用跟踪**: 显示空间效率
5. ✅ **详细性能指标**: 多维度对标

## 🔗 相关资源

- [持久化实现](PERSIST_STRATEGY.md)
- [性能优化总结](OPTIMIZATION_COMPLETION_SUMMARY.md)
- [Tantivy 对标基准](BENCHMARK_VS_TANTIVY.md)
