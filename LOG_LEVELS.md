# CalmCore 日志级别说明

## 日志级别

CalmCore 使用 Rust 的 `log` crate,支持以下日志级别:

- **ERROR**: 错误信息,必须关注
- **WARN**: 警告信息,可能有问题但不影响运行
- **INFO**: 重要的业务信息(默认级别)
- **DEBUG**: 详细的调试信息
- **TRACE**: 非常详细的追踪信息

## 配置日志级别

### 方法 1: 环境变量 (推荐)

启动服务时设置 `RUST_LOG` 环境变量:

```bash
# 只显示 INFO 及以上级别 (默认,输出较少)
RUST_LOG=info cargo run --example mysql_server

# 显示所有 DEBUG 级别日志 (输出详细)
RUST_LOG=debug cargo run --example mysql_server

# 只显示特定模块的 DEBUG 日志
RUST_LOG=calm::compute::segment_scanner=debug cargo run --example mysql_server

# 组合配置
RUST_LOG=info,calm::compute=debug cargo run --example mysql_server
```

### 方法 2: 在代码中设置

在 `main.rs` 或 `lib.rs` 中:

```rust
env_logger::Builder::from_default_env()
    .filter_level(log::LevelFilter::Info)  // 默认 INFO
    .filter_module("calm::compute", log::LevelFilter::Debug)  // 特定模块 DEBUG
    .init();
```

## 当前日志分类

### INFO 级别 (默认显示)

这些信息对于理解查询执行很重要:

- **查询入口**: SQL 语句、查询类型
- **查询计划**: `[SegmentScanner::create_plan]` 过滤器、投影、排序、LIMIT
- **命中统计**: `[SegmentScanner]` 命中率、命中数量
- **优化决策**: 
  - `[SegmentScanner] Using index ordered scan` - 使用了索引有序扫描
- **重要警告**: 批次未找到、索引问题等

### DEBUG 级别 (需要开启才显示)

这些信息用于深入调试:

- **过滤器处理**: `[apply_filters]` 每个过滤器的处理细节
- **FilterExec 创建**: `[wrap_with_filter]` DataFusion FilterExec 的创建过程
- **数据读取**: `[read_docs]` 批次读取、合并的详细时间
- **有序扫描**: `[OrderedScan]` 索引扫描返回的文档数
- **流处理**: `[SegmentStream]` 分块处理的详细信息
- **跳过优化**: `[SegmentScanner] Skipping ordered scan` - 为什么不使用有序扫描

## 使用建议

### 日常开发

```bash
# 使用 INFO 级别,输出简洁
RUST_LOG=info cargo run --example mysql_server
```

### 调试查询性能问题

```bash
# 开启 DEBUG,查看详细的执行过程
RUST_LOG=debug cargo run --example mysql_server
```

### 调试特定模块

```bash
# 只看计算引擎的详细日志
RUST_LOG=info,calm::compute=debug cargo run --example mysql_server

# 只看 segment_scanner 的详细日志
RUST_LOG=info,calm::compute::segment_scanner=debug cargo run --example mysql_server
```

### 生产环境

```bash
# 只看 WARN 及以上,最少输出
RUST_LOG=warn cargo run --example mysql_server
```

## 示例对比

### INFO 级别输出 (简洁)

```
🔍 [SegmentScanner::create_plan] filters=[...], projection=Some([0, 1, 2]), limit=Some(10)
🎯 [SegmentScanner] hit=1000/1000000 (0.1%), limit=Some(10)
🚀 [SegmentScanner] Using index ordered scan for ORDER BY id ASC
```

### DEBUG 级别输出 (详细)

```
🔍 [SegmentScanner::create_plan] filters=[...], projection=Some([0, 1, 2]), limit=Some(10)
🔍 [apply_filters] Processing 2 filters, valid_docs=1000000
✓ [apply_filters] Filter handled via index: BinaryExpr { ... }, bitmap_size=1000
📋 [apply_filters] Has supported filter with 1000 results, returning bitmap
✅ [apply_filters] Final result: bitmap=Some, unsupported_filters=0
🎯 [SegmentScanner] hit=1000/1000000 (0.1%), limit=Some(10)
🎯 [OrderedScan] Field 'id' returned 10 docs (limit=10)
🚀 [SegmentScanner] Using index ordered scan for ORDER BY id ASC
      ⏱️  [read_docs] batch_lookup: 123µs
      ⏱️  [read_docs] batch_read (1 RowGroups, 3 cols): 456µs
      ⏱️  [read_docs] merge & extract: 78µs
      ⏱️  [read_docs] TOTAL read_docs_by_ids_with_projection: 657µs
```

## 修改历史

- 2025-11-25: 将大量详细日志从 INFO 改为 DEBUG,减少默认输出
  - `apply_filters` 的详细过滤过程
  - `wrap_with_filter` 的 FilterExec 创建细节
  - `read_docs` 的时间统计
  - `OrderedScan` 的返回结果
  - `SegmentStream` 的分块处理
