# 测试合并问题

## 问题描述

查询 `SELECT * FROM users ORDER BY id DESC LIMIT 1` 返回了 4 个 partition 的结果，而不是合并后的 1 行。

## 可能的原因

1. **没有走优化路径** - 查询没有被识别为 SortLimit 类型
2. **合并失败** - TopKMerger 没有正确合并多个 partition 的结果
3. **返回了多个 batch** - 每个 partition 返回一个 batch，没有合并成一个

## 测试步骤

```bash
# 1. 写入数据
./demo.sh write

# 2. 运行查询并查看日志
RUST_LOG=info cargo run --example demo --release -- query
```

## 预期日志

如果走了优化路径，应该看到：
```
INFO [DistributedExecutor] Query plan: SortLimit(...)
INFO [DistributedExecutor] Using optimized Sort+Limit path
INFO [SortLimitOptimizer] Merging results from 4 partitions
INFO [SortLimitOptimizer] Query completed, returning 1 rows
```

如果没有走优化路径，会看到：
```
INFO [DistributedExecutor] Query plan: Simple
INFO [DistributedExecutor] Using simple query path
```

## 调试输出

新的 demo 会显示：
```
✅ 查询完成
   - 耗时: 15ms
   - 返回: 4 行（4 个 batch）  ← 如果是这样，说明没有合并
   ⚠️  警告：返回了多个 batch，可能没有正确合并
      Batch 0: 1 行
      Batch 1: 1 行
      Batch 2: 1 行
      Batch 3: 1 行
```

或者（正确的情况）：
```
✅ 查询完成
   - 耗时: 15ms
   - 返回: 1 行（1 个 batch）  ← 正确合并
```

## 解决方案

如果确认是没有走优化路径，需要检查：
1. `analyze_query` 是否正确识别了查询
2. `DistributedExecutor` 是否正确路由到优化器

如果确认走了优化路径但没有合并，需要检查：
1. `TopKMerger::merge` 是否正确合并了 batches
2. `SortLimitOptimizer` 是否正确返回了合并后的结果
