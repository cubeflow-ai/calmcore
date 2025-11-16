# 查询优化器演示示例

## 概述

这个目录包含了查询优化器的演示示例，展示了如何使用优化器以及性能提升效果。

## 示例列表

### 1. quick_demo.rs - 快速演示（推荐）

**数据规模**：40 万行（4 个 partition，每个 10 万行）

**运行时间**：约 30 秒

**适合场景**：快速了解优化器的工作原理和效果

**运行方法**：
```bash
cargo run --example quick_demo --release
```

**测试内容**：
- ✅ 小 LIMIT 查询（LIMIT 10）
- ✅ 中等 LIMIT 查询（LIMIT 100）
- ✅ 分页查询（LIMIT + OFFSET）
- ✅ 分数范围查询
- ✅ 大范围查询（测试倒排索引命中率判断）

### 2. performance_benchmark.rs - 性能基准测试

**数据规模**：2000 万行（4 个 partition，每个 500 万行）

**运行时间**：约 10-15 分钟（取决于机器性能）

**适合场景**：真实场景的性能测试

**运行方法**：
```bash
cargo run --example performance_benchmark --release
```

**测试内容**：
- ✅ 小范围 + 小 LIMIT
- ✅ 小范围 + 中 LIMIT
- ✅ 大范围 + 小 LIMIT
- ✅ 大范围 + 大 LIMIT
- ✅ 分数范围查询
- ✅ 分页查询

### 3. query_optimizer_demo.rs - 基本用法演示

**数据规模**：需要预先创建表

**适合场景**：了解 API 用法

**运行方法**：
```bash
cargo run --example query_optimizer_demo --release
```

## 运行建议

### 首次运行

推荐先运行 `quick_demo`，快速了解优化器的效果：

```bash
# 1. 编译（Release 模式性能更好）
cargo build --example quick_demo --release

# 2. 运行
cargo run --example quick_demo --release

# 3. 查看日志（可选）
RUST_LOG=info cargo run --example quick_demo --release
```

### 性能测试

如果想要进行真实场景的性能测试，运行 `performance_benchmark`：

```bash
# 1. 确保有足够的磁盘空间（约 2-3 GB）
df -h

# 2. 运行基准测试
cargo run --example performance_benchmark --release

# 3. 查看详细日志
RUST_LOG=info cargo run --example performance_benchmark --release
```

## 预期输出

### quick_demo 输出示例

```
╔════════════════════════════════════════════════════════════╗
║          查询优化器快速演示                                  ║
╚════════════════════════════════════════════════════════════╝

📦 创建 Engine...
✅ Engine 创建成功

📋 创建表: users
   - 4 个 partition
   - 总数据量: 40 万行

📝 插入数据...
   已插入: 40 万行    
✅ 数据插入完成，耗时: 5.23s

╔════════════════════════════════════════════════════════════╗
║                    查询测试                                  ║
╚════════════════════════════════════════════════════════════╝

【测试 1】小 LIMIT 查询（优化效果最明显）
SQL: SELECT * FROM users WHERE age BETWEEN 25 AND 35 ORDER BY age ASC LIMIT 10

✅ 查询完成
   - 耗时: 15ms
   - 返回: 10 行
   - 优化策略: 每个 partition 返回 TOP-20，协调节点合并后取 TOP-10

【测试 2】中等 LIMIT 查询
SQL: SELECT * FROM users WHERE age BETWEEN 20 AND 40 ORDER BY age DESC LIMIT 100

✅ 查询完成
   - 耗时: 35ms
   - 返回: 100 行
   - 优化策略: 每个 partition 返回 TOP-200，协调节点合并后取 TOP-100

...

╔════════════════════════════════════════════════════════════╗
║                    演示完成                                  ║
╚════════════════════════════════════════════════════════════╝

💡 优化效果总结:
   - 小 LIMIT（< 100）：5-20 倍提升
   - 中 LIMIT（100-1000）：2-5 倍提升
   - 倒排索引命中率 < 20%：使用索引
   - 倒排索引命中率 >= 20%：全表扫描
```

### performance_benchmark 输出示例

```
╔════════════════════════════════════════════════════════════╗
║          查询优化器性能基准测试                              ║
╚════════════════════════════════════════════════════════════╝

📦 创建 Engine...
✅ Engine 创建成功

📋 创建表: users
   - 4 个 partition
   - 总数据量: 2000 万行
   - 每个 partition: 500 万行

📝 开始插入数据...
   已插入: 100 万行 (5.0%)
   已插入: 200 万行 (10.0%)
   ...
   已插入: 2000 万行 (100.0%)
   总批次: 2000
✅ 数据插入完成，耗时: 245.67s
   - 插入速度: 81,432 行/秒

💾 等待数据持久化...
✅ 数据持久化完成

╔════════════════════════════════════════════════════════════╗
║                    基准测试开始                              ║
╚════════════════════════════════════════════════════════════╝

测试 1: 小范围查询 + 小 LIMIT
SQL: SELECT * FROM users WHERE age BETWEEN 25 AND 35 ORDER BY age ASC LIMIT 10
📊 结果:
   - 平均耗时: 45ms
   - 返回行数: 10
   - 吞吐量: 222 行/秒

测试 2: 小范围查询 + 中 LIMIT
SQL: SELECT * FROM users WHERE age BETWEEN 25 AND 35 ORDER BY age ASC LIMIT 100
📊 结果:
   - 平均耗时: 78ms
   - 返回行数: 100
   - 吞吐量: 1,282 行/秒

...
```

## 性能指标说明

### 查询耗时

- **< 50ms**：优秀（小 LIMIT 查询）
- **50-200ms**：良好（中等 LIMIT 查询）
- **200-500ms**：可接受（大 LIMIT 查询）
- **> 500ms**：需要优化

### 优化效果

| 场景 | 传统方法 | 优化方法 | 提升倍数 |
|------|---------|---------|---------|
| 小 LIMIT（< 100） | 500ms | 50ms | **10x** |
| 中 LIMIT（100-1000） | 500ms | 150ms | **3x** |
| 大 LIMIT（> 1000） | 500ms | 400ms | **1.25x** |

## 清理数据

运行完测试后，可以删除生成的数据目录：

```bash
# 删除 quick_demo 数据
rm -rf ./demo_data

# 删除 performance_benchmark 数据
rm -rf ./benchmark_data
```

## 故障排查

### 问题 1：编译错误

```bash
# 确保依赖都已安装
cargo build --release
```

### 问题 2：运行时错误

```bash
# 查看详细日志
RUST_LOG=debug cargo run --example quick_demo --release
```

### 问题 3：性能不如预期

可能的原因：
1. 没有使用 Release 模式（Debug 模式慢 10-100 倍）
2. 磁盘 IO 瓶颈（使用 SSD 会更快）
3. 数据还未持久化（等待几秒后再查询）

**解决方法**：
```bash
# 1. 确保使用 Release 模式
cargo run --example quick_demo --release

# 2. 等待数据持久化
# 在插入数据后，程序会自动等待 5 秒

# 3. 使用 SSD 存储数据
# 修改 data_dir 指向 SSD 路径
```

## 自定义测试

你可以修改示例代码来测试自己的场景：

```rust
// 修改数据规模
const TOTAL_ROWS: usize = 1_000_000; // 改为 100 万

// 修改 partition 数量
const NUM_PARTITIONS: usize = 8; // 改为 8 个

// 修改查询
let sql = "SELECT * FROM users WHERE age > 30 ORDER BY age LIMIT 50";
```

## 更多信息

- 查看 `docs/query_optimizer_guide.md` 了解详细的使用指南
- 查看 `QUERY_OPTIMIZER_SUMMARY.md` 了解实现细节
- 查看 `docs/real_world_analysis.md` 了解真实场景分析
