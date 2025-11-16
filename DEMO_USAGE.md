# Demo 使用说明

## 🚀 快速开始

新的 demo 支持分阶段操作，可以分别执行写入、查询、持久化。

### 方式 1：使用脚本（推荐）

```bash
# 给脚本添加执行权限
chmod +x demo.sh

# 写入数据并查询
./demo.sh write query

# 只查询（需要先写入数据）
./demo.sh query

# 持久化数据
./demo.sh persist

# 全部操作
./demo.sh all

# 清理数据
./demo.sh clean
```

### 方式 2：直接运行

```bash
# 写入数据
cargo run --example demo --release -- write

# 查询数据
cargo run --example demo --release -- query

# 持久化数据
cargo run --example demo --release -- persist

# 写入并查询
cargo run --example demo --release -- write query

# 全部操作
cargo run --example demo --release -- all
```

## 📊 操作说明

### 1. write - 写入数据

```bash
./demo.sh write
```

**功能**：
- 创建表（如果不存在）
- 写入 40 万条数据
- 使用 Hash 分区策略（根据 id 的 hash 值分配到 4 个 partition）
- 数据写入内存，立即可查询

**输出示例**：
```
╔════════════════════════════════════════════════════════════╗
║                    写入数据                                  ║
╚════════════════════════════════════════════════════════════╝

📋 创建表: users
✅ 表创建成功

📝 开始写入数据...
   - 总数据量: 40 万行
   - Partition 数量: 4
   - 分区策略: Hash(id)

   已插入: 40 万行    

✅ 数据写入完成
   - 耗时: 5.23s
   - 速度: 76,482 行/秒
   - 数据在内存中，可立即查询
```

### 2. query - 查询数据

```bash
./demo.sh query
```

**功能**：
- 运行 5 个测试查询
- 展示查询优化器的效果
- 统计查询性能

**测试场景**：
1. 小 LIMIT 查询（LIMIT 10）
2. 中等 LIMIT 查询（LIMIT 100）
3. 分页查询（LIMIT + OFFSET）
4. 分数范围查询
5. 大范围查询（测试倒排索引命中率）

**输出示例**：
```
╔════════════════════════════════════════════════════════════╗
║                    查询测试                                  ║
╚════════════════════════════════════════════════════════════╝

【测试 1】小 LIMIT 查询（优化效果最明显）
SQL: SELECT * FROM users WHERE age BETWEEN 25 AND 35 ORDER BY age ASC LIMIT 10

✅ 查询完成
   - 耗时: 15ms
   - 返回: 10 行
   - 吞吐量: 667 行/秒

【测试 2】中等 LIMIT 查询
SQL: SELECT * FROM users WHERE age BETWEEN 20 AND 40 ORDER BY age DESC LIMIT 100

✅ 查询完成
   - 耗时: 35ms
   - 返回: 100 行
   - 吞吐量: 2,857 行/秒

...
```

### 3. persist - 持久化数据

```bash
./demo.sh persist
```

**功能**：
- 将内存中的数据持久化到磁盘
- 数据目录：`./demo_data`

**输出示例**：
```
╔════════════════════════════════════════════════════════════╗
║                    持久化数据                                ║
╚════════════════════════════════════════════════════════════╝

💾 开始持久化数据到磁盘...
   这可能需要几秒钟...

✅ 持久化完成
   - 数据已写入磁盘: ./demo_data
```

### 4. 组合操作

```bash
# 写入并查询（最常用）
./demo.sh write query

# 全部操作（写入 + 查询 + 持久化）
./demo.sh all
```

## 🔍 查看详细日志

```bash
# 启用详细日志
RUST_LOG=info cargo run --example demo --release -- write query
```

**日志示例**：
```
INFO [DistributedExecutor] Query plan: SortLimit(...)
INFO [DistributedExecutor] Using optimized Sort+Limit path
INFO [SortLimitOptimizer] Executing optimized query on table 'users' with 4 partitions
INFO [SortLimitOptimizer] Each partition will return up to 100 rows (global limit: 10)
INFO [SortLimitOptimizer] Partition 0 returned 100 rows
INFO [SortLimitOptimizer] Partition 1 returned 100 rows
INFO [SortLimitOptimizer] Partition 2 returned 100 rows
INFO [SortLimitOptimizer] Partition 3 returned 100 rows
INFO [SortLimitOptimizer] Merging results from 4 partitions
INFO [SortLimitOptimizer] Query completed, returning 10 rows
```

## 📈 性能验证

### 验证数据分布

写入数据后，每个 partition 应该有大约 10 万行数据（使用 Hash 分区策略）。

### 验证查询优化

查看日志中是否有：
- ✅ "Using optimized Sort+Limit path" - 使用了优化路径
- ✅ "Each partition will return up to X rows" - 每个 partition 返回 TOP-K
- ✅ "Merging results from X partitions" - 合并多个 partition 的结果

### 验证倒排索引

对于小范围查询（如 age BETWEEN 25 AND 35），应该看到：
- ✅ 使用倒排索引（命中率 < 20%）
- ✅ 查询速度快（< 50ms）

对于大范围查询（如 age BETWEEN 18 AND 80），应该看到：
- ✅ 切换到全表扫描（命中率 > 20%）
- ✅ ORDER BY + LIMIT 仍然优化

## 🧹 清理数据

```bash
# 删除所有测试数据
./demo.sh clean

# 或手动删除
rm -rf ./demo_data
```

## 🎯 典型工作流

### 开发测试流程

```bash
# 1. 写入数据
./demo.sh write

# 2. 多次查询测试（数据在内存中）
./demo.sh query
./demo.sh query
./demo.sh query

# 3. 持久化数据
./demo.sh persist

# 4. 清理
./demo.sh clean
```

### 快速验证流程

```bash
# 一次性完成所有操作
./demo.sh write query

# 查看结果，验证优化效果
```

## 💡 提示

1. **数据在内存中**：写入后立即可查询，无需等待
2. **Hash 分区**：数据根据 id 的 hash 值均匀分布到 4 个 partition
3. **可重复查询**：写入一次后，可以多次运行 query 操作
4. **持久化可选**：如果只是测试，不需要持久化

## ❓ 常见问题

### Q: 为什么返回 0 行？

A: 可能是：
1. 没有先运行 `write` 操作
2. 表不存在

**解决方法**：
```bash
./demo.sh write query
```

### Q: 查询很慢？

A: 确保使用 `--release` 模式：
```bash
cargo run --example demo --release -- query
```

### Q: 想修改数据量？

A: 编辑 `examples/demo.rs`，修改 `TOTAL_ROWS` 常量：
```rust
const TOTAL_ROWS: usize = 1_000_000; // 改为 100 万
```

## 🚀 开始使用

```bash
# 给脚本添加执行权限
chmod +x demo.sh

# 运行
./demo.sh write query
```

享受查询优化带来的性能提升！🎉
