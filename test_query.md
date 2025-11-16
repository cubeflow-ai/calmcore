# 查询测试和问题诊断

## 问题 1：查询返回多个 partition 的结果

### 症状
```sql
SELECT * FROM users ORDER BY id DESC LIMIT 1
```
返回了 4 行（每个 partition 一行），而不是 1 行。

### 诊断步骤

1. **运行测试并查看日志**：
```bash
./demo.sh write
RUST_LOG=info cargo run --example demo --release -- query 2>&1 | grep -E "Query plan|Using|Merging|completed"
```

2. **查看输出**：
- 如果看到 "Using optimized Sort+Limit path" → 走了优化路径
- 如果看到 "Using simple query path" → 没有走优化路径

### 可能的原因

#### 原因 1：查询没有走优化路径

**检查**：查看日志是否有 "Using optimized Sort+Limit path"

**原因**：
- `analyze_query` 没有正确识别查询
- 查询被识别为 Simple 而不是 SortLimit

**解决方法**：
- 检查 `plan_analyzer.rs` 的 `analyze_sort_limit` 函数
- 确保没有 WHERE 条件的 ORDER BY + LIMIT 也能被识别

#### 原因 2：走了优化路径但没有合并

**检查**：日志显示 "Merging results from 4 partitions" 但返回了 4 行

**原因**：
- `TopKMerger::merge` 返回了多个 batch
- 或者 `SortLimitOptimizer` 没有正确返回合并后的结果

**解决方法**：
- 检查 `TopKMerger::merge` 的返回值
- 确保返回 `vec![final_batch]` 而不是多个 batch

#### 原因 3：默认查询路径返回了多个 batch

**检查**：日志显示 "Using simple query path"

**原因**：
- 默认的查询路径会从每个 partition 收集结果
- 但不会合并和排序

**解决方法**：
- 确保 ORDER BY + LIMIT 查询走优化路径
- 或者在默认路径中也添加合并逻辑

## 问题 2：MySQL 段错误

### 症状
```bash
mysql -h 127.0.0.1 -P 3307 -u root
zsh: segmentation fault
```

### 可能的原因

1. **空指针解引用** - 某个字段或数据结构为 null
2. **内存越界** - 数组访问超出边界
3. **线程安全问题** - 多线程访问共享数据时出现竞态条件
4. **msql_srv 库的问题** - 第三方库的 bug

### 调试方法

1. **使用 RUST_BACKTRACE 查看堆栈**：
```bash
RUST_BACKTRACE=full cargo run --bin calm --release
# 然后在另一个终端连接
mysql -h 127.0.0.1 -P 3307 -u root
```

2. **添加日志**：
在 `on_query` 方法开始处添加：
```rust
eprintln!("Received query: {}", query);
```

3. **检查空指针**：
确保所有 `unwrap()` 都有正确的错误处理

### 临时解决方案

暂时不使用 MySQL 协议，使用 demo 程序测试查询优化器：
```bash
./demo.sh write query
```

## 建议的测试流程

1. **先测试查询合并问题**：
```bash
./demo.sh write query
```
查看输出，确认是否返回了正确的行数。

2. **查看详细日志**：
```bash
RUST_LOG=info cargo run --example demo --release -- query
```
确认是否走了优化路径。

3. **如果需要，修复查询分析器**：
确保 `analyze_query` 正确识别所有 ORDER BY + LIMIT 查询。

4. **MySQL 问题单独处理**：
这是一个独立的 bug，需要详细的堆栈跟踪和调试。

## 下一步

请运行以下命令并告诉我输出：

```bash
# 1. 测试查询
./demo.sh write query

# 2. 查看详细日志
RUST_LOG=info cargo run --example demo --release -- query 2>&1 | grep -E "Query plan|Using|Merging|completed|返回"
```

这样我可以确定具体是哪个问题，然后针对性地修复。
