 
## 问题描述

### 问题 1: 内存泄漏
- 客户端断开连接后,服务器内存不释放
- 执行大结果集查询后,内存持续增长

### 问题 2: 缺少背压机制
- 客户端消费慢时,服务器仍然疯狂写入缓冲区
- 导致服务器内存无限增长

## 修复方案

### 1. 添加行数限制和警告

**位置**: `src/protocol/mysql/mod.rs::execute_query()`

```rust
const WARN_THRESHOLD: usize = 100_000;      // 10万行警告
const MAX_ROWS_WITHOUT_LIMIT: usize = 1_000_000;  // 100万行拒绝

// 超过10万行: 打印警告
// 超过100万行且无LIMIT: 返回错误
```

**效果**:
- 防止意外的全表扫描导致内存爆炸
- 强制用户使用 `LIMIT` 子句

### 2. 实现写入背压

**位置**: `src/protocol/mysql/mod.rs::write_query_result()`

```rust
const FLUSH_INTERVAL: usize = 1000;  // 每1000行一个检查点

for batch in batches {
    for row_idx in 0..batch.num_rows() {
        // 写入数据...
        
        // 检查客户端是否断开
        if let Err(e) = row_writer.write_col(value) {
            eprintln!("Client disconnected");
            return Err(e);  // 立即停止
        }
        
        rows_written += 1;
        
        // 每1000行记录一次
        if rows_written % FLUSH_INTERVAL == 0 {
            // TCP socket 自然实现背压
            // 如果客户端消费慢,这里会阻塞
        }
    }
}
```

**效果**:
- 客户端消费慢时,服务器阻塞在 `write()` 调用
- 不会无限缓冲数据到内存
- 客户端断开时立即停止发送

### 3. 连接清理日志

**位置**: `src/protocol/mysql/mod.rs::start()`

```rust
match MysqlIntermediary::run_on_tcp(backend, std_stream) {
    Ok(_) => {
        eprintln!("✅ Client {} disconnected normally", peer_addr);
    }
    Err(e) => {
        eprintln!("❌ Client {} error: {}", peer_addr, e);
    }
}

eprintln!("🧹 Cleaning up connection from {}", peer_addr);
```

**效果**:
- 可以追踪每个连接的生命周期
- 验证资源是否正确释放

## 测试

### 测试 1: 内存释放

```java
// 连接后立即断开
try (Connection conn = DriverManager.getConnection(url, user, pass)) {
    Statement stmt = conn.createStatement();
    stmt.executeQuery("SELECT * FROM large_table");
    // 不读取数据,直接关闭
}

// 服务器日志应该显示:
// ⚠️  Client disconnected at row XXX
// 🧹 Cleaning up connection from 127.0.0.1:XXXXX
```

**验证**: 使用 `ps aux | grep calm` 观察内存(RSS),应该回落

### 测试 2: 背压机制

```java
try (Connection conn = DriverManager.getConnection(url, user, pass)) {
    Statement stmt = conn.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT * FROM large_table LIMIT 100000");
    
    // 慢速消费
    int count = 0;
    while (rs.next() && count < 10) {
        Thread.sleep(1000);  // 每秒读一行
        System.out.println("Row: " + rs.getString(1));
        count++;
    }
}
```

**服务器日志应该显示**:
```
📤 [MySQL] Sent 1000 rows (may block if client is slow)
📤 [MySQL] Sent 2000 rows (may block if client is slow)
...
⚠️  Client disconnected at row 10
🧹 Cleaning up connection
```

### 测试 3: 大结果集保护

```java
// 不使用 LIMIT 查询大表
ResultSet rs = stmt.executeQuery("SELECT * FROM large_table");
```

**预期**:
- 10万行: 警告日志
- 100万行: 直接拒绝,返回错误

```
⚠️  [MySQL] Large result set: 655035 rows. Consider adding LIMIT clause.
❌ [MySQL] Query without LIMIT returned 655035 rows (max: 1000000)
```

客户端收到错误:
```
SQLException: Result set too large (655035 rows). Please add LIMIT clause.
```

## 配置参数

可以根据实际情况调整:

```rust
// src/protocol/mysql/mod.rs

// 警告阈值 (行数)
const WARN_THRESHOLD: usize = 100_000;

// 最大行数限制 (无LIMIT时)
const MAX_ROWS_WITHOUT_LIMIT: usize = 1_000_000;

// 背压检查间隔 (行数)
const FLUSH_INTERVAL: usize = 1000;
```

**建议配置**:
- 开发环境: WARN=10K, MAX=100K
- 生产环境: WARN=100K, MAX=1M

## 性能影响

### 背压机制
- **开销**: 每1000行一次日志输出 - 可忽略
- **好处**: 防止内存爆炸,保护服务器稳定性

### 行数限制
- **开销**: 一次字符串检查 - 可忽略
- **好处**: 强制用户写出合理的查询

### TCP 背压
- **原理**: 操作系统 TCP stack 自动管理
- **效果**: 客户端慢 → 服务器 write() 阻塞 → 查询执行暂停
- **开销**: 零额外开销 (操作系统级别)

## 最佳实践

### 应用端

1. **总是使用 LIMIT**
```sql
-- ❌ 危险
SELECT * FROM large_table;

-- ✅ 安全
SELECT * FROM large_table LIMIT 10000;
```

2. **使用游标分页**
```java
stmt.setFetchSize(1000);  // 每次只获取1000行
ResultSet rs = stmt.executeQuery("SELECT * FROM large_table LIMIT 100000");

while (rs.next()) {
    // 处理数据...
    // 背压自动工作
}
```

3. **及时关闭资源**
```java
try (Connection conn = ...;
     Statement stmt = ...;
     ResultSet rs = ...) {
    // 使用资源
}  // 自动关闭,释放服务器资源
```

### 服务器端

1. **监控日志**
```bash
# 查看大查询警告
tail -f /tmp/calm_mysql.log | grep "Large result set"

# 查看客户端断开
tail -f /tmp/calm_mysql.log | grep "disconnected"
```

2. **调整限制**
- 根据实际内存大小调整 `MAX_ROWS_WITHOUT_LIMIT`
- 监控每个查询的行数分布

3. **定期检查内存**
```bash
# 每5秒检查一次内存
watch -n 5 'ps aux | grep calm | grep -v grep'
```

## 技术细节

### TCP 背压工作原理

1. **客户端慢速消费**
   - TCP 接收缓冲区满
   - 客户端 TCP 发送 window=0 给服务器

2. **服务器端响应**
   - `write()` 系统调用阻塞
   - 当前线程暂停
   - 不消耗额外内存

3. **客户端恢复**
   - 读取数据,接收缓冲区空出
   - 发送 window update
   - 服务器 `write()` 返回,继续发送

### Rust Drop Trait

```rust
impl Drop for CalmBackend {
    fn drop(&mut self) {
        // 自动清理资源
        // Arc 引用计数归零时释放 Engine
        // CursorManager 中的 HashMap 自动清理
    }
}
```

**保证**:
- 线程结束 → Drop 调用 → 资源释放
- 即使 panic 也会调用 Drop

## 故障排查

### 问题: 内存仍然不释放

**检查**:
1. 是否有其他地方持有 `RecordBatch` 引用?
2. 使用 `valgrind` 或 `heaptrack` 分析内存

```bash
# 安装
brew install valgrind  # macOS 可能不支持
cargo install cargo-instruments  # macOS 推荐

# 运行
cargo instruments --release --template Allocations
```

### 问题: 背压不工作

**检查**:
1. 客户端是否使用了应用层缓冲? (如 BufferedInputStream)
2. TCP 缓冲区是否太大?

```bash
# 查看 TCP 缓冲区大小
sysctl net.inet.tcp.recvspace
sysctl net.inet.tcp.sendspace
```

## 未来改进

1. **流式查询执行**
   - 不在内存中一次性生成所有结果
   - 边执行边发送

2. **游标真正实现**
   - DECLARE CURSOR 不执行查询
   - FETCH 每次返回 N 行

3. **连接池监控**
   - 统计每个连接的内存使用
   - 自动断开超时连接

4. **动态限制**
   - 根据服务器内存动态调整 MAX_ROWS
   - 多个并发连接时降低单连接限制
