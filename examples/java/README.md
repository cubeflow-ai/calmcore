# Java 流式读取示例

演示如何使用 JDBC 高效读取 Calm 数据库的大结果集。

## 文件说明

- `StreamingReadExample.java` - 5 个流式读取示例
- `run_streaming_example.sh` - 一键编译运行脚本

## 快速开始

```bash
cd examples/java
./run_streaming_example.sh
```

## 示例说明

### 示例 1: 基础流式读取
```java
stmt.setFetchSize(1000);  // 每次从服务器拉取 1000 行
```
- 最简单的流式读取方式
- 客户端缓冲 1000 行，用完再拉取
- 适合: 简单的数据处理场景

### 示例 2: 带进度显示
```java
// 每 5000 行显示速度、耗时等信息
```
- 显示实时读取速度和进度
- 方便监控长时间运行的任务
- 适合: 需要进度反馈的批处理任务

### 示例 3: 分页读取 (LIMIT + OFFSET)
```sql
SELECT * FROM table LIMIT 1000 OFFSET 5000
```
- 适合需要跳页的场景
- 可以并行读取不同的页
- 注意: OFFSET 很大时性能会下降

### 示例 4: 游标流式读取 ⭐ 推荐
```java
pstmt.setFetchSize(Integer.MIN_VALUE);  // MySQL 特殊值：启用流式
```
- **最高效的方式**
- 使用服务器端游标，真正的流式传输
- 内存占用最小
- 适合: 大数据量顺序处理

### 示例 5: 内存监控版
```java
// 监控 JVM 内存使用情况
```
- 验证流式读取的内存效率
- 显示峰值内存和内存增长
- 适合: 性能测试和优化

## 配置说明

### 连接参数
```java
String url = "jdbc:mysql://localhost:3306/calm";
String user = "root";
String password = "password";
```

### FetchSize 设置
- `stmt.setFetchSize(1000)` - 普通流式，每次拉取 1000 行
- `pstmt.setFetchSize(Integer.MIN_VALUE)` - MySQL 流式游标（推荐）

### PreparedStatement 配置（游标模式）
```java
PreparedStatement pstmt = conn.prepareStatement(
    sql,
    ResultSet.TYPE_FORWARD_ONLY,      // 只向前
    ResultSet.CONCUR_READ_ONLY        // 只读
);
pstmt.setFetchSize(Integer.MIN_VALUE); // 关键！
```

## 性能对比

| 方式 | 内存占用 | 速度 | 适用场景 |
|-----|---------|------|---------|
| 不设置 FetchSize | 全部加载到内存 | 快 | 小结果集 (<1万行) |
| setFetchSize(1000) | 1000行 × 列数 | 中等 | 中等结果集 |
| 游标流式 (推荐) | 最小 (~100行) | 快 | 大结果集 (>10万行) |
| LIMIT + OFFSET | 单页大小 | 慢（大OFFSET） | 需要跳页 |

## 测试数据

使用 `r2api` 表（默认有几十万行数据）：
```sql
SELECT * FROM r2api LIMIT 100000
```

## 背压测试

取消注释来模拟慢速消费：
```java
if (count % 1000 == 0) {
    Thread.sleep(100);  // 每 1000 行暂停 100ms
}
```

观察服务器日志，应该看到服务器发送速度会自动降低（TCP 背压生效）。

## 最佳实践

### ✅ 推荐做法

1. **大结果集用游标流式**
   ```java
   pstmt.setFetchSize(Integer.MIN_VALUE);
   ```

2. **总是使用 LIMIT**
   ```sql
   SELECT * FROM table LIMIT 100000  -- 避免意外全表扫描
   ```

3. **及时关闭资源**
   ```java
   try (Connection conn = ...; Statement stmt = ...; ResultSet rs = ...) {
       // 使用资源
   }  // 自动关闭
   ```

4. **显示进度**
   ```java
   if (count % 10000 == 0) {
       System.out.println("已处理: " + count + " 行");
   }
   ```

### ❌ 避免的做法

1. **不要一次性加载全部**
   ```java
   // ❌ 错误
   stmt.executeQuery("SELECT * FROM huge_table");  // 可能 OOM
   
   // ✅ 正确
   stmt.setFetchSize(Integer.MIN_VALUE);
   stmt.executeQuery("SELECT * FROM huge_table LIMIT 1000000");
   ```

2. **不要在循环中创建新连接**
   ```java
   // ❌ 错误
   for (int i = 0; i < 100; i++) {
       Connection conn = DriverManager.getConnection(...);
       // ...
   }
   
   // ✅ 正确
   try (Connection conn = DriverManager.getConnection(...)) {
       for (int i = 0; i < 100; i++) {
           // ...
       }
   }
   ```

3. **不要忘记处理异常**
   ```java
   // ❌ 错误
   Connection conn = DriverManager.getConnection(...);
   // 异常时连接不会关闭
   
   // ✅ 正确
   try (Connection conn = DriverManager.getConnection(...)) {
       // ...
   } catch (SQLException e) {
       // 处理异常，资源自动关闭
   }
   ```

## 依赖

- JDK 8+
- MySQL Connector/J 8.2.0+

## 故障排查

### 问题：内存仍然很高

**可能原因**：
1. 没有设置 `setFetchSize(Integer.MIN_VALUE)`
2. 使用了 `Statement` 而不是 `PreparedStatement`
3. ResultSet 类型不是 `TYPE_FORWARD_ONLY`

**解决**：
```java
PreparedStatement pstmt = conn.prepareStatement(
    sql,
    ResultSet.TYPE_FORWARD_ONLY,
    ResultSet.CONCUR_READ_ONLY
);
pstmt.setFetchSize(Integer.MIN_VALUE);
```

### 问题：速度很慢

**可能原因**：
1. 没有添加 LIMIT
2. OFFSET 太大
3. 网络延迟

**解决**：
- 添加 `LIMIT`
- 使用游标代替 OFFSET
- 检查网络连接

### 问题：连接断开

**可能原因**：
- 查询时间过长，超过 `wait_timeout`

**解决**：
```sql
SET SESSION wait_timeout = 28800;  -- 8 小时
```

## 扩展阅读

- [MySQL JDBC 驱动文档](https://dev.mysql.com/doc/connector-j/8.0/en/)
- [Calm 数据库文档](../../docs/)
- [JDBC 最佳实践](../../docs/JDBC_CONNECTION_GUIDE.md)
