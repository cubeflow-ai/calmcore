# MySQL JDBC 连接指南

## 问题说明

MySQL JDBC 驱动默认要求服务器支持 `CLIENT_SECURE_CONNECTION` capability flag。
由于 `msql-srv` 库的限制,需要在 JDBC 连接字符串中添加特定参数。

## 解决方案

### 方案 1: 基础连接 (推荐)

```java
String url = String.format(
    "jdbc:mysql://%s:%d/%s?useSSL=false&allowPublicKeyRetrieval=true&useCursorFetch=true&defaultFetchSize=%d",
    host, port, database, fetchSize
);
```

### 方案 2: 完整配置

```java
String url = String.format(
    "jdbc:mysql://%s:%d/%s?" +
    "useSSL=false&" +                      // 禁用 SSL
    "allowPublicKeyRetrieval=true&" +      // 允许公钥检索
    "serverTimezone=UTC&" +                // 设置时区
    "useCursorFetch=true&" +               // 启用游标模式
    "defaultFetchSize=%d&" +               // 默认 fetch 大小
    "useServerPrepStmts=true",             // 使用服务器端预处理语句
    host, port, database, fetchSize
);
```

## 完整示例代码

```java
import java.sql.*;

public class CalmJdbcCursorExample {
    public static void main(String[] args) {
        String host = "localhost";
        int port = 3306;
        String database = "r2api";
        String username = "root";
        String password = "";
        int fetchSize = 1000;
        
        // ✅ 正确的连接字符串
        String url = String.format(
            "jdbc:mysql://%s:%d/%s?useSSL=false&allowPublicKeyRetrieval=true&useCursorFetch=true&defaultFetchSize=%d",
            host, port, database, fetchSize
        );
        
        String sql = "SELECT * FROM " + database + " LIMIT 10000";
        
        try (Connection conn = DriverManager.getConnection(url, username, password);
             Statement stmt = conn.createStatement()) {
            
            stmt.setFetchSize(fetchSize);
            
            try (ResultSet rs = stmt.executeQuery(sql)) {
                int count = 0;
                while (rs.next()) {
                    count++;
                    if (count % 1000 == 0) {
                        System.out.println("已读取 " + count + " 行");
                    }
                }
                System.out.println("总共读取 " + count + " 行");
            }
            
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
```

## 关键参数说明

| 参数 | 说明 | 必需 |
|------|------|------|
| `useSSL=false` | 禁用 SSL 连接 | ✅ |
| `allowPublicKeyRetrieval=true` | 允许客户端从服务器获取公钥 | ✅ |
| `useCursorFetch=true` | 启用服务器端游标 | ✅ (流式读取) |
| `defaultFetchSize=N` | 设置默认每次 fetch 的行数 | ✅ (流式读取) |
| `useServerPrepStmts=true` | 使用服务器端预处理语句 | 可选 |
| `serverTimezone=UTC` | 设置服务器时区 | 可选 |

## 测试连接

```bash
# 方法 1: 使用 MySQL 命令行客户端
mysql -h localhost -P 3306 -u root

# 方法 2: 使用 Java 测试程序
javac CalmJdbcCursorExample.java
java -cp .:mysql-connector-java-8.0.xx.jar CalmJdbcCursorExample
```

## 常见错误

### 错误 1: CLIENT_SECURE_CONNECTION is required
```
java.sql.SQLNonTransientConnectionException: CLIENT_SECURE_CONNECTION is required
```
**解决方案**: 确保连接字符串包含 `useSSL=false&allowPublicKeyRetrieval=true`

### 错误 2: Public Key Retrieval is not allowed
```
java.sql.SQLException: Public Key Retrieval is not allowed
```
**解决方案**: 添加 `allowPublicKeyRetrieval=true` 参数

### 错误 3: Unknown system variable 'XXX'
```
java.sql.SQLException: Unknown system variable 'XXX'
```
**解决方案**: 这是正常的,JDBC 驱动会查询一些系统变量,服务器会忽略不支持的变量

## 游标功能验证

连接成功后,可以通过以下方式验证游标功能:

```java
// 设置 fetchSize 启用流式读取
stmt.setFetchSize(1000);

// 执行大数据量查询
ResultSet rs = stmt.executeQuery("SELECT * FROM large_table");

// 数据会分批返回,内存占用低
while (rs.next()) {
    // 处理每一行...
}
```

## 性能建议

1. **fetchSize 设置**: 通常 500-2000 之间,取决于行大小
2. **网络延迟**: 调整 fetchSize 平衡网络往返次数和内存使用
3. **服务器负载**: 大 fetchSize 减少往返,但增加服务器内存压力

## 支持的功能

✅ SELECT 查询
✅ WHERE 条件过滤
✅ ORDER BY 排序
✅ LIMIT / OFFSET
✅ GROUP BY 聚合
✅ COUNT / SUM / AVG / MAX / MIN
✅ 服务器端游标 (CURSOR)
✅ Prepared Statements
✅ INSERT / DELETE

## 不支持的功能

❌ SSL/TLS 加密连接
❌ 复杂的事务 (COMMIT/ROLLBACK 被忽略)
❌ 存储过程
❌ 触发器
❌ 视图
