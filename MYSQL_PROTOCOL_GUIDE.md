# MySQL 协议支持使用指南

## 概述

Calm 数据库支持 MySQL 协议,允许使用标准 MySQL 客户端和 JDBC 驱动进行连接和操作。

## 当前功能

### 已支持的操作

1. **元数据查询**
   - `SHOW DATABASES` - 显示数据库列表
   - `SHOW TABLES` - 显示表列表
   - `USE database` - 切换数据库

2. **SELECT 查询**
   - 标准 SELECT 语句
   - WHERE 过滤条件
   - 通过 DataFusion 执行

### 限制说明

当前版本的 MySQL 协议实现主要面向**查询场景**,以下操作需要通过 Rust API 完成:

- **INSERT**: 使用 `Partition::upsert()` API
- **UPDATE**: 使用 `Partition::upsert()` API
- **DELETE**: 使用 `Partition::delete()` API
- **CREATE TABLE**: 使用 `Engine::create_table()` API
- **DROP TABLE**: 使用 `Engine::drop_table()` API

## 使用示例

### 1. 启动 MySQL 服务器

```bash
cd /Users/sunjian/rustworkspace/calmcore
cargo run --example mysql_server
```

服务器将在 `127.0.0.1:3306` 启动。

### 2. 使用 MySQL 客户端连接

```bash
mysql -h 127.0.0.1 -P 3306
```

### 3. 执行查询

```sql
-- 查看数据库
SHOW DATABASES;

-- 使用数据库
USE calm;

-- 查看表
SHOW TABLES;

-- 查询数据
SELECT * FROM data;
SELECT id, name, age FROM data WHERE age > 25;
SELECT COUNT(*) FROM data;
```

### 4. 使用 JDBC 连接 (Java)

#### Maven 依赖

```xml
<dependency>
    <groupId>mysql</groupId>
    <artifactId>mysql-connector-java</artifactId>
    <version>8.0.33</version>
</dependency>
```

#### Java 示例代码

```java
import java.sql.*;

public class CalmJdbcExample {
    public static void main(String[] args) {
        String url = "jdbc:mysql://127.0.0.1:3306/calm";
        String user = "root";  // 任意用户名
        String password = "";  // 无需密码
        
        try (Connection conn = DriverManager.getConnection(url, user, password)) {
            System.out.println("✅ Connected to Calm database");
            
            // 查询数据
            String query = "SELECT * FROM data";
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(query)) {
                
                ResultSetMetaData metaData = rs.getMetaData();
                int columnCount = metaData.getColumnCount();
                
                // 打印列名
                for (int i = 1; i <= columnCount; i++) {
                    System.out.print(metaData.getColumnName(i) + "\t");
                }
                System.out.println();
                
                // 打印数据
                while (rs.next()) {
                    for (int i = 1; i <= columnCount; i++) {
                        System.out.print(rs.getString(i) + "\t");
                    }
                    System.out.println();
                }
            }
            
            // 带条件的查询
            String queryWithWhere = "SELECT * FROM data WHERE age > ?";
            try (PreparedStatement pstmt = conn.prepareStatement(queryWithWhere)) {
                pstmt.setInt(1, 25);
                
                try (ResultSet rs = pstmt.executeQuery()) {
                    while (rs.next()) {
                        long id = rs.getLong("id");
                        String name = rs.getString("name");
                        int age = rs.getInt("age");
                        System.out.printf("ID: %d, Name: %s, Age: %d%n", id, name, age);
                    }
                }
            }
            
        } catch (SQLException e) {
            System.err.println("❌ Database error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
```

### 5. 通过 Rust API 进行数据操作

对于 INSERT/UPDATE/DELETE 操作,需要使用 Calm 的 Rust API:

```rust
use calm::{
    engine::{Engine, EngineConfig},
    partition::Partition,
    schema::{field::FieldOption, Schema},
};
use std::collections::HashMap;
use std::path::PathBuf;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 创建 Engine
    let config = EngineConfig {
        data_dir: PathBuf::from("/tmp/calm_data"),
        ..Default::default()
    };
    let engine = Engine::new(config)?;
    
    // 获取 Partition
    let partition = engine.get_partition(0).await.unwrap();
    
    // 插入数据
    let mut data = HashMap::new();
    data.insert("id".to_string(), "1001".to_string());
    data.insert("name".to_string(), "Alice".to_string());
    data.insert("age".to_string(), "30".to_string());
    partition.upsert(data)?;
    
    // 更新数据
    let mut update_data = HashMap::new();
    update_data.insert("id".to_string(), "1001".to_string());
    update_data.insert("age".to_string(), "31".to_string());
    partition.upsert(update_data)?;
    
    // 删除数据
    partition.delete("1001")?;
    
    Ok(())
}
```

## 架构说明

### 当前架构

```
MySQL Client/JDBC
       ↓
  MySQL Protocol
       ↓
   Partition (查询)
       ↓
   DataFusion
       ↓
  Arrow Record Batches
```

### 设计限制

1. **单 Partition 模式**: 当前 MySQL 协议绑定到单个 Partition
2. **查询优先**: 主要支持 SELECT 查询场景
3. **写入通过 API**: INSERT/UPDATE/DELETE 需要通过 Rust API

## 后续改进方向

### 计划中的功能

1. **完整的 DML 支持**
   - 解析 INSERT 语句并调用 `partition.upsert()`
   - 解析 UPDATE 语句并调用 `partition.upsert()`
   - 解析 DELETE 语句并调用 `partition.delete()`

2. **DDL 支持**
   - CREATE TABLE - 集成 Engine::create_table()
   - DROP TABLE - 集成 Engine::drop_table()
   - ALTER TABLE - 修改表结构

3. **多表支持**
   - 使用 Engine 管理多个表
   - 根据 SQL 中的表名路由到对应的 Partition
   - 支持 JOIN 操作

4. **事务支持**
   - BEGIN/COMMIT/ROLLBACK
   - 事务隔离级别

5. **更好的 SQL 解析**
   - 使用 sqlparser-rs 进行完整的 SQL 解析
   - 支持更复杂的 SQL 语法

### 实现建议

#### 1. 简单的 INSERT 支持

```rust
// 在 CalmBackend::on_query 中添加:
if query_lower.starts_with("insert") {
    return self.handle_insert(query, results);
}

fn handle_insert(&mut self, query: &str, results: QueryResultWriter<W>) -> io::Result<()> {
    // 解析: INSERT INTO table (col1, col2) VALUES (val1, val2);
    // 调用: partition.upsert(data)
    // 返回: results.completed(1, 0)
}
```

#### 2. Engine 集成

```rust
pub struct MysqlServer {
    engine: Arc<Engine>,  // 使用 Engine 而不是单个 Partition
}

// 支持多表查询:
// 1. 从 SQL 中提取表名
// 2. 使用 engine.get_table_meta(table_name) 获取表信息
// 3. 获取对应的 Partition
// 4. 执行查询
```

## 测试数据准备

使用 Rust API 准备测试数据后,通过 MySQL 客户端查询:

```rust
// 准备数据
let partition = /* ... */;

for i in 1..=100 {
    let mut data = HashMap::new();
    data.insert("id".to_string(), i.to_string());
    data.insert("name".to_string(), format!("User_{}", i));
    data.insert("age".to_string(), (20 + i % 50).to_string());
    partition.upsert(data)?;
}
```

然后通过 MySQL 查询:

```sql
SELECT * FROM data LIMIT 10;
SELECT COUNT(*) FROM data;
SELECT AVG(age) FROM data;
SELECT * FROM data WHERE age BETWEEN 30 AND 40;
```

## 性能考虑

1. **查询性能**: 通过 DataFusion 执行,支持向量化处理
2. **连接池**: JDBC 客户端应使用连接池(如 HikariCP)
3. **批量查询**: 使用 LIMIT 和 OFFSET 进行分页查询
4. **索引利用**: 确保查询条件使用了索引字段

## 故障排除

### 连接失败

```bash
# 检查服务器是否在运行
lsof -i :3306

# 检查防火墙设置
# macOS: 系统偏好设置 -> 安全性与隐私 -> 防火墙
```

### 查询错误

- 确保表名正确(当前默认表名为 "data")
- 检查字段名是否匹配 Schema 定义
- 查看服务器日志中的错误信息

### JDBC 连接参数

```java
// 禁用 SSL (Calm 不支持 SSL)
String url = "jdbc:mysql://127.0.0.1:3306/calm?useSSL=false&allowPublicKeyRetrieval=true";
```

## 总结

当前的 MySQL 协议实现提供了**完整的查询支持**,非常适合以下场景:

✅ 数据分析和报表
✅ 与 BI 工具集成
✅ 使用标准 SQL 工具查询数据
✅ JDBC 应用的只读访问

对于写入操作,建议:
- 开发环境: 使用 Rust API
- 生产环境: 等待完整的 DML 支持实现

未来版本将提供完整的 DDL/DML 支持,实现真正的 MySQL 兼容性!
