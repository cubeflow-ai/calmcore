# Calm MySQL 协议演示

本目录包含 Calm 数据库的 MySQL 协议演示,包括 Rust 服务器端和 Java JDBC 客户端。

## 目录结构

```
examples/
├── mysql_server.rs          # Rust MySQL 服务器示例
├── CalmJdbcDemo.java        # Java JDBC 客户端示例  
├── jdbc-demo/
│   └── pom.xml              # Maven 配置文件
└── README_MYSQL_DEMO.md     # 本文件
```

## 快速开始

### 步骤 1: 准备测试数据

首先需要通过 Rust API 准备一些测试数据:

```rust
// 创建一个脚本准备数据 (examples/prepare_test_data.rs)
use calm::{
    partition::Partition,
    schema::{field::FieldOption, Schema},
};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::mpsc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 创建 Schema
    let schema = Schema {
        name: "test_data".to_string(),
        primary_key: None,
        store_source: false,
        fields: vec![
            FieldOption::I64 {
                name: "id".to_string(),
                index: true,
            },
            FieldOption::Keyword {
                name: "name".to_string(),
                index: true,
                is_array: false,
                persist_option: None,
                case_sensitive: true,
            },
            FieldOption::I64 {
                name: "age".to_string(),
                index: true,
            },
        ],
        persist_policy: Default::default(),
    };
    
    // 创建 Partition
    let (tx, _rx) = mpsc::unbounded_channel();
    let partition = Arc::new(Partition::new(
        0,
        PathBuf::from("/tmp/mysql_demo"),
        schema,
        tx,
    ));
    
    // 插入测试数据
    println!("插入 100 条测试数据...");
    for i in 1..=100 {
        let mut data = HashMap::new();
        data.insert("id".to_string(), i.to_string());
        data.insert("name".to_string(), format!("User_{}", i));
        data.insert("age".to_string(), (20 + (i % 50)).to_string());
        partition.upsert(data)?;
    }
    
    // Flush 数据
    partition.flush()?;
    
    println!("✅ 测试数据准备完成!");
    Ok(())
}
```

运行准备数据脚本:

```bash
cargo run --example prepare_test_data
```

### 步骤 2: 启动 MySQL 服务器

```bash
cargo run --example mysql_server
```

服务器将在 `127.0.0.1:3306` 启动,输出类似:

```
=== Calm MySQL Protocol Server Demo ===

Partition created with 0 segments
MySQL server listening on 127.0.0.1:3306
```

保持此终端窗口运行。

### 步骤 3: 使用 MySQL 客户端测试 (可选)

在另一个终端窗口:

```bash
mysql -h 127.0.0.1 -P 3306
```

执行一些测试查询:

```sql
mysql> SHOW DATABASES;
+----------+
| Database |
+----------+
| calm     |
+----------+

mysql> USE calm;

mysql> SHOW TABLES;
+----------------+
| Tables_in_calm |
+----------------+
| data           |
+----------------+

mysql> SELECT * FROM data LIMIT 5;
+----+---------+-----+
| id | name    | age |
+----+---------+-----+
|  1 | User_1  |  21 |
|  2 | User_2  |  22 |
|  3 | User_3  |  23 |
|  4 | User_4  |  24 |
|  5 | User_5  |  25 |
+----+---------+-----+

mysql> SELECT COUNT(*) FROM data;
+----------+
| COUNT(*) |
+----------+
|      100 |
+----------+

mysql> SELECT AVG(age) FROM data;
+----------+
| AVG(age) |
+----------+
|    44.50 |
+----------+

mysql> SELECT * FROM data WHERE age > 40 LIMIT 10;
```

### 步骤 4: 运行 JDBC 客户端

#### 方法 1: 使用 Maven

```bash
cd examples/jdbc-demo

# 确保 pom.xml 和 CalmJdbcDemo.java 在正确位置
cp ../CalmJdbcDemo.java src/main/java/

# 编译并运行
mvn clean compile exec:java
```

#### 方法 2: 直接编译运行

首先下载 MySQL Connector/J:

```bash
cd examples
wget https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.33/mysql-connector-java-8.0.33.jar
```

然后编译运行:

```bash
# 编译
javac -cp mysql-connector-java-8.0.33.jar CalmJdbcDemo.java

# 运行
java -cp .:mysql-connector-java-8.0.33.jar CalmJdbcDemo
```

### 预期输出

JDBC 客户端应该输出类似:

```
╔══════════════════════════════════════════════════════════════╗
║          Calm Database JDBC 连接演示                        ║
╚══════════════════════════════════════════════════════════════╝

📡 Testing database connection...
   ✓ Connected to: MySQL
   ✓ Driver: MySQL Connector/J mysql-connector-java-8.0.33
   ✓ URL: jdbc:mysql://127.0.0.1:3306/calm?useSSL=false&allowPublicKeyRetrieval=true

📋 Listing databases and tables...

   Databases:
   - calm

   Tables:
   - data

📊 Querying all data from 'data' table...

   id             name           age            
   ───────────────────────────────────────────
   1              User_1         21             
   2              User_2         22             
   ...

   Total rows: 10

🔍 Querying data with condition (age > 25)...

   ID         Name                 Age       
   ──────────────────────────────────────────
   6          User_6               26        
   7          User_7               27        
   ...

   Found 10 rows where age > 25

📈 Running aggregate queries...
   Total records: 100
   Average age: 44.50
   Age range: 21 - 69

✅ All tests completed successfully!
```

## 支持的 SQL 操作

### ✅ 已支持

- `SHOW DATABASES`
- `SHOW TABLES`
- `USE database`
- `SELECT * FROM table`
- `SELECT col1, col2 FROM table WHERE condition`
- `SELECT COUNT(*), AVG(col), MIN(col), MAX(col) FROM table`
- `SELECT * FROM table LIMIT n`

### ⏳ 计划支持 (需要扩展)

- `INSERT INTO table VALUES (...)`
- `UPDATE table SET ... WHERE ...`
- `DELETE FROM table WHERE ...`
- `CREATE TABLE ...`
- `DROP TABLE ...`

## 架构说明

```
                  JDBC Client
                      ↓
              MySQL Protocol (3306)
                      ↓
          CalmBackend (MysqlShim)
                      ↓
              Partition (Data)
                      ↓
           DataFusion (Query Engine)
                      ↓
        Arrow Record Batches (Results)
```

## 当前限制

1. **只读查询**: 当前主要支持 SELECT 查询
2. **单 Partition**: 绑定到单个 Partition,不支持多表
3. **简单过滤**: WHERE 条件通过 DataFusion 处理
4. **写入操作**: INSERT/UPDATE/DELETE 需要通过 Rust API

## 扩展建议

### 1. 添加 INSERT 支持

在 `src/protocol/mysql/mod.rs` 中:

```rust
if query_lower.starts_with("insert") {
    return self.handle_insert(query, results);
}

fn handle_insert<W: io::Read + io::Write>(
    &mut self,
    query: &str,
    results: QueryResultWriter<W>,
) -> io::Result<()> {
    // 解析 SQL: INSERT INTO table (col1, col2) VALUES (val1, val2);
    // 调用: self.partition.upsert(data)
    // 返回: results.completed(1, 0)
}
```

### 2. 集成 Engine 支持多表

```rust
pub struct MysqlServer {
    engine: Arc<Engine>,  // 使用 Engine 而不是 Partition
}

// 从 SQL 提取表名并路由到对应的 Partition
```

### 3. 使用 SQL Parser

```toml
[dependencies]
sqlparser = "0.35"
```

```rust
use sqlparser::parser::Parser;
use sqlparser::dialect::MySqlDialect;

let dialect = MySqlDialect {};
let ast = Parser::parse_sql(&dialect, query)?;
// 处理解析后的 AST
```

## 故障排除

### 问题: 连接被拒绝

```
ERROR 2003 (HY000): Can't connect to MySQL server on '127.0.0.1:3306'
```

**解决方案**:
- 确认 MySQL 服务器正在运行 (`cargo run --example mysql_server`)
- 检查端口 3306 是否被占用 (`lsof -i :3306`)

### 问题: Table 'data' doesn't exist

**解决方案**:
- 确认已运行数据准备脚本
- 检查数据目录 `/tmp/mysql_demo` 是否存在

### 问题: No data returned

**解决方案**:
- 确认数据已经 flush: `partition.flush()?`
- 检查查询条件是否正确

## 性能提示

1. **使用连接池**: 在生产环境使用 HikariCP
2. **批量查询**: 使用 `LIMIT` 和 `OFFSET` 分页
3. **索引利用**: 确保 WHERE 条件使用索引字段
4. **结果集大小**: 避免 `SELECT *`,只查询需要的字段

## 后续开发

- [ ] 完整的 INSERT/UPDATE/DELETE 支持
- [ ] CREATE TABLE / DROP TABLE 支持
- [ ] 多表查询和 JOIN
- [ ] 事务支持 (BEGIN/COMMIT/ROLLBACK)
- [ ] PreparedStatement 参数绑定
- [ ] 更好的错误消息
- [ ] 性能优化和查询计划

## 参考资料

- [MySQL Protocol Documentation](https://dev.mysql.com/doc/internals/en/client-server-protocol.html)
- [MySQL Connector/J Documentation](https://dev.mysql.com/doc/connector-j/8.0/en/)
- [DataFusion Query Engine](https://github.com/apache/arrow-datafusion)
- [msql-srv Crate](https://docs.rs/msql-srv/)

## 贡献

欢迎提交 PR 来扩展 MySQL 协议支持!重点领域:

1. DML 操作解析和执行
2. DDL 操作支持
3. 多表查询
4. 性能优化

---

如有问题,请查看 `MYSQL_PROTOCOL_GUIDE.md` 或提交 Issue。
