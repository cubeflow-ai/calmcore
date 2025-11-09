# Calm MySQL 协议快速启动指南

完整的 MySQL 协议演示,包含数据准备、服务器启动和 JDBC 客户端测试。

## 🚀 一键启动流程

### 第 1 步: 准备测试数据

```bash
cargo run --example prepare_test_data
```

输出示例:
```
🚀 准备 MySQL 演示测试数据...

📋 Schema 创建:
   - 表名: test_data
   - 字段: id (I64), name (Keyword), age (I64), city (Keyword)

📁 数据目录: /tmp/mysql_demo
✅ Partition 创建完成

💾 插入测试数据...
   已插入 20 条记录...
   已插入 40 条记录...
   已插入 60 条记录...
   已插入 80 条记录...
   已插入 100 条记录...
   ✓ 总共插入 100 条记录

💾 将数据持久化到磁盘...
   ✓ 数据已 flush (segment_id: 1)

📊 数据统计:
   - 总记录数: 100
   - ID 范围: 1-100
   - 年龄范围: 20-69
   - 城市数量: 6

✅ 测试数据准备完成!
```

### 第 2 步: 启动 MySQL 服务器

在**新终端窗口**运行:

```bash
cargo run --example mysql_server
```

服务器启动输出:
```
=== Calm MySQL Protocol Server Demo ===

Partition created with 1 segments
MySQL server listening on 127.0.0.1:3306

等待客户端连接...
```

保持此窗口运行!

### 第 3 步: 测试 MySQL 客户端 (可选)

在**另一个终端窗口**:

```bash
mysql -h 127.0.0.1 -P 3306
```

运行测试查询:

```sql
-- 显示数据库
SHOW DATABASES;

-- 切换数据库
USE calm;

-- 显示表
SHOW TABLES;

-- 查询所有数据 (前10条)
SELECT * FROM data LIMIT 10;

-- 条件查询
SELECT * FROM data WHERE age > 40 LIMIT 10;

-- 统计查询
SELECT COUNT(*) as total FROM data;

-- 聚合查询
SELECT 
    COUNT(*) as total,
    AVG(age) as avg_age,
    MIN(age) as min_age,
    MAX(age) as max_age
FROM data;

-- 按城市统计
SELECT city, COUNT(*) as count 
FROM data 
GROUP BY city;

-- 退出
exit;
```

### 第 4 步: 运行 JDBC 客户端

在**第三个终端窗口**:

```bash
cd examples/jdbc-demo
mvn clean compile exec:java
```

预期输出:

```
╔══════════════════════════════════════════════════════════════╗
║          Calm Database JDBC 连接演示                        ║
╚══════════════════════════════════════════════════════════════╝

📡 Testing database connection...
   ✓ Connected to: MySQL
   ✓ Driver: MySQL Connector/J mysql-connector-java-8.0.33
   ✓ URL: jdbc:mysql://127.0.0.1:3306/calm?...

📋 Listing databases and tables...
   Databases:
   - calm

   Tables:
   - data

📊 Querying all data from 'data' table...
   id             name           age            city           
   ─────────────────────────────────────────────────────────────
   1              用户_1         21             上海           
   2              用户_2         22             广州           
   ...
   Total rows: 10

🔍 Querying data with condition (age > 25)...
   ID         Name                 Age        City       
   ──────────────────────────────────────────────────────
   6          用户_6               26         北京       
   7          用户_7               27         上海       
   ...
   Found 45 rows where age > 25

📈 Running aggregate queries...
   Total records: 100
   Average age: 44.50
   Age range: 21 - 69

✅ All tests completed successfully!
```

## 📁 项目结构

```
examples/
├── prepare_test_data.rs      # 步骤1: 数据准备脚本
├── mysql_server.rs            # 步骤2: MySQL 服务器
├── jdbc-demo/                 # 步骤4: JDBC 客户端
│   ├── pom.xml                # Maven 配置
│   └── src/main/java/com/calm/demo/
│       └── CalmJdbcDemo.java  # Java 客户端代码
└── README_MYSQL_DEMO.md       # 详细文档
```

## 🔧 常见问题

### Q: 端口 3306 被占用

```bash
# 检查端口占用
lsof -i :3306

# 停止已有 MySQL 服务
brew services stop mysql
# 或
sudo systemctl stop mysql
```

### Q: 数据准备失败

```bash
# 清理旧数据重试
rm -rf /tmp/mysql_demo
cargo run --example prepare_test_data
```

### Q: JDBC 连接失败

确保:
1. MySQL 服务器正在运行 (`cargo run --example mysql_server`)
2. 数据已准备 (`cargo run --example prepare_test_data`)
3. 端口 3306 可访问

### Q: Maven 编译失败

```bash
# 清理 Maven 缓存
cd examples/jdbc-demo
mvn clean

# 重新编译
mvn compile

# 检查 Java 版本 (需要 Java 11+)
java -version
```

## 📊 支持的 SQL 操作

| SQL 命令 | 状态 | 示例 |
|---------|------|------|
| `SHOW DATABASES` | ✅ 支持 | `SHOW DATABASES;` |
| `SHOW TABLES` | ✅ 支持 | `SHOW TABLES;` |
| `USE database` | ✅ 支持 | `USE calm;` |
| `SELECT *` | ✅ 支持 | `SELECT * FROM data LIMIT 10;` |
| `SELECT ... WHERE` | ✅ 支持 | `SELECT * FROM data WHERE age > 25;` |
| `SELECT COUNT(*)` | ✅ 支持 | `SELECT COUNT(*) FROM data;` |
| `SELECT ... GROUP BY` | ✅ 支持 | `SELECT city, COUNT(*) FROM data GROUP BY city;` |
| `INSERT` | ⏳ 计划中 | `INSERT INTO data VALUES (...);` |
| `UPDATE` | ⏳ 计划中 | `UPDATE data SET age = 30 WHERE id = 1;` |
| `DELETE` | ⏳ 计划中 | `DELETE FROM data WHERE id = 1;` |
| `CREATE TABLE` | ⏳ 计划中 | `CREATE TABLE users (...);` |
| `DROP TABLE` | ⏳ 计划中 | `DROP TABLE data;` |

## 🎯 架构说明

```
MySQL Client (CLI/JDBC)
         ↓
  TCP Port 3306
         ↓
   MysqlServer
         ↓
   CalmBackend (MysqlShim)
         ↓
     Partition
         ↓
   DataFusion Engine
         ↓
  Arrow Record Batches
```

## 📚 深入学习

- 详细文档: `examples/README_MYSQL_DEMO.md`
- 协议指南: `MYSQL_PROTOCOL_GUIDE.md`
- 代码示例: `examples/CalmJdbcDemo.java`

## 🔮 后续计划

1. **INSERT/UPDATE/DELETE 支持**
   - SQL 解析
   - 路由到 Partition 操作
   - 返回影响行数

2. **CREATE/DROP TABLE 支持**
   - 集成 Engine API
   - 多表管理
   - Schema 定义

3. **高级特性**
   - PreparedStatement 参数绑定
   - 事务支持 (BEGIN/COMMIT/ROLLBACK)
   - 多表 JOIN
   - 索引优化

## 💡 提示

- 使用 `LIMIT` 控制返回结果数量
- WHERE 条件使用索引字段性能更好
- 当前只支持只读查询
- 写入操作请使用 Rust API (参考 `prepare_test_data.rs`)

---

**快速启动完成!** 🎉

如有问题,请查看详细文档或提交 Issue。
