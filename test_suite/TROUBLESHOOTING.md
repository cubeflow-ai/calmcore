# 故障排查指南

## ❌ 错误: "Only SELECT/INSERT/DELETE/CREATE TABLE/DROP TABLE/SHOW TABLES/SHOW DATABASES/DESCRIBE supported"

### 原因

这个错误通常由以下原因引起：

1. **服务器运行的是旧版本代码**（最常见）
2. SQL 语句格式不正确
3. 客户端发送了不支持的命令

### 解决方案

#### 1. 重新编译并重启服务器

```bash
# 停止当前服务器（在服务器终端按 Ctrl+C）

# 重新编译
cd ..
cargo build --release

# 回到 test_suite 目录
cd test_suite

# 重新启动服务器
python3 start_server.py
```

#### 2. 测试 INSERT 功能

在另一个终端运行测试脚本：

```bash
cd test_suite
python3 test_insert.py
```

预期输出：
```
=== Testing INSERT ===

✓ Connected to MySQL

Test 1: Single INSERT
✓ Single INSERT succeeded

Test 2: Batch INSERT (3 rows)
✓ Batch INSERT succeeded

Test 3: Verify data
✓ Found 4 test rows (expected 4)

🎉 All tests passed!
```

#### 3. 如果测试通过，重新加载数据

```bash
python3 load_dataset.py nyc-taxi --limit 100000
```

## ❌ 错误: "Failed to connect"

### 原因

服务器未启动或端口被占用。

### 解决方案

#### 1. 检查服务器是否运行

```bash
ps aux | grep calm
```

#### 2. 检查端口是否被占用

```bash
lsof -i :3306  # MySQL
lsof -i :8000  # GraphQL
lsof -i :9200  # Elasticsearch
```

#### 3. 使用不同端口

```bash
# 启动服务器
python3 start_server.py --mysql-port 3307 --graphql-port 8001

# 加载数据
python3 load_dataset.py nyc-taxi --port 3307 --graphql-port 8001 --limit 100000
```

## ❌ 错误: "Table not found" 或 "Column not found"

### 原因

表未创建或字段名不匹配。

### 解决方案

#### 1. 检查表是否存在

```bash
mysql -h 127.0.0.1 -P 3306 -u root -e "SHOW TABLES"
```

#### 2. 检查表结构

```bash
mysql -h 127.0.0.1 -P 3306 -u root -e "DESCRIBE taxi_trips"
```

#### 3. 重新创建表

使用 GraphQL Playground 手动创建：

1. 打开浏览器访问: http://127.0.0.1:8000/playground
2. 执行以下 mutation:

```graphql
mutation {
  createTable(input: {
    name: "taxi_trips"
    primaryKey: "id"
    partitionCount: 4
    fields: [
      { name: "id", fieldType: KEYWORD, indexed: true }
      { name: "pickup_datetime", fieldType: TIMESTAMP, indexed: true }
      { name: "dropoff_datetime", fieldType: TIMESTAMP, indexed: true }
      { name: "passenger_count", fieldType: I64, indexed: true }
      { name: "trip_distance", fieldType: F64, indexed: false }
      { name: "fare_amount", fieldType: F64, indexed: false }
      { name: "tip_amount", fieldType: F64, indexed: false }
      { name: "total_amount", fieldType: F64, indexed: false }
      { name: "payment_type", fieldType: I64, indexed: true }
      { name: "pickup_location_id", fieldType: I64, indexed: true }
      { name: "dropoff_location_id", fieldType: I64, indexed: true }
    ]
  }) {
    name
    partitionCount
  }
}
```

## ❌ 错误: "GraphQL request failed"

### 原因

GraphQL 服务器未启动或端口错误。

### 解决方案

#### 1. 确认 GraphQL 端口

检查服务器启动日志：
```
📊 Server endpoints:
  MySQL: mysql -h 127.0.0.1 -P 3306 -u root
  GraphQL: http://127.0.0.1:8000/graphql
  ...
```

#### 2. 测试 GraphQL 连接

```bash
curl http://127.0.0.1:8000/graphql
```

应该返回 GraphQL 错误（说明服务正常）：
```json
{"errors":[{"message":"..."}]}
```

#### 3. 使用浏览器测试

访问: http://127.0.0.1:8000/playground

## 🐛 调试技巧

### 1. 查看服务器日志

服务器会输出详细的日志信息，包括：
- 接收到的 SQL 语句
- 查询执行结果
- 错误信息

### 2. 使用 DEBUG 日志级别

```bash
python3 start_server.py --log-level debug
```

### 3. 手动测试 SQL

使用 MySQL 客户端手动测试：

```bash
mysql -h 127.0.0.1 -P 3306 -u root

# 在 MySQL 提示符下
mysql> SHOW TABLES;
mysql> DESCRIBE taxi_trips;
mysql> INSERT INTO taxi_trips (id, pickup_datetime, ...) VALUES ('test', 1704067200000, ...);
mysql> SELECT * FROM taxi_trips LIMIT 10;
```

### 4. 检查数据文件

```bash
ls -lh test_data/
ls -lh datasets/
```

## 📝 常见问题

### Q: 为什么需要同时使用 GraphQL 和 MySQL？

A: 
- **GraphQL**: 用于创建表（支持完整的字段类型定义，包括 Timestamp）
- **MySQL**: 用于插入数据和查询（更高效的批量操作）

### Q: 可以只用 MySQL 吗？

A: 目前 MySQL 协议的 CREATE TABLE 支持有限，建议使用 GraphQL 创建表。

### Q: 批量 INSERT 有大小限制吗？

A: 建议每批不超过 10,000 条记录，以避免内存问题。

### Q: 数据加载很慢怎么办？

A: 
1. 减少 batch_size（默认 5000）
2. 使用 `--limit` 限制加载的记录数
3. 检查磁盘空间和 I/O 性能

## 🆘 仍然有问题？

1. 查看服务器日志输出
2. 运行 `test_insert.py` 验证基本功能
3. 检查 GitHub Issues
4. 提供完整的错误信息和日志
