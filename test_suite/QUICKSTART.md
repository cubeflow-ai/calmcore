# 快速开始指南

5 分钟内完成 Calm 数据库的 SQL 测试。

## 🎯 目标

使用真实的 NYC Taxi 数据集测试 Calm 的 SQL 查询能力，特别是：
- 时间范围查询
- ORDER BY + LIMIT
- 聚合函数（COUNT, AVG）
- GROUP BY

## 📋 步骤

### 1. 编译项目（2 分钟）

```bash
# 在项目根目录
cd ..
cargo build --release
cd test_suite
```

### 2. 安装依赖（1 分钟）

```bash
pip3 install pymysql pandas pyarrow requests
```

### 3. 下载数据集（2 分钟）

```bash
./download_dataset.sh nyc-taxi small
```

这会下载约 100MB 的 Parquet 文件，包含 2024年1月的 NYC 出租车数据（~100万条记录）。

### 4. 启动服务器（30 秒）

在一个终端窗口：

```bash
python3 start_server.py
```

等待看到：
```
✓ Server started (PID: xxxxx)
📊 Server endpoints:
  MySQL: mysql -h 127.0.0.1 -P 3306 -u root
Press Ctrl+C to stop the server
```

### 4. 加载数据（1 分钟）

在另一个终端窗口：

```bash
python3 load_dataset.py nyc-taxi --limit 100000
```

这会加载 10 万条记录到数据库（足够测试了）。

### 5. 运行测试（30 秒）

```bash
python3 test_queries.py nyc-taxi
```

你会看到 12 个 SQL 查询的执行结果和性能数据。

## 🎉 完成！

你应该看到类似这样的输出：

```
=== NYC Taxi Dataset Queries ===

📊 1. Simple SELECT with LIMIT
SQL: SELECT * FROM taxi_trips LIMIT 10
✓ Success: 10 rows in 0.023s
Preview (first 3 rows):
  Row 0: ['trip_0', '2024-01-01 00:35:40', '2024-01-01 00:46:31', 1, 1.7, 11.7, 3.25, 18.55, 1, 186, 234]
  ...

📊 2. COUNT total trips
SQL: SELECT COUNT(*) FROM taxi_trips
✓ Success: 1 rows in 0.156s
Preview (first 1 rows):
  Row 0: [100000]

📊 4. Time range with ORDER BY DESC
SQL: SELECT * FROM taxi_trips WHERE pickup_datetime >= 1704067200000 AND pickup_datetime < 1704153600000 ORDER BY pickup_datetime DESC LIMIT 10
✓ Success: 10 rows in 0.089s
...

=== Test Summary ===

Total tests: 12
Passed: 12
Total time: 1.234s

Performance breakdown:
  ✓ 1. Simple SELECT with LIMIT: 0.023s (10 rows)
  ✓ 2. COUNT total trips: 0.156s (1 rows)
  ✓ 3. Time range query (Jan 1, 2024): 0.078s (100 rows)
  ✓ 4. Time range with ORDER BY DESC: 0.089s (10 rows)
  ...

🎉 All tests passed!
```

## 🔍 重点测试的查询

这个查询类似你提到的 `label_event_v1` 场景：

```sql
SELECT * FROM taxi_trips 
WHERE pickup_datetime >= 1704067200000 
  AND pickup_datetime < 1704153600000 
ORDER BY pickup_datetime DESC 
LIMIT 10
```

它测试了：
- ✅ 时间范围过滤（索引查询）
- ✅ 倒序排序
- ✅ LIMIT 限制
- ✅ 跨多个 partition 的查询

## 📊 数据分布

加载 10 万条记录后：
- **分区数**: 4 个（默认配置）
- **每个分区**: ~25,000 条记录
- **Segments**: 每个分区 2-3 个 segments（每个 segment 50,000 条）
- **时间跨度**: 2024年1月1日的部分数据

## 🚀 下一步

### 测试更大的数据集

```bash
# 加载全部 100 万条
python3 load_dataset.py nyc-taxi

# 或下载更多月份的数据
./download_dataset.sh nyc-taxi medium  # 3个月，~300万条
```

### 自定义查询

编辑 `test_queries.py`，添加你自己的测试用例：

```python
("My custom query",
 "SELECT * FROM taxi_trips WHERE trip_distance > 10.0 LIMIT 20",
 True),
```

### 查看性能指标

服务器日志会显示详细的查询执行信息：
- 索引使用情况
- 扫描的 segments 数量
- 过滤效率

## 🐛 遇到问题？

### 数据加载失败

确保 Parquet 文件已下载：
```bash
ls -lh datasets/
```

### 查询很慢

检查是否创建了索引：
- `pickup_datetime` 应该有 BTree 索引
- 查看服务器日志确认索引被使用

### 连接失败

确保服务器正在运行：
```bash
ps aux | grep calm
netstat -an | grep 3306
```

## 📚 更多信息

- 完整文档: `README.md`
- 数据集信息: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
