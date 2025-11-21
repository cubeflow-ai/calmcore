# SQL 测试套件

这个目录包含了用于测试 Calm 数据库 SQL 查询功能的完整测试套件，支持真实开放数据集。

## 📁 文件说明

### 核心文件
- `download_dataset.sh` - 下载开放数据集（NYC Taxi 等）
- `load_dataset.py` - 加载数据集到数据库
- `test_queries.py` - 执行 SQL 测试用例
- `start_server.py` - 启动测试服务器

### 文档
- `README.md` - 完整文档
- `QUICKSTART.md` - 5分钟快速开始指南
- `PERFORMANCE.md` - 性能优化指南
- `TROUBLESHOOTING.md` - 故障排查指南

## 🚀 快速开始

### 方式 1: 使用真实数据集（推荐）

使用 NYC Taxi 真实数据进行测试：

```bash
cd test_suite

# 1. 安装依赖
pip3 install pymysql pandas pyarrow requests

# 2. 下载数据集（约 100MB，包含 ~100 万条记录）
./download_dataset.sh nyc-taxi small

# 3. 启动服务器（在一个终端）
python3 start_server.py

# 4. 加载数据（在另一个终端）
# 注意：需要同时指定 MySQL 和 GraphQL 端口
python3 load_dataset.py nyc-taxi --limit 100000

# 5. 运行测试
python3 test_queries.py nyc-taxi
```

### 方式 2: 自定义配置

使用自定义端口和数据目录：

```bash
# 启动服务器
python3 start_server.py \
    --data-dir ./my_test_data \
    --mysql-port 3307 \
    --es-port 9201 \
    --log-level debug

# 加载数据（在另一个终端）
python3 load_dataset.py nyc-taxi --port 3307 --limit 50000

# 运行测试
python3 test_queries.py nyc-taxi --port 3307
```

## 📊 测试用例

### NYC Taxi 数据集测试（12 个查询）

1. **简单查询**: `SELECT * FROM taxi_trips LIMIT 10`
2. **COUNT 聚合**: `SELECT COUNT(*) FROM taxi_trips`
3. **时间范围查询**: `WHERE pickup_datetime >= X AND pickup_datetime < Y LIMIT 100`
4. **时间范围 + 排序**: `WHERE ... ORDER BY pickup_datetime DESC LIMIT 10`
5. **条件过滤**: `WHERE passenger_count = 2 LIMIT 50`
6. **GROUP BY 统计**: `SELECT passenger_count, COUNT(*) GROUP BY passenger_count`
7. **AVG 聚合**: `SELECT payment_type, AVG(fare_amount) GROUP BY payment_type`
8. **复杂条件**: `WHERE pickup_datetime >= X AND passenger_count >= 2 AND trip_distance > 5.0`
9. **排序查询**: `ORDER BY total_amount DESC LIMIT 10`
10. **条件排序**: `WHERE tip_amount > 10.0 ORDER BY tip_amount DESC`
11. **TOP N 查询**: `GROUP BY pickup_location_id ORDER BY count DESC LIMIT 10`
12. **全表聚合**: `SELECT AVG(trip_distance) FROM taxi_trips`

### 合成数据测试（8 个查询）

1. **简单查询**: `SELECT * FROM label_event_v1 LIMIT 10`
2. **时间范围查询**: `WHERE updateTime >= X AND updateTime < Y ORDER BY updateTime DESC LIMIT 10`
3. **条件过滤**: `WHERE action = 'click' LIMIT 100`
4. **COUNT 聚合**: `SELECT COUNT(*) FROM label_event_v1`
5. **带条件的 COUNT**: `SELECT COUNT(*) WHERE action = 'view'`
6. **GROUP BY**: `SELECT action, COUNT(*) GROUP BY action`
7. **复杂条件**: `WHERE action = 'click' AND app = 'app_a' LIMIT 50`
8. **排序查询**: `ORDER BY updateTime DESC LIMIT 20`

## 🎯 重点测试的 SQL

当前重点排查的 SQL 语句：

```sql
SELECT * FROM label_event_v1 
WHERE (updateTime >= 1732060800000 AND updateTime < 1732147200000) 
ORDER BY updateTime DESC 
LIMIT 10
```

这个查询测试了：
- 时间范围过滤（BETWEEN 语义）
- 倒序排序
- LIMIT 限制

## 📈 测试数据说明

### NYC Taxi 数据集

**表名**: `taxi_trips`

| 字段 | 类型 | 说明 |
|------|------|------|
| id | VARCHAR(255) | 主键，格式: `trip_0`, `trip_1`, ... |
| pickup_datetime | BIGINT | 上车时间戳（毫秒） |
| dropoff_datetime | BIGINT | 下车时间戳（毫秒） |
| passenger_count | BIGINT | 乘客数量 |
| trip_distance | DOUBLE | 行程距离（英里） |
| fare_amount | DOUBLE | 车费金额 |
| tip_amount | DOUBLE | 小费金额 |
| total_amount | DOUBLE | 总金额 |
| payment_type | BIGINT | 支付类型 |
| pickup_location_id | BIGINT | 上车地点 ID |
| dropoff_location_id | BIGINT | 下车地点 ID |

**数据规模**:
- **Small**: ~100 万条记录（2024年1月）
- **Medium**: ~300 万条记录（2024年1-3月）
- **Large**: ~1200 万条记录（2024年全年）

**数据来源**: [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

### 合成数据（label_event_v1）

| 字段 | 类型 | 说明 |
|------|------|------|
| id | VARCHAR(255) | 主键，格式: `id_0`, `id_1`, ... |
| updateTime | BIGINT | 时间戳（毫秒），从 2025-11-20 开始，每条记录间隔 5 秒 |
| action | VARCHAR(255) | 操作类型: `view`, `click`, `search`, `submit` |
| app | VARCHAR(255) | 应用名: `app_a`, `app_b`, `app_c` |
| contextSize | BIGINT | 上下文大小: 0-999 循环 |

**数据规模**:
- **快速模式**: 1,000 条记录
- **完整模式**: 1,000,000 条记录
- **时间跨度**: 约 5 天（2025-11-20 到 2025-11-25）

## 🔧 依赖要求

### Python 依赖

```bash
# 完整依赖（推荐一次性安装）
pip3 install pymysql pandas pyarrow requests

# 或分开安装
pip3 install pymysql requests  # 基础依赖
pip3 install pandas pyarrow     # 数据集加载
```

### 系统要求

- Rust 1.70+
- Python 3.7+
- 磁盘空间:
  - NYC Taxi Small: ~200MB（数据 + 索引）
  - NYC Taxi Medium: ~600MB
  - NYC Taxi Large: ~2.5GB
  - 合成数据: ~500MB（100万条）

## 📝 测试输出示例

```
=== Calm SQL Client Test ===

Mode: quick (1,000 records)
Connecting to 127.0.0.1:3306...

✓ Connected to server

📋 Creating table 'label_event_v1'...
✓ Table created

📝 Inserting 1,000 test records...
  Progress: 10/10 batches (1,000 records)
✓ Inserted 1,000 records in 2.34s (~427 records/sec)

🔍 Running SQL test cases...

📊 Test 1: Simple SELECT with LIMIT
   SQL: SELECT * FROM label_event_v1 LIMIT 10
   ✓ Success: 10 rows in 0.023s
   Preview (first 3 rows):
     Row 0: ('id_0', 1732060800000, 'view', 'app_a', 0)
     Row 1: ('id_1', 1732060805000, 'click', 'app_b', 1)
     Row 2: ('id_2', 1732060810000, 'search', 'app_c', 2)

...

=== Test Summary ===
Total: 8 tests
Passed: 8

🎉 All tests passed!
```

## 🐛 故障排查

遇到问题？查看详细的故障排查指南：

📖 **[TROUBLESHOOTING.md](TROUBLESHOOTING.md)** - 完整的故障排查文档

### 快速检查清单

1. **服务器是否运行最新代码？**
   ```bash
   # 重新编译
   cd .. && cargo build --release && cd test_suite
   # 重启服务器
   python3 start_server.py
   ```

2. **测试基本功能**
   ```bash
   python3 test_insert.py
   ```

3. **检查端口**
   ```bash
   lsof -i :3306  # MySQL
   lsof -i :8000  # GraphQL
   ```

4. **查看服务器日志**
   - 服务器终端会显示详细的执行日志
   - 使用 `--log-level debug` 获取更多信息

## 🧹 清理

自动化脚本会在退出时自动清理，手动测试需要：

```bash
# 停止服务器（Ctrl+C）

# 清理测试数据
rm -rf ./test_data
rm -rf ./test_data_server
rm -f ./test_server.log
rm -f ./test_server.pid
```

## 📚 更多信息

- 查看项目根目录的 `MYSQL_QUICKSTART.md` 了解 MySQL 协议使用
- 查看 `ELASTICSEARCH_API.md` 了解 Elasticsearch API
- 查看 `QUICK_START.md` 了解快速开始指南
