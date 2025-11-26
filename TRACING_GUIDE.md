# 查询链路追踪功能使用指南

## 功能简介

CalmCore 现在支持查询链路追踪功能,可以详细记录每个查询的执行过程和每一步的耗时。这对于性能调优和问题诊断非常有用。

## 开启/关闭追踪

### 方法 1: 通过 SQL 命令(推荐)

连接到 MySQL 服务后,可以通过 SQL 命令控制追踪:

```sql
-- 开启追踪
SET TRACING ON;

-- 关闭追踪  
SET TRACING OFF;

-- 切换追踪状态(开<->关)
SET TRACING;
```

### 方法 2: 通过代码

在代码中可以调用:

```rust
use calm::utils::tracing::{enable_tracing, disable_tracing, toggle_tracing};

// 开启追踪
enable_tracing();

// 关闭追踪
disable_tracing();

// 切换状态
let is_enabled = toggle_tracing();
```

## 使用示例

### 1. 开启追踪并执行查询

```bash
# 启动服务
cargo run --example mysql_server
```

```sql
-- 连接到数据库后
mysql> SET TRACING ON;
Query OK, 0 rows affected (0.00 sec)

-- 执行查询
mysql> SELECT COUNT(*) FROM taxi_trips WHERE passenger_count = 2;
+----------+
| COUNT(*) |
+----------+
|  1234567 |
+----------+
1 row in set (0.15 sec)
```

### 2. 查看追踪报告

执行查询后,服务端日志会输出详细的追踪报告:

```
[INFO ] ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
[INFO ] 📊 Query Trace Report: query-SELECT COUNT(*) FROM taxi_trips WHERE pa
[INFO ]    Total Time: 152.345ms
[INFO ] ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
[INFO ]   [  0] SQL Execution                            | +   0.000ms |  150.123ms ( 98.5%)
[INFO ]         └─ query: SELECT COUNT(*) FROM taxi_trips WHERE passenger_count = 2
[INFO ]   [  1] Write Result                             | + 150.234ms |    2.111ms (  1.4%)
[INFO ]         └─ rows: 1
[INFO ]         └─ columns: 1
[INFO ] ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
```

### 3. 关闭追踪

```sql
mysql> SET TRACING OFF;
Query OK, 0 rows affected (0.00 sec)
```

## 追踪报告说明

追踪报告包含以下信息:

- **Query Trace Report**: 查询ID(SQL前50个字符)
- **Total Time**: 总执行时间
- **每一步信息**:
  - `[序号]` 步骤名称
  - `+时间` 相对于查询开始的时间偏移
  - `耗时` 该步骤的执行时间
  - `(百分比%)` 占总时间的百分比
  - `└─ key: value` 该步骤的元数据(如行数、列数、查询语句等)

## 性能影响

- **开启追踪**: 会有极小的性能开销(<1%),主要用于记录时间戳
- **关闭追踪**: 几乎零开销,只是一个布尔值检查
- **建议**: 
  - 开发/测试环境: 可以始终开启
  - 生产环境: 按需开启,用于问题诊断

## 配合日志级别使用

追踪报告使用 `INFO` 级别输出,配合日志级别设置:

```bash
# 只看追踪报告,不看其他详细日志
RUST_LOG=info cargo run --example mysql_server

# 看追踪报告和所有详细日志
RUST_LOG=debug cargo run --example mysql_server
```

## 典型使用场景

### 1. 性能分析

```sql
SET TRACING ON;

-- 执行慢查询
SELECT * FROM taxi_trips 
WHERE pickup_datetime >= 1704067200000 
  AND passenger_count >= 2 
  AND trip_distance > 5.0 
ORDER BY pickup_datetime DESC 
LIMIT 100;

-- 查看哪一步最慢
```

### 2. 对比优化效果

```sql
SET TRACING ON;

-- 优化前
SELECT COUNT(*) FROM taxi_trips WHERE id LIKE 't%';

-- 记录耗时

-- 优化后再执行
SELECT COUNT(*) FROM taxi_trips WHERE id LIKE 't%';

-- 对比两次追踪报告
```

### 3. 问题诊断

当查询返回错误结果或性能异常时:

```sql
SET TRACING ON;

-- 执行问题查询
SELECT ... 

-- 查看追踪报告,定位问题环节
```

## 限制

- 追踪是全局开关,影响所有连接
- 目前只追踪 SELECT 查询的主要步骤
- 不追踪 INSERT/DELETE/CREATE TABLE 等命令
- 追踪报告输出到服务端日志,不返回给客户端

## 未来扩展

计划支持:

- [ ] 更细粒度的追踪(每个分区扫描、每个索引查找等)
- [ ] 追踪结果可查询(SELECT * FROM tracing_history)
- [ ] 每连接独立的追踪开关
- [ ] 追踪结果导出到文件
- [ ] 可视化追踪时间线

## 示例输出

完整的追踪报告示例:

```
[2025-11-26 10:15:30 INFO ] 🔍 Query tracing enabled
[2025-11-26 10:15:35 INFO ] 📥 [Executor] Received SQL: SELECT COUNT(*) FROM taxi_trips WHERE passenger_count = 2
[2025-11-26 10:15:35 INFO ] 🔢 [COUNT] with WHERE clause
[2025-11-26 10:15:35 INFO ] 🎯 [SegmentScanner] hit=1234567/2819998 (43.8%), limit=None, sort=None
[2025-11-26 10:15:35 INFO ] ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
[2025-11-26 10:15:35 INFO ] 📊 Query Trace Report: query-SELECT COUNT(*) FROM taxi_trips WHERE pa
[2025-11-26 10:15:35 INFO ]    Total Time: 152.345ms
[2025-11-26 10:15:35 INFO ] ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
[2025-11-26 10:15:35 INFO ]   [  0] SQL Execution                            | +   0.000ms |  150.123ms ( 98.5%)
[2025-11-26 10:15:35 INFO ]         └─ query: SELECT COUNT(*) FROM taxi_trips WHERE passenger_count = 2
[2025-11-26 10:15:35 INFO ]   [  1] Write Result                             | + 150.234ms |    2.111ms (  1.4%)
[2025-11-26 10:15:35 INFO ]         └─ rows: 1
[2025-11-26 10:15:35 INFO ]         └─ columns: 1
[2025-11-26 10:15:35 INFO ] ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
```
