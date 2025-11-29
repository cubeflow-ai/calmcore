# taxi_trips 表测试用例详细说明

## 测试用例总览

本测试套件包含 **170 条**自动生成的测试用例 + **16 条**手动编写的测试用例，总计 **186 条**测试。

所有测试用例都基于 `taxi_trips` 表的实际数据，确保测试的实用性和有效性。

---

## 1. 聚合函数测试 (29 条)

### 基础聚合
```sql
-- COUNT 系列
SELECT COUNT(*) FROM taxi_trips
SELECT COUNT(DISTINCT passenger_count) FROM taxi_trips
SELECT COUNT(DISTINCT payment_type) FROM taxi_trips
SELECT COUNT(DISTINCT pickup_location_id) FROM taxi_trips

-- SUM 系列
SELECT SUM(trip_distance) FROM taxi_trips
SELECT SUM(fare_amount) FROM taxi_trips
SELECT SUM(tip_amount) FROM taxi_trips
SELECT SUM(total_amount) FROM taxi_trips

-- AVG 系列
SELECT AVG(trip_distance) FROM taxi_trips
SELECT AVG(fare_amount) FROM taxi_trips
SELECT AVG(tip_amount) FROM taxi_trips

-- MIN/MAX 系列
SELECT MIN(trip_distance) FROM taxi_trips
SELECT MAX(trip_distance) FROM taxi_trips
SELECT MIN(fare_amount), MAX(fare_amount) FROM taxi_trips
SELECT MIN(pickup_datetime), MAX(pickup_datetime) FROM taxi_trips
```

### 多列聚合
```sql
SELECT SUM(fare_amount), AVG(fare_amount) FROM taxi_trips
SELECT COUNT(*), SUM(trip_distance), AVG(trip_distance) FROM taxi_trips
SELECT MIN(fare_amount), MAX(fare_amount), AVG(fare_amount) FROM taxi_trips
```

### 条件聚合
```sql
SELECT COUNT(*) FROM taxi_trips WHERE trip_distance > 0
SELECT AVG(fare_amount) FROM taxi_trips WHERE fare_amount > 0
SELECT SUM(tip_amount) FROM taxi_trips WHERE tip_amount > 0
```

---

## 2. GROUP BY 测试 (25 条)

### 单列分组
```sql
SELECT passenger_count, COUNT(*) FROM taxi_trips 
GROUP BY passenger_count

SELECT passenger_count, COUNT(*) FROM taxi_trips 
GROUP BY passenger_count 
ORDER BY passenger_count

SELECT payment_type, COUNT(*) FROM taxi_trips 
GROUP BY payment_type 
ORDER BY COUNT(*) DESC

SELECT pickup_location_id, COUNT(*) FROM taxi_trips 
GROUP BY pickup_location_id 
LIMIT 20
```

### 带聚合函数的分组
```sql
SELECT passenger_count, AVG(trip_distance) FROM taxi_trips 
GROUP BY passenger_count

SELECT passenger_count, SUM(fare_amount) FROM taxi_trips 
GROUP BY passenger_count

SELECT passenger_count, MIN(fare_amount), MAX(fare_amount) FROM taxi_trips 
GROUP BY passenger_count

SELECT payment_type, AVG(fare_amount) FROM taxi_trips 
GROUP BY payment_type
```

### 多列分组
```sql
SELECT passenger_count, payment_type, COUNT(*) FROM taxi_trips 
GROUP BY passenger_count, payment_type

SELECT pickup_location_id, dropoff_location_id, COUNT(*) FROM taxi_trips 
GROUP BY pickup_location_id, dropoff_location_id 
LIMIT 50

SELECT pickup_location_id, dropoff_location_id, AVG(trip_distance) FROM taxi_trips 
GROUP BY pickup_location_id, dropoff_location_id 
LIMIT 50
```

### HAVING 子句
```sql
SELECT passenger_count, COUNT(*) as cnt FROM taxi_trips 
GROUP BY passenger_count 
HAVING cnt > 100

SELECT passenger_count, AVG(trip_distance) as avg_dist FROM taxi_trips 
GROUP BY passenger_count 
HAVING avg_dist > 2.0

SELECT pickup_location_id, COUNT(*) as cnt FROM taxi_trips 
GROUP BY pickup_location_id 
HAVING cnt > 100 
ORDER BY cnt DESC 
LIMIT 20
```

---

## 3. WHERE 条件测试 (44 条)

### 等值查询
```sql
SELECT COUNT(*) FROM taxi_trips WHERE passenger_count = 1
SELECT COUNT(*) FROM taxi_trips WHERE passenger_count = 2
SELECT COUNT(*) FROM taxi_trips WHERE payment_type = 1
```

### 比较查询
```sql
SELECT COUNT(*) FROM taxi_trips WHERE passenger_count > 2
SELECT COUNT(*) FROM taxi_trips WHERE passenger_count >= 2
SELECT COUNT(*) FROM taxi_trips WHERE trip_distance > 5.0
SELECT COUNT(*) FROM taxi_trips WHERE fare_amount >= 20.0
SELECT COUNT(*) FROM taxi_trips WHERE passenger_count != 1
```

### BETWEEN 查询
```sql
SELECT COUNT(*) FROM taxi_trips WHERE passenger_count BETWEEN 2 AND 4
SELECT COUNT(*) FROM taxi_trips WHERE trip_distance BETWEEN 1.0 AND 5.0
SELECT COUNT(*) FROM taxi_trips WHERE fare_amount BETWEEN 10.0 AND 20.0
```

### IN 查询
```sql
SELECT COUNT(*) FROM taxi_trips WHERE passenger_count IN (1, 2, 3)
SELECT COUNT(*) FROM taxi_trips WHERE passenger_count NOT IN (1, 2)
SELECT COUNT(*) FROM taxi_trips WHERE payment_type IN (1, 2)
```

### AND 组合条件
```sql
SELECT COUNT(*) FROM taxi_trips 
WHERE trip_distance > 5.0 AND fare_amount > 10.0

SELECT COUNT(*) FROM taxi_trips 
WHERE passenger_count = 1 AND trip_distance > 5.0

SELECT COUNT(*) FROM taxi_trips 
WHERE passenger_count > 0 AND trip_distance > 0 AND fare_amount > 0
```

### OR 组合条件
```sql
SELECT COUNT(*) FROM taxi_trips 
WHERE trip_distance > 10.0 OR fare_amount > 20.0

SELECT COUNT(*) FROM taxi_trips 
WHERE passenger_count = 1 OR passenger_count = 2
```

### 边界值测试
```sql
SELECT COUNT(*) FROM taxi_trips WHERE fare_amount = 0
SELECT COUNT(*) FROM taxi_trips WHERE tip_amount = 0
SELECT COUNT(*) FROM taxi_trips WHERE trip_distance = 0
SELECT COUNT(*) FROM taxi_trips WHERE passenger_count = 0
```

### 大值测试
```sql
SELECT COUNT(*) FROM taxi_trips WHERE tip_amount > 10.0
SELECT COUNT(*) FROM taxi_trips WHERE trip_distance > 50.0
SELECT COUNT(*) FROM taxi_trips WHERE fare_amount > 100.0
```

---

## 4. ORDER BY & LIMIT 测试 (34 条)

### 单列排序
```sql
SELECT * FROM taxi_trips ORDER BY passenger_count LIMIT 10
SELECT * FROM taxi_trips ORDER BY passenger_count DESC LIMIT 10
SELECT * FROM taxi_trips ORDER BY trip_distance LIMIT 10
SELECT * FROM taxi_trips ORDER BY fare_amount DESC LIMIT 10
SELECT * FROM taxi_trips ORDER BY pickup_datetime LIMIT 10
```

### 多列排序
```sql
SELECT * FROM taxi_trips 
ORDER BY passenger_count, trip_distance LIMIT 10

SELECT * FROM taxi_trips 
ORDER BY passenger_count DESC, trip_distance DESC LIMIT 10

SELECT * FROM taxi_trips 
ORDER BY payment_type, fare_amount LIMIT 10
```

### 不同 LIMIT 值
```sql
SELECT * FROM taxi_trips LIMIT 5
SELECT * FROM taxi_trips LIMIT 20
SELECT * FROM taxi_trips LIMIT 100
SELECT * FROM taxi_trips LIMIT 1000
```

### OFFSET 分页
```sql
SELECT * FROM taxi_trips LIMIT 10 OFFSET 0
SELECT * FROM taxi_trips LIMIT 10 OFFSET 10
SELECT * FROM taxi_trips LIMIT 100 OFFSET 50
SELECT * FROM taxi_trips ORDER BY passenger_count LIMIT 50 OFFSET 100
```

### 带 WHERE 的排序
```sql
SELECT * FROM taxi_trips 
WHERE passenger_count > 0 
ORDER BY trip_distance LIMIT 20

SELECT * FROM taxi_trips 
WHERE trip_distance > 5.0 
ORDER BY fare_amount DESC LIMIT 20
```

---

## 5. DISTINCT 测试 (16 条)

### 单列去重
```sql
SELECT DISTINCT passenger_count FROM taxi_trips
SELECT DISTINCT passenger_count FROM taxi_trips ORDER BY passenger_count
SELECT DISTINCT payment_type FROM taxi_trips
SELECT DISTINCT pickup_location_id FROM taxi_trips LIMIT 50
```

### 多列去重
```sql
SELECT DISTINCT passenger_count, payment_type FROM taxi_trips
SELECT DISTINCT passenger_count, payment_type FROM taxi_trips 
ORDER BY passenger_count, payment_type

SELECT DISTINCT pickup_location_id, dropoff_location_id FROM taxi_trips 
LIMIT 100
```

### COUNT DISTINCT
```sql
SELECT COUNT(DISTINCT passenger_count) FROM taxi_trips
SELECT COUNT(DISTINCT payment_type) FROM taxi_trips
SELECT COUNT(DISTINCT pickup_location_id) FROM taxi_trips
```

### DISTINCT with WHERE
```sql
SELECT DISTINCT passenger_count FROM taxi_trips 
WHERE trip_distance > 5.0

SELECT COUNT(DISTINCT passenger_count) FROM taxi_trips 
WHERE trip_distance > 0
```

---

## 6. 复杂查询测试 (22 条)

### 子查询 - MAX/MIN
```sql
SELECT * FROM taxi_trips 
WHERE passenger_count = (SELECT MAX(passenger_count) FROM taxi_trips) 
LIMIT 10

SELECT * FROM taxi_trips 
WHERE trip_distance = (SELECT MAX(trip_distance) FROM taxi_trips)

SELECT * FROM taxi_trips 
WHERE fare_amount = (SELECT MAX(fare_amount) FROM taxi_trips)
```

### 子查询 - AVG
```sql
SELECT * FROM taxi_trips 
WHERE trip_distance > (SELECT AVG(trip_distance) FROM taxi_trips) 
LIMIT 20

SELECT COUNT(*) FROM taxi_trips 
WHERE fare_amount > (SELECT AVG(fare_amount) FROM taxi_trips)
```

### CASE WHEN
```sql
SELECT passenger_count, 
       CASE WHEN trip_distance < 2 THEN 'short' 
            WHEN trip_distance < 5 THEN 'medium' 
            ELSE 'long' 
       END as distance_category 
FROM taxi_trips LIMIT 20

SELECT payment_type, 
       CASE WHEN fare_amount < 10 THEN 'cheap' 
            WHEN fare_amount < 20 THEN 'normal' 
            ELSE 'expensive' 
       END as fare_category 
FROM taxi_trips LIMIT 20
```

### UNION
```sql
SELECT passenger_count FROM taxi_trips WHERE passenger_count = 1 
UNION 
SELECT passenger_count FROM taxi_trips WHERE passenger_count = 2
```

### 复杂 GROUP BY + HAVING
```sql
SELECT passenger_count, 
       COUNT(*) as cnt, 
       AVG(trip_distance) as avg_dist, 
       SUM(fare_amount) as total_fare 
FROM taxi_trips 
GROUP BY passenger_count 
HAVING cnt > 50 
ORDER BY total_fare DESC

SELECT pickup_location_id, 
       COUNT(*) as cnt, 
       AVG(trip_distance) as avg_dist 
FROM taxi_trips 
GROUP BY pickup_location_id 
HAVING cnt > 50 
ORDER BY avg_dist DESC 
LIMIT 20
```

### 多表达式计算
```sql
SELECT passenger_count, trip_distance, fare_amount, 
       (fare_amount / NULLIF(trip_distance, 0)) as fare_per_mile 
FROM taxi_trips 
WHERE trip_distance > 0 
LIMIT 20

SELECT passenger_count, 
       SUM(fare_amount) as total_fare, 
       COUNT(*) as trip_count, 
       (SUM(fare_amount) / COUNT(*)) as avg_fare_per_trip 
FROM taxi_trips 
GROUP BY passenger_count
```

---

## 7. 手动编写测试 (16 条)

这些测试用例是针对特定业务场景手动编写的：

```sql
-- 时间范围查询
SELECT COUNT(*) FROM taxi_trips 
WHERE pickup_datetime >= 1704067200000 
  AND pickup_datetime < 1704153600000

-- 复杂过滤条件
SELECT COUNT(*) FROM taxi_trips 
WHERE pickup_datetime >= 1704067200000 
  AND pickup_datetime < 1704672000000 
  AND passenger_count >= 2 
  AND trip_distance > 5.0

-- LIKE 模式匹配
SELECT id, pickup_datetime, passenger_count FROM taxi_trips 
WHERE pickup_datetime >= 1704067200000 
  AND pickup_datetime < 1704153600000 
  AND id LIKE '%1%' 
ORDER BY pickup_datetime DESC 
LIMIT 10 OFFSET 10

-- Top N 查询
SELECT pickup_location_id, COUNT(*) as count FROM taxi_trips 
GROUP BY pickup_location_id 
ORDER BY count DESC 
LIMIT 10
```

---

## 测试覆盖范围

### SQL 功能覆盖

- ✅ SELECT 基础查询
- ✅ WHERE 条件过滤（=, !=, >, <, >=, <=, BETWEEN, IN, AND, OR, NOT）
- ✅ 聚合函数（COUNT, SUM, AVG, MIN, MAX）
- ✅ GROUP BY 分组（单列、多列）
- ✅ HAVING 过滤
- ✅ ORDER BY 排序（单列、多列、ASC/DESC）
- ✅ LIMIT/OFFSET 分页
- ✅ DISTINCT 去重
- ✅ 子查询
- ✅ CASE WHEN 条件表达式
- ✅ UNION 联合查询
- ✅ 表达式计算（算术、函数）
- ✅ NULL 值处理

### 数据类型覆盖

- ✅ 整数（passenger_count, payment_type）
- ✅ 浮点数（trip_distance, fare_amount, tip_amount）
- ✅ 时间戳（pickup_datetime, dropoff_datetime）
- ✅ 字符串（id, pickup_location_id, dropoff_location_id）

### 边界情况覆盖

- ✅ 零值检查
- ✅ NULL 值检查
- ✅ 极大值查询
- ✅ 空结果集
- ✅ 大量数据分页

---

## 运行测试

```bash
cd test_suite
python3 test_queries.py nyc-taxi
```

预期输出示例：

```
================================================================================
=== NYC Taxi Dataset - Calm vs MySQL Comparison ===
================================================================================

总共 186 个测试用例
  - 自动生成测试: 170 条
  - 手动编写测试: 16 条

📊 Test: 聚合函数: SELECT COUNT(*) FROM taxi_trips...
  [Calm] ✓ Completed in 0.015s (1 rows)
  [MySQL] ✓ Completed in 0.012s (1 rows)
    ✓ Results match (1 rows)

📊 Test: GROUP BY: SELECT passenger_count, COUNT(*) FROM...
  [Calm] ✓ Completed in 0.023s (7 rows)
  [MySQL] ✓ Completed in 0.019s (7 rows)
    ✓ Results match (7 rows)
    ⚡ Calm is 1.21x faster

...

================================================================================
=== Test Summary ===
================================================================================
Total tests: 186
Passed: 184
Failed: 2

🎉 Most tests passed!
```

---

## 测试设计原则

1. **实用性**: 所有测试基于真实数据和实际查询场景
2. **全面性**: 覆盖各种 SQL 功能和边界情况
3. **可扩展性**: 易于添加新的测试用例
4. **可维护性**: 通过生成器自动生成，减少手动维护
5. **可对比性**: 与 MySQL 8.0 结果对比，确保兼容性

---

## 下一步计划

- [ ] 添加 JOIN 查询测试（需要多表）
- [ ] 添加 INSERT/UPDATE/DELETE 测试
- [ ] 添加事务测试
- [ ] 添加并发查询测试
- [ ] 添加索引相关测试
- [ ] 添加性能基准测试
- [ ] 扩展到 1000+ 条测试用例
