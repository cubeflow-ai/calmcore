# Range 分区策略 - Timestamp 字段支持

## 概述

Range 分区策略现在支持 **Timestamp** 字段进行时间范围分区,特别适合按时间维度分区的场景。

## 支持的字段类型

1. **Int64**: 普通整数字段
2. **Timestamp**: 时间戳字段(支持多种精度)
   - `Millisecond`: 毫秒精度 (默认)
   - `Second`: 秒精度 (自动转换为毫秒)
   - `Microsecond`: 微秒精度 (自动转换为毫秒)
   - `Nanosecond`: 纳秒精度 (自动转换为毫秒)

## 使用示例

### GraphQL 方式

```graphql
mutation {
  createTable(
    input: {
      tableName: "events"
      schema: [
        { name: "id", dataType: "UInt64" }
        { name: "insertTime", dataType: "Timestamp" }  # 毫秒时间戳
        { name: "event_type", dataType: "Utf8" }
      ]
      partitionStrategy: {
        strategyType: Range
        field: "insertTime"
        range_start: 28800000      # 起始时间: 1970-01-01 08:00:00 (UTC+8)
        range_step: 86400000       # 步长: 1天 = 86400秒 * 1000毫秒
      }
    }
  ) {
    success
    message
  }
}
```

### 时间计算说明

#### 常用时间单位转换 (毫秒)

```rust
1 秒  = 1,000 毫秒
1 分钟 = 60,000 毫秒
1 小时 = 3,600,000 毫秒
1 天  = 86,400,000 毫秒
1 周  = 604,800,000 毫秒
```

#### 时区处理

```bash
# UTC 时间的 1970-01-01 00:00:00
start: 0

# UTC+8 时区的 1970-01-01 08:00:00
start: 28800000  # 8小时 = 8 * 3600 * 1000

# 2024-01-01 00:00:00 UTC
start: 1704067200000
```

## 分区示例

### 按天分区

```graphql
partitionStrategy: {
  strategyType: Range
  field: "insertTime"
  range_start: 1704067200000    # 2024-01-01 00:00:00
  range_step: 86400000          # 1天
}
```

**生成的分区**:
- `partition_1704067200000`: 2024-01-01
- `partition_1704153600000`: 2024-01-02
- `partition_1704240000000`: 2024-01-03

### 按小时分区

```graphql
partitionStrategy: {
  strategyType: Range
  field: "insertTime"
  range_start: 1704067200000    # 2024-01-01 00:00:00
  range_step: 3600000           # 1小时
}
```

**生成的分区**:
- `partition_1704067200000`: 2024-01-01 00:00:00
- `partition_1704070800000`: 2024-01-01 01:00:00
- `partition_1704074400000`: 2024-01-01 02:00:00

### 按周分区

```graphql
partitionStrategy: {
  strategyType: Range
  field: "insertTime"
  range_start: 1704067200000    # 2024-01-01 00:00:00
  range_step: 604800000         # 1周 = 7天
}
```

## 路由逻辑

对于给定的 timestamp 值,分区计算公式:

```rust
offset = value - start
partition_index = offset / step
partition_start = start + (partition_index * step)
partition_name = format!("partition_{:019}", partition_start)
```

### 示例计算

假设:
- `start = 1704067200000` (2024-01-01 00:00:00)
- `step = 86400000` (1天)

对于 `value = 1704153600000` (2024-01-02 00:00:00):

```rust
offset = 1704153600000 - 1704067200000 = 86400000
partition_index = 86400000 / 86400000 = 1
partition_start = 1704067200000 + (1 * 86400000) = 1704153600000
partition_name = "partition_1704153600000"
```

## 注意事项

1. **时间精度统一**: 所有时间戳字段会自动转换为毫秒精度
2. **null 值处理**: Range 字段不允许 null 值,会返回错误
3. **按需创建**: Range 分区是按需创建的,只有数据到达时才会创建对应分区
4. **分区命名**: 分区名使用 partition_start 值,格式为 19 位数字 (支持负数)

## 完整示例

### 创建按天分区的日志表

```graphql
mutation {
  createTable(
    input: {
      tableName: "app_logs"
      schema: [
        { name: "log_id", dataType: "UInt64" }
        { name: "timestamp", dataType: "Timestamp" }
        { name: "level", dataType: "Utf8" }
        { name: "message", dataType: "Utf8" }
        { name: "user_id", dataType: "UInt64" }
      ]
      partitionStrategy: {
        strategyType: Range
        field: "timestamp"
        range_start: 0                # 从 1970-01-01 开始
        range_step: 86400000          # 按天分区
      }
    }
  ) {
    success
    message
  }
}
```

### 插入数据

```graphql
mutation {
  insertData(
    tableName: "app_logs"
    data: [
      {
        log_id: 1
        timestamp: 1704067200000      # 2024-01-01
        level: "INFO"
        message: "User login"
        user_id: 100
      }
      {
        log_id: 2
        timestamp: 1704153600000      # 2024-01-02
        level: "WARN"
        message: "High memory usage"
        user_id: 101
      }
    ]
  ) {
    rowsInserted
  }
}
```

**结果**: 数据会自动路由到对应的日期分区:
- log_id=1 -> `partition_1704067200000`
- log_id=2 -> `partition_1704153600000`

## 查询优化

使用时间分区可以加速时间范围查询:

```sql
-- 只扫描单天分区
SELECT * FROM app_logs 
WHERE timestamp >= 1704067200000 
  AND timestamp < 1704153600000;

-- 只扫描一周的分区
SELECT * FROM app_logs 
WHERE timestamp >= 1704067200000 
  AND timestamp < 1704672000000;
```

分区裁剪会自动识别需要扫描的分区,避免全表扫描。
