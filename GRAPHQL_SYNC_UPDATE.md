# GraphQL 参数和文档同步更新完成

## 修复的问题

### 1. segment_loader.rs 编译错误
- ❌ **问题**: 使用了已删除的 `generate_partition_dir_name()` 方法
- ✅ **修复**: 改为直接使用 `partition_name` 构建路径
  ```rust
  // 之前
  .join(PartitionStrategy::generate_partition_dir_name(partition_name))
  
  // 现在
  .join("partitions")
  .join(partition_name)
  ```

## GraphQL 参数更新

### PartitionStrategyInput 参数变更

#### 之前的 Range 策略
```graphql
partition_strategy: {
  strategy_type: RANGE
  field: "timestamp"
  ranges: [
    {
      partition_id: 0
      start: { int_value: 0 }
      end: { int_value: 1704067200000 }
    }
    # ... 更多范围
  ]
}
```

#### 现在的 Range 策略
```graphql
partition_strategy: {
  strategy_type: RANGE
  field: "timestamp"
  range_start: 1704067200000    # 起始值
  range_step: 86400000          # 步长(1天)
}
```

**变更说明:**
- ✅ 删除 `ranges` 参数
- ✅ 添加 `range_start` 参数 (i64)
- ✅ 添加 `range_step` 参数 (i64)
- ✅ 分区按需创建,无需预定义

#### 新增的 PKHash 策略
```graphql
partition_strategy: {
  strategy_type: PKHash
  num_partitions: 4
}
```

**特性:**
- ✅ 仅用于有主键的表
- ✅ 自动基于主键哈希分区
- ✅ 无需指定 field 参数

## 文档更新

### 1. graphql_ddl_complete_example.md

#### 更新的分区策略表格
| 策略 | 参数 | 说明 | 使用场景 |
|------|------|------|----------|
| `PKHash` | num_partitions | 基于主键哈希分配 | 适合有主键的表,自动负载均衡 |
| `HASH` | field, num_partitions | 基于指定字段哈希分配 | 无主键表,均衡负载 |
| `RANGE` | field, range_start, range_step | 基于范围划分(按需创建) | 时序数据,支持 Int64/Timestamp 字段 |
| `CUSTOM` | - | 用户自定义分区名 | 特殊业务需求,手动指定分区 |
| `NONE` | - | 单分区 | 小表,测试环境 |

#### 添加 Range 分区详细说明
```
**Range 分区说明:**
- `range_start`: 起始值(毫秒时间戳或整数)
- `range_step`: 步长(例如: 86400000 = 1天)
- 分区按需创建,命名格式: `partition_{start_value}`
- 支持字段类型: Int64, Timestamp(毫秒/秒/微秒/纳秒)
```

#### 更新示例 2: 日志表
**之前:** 使用 ranges 数组预定义 3 个分区

**现在:** 使用 range_start + range_step 按需创建
```graphql
partition_strategy: {
  strategy_type: RANGE
  field: "timestamp"
  range_start: 1704067200000    # 2024-01-01 00:00:00
  range_step: 86400000          # 1天
}
```

#### 更新示例 3: 商品表
**之前:** 单分区

**现在:** PKHash 4 分区
```graphql
partition_strategy: {
  strategy_type: PKHash
  num_partitions: 4
}
```

### 2. GraphQL Schema 内联文档

所有 GraphQL 类型和方法的内联文档已保持最新:

#### PartitionStrategyInput
```rust
/// Range 分区策略 (按需创建)
/// - range_start: 起始值(毫秒时间戳或整数)
/// - range_step: 步长
pub range_start: Option<i64>,
pub range_step: Option<i64>,
```

#### LoadSegmentInput
```rust
/// 分区名称（必须提供，且符合目录名称规范）
/// 新的分区命名格式: partition_{id} (19位数字)
pub partition_name: String,
```

## 时间戳支持增强

### Range 分区支持的 Timestamp 精度

| 精度 | DataType | 自动转换 |
|------|----------|----------|
| 毫秒 | Timestamp(Millisecond) | 无需转换 |
| 秒 | Timestamp(Second) | → 毫秒 × 1000 |
| 微秒 | Timestamp(Microsecond) | → 毫秒 ÷ 1000 |
| 纳秒 | Timestamp(Nanosecond) | → 毫秒 ÷ 1,000,000 |

### 时间单位常量
```rust
1 秒  = 1,000 毫秒
1 分钟 = 60,000 毫秒
1 小时 = 3,600,000 毫秒
1 天  = 86,400,000 毫秒
1 周  = 604,800,000 毫秒
```

### 使用示例
```graphql
# 按天分区
partition_strategy: {
  strategy_type: RANGE
  field: "timestamp"
  range_start: 1704067200000    # 2024-01-01 00:00:00 UTC
  range_step: 86400000          # 1天
}

# 按小时分区
partition_strategy: {
  strategy_type: RANGE
  field: "timestamp"
  range_start: 1704067200000    # 2024-01-01 00:00:00 UTC
  range_step: 3600000           # 1小时
}

# 按周分区
partition_strategy: {
  strategy_type: RANGE
  field: "created_time"
  range_start: 0                # 1970-01-01 00:00:00 UTC
  range_step: 604800000         # 1周
}
```

## 编译验证

✅ 所有修改已通过编译验证:
```bash
cargo check --lib
# Result: Success (仅有 3 个 unused import warnings)
```

## 受影响的模块

### 已修复
- ✅ `src/segment_loader.rs` - 修复分区路径构建
- ✅ `docs/graphql_ddl_complete_example.md` - 更新分区策略文档
- ✅ GraphQL Schema 内联文档 - 已在之前的重构中更新

### 无需修改
- ✅ `src/protocol/graphql/mod.rs` - 已在 Router 重构中更新
- ✅ `src/router/range_router.rs` - 已支持 Timestamp
- ✅ `src/catalog/table_meta.rs` - PartitionStrategy 已重构

## 迁移指南

### 从旧 Range 策略迁移

**旧代码:**
```graphql
partition_strategy: {
  strategy_type: RANGE
  field: "timestamp"
  ranges: [
    { partition_id: 0, start: { int_value: 0 }, end: { int_value: 1704067200000 } }
    { partition_id: 1, start: { int_value: 1704067200000 }, end: { int_value: 1735689600000 } }
  ]
}
```

**新代码:**
```graphql
partition_strategy: {
  strategy_type: RANGE
  field: "timestamp"
  range_start: 0           # 第一个范围的起始值
  range_step: 86400000     # 每个分区的时间跨度
}
```

**优势:**
- 无需预定义所有分区
- 按需自动创建分区
- 配置更简洁
- 支持无限扩展

### 新建表推荐配置

#### 有主键的表 → 使用 PKHash
```graphql
partition_strategy: {
  strategy_type: PKHash
  num_partitions: 4
}
```

#### 时序数据表 → 使用 Range
```graphql
partition_strategy: {
  strategy_type: RANGE
  field: "timestamp"
  range_start: 1704067200000
  range_step: 86400000
}
```

#### 无主键且需要负载均衡 → 使用 Hash
```graphql
partition_strategy: {
  strategy_type: HASH
  field: "user_id"
  num_partitions: 8
}
```

#### 小表或测试 → 使用 None
```graphql
partition_strategy: {
  strategy_type: NONE
}
```

## 总结

✅ **所有同步更新已完成:**
1. 修复了 segment_loader.rs 的编译错误
2. 更新了 GraphQL DDL 完整示例文档
3. 添加了 PKHash 策略示例
4. 更新了 Range 策略说明和示例
5. 添加了 Timestamp 支持详细说明
6. 提供了迁移指南

🎉 **系统现在完全同步,文档和代码保持一致!**
