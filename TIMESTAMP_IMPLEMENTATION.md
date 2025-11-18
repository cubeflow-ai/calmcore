# Timestamp 字段类型实现总结

## 概述
成功为 calmcore 添加了完整的 Timestamp 字段类型支持,允许存储和查询时间戳数据,支持多种输入格式的自动转换。

## 实现的功能

### 1. 字段类型定义 (`src/schema/field.rs`)
- **FieldType::Timestamp**: 新增时间戳字段类型枚举
- **FieldOption::Timestamp**: 字段配置选项,包含:
  - `name`: 字段名
  - `index`: 是否建立索引
  - `persist_option`: 持久化选项
  - `format`: 可选的时间格式字符串
- **timestamp_format()**: 获取时间格式的辅助方法

### 2. 时间戳解析工具 (`src/utils/timestamp.rs`)
- **parse_scalar_to_timestamp()**: 核心解析函数,支持:
  - `Int64`: 自动识别秒/毫秒时间戳
  - `Int32`: Unix 秒时间戳
  - `Utf8`: 字符串时间,支持多种格式:
    - ISO8601: `2024-01-01T00:00:00Z`
    - RFC3339: `2024-01-01T00:00:00+08:00`
    - MySQL格式: `2024-01-01 00:00:00`
    - Unix时间戳字符串: `"1704067200"`
  - `Date32`: Arrow Date32类型
  - `Timestamp*`: Arrow各种时间戳类型

- **normalize_timestamp()**: 智能归一化
  - 小于 10,000,000,000 → 秒级,自动转为毫秒
  - 否则保持毫秒

### 3. TimestampKey 类型 (`src/segment/field_store/timestamp_key.rs`)
- **TimestampKey(i64)**: 类型安全的时间戳包装类型
- **IndexKey trait 实现**:
  - `extract_from_array()`: 支持从以下数组提取:
    - `Int64Array`: 直接毫秒时间戳
    - `TimestampMillisecondArray`: Arrow 原生时间戳
    - `StringArray`: 字符串时间 (自动解析)
  - `from_scalar()`: 使用 `parse_scalar_to_timestamp` 自动转换
  - `new_serializer()`: 创建序列化器

### 4. 序列化器 (`src/segment/field_store/timestamp_key.rs`)
- **TimestampRoaringSerializer**: 
  - 包装 `I64RoaringSerializer`
  - 自动类型转换:
    - 序列化: `Vec<TimestampKey>` → `Vec<i64>` → bytes
    - 反序列化: bytes → `Vec<i64>` → `Vec<TimestampKey>`
  - 兼容 `WriteSerializer` 和 `ReadSerializer` trait

### 5. Schema 集成 (`src/schema/mod.rs`)
- **to_arrow_schema()**: Timestamp 映射到 Arrow 类型
  - `DataType::Timestamp(TimeUnit::Millisecond, None)`

### 6. Segment 集成 (`src/segment/mod.rs`)
- **Segment::new()**: 创建 TimestampField 索引
- **Segment::from_parquet_file()**: 从 Parquet 加载时支持 Timestamp
- **Segment::recover_from_disk()**: 从磁盘恢复时支持 Timestamp
- **Segment::from_frozen_disk()**: 从冻结段加载时支持 Timestamp

### 7. 协议层集成
- **MySQL 协议** (`src/protocol/mysql/mod.rs`):
  - DESCRIBE TABLE: 显示为 `timestamp` 类型
- **Arrow Schema**: 转换为 `Timestamp(Millisecond, None)`

## 使用示例

### 定义 Schema
```rust
use calm::schema::{field::FieldOption, PersistPolicy, Schema};

let schema = Schema {
    name: "events".to_string(),
    primary_key: Some("id".to_string()),
    store_source: true,
    persist_policy: PersistPolicy::default(),
    fields: vec![
        FieldOption::Keyword {
            name: "id".to_string(),
            index: true,
            is_array: false,
            persist_option: None,
            case_sensitive: true,
        },
        FieldOption::Timestamp {
            name: "created_at".to_string(),
            index: true,
            persist_option: None,
            format: None, // 默认毫秒精度
        },
    ],
};
```

### 支持的输入格式
```rust
// 1. 毫秒时间戳 (Int64)
ScalarValue::Int64(Some(1704067200000))

// 2. 秒时间戳 (自动转换)
ScalarValue::Int64(Some(1704067200))

// 3. 字符串时间
ScalarValue::Utf8(Some("2024-01-01 00:00:00".to_string()))
ScalarValue::Utf8(Some("2024-01-01T00:00:00Z".to_string()))
ScalarValue::Utf8(Some("2024-01-01T00:00:00+08:00".to_string()))
```

### SQL 查询 (未来支持)
```sql
-- 时间范围查询
SELECT * FROM events WHERE created_at >= '2024-01-01' AND created_at < '2024-02-01';

-- 时间戳比较
SELECT * FROM events WHERE created_at > 1704067200000;
```

### Elasticsearch API (未来支持)
```json
{
  "query": {
    "range": {
      "created_at": {
        "gte": "2024-01-01",
        "lt": "2024-02-01"
      }
    }
  }
}
```

## 测试覆盖

### 单元测试 (`tests/test_timestamp_field.rs`)
1. **test_timestamp_field_schema**: Schema 定义和转换
2. **test_timestamp_normalization**: 秒/毫秒自动识别
3. **test_timestamp_key_parsing**: 多种格式解析
4. **test_timestamp_array_extraction**: Array 提取功能

### 测试结果
```
running 4 tests
test test_timestamp_normalization ... ok
test test_timestamp_field_schema ... ok
test test_timestamp_key_parsing ... ok
test test_timestamp_array_extraction ... ok

test result: ok. 4 passed; 0 failed; 0 ignored; 0 measured
```

## 技术细节

### 存储格式
- **内部表示**: i64 毫秒级 Unix 时间戳
- **持久化**: 使用 I64RoaringSerializer + zstd 压缩
- **索引**: B-tree 索引,支持高效范围查询

### 性能特性
- **自动转换**: 解析字符串时间有一定开销,建议批量插入时使用毫秒时间戳
- **索引查询**: 继承 mem_btree 的高性能范围查询 (24-55x 加速)
- **存储开销**: 与 I64 字段相同

### 精度
- **存储精度**: 毫秒 (10^-3 秒)
- **支持范围**: 
  - 最小: -292275055-05-16 (i64::MIN)
  - 最大: 292278994-08-17 (i64::MAX)

## 下一步工作

### 查询层集成 (待实现)
1. **SQL 层**: 修改 query executor 识别 Timestamp 字段,转换字符串查询条件
2. **ES API 层**: 实现 range 查询的时间戳转换
3. **排序支持**: Timestamp 字段的排序查询

### 功能增强 (可选)
1. **时区支持**: 可选存储时区信息
2. **自定义格式**: 支持用户自定义时间格式
3. **时间函数**: 年/月/日提取、时间差计算等

## 兼容性
- ✅ 与现有字段类型完全兼容
- ✅ 序列化格式向后兼容
- ✅ 支持 Parquet 导入/导出
- ✅ Arrow 互操作性

## 相关文件
- `src/schema/field.rs` - 字段类型定义
- `src/utils/timestamp.rs` - 时间戳解析工具
- `src/segment/field_store/timestamp_key.rs` - TimestampKey 实现
- `src/segment/field_store/mod.rs` - TimestampField 类型别名
- `src/segment/mod.rs` - Segment 集成
- `tests/test_timestamp_field.rs` - 测试文件
