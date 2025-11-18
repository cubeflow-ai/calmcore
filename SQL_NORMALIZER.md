# SQL 标准化器 (SqlNormalizer)

## 概述

`SqlNormalizer` 是一个SQL预处理模块，在执行SQL之前对SQL语句进行验证和标准化，确保：

1. **SQL语法正确性验证** - 使用DataFusion的Parser验证SQL语法
2. **MySQL语法转换** - 将MySQL特有的语法转换为标准SQL
3. **执行前错误检测** - 在分布式执行前就发现SQL错误

## 功能特性

### 1. SQL语法验证

使用DataFusion的SQL Parser验证SQL语法，在执行前就能发现语法错误：

```sql
-- ✅ 合法SQL
SELECT * FROM users WHERE age > 18

-- ❌ 语法错误 (会被SqlNormalizer拒绝)
SELCT * FROM users  -- 拼写错误
```

### 2. MySQL LIMIT语法转换

MySQL支持两种LIMIT语法：

```sql
-- MySQL风格 (逗号分隔): LIMIT offset, limit
SELECT * FROM users LIMIT 10, 20  -- 跳过10条，取20条

-- 标准SQL风格: LIMIT limit OFFSET offset  
SELECT * FROM users LIMIT 20 OFFSET 10  -- 跳过10条，取20条
```

`SqlNormalizer` 自动将MySQL风格转换为标准SQL风格：

| 原始SQL | 标准化后的SQL |
|---------|---------------|
| `LIMIT 0,10` | `LIMIT 10 OFFSET 0` |
| `LIMIT 5, 20` | `LIMIT 20 OFFSET 5` |
| `LIMIT 10` | `LIMIT 10` (无需转换) |
| `LIMIT 10 OFFSET 5` | `LIMIT 10 OFFSET 5` (无需转换) |

### 3. 支持格式

支持有空格和无空格两种格式：

```sql
-- 有空格
SELECT * FROM users LIMIT 5, 20

-- 无空格 (也能正确解析)
SELECT * FROM users LIMIT 5,20
```

## 使用方式

### 在DistributedExecutor中自动应用

`SqlNormalizer` 已集成到 `DistributedExecutor::execute_sql()` 中，所有SQL执行前都会自动标准化：

```rust
// 用户代码无需更改
let result = executor.execute_sql("SELECT * FROM users LIMIT 0,10").await?;
// SQL会自动转换为: "SELECT * FROM users LIMIT 10 OFFSET 0"
```

### 日志输出

当SQL被转换时，会输出日志：

```
🔄 [DistributedExecutor] SQL normalized: 
   SELECT * FROM users LIMIT 0,10 
   -> SELECT * FROM users LIMIT 10 OFFSET 0
```

### 独立使用

也可以单独使用 `SqlNormalizer`：

```rust
use calm::compute::SqlNormalizer;

let sql = "SELECT * FROM users LIMIT 5,20";
let normalized = SqlNormalizer::normalize(sql)?;
// normalized = "SELECT * FROM users LIMIT 20 OFFSET 5"
```

## 实现细节

### 转换流程

```
原始SQL
  ↓
[检测MySQL语法]
  ↓
[转换为标准SQL]
  ↓
[DataFusion验证]
  ↓
标准化SQL
```

### 错误处理

如果SQL有语法错误，会返回详细的错误信息：

```rust
// 语法错误示例
let sql = "SELCT * FROM users";  // 拼写错误
let result = SqlNormalizer::normalize(sql);
// 返回: Err(CoreError::InvalidParam("SQL syntax error: ..."))
```

## 性能影响

- **SQL解析开销**: 增加了一次SQL解析操作，但解析速度很快（微秒级）
- **执行效率**: 转换后的标准SQL可以被DataFusion更好地优化
- **错误检测**: 在分布式执行前检测错误，避免浪费资源

## 测试覆盖

完整的单元测试覆盖：

- ✅ 标准SQL (无需转换)
- ✅ MySQL LIMIT语法转换 (有空格)
- ✅ MySQL LIMIT语法转换 (无空格)
- ✅ LIMIT 0,10 边界情况
- ✅ 标准SQL LIMIT OFFSET (无需转换)
- ✅ 非法SQL验证

运行测试：
```bash
cargo test sql_normalizer --lib -- --nocapture
```

## 未来扩展

可以继续扩展支持更多MySQL特有语法：

- `LIMIT 10` → `LIMIT 10 OFFSET 0` (显式offset)
- MySQL的 `GROUP BY` 隐式排序
- MySQL的字符串函数别名
- 等等...

## 相关模块

- `src/compute/sql_normalizer.rs` - SQL标准化器实现
- `src/compute/executor/distributed.rs` - 集成到分布式执行器
- `src/compute/optimizer/plan_analyzer.rs` - SQL查询计划分析器
