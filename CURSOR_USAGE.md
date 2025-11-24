# MySQL 游标使用指南

## 功能概述

CalmCore MySQL 协议现已支持游标功能,可以分批获取大结果集,避免一次性加载所有数据导致内存问题。

## 支持的功能

### 1. Prepared Statements (预编译语句)

```sql
-- 准备语句
PREPARE stmt_name FROM 'SELECT * FROM table WHERE id > ?';

-- 执行语句 (目前参数支持有限)
EXECUTE stmt_name;

-- 关闭语句
DEALLOCATE PREPARE stmt_name;
```

### 2. Cursor (游标)

#### 2.1 声明游标

```sql
-- 基本语法
DECLARE cursor_name CURSOR FOR SELECT * FROM table_name;

-- 带条件的查询
DECLARE my_cursor CURSOR FOR SELECT id, name FROM users WHERE age > 18;
```

#### 2.2 获取数据 (FETCH)

```sql
-- 获取 1 行 (默认)
FETCH FROM cursor_name;

-- 获取指定行数
FETCH 10 FROM cursor_name;

-- 使用 FORWARD 关键字
FETCH FORWARD 5 FROM cursor_name;
```

#### 2.3 关闭游标

```sql
CLOSE cursor_name;
```

## 使用示例

### 示例 1: 基本游标使用

```sql
-- 1. 声明游标
DECLARE user_cursor CURSOR FOR SELECT * FROM r2api LIMIT 100;

-- 2. 分批获取数据 (每次 10 行)
FETCH 10 FROM user_cursor;
FETCH 10 FROM user_cursor;
FETCH 10 FROM user_cursor;

-- 3. 关闭游标
CLOSE user_cursor;
```

### 示例 2: 处理大结果集

```sql
-- 假设有 10000 条记录
DECLARE large_cursor CURSOR FOR SELECT * FROM big_table;

-- 分批处理,每次 100 行
FETCH 100 FROM large_cursor;  -- 第 1-100 行
FETCH 100 FROM large_cursor;  -- 第 101-200 行
FETCH 100 FROM large_cursor;  -- 第 201-300 行
-- ... 继续获取

-- 处理完毕后关闭
CLOSE large_cursor;
```

### 示例 3: Prepared Statement + Cursor

```sql
-- 1. 准备语句
PREPARE search_stmt FROM 'SELECT * FROM users WHERE name LIKE "%test%"';

-- 2. 声明游标使用准备好的查询
DECLARE result_cursor CURSOR FOR SELECT * FROM users WHERE name LIKE "%test%";

-- 3. 分批获取
FETCH 50 FROM result_cursor;

-- 4. 清理
CLOSE result_cursor;
```

## 工作原理

### Prepared Statements

1. **on_prepare**: 接收 SQL 语句,分配唯一的 statement ID,保存在 HashMap 中
2. **on_execute**: 根据 statement ID 查找 SQL,执行并返回结果
3. **on_close**: 从 HashMap 中删除 statement

### Cursors (流式游标)

**重要**: 游标采用**按需加载**的流式设计,不会一次性加载所有数据到内存!

1. **DECLARE**: 
   - 保存查询 SQL,但**不立即执行**
   - 创建 CursorState (初始状态,无数据)
   - 内存占用极小 (只存储 SQL 字符串)

2. **FETCH**:
   - **首次 FETCH**: 执行 SQL 查询,获取 RecordBatch
   - **后续 FETCH**: 使用 `batch.slice()` 零拷贝切片,返回指定行数
   - 只缓存**当前正在使用的批次**
   - 更新游标位置,记录已读取的行数
   - 内存占用: 仅当前批次数据

3. **CLOSE**:
   - 从 HashMap 中删除 cursor
   - 释放所有相关内存 (包括缓存的批次)

### 流式处理优势

```rust
// CursorState 结构
struct CursorState {
    query: String,              // 只存 SQL,不存数据
    batch_index: usize,         // 当前批次索引
    row_in_batch: usize,        // 当前行位置
    cached_batches: Vec<RecordBatch>,  // 只缓存当前批次
    exhausted: bool,            // 是否已读完
}
```

**关键特性**:
- ✅ 延迟执行: DECLARE 时不执行查询
- ✅ 零拷贝切片: 使用 Arrow 的 slice 操作
- ✅ 按需加载: 只在 FETCH 时读取数据
- ✅ 最小缓存: 只缓存当前批次,不保存历史数据

## 性能考虑

### 优点

- ✅ **内存效率**: 不会一次性加载全部结果到内存
- ✅ **延迟执行**: DECLARE 时不执行查询,只在 FETCH 时按需加载
- ✅ **零拷贝**: 使用 Arrow 的 slice 操作,无额外内存分配
- ✅ **网络优化**: 减少单次传输的数据量
- ✅ **灵活性**: 应用可以控制数据处理速度
- ✅ **流式处理**: 适合处理超大结果集

### 内存对比

**旧版本 (全量加载)**:
```
DECLARE cursor → 执行查询 → 加载 100万行到内存 (占用 GB 级内存)
FETCH → 从内存切片返回
```

**新版本 (流式按需)**:
```
DECLARE cursor → 只保存 SQL (几百字节)
FETCH → 执行查询 → 获取 RecordBatch → 切片返回 (只占用当前批次内存)
FETCH → 从缓存批次切片 → 释放旧批次
```

### 注意事项

- ⚠️ 首次 FETCH 会执行 SQL 查询,可能需要时间
- ⚠️ 当前实现缓存一个 RecordBatch,适合大多数场景
- ⚠️ 建议及时关闭不再使用的 cursor

## 限制

当前实现的限制:

1. **无参数绑定**: Prepared statements 暂不支持参数绑定 (`?` 占位符)
2. **只读游标**: 游标只支持 SELECT,不支持 UPDATE/DELETE
3. **单向游标**: 只支持 FORWARD,不支持 BACKWARD
4. **单批次缓存**: 只缓存当前 RecordBatch,不支持跨批次的高级特性

## 架构设计

### 代码组织

```
src/protocol/mysql/
├── mod.rs        # MySQL 协议主逻辑
├── cursor.rs     # 游标管理器 (独立模块)
└── insert_handler.rs
```

**cursor.rs** 提供:
- `CursorManager`: 管理 prepared statements 和 cursors
- `CursorState`: 游标状态 (SQL, 位置, 缓存批次)
- `PreparedStatement`: Prepared statement 信息

**mod.rs** 负责:
- MySQL 协议实现 (MysqlShim trait)
- SQL 命令解析和路由
- 调用 CursorManager 执行 cursor 操作

### 模块职责分离

- ✅ **cursor.rs**: 纯粹的 cursor 逻辑,无协议相关代码
- ✅ **mod.rs**: 协议层,不包含 cursor 实现细节
- ✅ 易于测试和维护
- ✅ 可复用到其他协议 (PostgreSQL, etc.)

## 对比普通查询

### 普通 SELECT
```sql
-- 一次性返回所有数据
SELECT * FROM big_table;  -- 可能返回 100万行
```
- 客户端必须接收全部数据
- 占用大量网络带宽
- 可能导致客户端内存溢出

### 使用 Cursor
```sql
DECLARE my_cursor CURSOR FOR SELECT * FROM big_table;
FETCH 1000 FROM my_cursor;  -- 只获取 1000 行
-- 处理这 1000 行...
FETCH 1000 FROM my_cursor;  -- 再获取 1000 行
-- ...
CLOSE my_cursor;
```
- 客户端按需获取数据
- 控制内存使用
- 可以实现进度展示

## 错误处理

### 游标未找到
```sql
mysql> FETCH FROM non_existent_cursor;
ERROR: Cursor 'non_existent_cursor' not found
```

### 游标已耗尽
```sql
mysql> FETCH FROM cursor_name;
-- 返回 0 行,表示没有更多数据
```

### 语法错误
```sql
mysql> DECLARE cursor_name;  -- 缺少 CURSOR FOR
ERROR: Invalid DECLARE CURSOR syntax
```

## 日志输出

启用日志后可以看到游标操作:

```
📝 Prepared statement ID=1: SELECT * FROM users
📌 Declaring cursor 'user_cursor' for: SELECT * FROM users
✅ Cursor 'user_cursor' declared successfully
🔍 Fetching 10 rows from cursor 'user_cursor'
🔄 Executing query for cursor: SELECT * FROM users
📤 Returning rows [0, 10) from cursor 'user_cursor'
🔍 Fetching 10 rows from cursor 'user_cursor'
📤 Returning rows [10, 20) from cursor 'user_cursor'
🗑️  Closed cursor 'user_cursor'
```

注意 `🔄 Executing query` 只在首次 FETCH 时出现!

## 最佳实践

1. **及时关闭游标**
   ```sql
   -- ✅ 好的做法
   DECLARE my_cursor CURSOR FOR SELECT * FROM table;
   FETCH 100 FROM my_cursor;
   CLOSE my_cursor;
   
   -- ❌ 不好的做法 - 忘记关闭
   DECLARE my_cursor CURSOR FOR SELECT * FROM table;
   FETCH 100 FROM my_cursor;
   -- 连接断开前游标一直占用内存
   ```

2. **合理设置批次大小**
   ```sql
   -- 太小 - 网络往返次数多
   FETCH 1 FROM cursor;
   
   -- 太大 - 失去游标的意义
   FETCH 1000000 FROM cursor;
   
   -- ✅ 合理 - 根据实际情况调整
   FETCH 100 FROM cursor;  -- 或 500, 1000 等
   ```

3. **错误处理**
   ```sql
   -- 客户端应该捕获错误
   DECLARE my_cursor CURSOR FOR SELECT * FROM table;
   -- 循环 FETCH 直到返回 0 行
   -- CLOSE my_cursor (即使出错也要关闭)
   ```

## 兼容性

- ✅ MySQL 客户端 (mysql CLI)
- ✅ MySQL Workbench
- ✅ 支持 MySQL 协议的各种客户端库
- ✅ JDBC/ODBC 驱动

## 未来改进

计划中的功能:
- [ ] 参数绑定支持
- [ ] SCROLL 游标 (支持 BACKWARD)
- [ ] 多批次流式处理 (处理超大 RecordBatch)
- [ ] 游标超时自动清理
- [ ] 游标统计信息 (SHOW CURSOR STATUS)
- [ ] Segment 级别的流式扫描
