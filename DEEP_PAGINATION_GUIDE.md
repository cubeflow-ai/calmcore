# 深度分页优化指南

## 问题分析

### 传统 OFFSET 分页的性能问题

```sql
-- ❌ 慢：需要读取 11,000 行，然后丢弃前 10,000 行
SELECT * FROM r2api 
ORDER BY id 
LIMIT 1000 OFFSET 10000;

-- 数据量越大，OFFSET 越大，性能越差：
-- OFFSET 0     : ~10ms
-- OFFSET 10000 : ~500ms
-- OFFSET 100000: ~5s
```

**根本原因：** 数据库必须扫描并丢弃所有 OFFSET 之前的行。

---

## 解决方案 1：游标分页（Cursor-based Pagination）

### 核心思想

用 WHERE 条件替代 OFFSET，只读取需要的数据。

```sql
-- ✅ 快：只读取 1,000 行
SELECT * FROM r2api 
WHERE id > 'last_id_from_previous_page'
ORDER BY id 
LIMIT 1000;

-- 性能稳定：
-- 第 1 页   : ~10ms
-- 第 10 页  : ~10ms
-- 第 100 页 : ~10ms
```

### 使用方法

#### 1. 客户端代码示例（Java/JDBC）

```java
import java.sql.*;

public class CursorPaginationExample {
    public static void main(String[] args) throws SQLException {
        Connection conn = DriverManager.getConnection(
            "jdbc:mysql://localhost:3307/calm", 
            "root", 
            ""
        );

        String lastId = null; // 第一页为 null
        int pageSize = 1000;
        int pageNum = 1;

        while (true) {
            // 构造游标查询
            String sql;
            if (lastId == null) {
                // 第一页
                sql = String.format(
                    "SELECT * FROM r2api " +
                    "WHERE _partition = '16_0000000000002410860' " +
                    "ORDER BY id LIMIT %d",
                    pageSize
                );
            } else {
                // 后续页
                sql = String.format(
                    "SELECT * FROM r2api " +
                    "WHERE _partition = '16_0000000000002410860' " +
                    "  AND id > '%s' " +
                    "ORDER BY id LIMIT %d",
                    lastId, pageSize
                );
            }

            // 执行查询
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(sql);

            int count = 0;
            while (rs.next()) {
                count++;
                String id = rs.getString("id");
                // 处理数据...
                
                // 保存最后一个 ID 作为下次查询的游标
                lastId = id;
            }

            System.out.println("Page " + pageNum + ": " + count + " rows");

            // 如果返回行数 < pageSize，说明已经到最后一页
            if (count < pageSize) {
                break;
            }

            pageNum++;
            rs.close();
            stmt.close();
        }

        conn.close();
    }
}
```

#### 2. 服务端 API 设计（推荐）

```rust
// HTTP API: GET /api/r2api?cursor=xxx&limit=1000

use crate::compute::executor::{CursorInfo, CursorPagination};

pub async fn query_with_cursor(
    cursor: Option<String>, // 上次返回的 cursor
    limit: usize,
) -> Result<QueryResponse> {
    let cursor_info = cursor.map(|c| CursorInfo {
        field_name: "id".to_string(),
        last_value: c,
        ascending: true,
    });

    // 原始 SQL
    let sql = format!(
        "SELECT * FROM r2api WHERE _partition='xxx' ORDER BY id LIMIT {}",
        limit
    );

    // 转换为游标 SQL
    let cursor_sql = CursorPagination::convert_to_cursor_sql(&sql, cursor_info.as_ref())?;

    // 执行查询
    let result = executor.execute_sql(&cursor_sql).await?;

    // 提取下一个游标
    let next_cursor = CursorPagination::extract_next_cursor(&result.batch, "id");

    Ok(QueryResponse {
        data: result.batch,
        next_cursor, // 返回给客户端用于下次请求
        has_more: result.batch.num_rows() == limit,
    })
}
```

---

## 解决方案 2：分区 + 小范围 OFFSET

### 核心思想

结合 `_partition` 过滤，将大表分散到多个小表，每个小表内用 OFFSET。

✅ **`_partition` 过滤已实现**：查询会自动进行分区裁剪（Partition Pruning），只扫描匹配的分区。

```sql
-- ✅ 推荐：使用 _partition 过滤（自动分区裁剪）
SELECT * FROM events 
WHERE _partition = '20240117_1' 
ORDER BY id 
LIMIT 1000;

-- ✅ 支持 IN 条件
SELECT * FROM events 
WHERE _partition IN ('20240117_0', '20240117_1')
ORDER BY event_time 
LIMIT 1000;

-- ✅ 支持 LIKE 模式
SELECT * FROM events 
WHERE _partition LIKE '202401%' 
ORDER BY event_time 
LIMIT 1000;
```

-- 性能：
-- 单 partition 86 万行
-- OFFSET 5000: ~50ms（可接受）
-- OFFSET 50000: ~500ms（勉强）
```

**适用场景：** 
- 需要按时间/分区遍历数据
- 单个 partition 内翻页深度不大（< 10万）

### 使用方法

```java
// 1. 获取所有 partition 列表
ResultSet partitions = stmt.executeQuery("SHOW PARTITIONS");

List<String> partitionList = new ArrayList<>();
while (partitions.next()) {
    partitionList.add(partitions.getString("partition_name"));
}

// 2. 遍历每个 partition
for (String partition : partitionList) {
    String lastId = null;
    
    while (true) {
        String sql = String.format(
            "SELECT * FROM r2api " +
            "WHERE _partition = '%s' " +
            "  AND id > '%s' " +
            "ORDER BY id LIMIT 1000",
            partition, lastId
        );
        
        ResultSet rs = stmt.executeQuery(sql);
        // 处理数据...
    }
}
```

---

## 解决方案 3：导出 + 离线处理（推荐大数据量）

### 核心思想

对于真正的大数据量遍历（百万/千万级），不要用在线查询，而是导出数据。

```bash
# 1. 导出为 Parquet 文件（高效压缩格式）
SELECT * FROM r2api 
WHERE _partition = 'xxx'
INTO OUTFILE '/tmp/r2api_partition1.parquet'
FORMAT Parquet;

# 2. 使用 Spark/Pandas 离线处理
# Python 示例：
import pandas as pd

df = pd.read_parquet('/tmp/r2api_partition1.parquet')

# 分块处理
chunk_size = 10000
for chunk in [df[i:i+chunk_size] for i in range(0, len(df), chunk_size)]:
    process(chunk)  # 你的业务逻辑
```

---

## 性能对比

| 方案 | 第1页 | 第10页 | 第100页 | 适用场景 |
|------|-------|--------|---------|----------|
| **OFFSET 全表** | 10ms | 500ms | 5s | ❌ 不推荐 |
| **OFFSET + _partition** | 10ms | 50ms | 500ms | ✅ 单分区小范围翻页 |
| **游标分页** | 10ms | 10ms | 10ms | ✅ 深度翻页首选 |
| **导出文件** | - | - | - | ✅ 百万级全量遍历 |

---

## 推荐实践

### 场景 1：Web 分页（用户点击下一页）

**推荐：游标分页**

```javascript
// 前端代码
let cursor = null;

async function loadNextPage() {
    const response = await fetch(`/api/r2api?cursor=${cursor}&limit=50`);
    const data = await response.json();
    
    // 显示数据
    renderTable(data.rows);
    
    // 保存游标用于下次请求
    cursor = data.next_cursor;
    
    // 是否有更多数据
    if (!data.has_more) {
        $('#next-button').disable();
    }
}
```

### 场景 2：数据同步/ETL（遍历所有数据）

**推荐：分区 + 游标**

```python
import requests

def sync_all_data():
    # 1. 获取所有 partition
    partitions = get_partitions()
    
    for partition in partitions:
        cursor = None
        
        # 2. 用游标遍历每个 partition
        while True:
            sql = f"""
                SELECT * FROM r2api 
                WHERE _partition = '{partition}' 
                {"AND id > '" + cursor + "'" if cursor else ""}
                ORDER BY id LIMIT 10000
            """
            
            result = query(sql)
            
            # 3. 批量处理数据
            batch_insert_to_target(result)
            
            # 4. 更新游标
            if len(result) < 10000:
                break
            cursor = result[-1]['id']
```

### 场景 3：全量数据分析（一次性）

**推荐：导出文件**

```sql
-- 1. 导出所有 partition
SELECT * FROM r2api 
WHERE _partition = 'xxx'
INTO OUTFILE '/data/export/partition1.parquet'
FORMAT Parquet;

-- 2. 用 Spark/Presto 处理
spark.read.parquet('/data/export/*.parquet')
     .groupBy('category')
     .count()
     .show()
```

---

## 常见问题

### Q1: 游标分页如何支持跳页（如直接跳到第 50 页）？

**A:** 游标分页不支持直接跳页，这是设计权衡。如果需要跳页功能，考虑：
- 场景 1：Web UI 只显示"上一页/下一页"按钮（不显示页码）
- 场景 2：使用 Elasticsearch 的 `search_after` + 缓存前 N 页的游标
- 场景 3：混合方案：前 10 页用 OFFSET，之后用游标

### Q2: 如何保证游标分页的一致性（数据变化）？

**A:** 游标分页在数据变化时可能跳过或重复记录。解决方案：
- 场景 1：可容忍数据变化（如社交媒体 feed）→ 直接使用游标
- 场景 2：需要快照一致性 → 使用时间戳游标 + `WHERE timestamp > cursor`
- 场景 3：严格一致性 → 创建临时快照表或使用数据库快照功能

### Q3: 游标字段选什么？

**A:** 
- ✅ 推荐：主键（`id`）、时间戳（`created_at`）
- ✅ 要求：单调递增、有索引、不会被修改
- ❌ 不推荐：非唯一字段、可变字段

---

## 迁移步骤

### 从 OFFSET 迁移到游标分页

```java
// ====== 旧代码（OFFSET） ======
public List<Record> queryPage(int pageNum, int pageSize) {
    int offset = (pageNum - 1) * pageSize;
    String sql = String.format(
        "SELECT * FROM r2api ORDER BY id LIMIT %d OFFSET %d",
        pageSize, offset
    );
    return executeQuery(sql);
}

// ====== 新代码（游标） ======
public PageResult queryPage(String cursor, int pageSize) {
    String sql;
    if (cursor == null) {
        sql = String.format(
            "SELECT * FROM r2api ORDER BY id LIMIT %d",
            pageSize
        );
    } else {
        sql = String.format(
            "SELECT * FROM r2api WHERE id > '%s' ORDER BY id LIMIT %d",
            cursor, pageSize
        );
    }
    
    List<Record> records = executeQuery(sql);
    String nextCursor = records.isEmpty() ? null : records.get(records.size() - 1).getId();
    
    return new PageResult(records, nextCursor, records.size() == pageSize);
}
```

---

## 总结

1. **小数据量（< 1万）**：OFFSET 完全够用
2. **中等数据量（1万-100万）**：_partition + 游标分页
3. **大数据量（> 100万）**：导出文件 + 离线处理

**黄金法则：避免深度分页，用游标替代 OFFSET！**
