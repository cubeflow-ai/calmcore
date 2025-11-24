# MySQL 协议内存问题紧急修复

## 根本原因

**`SELECT * FROM table` 会把所有数据加载到内存!**

执行流程:
```
Java Client 
  → MySQL Protocol (execute_query)
    → Engine.execute_sql()  
      → DistributedExecutor.execute_serial_full_scan()
        → 遍历所有 partition
        → all_batches.extend(batches)  ← 全部数据进内存!
        → concat_batches(all_batches)  ← 合并成巨大的 RecordBatch
      ← 返回 QueryResult { batch: 巨大的RecordBatch }
    ← 65万行都在内存里
  ← 开始发送给客户端
```

即使客户端只读 1 行就断开,服务器已经把 65 万行全部加载到内存了!

## 当前紧急修复

### 修改 1: 降低安全限制

```rust
// 原来: 100万行才报错
const MAX_ROWS_WITHOUT_LIMIT: usize = 1_000_000;

// 现在: 5万行就报错
const MAX_ROWS_WITHOUT_LIMIT: usize = 50_000;
```

### 修改 2: 查询前警告

```rust
if !has_limit && query_upper.starts_with("SELECT") {
    eprintln!("⚠️  Query without LIMIT may load all data into memory");
}
```

## 测试

### 修改你的 Java 代码

**❌ 错误 - 会加载所有数据到内存**:
```java
ResultSet rs = stmt.executeQuery("SELECT * FROM r2api");
```

**✅ 正确 - 只加载 1000 行**:
```java
ResultSet rs = stmt.executeQuery("SELECT * FROM r2api LIMIT 1000");

int count = 0;
while (rs.next()) {
    System.out.println("app_name: " + rs.getString("app_name"));
    count++;
}
System.out.println("读取了 " + count + " 行");
```

### 验证效果

```bash
# 终端 1: 启动服务并监控内存
./target/release/calm mysql --port 3307 &
watch -n 1 'ps aux | grep calm | grep -v grep | awk "{print \$6/1024 \" MB\"}"'

# 终端 2: 运行 Java 程序(带 LIMIT)
java -cp .:mysql-connector-*.jar MysqlCursor
```

**预期**:
- 有 LIMIT: 内存稳定在 ~200MB
- 无 LIMIT: 返回错误,不执行查询

## 长期解决方案(需要重构)

### 方案 A: 流式查询执行

修改 `DistributedExecutor.execute_serial_full_scan()`:

```rust
// ❌ 当前实现
async fn execute_serial_full_scan() -> QueryResult {
    let mut all_batches = Vec::new();
    for partition in partitions {
        all_batches.extend(execute_on_partition());  // 全部累积
    }
    concat_batches(all_batches)  // 一次性合并
}

// ✅ 流式实现
async fn execute_serial_full_scan_stream() 
    -> impl Stream<Item = RecordBatch> 
{
    stream! {
        for partition in partitions {
            let batches = execute_on_partition();
            for batch in batches {
                yield batch;  // 立即返回,不累积
            }
        }
    }
}
```

### 方案 B: 分页查询

在 MySQL 协议层实现自动分页:

```rust
async fn execute_query_paged<W: io::Write>(
    engine: Arc<Engine>,
    query: &str,
    results: QueryResultWriter<'_, W>,
) -> io::Result<()> {
    const PAGE_SIZE: usize = 10_000;
    
    let mut offset = 0;
    loop {
        // 自动添加 LIMIT/OFFSET
        let paged_query = format!("{} LIMIT {} OFFSET {}", 
            query, PAGE_SIZE, offset);
        
        let result = engine.execute_sql(&paged_query).await?;
        
        // 发送这一页
        write_partial_result(results, &result.batch)?;
        
        if result.batch.num_rows() < PAGE_SIZE {
            break;  // 最后一页
        }
        offset += PAGE_SIZE;
    }
}
```

### 方案 C: Cursor 真实实现

游标应该是懒执行的:

```rust
struct Cursor {
    query: String,      // 保存 SQL
    executed: bool,     // 是否已执行
    partitions: Vec<String>,
    current_partition: usize,
    current_offset: usize,
}

impl Cursor {
    fn new(query: String) -> Self {
        // DECLARE 时不执行,只保存
        Cursor {
            query,
            executed: false,
            ...
        }
    }
    
    async fn fetch(&mut self, n: usize) -> Vec<RecordBatch> {
        // FETCH 时才执行,每次只取 n 行
        let results = Vec::new();
        let mut remaining = n;
        
        while remaining > 0 {
            // 从当前 partition 的当前 offset 开始取
            let batch = self.fetch_from_partition(
                self.current_partition,
                self.current_offset,
                remaining
            ).await?;
            
            results.push(batch);
            remaining -= batch.num_rows();
            
            // 更新位置
            self.current_offset += batch.num_rows();
        }
        
        results
    }
}
```

## 为什么之前的修改没生效?

1. **背压修复**: 只在发送阶段生效,但数据已经在内存了
2. **行数限制**: 检查在查询执行**之后**,内存已经占用了
3. **连接清理**: 可以释放内存,但需要等连接断开

## 立即行动

### 应用端(Java)

修改所有查询,添加 LIMIT:

```java
// 全表扫描: 分批读取
for (int offset = 0; ; offset += 10000) {
    String sql = "SELECT * FROM r2api LIMIT 10000 OFFSET " + offset;
    ResultSet rs = stmt.executeQuery(sql);
    
    int count = 0;
    while (rs.next()) {
        // 处理数据
        count++;
    }
    
    if (count < 10000) {
        break;  // 最后一批
    }
}
```

### 服务器端

降低限制,强制应用加 LIMIT:

```rust
// 测试环境
const MAX_ROWS_WITHOUT_LIMIT: usize = 10_000;

// 生产环境
const MAX_ROWS_WITHOUT_LIMIT: usize = 50_000;
```

## 监控

```bash
# 1. 实时内存
watch -n 1 'ps aux | grep calm | awk "{print \$6/1024 \" MB\"}"'

# 2. 查看大查询日志
tail -f /tmp/calm_mysql.log | grep "Large result"

# 3. 连接数
watch -n 5 'lsof -i :3307 | wc -l'
```

## 总结

**问题**: `SELECT * FROM table` 一次性加载所有数据到内存

**临时方案**: 
- 降低行数限制到 5万
- 强制用户加 LIMIT
- 修改 Java 代码,所有查询加 LIMIT

**长期方案**: 
- 实现流式查询执行
- 或实现真正的游标(懒执行)
- 或在协议层自动分页

**重要**: 
- 当前的所有修复(背压、清理日志)都是在查询执行**之后**
- 真正要解决内存问题,必须在查询执行**之前**或**过程中**控制
