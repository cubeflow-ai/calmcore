# Federation 分布式查询模块

## 概述

Federation 模块实现了 Calm Database 的分布式查询能力，基于 `datafusion-federation` 框架，使用 Arrow Flight 作为节点间通信协议。

## 架构

```
┌─────────────────────────────────────────────────┐
│         FederatedQueryExecutor                  │
│  (协调器 - 解析SQL、分类分区、注册表)              │
└─────────────────┬───────────────────────────────┘
                  │
        ┌─────────┴─────────┐
        │                   │
┌───────▼────────┐  ┌──────▼──────────┐
│ Local Partitions│  │Remote Partitions│
│UnionTableProvider│  │RemoteTableProvider│
└────────┬────────┘  └──────┬──────────┘
         │                  │
         │           ┌──────▼──────────┐
         │           │ RemoteScanExec  │
         │           │ (执行计划)       │
         │           └──────┬──────────┘
         │                  │
         │           ┌──────▼──────────┐
         │           │ FlightExecutor  │
         │           │ (Flight 客户端) │
         │           └──────┬──────────┘
         │                  │
         └──────────────────┴─── Arrow Flight ───► 远程节点
```

## 核心组件

### 1. FlightExecutor ([flight_executor.rs](flight_executor.rs))

Arrow Flight 客户端封装，负责与远程节点通信。

**关键功能**:
- 建立 Flight 连接
- 发送 SQL 查询（通过 Ticket）
- 接收流式结果
- 类型转换（FlightRecordBatchStream → SendableRecordBatchStream）

**使用示例**:
```rust
let executor = FlightExecutor::new(
    "node_20231225120530_127.0.0.1_52000".to_string(),
    "http://127.0.0.1:52000".to_string(),
);

let stream = executor.execute_sql("SELECT * FROM events WHERE value > 100").await?;
```

### 2. RemoteTableProvider ([remote_provider.rs](remote_provider.rs))

实现 DataFusion 的 `TableProvider` trait，将远程分区包装为可查询的表。

**关键功能**:
- Filter Pushdown（WHERE 条件下推）
- Projection Pushdown（列裁剪）
- Limit Pushdown
- Expr → SQL 转换

**Pushdown 支持**:

| 功能 | 状态 | 说明 |
|------|------|------|
| Filter | ✅ | 支持 `=, !=, <, <=, >, >=, AND, OR` |
| Projection | ✅ | 自动生成 SELECT 子句 |
| Limit | ✅ | 直接传递 LIMIT |
| Join | ❌ | 计划中 |
| Aggregate | ❌ | 计划中 |

**示例**:
```rust
let provider = RemoteTableProvider::new(
    "events".to_string(),
    vec!["partition_0".to_string(), "partition_1".to_string()],
    schema.clone(),
    Arc::new(executor),
);

// 会生成 SQL: SELECT event_id, value FROM events WHERE value > 100
```

### 3. RemoteScanExec ([remote_scan_exec.rs](remote_scan_exec.rs))

DataFusion 执行计划节点，调用 FlightExecutor 获取远程数据。

**特点**:
- 异步流式执行
- 自动错误转换
- 支持并行分区扫描

### 4. FederatedQueryExecutor ([query_executor.rs](query_executor.rs))

顶层协调器，负责整个分布式查询的执行流程。

**执行流程**:
1. 解析 SQL，提取表名（使用 `sqlparser`）
2. 加载表元数据（从 Catalog）
3. 分类分区（本地 vs 远程，根据 `owner` 字段）
4. 创建 TableProvider：
   - 本地 → `UnionTableProvider`
   - 远程 → `RemoteTableProvider`
5. 注册到 DataFusion SessionContext
6. 执行查询，返回流式结果

## 使用方法

### 集成到 CalmService

已自动集成到 `CalmService::execute_query_stream()`，无需手动调用：

```rust
// src/calm/mod.rs
pub async fn execute_query_stream(&self, sql: &str) -> CoreResult<SendableRecordBatchStream> {
    if let Some(cm) = self.cluster_manager.as_ref() {
        // 集群模式 → Federation
        let executor = FederatedQueryExecutor::new(
            self.catalog.clone(),
            self.engine.clone(),
            cm.clone(),
        );
        return executor.execute(sql).await;
    }
    
    // 单机模式 → 回退
    // ...
}
```

### 单机模式兼容

如果没有 `ClusterManager`（单机模式），自动回退到本地查询，不影响现有功能。

## Filter Pushdown 详解

### 支持的表达式

```rust
// 比较运算符
WHERE value = 100
WHERE value != 100
WHERE value < 100
WHERE value >= 100

// 逻辑运算符
WHERE value > 100 AND event_type = 'purchase'
WHERE user_id = 123 OR user_id = 456

// 支持的数据类型
String, Int64, UInt64, Float64, Boolean
```

### 转换示例

```rust
// DataFusion Expr
BinaryExpr {
    left: Column("value"),
    op: Operator::Gt,
    right: Literal(ScalarValue::Int64(100))
}

// 转换为 SQL
"value > 100"
```

### 实现原理

`RemoteTableProvider::expr_to_sql()` 递归遍历表达式树：

```rust
match expr {
    Expr::BinaryExpr(binary) => {
        let left = expr_to_sql(&binary.left)?;
        let right = expr_to_sql(&binary.right)?;
        format!("{} {} {}", left, op, right)
    }
    Expr::Column(col) => col.name.clone(),
    Expr::Literal(scalar, _) => format_scalar(scalar),
}
```

## 配置

### 节点端点

通过 ClusterManager 自动管理：

```rust
let endpoint = format!("http://{}:{}", node.host, node.grpc_port);
```

### 分区路由

使用 `PartitionMeta.owner` 字段：

```rust
for (partition_name, partition_meta) in partitions.iter() {
    if partition_meta.owner == my_node_id {
        // 本地分区
        local_partitions.push(...);
    } else {
        // 远程分区
        remote_partitions.entry(partition_meta.owner)
            .or_insert_with(Vec::new)
            .push(partition_name);
    }
}
```

## 性能优化

### 1. 并行执行

本地和远程扫描可以并行：

```rust
// UnionExec 自动并行化
UnionExec::new(vec![local_scan, remote_scan])
```

### 2. 流式传输

Arrow Flight 原生支持流式传输，无需等待全部数据。

### 3. 谓词下推

减少网络传输：

```sql
-- 不使用 pushdown: 传输 10,000 行
SELECT * FROM events  
-- 本地过滤: WHERE value > 9000

-- 使用 pushdown: 只传输 1,000 行
SELECT * FROM events WHERE value > 9000
```

## 测试

### 运行测试

```bash
# 完整测试（单机 + 集群）
./test_federation.sh

# 单元测试（TODO: 待添加）
cargo test federation
```

### 测试场景

1. **单机回退**: 验证无 ClusterManager 时正常工作
2. **跨节点查询**: 数据分布在多个节点
3. **Filter Pushdown**: WHERE 条件正确下推
4. **Projection Pushdown**: 列裁剪正确应用
5. **混合查询**: 本地 + 远程分区同时查询

## 故障排查

### 常见问题

**1. Flight 连接失败**
```
Error: Network(Failed to connect: ...)
```
解决：检查远程节点的 grpc_port 是否正确，网络是否可达。

**2. Schema 不匹配**
```
Error: Flight stream has no schema
```
解决：确保远程节点返回了正确的 Flight 元数据。

**3. Filter 不生效**
```
Warning: Filters present but not pushed down yet
```
解决：检查 Filter 表达式是否支持（参考支持列表）。

### 调试日志

启用详细日志：

```bash
RUST_LOG=calm::compute::federation=debug cargo run
```

关键日志：
- `[FlightExecutor] Executing SQL on node ...` - 远程查询开始
- `[RemoteTableProvider] Generated SQL ...` - Pushdown SQL
- `[FederatedQueryExecutor] Registering table ...` - 表注册

## 限制和已知问题

### 当前限制

1. **复杂表达式**: LIKE, IN, BETWEEN 暂不支持
2. **Join Pushdown**: 跨节点 Join 尚未优化
3. **Aggregate Pushdown**: 聚合函数需要在本地执行
4. **事务**: 不支持跨节点事务

### 计划支持

- [ ] 复杂 Filter 表达式（LIKE, IN, BETWEEN）
- [ ] Join Pushdown
- [ ] Aggregate Pushdown
- [ ] 连接池（复用 Flight 连接）
- [ ] 查询结果缓存
- [ ] 部分结果返回（容错）

## 与其他模块的关系

```
CalmService
    │
    ├── execute_query_stream() ──► FederatedQueryExecutor
    │                                  │
    │                                  ├── Catalog (元数据)
    │                                  ├── Engine (本地分区)
    │                                  └── ClusterManager (节点信息)
    │
    ├── insert_data() ──────────────► Router (分区路由)
    │
    └── create_table() ─────────────► Catalog
```

## 扩展指南

### 添加新的 Pushdown

1. 在 `RemoteTableProvider::expr_to_sql()` 中添加表达式匹配
2. 更新测试用例
3. 更新文档

### 添加连接池

```rust
pub struct FlightExecutor {
    node_id: String,
    endpoint: String,
    connection_pool: Arc<ConnectionPool>, // 新增
}

impl FlightExecutor {
    async fn get_connection(&self) -> CoreResult<FlightClient> {
        self.connection_pool.get_or_create(&self.endpoint).await
    }
}
```

## 参考资料

- [DataFusion Federation](https://docs.rs/datafusion-federation/)
- [Arrow Flight](https://arrow.apache.org/docs/format/Flight.html)
- [Calm Architecture](../../docs/architecture.md)

## 版本历史

- **v1.0** (2026-01-02): 初始实现
  - FlightExecutor
  - RemoteTableProvider
  - Filter/Projection/Limit Pushdown
  - FederatedQueryExecutor
