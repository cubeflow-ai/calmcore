# Design Document: Distributed Query

## Overview

本文档描述 CalmDB 分布式查询系统的技术设计。核心设计原则是**零侵入**：通过新增独立模块实现分布式能力，不修改现有单机代码路径。

### Key Design Decisions

1. **零侵入设计**: 单机代码完全不动，分布式代码在独立目录
2. **复用核心组件**: 分布式模式复用 SegmentScanner、索引优化等
3. **Shuffle 在 Scan 层**: GROUP BY 的 Hash 分发在数据扫描时完成
4. **流式处理**: 全程使用 Stream，避免内存爆炸

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           Client Request                                 │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                         QueryCoordinator                                 │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │  1. 判断单机/分布式模式                                           │   │
│  │  2. 解析 SQL，提取 GROUP BY 字段                                  │   │
│  │  3. 查询 PartitionManager 获取 Partition 分布                     │   │
│  │  4. 选择执行路径                                                  │   │
│  └─────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────┘
                    │                               │
         单机模式    │                               │  分布式模式
                    ▼                               ▼
┌──────────────────────────┐       ┌──────────────────────────────────────┐
│  DataFusionExecutor      │       │  DistributedExecutor                 │
│  (现有代码，不修改)        │       │  (新增代码)                           │
│                          │       │                                      │
│  UnionTableProvider      │       │  DistributedTableProvider            │
│         ↓                │       │         ↓                            │
│  SegmentScanner          │       │  ShuffleExec (如果有 GROUP BY)        │
│         ↓                │       │         ↓                            │
│  SegmentStream           │       │  本地: 复用 SegmentScanner            │
│                          │       │  远程: RemoteScanExec                │
└──────────────────────────┘       └──────────────────────────────────────┘
```

## Components and Interfaces

### 1. QueryCoordinator

查询协调器，统一入口。

```rust
pub struct QueryCoordinator {
    /// 单机执行器 (现有)
    datafusion_executor: Arc<DataFusionExecutor>,
    /// 分布式执行器 (新增)
    distributed_executor: Option<Arc<DistributedExecutor>>,
    /// 集群管理器
    cluster_manager: Arc<ClusterManager>,
    /// 分区管理器
    partition_manager: Arc<PartitionManager>,
}

impl QueryCoordinator {
    /// 执行 SQL 查询
    pub async fn execute_sql(&self, sql: &str) -> CoreResult<SendableRecordBatchStream> {
        // 单机模式：直接走现有路径
        if self.cluster_manager.is_standalone() {
            return self.datafusion_executor.execute_sql_stream(sql).await;
        }
        
        // 分布式模式
        self.distributed_executor
            .as_ref()
            .unwrap()
            .execute_sql(sql)
            .await
    }
}
```

### 2. DistributedExecutor

分布式执行器。

```rust
pub struct DistributedExecutor {
    /// 本地引擎
    engine: Arc<Engine>,
    /// 集群管理器
    cluster_manager: Arc<ClusterManager>,
    /// 分区管理器
    partition_manager: Arc<PartitionManager>,
    /// 节点客户端管理
    node_clients: Arc<NodeClientManager>,
    /// 配置
    config: DistributedConfig,
}

impl DistributedExecutor {
    /// 执行分布式 SQL
    pub async fn execute_sql(&self, sql: &str) -> CoreResult<SendableRecordBatchStream> {
        // 1. 解析 SQL
        let parsed = SqlNormalizer::normalize(sql)?;
        let table_name = self.extract_table_name(&parsed.rewritten_sql)?;
        
        // 2. 获取 Partition 分布
        let topology = self.partition_manager.get_table_topology(&table_name).await;
        let (local_partitions, remote_partitions) = self.classify_partitions(&topology);
        
        // 3. 判断是否需要 Shuffle
        let group_by_cols = self.extract_group_by_columns(&parsed)?;
        let needs_shuffle = !group_by_cols.is_empty();
        
        // 4. 创建执行计划
        if needs_shuffle {
            self.execute_with_shuffle(sql, &local_partitions, &remote_partitions, &group_by_cols).await
        } else {
            self.execute_scatter_gather(sql, &local_partitions, &remote_partitions).await
        }
    }
}
```

### 3. DistributedTableProvider

分布式 TableProvider。

```rust
pub struct DistributedTableProvider {
    schema: SchemaRef,
    local_partitions: Vec<Arc<Partition>>,
    remote_partitions: Vec<RemotePartitionInfo>,
    group_by_cols: Vec<String>,
    node_clients: Arc<NodeClientManager>,
}

pub struct RemotePartitionInfo {
    pub node_id: String,
    pub node_addr: String,
    pub partition_id: String,
    pub table_name: String,
}

#[async_trait]
impl TableProvider for DistributedTableProvider {
    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if self.group_by_cols.is_empty() {
            // 无 GROUP BY: Scatter-Gather
            self.create_scatter_gather_plan(projection, filters, limit).await
        } else {
            // 有 GROUP BY: Shuffle
            self.create_shuffle_plan(projection, filters, limit).await
        }
    }
}
```

### 4. ShuffleExec

Shuffle 执行计划。

```rust
pub struct ShuffleExec {
    /// 输入计划 (本地扫描)
    input: Arc<dyn ExecutionPlan>,
    /// GROUP BY 列索引
    group_by_indices: Vec<usize>,
    /// 节点数量 (用于 Hash 取模)
    num_nodes: usize,
    /// 本节点 ID
    my_node_index: usize,
    /// 发送到其他节点的 channel
    remote_senders: Vec<mpsc::Sender<RecordBatch>>,
    /// 从其他节点接收的 channel
    remote_receiver: mpsc::Receiver<RecordBatch>,
    /// Schema
    schema: SchemaRef,
}

impl ExecutionPlan for ShuffleExec {
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        // 创建 ShuffleStream
        let input_stream = self.input.execute(partition, context)?;
        
        Ok(Box::pin(ShuffleStream::new(
            input_stream,
            self.group_by_indices.clone(),
            self.num_nodes,
            self.my_node_index,
            self.remote_senders.clone(),
            self.remote_receiver.clone(),
            self.schema.clone(),
        )))
    }
}
```

### 5. ShuffleStream

Shuffle 流处理。

```rust
pub struct ShuffleStream {
    /// 本地输入流
    input: SendableRecordBatchStream,
    /// GROUP BY 列索引
    group_by_indices: Vec<usize>,
    /// 节点数量
    num_nodes: usize,
    /// 本节点索引
    my_node_index: usize,
    /// 远程发送器
    remote_senders: Vec<mpsc::Sender<RecordBatch>>,
    /// 远程接收器
    remote_receiver: mpsc::Receiver<RecordBatch>,
    /// Schema
    schema: SchemaRef,
    /// 本地数据缓冲
    local_buffer: VecDeque<RecordBatch>,
    /// 输入是否结束
    input_finished: bool,
}

impl Stream for ShuffleStream {
    type Item = DataFusionResult<RecordBatch>;
    
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        
        // 1. 先返回本地缓冲的数据
        if let Some(batch) = this.local_buffer.pop_front() {
            return Poll::Ready(Some(Ok(batch)));
        }
        
        // 2. 尝试从远程接收数据
        if let Poll::Ready(Some(batch)) = Pin::new(&mut this.remote_receiver).poll_recv(cx) {
            return Poll::Ready(Some(Ok(batch)));
        }
        
        // 3. 如果输入已结束，检查是否还有远程数据
        if this.input_finished {
            return Poll::Ready(None);
        }
        
        // 4. 从输入流读取数据并分发
        match Pin::new(&mut this.input).poll_next(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                this.distribute_batch(batch)?;
                // 递归调用以返回本地数据
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => {
                this.input_finished = true;
                // 发送 EOF 到所有远程节点
                this.send_eof();
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl ShuffleStream {
    /// 按 Hash 分发 batch
    fn distribute_batch(&mut self, batch: RecordBatch) -> DataFusionResult<()> {
        // 计算每行的目标节点
        let hash_values = self.compute_hash(&batch)?;
        
        // 按目标节点分组
        let mut node_batches: Vec<Vec<u32>> = vec![Vec::new(); self.num_nodes];
        for (row_idx, hash) in hash_values.iter().enumerate() {
            let target_node = (*hash as usize) % self.num_nodes;
            node_batches[target_node].push(row_idx as u32);
        }
        
        // 分发到各节点
        for (node_idx, row_indices) in node_batches.into_iter().enumerate() {
            if row_indices.is_empty() {
                continue;
            }
            
            let sub_batch = self.take_rows(&batch, &row_indices)?;
            
            if node_idx == self.my_node_index {
                // 本地数据
                self.local_buffer.push_back(sub_batch);
            } else {
                // 远程数据
                let _ = self.remote_senders[node_idx].try_send(sub_batch);
            }
        }
        
        Ok(())
    }
    
    /// 计算 GROUP BY 列的 Hash
    fn compute_hash(&self, batch: &RecordBatch) -> DataFusionResult<Vec<u64>> {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        
        let num_rows = batch.num_rows();
        let mut hashes = vec![0u64; num_rows];
        
        for col_idx in &self.group_by_indices {
            let column = batch.column(*col_idx);
            // 对每行计算 hash
            for row in 0..num_rows {
                let mut hasher = DefaultHasher::new();
                // 根据列类型获取值并 hash
                self.hash_value(column, row, &mut hasher);
                hashes[row] = hashes[row].wrapping_add(hasher.finish());
            }
        }
        
        Ok(hashes)
    }
}
```

### 6. NodeClientManager

节点客户端管理。

```rust
pub struct NodeClientManager {
    /// 节点连接池
    clients: RwLock<HashMap<String, Arc<NodeClient>>>,
    /// 集群管理器
    cluster_manager: Arc<ClusterManager>,
}

pub struct NodeClient {
    node_id: String,
    addr: String,
    /// HTTP 客户端 (用于发送查询)
    http_client: reqwest::Client,
}

impl NodeClient {
    /// 发送查询请求
    pub async fn execute_query(&self, sql: &str) -> CoreResult<SendableRecordBatchStream> {
        // 发送 HTTP 请求
        let response = self.http_client
            .post(&format!("http://{}/query", self.addr))
            .json(&QueryRequest { sql: sql.to_string() })
            .send()
            .await?;
        
        // 返回流式响应
        Ok(Box::pin(RemoteResultStream::new(response)))
    }
    
    /// 发送 Shuffle 数据
    pub async fn send_shuffle_batch(&self, batch: RecordBatch) -> CoreResult<()> {
        // 序列化为 Arrow IPC
        let ipc_data = self.serialize_batch(&batch)?;
        
        // 发送
        self.http_client
            .post(&format!("http://{}/shuffle", self.addr))
            .body(ipc_data)
            .send()
            .await?;
        
        Ok(())
    }
}
```

## Data Models

### Query Request/Response

```rust
#[derive(Serialize, Deserialize)]
pub struct QueryRequest {
    pub sql: String,
    pub query_id: String,
    pub timeout_ms: Option<u64>,
}

#[derive(Serialize, Deserialize)]
pub struct QueryResponse {
    pub query_id: String,
    pub status: QueryStatus,
    pub error: Option<String>,
}

pub enum QueryStatus {
    Running,
    Completed,
    Failed,
}
```

### Shuffle Message

```rust
#[derive(Serialize, Deserialize)]
pub struct ShuffleMessage {
    pub query_id: String,
    pub source_node: String,
    pub target_node: String,
    pub batch_index: u64,
    pub is_eof: bool,
    pub data: Vec<u8>,  // Arrow IPC 格式
}
```

## File Structure

```
src/compute/
├── mod.rs                          # 添加 distributed 模块
├── ballista_executor.rs            # 不变
├── sql_normalizer.rs               # 不变
├── table_provider/                 # 不变
│   ├── mod.rs
│   ├── union_table.rs
│   ├── segment_scanner.rs
│   └── partition_table_provider.rs
│
└── distributed/                    # 🆕 新增
    ├── mod.rs
    ├── query_coordinator.rs        # 查询协调器
    ├── distributed_executor.rs     # 分布式执行器
    ├── distributed_table.rs        # 分布式 TableProvider
    ├── shuffle_exec.rs             # Shuffle 执行计划
    ├── shuffle_stream.rs           # Shuffle 流
    ├── node_client.rs              # 节点客户端
    └── config.rs                   # 配置
```

## Correctness Properties

*A property is a characteristic or behavior that should hold true across all valid executions of a system-essentially, a formal statement about what the system should do. Properties serve as the bridge between human-readable specifications and machine-verifiable correctness guarantees.*

### Property 1: 单机模式路径不变

*For any* SQL 查询在单机模式下执行，代码路径 SHALL 与分布式功能添加前完全相同，不经过任何新增代码。

**Validates: Requirements 1.1, 1.2**

### Property 2: 单机模式性能不变

*For any* SQL 查询在单机模式下执行，执行时间 SHALL 与分布式功能添加前相差不超过 5%。

**Validates: Requirements 1.3**

### Property 3: Shuffle Hash 一致性

*For any* GROUP BY 查询，相同 GROUP BY key 的所有行 SHALL 被分发到同一个节点。

**Validates: Requirements 5.2, 5.3, 5.4, 6.3**

### Property 4: Shuffle 完整性

*For any* 分布式 GROUP BY 查询，所有输入行 SHALL 恰好出现在一个节点的聚合输入中（不丢失、不重复）。

**Validates: Requirements 6.1, 6.2, 6.4, 6.5**

### Property 5: 聚合结果正确性

*For any* 分布式 GROUP BY 查询，最终结果 SHALL 与单机执行相同查询的结果一致。

**Validates: Requirements 5.5, 5.6, 7.2**

### Property 6: Scatter-Gather 结果正确性

*For any* 无 GROUP BY 的分布式查询，最终结果 SHALL 与单机执行相同查询的结果一致（行顺序可能不同）。

**Validates: Requirements 4.1, 4.2, 7.1**

## Error Handling

| 错误场景 | 处理策略 |
|---------|---------|
| 节点不可达 | 返回错误，包含节点信息 |
| 查询超时 | 取消所有节点查询，返回超时错误 |
| Shuffle 失败 | 取消查询，返回 Shuffle 错误 |
| 部分节点失败 | 取消其他节点，返回错误 |

## Testing Strategy

### Unit Tests

1. QueryCoordinator 路径选择
2. Hash 计算一致性
3. Batch 分发逻辑
4. 结果合并逻辑

### Property-Based Tests

使用 `proptest` 验证：
1. Shuffle Hash 一致性 (Property 3)
2. Shuffle 完整性 (Property 4)
3. 聚合结果正确性 (Property 5)

### Integration Tests

1. 单机模式性能对比
2. 多节点 Scatter-Gather
3. 多节点 Shuffle + GROUP BY
4. 错误处理场景

## Configuration

```toml
[distributed]
# 查询超时 (毫秒)
query_timeout_ms = 30000

# Shuffle 缓冲区大小 (行数)
shuffle_buffer_size = 10000

# 最大并发查询数
max_concurrent_queries = 100

# 节点间通信端口
rpc_port = 7947
```

