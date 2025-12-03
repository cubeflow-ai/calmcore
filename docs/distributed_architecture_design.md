# CalmCore 分布式架构设计方案

## 📋 系统特性与需求

### 核心特性
- ✅ **HTAP 混合负载**：同时支持事务处理 (TP) 和分析查询 (AP)
- ✅ **主键约束**：需要唯一性保证和去重
- ✅ **共享存储**：使用 CubeFS（类似 S3 的文件系统协议）
- ✅ **分区表**：数据按时间或其他维度分区
- ✅ **无状态计算**：所有节点共享同一存储，无本地数据副本

### 关键约束
- ⚠️ **写入一致性**：同一分区不能并发写入（会导致主键冲突）
- ⚠️ **主键去重**：需要读取现有数据来检查重复
- ⚠️ **顺序保证**：相同主键的多次写入需要保证顺序

---

## 🏗️ 推荐架构：Partition Owner + Gossip

### 架构图

```
┌────────────────────────────────────────────────┐
│           Gossip 协调层 (Chitchat)              │
│  - 节点发现和健康检查                            │
│  - Partition Owner 映射维护                     │
│  - 元数据缓存同步 (Schema, Segment List)        │
│  - 故障检测与自动转移                            │
└────────────────────────────────────────────────┘
                    ↓
┌──────────────┐  ┌──────────────┐  ┌──────────────┐
│   Node 1     │  │   Node 2     │  │   Node 3     │
│ ┌──────────┐ │  │ ┌──────────┐ │  │ ┌──────────┐ │
│ │Owner:    │ │  │ │Owner:    │ │  │ │Owner:    │ │
│ │P0, P3, P6│ │  │ │P1, P4, P7│ │  │ │P2, P5, P8│ │
│ │          │ │  │ │          │ │  │ │          │ │
│ │可读:     │ │  │ │可读:     │ │  │ │可读:     │ │
│ │所有分区  │ │  │ │所有分区  │ │  │ │所有分区  │ │
│ └──────────┘ │  │ └──────────┘ │  │ └──────────┘ │
└──────┬───────┘  └──────┬───────┘  └──────┬───────┘
       │                 │                 │
       └─────────────────┼─────────────────┘
                         ↓
                  ┌─────────────┐
                  │   CubeFS    │
                  │ (共享存储)   │
                  │             │
                  │ /data/      │
                  │   r2api/    │
                  │     p0/     │
                  │     p1/     │
                  │     ...     │
                  │ /index/     │
                  └─────────────┘
```

### 核心设计原则

#### 1. Partition Ownership（分区所有权）
- **每个分区在任意时刻只有一个 Owner 节点负责写入**
- Owner 信息存储在 Gossip 共享状态中，所有节点可见
- Owner 节点故障时，自动选举新的 Owner（秒级转移）
- **所有节点都可以读取任意分区**（因为是共享存储）

#### 2. 无状态计算节点
- 计算节点不存储数据，只负责计算
- 节点可以随时加入或离开集群
- 弹性伸缩简单：加减节点无需数据迁移
- 故障恢复容易：节点挂了重启即可

#### 3. 共享存储优势
- 所有数据存在 CubeFS，天然支持多读
- 无需数据复制，CubeFS 自己保证持久性
- 避免了传统分布式数据库的数据搬迁问题

---

## 🔄 核心流程设计

### 1. 写入流程

```rust
┌─────────┐      1. INSERT         ┌─────────┐
│ Client  │ ──────────────────────>│ Any Node│
└─────────┘                        └────┬────┘
                                        │
                        2. 计算分区键 (hash/range)
                                        │
                        3. 查询 Partition Owner
                                        │
                         ┌──────────────┴──────────────┐
                         │                             │
                    本节点是Owner              非Owner，转发RPC
                         │                             │
                         ↓                             ↓
                  ┌─────────────┐            ┌─────────────┐
                  │ 本地写入:    │            │ RPC Forward │
                  │ 1.加载PK索引 │            │  to Owner   │
                  │ 2.主键去重   │            └─────────────┘
                  │ 3.写Parquet  │
                  │ 4.更新索引   │
                  └──────┬──────┘
                         │
                         ↓
                    ┌─────────┐
                    │ CubeFS  │
                    └─────────┘
```

#### 代码示例

```rust
impl WriteCoordinator {
    pub async fn insert(&self, table: &str, batch: RecordBatch) -> Result<()> {
        // 1. 计算分区键
        let partition_id = self.compute_partition(table, &batch)?;
        
        // 2. 查找 Partition Owner（从 Gossip 共享状态）
        let owner = self.gossip.get_partition_owner(table, partition_id)?;
        
        // 3. 路由：本地或转发
        if owner == self.node_id {
            self.write_local(table, partition_id, batch).await?;
        } else {
            // RPC 转发到 Owner 节点
            self.rpc_client
                .forward_write(owner, table, partition_id, batch)
                .await?;
        }
        
        Ok(())
    }
    
    async fn write_local(&self, table: &str, partition_id: u32, batch: RecordBatch) 
        -> Result<()> 
    {
        let partition_path = format!("/cubefs/data/{}/p{}", table, partition_id);
        
        // 1. 加载现有段的主键索引
        let existing_keys = self.load_primary_keys(&partition_path)?;
        
        // 2. 主键去重（Upsert 语义）
        let deduplicated = self.deduplicate_by_primary_key(batch, &existing_keys)?;
        
        // 3. 写入新段到 CubeFS
        let segment_id = self.next_segment_id(table, partition_id)?;
        let segment_path = format!("{}/segment_{:06}.parquet", 
                                  partition_path, segment_id);
        self.write_parquet_to_cubefs(&segment_path, deduplicated).await?;
        
        // 4. 更新主键索引
        self.update_primary_key_index(&partition_path, segment_id).await?;
        
        Ok(())
    }
}
```

### 2. 读取流程（分布式并行扫描）

```rust
┌─────────┐      SELECT         ┌─────────────┐
│ Client  │ ─────────────────>  │ Any Node    │
└─────────┘                     └──────┬──────┘
                                       │
                        1. 列出所有分区的段文件
                                       │
                        2. 分配给集群节点并行扫描
                                       │
            ┌──────────────────────────┼──────────────────────────┐
            │                          │                          │
            ↓                          ↓                          ↓
    ┌───────────┐            ┌───────────┐            ┌───────────┐
    │  Node 1   │            │  Node 2   │            │  Node 3   │
    │扫描 P0,P3 │            │扫描 P1,P4 │            │扫描 P2,P5 │
    └─────┬─────┘            └─────┬─────┘            └─────┬─────┘
          │                        │                        │
          └────────────────────────┼────────────────────────┘
                                   │
                           3. 流式返回并合并
                                   │
                                   ↓
                            ┌─────────────┐
                            │   Client    │
                            └─────────────┘
```

#### 代码示例

```rust
impl DistributedQueryExecutor {
    pub async fn execute(&self, sql: &str) -> Result<SendableRecordBatchStream> {
        // 1. 解析 SQL，提取表名
        let table_name = extract_table_name(sql)?;
        
        // 2. 从 CubeFS 列出所有段文件
        let all_segments = self.list_segments_from_cubefs(&table_name).await?;
        
        // 3. 分配给集群节点（任意节点都能读）
        let nodes = self.cluster.alive_nodes();
        let tasks = self.distribute_segments(&all_segments, &nodes);
        
        // 4. 并行执行
        let mut streams = vec![];
        for (node, segments) in tasks {
            if node.id == self.cluster.node_id {
                // 本地执行
                let stream = self.local_engine
                    .scan_segments(sql, segments)
                    .await?;
                streams.push(stream);
            } else {
                // 远程 RPC 调用
                let stream = self.rpc_client
                    .execute_remote(node, sql, segments)
                    .await?;
                streams.push(stream);
            }
        }
        
        // 5. 合并流
        Ok(merge_streams(streams))
    }
}
```

### 3. Partition Owner 管理

#### 初始分配

```rust
impl PartitionManager {
    pub async fn initialize_partitions(&self, table: &str, num_partitions: u32) 
        -> Result<()> 
    {
        let nodes = self.gossip.live_nodes();
        
        // 轮询分配
        for partition_id in 0..num_partitions {
            let owner_idx = (partition_id as usize) % nodes.len();
            let owner = &nodes[owner_idx];
            
            // 写入 Gossip 共享状态
            let key = format!("partition_owner:{}:{}", table, partition_id);
            self.gossip.set_key_value(key, owner.node_id.clone());
        }
        
        log::info!("✅ Initialized {} partitions for {}", num_partitions, table);
        Ok(())
    }
}
```

#### 故障转移

```rust
impl PartitionManager {
    pub async fn handle_node_failure(&self, failed_node: &str) -> Result<()> {
        log::warn!("⚠️  Node {} failed, transferring partitions", failed_node);
        
        // 1. 找出所有由该节点负责的分区
        let owned_partitions = self.gossip
            .iter()
            .filter(|(k, v)| {
                k.starts_with("partition_owner:") && v == failed_node
            })
            .map(|(k, _)| k.clone())
            .collect::<Vec<_>>();
        
        // 2. 重新分配给其他活跃节点
        let alive_nodes = self.gossip.live_nodes();
        for (idx, partition_key) in owned_partitions.iter().enumerate() {
            let new_owner = &alive_nodes[idx % alive_nodes.len()];
            self.gossip.set_key_value(
                partition_key.clone(),
                new_owner.node_id.clone(),
            );
            log::info!("✅ Transferred {} to {}", partition_key, new_owner.node_id);
        }
        
        Ok(())
    }
}
```

---

## 🛠️ 技术选型

### Gossip 协议库

推荐使用 **Chitchat**：
- Rust 原生实现，性能好
- Quickwit 搜索引擎在用，稳定性经过验证
- API 简单易用
- 支持键值对存储（用于 Partition Owner 映射）

```toml
[dependencies]
chitchat = "0.7"
```

### RPC 框架

推荐使用 **Tonic (gRPC)**：
- 高性能、流式支持
- 与 Arrow 生态集成好
- 支持双向流（适合查询结果返回）

```toml
[dependencies]
tonic = "0.11"
prost = "0.12"
```

### 配置示例

```toml
# calm.toml
[cluster]
enabled = true
node_id = "node-1"  # 自动生成或手动指定
listen_addr = "0.0.0.0:7946"

# 种子节点列表
seeds = [
    "node-1.example.com:7946",
    "node-2.example.com:7946",
    "node-3.example.com:7946"
]

# RPC 服务
rpc_listen = "0.0.0.0:9090"

[storage]
type = "cubefs"
mount_point = "/mnt/cubefs"
data_dir = "/mnt/cubefs/calm/data"
index_dir = "/mnt/cubefs/calm/index"

[table.r2api]
partition_by = "date_trunc('day', timestamp)"
partition_count = 365
primary_key = "id"
```

---

## 📊 性能分析

### 写入吞吐量

假设：
- 10 个节点
- 每个节点负责 100 个分区
- 单节点写入吞吐：10K rows/s

**总吞吐**：10 节点 × 10K = **100K rows/s**

#### 优化热点分区

如果写入集中在热分区（例如今天的分区），可以提高分区粒度：

```rust
// 从按天分区改为按小时分区
partition_by = "date_trunc('hour', timestamp)"

// 热点分区从 1 个变成 24 个
// 写入吞吐提升 24 倍！
```

### 查询性能

- **并行度**：等于集群节点数
- **网络开销**：仅传输最终结果，不传输原始数据
- **缓存优化**：元数据（Schema、段列表）在本地缓存

---

## 🎯 实现路线图

### Phase 1: 基础 Gossip 集群（1-2周）

- [ ] 集成 Chitchat
- [ ] 实现节点发现和健康检查
- [ ] 实现 Partition Owner 映射维护
- [ ] 配置文件支持

### Phase 2: 分布式写入（2-3周）

- [ ] 实现写入路由逻辑
- [ ] RPC 服务搭建（Tonic）
- [ ] 主键索引维护
- [ ] 故障转移机制

### Phase 3: 分布式查询（2-3周）

- [ ] 任务分配算法
- [ ] 并行扫描实现
- [ ] 结果流合并
- [ ] 元数据缓存

### Phase 4: 优化与监控（持续）

- [ ] 负载均衡
- [ ] 性能调优
- [ ] 监控指标
- [ ] 故障恢复测试

---

## 🆚 对比其他架构

| 系统 | 架构模式 | 主键处理 | 存储类型 | 一致性 |
|------|---------|---------|---------|-------|
| **CalmCore** | Partition Owner + 共享存储 | 写时去重 | 共享(CubeFS) | 强一致 |
| **Snowflake** | Virtual Warehouse + S3 | 不支持主键 | 共享(S3) | 强一致 |
| **TiDB** | Region Leader + Raft | LSM-Tree | 分布式KV | 强一致 |
| **ClickHouse** | 本地分区 + 分布式表 | ReplacingMergeTree | 本地磁盘 | 最终一致 |
| **StarRocks** | Tablet Owner | Primary Key Table | 本地磁盘 | 强一致 |
| **Databend** | Partition Owner | Merge on Read | 共享(S3) | 强一致 |

**CalmCore 架构最接近 Snowflake + Databend 模式**，是云原生数据库的主流方向！

---

## 🔒 一致性保证

### 写入一致性
- 同一分区只有一个 Owner 写入 → **强一致性**
- 不同分区可以并发写入 → **高吞吐**

### 读取一致性
- 从 CubeFS 读取最新数据 → **读己之写一致性**
- 元数据缓存可能略有延迟（Gossip 传播） → **最终一致**

### 故障恢复
- Owner 节点故障 → 自动转移（30s 内完成）
- 转移期间该分区不可写，但可读
- CubeFS 保证数据不丢失

---

## ⚠️ 潜在问题与解决方案

### 1. 热点分区

**问题**：某些分区写入量特别大（例如今天的分区）

**解决方案**：
- 提高分区粒度（按小时而非按天）
- 二级分区（时间 + hash）
- 动态分区分裂

### 2. Owner 节点过载

**问题**：某个节点负责太多热点分区

**解决方案**：
- 负载感知的分区分配算法
- 动态重新平衡
- 限流和背压机制

### 3. 元数据同步延迟

**问题**：Gossip 传播有延迟，可能路由到旧的 Owner

**解决方案**：
- 旧 Owner 收到请求后转发给新 Owner
- 在共享存储中加锁（轻量级文件锁）
- 增加 Gossip 传播频率

---

## 📚 参考资料

### 类似架构的开源项目
- **Databend** (Rust): https://github.com/datafuselabs/databend
- **Snowflake** (专利): Snowflake Architecture Paper
- **Quickwit** (Rust): https://github.com/quickwit-oss/quickwit
- **ClickHouse**: https://github.com/ClickHouse/ClickHouse

### 技术文档
- Gossip 协议: https://en.wikipedia.org/wiki/Gossip_protocol
- SWIM 故障检测: https://www.cs.cornell.edu/projects/Quicksilver/public_pdfs/SWIM.pdf
- Chitchat 文档: https://docs.rs/chitchat/latest/chitchat/

---

## 📝 下一步行动

1. **决策确认**：确认这个架构方向
2. **PoC 开发**：先做一个 3 节点的 PoC
3. **性能测试**：验证写入吞吐和查询延迟
4. **生产部署**：逐步推广到生产环境

---

**文档版本**: v1.0  
**创建日期**: 2025-12-02  
**作者**: CalmCore Team  
**状态**: 设计阶段
