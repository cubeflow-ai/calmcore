# 种子节点和故障检测机制详解

## 🌱 种子节点（Seed Nodes）

### 什么是种子节点？

**种子节点不是特殊角色，而是一种启动模式。**

在 Gossip 协议中，种子节点只是：
- 已知的、稳定的、先启动的节点
- 新节点加入集群时的**初始联系点**
- 帮助新节点发现集群中的其他节点

### 种子节点的职责

```
┌─────────────────────────────────────────────┐
│  种子节点的职责（仅在新节点加入时）          │
│                                             │
│  1. 接受新节点的连接请求                     │
│  2. 告诉新节点"集群中还有哪些节点"           │
│  3. 完成引导后，所有节点平等                 │
└─────────────────────────────────────────────┘
```

**关键点**：
- ❌ 种子节点不是 Master
- ❌ 种子节点不存储特殊数据
- ❌ 种子节点故障不影响已加入的节点
- ✅ 种子节点只在初始引导时有用
- ✅ 所有节点启动后地位平等

### 启动流程示例

```
时刻 T0: 启动节点 1（种子节点）
┌──────────┐
│  Node 1  │  seeds=[]  （第一个节点，无种子）
└──────────┘

时刻 T1: 启动节点 2
┌──────────┐    ┌──────────┐
│  Node 1  │───>│  Node 2  │  seeds=[Node1]
└──────────┘    └──────────┘
   引导         新节点通过 Node1 发现集群

时刻 T2: 启动节点 3
┌──────────┐    ┌──────────┐
│  Node 1  │    │  Node 2  │
└─────┬────┘    └─────┬────┘
      │               │
      └───────┬───────┘
              ↓
        ┌──────────┐
        │  Node 3  │  seeds=[Node1, Node2]
        └──────────┘
        
时刻 T3: 节点 1 故障
              ✗ Node 1 (Dead)
                    
        ┌──────────┐    ┌──────────┐
        │  Node 2  │────│  Node 3  │
        └──────────┘    └──────────┘
        
        节点 2 和 3 继续正常工作！
```

### 为什么可以有多个种子节点？

```bash
# 配置多个种子节点（高可用）
CALM_SEED_NODES="node1:7946,node2:7946,node3:7946"

# 新节点会尝试连接任意一个种子
# 只要有一个种子节点可用即可加入集群
```

---

## 💓 心跳和故障检测机制

### 心跳机制

每个节点都运行一个心跳任务：

```rust
// 每 5 秒更新一次自己的心跳时间戳
loop {
    sleep(5 seconds)
    my_node.last_heartbeat = current_timestamp()
}
```

### 故障检测

每个节点也运行一个故障检测任务：

```rust
// 每 5 秒检查一次所有节点
loop {
    sleep(5 seconds)
    
    for node in all_nodes {
        elapsed = now - node.last_heartbeat
        
        if elapsed > 30 seconds {
            mark_as_dead(node)
            trigger_failover(node)
        }
    }
}
```

### 时序图

```
时间 ──────────────────────────────────────────>
      0s    5s   10s   15s   20s   25s   30s   35s

Node1 ❤️────❤️────❤️────❤️────❤️────❤️────❤️────❤️
      
Node2 ❤️────❤️────❤️────✗ (crash)
                        │
                        └──> 30s 后被标记为 Dead
                                    ↓
                              触发故障转移
                              分区重新分配
```

### 配置参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `heartbeat_interval_secs` | 5 | 心跳更新间隔 |
| `failure_timeout_secs` | 30 | 故障判定超时 |

**调整建议**：
- 低延迟网络：`heartbeat=3s, timeout=15s`
- 高延迟网络：`heartbeat=10s, timeout=60s`
- 生产环境：`heartbeat=5s, timeout=30s`（推荐）

---

## 🔄 故障转移流程

### 完整流程

```
1. 节点 2 心跳超时
   ↓
2. 标记 Node2 为 Dead
   ↓
3. 触发故障回调
   ↓
4. PartitionManager 收到通知
   ↓
5. 查找 Node2 负责的所有分区
   ↓
6. 将这些分区重新分配给活跃节点
   ↓
7. 更新 Gossip 键值存储
   ↓
8. 写入请求自动路由到新 Owner
```

### 代码示例

```rust
// 注册故障回调
cluster.watch_failures(move |failed_node| {
    println!("节点 {} 故障", failed_node);
    
    // 自动触发分区转移
    partition_manager
        .handle_node_failure(&failed_node)
        .await
        .unwrap();
}).await;
```

---

## 🆚 与其他架构对比

### 1. Master-Slave 架构（如 MySQL）

```
Master 故障 → 需要手动/自动选举新 Master → 服务中断
```

**问题**：单点故障、选举复杂

### 2. Raft/Paxos 共识算法（如 etcd）

```
Leader 故障 → Raft 选举新 Leader → 服务短暂中断
```

**问题**：选举有开销、需要 quorum

### 3. Gossip + Partition Owner（CalmCore）

```
Owner 故障 → 直接转移分区到其他节点 → 秒级恢复
```

**优点**：
- ✅ 无 Master，无单点故障
- ✅ 无需共识算法，简单高效
- ✅ 故障转移快速（秒级）
- ✅ 易于扩展

---

## 📊 真实场景模拟

### 场景 1：正常启动 3 节点集群

```bash
# T0: 启动节点 1
$ CALM_NODE_ID=node1 CALM_SEED_NODES="" ./start.sh
  → [Cluster] Node1 started
  → [Partition] Initialized 100 partitions: P0-P99 → Node1

# T1: 启动节点 2
$ CALM_NODE_ID=node2 CALM_SEED_NODES="node1:7946" ./start.sh
  → [Cluster] Node2 joined via Node1
  → [Cluster] Discovered 1 nodes: [Node1]

# T2: 重新平衡分区
$ rebalance_partitions("my_table")
  → [Partition] Rebalanced 100 partitions:
      - Node1: P0, P3, P6, ... (33 partitions)
      - Node2: P1, P4, P7, ... (33 partitions)
      - Node3: P2, P5, P8, ... (34 partitions)
```

### 场景 2：节点 2 故障

```bash
# T0: 节点 2 崩溃
$ kill -9 <node2_pid>

# T5: 心跳检测发现异常
[Cluster] Node2 heartbeat timeout (30 seconds)

# T6: 标记为故障
[Cluster] Node2 marked as DEAD

# T7: 自动故障转移
[Partition] Handling failure of Node2
[Partition] Found 33 partitions owned by Node2
[Partition] Reassigning to Node1 and Node3...
[Partition] ✅ Reassigned 33 partitions

# 结果：
- Node1: 50 partitions
- Node3: 50 partitions
- 写入自动路由到新 Owner
- 服务无中断！
```

### 场景 3：节点 2 恢复

```bash
# T0: 重启节点 2
$ CALM_NODE_ID=node2 CALM_SEED_NODES="node1:7946" ./start.sh
  → [Cluster] Node2 rejoined

# T1: 手动重新平衡（可选）
$ rebalance_partitions("my_table")
  → [Partition] Rebalanced 100 partitions evenly across 3 nodes
```

---

## 🎯 最佳实践

### 1. 种子节点配置

```bash
# ❌ 不好：只有一个种子节点
CALM_SEED_NODES="node1:7946"

# ✅ 好：配置多个种子节点
CALM_SEED_NODES="node1:7946,node2:7946,node3:7946"
```

### 2. 心跳参数调优

```rust
// 开发环境：快速故障检测
heartbeat_interval_secs: 2
failure_timeout_secs: 10

// 生产环境：平衡检测速度和误报率
heartbeat_interval_secs: 5
failure_timeout_secs: 30

// 跨机房：容忍高延迟
heartbeat_interval_secs: 10
failure_timeout_secs: 60
```

### 3. 监控和告警

```rust
// 监控节点状态
let live_count = cluster.live_nodes().await.len();
if live_count < 2 {
    alert!("集群节点数量过低: {}", live_count);
}

// 监控分区分布
let partitions = partition_mgr.get_node_partitions(&node_id).await;
if partitions.len() > 50 {
    alert!("节点 {} 负载过高: {} 分区", node_id, partitions.len());
}
```

---

## 🔮 未来增强（Phase 2）

### 真正的 Gossip 协议

当前实现是单机内存模拟，Phase 2 将集成 Chitchat：

- ✅ 真正的节点间通信
- ✅ SWIM 故障检测算法
- ✅ 反熵机制（Anti-Entropy）
- ✅ 网络分区处理

### 增强的故障检测

- **Phi Accrual 故障检测器**：更智能的阈值判断
- **间接心跳**：通过第三方节点验证故障
- **优雅降级**：Suspect → Dead 的状态转换

---

**总结**：种子节点只是引导工具，故障检测靠心跳机制，CalmCore 的去中心化架构让故障转移变得简单高效！
