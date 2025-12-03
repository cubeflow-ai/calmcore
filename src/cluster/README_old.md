# CalmCore Cluster Module - Phase 1

## 📋 概述

集群模块负责管理 CalmCore 的分布式功能，当前实现 Phase 1 的基础功能。

## 🎯 Phase 1 功能

### 已实现

- ✅ **节点管理** (`node.rs`)
  - 节点信息定义（ID、地址、状态）
  - 节点状态：Alive、Suspect、Dead
  
- ✅ **集群管理器** (`gossip.rs`)
  - 简化的内存实现（PoC）
  - 节点注册和发现
  - 键值存储（用于 Partition Owner 映射）
  - 故障回调机制

- ✅ **分区管理器** (`partition_manager.rs`)
  - 分区所有权初始化
  - 获取/设置 Partition Owner
  - 自动故障转移
  - 负载重平衡

### 配置

通过环境变量配置：

```bash
CALM_NODE_ID="node-1"              # 节点 ID
CALM_CLUSTER_ID="calm-cluster"     # 集群 ID
CALM_GOSSIP_ADDR="0.0.0.0:7946"    # Gossip 监听地址
CALM_SEED_NODES="host1:7946,host2:7946"  # 种子节点列表
```

## 🚀 快速开始

### 1. 运行测试示例

```bash
# 启动 3 节点集群测试
./test_cluster.sh
```

### 2. 手动启动单个节点

```bash
# 节点 1（种子节点）
CALM_NODE_ID="node-1" \
CALM_GOSSIP_ADDR="127.0.0.1:7946" \
cargo run --example cluster_test

# 节点 2（加入集群）
CALM_NODE_ID="node-2" \
CALM_GOSSIP_ADDR="127.0.0.1:7947" \
CALM_SEED_NODES="127.0.0.1:7946" \
cargo run --example cluster_test
```

## 📊 架构说明

```
┌────────────────────────────────────────┐
│     ClusterManager (gossip.rs)        │
│  - 节点发现                            │
│  - 健康检查                            │
│  - 键值存储                            │
└──────────────┬─────────────────────────┘
               │
               ↓
┌────────────────────────────────────────┐
│  PartitionManager (partition_manager)  │
│  - Partition Owner 映射                │
│  - 故障转移                            │
│  - 负载均衡                            │
└────────────────────────────────────────┘
```

### 种子节点（Seed Nodes）

**重要**：种子节点不是特殊节点，只是新节点启动时的引导入口。

- 种子节点 = 已知的、稳定的、第一批启动的节点
- 新节点通过种子节点发现集群中的其他节点
- 种子节点故障后，已加入的节点仍可正常通信
- 可以有多个种子节点（高可用）

示例：
```bash
# 节点 1 启动（种子节点，CALM_SEED_NODES 为空）
CALM_SEED_NODES="" cargo run --example cluster_test

# 节点 2 启动（通过节点 1 加入）
CALM_SEED_NODES="127.0.0.1:7946" cargo run --example cluster_test

# 节点 3 启动（可以通过节点 1 或节点 2 加入）
CALM_SEED_NODES="127.0.0.1:7946,127.0.0.1:7947" cargo run --example cluster_test
```

### 故障检测机制

**心跳检测**：
- 每个节点定期更新自己的心跳时间戳（默认 5 秒）
- 其他节点通过检查心跳判断节点是否存活

**故障判定**：
- 如果节点心跳超时（默认 30 秒），标记为 Dead
- 触发故障回调，自动转移该节点的分区

**配置参数**：
```rust
heartbeat_interval_secs: 5   // 心跳间隔
failure_timeout_secs: 30      // 故障超时
```

### Partition Owner 机制

- 每个分区在任意时刻只有一个 Owner 节点负责写入
- Owner 信息存储在集群共享状态中
- 节点故障时自动转移分区所有权

## ⚠️ 当前限制

### Phase 1 的简化实现

- 🔴 **单机模拟**：当前使用内存 HashMap，不是真正的 Gossip 协议
- 🔴 **无网络通信**：节点间无法真正通信
- 🔴 **无持久化**：重启后状态丢失

### 后续 Phase 计划

- **Phase 2**：集成 Chitchat 真正的 Gossip 协议
- **Phase 3**：实现分布式写入路由
- **Phase 4**：实现分布式查询执行

## 🔧 开发指南

### 添加新的集群功能

1. 在相应模块添加方法
2. 使用 `ClusterManager` 的键值存储共享状态
3. 确保异步安全（使用 `Arc<RwLock<>>`）

### 测试

```bash
# 编译检查
cargo check

# 运行测试
cargo test --package calm --lib cluster

# 运行示例
cargo run --example cluster_test
```

## 📝 API 示例

```rust
use calm::cluster::{ClusterManager, PartitionManager, ClusterConfig};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 创建集群管理器
    let cluster = Arc::new(ClusterManager::new(
        "node-1".to_string(),
        "calm-cluster".to_string(),
        "0.0.0.0:7946".to_string(),
        vec![],
    ).await?);

    // 创建分区管理器
    let partition_mgr = PartitionManager::new(cluster.clone());

    // 初始化分区
    partition_mgr.initialize_partitions("my_table", 10).await?;

    // 查询分区 Owner
    let owner = partition_mgr.get_partition_owner("my_table", 0).await?;
    println!("Partition 0 owner: {}", owner);

    Ok(())
}
```

## 🎯 下一步

1. ✅ Phase 1 完成 - 基础架构和 PoC
2. ⏳ Phase 2 - 集成真正的 Gossip 协议（Chitchat）
3. ⏳ Phase 3 - 实现分布式写入路由
4. ⏳ Phase 4 - 实现分布式查询执行

详见：`docs/distributed_architecture_design.md`
