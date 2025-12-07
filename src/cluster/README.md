# Cluster Module - 分布式协调机制

## 核心挑战：节点视图一致性

### 问题描述

在使用一致性哈希进行分区转移时，必须确保所有节点对集群状态的认知一致：

```
场景：node3 故障

Node1 视图: [node1, node2, node3] (3个节点)
Node2 视图: [node1, node2, node3, node4] (4个节点，刚发现 node4)

计算新 Owner:
- Node1: hash(partition) % 3 = 选择 node1
- Node2: hash(partition) % 4 = 选择 node4  ❌ 冲突!
```

### 核心矛盾

- **一致性哈希**需要节点列表完全一致
- **Gossip 协议**是最终一致性，存在短暂不一致窗口
- **节点加入/离开**会改变节点列表

## 解决方案：Epoch + 快照机制

### 1. Epoch（纪元版本号）

为集群状态引入**版本号**，确保所有协调操作基于同一版本：

```rust
pub struct ClusterState {
    epoch: u64,                          // 版本号
    nodes: HashMap<NodeId, NodeInfo>,    // 节点列表
    membership_hash: u64,                // 节点列表的哈希值（快速比较）
}
```

**规则**：
- 节点加入/离开 → epoch + 1
- 故障转移必须基于**稳定的 epoch**
- 如果协调过程中 epoch 变化 → 重试

### 2. 稳定窗口（Stable Window）

只有当集群状态在一段时间内**没有变化**时，才执行故障转移：

```rust
const STABILITY_WINDOW_SECS: u64 = 10;  // 稳定窗口 10 秒

async fn wait_for_stable_cluster(&self) -> CoreResult<u64> {
    let mut last_epoch = self.current_epoch().await;
    let mut stable_since = Instant::now();
    
    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
        
        let current_epoch = self.current_epoch().await;
        if current_epoch != last_epoch {
            // epoch 变化，重置计时器
            last_epoch = current_epoch;
            stable_since = Instant::now();
            continue;
        }
        
        if stable_since.elapsed().as_secs() >= STABILITY_WINDOW_SECS {
            // 稳定了 10 秒，返回稳定的 epoch
            return Ok(current_epoch);
        }
    }
}
```

### 3. Epoch-Based 协调流程

```rust
pub async fn handle_node_failure(&self, failed_node: &str) -> CoreResult<()> {
    // Step 1: 等待集群稳定
    log::info!("⏳ Waiting for cluster to stabilize...");
    let stable_epoch = self.wait_for_stable_cluster().await?;
    log::info!("✅ Cluster stable at epoch {}", stable_epoch);
    
    // Step 2: 获取该 epoch 的节点快照
    let snapshot = self.get_cluster_snapshot(stable_epoch).await?;
    
    // Step 3: 选举 Coordinator（基于快照）
    let coordinator = self.elect_coordinator_with_snapshot(
        failed_node,
        &snapshot,
    ).await?;
    
    if coordinator != self.node_id() {
        log::info!("📋 Node '{}' is coordinator (epoch {})", coordinator, stable_epoch);
        return Ok(());
    }
    
    log::info!("👑 I am coordinator for epoch {}", stable_epoch);
    
    // Step 4: 执行转移（使用快照的节点列表）
    for (table, partition_id) in partitions {
        let new_owner = self.select_new_owner_with_snapshot(
            &table,
            partition_id,
            &snapshot,
        ).await;
        
        // Step 5: CAS 时携带 epoch（防止跨 epoch 冲突）
        let success = self.cas_partition_owner_with_epoch(
            &table,
            partition_id,
            failed_node,
            &new_owner.id,
            stable_epoch,  // 必须基于这个 epoch
        ).await?;
        
        if !success {
            log::warn!("⚠️  CAS failed, epoch may have changed");
            return Err(CoreError::Internal("Epoch changed during failover".into()));
        }
    }
    
    Ok(())
}
```

### 4. 增强的 CAS 操作

```rust
struct PartitionOwnership {
    owner: NodeId,
    epoch: u64,        // 分配时的集群版本
    timestamp: u64,
}

async fn cas_partition_owner_with_epoch(
    &self,
    table: &str,
    partition_id: u32,
    expected_owner: &str,
    new_owner: &str,
    expected_epoch: u64,  // 必须匹配的 epoch
) -> CoreResult<bool> {
    let key = self.partition_key(table, partition_id);
    let current = self.get_partition_ownership(&key).await;
    
    match current {
        Some(ownership) => {
            // 检查 epoch 是否一致
            if ownership.epoch != expected_epoch {
                log::warn!(
                    "⚠️  Epoch mismatch: expected {}, got {}",
                    expected_epoch, ownership.epoch
                );
                return Ok(false);
            }
            
            // 检查 owner 是否匹配
            if ownership.owner == expected_owner {
                // 更新到新 epoch
                let new_ownership = PartitionOwnership {
                    owner: new_owner.to_string(),
                    epoch: expected_epoch,
                    timestamp: current_timestamp(),
                };
                self.set_partition_ownership(key, new_ownership).await?;
                return Ok(true);
            } else {
                return Ok(false);
            }
        }
        None => {
            // 键不存在，创建新的
            let new_ownership = PartitionOwnership {
                owner: new_owner.to_string(),
                epoch: expected_epoch,
                timestamp: current_timestamp(),
            };
            self.set_partition_ownership(key, new_ownership).await?;
            Ok(true)
        }
    }
}
```

## 处理节点加入场景

### 场景 1：协调前节点加入

```
T0: node3 故障
T1: 开始等待稳定窗口
T2: node4 加入 → epoch 变化 → 重置稳定计时器
T3: 再等 10 秒稳定
T4: 基于新的节点列表 [node1,node2,node4] 进行协调
```

✅ **结果**：等待机制确保使用最新的节点列表

### 场景 2：协调中节点加入

```
T0: node3 故障，集群稳定在 epoch=10
T1: Coordinator 开始转移分区
T2: node4 加入 → epoch=11
T3: Coordinator 执行 CAS 时发现 epoch 不匹配
T4: 返回错误，触发重试
T5: 重新等待稳定，基于 epoch=11 再次协调
```

✅ **结果**：CAS 的 epoch 检查防止跨版本冲突

### 场景 3：协调后节点加入

```
T0: 协调完成，分区已重新分配
T1: node4 加入 → epoch=11
T2: 触发 Rebalance（可选，Phase 2 功能）
```

✅ **结果**：协调已完成，新节点加入不影响已有分配

## 实现优先级

### Phase 1（已完成）✅
- ✅ 基础 Coordinator 选举
- ✅ 一致性哈希分配
- ✅ CAS 防冲突
- ✅ Epoch 机制（投票轮次版本号）

### Phase 2（已完成）✅
- ✅ Gossip 真实网络传播（Chitchat 库集成）
- ⏳ Membership 变更事件
- ⏳ 分区 Rebalance

### Phase 3（已完成）✅
- ✅ 读请求路由（QueryRouter）
- ✅ 分布式查询执行（DistributedExecutor + Scatter-Gather）

> **注意**：稳定窗口检测已移除。Epoch + CAS + 投票机制足以保证一致性，无需额外等待。

## 关键设计原则

1. **先稳定，再协调**：只在集群状态稳定时执行故障转移
2. **基于快照**：所有决策基于同一个时间点的集群状态
3. **携带版本**：所有状态变更都携带 epoch，防止跨版本冲突
4. **失败重试**：如果协调过程中状态变化，重新开始
5. **最终一致性**：允许短暂不一致，通过 Gossip 最终收敛

## Trade-offs

| 方案 | 优点 | 缺点 |
|------|------|------|
| **Epoch + 稳定窗口** | 强一致性，无冲突 | 故障转移延迟（+10秒） |
| **乐观 CAS** | 快速响应 | 可能需要多次重试 |
| **Leader 选举** | 简单直接 | 引入 Master，失去去中心化 |

**我们的选择**：Epoch + 稳定窗口
- 理由：10 秒延迟可接受（节点故障本身已经是异常事件）
- 好处：强一致性，避免复杂的冲突解决逻辑

## 总结

通过 **Epoch 版本号 + 稳定窗口 + CAS epoch 检查**，我们实现了：

✅ 所有节点基于相同的集群视图做决策  
✅ 节点加入/离开不会破坏协调过程  
✅ 无需 Master，保持去中心化  
✅ 强一致性保证

这是在 **Gossip 最终一致性** 和 **分布式协调强一致性** 之间的优雅平衡！
