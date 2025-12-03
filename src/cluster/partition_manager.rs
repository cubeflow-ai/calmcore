//! Partition 管理器
//!
//! 负责：
//! - 维护 Partition -> Owner 映射
//! - 自动故障转移
//! - 负载均衡

use std::collections::HashMap;
use std::sync::Arc;

use super::gossip::ClusterManager;
use super::node::NodeId;
use crate::utils::error::{CoreError, CoreResult};

/// Partition 管理器
pub struct PartitionManager {
    cluster: Arc<ClusterManager>,
}

impl PartitionManager {
    pub fn new(cluster: Arc<ClusterManager>) -> Self {
        Self { cluster }
    }

    /// 初始化表的分区所有权
    ///
    /// # 参数
    /// - `table_name`: 表名
    /// - `num_partitions`: 分区数量
    pub async fn initialize_partitions(
        &self,
        table_name: &str,
        num_partitions: u32,
    ) -> CoreResult<()> {
        log::info!(
            "🎯 [Partition] Initializing {} partitions for table '{}'",
            num_partitions,
            table_name
        );

        let nodes = self.cluster.live_nodes().await;
        if nodes.is_empty() {
            return Err(CoreError::Internal(
                "No live nodes available for partition assignment".to_string(),
            ));
        }

        // 轮询分配分区给节点
        for partition_id in 0..num_partitions {
            let owner_idx = (partition_id as usize) % nodes.len();
            let owner_node = &nodes[owner_idx];

            // 存储到 Gossip 共享状态
            let key = self.partition_key(table_name, partition_id);
            self.cluster
                .set_key_value(key, owner_node.id.clone())
                .await?;

            log::debug!("  Partition {} -> Node {}", partition_id, owner_node.id);
        }

        log::info!(
            "✅ [Partition] Initialized {} partitions across {} nodes",
            num_partitions,
            nodes.len()
        );

        Ok(())
    }

    /// 获取分区的 Owner 节点
    pub async fn get_partition_owner(
        &self,
        table_name: &str,
        partition_id: u32,
    ) -> CoreResult<NodeId> {
        let key = self.partition_key(table_name, partition_id);
        self.cluster.get_key_value(&key).await.ok_or_else(|| {
            CoreError::Internal(format!(
                "Partition owner not found: {} (partition {})",
                table_name, partition_id
            ))
        })
    }

    /// 设置分区的 Owner 节点（用于故障转移）
    pub async fn set_partition_owner(
        &self,
        table_name: &str,
        partition_id: u32,
        owner_node: &str,
    ) -> CoreResult<()> {
        let key = self.partition_key(table_name, partition_id);
        self.cluster
            .set_key_value(key, owner_node.to_string())
            .await?;

        log::info!(
            "🔄 [Partition] Reassigned partition {} of '{}' to node '{}'",
            partition_id,
            table_name,
            owner_node
        );

        Ok(())
    }

    /// 获取某个节点负责的所有分区
    pub async fn get_node_partitions(&self, node_id: &str) -> HashMap<String, Vec<u32>> {
        let all_keys = self.cluster.get_all_keys().await;
        let mut result: HashMap<String, Vec<u32>> = HashMap::new();

        for (key, owner) in all_keys.iter() {
            if owner == node_id && key.starts_with("partition_owner:") {
                // 解析 key: partition_owner:table_name:partition_id
                let parts: Vec<&str> = key.split(':').collect();
                if parts.len() == 3 {
                    let table_name = parts[1].to_string();
                    if let Ok(partition_id) = parts[2].parse::<u32>() {
                        result
                            .entry(table_name)
                            .or_insert_with(Vec::new)
                            .push(partition_id);
                    }
                }
            }
        }

        result
    }

    /// 处理节点故障，自动转移分区
    ///
    /// 协调机制：
    /// 1. 使用确定性算法选出一个 Coordinator 节点
    /// 2. 只有 Coordinator 执行实际的分区转移
    /// 3. 其他节点跳过，避免冲突
    pub async fn handle_node_failure(&self, failed_node: &str) -> CoreResult<()> {
        log::warn!("⚠️  [Partition] Detected failure of node '{}'", failed_node);

        // 1. 选举 Coordinator（确定性选择：按字典序最小的活跃节点）
        let coordinator = self.elect_coordinator_for_failover(failed_node).await?;
        let my_node_id = self.cluster.node_id();

        if coordinator != my_node_id {
            log::info!(
                "📋 [Partition] Node '{}' is coordinator, I'll skip (I'm '{}')",
                coordinator,
                my_node_id
            );
            return Ok(());
        }

        log::info!("👑 [Partition] I am the coordinator, handling failover...");

        // 2. 找出该节点负责的所有分区
        let partitions = self.get_node_partitions(failed_node).await;
        if partitions.is_empty() {
            log::info!("  Node '{}' had no partitions", failed_node);
            return Ok(());
        }

        // 3. 获取活跃节点（排除故障节点）
        let alive_nodes: Vec<_> = self
            .cluster
            .live_nodes()
            .await
            .into_iter()
            .filter(|n| n.id != failed_node)
            .collect();

        if alive_nodes.is_empty() {
            return Err(CoreError::Internal(
                "No alive nodes for partition reassignment".to_string(),
            ));
        }

        // 4. 使用一致性哈希分配分区（避免热点）
        let mut reassigned_count = 0;
        for (table_name, partition_ids) in partitions {
            for partition_id in partition_ids {
                // 使用哈希选择新 Owner（确定性 + 负载均衡）
                let new_owner = self
                    .select_new_owner(&table_name, partition_id, &alive_nodes)
                    .await;

                // CAS 操作：只有当前值还是 failed_node 时才更新
                let success = self
                    .cas_partition_owner(&table_name, partition_id, failed_node, &new_owner.id)
                    .await?;

                if success {
                    log::info!(
                        "  ✅ Partition {}:{} → {}",
                        table_name,
                        partition_id,
                        new_owner.id
                    );
                    reassigned_count += 1;
                } else {
                    log::warn!(
                        "  ⚠️  Partition {}:{} already reassigned (race condition avoided)",
                        table_name,
                        partition_id
                    );
                }
            }
        }

        log::info!(
            "✅ [Partition] Coordinator '{}' reassigned {} partitions from '{}' to {} alive nodes",
            my_node_id,
            reassigned_count,
            failed_node,
            alive_nodes.len()
        );

        Ok(())
    }

    /// 启动自动故障转移监听
    pub async fn start_auto_failover(&self) {
        let partition_manager = Arc::new(PartitionManager {
            cluster: self.cluster.clone(),
        });

        self.cluster
            .watch_failures(move |failed_node| {
                let pm = partition_manager.clone();
                tokio::spawn(async move {
                    if let Err(e) = pm.handle_node_failure(&failed_node).await {
                        log::error!("Failed to handle node failure: {}", e);
                    }
                });
            })
            .await;

        log::info!("✅ [Partition] Auto-failover enabled");
    }

    /// 生成分区键
    fn partition_key(&self, table_name: &str, partition_id: u32) -> String {
        format!("partition_owner:{}:{}", table_name, partition_id)
    }

    /// 选举 Coordinator 节点（用于故障转移）
    ///
    /// 策略：确定性选择 - 对所有活跃节点 + 故障节点 ID 排序，选第一个
    /// 这样所有节点都会选出同一个 Coordinator，避免冲突
    async fn elect_coordinator_for_failover(&self, failed_node: &str) -> CoreResult<String> {
        let mut alive_nodes = self.cluster.live_nodes().await;

        if alive_nodes.is_empty() {
            return Err(CoreError::Internal("No alive nodes".to_string()));
        }

        // 排序（字典序）
        alive_nodes.sort_by(|a, b| a.id.cmp(&b.id));

        // 使用故障节点 ID 作为种子，选择一个确定的 Coordinator
        // 这样即使不同时刻检测到故障，也会选出同一个节点
        let hash = self.hash_string(&format!("failover:{}", failed_node));
        let idx = (hash as usize) % alive_nodes.len();

        Ok(alive_nodes[idx].id.clone())
    }

    /// 为分区选择新的 Owner（负载均衡）
    async fn select_new_owner(
        &self,
        table_name: &str,
        partition_id: u32,
        alive_nodes: &[crate::cluster::NodeInfo],
    ) -> crate::cluster::NodeInfo {
        // 使用一致性哈希：table + partition 作为 key
        let key = format!("{}:{}", table_name, partition_id);
        let hash = self.hash_string(&key);
        let idx = (hash as usize) % alive_nodes.len();

        alive_nodes[idx].clone()
    }

    /// 简单的字符串哈希函数
    fn hash_string(&self, s: &str) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        s.hash(&mut hasher);
        hasher.finish()
    }

    /// CAS (Compare-And-Swap) 更新分区 Owner
    ///
    /// 只有当前值等于 expected 时才更新为 new_value
    /// 返回是否成功更新
    async fn cas_partition_owner(
        &self,
        table_name: &str,
        partition_id: u32,
        expected_owner: &str,
        new_owner: &str,
    ) -> CoreResult<bool> {
        let key = self.partition_key(table_name, partition_id);

        // 获取当前值
        let current = self.cluster.get_key_value(&key).await;

        // CAS 检查
        if let Some(current_owner) = current {
            if current_owner == expected_owner {
                // 值匹配，执行更新
                self.cluster
                    .set_key_value(key, new_owner.to_string())
                    .await?;
                return Ok(true);
            } else {
                // 值已被其他节点修改
                return Ok(false);
            }
        }

        // 键不存在，直接设置
        self.cluster
            .set_key_value(key, new_owner.to_string())
            .await?;
        Ok(true)
    }

    /// 重新平衡分区（手动触发）
    pub async fn rebalance_partitions(&self, table_name: &str) -> CoreResult<()> {
        log::info!("🔄 [Partition] Rebalancing partitions for '{}'", table_name);

        // 获取当前所有分区
        let all_keys = self.cluster.get_all_keys().await;
        let mut partitions = Vec::new();

        for key in all_keys.keys() {
            if key.starts_with(&format!("partition_owner:{}:", table_name)) {
                let parts: Vec<&str> = key.split(':').collect();
                if parts.len() == 3 {
                    if let Ok(partition_id) = parts[2].parse::<u32>() {
                        partitions.push(partition_id);
                    }
                }
            }
        }

        if partitions.is_empty() {
            return Ok(());
        }

        // 获取活跃节点
        let nodes = self.cluster.live_nodes().await;
        if nodes.is_empty() {
            return Err(CoreError::Internal("No live nodes".to_string()));
        }

        // 重新分配
        for (idx, partition_id) in partitions.iter().enumerate() {
            let owner_idx = idx % nodes.len();
            let owner = &nodes[owner_idx];
            self.set_partition_owner(table_name, *partition_id, &owner.id)
                .await?;
        }

        log::info!(
            "✅ [Partition] Rebalanced {} partitions across {} nodes",
            partitions.len(),
            nodes.len()
        );

        Ok(())
    }
}
