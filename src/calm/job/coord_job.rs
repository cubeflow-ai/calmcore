use std::{collections::HashMap, sync::Arc};

use async_graphql::Data;
use futures::StreamExt;
use serde::{Deserialize, Serialize};

use crate::{
    calm::{new_data_client, CalmService},
    catalog::Catalog,
    cluster::{keys, ClusterManager},
    engine::Engine,
    utils::error::{CoreError, CoreResult},
};

/// Cluster event types for coordinator
#[derive(Debug, Clone)]
pub enum CoordClusterEvent {
    /// Partition topology has changed
    PartitionChanged {
        key: String,
        value: String,
        node: String,
    },
}

/// 节点状态
#[derive(Debug, Clone)]
struct NodeState {
    node_id: String,
    partitions: Vec<(String, String)>, // (table_name, partition_name)
    last_seen: u64,
}

/// 孤儿分区（owner 节点不存在）
#[derive(Debug, Clone)]
pub struct OrphanPartition {
    pub table_name: String,
    pub partition_name: String,
    pub old_owner: Option<String>,
}

/// 分区冲突（被多个节点同时持有）
#[derive(Debug, Clone)]
pub struct PartitionConflict {
    pub table_name: String,
    pub partition_name: String,
    pub nodes: Vec<String>,
    pub versions: Vec<u64>,
}

/// 健康报告
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthReport {
    pub total_partitions: usize,
    pub healthy: usize,
    pub orphaned: usize,
    pub conflicted: usize,
    pub timestamp: u64,
}

/// 协调节点任务服务
pub struct JobService {
    calm_service: Arc<CalmService>,

    /// 节点状态缓存
    node_states: HashMap<String, NodeState>,
}

impl JobService {
    pub fn new(calm_service: Arc<CalmService>) -> Self {
        Self {
            calm_service,
            node_states: HashMap::new(),
        }
    }

    // ==================== Phase 1: 故障恢复 ====================

    /// 处理节点下线：迁移该节点上的所有分区
    pub async fn handle_node_failure(&mut self, failed_node: &str) -> CoreResult<()> {
        log::warn!("💥 [Coordinator] Detected node failure: {}", failed_node);

        // 1. 找到该节点上的所有分区
        let affected_partitions = self.find_partitions_on_node(failed_node).await?;

        if affected_partitions.is_empty() {
            log::info!(
                "✅ [Coordinator] No partitions on failed node {}",
                failed_node
            );
            return Ok(());
        }

        log::warn!(
            "⚠️  [Coordinator] Found {} partitions on failed node {}",
            affected_partitions.len(),
            failed_node
        );

        // 2. 为每个分区选择新节点并恢复
        for (table, partition) in affected_partitions {
            if let Err(e) = self.recover_orphan_partition(&table, &partition).await {
                log::error!(
                    "❌ [Coordinator] Failed to recover partition {}/{}: {}",
                    table,
                    partition,
                    e
                );
            }
        }

        // 3. 清理节点状态
        self.node_states.remove(failed_node);

        log::info!(
            "✅ [Coordinator] Completed handling failure of node {}",
            failed_node
        );
        Ok(())
    }

    /// 查找某个节点上的所有分区
    async fn find_partitions_on_node(&self, node_id: &str) -> CoreResult<Vec<(String, String)>> {
        let mut result = Vec::new();

        // 遍历所有表
        for table_name in self.calm_service.catalog().list_tables().await {
            let table_info = self
                .calm_service
                .catalog()
                .get_table_info(&table_name)
                .await?;
            let partitions = table_info.partitions.read().await;

            for (partition_name, pm) in partitions.iter() {
                if pm.owner == node_id {
                    result.push((table_name.to_string(), partition_name.clone()));
                }
            }
        }

        Ok(result)
    }

    /// 检测孤儿分区：owner 节点已经不存在的分区
    pub async fn detect_orphan_partitions(&self) -> CoreResult<Vec<OrphanPartition>> {
        let cluster_manager = self.calm_service.cluster_manager().await?;
        let live_nodes = cluster_manager.live_nodes().await;
        let mut orphans = Vec::new();

        log::debug!("🔍 [Coordinator] Detecting orphan partitions...");

        // 遍历所有表的所有分区
        for table_name in self.calm_service.catalog().list_tables().await {
            let table_info = self
                .calm_service
                .catalog()
                .get_table_info(&table_name)
                .await?;
            let partitions = table_info.partitions.read().await;

            for (partition_name, pm) in partitions.iter() {
                // owner 节点不在 live_nodes 中
                if !live_nodes.contains(&pm.owner) {
                    orphans.push(OrphanPartition {
                        table_name: table_name.to_string(),
                        partition_name: partition_name.clone(),
                        old_owner: Some(pm.owner.clone()),
                    });
                }
            }
        }

        if !orphans.is_empty() {
            log::warn!(
                "⚠️  [Coordinator] Found {} orphan partitions",
                orphans.len()
            );
        }

        Ok(orphans)
    }

    /// 恢复孤儿分区：选择新节点，在新节点上创建分区
    pub async fn recover_orphan_partition(&self, table: &str, partition: &str) -> CoreResult<()> {
        log::info!(
            "🔧 [Coordinator] Recovering orphan partition {}/{}",
            table,
            partition
        );

        // 1. 选择新节点
        let new_node = self.select_node_for_partition().await?;

        // 2. 在新节点上创建分区（通过 RPC）
        log::info!(
            "📤 [Coordinator] Creating partition {}/{} on node {}",
            table,
            partition,
            new_node
        );

        let client = new_data_client(&new_node).await?;
        client
            .create_partition(
                tarpc::context::current(),
                table.to_string(),
                partition.to_string(),
            )
            .await
            .map_err(|e| {
                CoreError::ClusterState(format!("Failed to create partition via RPC: {:?}", e))
            })??;

        // 3. 更新 catalog 中的 owner
        self.calm_service
            .catalog()
            .set_partition_owner(table, partition, &new_node)
            .await?;

        log::info!(
            "✅ [Coordinator] Recovered partition {}/{} to node {}",
            table,
            partition,
            new_node
        );

        Ok(())
    }

    /// 检测分区冲突：是否有分区被多个节点同时持有
    pub async fn detect_partition_conflicts(&self) -> CoreResult<Vec<PartitionConflict>> {
        log::debug!("🔍 [Coordinator] Detecting partition conflicts...");

        // 从 gossip 中收集各节点声明的分区
        let mut partition_map: HashMap<(String, String), Vec<(String, u64)>> = HashMap::new();

        let cluster_manager = self.calm_service.cluster_manager().await?;
        let chitchat = cluster_manager.chitchat.lock().await;
        for node in chitchat.live_nodes() {
            if let Some(node_state) = chitchat.node_state(node) {
                for (key, value_str) in node_state.key_values() {
                    if key.starts_with(keys::PARTITION_PREFIX) {
                        if let Ok((table, partition, version)) = keys::parse_partition_key(key) {
                            partition_map
                                .entry((table, partition))
                                .or_default()
                                .push((node.node_id.clone(), version));
                        }
                    }
                }
            }
        }
        drop(chitchat);

        // 找出有冲突的分区（被多个节点持有）
        let mut conflicts = Vec::new();
        for ((table, partition), node_versions) in partition_map {
            if node_versions.len() > 1 {
                let (nodes, versions): (Vec<_>, Vec<_>) = node_versions.into_iter().unzip();
                conflicts.push(PartitionConflict {
                    table_name: table,
                    partition_name: partition,
                    nodes,
                    versions,
                });
            }
        }

        if !conflicts.is_empty() {
            log::warn!(
                "⚠️  [Coordinator] Found {} partition conflicts",
                conflicts.len()
            );
        }

        Ok(conflicts)
    }

    // ==================== Phase 2: 健康监控 ====================

    /// 检查分区健康状态
    pub async fn check_partition_health(&self) -> CoreResult<HealthReport> {
        let orphans = self.detect_orphan_partitions().await?;
        let conflicts = self.detect_partition_conflicts().await?;

        // 统计总分区数
        let mut total = 0;
        for table_name in self.calm_service.catalog().list_tables().await {
            let table_info = self
                .calm_service
                .catalog()
                .get_table_info(&table_name)
                .await?;
            total += table_info.partitions.read().await.len();
        }

        let healthy = total - orphans.len() - conflicts.len();

        let report = HealthReport {
            total_partitions: total,
            healthy,
            orphaned: orphans.len(),
            conflicted: conflicts.len(),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        };

        log::info!(
            "💓 [Coordinator] Health: {}/{} healthy, {} orphaned, {} conflicted",
            report.healthy,
            report.total_partitions,
            report.orphaned,
            report.conflicted
        );

        Ok(report)
    }

    // ==================== 辅助方法 ====================

    /// 选择一个节点来放置分区（负载均衡策略）
    async fn select_node_for_partition(&self) -> CoreResult<String> {
        let cluster_manager = self.calm_service.cluster_manager().await?;
        let available_nodes = cluster_manager.idle_nodes().await;

        if available_nodes.is_empty() {
            return Err(CoreError::ClusterState("No available nodes".to_string()));
        }

        // 如果只有一个节点，直接返回
        if available_nodes.len() == 1 {
            return Ok(available_nodes[0].clone());
        }

        log::debug!(
            "📊 Selecting node from {} available nodes",
            available_nodes.len()
        );

        // 获取每个节点的状态信息
        let mut node_states: Vec<(String, crate::calm::NodeInfo)> = Vec::new();

        for node_id in available_nodes {
            match new_data_client(&node_id).await {
                Ok(client) => match client.node_info(tarpc::context::current()).await {
                    Ok(Ok(node_info)) => {
                        log::debug!(
                            "📊 Node '{}': {} partitions, CPU: {:.1}%, Memory: {:.1}%, Load: {:.2}",
                            node_id,
                            node_info.partition_count,
                            node_info.cpu_usage,
                            node_info.memory_usage,
                            node_info.load_avg_1min
                        );
                        node_states.push((node_id.clone(), node_info));
                    }
                    Ok(Err(e)) => {
                        log::warn!(
                            "⚠️  Failed to get node info from '{}': {}, skipping",
                            node_id,
                            e
                        );
                    }
                    Err(e) => {
                        log::warn!("⚠️  RPC error from node '{}': {}, skipping", node_id, e);
                    }
                },
                Err(e) => {
                    log::warn!("⚠️  Cannot connect to node '{}': {}, skipping", node_id, e);
                }
            }
        }

        if node_states.is_empty() {
            return Err(CoreError::ClusterState(
                "No connectable nodes available".to_string(),
            ));
        }

        // 按照负载排序：综合评分 = partition数量*0.4 + CPU*0.3 + 内存*0.2 + 系统负载*10*0.1
        node_states.sort_by(|(_, a), (_, b)| {
            let score_a = a.partition_count as f32 * 0.4
                + a.cpu_usage * 0.3
                + a.memory_usage * 0.2
                + a.load_avg_1min * 10.0 * 0.1;
            let score_b = b.partition_count as f32 * 0.4
                + b.cpu_usage * 0.3
                + b.memory_usage * 0.2
                + b.load_avg_1min * 10.0 * 0.1;
            score_a
                .partial_cmp(&score_b)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        // 选择负载最低的节点
        let (selected_node, node_info) = &node_states[0];
        log::info!(
            "✅ Selected node '{}' with {} partitions, CPU: {:.1}%, Memory: {:.1}%, Load: {:.2}",
            selected_node,
            node_info.partition_count,
            node_info.cpu_usage,
            node_info.memory_usage,
            node_info.load_avg_1min
        );

        Ok(selected_node.clone())
    }

    /// 更新节点状态缓存
    fn update_node_state(&mut self, node_id: String, partitions: Vec<(String, String)>) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        self.node_states.insert(
            node_id.clone(),
            NodeState {
                node_id,
                partitions,
                last_seen: now,
            },
        );
    }
}

// ==================== 任务调度器 ====================
// ==================== 任务调度器 ====================

/// 启动协调节点后台任务
pub async fn start_coord_job(calm_service: Arc<CalmService>) -> CoreResult<()> {
    let cm = calm_service
        .cluster_manager
        .as_ref()
        .ok_or_else(|| CoreError::ClusterState("Cluster manager not available".to_string()))?;

    let mut job_service = JobService::new(calm_service.clone());

    let mut live_nodes_stream = cm.chitchat.lock().await.live_nodes_watch_stream();
    let mut last_live_nodes = cm.live_nodes().await;

    // 创建事件通道用于接收 gossip 事件
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

    // 订阅分区变更事件
    {
        let chitchat = cm.chitchat.lock().await;
        chitchat
            .subscribe_event(crate::cluster::keys::PARTITION_PREFIX, move |event| {
                // 同步回调，必须快速执行
                log::info!(
                    "🔔 [Coordinator-Callback] Gossip event received: key='{}', value='{}', node='{}'",
                    event.key,
                    event.value,
                    event.node.node_id
                );
                let _ = tx.send(CoordClusterEvent::PartitionChanged {
                    key: event.key.to_string(),
                    value: event.value.to_string(),
                    node: event.node.node_id.clone(),
                });
            })
            .forever(); // 保持订阅
    }

    log::info!("🚀 [Coordinator] Starting coordinator job...");

    loop {
        // 检查是否还是协调节点
        if !cm.am_i_coord_node() {
            log::warn!("⚠️  No longer the coord node, exiting coordinator job.");
            return Ok(());
        }

        tokio::select! {
            // 处理集群事件（分区变更）
            Some(event) = rx.recv() => {
                log::info!(
                    "📨 [Coordinator] Received event from channel: {:?}",
                    event
                );
                match event {
                    CoordClusterEvent::PartitionChanged { key, value, node } => {
                        if let Err(e) = crate::calm::job::handle_partition_changed(
                            &job_service.calm_service.catalog(),
                            &key,
                            &value,
                            &node,
                            "Coordinator"
                        ).await {
                            log::error!("❌ [Coordinator] Failed to handle partition change: {}", e);
                        }
                    }
                }
            }

            // 监听节点成员变化
            Some(live_nodes_map) = live_nodes_stream.next() => {
                // 从 (&ChitchatId, &NodeState) 提取 node_id
                let live_nodes: Vec<String> = live_nodes_map
                    .iter()
                    .map(|(id, _)| id.node_id.clone())
                    .collect();

                log::info!("👥 [Coordinator] Live nodes changed: {} nodes", live_nodes.len());

                // 检查是否应该继续作为协调节点
                // 在存活节点中找到最小的节点ID，如果不是自己则退出
                let my_id = cm.node_id();
                if let Some(min_node) = live_nodes.iter().min() {
                    if min_node.as_str() < my_id {
                        log::warn!(
                            "⚠️  [Coordinator] Found smaller node {} (I am {}), stepping down...",
                            min_node,
                            my_id
                        );
                        // 设置新的协调节点为发现的最小节点
                        if let Err(e) = cm.set_coord_node(min_node).await {
                            log::error!("❌ [Coordinator] Failed to set new coord node: {}", e);
                        }
                        return Ok(());
                    }
                }

                // 检测下线节点
                for old_node in &last_live_nodes {
                    if !live_nodes.contains(old_node) {
                        log::warn!("💥 [Coordinator] Node {} went offline", old_node);
                        if let Err(e) = job_service.handle_node_failure(old_node).await {
                            log::error!("❌ [Coordinator] Failed to handle node failure: {}", e);
                        }
                    }
                }

                // 检测新上线节点
                for new_node in &live_nodes {
                    if !last_live_nodes.contains(new_node) {
                        log::info!("✨ [Coordinator] Node {} came online", new_node);
                    }
                }

                last_live_nodes = live_nodes;
            }

            // 定期健康检查和维护任务
            _ = tokio::time::sleep(tokio::time::Duration::from_secs(10)) => {
                log::debug!("⏰ [Coordinator] Running periodic maintenance...");

                // 1. 检测并恢复孤儿分区
                match job_service.detect_orphan_partitions().await {
                    Ok(orphans) => {
                        if !orphans.is_empty() {
                            log::warn!("🔧 [Coordinator] Found {} orphan partitions, recovering...", orphans.len());
                            for orphan in orphans {
                                if let Err(e) = job_service
                                    .recover_orphan_partition(&orphan.table_name, &orphan.partition_name)
                                    .await
                                {
                                    log::error!(
                                        "❌ [Coordinator] Failed to recover {}/{}: {}",
                                        orphan.table_name,
                                        orphan.partition_name,
                                        e
                                    );
                                }
                            }
                        }
                    }
                    Err(e) => {
                        log::error!("❌ [Coordinator] Failed to detect orphan partitions: {}", e);
                    }
                }

                // 2. 检测分区冲突
                match job_service.detect_partition_conflicts().await {
                    Ok(conflicts) => {
                        if !conflicts.is_empty() {
                            log::error!("🔥 [Coordinator] Detected {} partition conflicts!", conflicts.len());
                            for conflict in conflicts {
                                log::error!(
                                    "⚠️  Conflict: {}/{} held by nodes: {:?} (versions: {:?})",
                                    conflict.table_name,
                                    conflict.partition_name,
                                    conflict.nodes,
                                    conflict.versions
                                );
                                // TODO: 实现冲突解决策略（保留版本号最大的）
                            }
                        }
                    }
                    Err(e) => {
                        log::error!("❌ [Coordinator] Failed to detect conflicts: {}", e);
                    }
                }

                // 3. 健康检查
                if let Err(e) = job_service.check_partition_health().await {
                    log::error!("❌ [Coordinator] Health check failed: {}", e);
                }
            }
        }
    }
}
