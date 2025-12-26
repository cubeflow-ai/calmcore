use std::sync::Arc;

use crate::{catalog::Catalog, cluster::ClusterManager, utils::error::CoreResult};

/// Cluster event types for subscribers
#[derive(Debug, Clone)]
pub enum ClusterEvent {
    /// Partition topology has changed
    PartitionChanged {
        key: String,
        value: String,
        node: String,
    },

    CoordNodeChanged {
        value: String,
        node: String,
    },
}

/// 启动 datanode 节点定期任务
///
/// 当节点不是协调节点时运行，定期检查自己需要加载的分区
pub async fn start_node_job(catalog: Arc<Catalog>, cm: Arc<ClusterManager>) -> CoreResult<()> {
    log::info!("🚀 [DataNode] Starting node job...");

    let mut check_counter = 0;

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

    {
        let chitchat = cm.chitchat.lock().await;
        let tx1 = tx.clone();
        chitchat
            .subscribe_event(crate::cluster::keys::PARTITION_PREFIX, move |event| {
                // 同步回调，必须快速执行
                let _ = tx1.send(ClusterEvent::PartitionChanged {
                    key: event.key.to_string(),
                    value: event.value.to_string(),
                    node: event.node.node_id.clone(),
                });
            })
            .forever(); // 保持订阅
    }

    {
        let chitchat = cm.chitchat.lock().await;
        chitchat
            .subscribe_event(crate::cluster::keys::CENTER_NODE_KEY, move |event| {
                // 同步回调，必须快速执行
                let _ = tx.send(ClusterEvent::CoordNodeChanged {
                    value: event.value.to_string(),
                    node: event.node.node_id.clone(),
                });
            })
            .forever(); // 保持订阅
    }

    loop {
        // 如果变成了协调节点，退出 node job，切换到 coordinator job
        if cm.am_i_coord_node() {
            log::warn!("⚠️  [DataNode] Became coord node, exiting node job.");
            return Ok(());
        }

        tokio::select! {
            // 处理集群事件
            Some(event) = rx.recv() => {
                match event {
                    ClusterEvent::PartitionChanged { key, value, node } => {
                        if let Err(e) = crate::calm::job::handle_partition_changed(
                            &catalog,
                            &key,
                            &value,
                            &node,
                            "DataNode"
                        ).await {
                            log::error!("❌ [DataNode] Failed to handle partition change: {}", e);
                        }
                    }
                    ClusterEvent::CoordNodeChanged { value, node } => {
                        log::info!(
                            "🎯 [DataNode] Coordinator changed: value={}, node={}",
                            value, node
                        );
                        // 检查自己是否变成了协调节点
                        if cm.am_i_coord_node() {
                            log::info!("🎯 [DataNode] I became the coordinator!");
                            return Ok(());
                        }

                        // 检查别人选出来的中央结点是否有效活着，如果活着和自己的中央结点对比，哪个更小，如果比自己的更小，则发起重新选举
                        let live_nodes = cm.live_nodes().await;
                        if live_nodes.contains(&value) {
                            // 新协调节点还活着，检查是否应该重新选举
                            if let Some(my_coord) = cm.get_coord().await {
                                if value < my_coord {
                                    log::warn!(
                                        "⚠️  [DataNode] Detected smaller coordinator {} (mine: {}), triggering re-election...",
                                        value, my_coord
                                    );

                                    // 重新选举
                                    if let Ok(new_coord) = cm.find_coord_node().await {
                                        if let Err(e) = cm.set_coord_node(&new_coord).await {
                                            log::error!("❌ [DataNode] Failed to set new coordinator: {}", e);
                                        } else {
                                            log::info!("✅ [DataNode] Re-elected coordinator: {}", new_coord);
                                        }
                                    }
                                }
                            }
                        } else {
                            log::warn!("⚠️  [DataNode] New coordinator {} is not in live nodes, ignoring...", value);
                        }
                    }
                }
            }

            // 定期检查协调节点状态（每10秒）
            _ = tokio::time::sleep(tokio::time::Duration::from_secs(10)) => {
                check_counter += 1;

                // 检查协调节点是否还活着，检查自己的中央结点是不是集群中最小的节点，如果不是，则发起重新选举
                if let Some(coord_id) = cm.get_coord().await {
                    let live_nodes = cm.live_nodes().await;

                    // 1. 检查协调节点是否还活着
                    if !live_nodes.contains(&coord_id) {
                        log::warn!(
                            "⚠️  [DataNode] Coordinator {} is down, triggering re-election...",
                            coord_id
                        );

                        // 重新选举
                        match cm.find_coord_node().await {
                            Ok(new_coord) => {
                                if let Err(e) = cm.set_coord_node(&new_coord).await {
                                    log::error!("❌ [DataNode] Failed to set new coordinator: {}", e);
                                } else {
                                    log::info!("✅ [DataNode] New coordinator elected: {}", new_coord);

                                    // 如果自己被选为协调节点，退出
                                    if cm.am_i_coord_node() {
                                        log::info!("🎯 [DataNode] I was elected as coordinator!");
                                        return Ok(());
                                    }
                                }
                            }
                            Err(e) => {
                                log::error!("❌ [DataNode] Failed to find new coordinator: {}", e);
                            }
                        }
                    } else {
                        // 2. 检查当前协调节点是不是集群中最小的节点
                        if let Some(min_node) = live_nodes.iter().min() {
                            if min_node != &coord_id {
                                log::warn!(
                                    "⚠️  [DataNode] Current coordinator {} is not the smallest (smallest: {}), triggering re-election...",
                                    coord_id, min_node
                                );

                                // 重新选举
                                match cm.find_coord_node().await {
                                    Ok(new_coord) => {
                                        if let Err(e) = cm.set_coord_node(&new_coord).await {
                                            log::error!("❌ [DataNode] Failed to set new coordinator: {}", e);
                                        } else {
                                            log::info!("✅ [DataNode] Re-elected coordinator: {}", new_coord);

                                            // 如果自己被选为协调节点，退出
                                            if cm.am_i_coord_node() {
                                                log::info!("🎯 [DataNode] I was elected as coordinator!");
                                                return Ok(());
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        log::error!("❌ [DataNode] Failed to find new coordinator: {}", e);
                                    }
                                }
                            } else if check_counter % 6 == 0 {
                                // 每60秒打印一次心跳日志
                                log::debug!("⏳ [DataNode] Heartbeat - coordinator: {}", coord_id);
                            }
                        }
                    }
                } else {
                    log::warn!("⚠️  [DataNode] No coordinator found, triggering election...");
                    // 没有协调节点，触发选举
                    if let Ok(new_coord) = cm.find_coord_node().await {
                        let _ = cm.set_coord_node(&new_coord).await;
                        log::info!("✅ [DataNode] Coordinator elected: {}", new_coord);
                    }
                }
            }
        }
    }
}
