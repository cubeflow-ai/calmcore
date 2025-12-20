use futures::StreamExt;
use std::sync::Arc;
use tokio::sync::mpsc;

use crate::{
    cluster::{keys, ClusterManager},
    service::CalmService,
    utils::error::CoreResult,
};

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

pub async fn start_cluster_job(service: Arc<CalmService>) -> CoreResult<()> {
    let cm = match service.cluster_manager.as_ref() {
        Some(cm) => cm,
        None => {
            log::info!("⚠️  Cluster manager not configured, skipping cluster job.");
            return Ok(());
        }
    };

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
        if cm.am_i_coord_node() {
            start_coord_job(&service, cm, &mut rx).await?;
        } else {
            start_node_job(&service, cm, &mut rx).await?;
        }
    }
}

pub async fn start_coord_job(
    service: &Arc<CalmService>,
    cm: &ClusterManager,
    rx: &mut mpsc::UnboundedReceiver<ClusterEvent>,
) -> CoreResult<()> {
    let mut live_nodes_stream = cm.chitchat.lock().await.live_nodes_watch_stream();

    loop {
        if !cm.am_i_coord_node() {
            log::warn!("⚠️  No longer the coord node, switching to node job.");
            return Ok(());
        }
        tokio::select! {
            // 监听节点成员变化（加入/离开）
            Some(live_nodes) = live_nodes_stream.next() => {
                // 检查是否是中央结点
            }

            // 监听 partition key 变化
            Some(event) = rx.recv() => {
                log::info!("🔑 [Cluster] gossip event: {:?}", event);
                // 处理 partition 变化
                match event {
                    ClusterEvent::PartitionChanged { key, value, node } => {
                        log::info!("🔑 [Cluster] Partition key changed: {}={} on {:?}", key, value, node);
                        match keys::parse_partition_key(&key){
                            Ok((table, partition, version)) => {
                                if let Err(e) = service.ddl.change_partition_route(&table, &partition, &node, version).await {
                                    log::warn!("⚠️  Failed to change partition route: table={}, partition={}, node={}, version={}, err={}", table, partition, node, version, e);
                                }
                            },
                            Err(e) => {
                                log::warn!("⚠️  Failed to parse partition key: {} err:{}", key,e);
                            }
                        }

                    }
                    ClusterEvent::CoordNodeChanged { value, node } => {
                        log::info!("🔑 [Cluster] Coord node changed: {} on {:?}", value, node);
                        if let Err(e) = cm.change_coord_node(&value).await {
                            log::warn!("⚠️  Failed to set coord node: {}. Retrying...", e);
                        }
                    }
                }
            }

            _ = tokio::time::sleep(tokio::time::Duration::from_secs(10)) => {

                // 定期任务
            }
        }
    }
}

pub async fn start_node_job(
    service: &Arc<CalmService>,
    cm: &ClusterManager,
    rx: &mut mpsc::UnboundedReceiver<ClusterEvent>,
) -> CoreResult<()> {
    loop {
        if !cm.am_i_coord_node() {
            log::warn!("⚠️  No longer the coord node, switching to node job.");
            return Ok(());
        }
        tokio::select! {
            // 监听 partition key 变化
            Some(envent) = rx.recv() => {
                // match envent {
                //     ClusterEvent::PartitionChanged { key, value, node } => {
                //         log::info!("🔑 [Cluster] Partition key changed: {}={} on {:?}", key, value, node);
                //         match keys::parse_partition_key(&key){
                //             Ok((table, partition, version)) => {
                //                 if let Err(e) = service.ddl.change_partition_route(&table, &partition, &node, version).await {
                //                     log::warn!("⚠️  Failed to change partition route: table={}, partition={}, node={}, version={}, err={}", table, partition, node, version, e);
                //                 }
                //             },
                //             Err(e) => {
                //                 log::warn!("⚠️  Failed to parse partition key: {} err:{}", key,e);
                //             }
                //         }

                //     }
                //     ClusterEvent::CoordNodeChanged { value, node } => {
                //         log::info!("🔑 [Cluster] Coord node changed: {} on {:?}", value, node);
                //         if let Err(e) = cm.change_coord_node(&value).await {
                //             log::warn!("⚠️  Failed to set coord node: {}. Retrying...", e);
                //         }
                //     }
                // }
                todo!()
            }

            _ = tokio::time::sleep(tokio::time::Duration::from_secs(10)) => {
                log::debug!("⏳ [Cluster] Node job workers...");
            }
        }
    }
}
