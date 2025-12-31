use std::sync::Arc;

use crate::{calm::CalmService, catalog::Catalog, utils::error::CoreResult};

pub(crate) mod coord_job;
pub(crate) mod node_job;

pub use coord_job::start_coord_job;
pub use node_job::start_node_job;

/// 处理分区变更事件的公共逻辑：从 gossip 同步单个分区信息到 catalog
///
/// 该方法被 coord_job 和 node_job 共同使用
pub async fn handle_partition_changed(
    catalog: &Arc<Catalog>,
    key: &str,
    value: &str,
    node: &str,
    node_type: &str, // "Coordinator" 或 "DataNode"
) -> CoreResult<()> {
    log::info!(
        "🔔 [{}] Received partition event: key='{}', value='{}', node='{}'",
        node_type,
        key,
        value,
        node
    );

    // 解析 key: subscribe_event 回调时会自动去掉订阅的前缀 "partition:"
    // 所以收到的格式是: "table_name:partition_name"
    let key_parts: Vec<&str> = key.split(':').collect();
    if key_parts.len() < 2 {
        log::error!(
            "⚠️  [{}] Invalid partition key format (expected 'table:partition'): '{}'",
            node_type,
            key
        );
        return Ok(());
    }

    let table_name = key_parts[0];
    let partition_name = key_parts[1];

    // 解析 value: "version:owner_node_id"
    let value_parts: Vec<&str> = value.split(':').collect();
    if value_parts.len() < 2 {
        log::error!(
            "⚠️  [{}] Invalid partition value format (expected 'version:owner_node_id'): '{}'",
            node_type,
            value
        );
        return Ok(());
    }

    let version: u64 = value_parts[0].parse().unwrap_or(0);
    let owner_node_id = value_parts[1];

    // 检查是否为删除标记
    if owner_node_id == "deleted" {
        log::info!(
            "🗑️  [{}] Partition deleted: table={}, partition={}, version={}",
            node_type,
            table_name,
            partition_name,
            version
        );

        // 从 catalog 中删除分区
        match catalog.get_or_load_table(table_name).await {
            Ok(table_info) => {
                let mut partitions = table_info.partitions.write().await;

                // 检查版本，只有更新的 deleted 标记才生效
                if let Some(existing) = partitions.get(partition_name) {
                    if existing.updated_at < version {
                        partitions.remove(partition_name);
                        log::info!(
                            "✅ [{}] Removed partition '{}/{}' from catalog (version: {})",
                            node_type,
                            table_name,
                            partition_name,
                            version
                        );
                    } else {
                        log::warn!(
                            "⏭️  [{}] Skipped outdated delete for partition '{}/{}' (existing version {} >= delete version {})",
                            node_type,
                            table_name,
                            partition_name,
                            existing.updated_at,
                            version
                        );
                    }
                } else {
                    log::debug!(
                        "ℹ️  [{}] Partition '{}/{}' already removed or never existed",
                        node_type,
                        table_name,
                        partition_name
                    );
                }
            }
            Err(e) => {
                log::error!(
                    "❌ [{}] Failed to load table '{}' for partition deletion: {}",
                    node_type,
                    table_name,
                    e
                );
            }
        }
        return Ok(());
    }

    log::info!(
        "📦 [{}] Partition changed: table={}, partition={}, owner={}, version={}",
        node_type,
        table_name,
        partition_name,
        owner_node_id,
        version
    );

    // 更新 catalog 中的分区信息
    match catalog.get_or_load_table(table_name).await {
        Ok(table_info) => {
            let mut partitions = table_info.partitions.write().await;

            log::info!(
                "📊 [{}] Current partitions for table '{}': {:?}",
                node_type,
                table_name,
                partitions.keys().collect::<Vec<_>>()
            );

            // 如果分区不存在，或版本更新，则更新
            let should_update = match partitions.get(partition_name) {
                Some(existing) => {
                    log::info!(
                        "📝 [{}] Partition '{}/{}' exists, comparing versions: existing={}, new={}",
                        node_type,
                        table_name,
                        partition_name,
                        existing.updated_at,
                        version
                    );
                    existing.updated_at < version
                }
                None => {
                    log::info!(
                        "🆕 [{}] New partition detected: {}/{}",
                        node_type,
                        table_name,
                        partition_name
                    );
                    true
                }
            };

            if should_update {
                let partition_meta = crate::catalog::table_meta::PartitionMeta {
                    partition_name: partition_name.to_string(),
                    owner: owner_node_id.to_string(),
                    created_at: version,
                    updated_at: version,
                };

                partitions.insert(partition_name.to_string(), partition_meta);

                log::info!(
                    "✅ [{}] Updated partition '{}/{}' -> owner: {}, version: {} (total partitions: {})",
                    node_type,
                    table_name,
                    partition_name,
                    owner_node_id,
                    version,
                    partitions.len()
                );
            } else {
                log::warn!(
                    "⏭️  [{}] Skipped outdated partition update: {}/{} (existing version is newer)",
                    node_type,
                    table_name,
                    partition_name
                );
            }
        }
        Err(e) => {
            log::error!(
                "❌ [{}] Failed to load table '{}' for partition update: {}",
                node_type,
                table_name,
                e
            );
        }
    }

    Ok(())
}

/// 启动集群后台任务
///
/// 根据当前节点角色动态启动相应的任务：
/// - 协调节点：运行 coordinator_job（故障恢复、健康监控）
/// - 数据节点：运行 node_job（定期维护）
///
/// 当节点角色变化时，自动切换任务
pub async fn start_cluster_job(calm_service: Arc<CalmService>) -> CoreResult<()> {
    let cluster_manager = match calm_service.cluster_manager.as_ref() {
        Some(cm) => cm.clone(),
        None => {
            log::info!("Standalone mode, no cluster job needed");
            return Ok(());
        }
    };

    log::info!("🚀 Starting cluster job manager...");

    loop {
        if cluster_manager.am_i_coord_node() {
            log::info!("🎯 Running as coordinator node, starting coordinator job...");

            // 运行协调节点任务
            if let Err(e) = start_coord_job(calm_service.clone()).await {
                log::error!("❌ Coordinator job failed: {}", e);
            }

            // coordinator_job 退出意味着不再是协调节点，继续循环检查
            log::info!("⚠️  Coordinator job exited, checking role...");
        } else {
            log::info!("📡 Running as data node, starting node job...");

            // 运行数据节点任务
            if let Err(e) =
                start_node_job(calm_service.catalog.clone(), cluster_manager.clone()).await
            {
                log::error!("❌ Node job failed: {}", e);
            }

            // node_job 退出意味着变成了协调节点，继续循环切换
            log::info!("⚠️  Node job exited, checking role...");
        }

        // 短暂延迟避免快速循环
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    }
}
