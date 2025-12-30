use crate::{calm::CalmService, utils::error::CoreResult};

impl CalmService {
    pub async fn idle_node(&self) -> CoreResult<Option<String>> {
        // 2. 选择合适的节点来创建分区（负载均衡）
        let available_nodes = self.cluster_manager.idle_nodes().await?;

        if available_nodes.is_empty() {
            return Err(CoreError::ClusterState(
                "No available nodes to create partition".to_string(),
            ));
        }

        // 获取各节点状态并选择负载最低的节点
        let mut best_node = None;
        let mut best_score = f32::MAX;

        for node_id in &available_nodes {
            match new_data_client(node_id).await {
                Ok(client) => match client.node_info(tarpc::context::current()).await {
                    Ok(Ok(node_info)) => {
                        // 综合评分：partition 数量权重 40%，CPU 30%，内存 20%，系统负载 10%
                        let score = node_info.partition_count as f32 * 0.4
                            + node_info.cpu_usage * 0.3
                            + node_info.memory_usage * 0.2
                            + node_info.load_avg_1min * 10.0 * 0.1;

                        if score < best_score {
                            best_score = score;
                            best_node = Some(node_id.clone());
                        }
                    }
                    _ => {
                        log::warn!("⚠️  Failed to get node info from '{}', skipping", node_id);
                    }
                },
                Err(e) => {
                    log::warn!("⚠️  Cannot connect to node '{}': {}, skipping", node_id, e);
                }
            }
        }
        Ok(best_node)
    }
}
