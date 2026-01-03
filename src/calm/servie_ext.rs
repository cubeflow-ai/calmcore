use crate::{
    calm::{new_data_client, CalmService, NodeInfo},
    cluster::keys,
    utils::error::{CoreError, CoreResult},
};

impl CalmService {
    pub async fn idle_nodes(&self) -> CoreResult<Vec<NodeInfo>> {
        // 1. 获取所有可用节点
        let available_nodes = self.cluster_manager.idle_nodes().await?;

        if available_nodes.is_empty() {
            return Err(CoreError::ClusterState(
                "No available nodes to create partitions".to_string(),
            ));
        }

        log::info!("📊 Found {} available nodes", available_nodes.len());

        // 2. 获取每个节点的状态（partition 数量、CPU、内存等），如果节点无法连接则跳过
        let mut node_states: Vec<NodeInfo> = Vec::new();

        for node_id in available_nodes {
            if node_id == keys::SINGLE_NODE_CLUSTER_ID {
                let mut local_info = self.local_node_info().await?;
                local_info.node_id = node_id;
                node_states.push(local_info);
                continue;
            }

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
                        node_states.push(node_info);
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
                "No connectable nodes available to create partitions".to_string(),
            ));
        }

        // 3. 排序节点：优先选择负载低的节点（综合考虑 partition 数量、CPU、内存、系统负载）
        node_states.sort_by(|a, b| {
            // 综合评分：partition 数量权重 40%，CPU 30%，内存 20%，系统负载 10%
            let score_a = a.partition_count as f32 * 0.8;
            // + a.cpu_usage * 0.1
            // + a.memory_usage * 0.2
            // + a.load_avg_1min * 10.0 * 0.1;
            let score_b = b.partition_count as f32 * 0.8;
            // + b.cpu_usage * 0.1
            // + b.memory_usage * 0.2
            // + b.load_avg_1min * 10.0 * 0.1;
            score_a
                .partial_cmp(&score_b)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        log::info!("📊 Node load distribution:");
        for info in &node_states {
            log::info!(
                "   - {}: {} partitions, CPU: {:.1}%, Memory: {:.1}% ({}/{}MB), Load: {:.2}",
                info.node_id,
                info.partition_count,
                info.cpu_usage,
                info.memory_usage,
                info.used_memory / 1024 / 1024,
                info.total_memory / 1024 / 1024,
                info.load_avg_1min
            );
        }

        Ok(node_states)
    }

    pub async fn idle_node(&self) -> CoreResult<String> {
        // 2. 选择合适的节点来创建分区（负载均衡）
        let idle_nodes = self.idle_nodes().await?;
        Ok(idle_nodes.first().map(|n| n.node_id.clone()).unwrap())
    }

    pub async fn check_partition_on_node(
        &self,
        table_name: &str,
        partition_name: &str,
        node_id: &str,
    ) -> CoreResult<Option<bool>> {
        match new_data_client(node_id)
            .await?
            .get_partition_detail(
                tarpc::context::current(),
                table_name.to_string(),
                partition_name.to_string(),
            )
            .await
        {
            Ok(Ok(_)) => Ok(Some(true)),
            Ok(Err(e)) => {
                if e.code() != CoreError::NotExisted("".to_string()).code() {
                    Ok(Some(false))
                } else {
                    Ok(None)
                }
            }
            _ => Ok(None),
        }
    }
}
