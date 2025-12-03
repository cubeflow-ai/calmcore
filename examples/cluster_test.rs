//! 集群功能测试示例
//!
//! 演示如何启动一个 3 节点集群

use calm::cluster::{ClusterConfig, ClusterManager, PartitionManager};
use std::sync::Arc;
use tokio::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    // 从环境变量读取节点配置
    let config = ClusterConfig::from_env();

    println!("🚀 Starting CalmCore Cluster Node");
    println!("  Node ID: {}", config.node_id);
    println!("  Listen: {}", config.listen_addr);
    println!("  Seeds: {:?}", config.seed_nodes);

    // 启动集群管理器
    let cluster = Arc::new(
        ClusterManager::new(
            config.node_id.clone(),
            config.cluster_id.clone(),
            config.listen_addr.clone(),
            config.seed_nodes.clone(),
        )
        .await?,
    );

    println!("✅ Cluster manager started");

    // 创建分区管理器
    let partition_manager = PartitionManager::new(cluster.clone());

    // 如果是第一个节点（种子节点），初始化分区
    if config.seed_nodes.is_empty() {
        println!("🌱 Initializing partitions (seed node)");
        partition_manager
            .initialize_partitions("test_table", 10)
            .await?;
    }

    // 启动自动故障转移
    partition_manager.start_auto_failover().await;

    println!("✅ Auto-failover enabled");
    println!("\n📊 Cluster Status:");

    // 定期打印集群状态
    loop {
        tokio::time::sleep(Duration::from_secs(10)).await;

        let nodes = cluster.live_nodes().await;
        println!("\n=== Cluster Status ===");
        println!("  Live nodes: {}", nodes.len());
        for node in &nodes {
            println!("    - {} ({})", node.id, node.gossip_addr);
        }

        // 打印分区分配情况
        let partitions = partition_manager.get_node_partitions(&config.node_id).await;
        if !partitions.is_empty() {
            println!("\n  Partitions owned by this node:");
            for (table, pids) in partitions {
                println!("    {} -> {:?}", table, pids);
            }
        }
    }
}
