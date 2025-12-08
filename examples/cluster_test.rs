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
    let mut config = ClusterConfig::from_env();

    // 如果没有配置 seed_nodes，使用自己作为种子节点（单节点测试）
    if config.seed_nodes.is_empty() {
        println!("⚠️  No seed nodes configured, using self as seed node");
        config.seed_nodes = vec![config.listen_addr.clone()];
    }

    println!("🚀 Starting CalmCore Cluster Node");
    println!("  Node ID: {}", config.node_id);
    println!("  Listen: {}", config.listen_addr);
    println!("  Seeds: {:?}", config.seed_nodes);

    // 启动集群管理器
    let cluster = Arc::new(ClusterManager::new(config.clone()).await?);

    println!("✅ Cluster manager started");

    // 创建分区管理器
    let partition_manager = PartitionManager::new_standalone(cluster.clone());

    // 如果是种子节点，初始化分区
    if config.is_seed_node() {
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
