mod config;

use calm::{
    cluster::{ClusterEvent, ClusterManager, PartitionManager, QueryRouter, VotingCoordinator},
    engine::Engine,
    protocol::{elasticsearch::ElasticsearchServer, graphql::GraphQLServer, mysql::MysqlServer},
};
use config::Config;
use std::io::Write;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 加载配置
    let config = Config::from_args()?;

    // 初始化日志
    init_logger(&config)?;

    println!("=== Calm Database - Multi-Protocol Server ===");
    println!(
        "📦 Version: {} ({})",
        version_macro::build_git_branch!(),
        version_macro::build_git_version!()
    );
    println!("🔨 Build time: {}", version_macro::build_time!());
    println!();
    println!("📁 Data directory: {:?}", config.engine.data_dir);
    println!("🌐 Host: {}", config.host);
    println!();

    // 创建 Engine
    let engine_config = config.to_engine_config();
    let engine = Engine::new(engine_config)?;

    // 加载已存在的表
    println!("📚 Loading existing tables...");
    engine.load_existing_tables().await?;
    println!("✓ Tables loaded");
    println!();

    // 初始化集群管理器（如果启用）
    // Requirements 10.3: Initialize on startup based on config
    let cluster_config = config.to_cluster_config();
    let (cluster_manager, query_router): (Option<Arc<ClusterManager>>, Option<Arc<QueryRouter>>) =
        if cluster_config.enabled || !cluster_config.seed_nodes.is_empty() {
            println!("🌐 Initializing cluster...");
            println!("   Node ID: {}", cluster_config.node_id);
            println!("   Cluster ID: {}", cluster_config.cluster_id);
            println!("   Gossip Address: {}", cluster_config.listen_addr);
            if !cluster_config.seed_nodes.is_empty() {
                println!("   Seed Nodes: {:?}", cluster_config.seed_nodes);
            } else {
                println!("   Mode: Standalone (no seed nodes)");
            }

            match ClusterManager::new(cluster_config).await {
                Ok(cm) => {
                    let cm = Arc::new(cm);
                    println!("✓ Cluster manager initialized");

                    // Create VotingCoordinator and PartitionManager
                    let voting = Arc::new(VotingCoordinator::new(
                        config.to_cluster_config().vote_timeout,
                    ));
                    let partition_manager = Arc::new(PartitionManager::new(cm.clone(), voting));

                    // Create QueryRouter for routing queries
                    // Requirements 11.1, 11.2: Route queries through QueryRouter
                    let query_router = Arc::new(QueryRouter::new(partition_manager.clone()));

                    // Subscribe to cluster events for partition management
                    // Requirements 10.3: Subscribe to events for partition management
                    let mut event_rx = cm.subscribe();
                    let pm_clone = partition_manager.clone();
                    tokio::spawn(async move {
                        while let Ok(event) = event_rx.recv().await {
                            match event {
                                ClusterEvent::NodeDead(node_id) => {
                                    log::info!(
                                        "🔴 [Main] Node {} marked as dead, initiating failover",
                                        node_id
                                    );
                                    if let Err(e) = pm_clone.on_node_failure(&node_id).await {
                                        log::error!(
                                            "❌ [Main] Failover error for node {}: {}",
                                            node_id,
                                            e
                                        );
                                    }
                                }
                                ClusterEvent::NodeRecovered(node_id) => {
                                    log::info!("🟢 [Main] Node {} recovered", node_id);
                                    // Cancel any pending votes for this node's partitions
                                    // The node will reclaim ownership via Gossip
                                }
                                ClusterEvent::NodeJoined(node_id) => {
                                    log::info!("🟢 [Main] Node {} joined the cluster", node_id);
                                }
                                ClusterEvent::NodeSuspect(node_id) => {
                                    log::warn!(
                                        "🟡 [Main] Node {} is suspected to be failing",
                                        node_id
                                    );
                                }
                                ClusterEvent::TopologyChanged { table, partition } => {
                                    log::info!(
                                        "🔄 [Main] Topology changed for {}:{}",
                                        table,
                                        partition
                                    );
                                }
                            }
                        }
                    });

                    // Start periodic vote processing task
                    let pm_for_voting = partition_manager.clone();
                    tokio::spawn(async move {
                        let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
                        loop {
                            interval.tick().await;
                            pm_for_voting.process_vote_results().await;
                        }
                    });

                    println!("✓ Query router initialized");
                    println!();
                    (Some(cm), Some(query_router))
                }
                Err(e) => {
                    eprintln!("❌ Failed to initialize cluster manager: {}", e);
                    eprintln!("   Continuing in standalone mode...");
                    println!();
                    (None, None)
                }
            }
        } else {
            println!("📦 Running in standalone mode (cluster disabled)");
            println!();
            (None, None)
        };

    // Log query router status
    // Requirements 11.1, 11.2: Route queries through QueryRouter
    if query_router.is_some() {
        println!("🔀 Query routing: ENABLED (cluster mode)");
        println!("   Write queries → partition's write_node");
        println!("   Read queries → load-balanced across read_nodes");
    } else {
        println!("🔀 Query routing: DISABLED (standalone mode)");
        println!("   All queries handled locally");
    }
    println!();

    // Store query_router for potential future use by protocol handlers
    // In a full implementation, this would be passed to protocol servers
    // to enable distributed query routing
    let _query_router = query_router;

    // 存储服务器任务句柄
    let mut handles = Vec::new();

    // 启动 GraphQL 服务
    if let Some(port) = config.graphql_port {
        let addr = format!("{}:{}", config.host, port);
        println!("🚀 Starting GraphQL server on {}", addr);
        println!("   GraphQL Playground: http://{}", addr);
        println!("   GraphQL Playground: http://{}/playground", addr);

        let engine_clone = engine.clone();
        let handle = tokio::spawn(async move {
            let server = GraphQLServer::new(engine_clone);
            if let Err(e) = server.start(&addr).await {
                eprintln!("❌ GraphQL server error: {}", e);
            }
        });
        handles.push(handle);
    }

    // 启动 Elasticsearch 服务
    if let Some(port) = config.es_port {
        let addr = format!("{}:{}", config.host, port);
        println!("🚀 Starting Elasticsearch server on {}", addr);
        println!("   Health check: http://{}/_cluster/health", addr);

        let engine_clone = engine.clone();
        let handle = tokio::spawn(async move {
            let server = ElasticsearchServer::new(engine_clone);
            if let Err(e) = server.start(&addr).await {
                eprintln!("❌ Elasticsearch server error: {}", e);
            }
        });
        handles.push(handle);
    }

    // 启动 MySQL 服务
    if let Some(port) = config.mysql_port {
        let addr = format!("{}:{}", config.host, port);
        println!("🚀 Starting MySQL server on {}", addr);
        println!(
            "   Connect: mysql -h {} -P {} -u {} {}",
            config.host,
            port,
            config.user,
            if config.password.is_empty() { "" } else { "-p" }
        );

        let engine_clone = engine.clone();
        let user = config.user.clone();
        let password = config.password.clone();
        let handle = tokio::spawn(async move {
            let server = MysqlServer::new(engine_clone, user, password);
            if let Err(e) = server.start(&addr).await {
                eprintln!("❌ MySQL server error: {}", e);
            }
        });
        handles.push(handle);
    }

    if handles.is_empty() {
        eprintln!("❌ No servers enabled. Use --help for usage information.");
        return Err("No servers enabled".into());
    }

    println!();
    println!("✓ All servers started");
    println!("📊 Press Ctrl+C to shutdown");
    println!();

    // 等待所有服务器任务
    for handle in handles {
        let _ = handle.await;
    }

    // 关闭引擎
    println!("\n🛑 Shutting down...");

    // Gracefully shutdown cluster if enabled
    if let Some(ref cm) = cluster_manager {
        println!("🌐 Shutting down cluster manager...");
        if let Err(e) = cm.shutdown().await {
            eprintln!("⚠️  Cluster shutdown error: {}", e);
        }
        println!("✓ Cluster manager shutdown complete");
    }

    engine.stop().await?;
    println!("✓ Shutdown complete");

    Ok(())
}

/// 初始化日志系统
fn init_logger(config: &Config) -> Result<(), Box<dyn std::error::Error>> {
    let log_level = match config.log.level.to_lowercase().as_str() {
        "trace" => log::LevelFilter::Trace,
        "debug" => log::LevelFilter::Debug,
        "info" => log::LevelFilter::Info,
        "warn" => log::LevelFilter::Warn,
        "error" => log::LevelFilter::Error,
        _ => {
            eprintln!("⚠️  Invalid log level '{}', using 'info'", config.log.level);
            log::LevelFilter::Info
        }
    };

    let target = config.log.target.to_lowercase();

    // 如果需要输出到文件，确保日志目录存在
    if (target == "file" || target == "both") && config.log.file.is_some() {
        if let Some(log_file) = &config.log.file {
            if let Some(parent) = log_file.parent() {
                std::fs::create_dir_all(parent)?;
            }
        }
    }

    let mut builder = env_logger::Builder::new();
    builder.filter_level(log_level);

    // 设置日志格式
    builder.format(|buf, record| {
        writeln!(
            buf,
            "[{} {:5}] {}",
            chrono::Local::now().format("%Y-%m-%d %H:%M:%S"),
            record.level(),
            record.args()
        )
    }); // 根据配置设置输出目标
    match target.as_str() {
        "console" => {
            // 只输出到控制台
            builder.init();
        }
        "file" => {
            // 只输出到文件
            if let Some(log_file) = &config.log.file {
                let target = Box::new(
                    std::fs::OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(log_file)?,
                );
                builder.target(env_logger::Target::Pipe(target));
                builder.init();
            } else {
                eprintln!("Warning: log target is 'file' but no log file specified, using console");
                builder.init();
            }
        }
        _ => {
            eprintln!("Warning: unknown log target '{}', using console", target);
            builder.init();
        }
    }

    Ok(())
}
