mod config;

use calm::{
    catalog::Catalog,
    cluster::{ClusterManager, PartitionManager},
    engine::Engine,
    // protocol::{elasticsearch::ElasticsearchServer, graphql::GraphQLServer, mysql::MysqlServer},
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

    // 初始化集群
    let cluster_manager = Arc::new(ClusterManager::new(config.to_cluster_config()).await?);

    // 创建 Catalog
    let catalog = Arc::new(Catalog::new(config.engine.data_dir.clone())?);

    // 创建 Engine
    let engine_config = config.to_engine_config();
    let engine = Engine::new(engine_config)?;

    // 加载已存在的表（暂时为空方法）
    println!("📚 Loading existing tables...");
    engine.load_existing_tables().await?;
    println!("✓ Tables loaded");
    println!();

    // 存储服务器任务句柄
    // let mut handles = Vec::new();

    // // 启动 GraphQL 服务
    // if let Some(port) = config.graphql_port {
    //     let addr = format!("{}:{}", config.host, port);
    //     println!("🚀 Starting GraphQL server on {}", addr);
    //     println!("   GraphQL Playground: http://{}", addr);
    //     println!("   GraphQL Playground: http://{}/playground", addr);

    //     let engine_clone = engine.clone();
    //     let catalog_clone = catalog.clone();
    //     let handle = tokio::spawn(async move {
    //         let server = GraphQLServer::new(engine_clone, catalog_clone);
    //         if let Err(e) = server.start(&addr).await {
    //             eprintln!("❌ GraphQL server error: {}", e);
    //         }
    //     });
    //     handles.push(handle);
    // }

    // // 启动 Elasticsearch 服务
    // if let Some(port) = config.es_port {
    //     let addr = format!("{}:{}", config.host, port);
    //     println!("🚀 Starting Elasticsearch server on {}", addr);
    //     println!("   Health check: http://{}/_cluster/health", addr);

    //     let engine_clone = engine.clone();
    //     let handle = tokio::spawn(async move {
    //         let server = ElasticsearchServer::new(engine_clone);
    //         if let Err(e) = server.start(&addr).await {
    //             eprintln!("❌ Elasticsearch server error: {}", e);
    //         }
    //     });
    //     handles.push(handle);
    // }
    // // 启动 MySQL 服务
    // if let Some(port) = config.mysql_port {
    //     let addr = format!("{}:{}", config.host, port);
    //     println!("🚀 Starting MySQL server on {}", addr);
    //     println!(
    //         "   Connect: mysql -h {} -P {} -u {} {}",
    //         config.host,
    //         port,
    //         config.user,
    //         if config.password.is_empty() { "" } else { "-p" }
    //     );

    //     let engine_clone = engine.clone();
    //     let user = config.user.clone();
    //     let password = config.password.clone();
    //     let handle = tokio::spawn(async move {
    //         let server = MysqlServer::new(engine_clone, user, password);
    //         if let Err(e) = server.start(&addr).await {
    //             eprintln!("❌ MySQL server error: {}", e);
    //         }
    //     });
    //     handles.push(handle);
    // }

    // if handles.is_empty() {
    //     eprintln!("❌ No servers enabled. Use --help for usage information.");
    //     return Err("No servers enabled".into());
    // }

    println!();
    println!("✓ All servers started");
    println!("📊 Press Ctrl+C to shutdown");
    println!();

    // // 等待所有服务器任务
    // for handle in handles {
    //     let _ = handle.await;
    // }

    // 关闭引擎
    println!("\n🛑 Shutting down...");

    // Gracefully shutdown cluster if enabled
    println!("🌐 Shutting down cluster manager...");
    if let Err(e) = cluster_manager.shutdown().await {
        eprintln!("⚠️  Cluster shutdown error: {}", e);
    }
    println!("✓ Cluster manager shutdown complete");

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
