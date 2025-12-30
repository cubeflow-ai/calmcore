use calm::{
    calm::CalmService,
    config::Config,
    protocol::{elasticsearch::ElasticsearchServer, graphql::GraphQLServer, mysql::MysqlServer},
};
use std::io::Write;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 🎨 安装 color-eyre 以获得美观的错误输出和堆栈追踪
    // 设置环境变量 RUST_BACKTRACE=1 或 RUST_LIB_BACKTRACE=1 启用堆栈
    color_eyre::install()?;

    let mut config = Config::from_args()?;

    init_logger(&config)?;

    config.init()?;

    println!("=== Calm Database - Multi-Protocol Server ===");
    println!(
        "📦 Version: {} ({})",
        version_macro::build_git_branch!(),
        version_macro::build_git_version!()
    );
    println!("🔨 Build time: {}", version_macro::build_time!());
    println!();
    println!("📁 Data directory: {:?}", config.engine.data_dir);
    println!("🌐 Host: {:?}", config.host);
    println!();

    let (graphql_port, mysql_port, es_port) =
        (config.graphql_port, config.mysql_port, config.es_port);

    let calm_service = CalmService::new(config.clone()).await?;

    // 存储服务器任务句柄
    let mut handles = Vec::new();

    // 启动 GraphQL 服务
    if let Some(port) = graphql_port {
        let addr = format!(
            "{}:{}",
            config.host.as_ref().unwrap_or(&"0.0.0.0".to_string()),
            port
        );
        println!("🚀 Starting GraphQL server on {}", addr);
        println!("   GraphQL Playground: http://{}", addr);
        let service = calm_service.clone();
        let handle = tokio::spawn(async move {
            if let Err(e) = GraphQLServer::new(service).start(&addr).await {
                eprintln!("❌ GraphQL server error: {}", e);
            }
        });
        handles.push(handle);
    }

    // 启动 Elasticsearch 服务
    if let Some(port) = es_port {
        let addr = format!(
            "{}:{}",
            config.host.as_ref().unwrap_or(&"0.0.0.0".to_string()),
            port
        );
        println!("🚀 Starting Elasticsearch server on {}", addr);
        println!("   Health check: http://{}/_cluster/health", addr);

        let service_clone = calm_service.clone();
        let handle = tokio::spawn(async move {
            let server = ElasticsearchServer::new(service_clone);
            if let Err(e) = server.start(&addr).await {
                eprintln!("❌ Elasticsearch server error: {}", e);
            }
        });
        handles.push(handle);
    }

    // 启动 MySQL 服务
    if let Some(port) = mysql_port {
        let addr = format!(
            "{}:{}",
            config.host.as_ref().unwrap_or(&"0.0.0.0".to_string()),
            port
        );
        println!("🚀 Starting MySQL server on {}", addr);
        println!(
            "   Connect: mysql -h {} -P {} -u {} {}",
            config.host.as_ref().unwrap_or(&"127.0.0.1".to_string()),
            port,
            config.user,
            if config.password.is_empty() { "" } else { "-p" }
        );

        let service_clone = calm_service.clone();
        let user = config.user.clone();
        let password = config.password.clone();
        let handle = tokio::spawn(async move {
            let server = MysqlServer::new(service_clone, user, password);
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

    calm_service.stop().await?;
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
