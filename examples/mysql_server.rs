use calm::{engine::Engine, protocol::mysql::MysqlServer};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    // 初始化 Engine
    let config = calm::engine::EngineConfig::default();
    let engine = Engine::new(config).expect("Failed to create engine");

    // 加载已存在的表
    engine
        .load_existing_tables()
        .await
        .expect("Failed to load existing tables");

    println!("=== Calm Database - MySQL Server ===");
    println!("✓ Engine initialized");
    println!();
    println!("🚀 MySQL Server starting on 127.0.0.1:3307");
    println!();
    println!("📚 Supported Commands:");
    println!("  - SHOW TABLES");
    println!("  - SELECT * FROM table_name");
    println!("  - SELECT * FROM table_name WHERE condition");
    println!("  - INSERT INTO table_name (col1, col2) VALUES (val1, val2)");
    println!("  - DELETE FROM table_name WHERE condition");
    println!();
    println!("💡 Connect using:");
    println!("   mysql -h 127.0.0.1 -P 3307 -u root");
    println!();

    // 创建并启动 MySQL 服务器
    let server = MysqlServer::new(engine);
    server.start("127.0.0.1:3307").await?;

    Ok(())
}
