/// MySQL CREATE TABLE 演示
///
/// 本示例演示如何通过 MySQL 协议创建表并插入查询数据
///
/// 步骤:
/// 1. 启动 MySQL 服务器
/// 2. 通过 mysql 客户端连接: mysql -h 127.0.0.1 -P 3307
/// 3. 执行 SQL:
///    CREATE TABLE users (id BIGINT PRIMARY KEY, name TEXT, age INT);
///    SHOW TABLES;
///    DROP TABLE users;
///
use calm::engine::{Engine, EngineConfig};
use calm::protocol::mysql::MysqlServer;
use std::path::PathBuf;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n╔══════════════════════════════════════════════════════════════╗");
    println!("║          MySQL CREATE TABLE 演示                             ║");
    println!("╚══════════════════════════════════════════════════════════════╝\n");

    // 清理旧数据
    let data_dir = PathBuf::from("/tmp/mysql_create_table_demo");
    if data_dir.exists() {
        std::fs::remove_dir_all(&data_dir)?;
        println!("🧹 Cleaned old data directory");
    }

    // 1. 创建 Engine
    let config = EngineConfig {
        data_dir: data_dir.clone(),
        persist_check_interval_secs: 60,
        max_concurrent_persists: 4,
        check_after_flush: true,
    };

    let engine = Engine::new(config)?;
    println!("✅ Engine created at: {}", data_dir.display());

    // 2. 启动 MySQL 服务器
    let server = MysqlServer::new(engine);

    println!("\n🚀 Starting MySQL server on 127.0.0.1:3307...");
    println!("\n📝 Connect using:");
    println!("   mysql -h 127.0.0.1 -P 3307");
    println!("\n💡 Try these SQL commands:");
    println!("   -- 创建表");
    println!("   CREATE TABLE users (id BIGINT PRIMARY KEY, name TEXT, age INT);");
    println!("");
    println!("   -- 查看表");
    println!("   SHOW TABLES;");
    println!("");
    println!("   -- 创建多个表");
    println!("   CREATE TABLE products (id INT PRIMARY KEY, title TEXT, price DOUBLE);");
    println!("   CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, amount DOUBLE);");
    println!("");
    println!("   -- 删除表");
    println!("   DROP TABLE users;");
    println!("\n⚠️  Note: INSERT/UPDATE/DELETE are not yet supported.");
    println!("    Use Rust API (Partition::upsert) for data operations.\n");
    println!("⏸️  Press Ctrl+C to stop the server\n");

    // 启动服务器(会阻塞)
    server.start("127.0.0.1:3307").await?;

    Ok(())
}
