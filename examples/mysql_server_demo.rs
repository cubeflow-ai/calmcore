/// MySQL 协议演示 - 通过 JDBC 连接
///
/// 本示例演示如何启动 MySQL 协议服务器,并通过 JDBC 进行连接和操作:
/// 1. CREATE TABLE - 创建表
/// 2. INSERT - 插入数据
/// 3. SELECT - 查询数据
/// 4. UPDATE - 更新数据
/// 5. DELETE - 删除数据
/// 6. DROP TABLE - 删除表
///
/// 需要先运行此 Rust 程序启动 MySQL 服务器,
/// 然后运行 Java JDBC 客户端进行连接测试。
use calm::engine::{Engine, EngineConfig};
use calm::protocol::mysql::MysqlServer;
use std::path::PathBuf;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║          MySQL 协议服务器演示                               ║");
    println!("╚══════════════════════════════════════════════════════════════╝\n");

    // 1. 创建 Engine
    let config = EngineConfig {
        data_dir: PathBuf::from("/tmp/mysql_demo"),
        persist_check_interval_secs: 60,
        max_concurrent_persists: 4,
        check_after_flush: true,
    };

    let engine = Engine::new(config)?;
    println!("✅ Engine created");

    // 2. 启动 MySQL 服务器
    let server = MysqlServer::new(engine);

    println!("\n🚀 Starting MySQL server on 127.0.0.1:3306...");
    println!("\n📝 You can now connect using:");
    println!("   - MySQL Client: mysql -h 127.0.0.1 -P 3306");
    println!("   - JDBC URL: jdbc:mysql://127.0.0.1:3306/calm");
    println!("\n💡 Supported SQL operations:");
    println!("   CREATE TABLE users (id U64 INDEXED, name KEYWORD, age I32);");
    println!("   INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30);");
    println!("   SELECT * FROM users;");
    println!("   UPDATE users SET age=31 WHERE id=1;");
    println!("   DELETE FROM users WHERE id=1;");
    println!("   DROP TABLE users;");
    println!("\n⏸️  Press Ctrl+C to stop the server\n");

    // 启动服务器(会阻塞)
    server.start("127.0.0.1:3306").await?;

    Ok(())
}
