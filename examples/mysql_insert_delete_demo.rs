/// MySQL INSERT/DELETE 演示
///
/// 本示例演示如何通过 MySQL 协议插入和删除数据
///
/// 步骤:
/// 1. 启动 MySQL 服务器
/// 2. 通过 mysql 客户端连接: mysql -h 127.0.0.1 -P 3308
/// 3. 执行 SQL:
///    CREATE TABLE users (id BIGINT PRIMARY KEY, name TEXT, age INT);
///    INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30);
///    INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25);
///    SELECT * FROM users;
///    DELETE FROM users WHERE id = 1;
///    SELECT * FROM users;
///
use calm::engine::{Engine, EngineConfig};
use calm::protocol::mysql::MysqlServer;
use std::path::PathBuf;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n╔══════════════════════════════════════════════════════════════╗");
    println!("║          MySQL INSERT/DELETE 演示                            ║");
    println!("╚══════════════════════════════════════════════════════════════╝\n");

    // 清理旧数据
    let data_dir = PathBuf::from("/tmp/mysql_insert_delete_demo");
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

    println!("\n🚀 Starting MySQL server on 127.0.0.1:3308...");
    println!("\n📝 Connect using:");
    println!("   mysql -h 127.0.0.1 -P 3308");
    println!("\n💡 Try these SQL commands:");
    println!("   -- 创建表");
    println!("   CREATE TABLE users (id BIGINT PRIMARY KEY, name TEXT, age INT);");
    println!("");
    println!("   -- 插入数据");
    println!("   INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30);");
    println!("   INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25);");
    println!("   INSERT INTO users (id, name, age) VALUES (3, 'Charlie', 35);");
    println!("");
    println!("   -- 查询数据");
    println!("   SELECT * FROM users;");
    println!("   SELECT * FROM users WHERE age > 25;");
    println!("");
    println!("   -- 删除数据");
    println!("   DELETE FROM users WHERE id = 1;");
    println!("   DELETE FROM users WHERE age < 30;");
    println!("");
    println!("   -- 再次查询");
    println!("   SELECT * FROM users;");
    println!("");
    println!("   -- 测试更多操作");
    println!("   INSERT INTO users (id, name, age) VALUES (10, 'David', 40);");
    println!("   INSERT INTO users (id, name, age) VALUES (11, 'Eve', 28);");
    println!("   SELECT COUNT(*) FROM users;");
    println!("   DELETE FROM users WHERE name = 'Eve';");
    println!("   SELECT * FROM users ORDER BY age;");
    println!("\n⏸️  Press Ctrl+C to stop the server\n");

    // 启动服务器(会阻塞)
    server.start("127.0.0.1:3308").await?;

    Ok(())
}
