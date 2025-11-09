use calm::{
    partition::Partition, protocol::mysql::MysqlServer, schema::field::FieldOption, schema::Schema,
};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::mpsc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Calm MySQL Protocol Server (使用预准备数据) ===\n");

    // 1. 检查数据目录
    let data_dir = PathBuf::from("/tmp/mysql_demo");
    if !data_dir.exists() {
        eprintln!("❌ 错误: 数据目录不存在: {}", data_dir.display());
        eprintln!("请先运行: cargo run --example prepare_test_data");
        std::process::exit(1);
    }

    println!("✓ 数据目录: {}", data_dir.display());

    // 2. 创建 Schema (需要与准备数据时相同)
    let schema = Schema {
        name: "test_data".to_string(),
        primary_key: None,
        store_source: false,
        fields: vec![
            FieldOption::I64 {
                name: "id".to_string(),
                index: true,
            },
            FieldOption::Keyword {
                name: "name".to_string(),
                index: true,
                is_array: false,
                persist_option: None,
                case_sensitive: true,
            },
            FieldOption::I64 {
                name: "age".to_string(),
                index: true,
            },
            FieldOption::Keyword {
                name: "city".to_string(),
                index: true,
                is_array: false,
                persist_option: None,
                case_sensitive: true,
            },
        ],
        persist_policy: Default::default(),
    };

    // 3. 创建持久化通知通道
    let (persist_tx, mut persist_rx) = mpsc::unbounded_channel();

    // 启动后台持久化处理
    tokio::spawn(async move {
        while let Some(partition_id) = persist_rx.recv().await {
            println!("[Background] Partition {} needs persistence", partition_id);
        }
    });

    // 4. 加载现有 Partition
    let partition = Partition::new(0, data_dir.clone(), schema, persist_tx);

    println!("✓ Partition 加载完成\n");

    // 5. 启动 MySQL 服务器
    let partition = Arc::new(partition);
    let mysql_server = MysqlServer::new(partition);

    println!("🚀 MySQL 服务器正在启动...");
    println!("   地址: 127.0.0.1:3306");
    println!();
    println!("连接方式:");
    println!("   mysql -h 127.0.0.1 -P 3306");
    println!();
    println!("示例查询:");
    println!("   SHOW DATABASES;");
    println!("   USE calm;");
    println!("   SHOW TABLES;");
    println!("   SELECT * FROM data LIMIT 10;");
    println!("   SELECT * FROM data WHERE age > 40;");
    println!("   SELECT COUNT(*) FROM data;");
    println!("   SELECT AVG(age), MIN(age), MAX(age) FROM data;");
    println!("   SELECT city, COUNT(*) FROM data GROUP BY city;");
    println!();
    println!("按 Ctrl+C 停止服务器");
    println!();
    println!("═══════════════════════════════════════════════════════");

    // 6. 启动服务
    mysql_server.start("127.0.0.1:3306").await?;

    Ok(())
}
