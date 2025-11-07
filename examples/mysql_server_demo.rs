use calm::{
    partition::Partition,
    protocol::CalmMySQLServer,
    schema::{field::FieldOption, PersistPolicy, Schema},
};
use datafusion::arrow::{
    array::{Int64Array, StringArray},
    datatypes::{DataType, Field, Schema as ArrowSchema},
    record_batch::RecordBatch,
};
use std::{net::SocketAddr, path::PathBuf, sync::Arc};
use tokio::sync::mpsc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Calm MySQL Protocol Server Demo ===\n");

    // 1. 创建Schema
    let schema = Schema {
        name: "users".to_string(),
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
        persist_policy: PersistPolicy {
            max_docs_per_segment: 1_000_000,
            ..Default::default()
        },
    };

    // 2. 创建Partition
    let (persist_tx, mut persist_rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        while let Some(partition_id) = persist_rx.recv().await {
            println!("[Background] Partition {} needs persistence", partition_id);
        }
    });

    let data_dir = PathBuf::from("/tmp/calm_mysql_demo");
    std::fs::create_dir_all(&data_dir)?;

    let mut partition = Partition::create(1, data_dir.clone(), schema.clone(), persist_tx)?;

    // 3. 插入示例数据
    println!("📝 Inserting sample data...\n");

    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::Int64, false),
        Field::new("city", DataType::Utf8, false),
    ]));

    let users = vec![
        (1, "Alice", 30, "Beijing"),
        (2, "Bob", 25, "Shanghai"),
        (3, "Charlie", 35, "Guangzhou"),
        (4, "Diana", 28, "Shenzhen"),
        (5, "Eve", 32, "Hangzhou"),
        (6, "Frank", 29, "Chengdu"),
        (7, "Grace", 31, "Wuhan"),
        (8, "Henry", 27, "Nanjing"),
        (9, "Iris", 33, "Xi'an"),
        (10, "Jack", 26, "Chongqing"),
    ];

    for batch_start in (0..users.len()).step_by(5) {
        let batch_end = (batch_start + 5).min(users.len());
        let batch_users = &users[batch_start..batch_end];

        let ids: Vec<i64> = batch_users.iter().map(|u| u.0).collect();
        let names: Vec<&str> = batch_users.iter().map(|u| u.1).collect();
        let ages: Vec<i64> = batch_users.iter().map(|u| u.2 as i64).collect();
        let cities: Vec<&str> = batch_users.iter().map(|u| u.3).collect();

        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(names)),
                Arc::new(Int64Array::from(ages)),
                Arc::new(StringArray::from(cities)),
            ],
        )?;

        partition.upsert_batch(batch)?;
    }

    println!("✅ Inserted {} users\n", users.len());

    // 4. 启动MySQL协议服务器
    let addr: SocketAddr = "127.0.0.1:3307".parse()?;
    let server = CalmMySQLServer::new(addr, Arc::new(partition), schema);

    println!("════════════════════════════════════════════════════════");
    println!("  🚀 MySQL Server is ready!");
    println!("════════════════════════════════════════════════════════");
    println!();
    println!("  Connect with:");
    println!("    mysql -h 127.0.0.1 -P 3307 -u root");
    println!();
    println!("  Example queries:");
    println!("    SELECT * FROM users;");
    println!("    SELECT * FROM users WHERE age > 30;");
    println!("    SELECT name, city FROM users WHERE city = 'Beijing';");
    println!("    SELECT COUNT(*) FROM users;");
    println!("    SELECT city, COUNT(*) as count FROM users GROUP BY city;");
    println!();
    println!("════════════════════════════════════════════════════════");
    println!();

    server.run().await?;

    Ok(())
}
