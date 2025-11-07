use calm::{
    partition::Partition,
    protocol::mysql::MysqlServer,
    schema::{field::FieldOption, PersistPolicy, Schema},
};
use datafusion::arrow::{
    array::{Int64Array, StringArray},
    datatypes::{DataType, Field, Schema as ArrowSchema},
    record_batch::RecordBatch,
};
use std::{path::PathBuf, sync::Arc};
use tokio::sync::mpsc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Calm MySQL Protocol Server Demo ===\n");

    // 1. 创建 Calm Schema
    let schema = Schema {
        name: "demo".to_string(),
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
        ],
        persist_policy: PersistPolicy {
            max_docs_per_segment: 100_000,
            ..Default::default()
        },
    };

    // 2. 创建 Partition
    let (persist_tx, mut persist_rx) = mpsc::unbounded_channel();

    // 后台持久化任务
    tokio::spawn(async move {
        while let Some(partition_id) = persist_rx.recv().await {
            println!("[Background] Partition {} needs persistence", partition_id);
        }
    });

    let data_dir = PathBuf::from("/tmp/calm_mysql_demo");
    std::fs::create_dir_all(&data_dir)?;

    let mut partition = Partition::new(1, data_dir.clone(), schema.clone(), persist_tx);

    // 3. 插入一些示例数据
    println!("Inserting sample data...");

    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::Int64, false),
    ]));

    // 插入10条示例数据
    let ids = Int64Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    let names = StringArray::from(vec![
        "Alice", "Bob", "Charlie", "David", "Eve", "Frank", "Grace", "Henry", "Ivy", "Jack",
    ]);
    let ages = Int64Array::from(vec![25, 30, 35, 28, 32, 27, 29, 33, 26, 31]);

    let batch = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![Arc::new(ids), Arc::new(names), Arc::new(ages)],
    )?;

    partition.upsert(batch)?;

    println!("✓ Inserted 10 records\n");

    // 4. 启动 MySQL 服务器
    let partition = Arc::new(partition);
    let mysql_server = MysqlServer::new(partition);

    println!("Starting MySQL server on 127.0.0.1:13306...");
    println!("\nYou can now connect using:");
    println!("  mysql -h 127.0.0.1 -P 13306 -u root");
    println!("\nExample queries:");
    println!("  SHOW DATABASES;");
    println!("  SHOW TABLES;");
    println!("  SELECT * FROM data;");
    println!("  SELECT * FROM data WHERE age > 30;");
    println!("  SELECT name, age FROM data WHERE age BETWEEN 25 AND 30;");
    println!("  SELECT COUNT(*) FROM data;");
    println!("\nPress Ctrl+C to stop the server\n");

    mysql_server.start("127.0.0.1:13306").await?;

    Ok(())
}
