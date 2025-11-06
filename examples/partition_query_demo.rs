use std::{path::PathBuf, sync::Arc};

use calm::{
    partition::Partition,
    schema::{compute::PartitionTableProvider, field::FieldOption, Schema},
};
use datafusion::{
    arrow::array::{Int64Array, RecordBatch, StringArray},
    prelude::*,
};
use tokio::sync::mpsc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Partition Query Demo ===\n");

    // 1. 创建 Schema
    println!("Step 1: Creating schema...");
    let schema = Schema {
        name: "demo_table".to_string(),
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
        persist_policy: Default::default(),
    };

    println!("Schema created with fields: id, name, age\n");

    // 2. 创建 Partition
    println!("Step 2: Creating partition...");

    // 创建持久化通知通道
    let (persist_tx, mut persist_rx) = mpsc::unbounded_channel();

    // 后台任务处理持久化通知
    tokio::spawn(async move {
        while let Some(partition_id) = persist_rx.recv().await {
            println!(
                "  [Background] Partition {} needs persistence",
                partition_id
            );
        }
    });

    let partition = Arc::new(Partition::new(
        1,                               // partition_id
        PathBuf::from("/tmp/calm_demo"), // base_dir
        schema,                          // schema
        persist_tx,                      // persist_notify
    ));
    println!("Partition created with ID: {}\n", partition.id());

    // 3. 插入测试数据
    println!("Step 3: Inserting test data...");

    let arrow_schema = partition.arrow_schema.clone();

    // Batch 1: 3 records
    let batch1 = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
            Arc::new(Int64Array::from(vec![25, 30, 35])),
        ],
    )?;
    partition.upsert(batch1)?;
    println!("  Inserted 3 records: Alice(25), Bob(30), Charlie(35)");

    // Batch 2: 3 more records
    let batch2 = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![4, 5, 6])),
            Arc::new(StringArray::from(vec!["David", "Eve", "Frank"])),
            Arc::new(Int64Array::from(vec![28, 32, 27])),
        ],
    )?;
    partition.upsert(batch2)?;
    println!("  Inserted 3 records: David(28), Eve(32), Frank(27)");

    // Batch 3: 2 more records
    let batch3 = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![7, 8])),
            Arc::new(StringArray::from(vec!["Grace", "Henry"])),
            Arc::new(Int64Array::from(vec![29, 31])),
        ],
    )?;
    partition.upsert(batch3)?;
    println!("  Inserted 2 records: Grace(29), Henry(31)");
    println!("Total: 8 records inserted\n");

    // 4. 创建 DataFusion Context
    println!("Step 4: Setting up DataFusion...");
    let ctx = SessionContext::new();

    // 5. 注册 PartitionTableProvider
    let provider = PartitionTableProvider::new(partition.clone());
    ctx.register_table("demo_table", Arc::new(provider))?;
    println!("Table 'demo_table' registered\n");

    // 6. 执行 SQL 查询
    println!("=== Running SQL Queries ===\n");

    // Query 1: SELECT ALL
    println!("Query 1: SELECT * FROM demo_table");
    println!("----------------------------------------");
    let df = ctx.sql("SELECT * FROM demo_table").await?;
    df.show().await?;
    println!();

    // Query 2: WHERE age > 30
    println!("Query 2: SELECT * FROM demo_table WHERE age > 30");
    println!("----------------------------------------");
    let df = ctx.sql("SELECT * FROM demo_table WHERE age > 30").await?;
    df.show().await?;
    println!();

    // Query 3: WHERE name = 'Alice'
    println!("Query 3: SELECT * FROM demo_table WHERE name = 'Alice'");
    println!("----------------------------------------");
    let df = ctx
        .sql("SELECT * FROM demo_table WHERE name = 'Alice'")
        .await?;
    df.show().await?;
    println!();

    // Query 4: WHERE age BETWEEN 28 AND 30
    println!("Query 4: SELECT * FROM demo_table WHERE age BETWEEN 28 AND 30");
    println!("----------------------------------------");
    let df = ctx
        .sql("SELECT * FROM demo_table WHERE age BETWEEN 28 AND 30")
        .await?;
    df.show().await?;
    println!();

    // Query 5: Complex condition
    println!("Query 5: SELECT name, age FROM demo_table WHERE age >= 30");
    println!("----------------------------------------");
    let df = ctx
        .sql("SELECT name, age FROM demo_table WHERE age >= 30")
        .await?;
    df.show().await?;
    println!();

    // Query 6: COUNT and aggregation
    println!("Query 6: SELECT COUNT(*), AVG(age) FROM demo_table");
    println!("----------------------------------------");
    let df = ctx
        .sql("SELECT COUNT(*) as total_count, AVG(age) as avg_age FROM demo_table")
        .await?;
    df.show().await?;
    println!();

    // Query 7: ORDER BY
    println!("Query 7: SELECT name, age FROM demo_table ORDER BY age DESC");
    println!("----------------------------------------");
    let df = ctx
        .sql("SELECT name, age FROM demo_table ORDER BY age DESC")
        .await?;
    df.show().await?;
    println!();

    println!("=== Demo completed successfully! ===");

    Ok(())
}
