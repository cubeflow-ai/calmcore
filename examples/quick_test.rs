use calm::{
    partition::Partition,
    schema::{compute::PartitionTableProvider, field::FieldOption, Schema},
};
use datafusion::{
    arrow::{
        array::{Int64Array, StringArray},
        datatypes::{DataType, Field, Schema as ArrowSchema},
        record_batch::RecordBatch,
    },
    prelude::*,
};
use std::{path::PathBuf, sync::Arc, time::Instant};
use tokio::sync::mpsc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Quick Performance Test ===\n");

    // 1. 创建 Schema
    let schema = Schema {
        name: "test".to_string(),
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
        ],
        persist_policy: Default::default(),
    };

    let (persist_tx, mut persist_rx) = mpsc::unbounded_channel();
    tokio::spawn(async move { while let Some(_) = persist_rx.recv().await {} });

    let data_dir = PathBuf::from("/tmp/calm_quick_test");
    let _ = std::fs::remove_dir_all(&data_dir);

    let partition = Arc::new(Partition::new(
        1,
        data_dir.clone(),
        schema.clone(),
        persist_tx,
    ));

    // 2. 插入 10,000 条数据
    println!("Inserting 10,000 records...");
    let insert_start = Instant::now();

    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
    ]));

    let ids: Vec<i64> = (1..=10000).collect();
    let names: Vec<String> = (1..=10000).map(|i| format!("name_{}", i)).collect();

    let batch = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(names)),
        ],
    )?;

    partition.upsert(batch)?;
    println!("✓ Inserted in {:?}", insert_start.elapsed());
    println!(
        "  Current segment doc_count: {}\n",
        partition.get_current_segment().doc_count()
    );

    // 3. 创建 DataFusion 上下文
    let ctx = SessionContext::new();
    let provider = Arc::new(PartitionTableProvider::new(partition.clone()));
    ctx.register_table("test_data", provider)?;

    // 4. 测试简单查询
    println!("Testing queries:\n");

    // Q1: COUNT(*)
    println!("Q1: SELECT COUNT(*) FROM test_data");
    let start = Instant::now();
    let df = ctx.sql("SELECT COUNT(*) as total FROM test_data").await?;
    let result = df.collect().await?;
    println!("  Time: {:?}", start.elapsed());
    println!("  Result: {:?}\n", result[0]);

    // Q2: 点查询
    println!("Q2: SELECT * FROM test_data WHERE id = 5000");
    let start = Instant::now();
    let df = ctx.sql("SELECT * FROM test_data WHERE id = 5000").await?;
    let result = df.collect().await?;
    println!("  Time: {:?}", start.elapsed());
    println!(
        "  Rows returned: {}",
        result.iter().map(|b| b.num_rows()).sum::<usize>()
    );
    if !result.is_empty() && result[0].num_rows() > 0 {
        println!("  First row: {:?}\n", result[0].slice(0, 1));
    }

    // Q3: SELECT ALL
    println!("Q3: SELECT * FROM test_data");
    let start = Instant::now();
    let df = ctx.sql("SELECT * FROM test_data").await?;
    let result = df.collect().await?;
    println!("  Time: {:?}", start.elapsed());
    println!(
        "  Rows returned: {}",
        result.iter().map(|b| b.num_rows()).sum::<usize>()
    );

    println!("\n=== Now testing after persist and reload ===\n");

    // 5. 持久化segments
    println!("Persisting segments to disk...");
    let persist_start = Instant::now();
    partition.flush(false)?;
    partition.persist_unpersisted_segments()?;
    println!("✓ Persisted in {:?}\n", persist_start.elapsed());

    // 6. Drop partition并重新加载
    println!("Dropping partition and reloading from disk...");
    drop(ctx);
    drop(partition);

    let (persist_tx2, mut persist_rx2) = mpsc::unbounded_channel();
    tokio::spawn(async move { while let Some(_) = persist_rx2.recv().await {} });

    let reload_start = Instant::now();
    let partition2 = Arc::new(Partition::load(
        1,
        data_dir.clone(),
        schema.clone(),
        persist_tx2,
    )?);
    println!("✓ Reloaded in {:?}", reload_start.elapsed());
    println!("  Frozen segments: {}\n", partition2.frozen_count());

    // 7. 重新创建context并测试查询
    let ctx2 = SessionContext::new();
    let provider2 = Arc::new(PartitionTableProvider::new(partition2.clone()));
    ctx2.register_table("test_data", provider2)?;

    println!("Testing queries after reload:\n");

    // Q1: COUNT(*) after reload
    println!("Q1: SELECT COUNT(*) FROM test_data");
    let start = Instant::now();
    let df = ctx2.sql("SELECT COUNT(*) as total FROM test_data").await?;
    let result = df.collect().await?;
    println!("  Time: {:?}", start.elapsed());
    println!("  Result: {:?}\n", result[0]);

    // Q2: 点查询 after reload
    println!("Q2: SELECT * FROM test_data WHERE id = 5000");
    let start = Instant::now();
    let df = ctx2.sql("SELECT * FROM test_data WHERE id = 5000").await?;
    let result = df.collect().await?;
    println!("  Time: {:?}", start.elapsed());
    println!(
        "  Rows returned: {}",
        result.iter().map(|b| b.num_rows()).sum::<usize>()
    );
    if !result.is_empty() && result[0].num_rows() > 0 {
        println!("  First row: {:?}\n", result[0].slice(0, 1));
    }

    // Q3: SELECT ALL after reload
    println!("Q3: SELECT * FROM test_data");
    let start = Instant::now();
    let df = ctx2.sql("SELECT * FROM test_data").await?;
    let result = df.collect().await?;
    println!("  Time: {:?}", start.elapsed());
    println!(
        "  Rows returned: {}",
        result.iter().map(|b| b.num_rows()).sum::<usize>()
    );

    println!("\n=== Test Completed ===");
    Ok(())
}
