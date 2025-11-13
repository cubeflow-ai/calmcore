use calm::engine::{Engine, EngineConfig};
use calm::partition::PartitionStrategy;
use calm::schema::{Field, Schema, ValueType};
use std::path::PathBuf;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. 创建 Engine
    let config = EngineConfig {
        data_dir: PathBuf::from("/tmp/calmcore_test"),
        ..Default::default()
    };
    let engine = Engine::new(config)?;

    // 2. 创建表
    let mut schema = Schema::new();
    schema.add_field(Field {
        name: "id".to_string(),
        value_type: ValueType::Keyword,
        index: true,
        indexed: true,
        is_primary: true,
        ..Default::default()
    })?;
    schema.add_field(Field {
        name: "name".to_string(),
        value_type: ValueType::Keyword,
        index: true,
        indexed: true,
        ..Default::default()
    })?;
    schema.add_field(Field {
        name: "updateTime".to_string(),
        value_type: ValueType::I64,
        index: true,
        indexed: true,
        ..Default::default()
    })?;
    schema.set_primary_key("id".to_string());

    engine
        .create_table("test_events", schema, PartitionStrategy::Hash, 2)
        .await?;

    println!("✅ Table 'test_events' created");

    // 3. 插入数据
    let data = vec![
        serde_json::json!({
            "id": "evt-001",
            "name": "Test Event 1",
            "updateTime": 1736553600000i64  // 2025-01-11
        }),
        serde_json::json!({
            "id": "evt-002",
            "name": "Test Event 2",
            "updateTime": 1736640000000i64  // 2025-01-12
        }),
    ];

    let batch_json = serde_json::to_string(&data)?;
    let write_result = engine.upsert_json("test_events", &batch_json).await?;
    println!("✅ Inserted {} documents", write_result.len());

    // 4. 检查分区状态
    for partition_id in 0..2 {
        if let Some(partition) = engine.get_partition("test_events", partition_id).await {
            let current_seg = partition.get_current_segment();
            println!(
                "📊 Partition {} current_segment doc_count: {}",
                partition_id,
                current_seg.doc_count()
            );

            let frozen_segs = partition.get_frozen_segments();
            println!(
                "📊 Partition {} frozen_segments count: {}",
                partition_id,
                frozen_segs.len()
            );
        }
    }

    // 5. 执行 SQL 查询 (ES/MySQL 使用的路径)
    println!("\n🔍 Executing SQL query via execute_sql (ES/MySQL path)...");
    let sql = "SELECT * FROM test_events";
    let result = engine.execute_sql(sql).await?;

    println!("📊 SQL query returned {} batches", result.batches.len());
    for (i, batch) in result.batches.iter().enumerate() {
        println!("  Batch {}: {} rows", i, batch.num_rows());
    }

    // 6. 测试范围查询
    println!("\n🔍 Testing range query...");
    let range_sql = "SELECT * FROM test_events WHERE updateTime >= 1736553600000";
    let range_result = engine.execute_sql(range_sql).await?;

    println!(
        "📊 Range query returned {} batches",
        range_result.batches.len()
    );
    for (i, batch) in range_result.batches.iter().enumerate() {
        println!("  Batch {}: {} rows", i, batch.num_rows());
    }

    Ok(())
}
