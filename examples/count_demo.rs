use calm::catalog::PartitionStrategy;
/// COUNT(*) 优化测试 - 简化版
///
/// 直接打印日志观察 COUNT 优化路径
/// 观察点:
/// - "Using total count (instant)" - 无 WHERE 的快速路径
/// - "Using bitmap cardinality" - 有 WHERE 的 bitmap 优化
use calm::compute::DistributedExecutor;
use calm::engine::{Engine, EngineConfig};
use calm::schema::{
    field::{FieldOption, FieldType},
    PersistPolicy, Schema,
};
use datafusion::arrow::{
    array::{Int64Array, StringArray},
    datatypes::{DataType, Field, Schema as ArrowSchema},
    record_batch::RecordBatch,
};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    println!("\n🚀 COUNT(*) Optimization Demo");
    println!("{}", "=".repeat(80));

    let config = EngineConfig {
        data_dir: "/tmp/calm_count_demo".into(),
        ..Default::default()
    };

    // 删除旧数据
    let _ = std::fs::remove_dir_all("/tmp/calm_count_demo");

    let engine = Engine::new(config)?;

    // 创建测试表
    println!("\n📋 Creating test table...");
    let schema = Schema {
        name: "test_count".to_string(),
        primary_key: None,
        fields: vec![
            calm::schema::field::Field {
                name: "id".to_string(),
                field_type: FieldType::Int64,
                option: FieldOption::NotIndexed,
            },
            calm::schema::field::Field {
                name: "status".to_string(),
                field_type: FieldType::Str,
                option: FieldOption::Indexed,
            },
        ],
        persist_policy: PersistPolicy {
            max_docs_per_segment: 1000000,
            max_segment_age: None,
        },
        store_source: false,
    };

    engine
        .create_table(schema, PartitionStrategy::Hash, 2, None)
        .await?;
    println!("✅ Table created");

    // 插入测试数据
    println!("\n📝 Inserting 10000 records...");
    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("status", DataType::Utf8, false),
    ]));

    for batch_num in 0..10 {
        let start_id = batch_num * 1000;
        let ids: Vec<i64> = (start_id..start_id + 1000).collect();
        let statuses: Vec<String> = ids
            .iter()
            .map(|id| {
                match id % 3 {
                    0 => "active",
                    1 => "pending",
                    _ => "completed",
                }
                .to_string()
            })
            .collect();

        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(statuses)),
            ],
        )?;

        engine.upsert("test_count", batch).await?;
    }
    println!("✅ 10000 records inserted");

    // 创建 executor
    let executor = DistributedExecutor::new(engine.clone());

    // 测试 1: COUNT(*) 无 WHERE
    println!("\n{}", "=".repeat(80));
    println!("📊 Test 1: COUNT(*) without WHERE");
    println!("{}", "=".repeat(80));
    println!("Expected log: '🎯 [COUNT Optimization] Using total count (instant)'");
    println!();

    let sql1 = "SELECT COUNT(*) FROM test_count";
    println!("SQL: {}", sql1);
    let start = std::time::Instant::now();
    let result1 = executor.execute_sql(sql1).await?;
    let elapsed1 = start.elapsed();

    println!("✅ Result: {} (expected: 10000)", result1.matched_docs);
    println!("⏱️  Time: {:?}", elapsed1);

    // 测试 2: COUNT(*) + WHERE
    println!("\n{}", "=".repeat(80));
    println!("📊 Test 2: COUNT(*) + WHERE status = 'active'");
    println!("{}", "=".repeat(80));
    println!("Expected log: '🎯 [COUNT Optimization] Using bitmap cardinality (no data read)'");
    println!();

    let sql2 = "SELECT COUNT(*) FROM test_count WHERE status = 'active'";
    println!("SQL: {}", sql2);
    let start = std::time::Instant::now();
    let result2 = executor.execute_sql(sql2).await?;
    let elapsed2 = start.elapsed();

    println!("✅ Result: {} (expected: ~3333)", result2.matched_docs);
    println!("⏱️  Time: {:?}", elapsed2);

    // 测试 3: COUNT(*) + WHERE (另一个值)
    println!("\n{}", "=".repeat(80));
    println!("📊 Test 3: COUNT(*) + WHERE status = 'pending'");
    println!("{}", "=".repeat(80));

    let sql3 = "SELECT COUNT(*) FROM test_count WHERE status = 'pending'";
    println!("SQL: {}", sql3);
    let start = std::time::Instant::now();
    let result3 = executor.execute_sql(sql3).await?;
    let elapsed3 = start.elapsed();

    println!("✅ Result: {} (expected: ~3333)", result3.matched_docs);
    println!("⏱️  Time: {:?}", elapsed3);

    // 性能总结
    println!("\n{}", "=".repeat(80));
    println!("🎯 Performance Summary");
    println!("{}", "=".repeat(80));
    println!(
        "1. COUNT(*) no WHERE:         {:?} (instant metadata read)",
        elapsed1
    );
    println!(
        "2. COUNT(*) + WHERE (active):  {:?} (bitmap cardinality)",
        elapsed2
    );
    println!(
        "3. COUNT(*) + WHERE (pending): {:?} (bitmap cardinality)",
        elapsed3
    );
    println!();
    println!("✅ All COUNT queries completed!");
    println!("🚀 Key optimization: Zero row data was read - only index/bitmap operations");
    println!("\n💡 Check the logs above for optimization paths:");
    println!("   - '🎯 [COUNT Optimization] Using total count (instant)'");
    println!("   - '🎯 [COUNT Optimization] Using bitmap cardinality (no data read)'");

    Ok(())
}
