/// 快速测试 - 验证 Timestamp 字段的范围查询
use calm::catalog::PartitionStrategy;
use calm::engine::Engine;
use calm::schema::field::FieldOption;
use calm::schema::{PersistPolicy, Schema};
use chrono::{TimeZone, Utc};
use datafusion::arrow::array::{Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use datafusion::arrow::record_batch::RecordBatch;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    println!("=== Timestamp 字段范围查询测试 ===\n");

    // 1. 创建 Engine
    let config = calm::engine::EngineConfig {
        data_dir: std::path::PathBuf::from("/tmp/calm_timestamp_test"),
        persist_check_interval_secs: 60,
        max_concurrent_persists: 4,
        check_after_flush: true,
    };

    // 删除旧数据
    let _ = std::fs::remove_dir_all("/tmp/calm_timestamp_test");

    let engine = Engine::new(config)?;

    // 2. 创建测试表（使用 Timestamp 类型）
    println!("📊 创建测试表...");
    let schema = Schema {
        name: "test_ts".to_string(),
        primary_key: Some("id".to_string()),
        store_source: true,
        fields: vec![
            FieldOption::Keyword {
                name: "id".to_string(),
                index: true,
                is_array: false,
                persist_option: None,
                case_sensitive: true,
            },
            FieldOption::I64 {
                name: "timestamp".to_string(),
                index: true,
            },
            FieldOption::Keyword {
                name: "data".to_string(),
                index: false,
                is_array: false,
                persist_option: None,
                case_sensitive: true,
            },
        ],
        persist_policy: PersistPolicy {
            max_docs_per_segment: 1000,
            max_segment_age: std::time::Duration::from_secs(3600),
        },
    };

    engine
        .create_table(
            "test_ts",
            schema,
            PartitionStrategy::Hash {
                field: "id".to_string(),
                num_partitions: 1,
            },
            1,
        )
        .await?;

    println!("✅ 表创建成功\n");

    // 3. 插入测试数据（10条记录，时间戳从 2025-11-20 00:00 到 2025-11-20 00:09）
    println!("📝 插入测试数据...");
    let base_ts = Utc.with_ymd_and_hms(2025, 11, 20, 0, 0, 0).unwrap();

    let mut ids = Vec::new();
    let mut timestamps = Vec::new();
    let mut data_values = Vec::new();

    for i in 0..10 {
        ids.push(format!("id_{}", i));
        timestamps.push(base_ts.timestamp_millis() + i * 60 * 1000); // 每分钟一条
        data_values.push(format!("data_{}", i));
    }

    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("timestamp", DataType::Int64, false),
        Field::new("data", DataType::Utf8, false),
    ]));

    let batch = RecordBatch::try_new(
        arrow_schema,
        vec![
            Arc::new(StringArray::from(ids)),
            Arc::new(Int64Array::from(timestamps.clone())),
            Arc::new(StringArray::from(data_values)),
        ],
    )?;

    let partition = engine
        .get_partition("test_ts", 0)
        .await
        .ok_or("Partition not found")?;
    partition.upsert(batch)?;

    println!("✅ 插入了 10 条记录\n");
    println!("时间戳范围:");
    println!(
        "   第一条: {} ({})",
        timestamps[0],
        Utc.timestamp_millis_opt(timestamps[0])
            .unwrap()
            .format("%Y-%m-%d %H:%M:%S")
    );
    println!(
        "   最后一条: {} ({})",
        timestamps[9],
        Utc.timestamp_millis_opt(timestamps[9])
            .unwrap()
            .format("%Y-%m-%d %H:%M:%S")
    );
    println!();

    // 4. 测试查询
    println!("🔍 测试查询:\n");

    // 测试 1: COUNT(*) 全部
    println!("测试 1: COUNT(*) 全部");
    let sql1 = "SELECT COUNT(*) as count FROM test_ts";
    println!("   SQL: {}", sql1);
    let result1 = engine.execute_sql(sql1).await?;
    println!("   结果行数: {}", result1.batch.num_rows());
    if result1.batch.num_rows() > 0 {
        let count_array = result1
            .batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        println!("   COUNT(*) = {}", count_array.value(0));
    }
    println!();

    // 测试 2: 范围查询 (前 5 条)
    println!(
        "测试 2: 范围查询 (timestamp >= {} AND timestamp < {})",
        timestamps[0], timestamps[5]
    );
    let sql2 = format!(
        "SELECT COUNT(*) as count FROM test_ts WHERE timestamp >= {} AND timestamp < {}",
        timestamps[0], timestamps[5]
    );
    println!("   SQL: {}", sql2);
    let result2 = engine.execute_sql(&sql2).await?;
    println!("   结果行数: {}", result2.batch.num_rows());
    println!("   matched_docs: {}", result2.matched_docs);
    if result2.batch.num_rows() > 0 {
        let count_array = result2
            .batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        println!("   COUNT(*) = {} (预期: 5)", count_array.value(0));
    }
    println!();

    // 测试 3: 单个值查询
    println!("测试 3: 等值查询 (timestamp = {})", timestamps[0]);
    let sql3 = format!(
        "SELECT COUNT(*) as count FROM test_ts WHERE timestamp = {}",
        timestamps[0]
    );
    println!("   SQL: {}", sql3);
    let result3 = engine.execute_sql(&sql3).await?;
    println!("   结果行数: {}", result3.batch.num_rows());
    println!("   matched_docs: {}", result3.matched_docs);
    if result3.batch.num_rows() > 0 {
        let count_array = result3
            .batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        println!("   COUNT(*) = {} (预期: 1)", count_array.value(0));
    }
    println!();

    // 测试 4: SELECT * 范围查询
    println!("测试 4: SELECT * 范围查询 (前 3 条)");
    let sql4 = format!(
        "SELECT id, timestamp, data FROM test_ts WHERE timestamp >= {} AND timestamp < {} LIMIT 3",
        timestamps[0], timestamps[3]
    );
    println!("   SQL: {}", sql4);
    let result4 = engine.execute_sql(&sql4).await?;
    println!("   结果行数: {} (预期: 3)", result4.batch.num_rows());
    println!("   matched_docs: {}", result4.matched_docs);

    if result4.batch.num_rows() > 0 {
        let id_array = result4
            .batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let ts_array = result4
            .batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        println!("   返回的数据:");
        for i in 0..result4.batch.num_rows() {
            println!(
                "     - id: {}, timestamp: {}",
                id_array.value(i),
                ts_array.value(i)
            );
        }
    }

    println!("\n=== 测试完成 ===");

    Ok(())
}
