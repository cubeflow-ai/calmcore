/// 测试字符串格式的日期查询
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

    println!("=== 测试字符串日期查询 ===\n");

    let config = calm::engine::EngineConfig {
        data_dir: std::path::PathBuf::from("/tmp/calm_string_date_test"),
        persist_check_interval_secs: 60,
        max_concurrent_persists: 4,
        check_after_flush: true,
    };

    let _ = std::fs::remove_dir_all("/tmp/calm_string_date_test");
    let engine = Engine::new(config)?;

    // 创建表 - 使用 I64 类型存储时间戳
    println!("📊 创建测试表（I64 类型）...");
    let schema = Schema {
        name: "test_table".to_string(),
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
                name: "updateTime".to_string(),
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
            "test_table",
            schema,
            PartitionStrategy::Hash {
                field: "id".to_string(),
                num_partitions: 1,
            },
            1,
        )
        .await?;

    // 插入测试数据
    println!("📝 插入测试数据...");
    let base_ts = Utc.with_ymd_and_hms(2025, 11, 20, 0, 0, 0).unwrap();

    let mut ids = Vec::new();
    let mut timestamps = Vec::new();
    let mut data_values = Vec::new();

    for i in 0..5 {
        ids.push(format!("id_{}", i));
        timestamps.push(base_ts.timestamp_millis() + i * 3600 * 1000); // 每小时一条
        data_values.push(format!("data_{}", i));
    }

    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("updateTime", DataType::Int64, false),
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
        .get_partition("test_table", 0)
        .await
        .ok_or("Partition not found")?;
    partition.upsert(batch)?;

    println!("✅ 插入了 5 条记录\n");

    // 测试 1: 使用数值时间戳查询（正确方式）
    println!("测试 1: 使用数值时间戳查询");
    let sql1 = format!(
        "SELECT * FROM test_table WHERE updateTime >= {} AND updateTime < {} ORDER BY updateTime DESC LIMIT 10",
        base_ts.timestamp_millis(),
        base_ts.timestamp_millis() + 86400000  // +1 天
    );
    println!("   SQL: {}", sql1);

    match engine.execute_sql(&sql1).await {
        Ok(result) => {
            println!("   ✅ 成功! 返回 {} 条记录", result.batch.num_rows());
        }
        Err(e) => {
            println!("   ❌ 失败: {}", e);
        }
    }
    println!();

    // 测试 2: 使用字符串日期查询（可能失败）
    println!("测试 2: 使用字符串日期查询");
    let sql2 = "SELECT * FROM test_table WHERE updateTime >= '2025-11-20' AND updateTime < '2025-11-21' ORDER BY updateTime DESC LIMIT 10";
    println!("   SQL: {}", sql2);

    match engine.execute_sql(sql2).await {
        Ok(result) => {
            println!("   ✅ 成功! 返回 {} 条记录", result.batch.num_rows());
        }
        Err(e) => {
            println!("   ❌ 失败: {}", e);
        }
    }
    println!();

    // 测试 3: 使用 CAST 转换
    println!("测试 3: 使用 CAST 转换字符串日期");
    let sql3 = "SELECT * FROM test_table WHERE updateTime >= CAST('2025-11-20' AS BIGINT) ORDER BY updateTime DESC LIMIT 10";
    println!("   SQL: {}", sql3);

    match engine.execute_sql(sql3).await {
        Ok(result) => {
            println!("   ✅ 成功! 返回 {} 条记录", result.batch.num_rows());
        }
        Err(e) => {
            println!("   ❌ 失败: {}", e);
        }
    }

    println!("\n=== 测试完成 ===");
    println!("\n💡 建议:");
    println!("   1. 如果字段是 I64 类型，使用数值时间戳查询");
    println!("   2. 如果需要字符串日期格式，考虑:");
    println!("      - 在应用层将字符串转换为时间戳");
    println!("      - 或使用 Timestamp 类型字段（但注意类型兼容性）");

    Ok(())
}
