//! DatetimeRange 分区 + _partition 过滤查询完整示例
//!
//! 演示:
//! 1. 创建带 DatetimeRange 分区的表
//! 2. 插入不同时间的数据
//! 3. 使用 _partition 条件过滤查询
//! 4. 验证分区裁剪效果
//!
//! 运行: cargo run --example partition_filter_demo

use calm::catalog::schema::field::FieldOption;
use calm::catalog::schema::{PersistPolicy, Schema};
use calm::catalog::{PartitionStrategy, TimeGranularity};
use calm::engine::{Engine, EngineConfig};
use datafusion::arrow::array::{Int64Array, StringArray, TimestampMillisecondArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use futures::StreamExt;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化日志
    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .init();

    println!("🚀 DatetimeRange 分区 + _partition 过滤查询示例");
    println!("{}", "=".repeat(60));

    // 1. 创建 Engine
    let config = EngineConfig {
        data_dir: "/tmp/calm_partition_filter_demo".into(),
        ..Default::default()
    };

    // 清理旧数据
    if std::path::Path::new(&config.data_dir).exists() {
        std::fs::remove_dir_all(&config.data_dir)?;
    }

    let engine = Arc::new(Engine::new(config)?);
    println!("✅ Engine 创建成功");

    // 2. 定义表 Schema (事件日志表)
    let fields = vec![
        FieldOption::Keyword {
            name: "event_id".to_string(),
            index: true,
            is_array: false,
            persist_option: None,
            case_sensitive: true,
            description: None,
            default_value: None,
            nullable: false,
        },
        FieldOption::Timestamp {
            name: "event_time".to_string(),
            index: true,
            format: None,
            description: None,
            default_value: None,
            nullable: false,
        },
        FieldOption::Keyword {
            name: "event_type".to_string(),
            index: true,
            is_array: false,
            persist_option: None,
            case_sensitive: false,
            description: None,
            default_value: None,
            nullable: false,
        },
        FieldOption::I64 {
            name: "user_id".to_string(),
            index: true,
            description: None,
            default_value: None,
            nullable: false,
        },
        FieldOption::Keyword {
            name: "message".to_string(),
            index: true,
            is_array: false,
            persist_option: None,
            case_sensitive: false,
            description: None,
            default_value: None,
            nullable: false,
        },
    ];

    let schema = Schema::new(
        "event_logs".to_string(),
        Some("event_id".to_string()), // 主键
        false,                        // store_source
        fields,
        PersistPolicy {
            max_docs_per_segment: 10000,
            max_segment_age: std::time::Duration::from_secs(300),
        },
        Some("Event logs table with DatetimeRange partition".to_string()),
    );

    // 3. 创建表，使用 DatetimeRange 分区策略（按天分区）
    let partition_strategy = PartitionStrategy::DatetimeRange {
        field: "event_time".to_string(),
        granularity: TimeGranularity::Day, // 按天分区
        timezone: Some("UTC".to_string()), // UTC 时区
        parallelism: None,                 // 不启用并行度
    };

    engine
        .create_table("event_logs", schema.clone(), partition_strategy, 0) // DatetimeRange 不需要 num_partitions
        .await?;

    println!("✅ 表 'event_logs' 创建成功 (DatetimeRange 分区，按天，UTC 时区)");
    println!();

    // 4. 插入不同日期的测试数据
    println!("📝 插入测试数据...");

    // 准备 Arrow Schema
    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        Field::new("event_id", DataType::Utf8, false),
        Field::new(
            "event_time",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("event_type", DataType::Utf8, false),
        Field::new("user_id", DataType::Int64, false),
        Field::new("message", DataType::Utf8, false),
    ]));

    // 2024-01-01 的数据 (10 条)
    let batch_20240101 = create_batch(
        arrow_schema.clone(),
        1,
        "2024-01-01",
        1704067200000, // 2024-01-01 00:00:00 UTC
        10,
    );
    engine
        .insert_batch("event_logs", batch_20240101, None)
        .await?;
    println!("  ✓ 插入 10 条 2024-01-01 数据 -> partition_20240101");

    // 2024-01-02 的数据 (15 条)
    let batch_20240102 = create_batch(
        arrow_schema.clone(),
        11,
        "2024-01-02",
        1704153600000, // 2024-01-02 00:00:00 UTC
        15,
    );
    engine
        .insert_batch("event_logs", batch_20240102, None)
        .await?;
    println!("  ✓ 插入 15 条 2024-01-02 数据 -> partition_20240102");

    // 2024-01-03 的数据 (20 条)
    let batch_20240103 = create_batch(
        arrow_schema.clone(),
        26,
        "2024-01-03",
        1704240000000, // 2024-01-03 00:00:00 UTC
        20,
    );
    engine
        .insert_batch("event_logs", batch_20240103, None)
        .await?;
    println!("  ✓ 插入 20 条 2024-01-03 数据 -> partition_20240103");

    // 2024-02-01 的数据 (12 条)
    let batch_20240201 = create_batch(
        arrow_schema.clone(),
        46,
        "2024-02-01",
        1706745600000, // 2024-02-01 00:00:00 UTC
        12,
    );
    engine
        .insert_batch("event_logs", batch_20240201, None)
        .await?;
    println!("  ✓ 插入 12 条 2024-02-01 数据 -> partition_20240201");

    println!();
    println!("📊 总计插入: 57 条记录，分布在 4 个分区");
    println!();

    // 等待持久化
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    // 5. 查看所有分区
    let all_partitions = engine.list_partitions("event_logs").await;
    println!("📂 当前分区列表: {:?}", all_partitions);
    println!("   分区数量: {}", all_partitions.len());
    println!();

    // 6. 执行各种 _partition 过滤查询
    println!("🔍 开始测试 _partition 过滤查询");
    println!("{}", "=".repeat(60));

    // 测试 1: 精确匹配单个分区
    println!();
    println!("【测试 1】精确匹配: _partition = '20240101'");
    let sql1 = "SELECT COUNT(*) as count FROM event_logs WHERE _partition = '20240101'";
    let result1 = execute_and_collect(&engine, sql1).await?;
    println!("  SQL: {}", sql1);
    println!("  结果: {:?}", result1);
    println!("  预期: count = 10");
    verify_count(&result1, 10)?;

    // 测试 2: IN 查询多个分区
    println!();
    println!("【测试 2】IN 查询: _partition IN ('20240101', '20240102')");
    let sql2 =
        "SELECT COUNT(*) as count FROM event_logs WHERE _partition IN ('20240101', '20240102')";
    let result2 = execute_and_collect(&engine, sql2).await?;
    println!("  SQL: {}", sql2);
    println!("  结果: {:?}", result2);
    println!("  预期: count = 25 (10 + 15)");
    verify_count(&result2, 25)?;

    // 测试 3: LIKE 前缀匹配
    println!();
    println!("【测试 3】LIKE 模式: _partition LIKE '202401%'");
    let sql3 = "SELECT COUNT(*) as count FROM event_logs WHERE _partition LIKE '202401%'";
    let result3 = execute_and_collect(&engine, sql3).await?;
    println!("  SQL: {}", sql3);
    println!("  结果: {:?}", result3);
    println!("  预期: count = 45 (10 + 15 + 20)");
    verify_count(&result3, 45)?;

    // 测试 4: 组合条件查询
    println!();
    println!("【测试 4】组合条件: _partition = '20240102' AND event_type = 'login'");
    let sql4 = "SELECT COUNT(*) as count FROM event_logs WHERE _partition = '20240102' AND event_type = 'login'";
    let result4 = execute_and_collect(&engine, sql4).await?;
    println!("  SQL: {}", sql4);
    println!("  结果: {:?}", result4);
    println!("  预期: count = 5 (20240102 有 15 条，其中 5 条是 login)");
    verify_count(&result4, 5)?;

    // 测试 5: 无 _partition 条件（扫描所有分区）
    println!();
    println!("【测试 5】无过滤条件: 扫描所有分区");
    let sql5 = "SELECT COUNT(*) as count FROM event_logs";
    let result5 = execute_and_collect(&engine, sql5).await?;
    println!("  SQL: {}", sql5);
    println!("  结果: {:?}", result5);
    println!("  预期: count = 57 (所有数据)");
    verify_count(&result5, 57)?;

    // 测试 6: 查询具体字段
    println!();
    println!("【测试 6】查询具体数据: _partition = '20240101' LIMIT 3");
    let sql6 =
        "SELECT event_id, event_type, user_id FROM event_logs WHERE _partition = '20240101' ORDER BY event_id LIMIT 3";
    let result6 = execute_and_collect(&engine, sql6).await?;
    println!("  SQL: {}", sql6);
    println!("  结果:");
    print_batch(&result6);

    println!();
    println!("{}", "=".repeat(60));
    println!("✅ 所有测试通过！");
    println!();
    println!("📋 总结:");
    println!("  • DatetimeRange 分区自动按日期创建分区");
    println!("  • _partition 条件自动提取和改写");
    println!("  • 分区裁剪显著减少扫描数据量");
    println!("  • 支持 =, IN, LIKE 等多种过滤方式");

    Ok(())
}

/// 执行 SQL 并收集结果
async fn execute_and_collect(
    engine: &Arc<Engine>,
    sql: &str,
) -> Result<RecordBatch, Box<dyn std::error::Error>> {
    let mut stream = engine.execute_sql_stream(sql).await?;

    // 收集所有 batch
    let mut batches = Vec::new();
    while let Some(batch_result) = stream.next().await {
        batches.push(batch_result?);
    }

    if batches.is_empty() {
        return Err("No data returned".into());
    }

    // 如果有多个 batch，合并它们
    if batches.len() == 1 {
        Ok(batches.into_iter().next().unwrap())
    } else {
        // 使用 datafusion 的 concat_batches
        use datafusion::arrow::compute::concat_batches;
        Ok(concat_batches(&batches[0].schema(), &batches)?)
    }
}

/// 创建测试数据批次
fn create_batch(
    schema: Arc<ArrowSchema>,
    start_id: u64,
    date_str: &str,
    base_timestamp_ms: i64,
    count: usize,
) -> RecordBatch {
    let event_ids: Vec<u64> = (start_id..start_id + count as u64).collect();

    // 时间戳: 在同一天内均匀分布
    let timestamps: Vec<i64> = (0..count)
        .map(|i| base_timestamp_ms + (i as i64 * 3600000)) // 每小时一条
        .collect();

    // 事件类型: 循环 login, logout, click
    let event_types: Vec<String> = (0..count)
        .map(|i| match i % 3 {
            0 => "login".to_string(),
            1 => "logout".to_string(),
            _ => "click".to_string(),
        })
        .collect();

    // 用户 ID
    let user_ids: Vec<i64> = (0..count).map(|i| (i % 10) as i64 + 1).collect();

    // 消息
    let messages: Vec<String> = (0..count)
        .map(|i| format!("Event {} on {}", event_ids[i], date_str))
        .collect();

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(
                event_ids
                    .iter()
                    .map(|&id| id.to_string())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(TimestampMillisecondArray::from(timestamps)),
            Arc::new(StringArray::from(event_types)),
            Arc::new(Int64Array::from(user_ids)),
            Arc::new(StringArray::from(messages)),
        ],
    )
    .unwrap()
}

/// 验证 COUNT(*) 查询结果
fn verify_count(batch: &RecordBatch, expected: i64) -> Result<(), Box<dyn std::error::Error>> {
    if batch.num_rows() == 0 {
        return Err(format!("Expected count = {}, but got empty result", expected).into());
    }

    let count_array = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or("Failed to downcast to Int64Array")?;

    let actual = count_array.value(0);
    if actual != expected {
        return Err(format!("Expected count = {}, but got {}", expected, actual).into());
    }

    println!("  ✓ 验证通过: count = {}", actual);
    Ok(())
}

/// 打印 RecordBatch 内容
fn print_batch(batch: &RecordBatch) {
    for i in 0..batch.num_rows() {
        print!("    Row {}: ", i);
        for (col_idx, column) in batch.columns().iter().enumerate() {
            if col_idx > 0 {
                print!(", ");
            }
            print!(
                "{} = {}",
                batch.schema().field(col_idx).name(),
                column
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .map(|arr| format!("'{}'", arr.value(i)))
                    .or_else(|| column
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .map(|arr| arr.value(i).to_string()))
                    .unwrap_or_else(|| "?".to_string())
            );
        }
        println!();
    }
}
