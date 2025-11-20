/// 大数据集测试
///
/// 测试目标：
/// 1. 插入 1000 万条数据
/// 2. 每 10 万条一个 segment (max_docs_per_segment = 100000)
/// 3. 验证 timestamp range 查询是否正确
/// 4. 验证索引过滤是否生效
use calm::catalog::PartitionStrategy;
use calm::engine::Engine;
use calm::schema::field::FieldOption;
use calm::schema::{PersistPolicy, Schema};
use chrono::{Duration, TimeZone, Utc};
use datafusion::arrow::array::{Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use datafusion::arrow::record_batch::RecordBatch;
use std::sync::Arc;
use std::time::Instant;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    println!("=== 大数据集测试 ===\n");

    // 1. 创建 Engine
    println!("📁 创建 Engine...");
    let config = calm::engine::EngineConfig {
        data_dir: std::path::PathBuf::from("/tmp/calm_large_test"),
        persist_check_interval_secs: 60,
        max_concurrent_persists: 4,
        check_after_flush: true,
    };
    let engine = Engine::new(config)?;

    // 2. 创建测试表
    println!("📊 创建测试表 'large_test'...");
    let schema = Schema {
        name: "large_test".to_string(),
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
                index: true, // 重要：timestamp 字段需要索引
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
            max_docs_per_segment: 100_000, // 每 10 万条一个 segment
            max_segment_age: std::time::Duration::from_secs(3600),
        },
    };

    engine
        .create_table(
            "large_test",
            schema,
            PartitionStrategy::Hash {
                field: "id".to_string(),
                num_partitions: 4,
            },
            4,
        )
        .await?;

    println!("✅ 表创建成功\n");

    // 3. 插入数据
    let total_records = 10_000_000;
    let batch_size = 10_000;
    let num_batches = total_records / batch_size;

    println!("📝 开始插入数据:");
    println!("   - 总记录数: {}", total_records);
    println!("   - 批次大小: {}", batch_size);
    println!("   - 批次数量: {}", num_batches);
    println!("   - 每个 segment: 100,000 条\n");

    let start_time = Instant::now();

    // 时间范围: 2025-11-01 到 2025-11-30 (30 天)
    let base_timestamp = Utc.with_ymd_and_hms(2025, 11, 1, 0, 0, 0).unwrap();

    for batch_idx in 0..num_batches {
        let start_id = batch_idx * batch_size;
        let end_id = start_id + batch_size;

        // 生成批次数据
        let mut ids = Vec::with_capacity(batch_size);
        let mut timestamps = Vec::with_capacity(batch_size);
        let mut data_values = Vec::with_capacity(batch_size);

        for i in start_id..end_id {
            ids.push(format!("id_{}", i));

            // 均匀分布在 30 天内
            let day_offset = (i as f64 / total_records as f64 * 30.0) as i64;
            let hour_offset = (i % 24) as i64;
            let minute_offset = (i % 60) as i64;

            let timestamp = base_timestamp
                + Duration::days(day_offset)
                + Duration::hours(hour_offset)
                + Duration::minutes(minute_offset);

            timestamps.push(timestamp.timestamp_millis());
            data_values.push(format!("data_{}", i));
        }

        // 按 partition 分组数据
        let mut partition_data: std::collections::HashMap<
            u64,
            (Vec<String>, Vec<i64>, Vec<String>),
        > = std::collections::HashMap::new();

        for i in 0..(ids.len()) {
            let partition_id = engine.route_partition("large_test", &ids[i])?;

            let entry = partition_data
                .entry(partition_id)
                .or_insert_with(|| (Vec::new(), Vec::new(), Vec::new()));
            entry.0.push(ids[i].clone());
            entry.1.push(timestamps[i]);
            entry.2.push(data_values[i].clone());
        }

        // 写入每个 partition
        for (partition_id, (p_ids, p_timestamps, p_data)) in partition_data {
            let arrow_schema = Arc::new(ArrowSchema::new(vec![
                Field::new("id", DataType::Utf8, false),
                Field::new("timestamp", DataType::Int64, false),
                Field::new("data", DataType::Utf8, false),
            ]));

            let batch = RecordBatch::try_new(
                arrow_schema,
                vec![
                    Arc::new(StringArray::from(p_ids)),
                    Arc::new(Int64Array::from(p_timestamps)),
                    Arc::new(StringArray::from(p_data)),
                ],
            )?;

            let partition = engine
                .get_partition("large_test", partition_id)
                .await
                .ok_or("Partition not found")?;

            partition.upsert(batch)?;
        }

        // 进度显示
        if (batch_idx + 1) % 10 == 0 {
            let progress = (batch_idx + 1) as f64 / num_batches as f64 * 100.0;
            let elapsed = start_time.elapsed().as_secs_f64();
            let records_inserted = (batch_idx + 1) * batch_size;
            let rate = records_inserted as f64 / elapsed;

            println!(
                "   进度: {:.1}% ({}/{} 批次, {} 条记录, {:.0} 条/秒)",
                progress,
                batch_idx + 1,
                num_batches,
                records_inserted,
                rate
            );
        }
    }

    let insert_duration = start_time.elapsed();
    println!(
        "\n✅ 数据插入完成! 耗时: {:.2} 秒, 平均: {:.0} 条/秒\n",
        insert_duration.as_secs_f64(),
        total_records as f64 / insert_duration.as_secs_f64()
    );

    // 4. 触发持久化
    println!("💾 触发持久化...");
    let persist_start = Instant::now();

    for partition_id in 0..4 {
        let partition = engine
            .get_partition("large_test", partition_id)
            .await
            .ok_or("Partition not found")?;

        partition.persist_all()?;
    }

    println!(
        "✅ 持久化完成! 耗时: {:.2} 秒\n",
        persist_start.elapsed().as_secs_f64()
    );

    // 5. 查询测试
    println!("🔍 开始查询测试:\n");

    // 调试: 显示时间戳范围
    println!("调试信息:");
    println!(
        "   Base timestamp (2025-11-01 00:00:00): {}",
        base_timestamp.timestamp_millis()
    );
    let ts_2025_11_20 = Utc.with_ymd_and_hms(2025, 11, 20, 0, 0, 0).unwrap();
    let ts_2025_11_21 = Utc.with_ymd_and_hms(2025, 11, 21, 0, 0, 0).unwrap();
    let ts_2025_11_30 = Utc.with_ymd_and_hms(2025, 11, 30, 0, 0, 0).unwrap();
    println!(
        "   2025-11-20 00:00:00 timestamp: {}",
        ts_2025_11_20.timestamp_millis()
    );
    println!(
        "   2025-11-21 00:00:00 timestamp: {}",
        ts_2025_11_21.timestamp_millis()
    );
    println!(
        "   2025-11-30 00:00:00 timestamp: {}",
        ts_2025_11_30.timestamp_millis()
    );

    // 显示几条实际插入的数据
    println!("\n   样本数据时间戳:");
    for i in [0, 1_000_000, 5_000_000, 9_000_000, 9_999_999] {
        let day_offset = (i as f64 / total_records as f64 * 30.0) as i64;
        let timestamp = base_timestamp + Duration::days(day_offset);
        println!(
            "     record[{}]: day_offset={}, timestamp={} ({})",
            i,
            day_offset,
            timestamp.timestamp_millis(),
            timestamp.format("%Y-%m-%d %H:%M:%S")
        );
    }
    println!();

    // 测试 1: 查询单天数据 (2025-11-20)
    println!("测试 1: 查询 2025-11-20 的数据");
    let query1_start = Instant::now();

    let ts_start = Utc
        .with_ymd_and_hms(2025, 11, 20, 0, 0, 0)
        .unwrap()
        .timestamp_millis();
    let ts_end = Utc
        .with_ymd_and_hms(2025, 11, 21, 0, 0, 0)
        .unwrap()
        .timestamp_millis();

    let sql1 = format!(
        "SELECT COUNT(*) as count FROM large_test WHERE timestamp >= {} AND timestamp < {}",
        ts_start, ts_end
    );

    println!("   SQL: {}", sql1);

    let result1 = engine.execute_sql(&sql1).await?;
    let query1_duration = query1_start.elapsed();

    println!("   结果: {} 条记录", result1.batch.num_rows());
    println!("   匹配文档数: {}", result1.matched_docs);
    println!("   查询耗时: {:.3} 秒", query1_duration.as_secs_f64());

    if result1.batch.num_rows() > 0 {
        let count_array = result1
            .batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        println!("   COUNT(*) = {}", count_array.value(0));
    }

    // 计算预期结果
    let expected_count = total_records / 30; // 均匀分布在 30 天
    println!("   预期约: {} 条记录\n", expected_count);

    // 测试 2: 查询多天数据 (2025-11-15 到 2025-11-25)
    println!("测试 2: 查询 2025-11-15 到 2025-11-25 的数据 (10 天)");
    let query2_start = Instant::now();

    let ts_start2 = Utc
        .with_ymd_and_hms(2025, 11, 15, 0, 0, 0)
        .unwrap()
        .timestamp_millis();
    let ts_end2 = Utc
        .with_ymd_and_hms(2025, 11, 25, 0, 0, 0)
        .unwrap()
        .timestamp_millis();

    let sql2 = format!(
        "SELECT COUNT(*) as count FROM large_test WHERE timestamp >= {} AND timestamp < {}",
        ts_start2, ts_end2
    );

    println!("   SQL: {}", sql2);

    let result2 = engine.execute_sql(&sql2).await?;
    let query2_duration = query2_start.elapsed();

    println!("   结果: {} 条记录", result2.batch.num_rows());
    println!("   匹配文档数: {}", result2.matched_docs);
    println!("   查询耗时: {:.3} 秒", query2_duration.as_secs_f64());

    if result2.batch.num_rows() > 0 {
        let count_array = result2
            .batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        println!("   COUNT(*) = {}", count_array.value(0));
    }

    let expected_count2 = total_records * 10 / 30;
    println!("   预期约: {} 条记录\n", expected_count2);

    // 测试 3: 查询全部数据
    println!("测试 3: 查询全部数据 (验证总数)");
    let query3_start = Instant::now();

    let sql3 = "SELECT COUNT(*) as count FROM large_test";
    println!("   SQL: {}", sql3);

    let result3 = engine.execute_sql(sql3).await?;
    let query3_duration = query3_start.elapsed();

    println!("   结果: {} 条记录", result3.batch.num_rows());
    println!("   匹配文档数: {}", result3.matched_docs);
    println!("   查询耗时: {:.3} 秒", query3_duration.as_secs_f64());

    if result3.batch.num_rows() > 0 {
        let count_array = result3
            .batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        println!("   COUNT(*) = {}", count_array.value(0));
    }

    println!("   预期: {} 条记录\n", total_records);

    // 测试 4: 带 LIMIT 的 range 查询
    println!("测试 4: 查询 2025-11-20 的数据 (带 LIMIT 10)");
    let query4_start = Instant::now();

    let sql4 = format!(
        "SELECT id, timestamp, data FROM large_test WHERE timestamp >= {} AND timestamp < {} LIMIT 10",
        ts_start, ts_end
    );

    println!("   SQL: {}", sql4);

    let result4 = engine.execute_sql(&sql4).await?;
    let query4_duration = query4_start.elapsed();

    println!("   结果: {} 条记录", result4.batch.num_rows());
    println!("   匹配文档数: {}", result4.matched_docs);
    println!("   查询耗时: {:.3} 秒", query4_duration.as_secs_f64());

    // 显示前几条数据
    if result4.batch.num_rows() > 0 {
        println!("   前 3 条数据:");
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

        for i in 0..result4.batch.num_rows().min(3) {
            let id = id_array.value(i);
            let ts = ts_array.value(i);
            let dt = Utc.timestamp_millis_opt(ts).unwrap();
            println!(
                "     - id: {}, timestamp: {} ({})",
                id,
                ts,
                dt.format("%Y-%m-%d %H:%M:%S")
            );
        }
    }

    println!("\n=== 测试完成 ===");

    // 统计信息
    println!("\n📊 统计信息:");
    println!("   总插入时间: {:.2} 秒", insert_duration.as_secs_f64());
    println!(
        "   持久化时间: {:.2} 秒",
        persist_start.elapsed().as_secs_f64()
    );
    println!(
        "   总测试时间: {:.2} 秒",
        start_time.elapsed().as_secs_f64()
    );

    Ok(())
}
