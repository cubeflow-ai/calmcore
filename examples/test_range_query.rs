/// 快速查询测试 - 测试已有数据的 range 查询
use calm::engine::Engine;
use chrono::{Duration, TimeZone, Utc};
use datafusion::arrow::array::Int64Array;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    println!("=== Range 查询测试 ===\n");

    // 连接已有数据库
    let config = calm::engine::EngineConfig {
        data_dir: std::path::PathBuf::from("/tmp/calm_large_test"),
        persist_check_interval_secs: 60,
        max_concurrent_persists: 4,
        check_after_flush: true,
    };
    let engine = Engine::new(config)?;

    // 加载已有的表和 partitions
    engine.load_existing_tables().await?;

    println!("已加载表: {:?}\n", engine.list_tables());

    // 时间戳信息
    let base_timestamp = Utc.with_ymd_and_hms(2025, 11, 1, 0, 0, 0).unwrap();
    let ts_2025_11_20 = Utc.with_ymd_and_hms(2025, 11, 20, 0, 0, 0).unwrap();
    let ts_2025_11_21 = Utc.with_ymd_and_hms(2025, 11, 21, 0, 0, 0).unwrap();

    println!("调试信息:");
    println!(
        "   Base (2025-11-01 00:00:00): {}",
        base_timestamp.timestamp_millis()
    );
    println!(
        "   Query start (2025-11-20 00:00:00): {}",
        ts_2025_11_20.timestamp_millis()
    );
    println!(
        "   Query end (2025-11-21 00:00:00): {}",
        ts_2025_11_21.timestamp_millis()
    );

    // 计算实际插入的样本时间戳
    println!("\n实际插入的样本时间戳:");
    let total_records = 10_000_000;
    for i in [0, 3_000_000, 6_000_000, 9_000_000, 9_999_999] {
        let day_offset = (i as f64 / total_records as f64 * 30.0) as i64;
        let timestamp = base_timestamp + Duration::days(day_offset);
        println!(
            "     record[{}]: day_offset={}, ts={} ({})",
            i,
            day_offset,
            timestamp.timestamp_millis(),
            timestamp.format("%Y-%m-%d")
        );
    }

    // 测试查询最小和最大时间戳
    println!("\n测试 1: 查询时间戳的最小值和最大值");
    let sql_minmax = "SELECT MIN(timestamp) as min_ts, MAX(timestamp) as max_ts FROM large_test";
    let result = engine.execute_sql(sql_minmax).await?;

    if result.batch.num_rows() > 0 {
        let min_array = result
            .batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let max_array = result
            .batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let min_ts = min_array.value(0);
        let max_ts = max_array.value(0);

        let min_dt = Utc.timestamp_millis_opt(min_ts).unwrap();
        let max_dt = Utc.timestamp_millis_opt(max_ts).unwrap();

        println!(
            "   MIN(timestamp) = {} ({})",
            min_ts,
            min_dt.format("%Y-%m-%d %H:%M:%S")
        );
        println!(
            "   MAX(timestamp) = {} ({})",
            max_ts,
            max_dt.format("%Y-%m-%d %H:%M:%S")
        );
    }

    // 测试查询 2025-11-20 的数据
    println!("\n测试 2: 查询 2025-11-20 的数据");
    let ts_start = ts_2025_11_20.timestamp_millis();
    let ts_end = ts_2025_11_21.timestamp_millis();

    let sql = format!(
        "SELECT COUNT(*) as count FROM large_test WHERE timestamp >= {} AND timestamp < {}",
        ts_start, ts_end
    );

    println!("   SQL: {}", sql);
    let result = engine.execute_sql(&sql).await?;

    if result.batch.num_rows() > 0 {
        let count_array = result
            .batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let count = count_array.value(0);
        println!("   COUNT(*) = {}", count);
        println!("   matched_docs = {}", result.matched_docs);
    }

    // 测试查询所有数据
    println!("\n测试 3: 查询所有数据的总数");
    let sql_total = "SELECT COUNT(*) as count FROM large_test";
    let result_total = engine.execute_sql(sql_total).await?;

    if result_total.batch.num_rows() > 0 {
        let count_array = result_total
            .batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        println!("   总记录数 = {}", count_array.value(0));
    }

    // 测试查询 11 月 1 日的数据 (应该有数据)
    println!("\n测试 4: 查询 2025-11-01 的数据 (应该有数据)");
    let ts_1101_start = Utc
        .with_ymd_and_hms(2025, 11, 1, 0, 0, 0)
        .unwrap()
        .timestamp_millis();
    let ts_1102_start = Utc
        .with_ymd_and_hms(2025, 11, 2, 0, 0, 0)
        .unwrap()
        .timestamp_millis();

    let sql4 = format!(
        "SELECT COUNT(*) as count FROM large_test WHERE timestamp >= {} AND timestamp < {}",
        ts_1101_start, ts_1102_start
    );

    println!("   SQL: {}", sql4);
    let result4 = engine.execute_sql(&sql4).await?;

    if result4.batch.num_rows() > 0 {
        let count_array = result4
            .batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let count = count_array.value(0);
        println!("   COUNT(*) = {}", count);
        println!("   matched_docs = {}", result4.matched_docs);
    }

    println!("\n=== 测试完成 ===");
    Ok(())
}
