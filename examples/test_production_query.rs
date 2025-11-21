use calm::catalog::PartitionStrategy;
use calm::engine::Engine;
use calm::schema::field::FieldOption;
use calm::schema::{PersistPolicy, Schema};
use chrono::{Local, TimeZone};
use datafusion::arrow::array::{Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::Result;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    println!("=== 模拟生产环境查询 ===\n");

    // 创建引擎和表
    let mut engine = CalmEngine::default();
    engine.change_data_dir("/tmp/calm_production_test");

    println!("📊 创建表结构...");

    let schema = r#"
    CREATE TABLE label_event_v1 (
        id Int64,
        eventType Keyword,
        updateTime Int64
    ) SORTED BY updateTime
    "#;

    engine.execute(schema).await?;
    println!("✅ 表创建成功\n");

    // 插入测试数据 - 2025年11月的数据
    println!("📝 插入测试数据（2025年11月）...");

    // 2025-11-20 00:00:00 UTC
    let nov_20 = Local.with_ymd_and_hms(2025, 11, 20, 0, 0, 0).unwrap();
    let base_ts = nov_20.timestamp_millis();

    let mut insert_sql = String::from("INSERT INTO label_event_v1 VALUES\n");
    for i in 0..100 {
        let ts = base_ts + (i * 3600 * 1000); // 每小时一条
        if i > 0 {
            insert_sql.push_str(",\n");
        }
        insert_sql.push_str(&format!("({}, 'event_type_{}', {})", i, i % 10, ts));
    }

    engine.execute(&insert_sql).await?;
    println!("✅ 插入了 100 条记录\n");

    // 等待数据刷盘
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    // 测试 1: 使用数值时间戳查询（标准方式）
    println!("测试 1: 数值时间戳查询");
    let nov_20_start = base_ts;
    let nov_21_start = base_ts + (24 * 3600 * 1000);

    let query1 = format!(
        "SELECT COUNT(*) as cnt FROM label_event_v1 WHERE updateTime >= {} AND updateTime < {}",
        nov_20_start, nov_21_start
    );
    println!("   SQL: {}", query1);

    let df1 = engine.query(&query1).await?;
    let batches1 = df1.collect().await?;

    let count1 = if let Some(batch) = batches1.first() {
        if let Some(col) = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
        {
            col.value(0)
        } else {
            0
        }
    } else {
        0
    };

    println!("   结果: {} 条记录", count1);
    if count1 >= 24 {
        println!("   ✅ 成功!");
    } else {
        println!("   ❌ 失败! 预期至少 24 条记录");
    }
    println!();

    // 测试 2: 使用字符串日期查询（你的场景）
    println!("测试 2: 字符串日期查询（生产场景）");
    let query2 = "SELECT COUNT(*) as cnt FROM label_event_v1 WHERE updateTime >= '2025-11-20' AND updateTime < '2025-11-21'";
    println!("   SQL: {}", query2);

    let df2 = engine.query(query2).await?;
    let batches2 = df2.collect().await?;

    let count2 = if let Some(batch) = batches2.first() {
        if let Some(col) = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
        {
            col.value(0)
        } else {
            0
        }
    } else {
        0
    };

    println!("   结果: {} 条记录", count2);
    if count2 >= 24 {
        println!("   ✅ 成功!");
    } else {
        println!("   ❌ 失败! 预期至少 24 条记录");
        println!("   ⚠️  这是你报告的问题！");
    }
    println!();

    // 测试 3: 完整的 SELECT * 查询
    println!("测试 3: SELECT * 查询（带 ORDER BY LIMIT）");
    let query3 = "SELECT * FROM label_event_v1 WHERE updateTime >= '2025-11-20' AND updateTime < '2025-11-21' ORDER BY updateTime DESC LIMIT 10";
    println!("   SQL: {}", query3);

    let df3 = engine.query(query3).await?;
    let batches3 = df3.collect().await?;

    let mut total_rows = 0;
    for batch in &batches3 {
        total_rows += batch.num_rows();
    }

    println!("   结果: {} 条记录", total_rows);
    if total_rows == 10 {
        println!("   ✅ 成功!");
    } else {
        println!("   ❌ 失败! 预期 10 条记录");
    }
    println!();

    // 测试 4: 检查实际返回的数据
    println!("测试 4: 验证返回的数据内容");
    let query4 = "SELECT id, eventType, updateTime FROM label_event_v1 WHERE updateTime >= '2025-11-20' ORDER BY updateTime ASC LIMIT 3";
    println!("   SQL: {}", query4);

    let df4 = engine.query(query4).await?;
    let batches4 = df4.collect().await?;

    if let Some(batch) = batches4.first() {
        println!("   返回数据:");
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        let types = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        let times = batch
            .column(2)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();

        for i in 0..batch.num_rows() {
            let ts = times.value(i);
            let dt = Local.timestamp_millis_opt(ts).unwrap();
            println!(
                "   - id={}, type={}, time={} ({})",
                ids.value(i),
                types.value(i),
                ts,
                dt.format("%Y-%m-%d %H:%M:%S")
            );
        }
        println!("   ✅ 数据格式正确!");
    }
    println!();

    // 测试 5: 边界测试
    println!("测试 5: 边界条件测试");
    let query5 =
        "SELECT COUNT(*) as cnt FROM label_event_v1 WHERE updateTime >= '2025-11-20 00:00:00'";
    println!("   SQL: {}", query5);

    let df5 = engine.query(query5).await?;
    let batches5 = df5.collect().await?;

    let count5 = if let Some(batch) = batches5.first() {
        if let Some(col) = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
        {
            col.value(0)
        } else {
            0
        }
    } else {
        0
    };

    println!("   结果: {} 条记录", count5);
    if count5 > 0 {
        println!("   ✅ 单边界查询成功!");
    } else {
        println!("   ❌ 失败! 应该有数据");
    }

    println!("\n=== 测试完成 ===");
    println!("\n如果测试 2 失败，说明字符串日期查询有问题");
    println!("如果测试 1 成功但测试 2 失败，说明是类型转换问题");

    Ok(())
}
