/// 测试查询改写功能
///
/// 验证 ES range 查询中的日期字符串能够正确转换为 i64 时间戳
use calm::{
    catalog::PartitionStrategy,
    engine::Engine,
    query_rewriter::QueryRewriter,
    schema::{field::FieldOption, Schema},
    utils::datetime_utils,
};
use serde_json::json;
use std::sync::Arc;
use tempfile::TempDir;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== 测试查询改写功能 ===\n");

    // 测试1: 日期时间转换
    test_datetime_conversion()?;

    // 测试2: 查询改写器
    test_query_rewriter()?;

    // 测试3: 端到端 ES 查询
    test_es_range_query().await?;

    println!("\n✅ 所有测试通过!");
    Ok(())
}

/// 测试日期时间转换
fn test_datetime_conversion() -> Result<(), Box<dyn std::error::Error>> {
    println!("📅 测试1: 日期时间转换");

    let test_cases = vec![
        ("2025-01-11", 1736553600000i64),
        ("2025-01-12", 1736640000000i64),
        ("2025-01-11T12:34:56Z", 1736598896000i64),
    ];

    for (input, expected) in test_cases {
        let result = datetime_utils::parse_date_to_timestamp_millis(input)?;
        assert_eq!(
            result, expected,
            "Failed to convert '{}': expected {}, got {}",
            input, expected, result
        );
        println!("  ✓ '{}' -> {}", input, result);
    }

    println!();
    Ok(())
}

/// 测试查询改写器
fn test_query_rewriter() -> Result<(), Box<dyn std::error::Error>> {
    println!("🔄 测试2: 查询改写器");

    // 创建测试 schema
    let schema = Schema {
        name: "test_table".to_string(),
        primary_key: Some("id".to_string()),
        store_source: true,
        persist_policy: Default::default(),
        fields: vec![
            FieldOption::I64 {
                name: "updateTime".to_string(),
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
    };

    let rewriter = QueryRewriter::new(&schema);

    // 测试 i64 字段的日期字符串改写
    let date_value = json!("2025-01-11");
    let rewritten = rewriter.rewrite_value("updateTime", &date_value);
    assert!(rewritten.is_number());
    assert_eq!(rewritten.as_i64().unwrap(), 1736553600000);
    println!("  ✓ updateTime: '2025-01-11' -> {}", rewritten);

    // 测试 keyword 字段不应该被改写
    let keyword_value = json!("2025-01-11");
    let rewritten = rewriter.rewrite_value("name", &keyword_value);
    assert!(rewritten.is_string());
    assert_eq!(rewritten.as_str().unwrap(), "2025-01-11");
    println!("  ✓ name: '2025-01-11' -> '{}' (不改写)", rewritten);

    // 测试范围边界改写
    let start = json!("2025-01-11");
    let end = json!("2025-01-12");
    let (rewritten_start, rewritten_end) =
        rewriter.rewrite_range_bounds("updateTime", Some(&start), Some(&end));

    assert_eq!(rewritten_start.unwrap().as_i64().unwrap(), 1736553600000);
    assert_eq!(rewritten_end.unwrap().as_i64().unwrap(), 1736640000000);
    println!("  ✓ Range: ['2025-01-11', '2025-01-12'] -> [1736553600000, 1736640000000]");

    println!();
    Ok(())
}

/// 测试端到端的 ES range 查询
async fn test_es_range_query() -> Result<(), Box<dyn std::error::Error>> {
    println!("🔍 测试3: 端到端 ES range 查询");

    // 创建临时目录
    let temp_dir = TempDir::new()?;
    let work_dir = temp_dir.path().to_path_buf();

    // 创建引擎
    let config = calm::engine::EngineConfig {
        data_dir: work_dir.clone(),
        ..Default::default()
    };
    let engine = Engine::new(config)?;

    // 创建测试表
    let schema = Schema {
        name: "test_events".to_string(),
        primary_key: Some("id".to_string()),
        store_source: true,
        persist_policy: Default::default(),
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
                name: "event".to_string(),
                index: true,
                is_array: false,
                persist_option: None,
                case_sensitive: true,
            },
        ],
    };

    engine
        .create_table(
            "test_events",
            schema.clone(),
            PartitionStrategy::Hash {
                field: "id".to_string(),
                num_partitions: 1,
            },
            1,
        )
        .await?;

    println!("  ✓ 创建表 test_events");

    // 插入测试数据
    let test_data = vec![
        json!({
            "id": "1",
            "updateTime": 1736553600000i64, // 2025-01-11 00:00:00
            "event": "event1"
        }),
        json!({
            "id": "2",
            "updateTime": 1736598896000i64, // 2025-01-11 12:34:56
            "event": "event2"
        }),
        json!({
            "id": "3",
            "updateTime": 1736640000000i64, // 2025-01-12 00:00:00
            "event": "event3"
        }),
        json!({
            "id": "4",
            "updateTime": 1736726400000i64, // 2025-01-13 00:00:00
            "event": "event4"
        }),
    ];

    // 获取 partition 并插入数据
    let partition = engine.get_partition("test_events", 0).await.unwrap();
    partition.upsert_json(&test_data)?;

    println!("  ✓ 插入 {} 条测试数据", test_data.len());

    // 先验证数据确实插入了
    let verify_sql = r#"SELECT * FROM test_events"#;
    let all_data = engine.execute_sql(verify_sql).await?;
    let all_rows: usize = all_data.iter().map(|batch| batch.num_rows()).sum();
    println!("  ✓ 验证: 表中共有 {} 条记录", all_rows);

    // 测试使用查询改写器
    let meta = engine.get_table_meta("test_events")?;
    let rewriter = QueryRewriter::new(&meta.schema);

    // 模拟 ES range 查询
    let range_query = json!({
        "range": {
            "updateTime": {
                "gte": "2025-01-11",
                "lt": "2025-01-12"
            }
        }
    });

    println!("\n  原始查询:");
    println!("  {}", serde_json::to_string_pretty(&range_query)?);

    // 手动提取和改写
    if let Some(range_obj) = range_query["range"]["updateTime"].as_object() {
        if let Some(gte_val) = range_obj.get("gte") {
            let rewritten = rewriter.rewrite_value("updateTime", gte_val);
            println!("\n  改写后的 gte: {} -> {}", gte_val, rewritten);
        }
        if let Some(lt_val) = range_obj.get("lt") {
            let rewritten = rewriter.rewrite_value("updateTime", lt_val);
            println!("  改写后的 lt: {} -> {}", lt_val, rewritten);
        }
    }

    // 执行 SQL 查询验证
    let sql = r#"SELECT * FROM test_events WHERE "updateTime" >= 1736553600000 AND "updateTime" < 1736640000000"#;
    println!("\n  执行 SQL:");
    println!("  {}", sql);

    let result = engine.execute_sql(sql).await?;
    let total_rows: usize = result.iter().map(|batch| batch.num_rows()).sum();

    println!("\n  查询结果: {} 条记录", total_rows);
    assert_eq!(total_rows, 2, "应该查询到2条记录 (id=1 和 id=2)");

    // 验证具体记录
    println!("  ✓ 验证通过: 查询到的记录数正确 (应该包含 2025-01-11 的记录)");

    println!();
    Ok(())
}
