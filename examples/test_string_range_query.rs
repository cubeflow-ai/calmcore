/// 测试 String 类型的范围查询
use calm::catalog::PartitionStrategy;
use calm::engine::{Engine, EngineConfig};
use calm::schema::field::FieldOption;
use calm::schema::{PersistPolicy, Schema};
use datafusion::arrow::array::{Int64Array, StringArray, UInt32Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use datafusion::arrow::record_batch::RecordBatch;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    println!("🚀 测试 String 类型范围查询");
    println!("{}", "=".repeat(60));

    // 1. 创建引擎
    let config = EngineConfig {
        data_dir: std::path::PathBuf::from("./test_data/string_range_test"),
        persist_check_interval_secs: 60,
        max_concurrent_persists: 4,
        check_after_flush: true,
    };
    let engine = Engine::new(config)?;

    // 2. 创建表结构
    let schema = Schema {
        name: "users".to_string(),
        primary_key: Some("id".to_string()),
        store_source: true,
        fields: vec![
            FieldOption::U32 {
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
            FieldOption::Keyword {
                name: "city".to_string(),
                index: true,
                is_array: false,
                persist_option: None,
                case_sensitive: true,
            },
        ],
        persist_policy: PersistPolicy {
            max_docs_per_segment: 10_000,
            max_segment_age: std::time::Duration::from_secs(3600),
        },
    };

    engine
        .create_table(
            "users",
            schema,
            PartitionStrategy::Hash {
                field: "id".to_string(),
                num_partitions: 1,
            },
            1,
        )
        .await?;
    println!("✅ 表创建成功");

    // 3. 插入测试数据
    let test_data = vec![
        (1u32, "Alice", "Amsterdam"),
        (2u32, "Bob", "Berlin"),
        (3u32, "Charlie", "Chicago"),
        (4u32, "David", "Denver"),
        (5u32, "Emma", "Edinburgh"),
        (6u32, "Frank", "Frankfurt"),
        (7u32, "Grace", "Geneva"),
        (8u32, "Henry", "Houston"),
        (9u32, "Iris", "Istanbul"),
        (10u32, "Jack", "Jakarta"),
    ];

    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::UInt32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("city", DataType::Utf8, false),
    ]));

    let ids: Vec<u32> = test_data.iter().map(|(id, _, _)| *id).collect();
    let names: Vec<&str> = test_data.iter().map(|(_, name, _)| *name).collect();
    let cities: Vec<&str> = test_data.iter().map(|(_, _, city)| *city).collect();

    let batch = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![
            Arc::new(UInt32Array::from(ids)),
            Arc::new(StringArray::from(names)),
            Arc::new(StringArray::from(cities)),
        ],
    )?;

    // 获取partition并写入数据
    // 对于Hash分区，需要根据主键路由到正确的partition
    let partition_id = engine.route_partition("users", "1")?;
    let partition = engine
        .get_partition("users", &partition_id)
        .await
        .ok_or("Partition not found")?;
    partition.upsert(batch)?;

    println!("✅ 插入 {} 条测试数据\n", test_data.len());

    // 4. 测试各种范围查询
    let test_cases = vec![
        (
            "名字从 A 到 D",
            "SELECT COUNT(*) FROM users WHERE name >= 'A' AND name < 'E'",
        ),
        (
            "名字从 C 到 G",
            "SELECT COUNT(*) FROM users WHERE name >= 'C' AND name < 'H'",
        ),
        ("名字 >= F", "SELECT COUNT(*) FROM users WHERE name >= 'F'"),
        ("名字 < D", "SELECT COUNT(*) FROM users WHERE name < 'D'"),
        (
            "城市从 B 到 E",
            "SELECT COUNT(*) FROM users WHERE city >= 'B' AND city < 'F'",
        ),
        ("城市 >= G", "SELECT COUNT(*) FROM users WHERE city >= 'G'"),
        (
            "BETWEEN 语法",
            "SELECT COUNT(*) FROM users WHERE name BETWEEN 'C' AND 'G'",
        ),
    ];

    for (desc, sql) in test_cases {
        println!("📊 测试: {}", desc);
        println!("   SQL: {}", sql);

        let start = std::time::Instant::now();
        let result = engine.execute_sql(sql).await?;
        let elapsed = start.elapsed();

        if result.batch.num_rows() > 0 {
            let count_array = result
                .batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let count = count_array.value(0);
            println!("   ⏱️  查询耗时: {:?}", elapsed);
            println!("   📝 结果: COUNT(*) = {}", count);
            println!("   🎯 匹配文档数: {}\n", result.matched_docs);
        }
    }

    // 5. 详细查询：显示具体结果
    println!("📋 详细查询：名字从 C 到 G");
    let detail_sql = "SELECT id, name FROM users WHERE name >= 'C' AND name < 'H' ORDER BY id";
    println!("   SQL: {}", detail_sql);

    let result = engine.execute_sql(detail_sql).await?;
    println!("   📝 结果:");

    if result.batch.num_rows() > 0 {
        let id_col = result
            .batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap();
        let name_col = result
            .batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        for row_idx in 0..result.batch.num_rows() {
            println!(
                "      id={}, name={}",
                id_col.value(row_idx),
                name_col.value(row_idx)
            );
        }
    }

    println!("\n{}", "=".repeat(60));
    println!("✅ String 范围查询测试完成！");

    Ok(())
}
