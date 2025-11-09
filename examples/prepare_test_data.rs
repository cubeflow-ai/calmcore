use calm::{
    partition::Partition,
    schema::{field::FieldOption, Schema},
};
use datafusion::arrow::{
    array::{Int64Array, RecordBatch, StringArray},
    datatypes::{DataType, Field, Schema as ArrowSchema},
};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::mpsc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 准备 MySQL 演示测试数据...\n");

    // 创建 Schema
    let schema = Schema {
        name: "test_data".to_string(),
        primary_key: None,
        store_source: false,
        fields: vec![
            FieldOption::I64 {
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
            FieldOption::I64 {
                name: "age".to_string(),
                index: true,
            },
            FieldOption::Keyword {
                name: "city".to_string(),
                index: true,
                is_array: false,
                persist_option: None,
                case_sensitive: true,
            },
        ],
        persist_policy: Default::default(),
    };

    println!("📋 Schema 创建:");
    println!("   - 表名: test_data");
    println!("   - 字段: id (I64), name (Keyword), age (I64), city (Keyword)");
    println!();

    // 创建 Partition
    let data_dir = PathBuf::from("/tmp/mysql_demo");
    println!("📁 数据目录: {}", data_dir.display());

    // 清理旧数据
    if data_dir.exists() {
        println!("🗑️  清理旧数据...");
        std::fs::remove_dir_all(&data_dir)?;
    }

    // 创建数据目录
    std::fs::create_dir_all(&data_dir)?;

    let (tx, _rx) = mpsc::unbounded_channel();
    let partition = Partition::new(0, data_dir.clone(), schema, tx);

    println!("✅ Partition 创建完成\n");

    // 准备 Arrow Schema
    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::Int64, false),
        Field::new("city", DataType::Utf8, false),
    ]));

    // 准备测试数据
    let cities = vec!["北京", "上海", "广州", "深圳", "杭州", "成都"];

    println!("💾 插入测试数据...");

    // 分批插入 100 条数据,每批 20 条
    for batch_num in 0..5 {
        let start_id = batch_num * 20 + 1;
        let end_id = (batch_num + 1) * 20 + 1;

        let mut ids = Vec::new();
        let mut names = Vec::new();
        let mut ages = Vec::new();
        let mut cities_data = Vec::new();

        for i in start_id..end_id {
            ids.push(i as i64);
            names.push(format!("用户_{}", i));
            ages.push((20 + (i % 50)) as i64);
            cities_data.push(cities[i as usize % cities.len()]);
        }

        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(names)),
                Arc::new(Int64Array::from(ages)),
                Arc::new(StringArray::from(cities_data)),
            ],
        )?;

        partition.upsert(batch)?;

        println!("   已插入 {} 条记录...", end_id - 1);
    }

    println!("   ✓ 总共插入 100 条记录");

    // Flush 并持久化数据到磁盘
    println!("\n💾 将数据持久化到磁盘...");
    let segment_id = partition.flush(false)?;
    println!("   ✓ 数据已 flush (segment_id: {})", segment_id);

    partition.persist_all()?;
    println!("   ✓ 数据已持久化到磁盘");

    // 显示统计信息
    println!("\n📊 数据统计:");
    println!("   - 总记录数: 100");
    println!("   - ID 范围: 1-100");
    println!("   - 年龄范围: 20-69");
    println!("   - 城市数量: {}", cities.len());
    println!("   - 数据目录: {}", data_dir.display());

    println!("\n✅ 测试数据准备完成!");
    println!("\n📌 下一步:");
    println!("   1. 启动 MySQL 服务器: cargo run --example mysql_server");
    println!("   2. 连接测试: mysql -h 127.0.0.1 -P 3306");
    println!("   3. 运行 JDBC demo: cd examples/jdbc-demo && mvn clean compile exec:java");

    Ok(())
}
