use std::time::Instant;

use calmcore::{util::CoreResult, *};
use proto::{core::field, result_wrapper::QueryResultWrapper};

fn main() -> CoreResult<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let schema_name = "big_test";
    let data_path = "big_data";

    if std::fs::exists(data_path)? {
        query(schema_name, data_path)
    } else {
        insert(schema_name, data_path)
    }
}

fn query(schema_name: &str, data_path: &str) -> CoreResult<()> {
    let core = CalmCore::new(data_path)?;
    let engine = core.load_engine(schema_name)?;

    let result =
        engine.sql("select * from validate_test where age < 30 and city='北京' limit 10")?;

    let result: QueryResultWrapper = result.to_wrapper();

    println!(
        "Query result: {}",
        serde_json::to_string_pretty(&result).unwrap()
    );

    let start = Instant::now();
    for _ in 0..100 {
        engine.sql("select * from validate_test where age < 30 and city='北京' limit 10")?;
    }

    println!("Query finished in {:?}", start.elapsed());

    Ok(())
}

fn insert(schema_name: &str, data_path: &str) -> CoreResult<()> {
    // 创建schema和space
    let core = CalmCore::new(data_path)?;
    let schema = calmcore::easy_schema(
        schema_name,
        vec![
            ("name".to_string(), field::Type::String, None),
            ("age".to_string(), field::Type::Int, None),
            ("score".to_string(), field::Type::Int, None),
            ("city".to_string(), field::Type::String, None),
        ],
    );

    let space = core.create_engine(schema)?;

    // 生成测试数据
    println!("Starting data insertion...");
    let start = Instant::now();
    let batch_size = 1000;
    let total = 1_000_000_000;

    let cities = ["北京", "上海", "广州", "深圳", "杭州"];

    for batch in 0..(total / batch_size) {
        let mut actions = Vec::with_capacity(batch_size);

        if space
            .segment_readers()
            .iter()
            .filter(|r| r.is_hot())
            .count()
            > 3
        {
            println!("Too many hot segments, waiting for compaction...");
            std::thread::sleep(std::time::Duration::from_secs(5));
            continue;
        }

        for i in 0..batch_size {
            let id = batch * batch_size + i;
            let age = rand::random_range(18..60);
            let score = rand::random_range(0..100);
            let city = cities[rand::random_range(0..cities.len())];

            let json = format!(
                r#"{{"name":"user_{}", "age":{}, "score":{}, "city":"{}"}}"#,
                id, age, score, city
            );

            actions.push(Action::new(ActionType::Append, "", json.as_bytes()));
        }

        space.mutate(actions, None)?;

        if batch % 1000 == 0 {
            println!("Inserted {} records", (batch + 1) * batch_size);
        }
    }

    space.persist()?;

    println!("Data insertion finished in {:?}", start.elapsed());

    Ok(())
}
