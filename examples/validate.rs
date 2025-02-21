use std::{collections::HashMap, sync::Arc, time::Instant};

use calmcore::{util::CoreResult, Engine, *};
use proto::core::field;

fn validate_query_results(
    space: &Arc<Engine>,
    query: &str,
    expected_count: usize,
    validation_fn: impl Fn(&serde_json::Value) -> bool,
) -> CoreResult<bool> {
    let results = space.sql(query)?;

    println!(
        "======================returned {} results{}",
        results.total_hits,
        results.hits.len()
    );

    if results.total_hits as usize != expected_count {
        println!(
            "Query '{}' returned {} results, expected {}",
            query, results.total_hits, expected_count
        );
        return Ok(false);
    }

    for record in results.hits {
        let value: serde_json::Value =
            serde_json::from_slice(record.record.unwrap().data.as_slice())?;
        if !validation_fn(&value) {
            println!("Record validation failed for query: {}", query);
            return Ok(false);
        }
    }
    Ok(true)
}

fn main() -> CoreResult<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let schema_name = "validate_test";
    let data_path = "validate_data";

    let _ = std::fs::remove_dir_all(data_path);

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
    let total = 10_000_000;

    let cities = ["北京", "上海", "广州", "深圳", "杭州"];
    let mut age_count = HashMap::new();
    let mut city_count = HashMap::new();
    let mut high_score_count = 0;

    for batch in 0..(total / batch_size) {
        let mut actions = Vec::with_capacity(batch_size);

        for i in 0..batch_size {
            let id = batch * batch_size + i;
            let age = rand::random_range(18..60);
            let score = rand::random_range(0..100);
            let city = cities[rand::random_range(0..cities.len())];

            // 统计数据分布
            *age_count.entry(age).or_insert(0) += 1;
            *city_count.entry(city.to_string()).or_insert(0) += 1;
            if score > 90 {
                high_score_count += 1;
            }

            let json = format!(
                r#"{{"name":"user_{}", "age":{}, "score":{}, "city":"{}"}}"#,
                id, age, score, city
            );

            actions.push(Action::new(ActionType::Append, "", json.as_bytes()));
        }

        space.mutate(actions, None)?;

        if batch % 100 == 0 {
            println!("Inserted {} records", (batch + 1) * batch_size);
        }
    }

    println!("Data insertion finished in {:?}", start.elapsed());

    // 验证查询结果
    println!("\nStarting validation...");

    let start = Instant::now();
    // 1. 验证年龄范围查询
    let young_count = age_count
        .iter()
        .filter(|(&age, _)| age < 30)
        .map(|(_, &count)| count)
        .sum();
    assert!(validate_query_results(
        &space,
        "select * from validate_test where age < 30",
        young_count,
        |record| record.get("age").and_then(|v| v.as_i64()).unwrap_or(0) < 30
    )?);

    // 2. 验证城市筛选
    for (city, &count) in &city_count {
        assert!(validate_query_results(
            &space,
            &format!("select * from validate_test where city='{}'", city),
            count,
            |record| record.get("city").and_then(|v| v.as_str()).unwrap_or("") == city
        )?);
    }

    // 3. 验证复合条件
    let beijing_young_count = space
        .sql("select * from validate_test where age < 30 and city='北京'")?
        .hits
        .len();
    println!("北京年轻人数量: {}", beijing_young_count);

    // 4. 验证分数统计
    assert!(validate_query_results(
        &space,
        "select * from validate_test where score > 90",
        high_score_count,
        |record| record.get("score").and_then(|v| v.as_i64()).unwrap_or(0) > 90
    )?);

    println!("Validation finished in {:?}", start.elapsed());

    // 5. 持久化后的验证
    space.persist()?;
    println!("Data persisted, validating after persistence...");

    let start = Instant::now();
    // 重新验证一些关键查询
    // 1. 验证年龄范围查询
    let young_count = age_count
        .iter()
        .filter(|(&age, _)| age < 30)
        .map(|(_, &count)| count)
        .sum();
    assert!(validate_query_results(
        &space,
        "select * from validate_test where age < 30",
        young_count,
        |record| record.get("age").and_then(|v| v.as_i64()).unwrap_or(0) < 30
    )?);

    // 2. 验证城市筛选
    for (city, &count) in &city_count {
        assert!(validate_query_results(
            &space,
            &format!("select * from validate_test where city='{}'", city),
            count,
            |record| record.get("city").and_then(|v| v.as_str()).unwrap_or("") == city
        )?);
    }

    // 3. 验证复合条件
    let beijing_young_count = space
        .sql("select * from validate_test where age < 30 and city='北京'")?
        .hits
        .len();
    println!("北京年轻人数量: {}", beijing_young_count);

    // 4. 验证分数统计
    assert!(validate_query_results(
        &space,
        "select * from validate_test where score > 90",
        high_score_count,
        |record| record.get("score").and_then(|v| v.as_i64()).unwrap_or(0) > 90
    )?);
    println!(
        "Validation after persistence finished in {:?}",
        start.elapsed()
    );

    Ok(())
}
