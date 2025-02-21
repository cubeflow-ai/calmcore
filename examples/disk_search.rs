use std::time::Instant;

use calmcore::{util::CoreResult, *};
use proto::core::field;
fn main() -> CoreResult<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let schema_name = "test";
    let data_path = "data";

    let _ = std::fs::remove_dir_all(data_path);

    // to create schema and space
    let core = CalmCore::new(data_path)?;
    let schema = calmcore::easy_schema(
        schema_name,
        vec![
            (format!("name"), field::Type::String, None),
            (format!("age"), field::Type::Int, None),
            (format!("skill"), field::Type::String, None),
        ],
    );

    let space = core.create_engine(schema)?;

    // 写入1000万条数据
    println!("Starting data insertion...");
    let start = Instant::now();
    let batch_size = 1000;
    let total = 10_000_000;

    let skills = ["java", "python", "rust", "go", "cpp"];

    for batch in 0..(total / batch_size) {
        let mut actions = Vec::with_capacity(batch_size);

        for _ in 0..batch_size {
            let age = rand::random_range(0..100);

            let skill = skills[rand::random_range(0..skills.len())];

            let json = format!(
                r#"{{"name":"user_{}", "age":{}, "skill":"{}"}}"#,
                batch * batch_size + actions.len(),
                age,
                skill
            );

            actions.push(Action::new(ActionType::Append, "", json.as_bytes()));
        }

        space.mutate(actions, None)?;

        if batch % 100 == 0 {
            println!("Inserted {} records", (batch + 1) * batch_size);
        }
    }

    println!("Data insertion finished in {:?}", start.elapsed());

    let start = Instant::now();
    for _ in 0..100 {
        for _ in 0..10 {
            space.sql("select * from test where age > 20 and skill='java'")?;
            space.sql("select * from test where age < 20 and skill='rust'")?;
        }
    }
    println!("hot search finished in {:?}", start.elapsed());

    let start = Instant::now();
    space.persist()?;
    println!("Data persist finished in {:?}", start.elapsed());

    let start = Instant::now();
    for _ in 0..100 {
        for _ in 0..10 {
            space.sql("select * from test where age > 20 and skill='java'")?;
            space.sql("select * from test where age < 20 and skill='rust'")?;
        }
    }
    println!("warm search finished in {:?}", start.elapsed());

    println!(
        "{:#?}",
        space
            .sql("select * from test where age < 20 and skill='rust'")?
            .to_wrapper()
    );

    Ok(())
}
