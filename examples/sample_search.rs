use calmcore::{util::CoreResult, *};
use proto::core::field;
fn main() -> CoreResult<()> {
    let schema_name = "test";
    let data_path = "data/calm_test";

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

    space.mutate(
        vec![Action::new(
            ActionType::Append,
            "1",
            br#"{"name":"hello" , "age":32 , "sex":true, "skill":["java", "php"]}"#,
        )],
        None,
    )?;

    space.mutate(
        vec![Action::new(
            ActionType::Append,
            "2",
            br#"{"name":"hello1" , "age":22 , "sex":false, "skill":["java", "rust"]}"#,
        )],
        None,
    )?;

    println!("\nexecute sql:select * from test where age > 20 and skill='java' ");
    let result = space.sql("select * from test where age > 20 and skill='java'")?;
    println!("sql result:{}", serde_json::to_string(&result).unwrap());

    println!("\nexecute sql:select * from test where age > 20 and skill='rust'");
    let result = space.sql("select * from test where age > 20 and skill='rust'")?;
    println!("sql result:{}", serde_json::to_string(&result).unwrap());

    let sql = "select * from test where age > 30 and skill='php'";
    println!("\nexecute sql:{}", sql);
    let result = space.sql(sql)?;
    println!("sql result:{}", serde_json::to_string(&result).unwrap());

    let sql =
        "select age, skill from test where age > 30 and skill='php' order by age desc limit 10";
    println!("\nexecute sql:{}", sql);
    let result = space.sql(sql)?;
    println!("sql result:{:?}", result.to_wrapper());

    Ok(())
}
