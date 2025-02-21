use calmcore::{util::CoreResult, *};
use proto::core::{
    field::{self, embedding_option::Metric, fulltext_option, EmbeddingOption, FulltextOption},
    QueryResult, Schema,
};
fn main() -> CoreResult<()> {
    let schema_name = "test";
    let data_path = "/tmp/calm_test";

    let _ = std::fs::remove_dir_all(data_path);

    // to create schema and space
    let core = CalmCore::new(data_path)?;

    let schema = make_schema(schema_name);

    let id = core.create_engine(schema)?;

    let space = core.load_engine(&schema_name)?;

    space.mutate_json(
        ActionType::Upsert,
        "1".to_string(),
        br#"{"name":"hello" , "age":32 , "sex":true, "content":"java golang rust", "skill":[1.0, 2.1,2.2]}"#,
    )?;

    space.mutate_json(
        ActionType::Upsert,
        "2".to_string(),
        br#"{"name":"hello1" , "age":22 , "sex":false, "content":"asp c++ php", "skill":[1.0, 5.1,2.2]}"#,
    )?;

    space.mutate_json(
        ActionType::Upsert,
        "3".to_string(),
        br#"{"name":"hello2" , "age":12 , "sex":true, "content":"java c++ php", "skill":[1.0, 5.1,2.2]}"#,
    )?;

    let _resp = space.sql("select * FROM test WHERE content = phrase('java golang')")?;

    let _resp = space.sql("select * FROM test WHERE content = phrase('java c++')")?;

    let _resp = space.sql("select * FROM test WHERE content = phrase('asp c++')")?;

    let _resp = space.sql("select * FROM test WHERE age < 20 AND content = phrase('java c++')")?;

    result_print(_resp);

    Ok(())
}

fn result_print(result: QueryResult) {
    println!("total: {}", result.total_hits);
    for hit in result.hits {
        let value = serde_json::from_slice::<serde_json::Value>(&hit.record.unwrap().data).unwrap();
        println!("{:?}-----{}", hit.id, value.to_string());
    }
}

fn make_schema(schema_name: &str) -> Schema {
    let mut fields = std::collections::HashMap::new();

    let name = String::from("name");
    fields.insert(
        name.clone(),
        proto::core::Field {
            name,
            r#type: proto::core::field::Type::String as i32,
            option: None,
        },
    );

    let name = String::from("age");
    fields.insert(
        name.clone(),
        proto::core::Field {
            name,
            r#type: proto::core::field::Type::Int as i32,
            option: None,
        },
    );

    let name = String::from("skill");
    fields.insert(
        name.clone(),
        proto::core::Field {
            name,
            r#type: proto::core::field::Type::Vector as i32,
            option: Some(field::Option::Embedding(EmbeddingOption {
                embedding: String::from("no"),
                dimension: 3,
                metric: Metric::Euclidean as i32,
                batch_size: 1000,
            })),
        },
    );

    let name = String::from("content");
    fields.insert(
        name.clone(),
        proto::core::Field {
            name,
            r#type: proto::core::field::Type::Text as i32,
            option: Some(field::Option::Fulltext(FulltextOption {
                tokenizer: fulltext_option::Tokenizer::Standard as i32,
                filters: Vec::new(),
                stopwords: None,
                synonyms: None,
            })),
        },
    );

    Schema {
        name: String::from(schema_name),
        id: 1,
        fields,
        metadata: None,
        schemaless: false,
    }
}
