use calmcore::{util::CoreResult, Action, ActionType, CalmCore};
use proto::core::field::{self, fulltext_option, FulltextOption, TermOption};

pub fn main() -> CoreResult<()> {
    let schema_name = "test";
    let data_path = "data/calm_test";

    let _ = std::fs::remove_dir_all(data_path);

    // to create schema and space
    let core = CalmCore::new(data_path)?;
    let schema = calmcore::easy_schema(
        schema_name,
        vec![
            (
                "id".to_string(),
                field::Type::String,
                Some(field::Option::Term(TermOption {
                    no_index: true,
                    no_store: false,
                })),
            ),
            (
                format!("text"),
                field::Type::Text,
                Some(field::Option::Fulltext(FulltextOption {
                    tokenizer: fulltext_option::Tokenizer::Standard as i32,
                    ..Default::default()
                })),
            ),
        ],
    );

    let space = core.create_engine(schema)?;

    // space.mutate(
    //     vec![Action::new(
    //         ActionType::Append,
    //         "1",
    //         br#"{"id":"hello1" , "text":"abc bowel obstruction"}"#,
    //     )],
    //     None,
    // )?;

    space.mutate(
        vec![Action::new(
            ActionType::Append,
            "2",
            br#"{"id":"hello2" , "text":"bcd bowel obstructions"}"#,
        )],
        None,
    )?;

    let result = space.sql("select id, text from test where text='bcd'")?;
    println!("sql result:{:?}", result.to_wrapper());

    space.persist().unwrap();

    let core = CalmCore::new(data_path)?;

    let space = core.load_engine(schema_name)?;
    let result = space.sql(
        "select id, text from test where text='bcd'
    ",
    )?;
    println!("sql result:{:?}", result.to_wrapper());

    Ok(())
}
