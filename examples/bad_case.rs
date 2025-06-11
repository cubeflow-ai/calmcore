use std::{
    fs::{self, File},
    io::{BufRead, BufReader},
};

use calmcore::{util::CoreResult, Action, ActionType, CalmCore, Config};
use proto::core::{
    field::{
        self,
        fulltext_option::{self, Tokenizer},
        FulltextOption, TermOption, Type,
    },
    Schema,
};

pub fn main() -> CoreResult<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    fs::remove_dir_all("data/calm_test").ok();

    let schema_name = "test";
    let data_path = "data/calm_test";

    let schema = make_schema(schema_name);
    let core = CalmCore::new_with_conf(Config {
        data_path: data_path.to_string(),
        segment_max_size: 500_000,
        flush_interval_secs: 300,
    })?;

    let space = core.create_engine(schema)?;

    //read line for file

    let file =
        File::open("/Users/sunjian/rustworkspace/search-benchmark-game/corpus.json").unwrap();

    let reader = BufReader::new(file);

    for (index, line_result) in reader.lines().enumerate() {
        let line = line_result.unwrap();

        // 为每个文档创建一个唯一的 ID，这里使用行号作为示例
        let doc_id = (index + 1).to_string();
        space.mutate(
            vec![Action::new(ActionType::Append, &doc_id, line.as_bytes())],
            None,
        )?;

        if index != 0 && index % 100_000 == 0 {
            println!("Processed {} lines", index);

            if index > 500000 {
                break;
            }
        }
    }

    space.persist().unwrap();

    loop {
        if space.segment_readers().iter().any(|s| s.is_hot()) {
            std::thread::sleep(std::time::Duration::from_secs(5));
        } else {
            break;
        }
    }

    Ok(())
}

fn make_schema(schema_name: &str) -> Schema {
    use proto::core::field::Option::Fulltext;
    use proto::core::field::Option::Term;
    calmcore::easy_schema(
        schema_name,
        vec![
            (
                "id".to_string(),
                Type::String,
                Some(Term(TermOption {
                    no_index: false,
                    no_store: false,
                })),
            ),
            (
                "text".to_string(),
                Type::Text,
                Some(Fulltext(FulltextOption {
                    tokenizer: Tokenizer::Standard as i32,
                    filters: Vec::new(),
                    stopwords: None,
                    synonyms: None,
                    no_store: true,
                })),
            ),
        ],
    )
}
