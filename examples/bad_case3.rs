use std::time::Duration;

use calmcore::{util::CoreResult, *};
use proto::core::field::fulltext_option::Tokenizer;
use proto::core::field::Option::{Fulltext, Term};
use proto::core::field::{FulltextOption, TermOption, Type};

fn main() -> CoreResult<()> {
    let schema_name = "calmcore-bench";
    let data_path = "idx";

    let _ = std::fs::remove_dir_all(data_path);
    // build_index(schema_name, data_path);
    build_index2(schema_name, data_path);

    let core = CalmCore::new(data_path)?;
    let space = core.load_engine(schema_name)?;
    let sql = vec![
        "wisconsin attorney general",
        "the english restoration",
        "freelance work",
        "personal loan",
        "texas state legislature",
        "georgia public broadcasting",
        "interest only",
        "people having sex",
        "battle of the bulge",
        "secretary of state",
        "university of washington",
        "jesus as a child",
    ];

    for item in sql {
        //println!("=======================================================");
        let sql = format!("select * from \"calmcore-bench\" where text='{item}' limit 0");
        //println!("execute sql：{}", sql);
        let result = space.sql(sql.as_str())?;
        println!("{item}    {:?}", result.total_hits);
    }

    Ok(())
}

fn build_index(schema_name: &str, data_path: &str) -> () {
    // to create schema and space
    let schema = easy_schema(
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
    );

    let core = CalmCore::new(data_path).unwrap();
    let space = core.create_engine(schema).unwrap();
    space.mutate(
        vec![Action::new(
            ActionType::Append,
            "",
            r#"{"id": "https://en.wikipedia.org/wiki?curid=48687903", "text": " jeon hye jin actress born jeon hye jin born june is a south korean actress personal life jeon married his smile you co star lee chun hee on march their daughter lee so yu was born on july "}"#.to_string().as_bytes(),
        )],
        None,
    ).unwrap();
    space.mutate(
        vec![Action::new(
            ActionType::Append,
            "",
            r#"{"id": "https://en.wikipedia.org/wiki?curid=48687919", "text": " benham indiana benham is an unincorporated community in ripley county in the u s state of indiana history an old variant name of the community was benhams store a post office opened under the name benham store in the name was shortened to benham and the post office was discontinued in john benham jr served as a first postmaster "}"#.to_string().as_bytes(),
        )],
        None,
    ).unwrap();
    space.mutate(
        vec![Action::new(
            ActionType::Append,
            "",
            r#"{"id": "https://en.wikipedia.org/wiki?curid=48687922", "text": " hilyat al muttaqin hilyat al muttaqin the adornment of the god fearing is a hadith book of muhammad baqir al majlisi this work is written in persian about islamic morality instructions and traditions the aim of writing according to book s foreword it was written because of a group of muslims asked majlisi to write a persian book in the islamic morality instructions and traditions from the hadith of ahl al bayt date of writing according a manuscript the date of writing of this work is but in another book has been mentioned to content and chapters the book has chapters about individual and collective morality and some fiqh rulings duasand practices and had an extra chapter about some etiquette miscellaneous and their benefits the titles of chapters are mentioned below "}"#.to_string().as_bytes(),
        )],
        None,
    ).unwrap();
    space.mutate(
        vec![Action::new(
            ActionType::Append,
            "",
            r#"{"id": "https://en.wikipedia.org/wiki?curid=48687925", "text": " mk preshow shimray mk preshow shimray is the sitting mla from chingai st assembly constituency in manipur india he was elected under indian national congress ticket in early life mk preshow shimray was born on april at poi village to mk somi shimray he did his b e and m e environment at salem engineering college tamil nadu after completing his education he worked as scientific officer and thereafter as senior scientific officer in the environment and ecology department manipur political career in he resigned from his engagement as senior scientific officer in order to contest the mla election under congress ticket he got elected beating his nearest rival with a simple majority vote in july mk preshow shimray was elected as the deputy speaker of the manipur legislative assembly and is still serving in that capacity assasination bids on april the cavalcade of the deputy speaker was ambushed near ukhrul by suspected nscn im cadres however there were no reports of casualty the second bid on his life was made on april by some rebel groups near litan but there too he escaped unharmed anti tribal bills protests all tribal mlas in manipur were requested to resign in protest against the passing of three anti tribal bills in manipur ligislative assembly on august however many tribal mlas paid no heed to the call of the tribal people for which many elected representatives were ostrcised from their respective constituencies mk preshow shimray along with the sitting mla of phungyar mla constituency victor keishing son of rishang keishing were also declared anti socials by the tangkhul frontal organisations for failing to tender their resignation letters "}"#.to_string().as_bytes(),
        )],
        None,
    ).unwrap();
    space.mutate(
        vec![Action::new(
            ActionType::Append,
            "",
            r#"{"id": "https://en.wikipedia.org/wiki?curid=48687930", "text": " clinton ripley county indiana clinton is an unincorporated community in ripley county in the u s state of indiana history clinton was founded in "}"#.to_string().as_bytes(),
        )],
        None,
    ).unwrap();
    space.mutate(
        vec![Action::new(
            ActionType::Append,
            "",
            r#"{"id": "https://en.wikipedia.org/wiki?curid=48687935", "text": " norma no remote memory access abbreviated as norma is a computer memory architecture for multiprocessor systems given its name by in a norma architecture the address space globally is not unique and the memory is not globally accessible by the processors accesses to remote memory modules are only indirectly possible by messages through the interconnection network to other processors which in turn possibly deliver the desired data in a reply message the entire storage configuration is partitioned statically among the processors the advantage of the norma model is the ability to construct extremely large configurations which is achieved by shifting the problem to the user configuration programs for norma architectures need to evenly partitioning the data into local memory modules ensure consistency of software caches to enforce the desired consistency model handle transformations of data identifiers from one processor s address space to another and realize a message passing system for remote access to data the programming model of norma architecture is therefore extremely complicated "}"#.to_string().as_bytes(),
        )],
        None,
    ).unwrap();
    space.mutate(
        vec![Action::new(
            ActionType::Append,
            "",
            r#"{"id": "https://en.wikipedia.org/wiki?curid=48687945", "text": " national capitol wing civil air patrol the national capital wing of the civil air patrol cap is the highest echelon of civil air patrol in the district of washington d c the national capital wing consists of nearly cadet and adult members at over locations across the district of washington d c mission the national capital wing performs the three missions of the civil air patrol providing emergency services offering cadet programs for youth and providing aerospace education for both cap members and the general public emergency services the civil air patrol provides emergency services which includes performing search and rescue and disaster relief missions as well as assisting in humanitarian aid assignments the cap also provides air force support through conducting light transport communications support and low altitude route surveys the civil air patrol can also offer support to counter drug missions cadet programs the civil air patrol offers a cadet program for youth aged to which includes aerospace education leadership training physical fitness and moral leadership aerospace education the civil air patrol offers aerospace education for cap members and the general public including providing training to the members of cap and offering workshops for youth throughout the nation through schools and public aviation events "}"#.to_string().as_bytes(),
        )],
        None,
    ).unwrap();
    space.mutate(
        vec![Action::new(
            ActionType::Append,
            "",
            r#"{"id": "https://en.wikipedia.org/wiki?curid=48687949", "text": " bae seong woo bae seong woo born november is a south korean actor he starred in film such as my love my bride office and inside men "}"#.to_string().as_bytes(),
        )],
        None,
    ).unwrap();
    space.mutate(
        vec![Action::new(
            ActionType::Append,
            "",
            r#"{"id": "https://en.wikipedia.org/wiki?curid=48687980", "text": " cross roads ripley county indiana cross roads is an unincorporated community in ripley county in the u s state of indiana history the community was so named for the fact it originally contained a store at a crossroads "}"#.to_string().as_bytes(),
        )],
        None,
    ).unwrap();
    space
        .mutate(
            vec![Action::new(
                ActionType::Append,
                "",
                r#"{"id": "https://en.wikipedia.org/wiki?curid=48687986", "text": ""}"#
                    .to_string()
                    .as_bytes(),
            )],
            None,
        )
        .unwrap();

    space.persist().unwrap();
}

fn build_index2(schema_name: &str, data_path: &str) -> () {
    // to create schema and space
    let schema = easy_schema(
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
    );

    let core = CalmCore::new(data_path).unwrap();
    let space = core.create_engine(schema).unwrap();
    let mut actions = Vec::with_capacity(10);
    actions.push(Action::new(ActionType::Append, "", r#"{"id": "https://en.wikipedia.org/wiki?curid=48687903", "text": " jeon hye jin actress born jeon hye jin born june is a south korean actress personal life jeon married his smile you co star lee chun hee on march their daughter lee so yu was born on july "}"#.as_bytes()));
    actions.push(Action::new(ActionType::Append, "", r#"{"id": "https://en.wikipedia.org/wiki?curid=48687919", "text": " benham indiana benham is an unincorporated community in ripley county in the u s state of indiana history an old variant name of the community was benhams store a post office opened under the name benham store in the name was shortened to benham and the post office was discontinued in john benham jr served as a first postmaster "}"#.as_bytes()));
    actions.push(Action::new(ActionType::Append, "", r#"{"id": "https://en.wikipedia.org/wiki?curid=48687922", "text": " hilyat al muttaqin hilyat al muttaqin the adornment of the god fearing is a hadith book of muhammad baqir al majlisi this work is written in persian about islamic morality instructions and traditions the aim of writing according to book s foreword it was written because of a group of muslims asked majlisi to write a persian book in the islamic morality instructions and traditions from the hadith of ahl al bayt date of writing according a manuscript the date of writing of this work is but in another book has been mentioned to content and chapters the book has chapters about individual and collective morality and some fiqh rulings duasand practices and had an extra chapter about some etiquette miscellaneous and their benefits the titles of chapters are mentioned below "}"#.as_bytes()));
    actions.push(Action::new(ActionType::Append, "", r#"{"id": "https://en.wikipedia.org/wiki?curid=48687925", "text": " mk preshow shimray mk preshow shimray is the sitting mla from chingai st assembly constituency in manipur india he was elected under indian national congress ticket in early life mk preshow shimray was born on april at poi village to mk somi shimray he did his b e and m e environment at salem engineering college tamil nadu after completing his education he worked as scientific officer and thereafter as senior scientific officer in the environment and ecology department manipur political career in he resigned from his engagement as senior scientific officer in order to contest the mla election under congress ticket he got elected beating his nearest rival with a simple majority vote in july mk preshow shimray was elected as the deputy speaker of the manipur legislative assembly and is still serving in that capacity assasination bids on april the cavalcade of the deputy speaker was ambushed near ukhrul by suspected nscn im cadres however there were no reports of casualty the second bid on his life was made on april by some rebel groups near litan but there too he escaped unharmed anti tribal bills protests all tribal mlas in manipur were requested to resign in protest against the passing of three anti tribal bills in manipur ligislative assembly on august however many tribal mlas paid no heed to the call of the tribal people for which many elected representatives were ostrcised from their respective constituencies mk preshow shimray along with the sitting mla of phungyar mla constituency victor keishing son of rishang keishing were also declared anti socials by the tangkhul frontal organisations for failing to tender their resignation letters "}"#.as_bytes()));
    actions.push(Action::new(ActionType::Append, "", r#"{"id": "https://en.wikipedia.org/wiki?curid=48687930", "text": " clinton ripley county indiana clinton is an unincorporated community in ripley county in the u s state of indiana history clinton was founded in "}"#.as_bytes()));
    actions.push(Action::new(ActionType::Append, "", r#"{"id": "https://en.wikipedia.org/wiki?curid=48687935", "text": " norma no remote memory access abbreviated as norma is a computer memory architecture for multiprocessor systems given its name by in a norma architecture the address space globally is not unique and the memory is not globally accessible by the processors accesses to remote memory modules are only indirectly possible by messages through the interconnection network to other processors which in turn possibly deliver the desired data in a reply message the entire storage configuration is partitioned statically among the processors the advantage of the norma model is the ability to construct extremely large configurations which is achieved by shifting the problem to the user configuration programs for norma architectures need to evenly partitioning the data into local memory modules ensure consistency of software caches to enforce the desired consistency model handle transformations of data identifiers from one processor s address space to another and realize a message passing system for remote access to data the programming model of norma architecture is therefore extremely complicated "}"#.as_bytes()));
    actions.push(Action::new(ActionType::Append, "", r#"{"id": "https://en.wikipedia.org/wiki?curid=48687945", "text": " national capitol wing civil air patrol the national capital wing of the civil air patrol cap is the highest echelon of civil air patrol in the district of washington d c the national capital wing consists of nearly cadet and adult members at over locations across the district of washington d c mission the national capital wing performs the three missions of the civil air patrol providing emergency services offering cadet programs for youth and providing aerospace education for both cap members and the general public emergency services the civil air patrol provides emergency services which includes performing search and rescue and disaster relief missions as well as assisting in humanitarian aid assignments the cap also provides air force support through conducting light transport communications support and low altitude route surveys the civil air patrol can also offer support to counter drug missions cadet programs the civil air patrol offers a cadet program for youth aged to which includes aerospace education leadership training physical fitness and moral leadership aerospace education the civil air patrol offers aerospace education for cap members and the general public including providing training to the members of cap and offering workshops for youth throughout the nation through schools and public aviation events "}"#.as_bytes()));
    actions.push(Action::new(ActionType::Append, "", r#"{"id": "https://en.wikipedia.org/wiki?curid=48687949", "text": " bae seong woo bae seong woo born november is a south korean actor he starred in film such as my love my bride office and inside men "}"#.as_bytes()));
    actions.push(Action::new(ActionType::Append, "", r#"{"id": "https://en.wikipedia.org/wiki?curid=48687980", "text": " cross roads ripley county indiana cross roads is an unincorporated community in ripley county in the u s state of indiana history the community was so named for the fact it originally contained a store at a crossroads "}"#.as_bytes()));
    actions.push(Action::new(
        ActionType::Append,
        "",
        r#"{"id": "https://en.wikipedia.org/wiki?curid=48687986", "text": ""}"#.as_bytes(),
    ));
    space.mutate(actions, None).unwrap();
    space.persist().unwrap();
}
