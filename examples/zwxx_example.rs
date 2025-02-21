use std::{collections::HashMap, io::BufRead};

use calmcore::{util::CoreResult, *};
use proto::core::{Record, Schema};
fn main() -> CoreResult<()> {
    let schema_name = "test";
    let data_path = "/tmp/calm_test";

    let _ = std::fs::remove_dir_all(data_path);

    // to create schema and space
    let core = CalmCore::new(data_path)?;
    let schema = make_schema(schema_name);

    let engine = core.create_engine(schema).unwrap();

    // store record
    let records = make_record();

    //batch insert
    let actions = records.into_iter().map(Action::Insert).collect::<Vec<_>>();

    println!("to write data len:{}", actions.len());
    let start = std::time::Instant::now();
    for action in actions {
        engine.mutate(vec![action], None)?;
    }
    println!("insert time: {:?}", start.elapsed());

    // // test get
    println!("------------get--------------------------------------------------------------");
    let record = engine.get(&"1588".to_string()).unwrap();
    println!("get result: {:?}", record.to_wrapper());

    // test search

    // println!("------------sql--------------------------------------------------------------");
    let result = engine
        .sql("select urltitle from test where urltitle=text('资格') order by _score desc")
        .unwrap();
    println!("sql result:{:?}", result.to_wrapper());

    println!("------------sql--------------------------------------------------------------");
    let result = engine.sql("select urltitle from test ").unwrap();
    println!("sql result:{:?}", result.to_wrapper());

    // println!("------------sql--------------------num query---------------------------------");
    let result = engine.sql("select * from test where district='浙江省交通运输厅办公室' and ssrwnf<=2014 limit 0,10").unwrap();
    println!("sql result:{:?}", result.to_wrapper());

    // println!("------------sql--------------------num query---------------------------------");
    let result = engine
        .sql("select ssrwnf,district from test where  ssrwnf>=2014 order by ssrwnf asc limit 0,10")
        .unwrap();
    println!("sql result:{:?}", result.to_wrapper());

    Ok(())
}

fn make_record() -> Vec<Record> {
    let mut result = vec![];
    for line in std::io::BufReader::new(std::fs::File::open("examples/zwxx.json").unwrap()).lines()
    {
        let line = line.unwrap();
        let value = serde_json::from_str::<serde_json::Value>(&line).unwrap();

        let name = value.get("id").unwrap().as_str().unwrap().to_string();

        result.push(Record {
            name,
            data: line.into_bytes(),
            ..Default::default()
        });
    }
    result
}

//{"abolitiondate":"","attachment_path":"","catalog":["科技"],"cdoi":"697f6aa847f1443b89650b1dc5fa0604","check_status":"2","district":"浙江省科学技术厅","efectdate":"","handle_state":"0","id":"1201","intime":"1445566238567","is_edit":"N","is_publish":"Y","keyword":["科技强市","科技强县","指标体系","通知"],"link":"http://www.zj.gov.cn/art/2008/5/5/art_13797_19433.html","only_mark":"357403","sitename":"www.zj.gov.cn","sreserved2":"浙科发政〔2004〕219号","ssrwnf":"2014","tcfl":"机构文件/其他","update_date":"10/23/2015 10:10:38","urldate":"2004-08-02 00:00:00","urltime":"2004-08-02 00:00:00","urltitle":"浙江省科学技术厅关于印发《浙江省科技强市和科技强县评价指标体系及有关说明（试行）》的通知","wz":"通知","zip_flag":"N"}
fn make_schema(schema_name: &str) -> Schema {
    let mut fields = HashMap::new();

    let name = String::from("district");
    fields.insert(
        name.clone(),
        proto::core::Field {
            name,
            r#type: proto::core::field::Type::String as i32,
            option: None,
        },
    );

    let name = String::from("urltitle");
    fields.insert(
        name.clone(),
        proto::core::Field {
            name,
            r#type: proto::core::field::Type::Text as i32,
            option: None,
        },
    );

    let name = String::from("ssrwnf");
    fields.insert(
        name.clone(),
        proto::core::Field {
            name,
            r#type: proto::core::field::Type::Int as i32,
            option: None,
        },
    );

    Schema {
        name: String::from(schema_name),
        fields,
        metadata: None,
        schemaless: false,
    }
}
