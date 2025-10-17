pub mod partition;
pub mod schema;
pub(crate) mod segment;
#[macro_use]
pub(crate) mod utils;

mod test {
    use std::{
        path::{Path, PathBuf},
        str::FromStr,
    };

    use crate::{
        partition::Partition,
        schema::{self, Schema},
    };
    #[test]
    fn test_example() {
        use arrow::array::{RecordBatch, StringBuilder, UInt32Array};
        use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
        use std::sync::Arc;

        let mut schema = schema::Schema {
            name: "test".to_string(),
            fields: vec![],
            store_source: true,
            primary_key: Some("id".to_string()),
        };

        schema.add_field(schema::field::FieldOption::Keyword {
            name: "id".to_string(),
            index: true,
            is_array: false,
        });

        schema.add_field(schema::field::FieldOption::Keyword {
            name: "name".to_string(),
            index: true,
            is_array: false,
        });

        let partition = Partition::new(0, PathBuf::from("./data"), schema);

        // 生成1000万条测试数据
        let total_count = 10_000_000;
        let batch_size = 10000; // 每批1万条
        let batch_count = total_count / batch_size;

        let start = std::time::Instant::now();

        for _ in 0..2 {
            for batch_idx in 0..batch_count {
                let mut id_builder = StringBuilder::new();
                let mut name_builder = StringBuilder::new();

                for i in 0..batch_size {
                    let id = (batch_idx * batch_size + i) as u32;
                    id_builder.append_value(format!("{}", id));
                    name_builder.append_value(format!("name_{}", id));
                }

                let data = RecordBatch::try_new(
                    Arc::new(ArrowSchema::new(vec![
                        Field::new("id", DataType::Utf8, false),
                        Field::new("name", DataType::Utf8, false),
                    ])),
                    vec![
                        Arc::new(id_builder.finish()),
                        Arc::new(name_builder.finish()),
                    ],
                )
                .unwrap();

                partition.upsert(data).unwrap();

                if (batch_idx + 1) % 100 == 0 {
                    println!(
                        "已插入 {} 条数据 存在 {}",
                        (batch_idx + 1) * batch_size,
                        partition.total_count()
                    );
                }
            }
        }

        println!("插入1000万条数据耗时: {:?}", start.elapsed());
    }
}
