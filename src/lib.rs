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
            zip_level: 3,
        });

        schema.add_field(schema::field::FieldOption::Keyword {
            name: "name".to_string(),
            index: true,
            is_array: false,
            zip_level: 3,
        });

        let partition = Partition::new(0, PathBuf::from("./data"), schema);

        // 生成300万条测试数据
        let total_count = 3_000_000;
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

    #[test]
    fn test_insert_1_million_records() {
        use arrow::array::{RecordBatch, StringBuilder};
        use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
        use std::sync::Arc;
        use std::time::Instant;

        println!("\n=== 测试插入100万条数据 ===\n");

        // 创建 schema
        let mut schema = Schema {
            name: "test_1m".to_string(),
            fields: vec![],
            store_source: true,
            primary_key: Some("id".to_string()),
        };

        schema.add_field(schema::field::FieldOption::Keyword {
            name: "id".to_string(),
            index: true,
            is_array: false,
            zip_level: 3,
        });

        schema.add_field(schema::field::FieldOption::Keyword {
            name: "name".to_string(),
            index: true,
            is_array: false,
            zip_level: 3,
        });

        // 创建临时目录用于测试
        let test_dir = PathBuf::from("/tmp/test_1m_data");
        let _ = std::fs::remove_dir_all(&test_dir);
        std::fs::create_dir_all(&test_dir).unwrap();

        let partition = Partition::new(0, test_dir.clone(), schema);

        // 插入100万条数据
        let total_count = 1_000_000;
        let batch_size = 10_000; // 每批1万条
        let batch_count = total_count / batch_size;

        println!("开始插入数据...");
        println!("总数: {} 条", total_count);
        println!("批次大小: {} 条", batch_size);
        println!("批次数量: {} 批\n", batch_count);

        let start = Instant::now();

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

            // 每10批输出一次进度（每10万条）
            if (batch_idx + 1) % 10 == 0 {
                println!(
                    "已插入 {} 条数据，当前总数: {}",
                    (batch_idx + 1) * batch_size,
                    partition.total_count()
                );
            }
        }

        let duration = start.elapsed();

        println!("\n=== 插入完成 ===");
        println!("总耗时: {:?}", duration);
        println!("最终数据量: {}", partition.total_count());
        println!(
            "插入速度: {:.2} 条/秒",
            total_count as f64 / duration.as_secs_f64()
        );

        // 验证数据量
        assert_eq!(partition.total_count(), total_count);

        // 清理测试目录
        let _ = std::fs::remove_dir_all(&test_dir);

        println!("\n测试通过！");
    }
}
