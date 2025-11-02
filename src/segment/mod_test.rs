#[cfg(test)]
mod tests {
    use crate::{
        schema::{field::FieldOption, Schema},
        segment::{FieldIndexMode, Segment},
    };
    use arrow::array::{RecordBatch, StringBuilder};
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use std::sync::{atomic::Ordering, Arc, RwLock};

    #[test]
    fn test_segment_persist() {
        // 1. 创建 schema
        let schema = Arc::new(Schema {
            name: "test_schema".to_string(),
            primary_key: Some("id".to_string()),
            store_source: false,
            fields: vec![FieldOption::Keyword {
                name: "tags".to_string(),
                is_array: false,
                index: true,
            }],
        });

        // 2. 创建 segment 并写入数据
        let segment = Segment::new(0, schema.clone());

        println!("写入测试数据...");

        // 写入 1000 条记录
        let batch_size = 100;
        let batch_count = 10;

        for batch in 0..batch_count {
            let mut id_builder = StringBuilder::new();
            let mut tags_builder = StringBuilder::new();

            for i in 0..batch_size {
                let id = format!("id{}", batch * batch_size + i);
                id_builder.append_value(&id);

                let tag = format!("tag{}", (batch * batch_size + i) % 50);
                tags_builder.append_value(&tag);
            }

            let data = RecordBatch::try_new(
                Arc::new(ArrowSchema::new(vec![
                    Field::new("id", DataType::Utf8, false),
                    Field::new("tags", DataType::Utf8, true),
                ])),
                vec![
                    Arc::new(id_builder.finish()),
                    Arc::new(tags_builder.finish()),
                ],
            )
            .unwrap();

            segment
                .write(&data, None, None, &RwLock::new(()), FieldIndexMode::Sync)
                .unwrap();
        }

        println!("写入完成，共 {} 条记录", batch_count * batch_size);
        println!(
            "Segment doc_id_gen: {}",
            segment.doc_id_gen.load(Ordering::Relaxed)
        );

        // 3. Persist segment
        let persist_path = "/tmp/test_segment_persist";
        let _ = std::fs::remove_dir_all(persist_path);

        println!("\n开始 persist segment...");
        let persist_start = std::time::Instant::now();

        // 空的历史 segments (测试中没有历史)
        let history_segments: Vec<(u64, Arc<Segment>)> = vec![];
        segment.persist(persist_path, 1, &history_segments).unwrap();

        println!("Persist 完成，耗时: {:?}", persist_start.elapsed());

        // 4. 验证已持久化的 segment 无法再次持久化（Field 已是 Disk 变体）
        let history_segments: Vec<(u64, Arc<Segment>)> = vec![];
        let write_result = segment.persist(persist_path, 1, &history_segments);
        assert!(write_result.is_err(), "已持久化的 segment 不能再次持久化");
        println!("确认：已持久化的 segment 无法再次持久化 ✓");

        // 6. 检查文件结构 (segment写入了1000个文档: 0-999)
        let segment_dir = format!("{}/segment-0-999", persist_path);
        assert!(std::path::Path::new(&segment_dir).exists());
        assert!(std::path::Path::new(&format!("{}/field-tags", segment_dir)).exists());
        assert!(std::path::Path::new(&format!("{}/deleted", segment_dir)).exists());
        assert!(std::path::Path::new(&format!("{}/rowdata", segment_dir)).exists());
        println!("文件结构验证 ✓");

        // 清理
        let _ = std::fs::remove_dir_all(persist_path);
        println!("\n测试完成！");
    }

    #[test]
    fn test_segment_load_frozen() {
        // 1. 创建并持久化 segment
        let schema = Arc::new(Schema {
            name: "test_schema".to_string(),
            primary_key: Some("id".to_string()),
            store_source: false,
            fields: vec![FieldOption::Keyword {
                name: "tags".to_string(),
                is_array: false,
                index: true,
            }],
        });

        let segment = Segment::new(0, schema.clone());

        // 写入数据
        println!("写入测试数据...");
        for i in 0..100 {
            let mut id_builder = StringBuilder::new();
            let mut tags_builder = StringBuilder::new();

            id_builder.append_value(&format!("id{}", i));
            tags_builder.append_value(&format!("tag{}", i % 10));

            let data = RecordBatch::try_new(
                Arc::new(ArrowSchema::new(vec![
                    Field::new("id", DataType::Utf8, false),
                    Field::new("tags", DataType::Utf8, true),
                ])),
                vec![
                    Arc::new(id_builder.finish()),
                    Arc::new(tags_builder.finish()),
                ],
            )
            .unwrap();

            segment
                .write(&data, None, None, &RwLock::new(()), FieldIndexMode::Sync)
                .unwrap();
        }

        let persist_path = "/tmp/test_segment_load";
        let _ = std::fs::remove_dir_all(persist_path);

        println!("Persist segment...");
        let history_segments: Vec<(u64, Arc<Segment>)> = vec![];
        segment.persist(persist_path, 2, &history_segments).unwrap();

        // 2. 从磁盘加载 frozen segment
        println!("\n从磁盘加载 frozen segment...");
        let load_start = std::time::Instant::now();

        // segment写入了100个文档 (0-99), end_id=99
        let loaded_segment = Segment::load_frozen(persist_path, 0, 99, schema.clone()).unwrap();

        println!("加载完成，耗时: {:?}", load_start.elapsed());

        // 3. 验证基本属性
        assert_eq!(
            loaded_segment.doc_id_gen.load(Ordering::Relaxed),
            segment.doc_id_gen.load(Ordering::Relaxed)
        );
        println!("doc_id_gen 一致 ✓");

        // 4. 验证 deleted bitmap
        assert_eq!(
            loaded_segment.deleted.read().unwrap().len(),
            segment.deleted.read().unwrap().len()
        );
        println!("deleted bitmap 一致 ✓");

        // 清理
        let _ = std::fs::remove_dir_all(persist_path);
        println!("\n测试完成！");
    }
}
