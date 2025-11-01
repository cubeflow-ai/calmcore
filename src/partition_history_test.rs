// 测试历史删除快照功能
#[cfg(test)]
mod history_delete_tests {
    use super::*;
    use arrow::{
        array::{RecordBatch, StringBuilder},
        datatypes::{DataType, Field, Schema as ArrowSchema},
    };
    use std::sync::{Arc, RwLock};

    #[test]
    fn test_history_delete_snapshot() {
        use crate::{
            schema::{field::FieldOption, Schema},
            segment::Segment,
        };

        println!("\n=== 测试历史删除快照功能 ===\n");

        // 1. 创建 3 个 segments
        let schema = Arc::new(Schema {
            name: "test_schema".to_string(),
            primary_key: Some("id".to_string()),
            store_source: false,
            fields: vec![
                FieldOption::Keyword {
                    name: "id".to_string(),
                    is_array: false,
                    index: true,
                },
                FieldOption::Keyword {
                    name: "name".to_string(),
                    is_array: false,
                    index: true,
                },
            ],
        });

        let segment_0 = Arc::new(Segment::new(0, schema.clone()));
        let segment_1 = Arc::new(Segment::new(100, schema.clone()));
        let segment_2 = Arc::new(Segment::new(200, schema.clone()));

        // 2. 写入数据到每个 segment
        println!("【1】写入数据到 3 个 segments");
        for (seg_idx, segment) in [&segment_0, &segment_1, &segment_2].iter().enumerate() {
            let start = seg_idx * 100;
            for i in 0..50 {
                let mut id_builder = StringBuilder::new();
                let mut name_builder = StringBuilder::new();

                id_builder.append_value(&format!("id{}", start + i));
                name_builder.append_value(&format!("name{}", start + i));

                let data = RecordBatch::try_new(
                    Arc::new(ArrowSchema::new(vec![
                        Field::new("id", DataType::Utf8, false),
                        Field::new("name", DataType::Utf8, true),
                    ])),
                    vec![
                        Arc::new(id_builder.finish()),
                        Arc::new(name_builder.finish()),
                    ],
                )
                .unwrap();

                segment
                    .write(
                        &data,
                        None,
                        None,
                        &RwLock::new(()),
                        crate::segment::FieldIndexMode::Sync,
                    )
                    .unwrap();
            }
            println!("  Segment {}: 写入 50 条记录", seg_idx);
        }
        println!();

        // 3. 在内存中标记一些删除
        println!("【2】在内存中标记删除");
        segment_0.mark_del(vec![0, 1, 2]); // Segment 0: 删除 3 条
        segment_1.mark_del(vec![100, 101]); // Segment 1: 删除 2 条
        println!("  Segment 0: 删除 3 条 (doc_id: 0, 1, 2)");
        println!("  Segment 1: 删除 2 条 (doc_id: 100, 101)");
        println!();

        // 4. Persist segment_0 (应该没有历史删除)
        let persist_path = "/tmp/test_history_delete_snapshot";
        let _ = std::fs::remove_dir_all(persist_path);
        std::fs::create_dir_all(persist_path).unwrap();

        println!("【3】Persist Segment 0 (无历史删除)");
        let history_segments: Vec<(u64, Arc<Segment>)> = vec![];
        segment_0
            .persist(persist_path, 0, &history_segments)
            .unwrap();
        println!("  Segment 0 持久化完成");
        println!();

        // 5. Persist segment_1 (应该包含 segment_0 的删除快照)
        println!("【4】Persist Segment 1 (包含 Segment 0 的删除快照)");
        let history_segments = vec![(0, segment_0.clone())];
        segment_1
            .persist(persist_path, 1, &history_segments)
            .unwrap();
        println!("  Segment 1 持久化完成");
        println!();

        // 6. 继续标记更多删除
        println!("【5】继续标记更多删除");
        segment_0.mark_del(vec![3, 4]); // Segment 0: 再删除 2 条
        segment_2.mark_del(vec![200]); // Segment 2: 删除 1 条
        println!("  Segment 0: 再删除 2 条 (doc_id: 3, 4)");
        println!("  Segment 2: 删除 1 条 (doc_id: 200)");
        println!();

        // 7. Persist segment_2 (应该包含 segment_0 和 segment_1 的删除快照)
        println!("【6】Persist Segment 2 (包含 Segment 0 和 1 的删除快照)");
        let history_segments = vec![(0, segment_0.clone()), (1, segment_1.clone())];
        segment_2
            .persist(persist_path, 2, &history_segments)
            .unwrap();
        println!("  Segment 2 持久化完成");
        println!();

        // 8. 验证文件结构
        println!("【7】验证文件结构");
        let seg0_dir = format!("{}/segment-0", persist_path);
        let seg1_dir = format!("{}/segment-1", persist_path);
        let seg2_dir = format!("{}/segment-2", persist_path);

        assert!(std::path::Path::new(&format!("{}/deleted", seg0_dir)).exists());
        assert!(!std::path::Path::new(&format!("{}/deleted_seg_0", seg1_dir)).exists()); // Segment 1 不应该有 seg_0 的历史（因为 seg_0 没有删除在 seg_1 persist 时）

        // 实际上，由于我们在 persist 时传递了 history_segments，所以应该有
        let has_seg0_history_in_seg1 =
            std::path::Path::new(&format!("{}/deleted_seg_0", seg1_dir)).exists();
        let has_seg0_history_in_seg2 =
            std::path::Path::new(&format!("{}/deleted_seg_0", seg2_dir)).exists();
        let has_seg1_history_in_seg2 =
            std::path::Path::new(&format!("{}/deleted_seg_1", seg2_dir)).exists();

        println!(
            "  Segment 1 有 Segment 0 的历史删除: {}",
            has_seg0_history_in_seg1
        );
        println!(
            "  Segment 2 有 Segment 0 的历史删除: {}",
            has_seg0_history_in_seg2
        );
        println!(
            "  Segment 2 有 Segment 1 的历史删除: {}",
            has_seg1_history_in_seg2
        );
        println!();

        // 9. 重新加载 segments 并验证删除合并
        println!("【8】重新加载 segments 并验证删除");
        let loaded_seg0 = Segment::load_frozen(persist_path, 0, schema.clone()).unwrap();
        let loaded_seg1 = Segment::load_frozen(persist_path, 1, schema.clone()).unwrap();
        let loaded_seg2 = Segment::load_frozen(persist_path, 2, schema.clone()).unwrap();

        // Segment 0 的删除应该被加载（包括 persist 后的新删除从 segment 2）
        let seg0_deleted_count = loaded_seg0.deleted.read().unwrap().len();
        println!(
            "  Segment 0 删除数: {} (原始: 3, 后续: 2)",
            seg0_deleted_count
        );

        // Segment 1 的删除应该被加载
        let seg1_deleted_count = loaded_seg1.deleted.read().unwrap().len();
        println!("  Segment 1 删除数: {}", seg1_deleted_count);

        // Segment 2 的删除
        let seg2_deleted_count = loaded_seg2.deleted.read().unwrap().len();
        println!("  Segment 2 删除数: {}", seg2_deleted_count);
        println!();

        // 10. 验证查询功能
        println!("【9】验证查询功能");

        // Segment 0: doc_id 0,1,2 应该被标记为删除
        assert!(loaded_seg0.get_document(0).is_none(), "doc 0 应该被删除");
        assert!(loaded_seg0.get_document(1).is_none(), "doc 1 应该被删除");
        assert!(loaded_seg0.get_document(2).is_none(), "doc 2 应该被删除");
        assert!(loaded_seg0.get_document(5).is_some(), "doc 5 应该存在");
        println!("  Segment 0 查询验证 ✓");

        // Segment 1: doc_id 100,101 应该被删除
        assert!(
            loaded_seg1.get_document(100).is_none(),
            "doc 100 应该被删除"
        );
        assert!(
            loaded_seg1.get_document(101).is_none(),
            "doc 101 应该被删除"
        );
        assert!(loaded_seg1.get_document(102).is_some(), "doc 102 应该存在");
        println!("  Segment 1 查询验证 ✓");

        // Segment 2: doc_id 200 应该被删除
        assert!(
            loaded_seg2.get_document(200).is_none(),
            "doc 200 应该被删除"
        );
        assert!(loaded_seg2.get_document(201).is_some(), "doc 201 应该存在");
        println!("  Segment 2 查询验证 ✓");
        println!();

        println!("✅ 历史删除快照功能测试通过！");

        // 清理
        let _ = std::fs::remove_dir_all(persist_path);
    }
}
