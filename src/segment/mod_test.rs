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
                .write(
                    &data,
                    None,
                    None,
                    &RwLock::new(()),
                    FieldIndexMode::Sync,
                )
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
                .write(
                    &data,
                    None,
                    None,
                    &RwLock::new(()),
                    FieldIndexMode::Sync,
                )
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

    #[test]
    fn test_segment_3m_performance() {
        println!("\n=== 开始300万数据性能测试（每100万flush一次） ===\n"); // 1. 创建 schema
        let schema = Arc::new(Schema {
            name: "test_schema".to_string(),
            primary_key: Some("id".to_string()),
            store_source: false,
            fields: vec![
                FieldOption::Keyword {
                    name: "id".to_string(), // 主键字段
                    is_array: false,
                    index: true,
                },
                FieldOption::Keyword {
                    name: "tags".to_string(),
                    is_array: false,
                    index: true,
                },
            ],
        });

        let total_records = 3_000_000;
        let flush_interval = 1_000_000; // 每100万条flush一次
        let batch_size = 10_000; // 每批1万条

        println!(
            "📝 写入策略: 总共{}条, 每{}条flush一次, 批大小: {}",
            total_records, flush_interval, batch_size
        );

        let persist_path = "/tmp/test_segment_3m_perf";
        let _ = std::fs::remove_dir_all(persist_path);
        std::fs::create_dir_all(persist_path).unwrap();

        // 2. 存储所有segments
        let mut segments: Vec<Arc<Segment>> = Vec::new();

        let total_start = std::time::Instant::now();
        let mut total_write_time = std::time::Duration::ZERO;

        // 3. 分3个segment写入（每个100万条）
        for seg_idx in 0..3 {
            let start_id = seg_idx * flush_interval;
            let end_id = start_id + flush_interval;

            println!(
                "\n--- Segment {} (docs {}-{}) ---",
                seg_idx,
                start_id,
                end_id - 1
            );

            let segment = Arc::new(Segment::new(start_id as u64, schema.clone()));
            let segment_clone = segment.clone();

            // 写入数据
            let write_start = std::time::Instant::now();
            let batch_count = flush_interval / batch_size;

            for batch in 0..batch_count {
                let mut id_builder = StringBuilder::new();
                let mut tags_builder = StringBuilder::new();

                for i in 0..batch_size {
                    let doc_id = start_id + batch * batch_size + i;
                    id_builder.append_value(&format!("id{}", doc_id));
                    tags_builder.append_value(&format!("tag{}", doc_id % 1000));
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
                    .write(
                        &data,
                        None,
                        None,
                        &RwLock::new(()),
                        FieldIndexMode::Sync,
                    )
                    .unwrap();

                // 每20批输出一次进度
                if (batch + 1) % 20 == 0 || batch + 1 == batch_count {
                    println!(
                        "  写入进度: {}/{} 批 ({:.1}%)",
                        batch + 1,
                        batch_count,
                        ((batch + 1) as f64 / batch_count as f64) * 100.0
                    );
                }
            }

            let write_elapsed = write_start.elapsed();
            total_write_time += write_elapsed;
            let write_throughput = flush_interval as f64 / write_elapsed.as_secs_f64();

            println!(
                "  ✅ 写入完成: {:?} ({:.2} 条/秒)",
                write_elapsed, write_throughput
            );
            println!("  📦 Flush segment {} (冻结到内存)", seg_idx);

            segments.push(segment_clone);
        }

        let write_throughput = total_records as f64 / total_write_time.as_secs_f64();

        println!("\n✅ 所有数据写入完成！");
        println!("  总写入时间: {:?}", total_write_time);
        println!("  写入吞吐量: {:.2} 条/秒", write_throughput);
        println!("  创建了 {} 个segment", segments.len());

        // 4. 后台持久化所有segment (顺序执行，模拟后台任务)
        println!("\n💾 开始后台持久化...");
        let persist_start = std::time::Instant::now();
        let mut persist_times = Vec::new();

        for (seg_idx, segment) in segments.iter().enumerate() {
            // 构建历史segments（之前已持久化的）
            let history_segments: Vec<(u64, Arc<Segment>)> = segments
                .iter()
                .enumerate()
                .filter(|(i, _)| *i < seg_idx)
                .map(|(i, s)| (i as u64, s.clone()))
                .collect();

            let seg_persist_start = std::time::Instant::now();
            segment
                .persist(persist_path, seg_idx as u64, &history_segments)
                .expect("Persist failed");
            let seg_persist_elapsed = seg_persist_start.elapsed();

            persist_times.push(seg_persist_elapsed);
            println!(
                "  ✅ Segment {} 持久化完成: {:?}",
                seg_idx, seg_persist_elapsed
            );
        }

        let total_persist_time = persist_start.elapsed();
        let persist_throughput = total_records as f64 / total_persist_time.as_secs_f64();
        println!("\n  总持久化时间: {:?}", total_persist_time);
        println!("  持久化吞吐量: {:.2} 条/秒", persist_throughput); // 5. 检查所有segment的文件大小
        println!("\n📊 检查磁盘空间...");

        fn dir_size(path: &str) -> u64 {
            let mut size = 0u64;
            if let Ok(entries) = std::fs::read_dir(path) {
                for entry in entries.flatten() {
                    if let Ok(metadata) = entry.metadata() {
                        if metadata.is_dir() {
                            size += dir_size(&entry.path().to_string_lossy());
                        } else {
                            size += metadata.len();
                        }
                    }
                }
            }
            size
        }

        let total_size = dir_size(persist_path);
        println!("  总大小: {:.2} MB", total_size as f64 / 1024.0 / 1024.0);
        println!(
            "  平均每条记录: {:.2} bytes",
            total_size as f64 / total_records as f64
        );

        // 6. 测试加载性能 - 加载所有3个segment
        println!("\n📂 测试加载所有segment...");
        let mut loaded_segments = Vec::new();
        let mut total_load_time = std::time::Duration::ZERO;

        for seg_idx in 0..3 {
            let start_id = (seg_idx * flush_interval) as u64;
            let end_id = start_id + flush_interval as u64 - 1;

            let load_start = std::time::Instant::now();
            let loaded_seg =
                Segment::load_frozen(persist_path, start_id, end_id, schema.clone()).unwrap();
            let load_elapsed = load_start.elapsed();

            total_load_time += load_elapsed;
            println!("  ✅ Segment {} 加载完成: {:?}", seg_idx, load_elapsed);
            loaded_segments.push(loaded_seg);
        }

        println!("\n  总加载时间: {:?}", total_load_time);

        // 7. 验证数据完整性
        println!("\n🔍 验证数据完整性...");
        let total_docs: u32 = loaded_segments.iter().map(|s| s.doc_count()).sum();
        assert_eq!(total_docs, total_records as u32);
        println!("  总doc_count: {} ✓", total_docs);

        // 8. 测试跨segment查询性能
        println!("\n🔎 测试跨segment查询性能 (100次)...");
        let query_start = std::time::Instant::now();
        let mut found_count = 0;

        for i in 0..100 {
            let doc_id = (i * 30000) % total_records;
            // 确定在哪个segment
            let seg_idx = doc_id / flush_interval;
            let local_doc_id = (doc_id % flush_interval) as u32;

            if let Some(_batch) = loaded_segments[seg_idx].get_document(local_doc_id) {
                found_count += 1;
            }
        }

        let query_time = query_start.elapsed();
        println!("✅ 查询完成！");
        println!("  总耗时: {:?}", query_time);
        println!("  平均每次查询: {:?}", query_time / 100);
        println!("  成功查询: {}/100", found_count);

        // 清理
        println!("\n🧹 清理测试数据...");
        let _ = std::fs::remove_dir_all(persist_path);

        // 计算总时间和综合吞吐量
        let total_elapsed = total_start.elapsed();
        let overall_throughput = total_records as f64 / total_elapsed.as_secs_f64();

        println!("\n=== 性能测试完成！===");
        println!("\n📊 性能摘要:");
        println!("  - 写入吞吐量: {:.2} 条/秒 (纯写入)", write_throughput);
        println!(
            "  - 持久化吞吐量: {:.2} 条/秒 (后台并行)",
            persist_throughput
        );
        println!("  - 综合吞吐量: {:.2} 条/秒 (端到端)", overall_throughput);
        println!("  - 总耗时: {:?}", total_elapsed);
        println!("    * 写入时间: {:?}", total_write_time);
        println!("    * 持久化时间: {:?} (最慢segment)", total_persist_time);
        println!("  - 加载时间: {:?} (3个segment)", total_load_time);
        println!("  - 平均查询耗时: {:?}", query_time / 100);
        println!(
            "  - 磁盘空间: {:.2} MB ({:.2} bytes/doc)",
            total_size as f64 / 1024.0 / 1024.0,
            total_size as f64 / total_records as f64
        );
        println!("  - Segment数量: {}", segments.len());
    }
}
