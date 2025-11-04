/// JSON 读写测试 Demo - 测试 1000 万条数据写入和查询
///
/// 运行方式：
/// ```bash
/// cargo run --example json_upsert_demo --release
/// ```
use calm::engine::{Engine, EngineConfig};
use calm::schema::{field::FieldOption, PersistPolicy, Schema};
use serde_json::json;
use std::time::{Duration, Instant};

#[tokio::main]
async fn main() {
    println!("🚀 JSON 读写测试 Demo - 1000万条数据\n");

    let engine = Engine::new(EngineConfig {
        data_dir: "./json_upsert_demo_data".into(),
        persist_check_interval_secs: 10,
        max_concurrent_persists: 4,
        check_after_flush: true,
    });
    let schema = create_schema();

    let partition = engine.create_partition(1, schema.clone()).await;

    // 测试参数
    let total_records = 10_000_000; // 1000万条数据
    let batch_size = 1_000;
    let total_batches = total_records / batch_size;

    println!("═══════════════════════════════════════════════════════════════");
    println!("[1] 写入阶段：写入 {} 条数据", total_records);
    println!("═══════════════════════════════════════════════════════════════\n");

    let start_write = Instant::now();

    for batch_num in 0..total_batches {
        let start_id = batch_num * batch_size;
        let batch_data = create_json_batch(start_id, batch_size);

        match partition.upsert_json(&batch_data) {
            Ok(_doc_ids) => {
                if (batch_num + 1) % 100 == 0 {
                    let progress = (batch_num + 1) as f64 / total_batches as f64 * 100.0;
                    let elapsed = start_write.elapsed().as_secs_f64();
                    let current_throughput = ((batch_num + 1) * batch_size) as f64 / elapsed;
                    println!(
                        "    进度: {:.1}% ({}/{} 批次, {} 条记录, {:.0} 条/秒)",
                        progress,
                        batch_num + 1,
                        total_batches,
                        (batch_num + 1) * batch_size,
                        current_throughput
                    );
                }
            }
            Err(e) => {
                println!("    ❌ 写入失败: {:?}", e);
                return;
            }
        }
    }

    let write_duration = start_write.elapsed();
    println!("\n    ✅ 写入完成!");
    println!("       - 总记录数: {} 条", partition.total_count());
    println!("       - 写入耗时: {:.2} 秒", write_duration.as_secs_f64());
    println!(
        "       - 平均吞吐量: {:.0} 条/秒",
        total_records as f64 / write_duration.as_secs_f64()
    );

    // 先 flush 确保数据可查询
    println!("\n═══════════════════════════════════════════════════════════════");
    println!("[2] 查询阶段：测试主键查询性能");
    println!("═══════════════════════════════════════════════════════════════\n");

    println!("    Flush数据以便查询...");
    partition.flush(false).ok();
    println!("    ✅ Flush 完成\n");

    // 先 flush 确保数据可查询
    println!("\n    Flush数据以便查询...");
    partition.flush(false).ok();

    // Step 5: 测试主键查询
    println!("\n[5] 测试主键查询 (使用 get_by_pk 方法)");
    // 选择不同位置的 ID 进行查询测试
    let test_ids = vec![
        "user_0000000000", // 第一条
        "user_0000001000", // 前面某条
        "user_0005000000", // 中间位置
        "user_0009999999", // 最后一条
    ];

    println!("    测试单个主键查询 ({} 条记录)...", test_ids.len());

    for test_id in &test_ids {
        let query_start = Instant::now();

        // 使用 get_by_pk 查询
        match partition.get_by_pk(&[test_id]) {
            Ok(Some(batch)) => {
                let query_time = query_start.elapsed().as_secs_f64() * 1000.0;
                println!(
                    "    ✅ 查询 ID: {} - 找到 {} 条记录 (耗时: {:.3} ms)",
                    test_id,
                    batch.num_rows(),
                    query_time
                );

                // 显示查询结果的部分数据
                if batch.num_rows() > 0 {
                    use arrow::array::Array;
                    if let Some(name_col) = batch.column_by_name("name") {
                        if let Some(name_array) = name_col
                            .as_any()
                            .downcast_ref::<arrow::array::StringArray>()
                        {
                            println!("       数据: name={}", name_array.value(0));
                        }
                    }
                }
            }
            Ok(None) => {
                println!("    ⚠️  查询 ID: {} - 未找到记录", test_id);
            }
            Err(e) => {
                println!("    ❌ 查询 ID: {} 失败: {:?}", test_id, e);
            }
        }
    }

    // 测试批量查询
    println!("\n    测试批量查询 (一次查询多个主键)...");
    let batch_query_start = Instant::now();
    let batch_ids = vec![
        "user_0000000000",
        "user_0001000000",
        "user_0005000000",
        "user_0009000000",
        "user_0009999999",
    ];

    match partition.get_by_pk(&batch_ids) {
        Ok(Some(batch)) => {
            let query_time = batch_query_start.elapsed().as_secs_f64() * 1000.0;
            println!(
                "    ✅ 批量查询 {} 个 ID - 找到 {} 条记录 (耗时: {:.3} ms)",
                batch_ids.len(),
                batch.num_rows(),
                query_time
            );
        }
        Ok(None) => {
            println!("    ⚠️  批量查询 - 未找到任何记录");
        }
        Err(e) => {
            println!("    ❌ 批量查询失败: {:?}", e);
        }
    }

    // 压力测试：随机查询性能
    println!("\n    压力测试：随机查询 1000 条记录...");
    let stress_test_start = Instant::now();
    let mut success_count = 0;
    let test_count = 1000;

    for i in 0..test_count {
        let random_id = format!("user_{:010}", i * 10000);
        if let Ok(Some(_)) = partition.get_by_pk(&[&random_id]) {
            success_count += 1;
        }
    }

    let stress_duration = stress_test_start.elapsed().as_secs_f64();
    println!(
        "    ✅ 完成 {} 次查询，成功 {} 次",
        test_count, success_count
    );
    println!("       - 总耗时: {:.2} 秒", stress_duration);
    println!(
        "       - 平均延迟: {:.3} ms",
        (stress_duration * 1000.0) / test_count as f64
    );
    println!("       - QPS: {:.0}", test_count as f64 / stress_duration);

    // Shutdown
    println!("\n═══════════════════════════════════════════════════════════════");
    println!("[3] 关闭 Engine");
    println!("═══════════════════════════════════════════════════════════════\n");
    engine.shutdown().await;
    println!("    ✅ Engine 已关闭");

    println!("\n{}", "=".repeat(80));
    println!("✨ 测试完成！\n");
    println!("� 性能总结:");
    println!("   ✅ 写入: {} 条记录", total_records);
    println!(
        "   ✅ 写入吞吐量: {:.0} 条/秒",
        total_records as f64 / write_duration.as_secs_f64()
    );
    println!(
        "   ✅ 查询 QPS: {:.0}",
        test_count as f64 / stress_duration
    );
    println!(
        "   ✅ 平均查询延迟: {:.3} ms",
        (stress_duration * 1000.0) / test_count as f64
    );
    println!();
}

fn create_schema() -> Schema {
    Schema {
        name: "json_upsert_schema".to_string(),
        primary_key: Some("id".to_string()), // 设置主键
        store_source: false,
        fields: vec![
            FieldOption::Keyword {
                name: "id".to_string(),
                index: true,
                is_array: false,
                persist_option: None,
                case_sensitive: true,
            },
            FieldOption::Keyword {
                name: "name".to_string(),
                index: true,
                is_array: false,
                persist_option: None,
                case_sensitive: true,
            },
            FieldOption::Keyword {
                name: "age".to_string(), // 改为字符串类型
                index: true,
                is_array: false,
                persist_option: None,
                case_sensitive: true,
            },
            FieldOption::Keyword {
                name: "city".to_string(),
                index: true,
                is_array: false,
                persist_option: None,
                case_sensitive: true,
            },
            FieldOption::Keyword {
                name: "score".to_string(), // 改为字符串类型
                index: true,
                is_array: false,
                persist_option: None,
                case_sensitive: true,
            },
        ],
        persist_policy: PersistPolicy {
            max_docs_per_segment: 10_000,
            max_segment_age: Duration::from_secs(30),
        },
    }
}

fn create_json_batch(start_id: usize, count: usize) -> Vec<serde_json::Value> {
    let mut batch = Vec::with_capacity(count);
    let cities = vec!["北京", "上海", "广州", "深圳", "杭州"];

    for i in start_id..start_id + count {
        let age = 20 + (i % 60);
        let score = (i % 100) as f64 + (i as f64 % 1000.0) / 1000.0;

        let json_obj = json!({
            "id": format!("user_{:010}", i),
            "name": format!("用户{}", i),
            "age": age.to_string(),  // 转为字符串
            "city": cities[i % 5],
            "score": format!("{:.3}", score),  // 转为字符串
        });
        batch.push(json_obj);
    }

    batch
}
