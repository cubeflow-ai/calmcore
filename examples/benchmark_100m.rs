/// 100M Data Benchmark - 测试 Parquet 压缩效果
///
/// 运行方式：
/// ```bash
/// cargo run --example benchmark_100m --release
/// ```
use arrow::array::{ArrayRef, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use calm::engine::{Engine, EngineConfig};
use calm::schema::{field::FieldOption, PersistPolicy, Schema};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[tokio::main]
async fn main() {
    println!("\n🚀 100M Data Benchmark - Parquet Compression Test\n");
    println!("{}", "=".repeat(60));

    // Step 1: Create Engine
    println!("\n[1] 创建 Engine");
    let engine = Engine::new(EngineConfig {
        data_dir: "./benchmark_100m_data".into(),
        persist_check_interval_secs: 5,
        max_concurrent_persists: 4,
        check_after_flush: true,
    });
    println!("    ✅ Engine 已创建");

    // Step 2: Define Schema
    println!("\n[2] 定义 Schema");
    let schema = create_schema();
    println!("    ✅ Schema 已创建: 3 个 Keyword 字段");

    // Step 3: Create Partition
    println!("\n[3] 创建 Partition");
    let partition_id = 1;
    let partition = engine.create_partition(partition_id, schema.clone()).await;
    println!("    ✅ Partition {} 已创建", partition_id);

    // Step 4: Write data in batches
    println!("\n[4] 写入数据 (目标: ~100MB 原始数据)");
    println!("    每批次: 1,000 条记录");

    let batch_size = 1_000;
    let total_records = 1_000_000; // 100万条记录
    let total_batches = total_records / batch_size;

    let start_write = Instant::now();
    let mut total_bytes = 0u64;

    for batch_num in 0..total_batches {
        let start_id = batch_num * batch_size;
        let batch = create_data_batch(start_id, batch_size);

        // 估算原始数据大小
        let batch_bytes = estimate_batch_size(&batch);
        total_bytes += batch_bytes as u64;

        if let Err(e) = partition.upsert(batch) {
            println!("    ❌ 写入失败: {:?}", e);
            return;
        }

        // 每写入 10,000 条(10个batch)就 flush 一次,生成新的 segment
        if (batch_num + 1) % 10 == 0 {
            let progress = (batch_num + 1) as f64 / total_batches as f64 * 100.0;
            let mb = total_bytes as f64 / 1024.0 / 1024.0;
            let current_count = (batch_num + 1) * batch_size;

            // Flush 当前 segment
            if let Ok(_) = partition.flush(false) {
                println!(
                    "    进度: {:.1}% ({}/{} 批次, ~{:.1} MB, {} 条记录, {} segments)",
                    progress,
                    batch_num + 1,
                    total_batches,
                    mb,
                    current_count,
                    partition.frozen_count()
                );
            }
        }
    }

    let write_duration = start_write.elapsed();
    let total_mb = total_bytes as f64 / 1024.0 / 1024.0;
    println!("\n    ✅ 写入完成!");
    println!("       - 总记录数: {} 条", total_records);
    println!("       - 原始数据大小: {:.2} MB", total_mb);
    println!("       - 写入耗时: {:.2} 秒", write_duration.as_secs_f64());
    println!(
        "       - 吞吐量: {:.0} 条/秒",
        total_records as f64 / write_duration.as_secs_f64()
    );

    // Step 5: Flush
    println!("\n[5] Flush segments");
    println!(
        "    当前: {} 条记录, {} 个 frozen segments",
        partition.total_count(),
        partition.frozen_count()
    );
    if let Ok(_) = partition.flush(false) {
        println!(
            "    ✅ Flush 完成: {} 个 frozen segments",
            partition.frozen_count()
        );
    }

    // Step 6: Trigger persistence
    println!("\n[6] 触发持久化 (Parquet + ZSTD 压缩)");
    let start_persist = Instant::now();
    engine.trigger_persist(partition_id as u64);

    // Wait for persistence to complete
    tokio::time::sleep(Duration::from_secs(3)).await;

    let persist_duration = start_persist.elapsed();
    println!("    ✅ 持久化完成");
    println!("       - 耗时: {:.2} 秒", persist_duration.as_secs_f64());

    // Step 7: Calculate compression ratio
    println!("\n[7] 分析压缩效果");
    let disk_size = calculate_directory_size("./benchmark_100m_data").unwrap_or(0);
    let disk_mb = disk_size as f64 / 1024.0 / 1024.0;
    let compression_ratio = total_mb / disk_mb;

    println!("    📊 压缩统计:");
    println!("       - 原始数据大小: {:.2} MB", total_mb);
    println!("       - 磁盘占用大小: {:.2} MB", disk_mb);
    println!("       - 压缩比: {:.2}:1", compression_ratio);
    println!(
        "       - 压缩率: {:.1}%",
        (1.0 - disk_mb / total_mb) * 100.0
    );
    println!("       - 节省空间: {:.2} MB", total_mb - disk_mb);

    // Step 8: Test loading from disk
    println!("\n[8] 测试从磁盘加载");
    let start_load = Instant::now();

    // Shutdown and recreate engine
    engine.shutdown().await;

    let engine2 = Engine::new(EngineConfig {
        data_dir: "./benchmark_100m_data".into(),
        persist_check_interval_secs: 5,
        max_concurrent_persists: 4,
        check_after_flush: true,
    });

    match engine2.load_partition(partition_id, schema).await {
        Ok(loaded) => {
            let load_duration = start_load.elapsed();
            println!("    ✅ 加载成功");
            println!("       - 总文档数: {}", loaded.total_count());
            println!("       - Frozen Segments: {}", loaded.frozen_count());
            println!("       - 加载耗时: {:.2} 秒", load_duration.as_secs_f64());
            println!(
                "       - 加载速度: {:.2} MB/s",
                disk_mb / load_duration.as_secs_f64()
            );
        }
        Err(e) => {
            println!("    ❌ 加载失败: {:?}", e);
        }
    }

    // Step 9: Shutdown
    println!("\n[9] 关闭 Engine");
    engine2.shutdown().await;
    println!("    ✅ Engine 已关闭");

    println!("\n{}", "=".repeat(60));
    println!("✨ Benchmark 完成！\n");
    println!("📈 性能总结:");
    println!(
        "   写入吞吐: {:.0} 条/秒",
        total_records as f64 / write_duration.as_secs_f64()
    );
    println!(
        "   压缩比: {:.2}:1 (节省 {:.1}% 空间)",
        compression_ratio,
        (1.0 - 1.0 / compression_ratio) * 100.0
    );
    println!(
        "   持久化速度: {:.2} MB/s",
        total_mb / persist_duration.as_secs_f64()
    );
    println!();
}

fn create_schema() -> Schema {
    Schema {
        name: "benchmark_schema".to_string(),
        primary_key: Some("id".to_string()),
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
                name: "description".to_string(),
                index: true,
                is_array: false,
                persist_option: None,
                case_sensitive: true,
            },
        ],
        persist_policy: PersistPolicy {
            max_docs_per_segment: 10_000,
            max_segment_age: Duration::from_secs(10),
            check_on_flush: true,
        },
    }
}

fn create_data_batch(start_id: usize, count: usize) -> RecordBatch {
    // Create Arrow schema
    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("description", DataType::Utf8, false),
    ]));

    // Generate data
    let ids: Vec<String> = (start_id..start_id + count)
        .map(|i| format!("user_{:010}", i))
        .collect();

    let names: Vec<String> = (start_id..start_id + count)
        .map(|i| format!("张三{}_李四{}_王五{}", i % 100, i % 200, i % 300))
        .collect();

    let descriptions: Vec<String> = (start_id..start_id + count)
        .map(|i| {
            format!(
                "这是一个测试用户的详细描述信息,包含了用户ID:{},用户编号:{},注册时间:2024-{:02}-{:02},地区:{}省{}市,爱好:编程、阅读、旅游,座右铭:代码改变世界!",
                i,
                i * 13 % 10000,
                (i % 12) + 1,
                (i % 28) + 1,
                ["广东", "北京", "上海", "深圳", "浙江"][i % 5],
                ["深圳", "广州", "杭州", "成都", "西安"][i % 5],
            )
        })
        .collect();

    // Create RecordBatch
    RecordBatch::try_new(
        arrow_schema,
        vec![
            Arc::new(StringArray::from(ids)) as ArrayRef,
            Arc::new(StringArray::from(names)) as ArrayRef,
            Arc::new(StringArray::from(descriptions)) as ArrayRef,
        ],
    )
    .expect("Failed to create RecordBatch")
}

fn estimate_batch_size(batch: &RecordBatch) -> usize {
    use arrow::array::Array;
    let mut size = 0;
    for col in batch.columns() {
        if let Some(string_array) = col.as_any().downcast_ref::<StringArray>() {
            for i in 0..string_array.len() {
                size += string_array.value(i).as_bytes().len();
            }
        }
    }
    size
}

fn calculate_directory_size(path: &str) -> Result<u64, Box<dyn std::error::Error>> {
    use std::fs;

    let mut total_size = 0u64;

    fn visit_dirs(dir: &std::path::Path, size: &mut u64) -> std::io::Result<()> {
        if dir.is_dir() {
            for entry in fs::read_dir(dir)? {
                let entry = entry?;
                let path = entry.path();
                if path.is_dir() {
                    visit_dirs(&path, size)?;
                } else {
                    *size += entry.metadata()?.len();
                }
            }
        }
        Ok(())
    }

    visit_dirs(std::path::Path::new(path), &mut total_size)?;
    Ok(total_size)
}
