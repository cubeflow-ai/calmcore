//! Partition 高压力测试 - 简化版（直接构建）
//!
//! 注意：这个测试**不走生产路径**（Catalog → Engine），
//! 而是直接使用 Partition::new 以专注测试 Partition 本身的并发性能。
//!
//! 生产路径应该是：
//!   1. Catalog.create_partition() - 创建目录和元数据
//!   2. Engine.load_partition() - 加载 Partition
//!   3. 写入数据
//!
//! 本测试为性能基准测试，绕过上层复杂性。

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::*;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use datafusion::arrow;
use rand::Rng;
use sysinfo::System;
use tokio::sync::mpsc;
use tokio::time::sleep;

use calm::catalog::schema::{field::FieldOption, PersistPolicy, Schema as CalmSchema};
use calm::partition::Partition;

/// 测试统计信息
struct TestStats {
    batches_written: AtomicU64,
    records_written: AtomicU64,
    bytes_written: AtomicU64,
    errors: AtomicU64,
}

impl TestStats {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            batches_written: AtomicU64::new(0),
            records_written: AtomicU64::new(0),
            bytes_written: AtomicU64::new(0),
            errors: AtomicU64::new(0),
        })
    }

    fn print(&self, elapsed: Duration) {
        let batches = self.batches_written.load(Ordering::Relaxed);
        let records = self.records_written.load(Ordering::Relaxed);
        let bytes = self.bytes_written.load(Ordering::Relaxed);
        let errors = self.errors.load(Ordering::Relaxed);

        let secs = elapsed.as_secs_f64();
        let batches_per_sec = batches as f64 / secs;
        let records_per_sec = records as f64 / secs;
        let mb_per_sec = (bytes as f64 / 1024.0 / 1024.0) / secs;

        println!("\n========== 测试统计 ==========");
        println!("运行时间: {:.2}s", secs);
        println!("写入批次: {} ({:.2} batch/s)", batches, batches_per_sec);
        println!("写入记录: {} ({:.2} rec/s)", records, records_per_sec);
        println!(
            "写入字节: {} MB ({:.2} MB/s)",
            bytes / 1024 / 1024,
            mb_per_sec
        );
        println!("错误次数: {}", errors);
        println!("==============================\n");
    }
}

/// 创建测试Schema（40个字段）
fn create_test_schema() -> CalmSchema {
    let mut fields = vec![
        // 主键
        FieldOption::U64 {
            name: "event_id".to_string(),
            index: true,
            description: None,
            default_value: None,
            nullable: false,
        },
    ];

    // 3个大字段（不索引）
    for i in 1..=3 {
        fields.push(FieldOption::Keyword {
            name: format!("large_field_{}", i),
            index: false,
            is_array: false,
            persist_option: None,
            case_sensitive: true,
            description: None,
            default_value: None,
            nullable: true,
        });
    }

    // 37个小字段（全部索引）
    fields.push(FieldOption::U64 {
        name: "user_id".to_string(),
        index: true,
        description: None,
        default_value: None,
        nullable: false,
    });

    fields.push(FieldOption::I64 {
        name: "age".to_string(),
        index: true,
        description: None,
        default_value: None,
        nullable: true,
    });

    fields.push(FieldOption::F64 {
        name: "score".to_string(),
        index: true,
        description: None,
        default_value: None,
        nullable: true,
    });

    fields.push(FieldOption::Keyword {
        name: "name".to_string(),
        index: true,
        is_array: false,
        persist_option: None,
        case_sensitive: true,
        description: None,
        default_value: None,
        nullable: true,
    });

    fields.push(FieldOption::Keyword {
        name: "description".to_string(),
        index: true,
        is_array: false,
        persist_option: None,
        case_sensitive: true,
        description: None,
        default_value: None,
        nullable: true,
    });

    fields.push(FieldOption::Boolean {
        name: "is_active".to_string(),
        index: true,
        description: None,
        default_value: None,
        nullable: true,
    });

    fields.push(FieldOption::Timestamp {
        name: "created_at".to_string(),
        index: true,
        format: Some("ms".to_string()),
        description: None,
        default_value: None,
        nullable: false,
    });

    fields.push(FieldOption::Timestamp {
        name: "updated_at".to_string(),
        index: true,
        format: Some("ms".to_string()),
        description: None,
        default_value: None,
        nullable: true,
    });

    // 凑够40个字段
    for i in 1..=32 {
        match i % 5 {
            0 => fields.push(FieldOption::U64 {
                name: format!("field_{}", i),
                index: true,
                description: None,
                default_value: None,
                nullable: true,
            }),
            1 => fields.push(FieldOption::I64 {
                name: format!("field_{}", i),
                index: true,
                description: None,
                default_value: None,
                nullable: true,
            }),
            2 => fields.push(FieldOption::F64 {
                name: format!("field_{}", i),
                index: true,
                description: None,
                default_value: None,
                nullable: true,
            }),
            _ => fields.push(FieldOption::Keyword {
                name: format!("field_{}", i),
                index: true,
                is_array: false,
                persist_option: None,
                case_sensitive: true,
                description: None,
                default_value: None,
                nullable: true,
            }),
        }
    }

    CalmSchema {
        name: "stress_test_events".to_string(),
        primary_key: Some("event_id".to_string()),
        store_source: true,
        fields,
        persist_policy: PersistPolicy::default(),
        description: Some("High pressure stress test table".to_string()),
    }
}

/// 生成真实的随机测试数据（避免重复导致压缩率虚高）
fn generate_large_batch(schema: Arc<Schema>, batch_size: usize, start_id: u64) -> RecordBatch {
    let mut rng = rand::rng();

    // 生成真实的随机大字符串（每行不同）
    let large_field_1: Vec<String> = (0..batch_size)
        .map(|_| {
            let len = rng.random_range(800..1200); // 800B-1200B 变长
            (0..len)
                .map(|_| {
                    let chars = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
                    chars[rng.random_range(0..chars.len())] as char
                })
                .collect()
        })
        .collect();

    let large_field_2: Vec<String> = (0..batch_size)
        .map(|_| {
            let len = rng.random_range(800..1200);
            (0..len)
                .map(|_| {
                    let chars = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
                    chars[rng.random_range(0..chars.len())] as char
                })
                .collect()
        })
        .collect();

    let large_field_3: Vec<String> = (0..batch_size)
        .map(|_| {
            let len = rng.random_range(800..1200);
            (0..len)
                .map(|_| {
                    let chars = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
                    chars[rng.random_range(0..chars.len())] as char
                })
                .collect()
        })
        .collect();

    let mut columns: Vec<Arc<dyn arrow::array::Array>> = vec![
        // event_id (唯一主键)
        Arc::new(UInt64Array::from_iter_values(
            start_id..start_id + batch_size as u64,
        )),
        // large_field_1/2/3 (真实随机大字段)
        Arc::new(StringArray::from_iter_values(
            large_field_1.iter().map(|s| s.as_str()),
        )),
        Arc::new(StringArray::from_iter_values(
            large_field_2.iter().map(|s| s.as_str()),
        )),
        Arc::new(StringArray::from_iter_values(
            large_field_3.iter().map(|s| s.as_str()),
        )),
        // user_id (随机分布，100万用户空间)
        Arc::new(UInt64Array::from_iter_values(
            (0..batch_size).map(|_| rng.random_range(1..1_000_000)),
        )),
        // age (正态分布，18-80岁)
        Arc::new(Int64Array::from_iter_values(
            (0..batch_size).map(|_| rng.random_range(18..80) as i64),
        )),
        // score (正态分布，0-100分)
        Arc::new(Float64Array::from_iter_values(
            (0..batch_size).map(|_| rng.random_range(0.0..100.0)),
        )),
        // name (随机姓名，包含中文和英文)
        Arc::new(StringArray::from_iter_values((0..batch_size).map(|_| {
            let names = [
                "张三", "李四", "王五", "赵六", "Alice", "Bob", "Charlie", "Diana",
            ];
            format!(
                "{}_{}",
                names[rng.random_range(0..names.len())],
                rng.random_range(0..10000)
            )
        }))),
        // description (随机描述文本，不同长度)
        Arc::new(StringArray::from_iter_values((0..batch_size).map(|_| {
            let len = rng.random_range(10..100);
            (0..len)
                .map(|_| {
                    let chars = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
                    chars[rng.random_range(0..chars.len())] as char
                })
                .collect::<String>()
        }))),
        // is_active (随机布尔值，70% true)
        Arc::new(BooleanArray::from(
            (0..batch_size)
                .map(|_| rng.random_bool(0.7))
                .collect::<Vec<_>>(),
        )),
        // created_at (随机时间戳，最近一年内)
        Arc::new(TimestampMillisecondArray::from_iter_values(
            (0..batch_size).map(|_| {
                let now = chrono::Utc::now().timestamp_millis();
                now - rng.random_range(0..365 * 24 * 3600 * 1000) // 最近一年
            }),
        )),
        // updated_at (在 created_at 之后)
        Arc::new(TimestampMillisecondArray::from_iter_values(
            (0..batch_size).map(|_| {
                chrono::Utc::now().timestamp_millis() - rng.random_range(0..30 * 24 * 3600 * 1000)
                // 最近30天
            }),
        )),
    ];

    // 剩余32个字段（真实随机数据）
    for i in 1..=32 {
        match i % 5 {
            0 => columns.push(Arc::new(UInt64Array::from_iter_values(
                (0..batch_size).map(|_| rng.random_range(0..1_000_000)),
            ))),
            1 => columns.push(Arc::new(Int64Array::from_iter_values(
                (0..batch_size).map(|_| rng.random_range(-10000..10000)),
            ))),
            2 => columns.push(Arc::new(Float64Array::from_iter_values(
                (0..batch_size).map(|_| rng.random_range(0.0..1000.0)),
            ))),
            _ => {
                let random_strings: Vec<String> = (0..batch_size)
                    .map(|_| {
                        let len = rng.random_range(5..30);
                        (0..len)
                            .map(|_| {
                                let chars = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
                                chars[rng.random_range(0..chars.len())] as char
                            })
                            .collect()
                    })
                    .collect();
                columns.push(Arc::new(StringArray::from_iter_values(
                    random_strings.iter().map(|s| s.as_str()),
                )))
            }
        }
    }

    RecordBatch::try_new(schema, columns).expect("Failed to create RecordBatch")
}

/// 工作线程
async fn worker_thread(
    worker_id: usize,
    partition: Arc<Partition>,
    schema: Arc<Schema>,
    stats: Arc<TestStats>,
    batch_size: usize,
    batches_per_worker: usize,
) {
    let mut start_id = (worker_id * batches_per_worker * batch_size) as u64;

    for batch_num in 0..batches_per_worker {
        let batch = generate_large_batch(schema.clone(), batch_size, start_id);
        start_id += batch_size as u64;

        // 实际计算 batch 的字节数（更精确）
        let batch_bytes = batch.get_array_memory_size();

        match partition.upsert(batch) {
            Ok(_) => {
                stats.batches_written.fetch_add(1, Ordering::Relaxed);
                stats
                    .records_written
                    .fetch_add(batch_size as u64, Ordering::Relaxed);
                stats
                    .bytes_written
                    .fetch_add(batch_bytes as u64, Ordering::Relaxed);

                if batch_num % 10 == 0 {
                    println!(
                        "[Worker {}] Batch {}/{} done",
                        worker_id,
                        batch_num + 1,
                        batches_per_worker
                    );
                }
            }
            Err(e) => {
                stats.errors.fetch_add(1, Ordering::Relaxed);
                eprintln!("[Worker {}] Error: {:?}", worker_id, e);
            }
        }
    }

    println!("[Worker {}] Completed!", worker_id);
}

/// 监控线程
async fn monitor_thread(partition: Arc<Partition>, stats: Arc<TestStats>, start_time: Instant) {
    loop {
        sleep(Duration::from_secs(5)).await;

        let total_count = partition.total_count();
        let frozen_segments = partition.get_frozen_segments();
        let unpersisted_segments = partition.get_unpersisted_segments();

        println!("\n========== Partition 状态 ==========");
        println!("总文档数: {}", total_count);
        println!("Frozen Segments: {}", frozen_segments.len());
        println!("Unpersisted Segments: {}", unpersisted_segments.len());
        println!("====================================");

        stats.print(start_time.elapsed());

        let mut sys = System::new_all();
        sys.refresh_memory();
        println!(
            "系统内存: {} / {} MB",
            sys.used_memory() / 1024 / 1024,
            sys.total_memory() / 1024 / 1024
        );
        println!();
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    println!("\n========== Partition 高压力测试（直接构建版）==========");
    println!("⚠️  注意：此测试直接使用 Partition::new，不走生产路径");
    println!("   生产路径应为：Catalog → Engine.load_partition → Partition");
    println!("   本测试专注于 Partition 本身的性能基准\n");

    // 测试参数
    let num_workers = 8;
    let batch_size = 1000;
    let batches_per_worker = 100;
    let total_records = num_workers * batch_size * batches_per_worker;

    println!("并发线程: {}", num_workers);
    println!("批大小: {}", batch_size);
    println!(
        "预计总记录: {} ({} MB)",
        total_records,
        total_records * 8 / 1024
    );
    println!();

    // 1. 创建目录和Schema
    let data_dir = PathBuf::from("/tmp/calm_stress_test");
    let _ = std::fs::remove_dir_all(&data_dir);
    let partition_dir = data_dir.join("partition_0");
    std::fs::create_dir_all(&partition_dir)?;

    let calm_schema = create_test_schema();
    let arrow_schema = calm_schema.to_arrow_schema();

    println!("✓ Schema 创建: {} 字段", calm_schema.fields.len());

    // 2. 直接创建 Partition（绕过 Catalog/Engine）
    let (persist_tx, mut persist_rx) = mpsc::unbounded_channel::<(String, String)>();

    let partition = Arc::new(Partition::new(
        "partition_0".to_string(),
        "stress_test_events".to_string(),
        partition_dir.clone(),
        calm_schema,
        persist_tx,
    ));

    println!("✓ Partition 直接创建成功（未经过 Catalog/Engine）\n");

    // 后台处理持久化通知
    tokio::spawn(async move {
        while let Some((table, part)) = persist_rx.recv().await {
            log::debug!("持久化通知: {}/{}", table, part);
        }
    });

    // 3. 启动监控
    let stats = TestStats::new();
    let start_time = Instant::now();

    let monitor_partition = partition.clone();
    let monitor_stats = stats.clone();
    tokio::spawn(async move {
        monitor_thread(monitor_partition, monitor_stats, start_time).await;
    });

    // 4. 启动工作线程（并发写入性能测试）
    println!("开始写入数据...\n");
    let mut handles = Vec::new();

    for worker_id in 0..num_workers {
        let worker_partition = partition.clone();
        let worker_schema = arrow_schema.clone();
        let worker_stats = stats.clone();

        let handle = tokio::spawn(async move {
            worker_thread(
                worker_id,
                worker_partition,
                worker_schema,
                worker_stats,
                batch_size,
                batches_per_worker,
            )
            .await;
        });

        handles.push(handle);
    }

    // 5. 等待完成
    for handle in handles {
        handle.await?;
    }

    println!("\n所有工作线程完成！");
    stats.print(start_time.elapsed());

    // 6. 最终状态
    let total_count = partition.total_count();
    let frozen_segments = partition.get_frozen_segments();
    let unpersisted_segments = partition.get_unpersisted_segments();

    println!("\n========== 最终状态 ==========");
    println!("总文档数: {}", total_count);
    println!("Frozen Segments: {}", frozen_segments.len());
    println!("Unpersisted Segments: {}", unpersisted_segments.len());
    println!("==============================\n");

    // 7. 持久化测试
    println!("开始持久化...");
    let persist_start = Instant::now();

    match partition.persist_all() {
        Ok(_) => {
            println!(
                "✓ 持久化完成, 耗时 {:.2}s",
                persist_start.elapsed().as_secs_f64()
            );
        }
        Err(e) => {
            eprintln!("✗ 持久化失败: {:?}", e);
        }
    }

    println!("\n✅ 测试完成！");
    println!("📁 数据目录: {}", data_dir.display());
    println!("📊 测试方式: Partition::new（直接构建，非生产路径）");
    println!("\n💡 提示：生产环境应使用：Catalog.create_partition → Engine.load_partition\n");

    Ok(())
}
