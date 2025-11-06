use calm::{
    partition::Partition,
    schema::{compute::PartitionTableProvider, field::FieldOption, Schema},
};
use datafusion::{
    arrow::{
        array::{Int64Array, StringArray},
        datatypes::{DataType, Field, Schema as ArrowSchema},
        record_batch::RecordBatch,
    },
    prelude::*,
};
use std::{path::PathBuf, sync::Arc, time::Instant};
use tokio::sync::mpsc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Query Performance Benchmark (Disk-based: Parquet) ===\n");
    println!("This benchmark compares Calm and DataFusion reading from Parquet files.\n");

    // 1. 创建 Calm Schema 和 Partition
    let schema = Schema {
        name: "benchmark".to_string(),
        primary_key: None,
        store_source: false,
        fields: vec![
            FieldOption::I64 {
                name: "id".to_string(),
                index: true,
            },
            FieldOption::Keyword {
                name: "category".to_string(),
                index: true,
                is_array: false,
                persist_option: None,
                case_sensitive: true,
            },
            FieldOption::I64 {
                name: "value".to_string(),
                index: true,
            },
            FieldOption::I64 {
                name: "timestamp".to_string(),
                index: true,
            },
        ],
        persist_policy: Default::default(),
    };

    let (persist_tx, mut persist_rx) = mpsc::unbounded_channel();

    tokio::spawn(async move {
        while let Some(partition_id) = persist_rx.recv().await {
            println!(
                "  [Background] Partition {} needs persistence",
                partition_id
            );
        }
    });

    let calm_data_dir = PathBuf::from("/tmp/calm_benchmark");

    // 2. 准备原生 DataFusion 的 Schema 和数据存储
    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("category", DataType::Utf8, true),
        Field::new("value", DataType::Int64, true),
        Field::new("timestamp", DataType::Int64, true),
    ]));

    // 3. 准备 Parquet 文件路径
    let parquet_dir = PathBuf::from("/tmp/calm_benchmark_parquet");
    std::fs::create_dir_all(&parquet_dir)?;
    let parquet_file = parquet_dir.join("data.parquet");

    // 检查是否已有持久化数据
    let calm_has_data = calm_data_dir.exists() && std::fs::read_dir(&calm_data_dir)?.count() > 0;
    let parquet_has_data = parquet_file.exists();

    // 4. 数据生成逻辑
    let total_records = 1_000_000; // 改成100万数据
    let batch_size = 10_000;
    let num_batches = total_records / batch_size;

    // 声明partition变量（在if-else外面）
    let partition: Arc<Partition>;

    if calm_has_data && parquet_has_data {
        println!("✓ Found existing data:");
        println!("  - Calm partition: {}", calm_data_dir.display());
        println!(
            "  - Parquet file: {} ({:.2} MB)",
            parquet_file.display(),
            std::fs::metadata(&parquet_file)?.len() as f64 / 1024.0 / 1024.0
        );
        println!("\nLoading partition from disk...\n");

        // 从磁盘加载已持久化的partition
        partition = Arc::new(Partition::load(
            1,
            calm_data_dir.clone(),
            schema.clone(),
            persist_tx,
        )?);

        println!(
            "✓ Partition loaded: {} frozen segments\n",
            partition.frozen_count()
        );
    } else {
        println!("Generating 1,000,000 records...\n");

        if !calm_has_data {
            println!("Creating Calm partition data...");
        }
        if !parquet_has_data {
            println!("Creating Parquet file...");
        }

        // 创建临时partition用于数据插入
        let temp_partition = Arc::new(Partition::new(
            1,
            calm_data_dir.clone(),
            schema.clone(),
            persist_tx,
        ));

        let insert_start = Instant::now();

        for batch_idx in 0..num_batches {
            let start_id = batch_idx * batch_size;

            // 生成数据
            let ids: Vec<i64> = (start_id..(start_id + batch_size))
                .map(|i| i as i64 + 1)
                .collect();
            let categories: Vec<String> = (start_id..(start_id + batch_size))
                .map(|i| {
                    match i % 5 {
                        0 => "A",
                        1 => "B",
                        2 => "C",
                        3 => "D",
                        _ => "E",
                    }
                    .to_string()
                })
                .collect();
            let values: Vec<i64> = (start_id..(start_id + batch_size))
                .map(|i| (i % 1000) as i64)
                .collect();
            let timestamps: Vec<i64> = (start_id..(start_id + batch_size))
                .map(|i| 1700000000 + (i as i64))
                .collect();

            // 创建 RecordBatch
            let batch = RecordBatch::try_new(
                arrow_schema.clone(),
                vec![
                    Arc::new(Int64Array::from(ids)),
                    Arc::new(StringArray::from(categories)),
                    Arc::new(Int64Array::from(values)),
                    Arc::new(Int64Array::from(timestamps)),
                ],
            )?;

            // 插入到 Calm Partition
            if !calm_has_data {
                temp_partition.upsert(batch.clone())?;
            }

            if (batch_idx + 1) % 100 == 0 {
                println!(
                    "  Inserted {} / {} batches ({} records)",
                    batch_idx + 1,
                    num_batches,
                    (batch_idx + 1) * batch_size
                );
            }
        }

        let insert_duration = insert_start.elapsed();
        println!("\n✓ Data generation completed in {:.2?}", insert_duration);
        println!("  Total records: {}", total_records);

        // 处理Calm partition的持久化和重新加载
        if !calm_has_data {
            let current_count = temp_partition.get_current_segment().doc_count();
            println!(
                "  Calm Partition current segment doc_count: {}",
                current_count
            );

            // 强制flush当前segment（即使未满）
            if current_count > 0 {
                println!("  Flushing current segment...");
                temp_partition.flush(false)?;
                println!("  ✓ Current segment flushed");
            }

            println!("  Total frozen segments: {}", temp_partition.frozen_count());

            // 持久化所有segments到磁盘
            println!("\n  Persisting all segments to disk...");
            let persist_start = std::time::Instant::now();
            temp_partition.persist_unpersisted_segments()?;
            println!(
                "  ✓ All segments persisted in {:.2?}",
                persist_start.elapsed()
            );

            // 重要：Drop temp_partition，释放内存中的数据
            println!("\n  Dropping in-memory partition to force reload from disk...");
            drop(temp_partition);
        }

        // 重新创建persist channel
        let (persist_tx2, mut persist_rx2) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            while let Some(partition_id) = persist_rx2.recv().await {
                println!(
                    "  [Background] Partition {} needs persistence",
                    partition_id
                );
            }
        });

        // 从磁盘重新加载partition（无论是新创建还是已存在）
        println!("  Loading partition from disk...");
        let load_start = std::time::Instant::now();
        partition = Arc::new(Partition::load(
            1,
            calm_data_dir.clone(),
            schema.clone(),
            persist_tx2,
        )?);
        println!(
            "  ✓ Partition loaded from disk in {:.2?}",
            load_start.elapsed()
        );
        println!("  Loaded {} frozen segments", partition.frozen_count());

        // 等待一下确保数据都写入完成
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // 如果需要创建 Parquet 文件,现在重新生成数据并写入
        if !parquet_has_data {
            println!("\nWriting data to Parquet file...");
            use datafusion::parquet::arrow::ArrowWriter;
            use datafusion::parquet::file::properties::WriterProperties;
            use std::fs::File;

            let file = File::create(&parquet_file)?;
            let props = WriterProperties::builder().build();
            let mut writer = ArrowWriter::try_new(file, arrow_schema.clone(), Some(props))?;

            // 重新生成数据并写入Parquet
            for batch_idx in 0..num_batches {
                let start_id = batch_idx * batch_size;

                let ids: Vec<i64> = (start_id..(start_id + batch_size))
                    .map(|i| i as i64 + 1)
                    .collect();
                let categories: Vec<String> = (start_id..(start_id + batch_size))
                    .map(|i| {
                        match i % 5 {
                            0 => "A",
                            1 => "B",
                            2 => "C",
                            3 => "D",
                            _ => "E",
                        }
                        .to_string()
                    })
                    .collect();
                let values: Vec<i64> = (start_id..(start_id + batch_size))
                    .map(|i| (i % 1000) as i64)
                    .collect();
                let timestamps: Vec<i64> = (start_id..(start_id + batch_size))
                    .map(|i| 1700000000 + (i as i64))
                    .collect();

                let batch = RecordBatch::try_new(
                    arrow_schema.clone(),
                    vec![
                        Arc::new(Int64Array::from(ids)),
                        Arc::new(StringArray::from(categories)),
                        Arc::new(Int64Array::from(values)),
                        Arc::new(Int64Array::from(timestamps)),
                    ],
                )?;

                writer.write(&batch)?;

                if (batch_idx + 1) % 10 == 0 {
                    println!(
                        "  Written {} / {} batches to Parquet",
                        batch_idx + 1,
                        num_batches
                    );
                }
            }

            writer.close()?;
            println!("✓ Parquet file created: {:?}", parquet_file);

            // 获取文件大小
            let metadata = std::fs::metadata(&parquet_file)?;
            println!(
                "  File size: {:.2} MB",
                metadata.len() as f64 / 1024.0 / 1024.0
            );
        }
    }

    println!("\n=== Starting Performance Comparison ===\n");

    // 4. 预热和准备 DataFusion 上下文

    // 4.1 Calm 查询上下文
    let calm_ctx = SessionContext::new();
    let calm_provider = Arc::new(PartitionTableProvider::new(partition.clone()));
    calm_ctx.register_table("calm_data", calm_provider)?;

    // 4.2 原生 DataFusion 查询上下文 (使用 Parquet 文件)
    let native_ctx = SessionContext::new();

    native_ctx
        .register_parquet(
            "native_data",
            parquet_file.to_str().unwrap(),
            datafusion::prelude::ParquetReadOptions::default(),
        )
        .await?;

    println!("Warming up (running each query once to eliminate cold start effects)...");

    // 预热查询
    println!("  Warming Calm query...");
    match calm_ctx.sql("SELECT COUNT(*) FROM calm_data").await {
        Ok(df) => match df.collect().await {
            Ok(_) => println!("  ✓ Calm warmup success"),
            Err(e) => println!("  ✗ Calm warmup failed: {}", e),
        },
        Err(e) => println!("  ✗ Calm SQL parsing failed: {}", e),
    }

    println!("  Warming Native query...");
    let _ = native_ctx
        .sql("SELECT COUNT(*) FROM native_data")
        .await?
        .collect()
        .await?;
    println!("  ✓ Native warmup success");

    println!("✓ Warmup completed\n");

    // 5. 定义测试查询
    let queries = vec![
        (
            "Q1: Full table scan (COUNT)",
            "SELECT COUNT(*) as total FROM {} ",
        ),
        (
            "Q2: Point query by ID (id = 500000)",
            "SELECT * FROM {} WHERE id = 500000",
        ),
        (
            "Q3: Simple filter (category = 'A')",
            "SELECT COUNT(*) as total FROM {} WHERE category = 'A'",
        ),
        (
            "Q4: Complex filter (category = 'B' AND value = 100)",
            "SELECT COUNT(*) as total FROM {} WHERE category = 'B' AND value = 100",
        ),
        (
            "Q5: Multiple conditions (OR)",
            "SELECT COUNT(*) as total FROM {} WHERE category = 'A' OR category = 'C'",
        ),
    ];

    // 6. 运行性能对比
    println!(
        "{:<50} {:>15} {:>15} {:>10}",
        "Query", "Calm (ms)", "Native (ms)", "Speedup"
    );
    println!("{}", "=".repeat(92));

    for (query_name, query_template) in queries {
        println!("\n[Running] {}", query_name);

        // 运行 Calm 查询 (3次取平均)
        let mut calm_times = Vec::new();
        let mut calm_result_batches = Vec::new();
        let mut calm_total_rows = 0;

        println!("  Calm query (3 runs)...");
        for run_idx in 0..3 {
            print!("    Run {}/3...", run_idx + 1);
            std::io::Write::flush(&mut std::io::stdout()).ok();

            let calm_query = query_template.replace("{}", "calm_data");
            let start = Instant::now();
            let df = calm_ctx.sql(&calm_query).await?;
            let result = df.collect().await?;
            let duration = start.elapsed();
            calm_times.push(duration.as_secs_f64() * 1000.0);

            println!(" {:.2}ms", duration.as_secs_f64() * 1000.0);

            // 第一次运行时记录结果统计
            if run_idx == 0 {
                calm_result_batches = result.clone();
                calm_total_rows = result.iter().map(|batch| batch.num_rows()).sum();
            }
        }
        let calm_avg = calm_times.iter().sum::<f64>() / calm_times.len() as f64;

        // 运行原生 DataFusion 查询 (3次取平均)
        let mut native_times = Vec::new();
        let mut native_result_batches = Vec::new();
        let mut native_total_rows = 0;

        println!("  Native query (3 runs)...");
        for run_idx in 0..3 {
            print!("    Run {}/3...", run_idx + 1);
            std::io::Write::flush(&mut std::io::stdout()).ok();

            let native_query = query_template.replace("{}", "native_data");
            let start = Instant::now();
            let df = native_ctx.sql(&native_query).await?;
            let result = df.collect().await?;
            let duration = start.elapsed();
            native_times.push(duration.as_secs_f64() * 1000.0);

            println!(" {:.2}ms", duration.as_secs_f64() * 1000.0);

            // 第一次运行时记录结果统计
            if run_idx == 0 {
                native_result_batches = result.clone();
                native_total_rows = result.iter().map(|batch| batch.num_rows()).sum();
            }
        }
        let native_avg = native_times.iter().sum::<f64>() / native_times.len() as f64;

        let speedup = native_avg / calm_avg;
        let speedup_str = if speedup > 1.0 {
            format!("{:.2}x faster", speedup)
        } else {
            format!("{:.2}x slower", 1.0 / speedup)
        };

        println!(
            "{:<50} {:>15.2} {:>15.2} {:>10}",
            query_name, calm_avg, native_avg, speedup_str
        );

        // 打印详细统计信息
        println!(
            "  Calm: {} batches, {} rows | Native: {} batches, {} rows",
            calm_result_batches.len(),
            calm_total_rows,
            native_result_batches.len(),
            native_total_rows
        );

        // 校验结果行数是否一致
        if calm_total_rows != native_total_rows {
            println!(
                "  ⚠️  WARNING: Row count mismatch! Calm={}, Native={}",
                calm_total_rows, native_total_rows
            );
        }

        // 对于简单查询,打印第一个 batch 的内容进行校验
        if query_name.contains("Point query") || query_name.contains("COUNT") {
            if !calm_result_batches.is_empty() && !native_result_batches.is_empty() {
                println!(
                    "  Calm result sample: {:?}",
                    calm_result_batches[0].slice(0, calm_result_batches[0].num_rows().min(1))
                );
                println!(
                    "  Native result sample: {:?}",
                    native_result_batches[0].slice(0, native_result_batches[0].num_rows().min(1))
                );
            }
        }
        println!();
    }

    println!("\n=== Benchmark Completed ===");
    println!("\nNote: Calm uses inverted indexes for indexed fields, which can significantly");
    println!("      speed up equality and range queries on indexed columns.");
    println!("      Native DataFusion performs full table scans for all queries.");

    Ok(())
}
