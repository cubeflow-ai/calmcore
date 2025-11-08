use calm::{
    partition::Partition,
    schema::{compute::PartitionTableProvider, field::FieldOption, PersistPolicy, Schema},
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
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║     Query Performance Benchmark (Disk-based: Parquet)      ║");
    println!("║          Calm vs Native DataFusion Comparison              ║");
    println!("╚══════════════════════════════════════════════════════════════╝\n");

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
        persist_policy: PersistPolicy {
            max_docs_per_segment: 1_000_000,
            ..Default::default()
        },
    };

    let (persist_tx, mut persist_rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        while let Some(_partition_id) = persist_rx.recv().await {
            // 后台持久化
        }
    });

    let calm_data_dir = PathBuf::from("/tmp/calm_benchmark");
    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("category", DataType::Utf8, true),
        Field::new("value", DataType::Int64, true),
        Field::new("timestamp", DataType::Int64, true),
    ]));

    let parquet_dir = PathBuf::from("/tmp/calm_benchmark_parquet");
    std::fs::create_dir_all(&parquet_dir)?;
    let parquet_file = parquet_dir.join("data.parquet");

    let calm_has_data = calm_data_dir.exists() && std::fs::read_dir(&calm_data_dir)?.count() > 0;
    let parquet_has_data = parquet_file.exists();

    let total_records = 100_000;
    let batch_size = 10_000;
    let num_batches = total_records / batch_size;

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
        
        let (persist_tx2, mut persist_rx2) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            while let Some(_partition_id) = persist_rx2.recv().await {}
        });

        let load_start = std::time::Instant::now();
        partition = Arc::new(Partition::load(
            1,
            calm_data_dir.clone(),
            schema.clone(),
            persist_tx2,
        )?);
        println!(
            "✓ Partition loaded in {:.2?}",
            load_start.elapsed()
        );
        println!("  Loaded {} frozen segments\n", partition.frozen_count());
    } else {
        println!("✓ Generating new test data ({} records)...\n", total_records);
        
        let mut temp_partition = Arc::new(Partition::new(
            1,
            calm_data_dir.clone(),
            schema.clone(),
            persist_tx,
        ));

        // 生成数据
        println!("  Inserting data into Calm partition...");
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

            Arc::get_mut(&mut temp_partition)
                .unwrap()
                .upsert(batch)?;

            if (batch_idx + 1) % 2 == 0 {
                print!(".");
                std::io::Write::flush(&mut std::io::stdout()).ok();
            }
        }
        println!(" ✓\n");

        // 持久化
        let current_count = temp_partition.get_current_segment().doc_count();
        if current_count > 0 {
            Arc::get_mut(&mut temp_partition)
                .unwrap()
                .flush(false)?;
        }

        println!("  Persisting segments to disk...");
        let persist_start = std::time::Instant::now();
        Arc::get_mut(&mut temp_partition)
            .unwrap()
            .persist_unpersisted_segments()?;
        println!("  ✓ Persisted in {:.2?}\n", persist_start.elapsed());

        drop(temp_partition);

        // 重新加载
        let (persist_tx2, mut persist_rx2) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            while let Some(_partition_id) = persist_rx2.recv().await {}
        });

        partition = Arc::new(Partition::load(
            1,
            calm_data_dir.clone(),
            schema.clone(),
            persist_tx2,
        )?);

        // 创建 Parquet 文件
        println!("  Writing data to Parquet file...");
        use datafusion::parquet::arrow::ArrowWriter;
        use datafusion::parquet::file::properties::WriterProperties;
        use std::fs::File;

        let file = File::create(&parquet_file)?;
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, arrow_schema.clone(), Some(props))?;

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
        }

        writer.close()?;
        println!("  ✓ Parquet file created ({:.2} MB)\n", 
            std::fs::metadata(&parquet_file)?.len() as f64 / 1024.0 / 1024.0);
    }

    // 设置查询上下文
    let calm_ctx = SessionContext::new();
    let calm_provider = Arc::new(PartitionTableProvider::new(partition.clone()));
    calm_ctx.register_table("calm_data", calm_provider)?;

    let native_ctx = SessionContext::new();
    native_ctx
        .register_parquet(
            "native_data",
            parquet_file.to_str().unwrap(),
            datafusion::prelude::ParquetReadOptions::default(),
        )
        .await?;

    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║                   Warm-up Phase                            ║");
    println!("╚════════════════════════════════════════════════════════════╝");
    println!("  Warming Calm query... ", );
    let _ = calm_ctx.sql("SELECT COUNT(*) FROM calm_data").await?.collect().await?;
    println!("✓");

    println!("  Warming Native query...");
    let _ = native_ctx.sql("SELECT COUNT(*) FROM native_data").await?.collect().await?;
    println!("✓\n");

    // 定义丰富的查询集合
    #[derive(Clone)]
    struct QueryDef {
        name: &'static str,
        template: &'static str,
        category: &'static str,
    }

    let queries = vec![
        // 基础扫描
        QueryDef {
            name: "Q1: Full table scan (COUNT)",
            template: "SELECT COUNT(*) as total FROM {} ",
            category: "Full Scan",
        },
        QueryDef {
            name: "Q2: Full table scan (SELECT ALL)",
            template: "SELECT * FROM {} LIMIT 10",
            category: "Full Scan",
        },
        
        // 点查询
        QueryDef {
            name: "Q3: Point query by ID",
            template: "SELECT * FROM {} WHERE id = 50000",
            category: "Point Query",
        },
        QueryDef {
            name: "Q4: Point query specific columns",
            template: "SELECT id, category FROM {} WHERE id = 50000",
            category: "Point Query",
        },
        
        // 简单过滤
        QueryDef {
            name: "Q5: Simple filter (category = 'A')",
            template: "SELECT COUNT(*) as total FROM {} WHERE category = 'A'",
            category: "Simple Filter",
        },
        QueryDef {
            name: "Q6: Simple filter (category = 'B')",
            template: "SELECT COUNT(*) as total FROM {} WHERE category = 'B'",
            category: "Simple Filter",
        },
        QueryDef {
            name: "Q7: Range filter (value < 100)",
            template: "SELECT COUNT(*) as total FROM {} WHERE value < 100",
            category: "Range Filter",
        },
        QueryDef {
            name: "Q8: Range filter (value >= 500)",
            template: "SELECT COUNT(*) as total FROM {} WHERE value >= 500",
            category: "Range Filter",
        },
        
        // 组合过滤
        QueryDef {
            name: "Q9: AND condition (category='B' AND value=100)",
            template: "SELECT COUNT(*) as total FROM {} WHERE category = 'B' AND value = 100",
            category: "AND Filter",
        },
        QueryDef {
            name: "Q10: AND condition (category='A' AND value < 50)",
            template: "SELECT COUNT(*) as total FROM {} WHERE category = 'A' AND value < 50",
            category: "AND Filter",
        },
        
        // OR 条件
        QueryDef {
            name: "Q11: OR (category='A' OR category='B')",
            template: "SELECT COUNT(*) as total FROM {} WHERE category = 'A' OR category = 'B'",
            category: "OR Filter",
        },
        QueryDef {
            name: "Q12: OR with 3 conditions",
            template: "SELECT COUNT(*) as total FROM {} WHERE category = 'A' OR category = 'C' OR category = 'E'",
            category: "OR Filter",
        },
        
        // 复杂组合
        QueryDef {
            name: "Q13: Complex ((category='A' OR category='B') AND value < 200)",
            template: "SELECT COUNT(*) as total FROM {} WHERE (category = 'A' OR category = 'B') AND value < 200",
            category: "Complex Filter",
        },
        QueryDef {
            name: "Q14: Multi-condition (category!='A' AND value>100 AND value<300)",
            template: "SELECT COUNT(*) as total FROM {} WHERE category != 'A' AND value > 100 AND value < 300",
            category: "Complex Filter",
        },
        
        // 返回多列查询
        QueryDef {
            name: "Q15: Filtered result with multiple columns",
            template: "SELECT id, category, value FROM {} WHERE category = 'A' AND value < 50 LIMIT 5",
            category: "Multi-column",
        },
        QueryDef {
            name: "Q16: Aggregation by category",
            template: "SELECT category, COUNT(*) as cnt FROM {} GROUP BY category",
            category: "Aggregation",
        },
    ];

    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║              Performance Benchmark Results                 ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    println!(
        "{:<6} {:<50} {:>12} {:>12} {:>10}",
        "ID", "Query", "Calm (ms)", "Native (ms)", "Speedup"
    );
    println!("{}", "─".repeat(95));

    let mut total_calm = 0.0;
    let mut total_native = 0.0;
    let mut category_stats: std::collections::HashMap<&str, (f64, f64, i32)> = std::collections::HashMap::new();

    for (idx, query) in queries.iter().enumerate() {
        let query_num = idx + 1;

        // 运行 Calm 查询
        let mut calm_times = Vec::new();
        for _ in 0..2 {
            let calm_query = query.template.replace("{}", "calm_data");
            let start = Instant::now();
            let _ = calm_ctx.sql(&calm_query).await?.collect().await?;
            calm_times.push(start.elapsed().as_secs_f64() * 1000.0);
        }
        let calm_avg = calm_times.iter().sum::<f64>() / calm_times.len() as f64;

        // 运行原生 DataFusion 查询
        let mut native_times = Vec::new();
        for _ in 0..2 {
            let native_query = query.template.replace("{}", "native_data");
            let start = Instant::now();
            let _ = native_ctx.sql(&native_query).await?.collect().await?;
            native_times.push(start.elapsed().as_secs_f64() * 1000.0);
        }
        let native_avg = native_times.iter().sum::<f64>() / native_times.len() as f64;

        let speedup = native_avg / calm_avg;
        let speedup_str = if speedup > 1.0 {
            format!("⚡{:.2}x", speedup)
        } else {
            format!("🐢{:.2}x", 1.0 / speedup)
        };

        println!(
            "{:<6} {:<50} {:>12.2} {:>12.2} {:>10}",
            format!("Q{}", query_num),
            query.name,
            calm_avg,
            native_avg,
            speedup_str
        );

        total_calm += calm_avg;
        total_native += native_avg;

        // 分类统计
        let stats = category_stats
            .entry(query.category)
            .or_insert((0.0, 0.0, 0));
        stats.0 += calm_avg;
        stats.1 += native_avg;
        stats.2 += 1;
    }

    println!("{}", "─".repeat(95));
    println!(
        "{:<6} {:<50} {:>12.2} {:>12.2} {:>10}",
        "AVG",
        "Average across all queries",
        total_calm / queries.len() as f64,
        total_native / queries.len() as f64,
        format!("⚡{:.2}x", total_native / total_calm)
    );

    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!("║                  Category Analysis                         ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    for category in &[
        "Full Scan",
        "Point Query",
        "Simple Filter",
        "Range Filter",
        "AND Filter",
        "OR Filter",
        "Complex Filter",
        "Multi-column",
        "Aggregation",
    ] {
        if let Some((calm_total, native_total, count)) = category_stats.get(category) {
            let calm_avg = calm_total / *count as f64;
            let native_avg = native_total / *count as f64;
            let speedup = if calm_avg > 0.0 {
                native_avg / calm_avg
            } else {
                0.0
            };

            let speedup_icon = if speedup > 1.5 {
                "🚀"
            } else if speedup > 1.0 {
                "⚡"
            } else if speedup > 0.7 {
                "🟡"
            } else {
                "🐢"
            };

            println!(
                "{:<25} | Calm: {:>8.2}ms | Native: {:>8.2}ms | Speedup: {} {:.2}x",
                category,
                calm_avg,
                native_avg,
                speedup_icon,
                if speedup > 1.0 { speedup } else { 1.0 / speedup }
            );
        }
    }

    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!("║                    Summary & Insights                      ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    let avg_calm = total_calm / queries.len() as f64;
    let avg_native = total_native / queries.len() as f64;
    let overall_speedup = avg_native / avg_calm;

    println!("📊 Overall Performance:");
    println!("  • Calm average:   {:.2}ms per query", avg_calm);
    println!("  • Native average: {:.2}ms per query", avg_native);
    println!("  • Overall speedup: {:.2}x\n", overall_speedup);

    if overall_speedup > 1.5 {
        println!("🚀 EXCELLENT: Calm significantly outperforms DataFusion!");
        println!("   Suggestion: Use Calm for indexed field queries.");
    } else if overall_speedup > 1.0 {
        println!("⚡ GOOD: Calm is faster than DataFusion on average.");
        println!("   Suggestion: Calm excels at selective queries with indexes.");
    } else if overall_speedup > 0.7 {
        println!("🟡 FAIR: Performance is comparable to DataFusion.");
        println!("   Suggestion: Focus optimization on specific query patterns.");
    } else {
        println!("🐢 Calm is currently slower than DataFusion.");
        println!("   Suggestion: Continue optimization work.");
    }

    println!("\n📈 Recommended Next Steps:");
    println!("  1. Profile the slowest query categories");
    println!("  2. Analyze index usage and bitmap operations");
    println!("  3. Optimize Parquet read patterns");
    println!("  4. Consider query result caching\n");

    Ok(())
}
