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

/// 性能测试工具 - 包含丰富的查询类型和详细分析
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n╔══════════════════════════════════════════════════════════════╗");
    println!("║   Comprehensive Query Performance Test Suite (v2.0)        ║");
    println!("║   Calm Database vs Native DataFusion                       ║");
    println!("╚══════════════════════════════════════════════════════════════╝\n");

    // Schema 定义
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
        while let Some(_) = persist_rx.recv().await {}
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

    let partition: Arc<Partition>;

    if calm_has_data && parquet_has_data {
        println!("ℹ️  Loading existing test data...\n");
        
        let (persist_tx2, mut persist_rx2) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            while let Some(_) = persist_rx2.recv().await {}
        });

        let load_start = std::time::Instant::now();
        partition = Arc::new(Partition::load(1, calm_data_dir.clone(), schema.clone(), persist_tx2)?);
        println!("✓ Loaded in {:.2?}\n", load_start.elapsed());
    } else {
        println!("ℹ️  Generating test data (100K records)...\n");
        
        let total_records = 100_000;
        let batch_size = 10_000;
        let num_batches = total_records / batch_size;

        let mut temp_partition = Arc::new(Partition::new(
            1,
            calm_data_dir.clone(),
            schema.clone(),
            persist_tx,
        ));

        for batch_idx in 0..num_batches {
            let start_id = batch_idx * batch_size;
            let ids: Vec<i64> = (start_id..(start_id + batch_size))
                .map(|i| i as i64 + 1)
                .collect();
            let categories: Vec<String> = (start_id..(start_id + batch_size))
                .map(|i| match i % 5 {
                    0 => "A",
                    1 => "B",
                    2 => "C",
                    3 => "D",
                    _ => "E",
                }.to_string())
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

        let current_count = temp_partition.get_current_segment().doc_count();
        if current_count > 0 {
            Arc::get_mut(&mut temp_partition)
                .unwrap()
                .flush(false)?;
        }

        Arc::get_mut(&mut temp_partition)
            .unwrap()
            .persist_unpersisted_segments()?;

        drop(temp_partition);

        let (persist_tx2, mut persist_rx2) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            while let Some(_) = persist_rx2.recv().await {}
        });

        partition = Arc::new(Partition::load(1, calm_data_dir.clone(), schema.clone(), persist_tx2)?);

        // 创建 Parquet 文件
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
                .map(|i| match i % 5 {
                    0 => "A",
                    1 => "B",
                    2 => "C",
                    3 => "D",
                    _ => "E",
                }.to_string())
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
        println!("✓ Created Parquet file ({:.2} MB)\n", 
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
    println!("╚════════════════════════════════════════════════════════════╝\n");

    let _ = calm_ctx.sql("SELECT COUNT(*) FROM calm_data").await?.collect().await?;
    println!("✓ Calm warmed up");

    let _ = native_ctx.sql("SELECT COUNT(*) FROM native_data").await?.collect().await?;
    println!("✓ DataFusion warmed up\n");

    // 测试查询集合
    struct QueryTest {
        name: &'static str,
        sql: &'static str,
        description: &'static str,
    }

    let tests = vec![
        // 基础测试
        QueryTest {
            name: "COUNT(*) - Full Scan",
            sql: "SELECT COUNT(*) as cnt FROM {}",
            description: "计数全表",
        },
        
        // 精确查询 - 应该很快
        QueryTest {
            name: "Point Query - Exact Match",
            sql: "SELECT * FROM {} WHERE id = 50000",
            description: "单行精确查询（应使用主键索引）",
        },
        
        // 单列过滤
        QueryTest {
            name: "Filter by Category A",
            sql: "SELECT COUNT(*) as cnt FROM {} WHERE category = 'A'",
            description: "单列过滤，应使用索引",
        },
        
        QueryTest {
            name: "Filter by Value Range",
            sql: "SELECT COUNT(*) as cnt FROM {} WHERE value BETWEEN 100 AND 200",
            description: "范围查询，应使用索引",
        },
        
        // 复杂条件
        QueryTest {
            name: "AND Condition",
            sql: "SELECT COUNT(*) as cnt FROM {} WHERE category = 'B' AND value = 100",
            description: "AND 条件，两个索引结果的交集",
        },
        
        QueryTest {
            name: "OR Condition",
            sql: "SELECT COUNT(*) as cnt FROM {} WHERE category = 'A' OR category = 'B'",
            description: "OR 条件，两个索引结果的并集",
        },
        
        QueryTest {
            name: "Complex Filter",
            sql: "SELECT COUNT(*) as cnt FROM {} WHERE (category = 'A' OR category = 'C') AND value < 300",
            description: "复杂条件，混合 AND/OR",
        },
    ];

    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║              Performance Test Results                     ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    for (idx, test) in tests.iter().enumerate() {
        println!("📋 Test {}: {}", idx + 1, test.name);
        println!("   {}", test.description);

        // 运行 3 次取平均值
        let mut calm_times = Vec::new();
        for _ in 0..3 {
            let calm_sql = test.sql.replace("{}", "calm_data");
            let start = Instant::now();
            let _ = calm_ctx.sql(&calm_sql).await?.collect().await?;
            calm_times.push(start.elapsed().as_secs_f64() * 1000.0);
        }
        let calm_avg = calm_times.iter().sum::<f64>() / calm_times.len() as f64;

        // 原生 DataFusion
        let mut native_times = Vec::new();
        for _ in 0..3 {
            let native_sql = test.sql.replace("{}", "native_data");
            let start = Instant::now();
            let _ = native_ctx.sql(&native_sql).await?.collect().await?;
            native_times.push(start.elapsed().as_secs_f64() * 1000.0);
        }
        let native_avg = native_times.iter().sum::<f64>() / native_times.len() as f64;

        let ratio = native_avg / calm_avg;
        let badge = if ratio > 1.5 {
            "🚀 FASTER"
        } else if ratio > 1.0 {
            "⚡ slightly faster"
        } else if ratio > 0.8 {
            "🟡 comparable"
        } else {
            "🐢 slower"
        };

        println!("   Results:");
        println!("     • Calm:       {:.2} ms", calm_avg);
        println!("     • DataFusion: {:.2} ms", native_avg);
        println!("     • Ratio:      {} ({:.2}x)\n", badge, 
            if ratio > 1.0 { ratio } else { 1.0 / ratio });
    }

    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║                   Test Complete                           ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    Ok(())
}
