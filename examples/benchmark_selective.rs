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
    println!("=== Query Performance Benchmark (Realistic Scenario) ===\n");
    println!("Scenario: Finding a needle in a haystack");
    println!("- 1,000,000 records total");
    println!("- Only 100 records match the filter criteria");
    println!("- Demonstrates the benefit of inverted indexes\n");

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
        ],
        persist_policy: Default::default(),
    };

    let (persist_tx, mut persist_rx) = mpsc::unbounded_channel();

    tokio::spawn(async move {
        while let Some(_partition_id) = persist_rx.recv().await {
            // Silent
        }
    });

    let partition = Arc::new(Partition::new(
        1,
        PathBuf::from("/tmp/calm_benchmark_realistic"),
        schema.clone(),
        persist_tx,
    ));

    // 2. 准备原生 DataFusion 的 Schema 和数据存储
    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("category", DataType::Utf8, true),
        Field::new("value", DataType::Int64, true),
    ]));

    let mut all_batches: Vec<RecordBatch> = Vec::new();

    // 3. 插入 100 万数据 (只有 100 条属于 "RARE" category)
    let total_records = 1_000_000;
    let batch_size = 10_000;
    let num_batches = total_records / batch_size;

    println!(
        "Inserting {} records in {} batches...",
        total_records, num_batches
    );
    let insert_start = Instant::now();

    for batch_idx in 0..num_batches {
        let start_id = batch_idx * batch_size;

        // 生成数据: 只有 100 条记录 (id: 500000-500099) 属于 "RARE" category
        let ids: Vec<i64> = (start_id..(start_id + batch_size))
            .map(|i| i as i64 + 1)
            .collect();
        let categories: Vec<String> = (start_id..(start_id + batch_size))
            .map(|i| {
                if i >= 500_000 && i < 500_100 {
                    "RARE".to_string()
                } else {
                    "COMMON".to_string()
                }
            })
            .collect();
        let values: Vec<i64> = (start_id..(start_id + batch_size))
            .map(|i| (i % 1000) as i64)
            .collect();

        // 创建 RecordBatch
        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(categories)),
                Arc::new(Int64Array::from(values)),
            ],
        )?;

        // 插入到 Calm Partition
        partition.upsert(batch.clone())?;

        // 保存到原生 DataFusion 数据
        all_batches.push(batch);
    }

    let insert_duration = insert_start.elapsed();
    println!("✓ Data insertion completed in {:.2?}\n", insert_duration);

    // 等待一下确保数据都写入完成
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    println!("=== Starting Performance Comparison ===\n");

    // 4. 预热和准备 DataFusion 上下文
    let calm_ctx = SessionContext::new();
    let calm_provider = Arc::new(PartitionTableProvider::new(partition.clone()));
    calm_ctx.register_table("calm_data", calm_provider)?;

    let native_ctx = SessionContext::new();
    let native_provider =
        datafusion::datasource::MemTable::try_new(arrow_schema.clone(), vec![all_batches])?;
    native_ctx.register_table("native_data", Arc::new(native_provider))?;

    println!("Warming up...");
    let _ = calm_ctx
        .sql("SELECT COUNT(*) FROM calm_data")
        .await?
        .collect()
        .await?;
    let _ = native_ctx
        .sql("SELECT COUNT(*) FROM native_data")
        .await?
        .collect()
        .await?;
    println!("✓ Warmup completed\n");

    // 5. 定义测试查询
    let queries = vec![
        (
            "Q1: Find rare records (category = 'RARE')",
            "SELECT COUNT(*) as total FROM {} WHERE category = 'RARE'",
            "Expected result: 100 records out of 1,000,000 (0.01%)",
        ),
        (
            "Q2: Find specific ID range",
            "SELECT COUNT(*) as total FROM {} WHERE id BETWEEN 500000 AND 500099",
            "Expected result: 100 records (demonstrates range query)",
        ),
        (
            "Q3: Complex filter",
            "SELECT COUNT(*) as total FROM {} WHERE category = 'RARE' AND value < 100",
            "Expected result: ~10 records (combines index and filter)",
        ),
        (
            "Q4: Full table scan (all COMMON records)",
            "SELECT COUNT(*) as total FROM {} WHERE category = 'COMMON'",
            "Expected result: 999,900 records (shows where native is faster)",
        ),
    ];

    // 6. 运行性能对比
    println!(
        "{:<50} {:>15} {:>15} {:>15}",
        "Query", "Calm (ms)", "Native (ms)", "Speedup"
    );
    println!("{}", "=".repeat(97));

    for (query_name, query_template, description) in queries {
        // 运行 Calm 查询 (5次取平均)
        let mut calm_times = Vec::new();
        let mut calm_result_count = 0;
        for _ in 0..5 {
            let calm_query = query_template.replace("{}", "calm_data");
            let start = Instant::now();
            let df = calm_ctx.sql(&calm_query).await?;
            let result = df.collect().await?;
            let duration = start.elapsed();
            calm_times.push(duration.as_secs_f64() * 1000.0);

            // 获取结果数量
            if calm_result_count == 0 && !result.is_empty() {
                if let Some(col) = result[0]
                    .column(0)
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::Int64Array>()
                {
                    calm_result_count = col.value(0);
                }
            }
        }
        calm_times.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let calm_median = calm_times[calm_times.len() / 2];

        // 运行原生 DataFusion 查询 (5次取平均)
        let mut native_times = Vec::new();
        for _ in 0..5 {
            let native_query = query_template.replace("{}", "native_data");
            let start = Instant::now();
            let df = native_ctx.sql(&native_query).await?;
            let _ = df.collect().await?;
            let duration = start.elapsed();
            native_times.push(duration.as_secs_f64() * 1000.0);
        }
        native_times.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let native_median = native_times[native_times.len() / 2];

        let speedup = if calm_median < native_median {
            format!("{:.2}x faster", native_median / calm_median)
        } else {
            format!("{:.2}x slower", calm_median / native_median)
        };

        println!(
            "{:<50} {:>15.2} {:>15.2} {:>15}",
            query_name, calm_median, native_median, speedup
        );
        println!("  {} (Result: {} records)", description, calm_result_count);
    }

    println!("\n=== Analysis ===");
    println!("1. Selective queries (Q1-Q3): Calm should be faster due to inverted indexes");
    println!("   - Only scans matching records (100 out of 1,000,000)");
    println!("   - Index lookup is O(1) for equality, O(log n) for range");
    println!("\n2. Full scan queries (Q4): Native DataFusion is faster");
    println!("   - Scans most of the data (999,900 out of 1,000,000)");
    println!("   - Native has optimized vectorized execution");
    println!("\n3. Conclusion:");
    println!("   - Use Calm for selective queries on indexed fields");
    println!("   - Use native for full table scans or aggregations on most data");

    Ok(())
}
