/// 性能基准测试
///
/// 创建一个包含 2000 万数据的表，测试查询优化器的性能
///
/// 配置：
/// - 4 个 partition
/// - 每个 partition 500 万数据
/// - 每 200 万数据一个 segment（每个 partition 约 2-3 个 segment）
/// - 测试 range 查询 + ORDER BY + LIMIT

use calm::compute::DistributedExecutor;
use calm::engine::{Engine, EngineConfig};
use datafusion::arrow::array::{Int64Array, StringArray, UInt32Array};
use datafusion::arrow::datatypes::{DataType, Field as ArrowField, Schema as ArrowSchema};
use datafusion::arrow::record_batch::RecordBatch;
use rand::Rng;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

const TOTAL_ROWS: usize = 20_000_000; // 2000 万
const NUM_PARTITIONS: usize = 4;
const ROWS_PER_PARTITION: usize = TOTAL_ROWS / NUM_PARTITIONS; // 500 万
const BATCH_SIZE: usize = 10_000; // 每批插入 1 万条

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化日志
    env_logger::Builder::from_default_env()
        .filter_level(log::LevelFilter::Info)
        .init();

    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║          查询优化器性能基准测试                              ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    // 创建 Engine
    let config = EngineConfig {
        data_dir: PathBuf::from("./benchmark_data"),
        persist_check_interval_secs: 60,
        max_concurrent_persists: 4,
        check_after_flush: true,
    };

    println!("📦 创建 Engine...");
    let engine = Engine::new(config)?;
    println!("✅ Engine 创建成功\n");

    // 创建表
    let table_name = "users";
    println!("📋 创建表: {}", table_name);
    println!("   - {} 个 partition", NUM_PARTITIONS);
    println!("   - 总数据量: {} 万行", TOTAL_ROWS / 10_000);
    println!("   - 每个 partition: {} 万行\n", ROWS_PER_PARTITION / 10_000);

    create_table(&engine, table_name).await?;

    // 插入数据
    println!("📝 开始插入数据...");
    let insert_start = Instant::now();
    insert_data(&engine, table_name).await?;
    let insert_duration = insert_start.elapsed();
    println!("✅ 数据插入完成，耗时: {:.2}s", insert_duration.as_secs_f64());
    println!("   - 插入速度: {:.0} 行/秒\n", TOTAL_ROWS as f64 / insert_duration.as_secs_f64());

    // 等待数据持久化
    println!("💾 等待数据持久化...");
    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
    println!("✅ 数据持久化完成\n");

    // 创建执行器
    let executor = DistributedExecutor::new(engine);

    // 运行基准测试
    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║                    基准测试开始                              ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    run_benchmarks(&executor).await?;

    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!("║                    测试完成                                  ║");
    println!("╚════════════════════════════════════════════════════════════╝");

    Ok(())
}

/// 创建表
async fn create_table(engine: &Arc<Engine>, table_name: &str) -> Result<(), Box<dyn std::error::Error>> {
    use calm::catalog::PartitionStrategy;
    use calm::schema::field::FieldOption;
    use calm::schema::{PersistPolicy, Schema};
    
    let schema = Schema {
        name: table_name.to_string(),
        primary_key: Some("id".to_string()),
        store_source: false,
        fields: vec![
            FieldOption::U32 {
                name: "id".to_string(),
                index: true,
            },
            FieldOption::I64 {
                name: "age".to_string(),
                index: true,
            },
            FieldOption::I64 {
                name: "score".to_string(),
                index: true,
            },
            FieldOption::Keyword {
                name: "name".to_string(),
                index: true,
                is_array: false,
                persist_option: None,
                case_sensitive: true,
            },
        ],
        persist_policy: PersistPolicy::default(),
    };
    
    engine
        .create_table(
            table_name,
            schema,
            PartitionStrategy::Hash {
                field: "id".to_string(),
                num_partitions: NUM_PARTITIONS,
            },
            NUM_PARTITIONS,
        )
        .await?;

    Ok(())
}

/// 插入数据
async fn insert_data(engine: &Arc<Engine>, table_name: &str) -> Result<(), Box<dyn std::error::Error>> {
    let mut rng = rand::thread_rng();

    // Arrow Schema
    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", DataType::UInt32, false),
        ArrowField::new("age", DataType::Int64, false),
        ArrowField::new("score", DataType::Int64, false),
        ArrowField::new("name", DataType::Utf8, false),
    ]));

    let mut total_inserted = 0;
    let mut batch_count = 0;

    while total_inserted < TOTAL_ROWS {
        let batch_size = BATCH_SIZE.min(TOTAL_ROWS - total_inserted);

        // 生成数据
        let mut ids = Vec::with_capacity(batch_size);
        let mut ages = Vec::with_capacity(batch_size);
        let mut scores = Vec::with_capacity(batch_size);
        let mut names = Vec::with_capacity(batch_size);

        for i in 0..batch_size {
            let id = (total_inserted + i) as u32;
            let age = rng.gen_range(18..80); // 年龄 18-80
            let score = rng.gen_range(0..1000); // 分数 0-1000
            let name = format!("user_{}", id);

            ids.push(id);
            ages.push(age);
            scores.push(score);
            names.push(name);
        }

        // 创建 RecordBatch
        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(UInt32Array::from(ids)),
                Arc::new(Int64Array::from(ages)),
                Arc::new(Int64Array::from(scores)),
                Arc::new(StringArray::from(names)),
            ],
        )?;

        // 插入数据到每个 partition（使用 hash 分区）
        let partition_id = (total_inserted / batch_size) % NUM_PARTITIONS;
        if let Some(partition) = engine.get_partition(table_name, partition_id as u64).await {
            partition.upsert(batch)?;
        }

        total_inserted += batch_size;
        batch_count += 1;

        // 每插入 100 万行打印一次进度
        if total_inserted % 1_000_000 == 0 {
            println!("   已插入: {} 万行 ({:.1}%)", 
                total_inserted / 10_000, 
                (total_inserted as f64 / TOTAL_ROWS as f64) * 100.0
            );
        }
    }

    println!("   总批次: {}", batch_count);

    Ok(())
}

/// 运行基准测试
async fn run_benchmarks(executor: &DistributedExecutor) -> Result<(), Box<dyn std::error::Error>> {
    // 测试 1: 小范围 + 小 LIMIT
    println!("测试 1: 小范围查询 + 小 LIMIT");
    println!("SQL: SELECT * FROM users WHERE age BETWEEN 25 AND 35 ORDER BY age ASC LIMIT 10");
    run_query(
        executor,
        "SELECT * FROM users WHERE age BETWEEN 25 AND 35 ORDER BY age ASC LIMIT 10",
        "小范围 + 小 LIMIT",
    ).await?;

    // 测试 2: 小范围 + 中 LIMIT
    println!("\n测试 2: 小范围查询 + 中 LIMIT");
    println!("SQL: SELECT * FROM users WHERE age BETWEEN 25 AND 35 ORDER BY age ASC LIMIT 100");
    run_query(
        executor,
        "SELECT * FROM users WHERE age BETWEEN 25 AND 35 ORDER BY age ASC LIMIT 100",
        "小范围 + 中 LIMIT",
    ).await?;

    // 测试 3: 大范围 + 小 LIMIT
    println!("\n测试 3: 大范围查询 + 小 LIMIT");
    println!("SQL: SELECT * FROM users WHERE age BETWEEN 20 AND 60 ORDER BY age DESC LIMIT 10");
    run_query(
        executor,
        "SELECT * FROM users WHERE age BETWEEN 20 AND 60 ORDER BY age DESC LIMIT 10",
        "大范围 + 小 LIMIT",
    ).await?;

    // 测试 4: 大范围 + 大 LIMIT
    println!("\n测试 4: 大范围查询 + 大 LIMIT");
    println!("SQL: SELECT * FROM users WHERE age BETWEEN 20 AND 60 ORDER BY age ASC LIMIT 1000");
    run_query(
        executor,
        "SELECT * FROM users WHERE age BETWEEN 20 AND 60 ORDER BY age ASC LIMIT 1000",
        "大范围 + 大 LIMIT",
    ).await?;

    // 测试 5: 分数范围查询
    println!("\n测试 5: 分数范围查询");
    println!("SQL: SELECT * FROM users WHERE score BETWEEN 500 AND 800 ORDER BY score DESC LIMIT 50");
    run_query(
        executor,
        "SELECT * FROM users WHERE score BETWEEN 500 AND 800 ORDER BY score DESC LIMIT 50",
        "分数范围查询",
    ).await?;

    // 测试 6: 带 OFFSET 的分页查询
    println!("\n测试 6: 分页查询（带 OFFSET）");
    println!("SQL: SELECT * FROM users WHERE age > 30 ORDER BY age LIMIT 20 OFFSET 10");
    run_query(
        executor,
        "SELECT * FROM users WHERE age > 30 ORDER BY age LIMIT 20 OFFSET 10",
        "分页查询",
    ).await?;

    Ok(())
}

/// 执行单个查询并统计性能
async fn run_query(
    executor: &DistributedExecutor,
    sql: &str,
    description: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    // 预热（第一次查询可能较慢）
    let _ = executor.execute_sql(sql).await;

    // 正式测试（运行 3 次取平均）
    let mut durations = Vec::new();
    let mut row_counts = Vec::new();

    for _ in 0..3 {
        let start = Instant::now();
        let result = executor.execute_sql(sql).await?;
        let duration = start.elapsed();

        let total_rows: usize = result.batches.iter().map(|b| b.num_rows()).sum();

        durations.push(duration);
        row_counts.push(total_rows);
    }

    // 计算平均值
    let avg_duration = durations.iter().sum::<std::time::Duration>() / durations.len() as u32;
    let avg_rows = row_counts.iter().sum::<usize>() / row_counts.len();

    println!("📊 结果:");
    println!("   - 平均耗时: {:.2}ms", avg_duration.as_millis());
    println!("   - 返回行数: {}", avg_rows);
    println!("   - 吞吐量: {:.0} 行/秒", avg_rows as f64 / avg_duration.as_secs_f64());

    Ok(())
}
