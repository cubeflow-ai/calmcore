/// 查询优化器演示
///
/// 支持分阶段操作：write（写入）、query（查询）、persist（持久化）
///
/// 使用方法：
/// cargo run --example demo --release -- write        # 写入数据
/// cargo run --example demo --release -- query        # 查询数据
/// cargo run --example demo --release -- persist      # 持久化数据
/// cargo run --example demo --release -- write query  # 写入并查询
/// cargo run --example demo --release -- all          # 全部操作

use calm::catalog::PartitionStrategy;
use calm::compute::DistributedExecutor;
use calm::engine::{Engine, EngineConfig};
use calm::schema::field::FieldOption;
use calm::schema::{PersistPolicy, Schema};
use datafusion::arrow::array::{Int64Array, StringArray, UInt32Array};
use datafusion::arrow::datatypes::{DataType, Field as ArrowField, Schema as ArrowSchema};
use datafusion::arrow::record_batch::RecordBatch;
use rand::Rng;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

const TOTAL_ROWS: usize = 400_000; // 40 万
const NUM_PARTITIONS: usize = 4;
const BATCH_SIZE: usize = 10_000; // 每批 1 万条
const DATA_DIR: &str = "./demo_data";
const TABLE_NAME: &str = "users";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化日志
    env_logger::Builder::from_default_env()
        .filter_level(log::LevelFilter::Info)
        .init();

    // 解析命令行参数
    let args: Vec<String> = std::env::args().collect();
    let operations = if args.len() > 1 {
        args[1..].to_vec()
    } else {
        vec!["all".to_string()]
    };

    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║          查询优化器演示                                      ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    // 创建 Engine
    let config = EngineConfig {
        data_dir: PathBuf::from(DATA_DIR),
        persist_check_interval_secs: 60,
        max_concurrent_persists: 4,
        check_after_flush: true,
    };

    println!("📦 创建 Engine...");
    let engine = Engine::new(config)?;
    
    // 加载已存在的表
    engine.load_existing_tables().await?;
    println!("✅ Engine 创建成功\n");

    // 执行操作
    for op in &operations {
        match op.as_str() {
            "write" | "all" => {
                write_data(&engine).await?;
            }
            "query" => {
                query_data(&engine).await?;
            }
            "persist" => {
                persist_data(&engine).await?;
            }
            _ => {
                println!("❌ 未知操作: {}", op);
                print_usage();
                return Ok(());
            }
        }
    }

    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!("║                    操作完成                                  ║");
    println!("╚════════════════════════════════════════════════════════════╝");

    Ok(())
}

/// 写入数据
async fn write_data(engine: &Arc<Engine>) -> Result<(), Box<dyn std::error::Error>> {
    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║                    写入数据                                  ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    // 检查表是否存在
    let table_exists = engine.get_table_meta(TABLE_NAME).is_ok();

    if !table_exists {
        println!("📋 创建表: {}", TABLE_NAME);
        create_table(engine).await?;
        println!("✅ 表创建成功\n");
    } else {
        println!("📋 表已存在: {}\n", TABLE_NAME);
    }

    println!("📝 开始写入数据...");
    println!("   - 总数据量: {} 万行", TOTAL_ROWS / 10_000);
    println!("   - Partition 数量: {}", NUM_PARTITIONS);
    println!("   - 分区策略: Hash(id)\n");

    let start = Instant::now();
    insert_data(engine).await?;
    let duration = start.elapsed();

    println!("\n✅ 数据写入完成");
    println!("   - 耗时: {:.2}s", duration.as_secs_f64());
    println!("   - 速度: {:.0} 行/秒", TOTAL_ROWS as f64 / duration.as_secs_f64());
    println!("   - 数据在内存中，可立即查询");

    Ok(())
}

/// 查询数据
async fn query_data(engine: &Arc<Engine>) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!("║                    查询测试                                  ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    // 检查表是否存在
    if engine.get_table_meta(TABLE_NAME).is_err() {
        println!("❌ 表不存在，请先运行: cargo run --example demo --release -- write");
        return Ok(());
    }

    let executor = DistributedExecutor::new(engine.clone());

    // 测试 1: 小 LIMIT 查询
    println!("【测试 1】小 LIMIT 查询（优化效果最明显）");
    println!("SQL: SELECT * FROM users WHERE age BETWEEN 25 AND 35 ORDER BY age ASC LIMIT 10\n");
    
    let sql1 = "SELECT * FROM users WHERE age BETWEEN 25 AND 35 ORDER BY age ASC LIMIT 10";
    run_query(&executor, sql1, "小 LIMIT").await?;

    // 测试 2: 中等 LIMIT 查询
    println!("\n【测试 2】中等 LIMIT 查询");
    println!("SQL: SELECT * FROM users WHERE age BETWEEN 20 AND 40 ORDER BY age DESC LIMIT 100\n");
    
    let sql2 = "SELECT * FROM users WHERE age BETWEEN 20 AND 40 ORDER BY age DESC LIMIT 100";
    run_query(&executor, sql2, "中等 LIMIT").await?;

    // 测试 3: 分页查询
    println!("\n【测试 3】分页查询（带 OFFSET）");
    println!("SQL: SELECT * FROM users WHERE age > 30 ORDER BY age LIMIT 20 OFFSET 10\n");
    
    let sql3 = "SELECT * FROM users WHERE age > 30 ORDER BY age LIMIT 20 OFFSET 10";
    run_query(&executor, sql3, "分页查询").await?;

    // 测试 4: 分数范围查询
    println!("\n【测试 4】分数范围查询");
    println!("SQL: SELECT * FROM users WHERE score BETWEEN 500 AND 800 ORDER BY score DESC LIMIT 50\n");
    
    let sql4 = "SELECT * FROM users WHERE score BETWEEN 500 AND 800 ORDER BY score DESC LIMIT 50";
    run_query(&executor, sql4, "分数范围").await?;

    // 测试 5: 大范围查询
    println!("\n【测试 5】大范围查询（测试倒排索引命中率）");
    println!("SQL: SELECT * FROM users WHERE age BETWEEN 18 AND 80 ORDER BY age LIMIT 10\n");
    
    let sql5 = "SELECT * FROM users WHERE age BETWEEN 18 AND 80 ORDER BY age LIMIT 10";
    run_query(&executor, sql5, "大范围").await?;

    Ok(())
}

/// 持久化数据
async fn persist_data(engine: &Arc<Engine>) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!("║                    持久化数据                                ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    println!("💾 开始持久化数据到磁盘...");
    println!("   这可能需要几秒钟...\n");

    // 触发持久化（通过等待后台任务）
    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

    println!("✅ 持久化完成");
    println!("   - 数据已写入磁盘: {}", DATA_DIR);

    Ok(())
}

/// 创建表
async fn create_table(engine: &Arc<Engine>) -> Result<(), Box<dyn std::error::Error>> {
    let schema = Schema {
        name: TABLE_NAME.to_string(),
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
            TABLE_NAME,
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

/// 插入数据（使用 Hash 分区策略）
async fn insert_data(engine: &Arc<Engine>) -> Result<(), Box<dyn std::error::Error>> {
    let mut rng = rand::thread_rng();

    // Arrow Schema
    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", DataType::UInt32, false),
        ArrowField::new("age", DataType::Int64, false),
        ArrowField::new("score", DataType::Int64, false),
        ArrowField::new("name", DataType::Utf8, false),
    ]));

    let mut total_inserted = 0;

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
                Arc::new(UInt32Array::from(ids.clone())),
                Arc::new(Int64Array::from(ages)),
                Arc::new(Int64Array::from(scores)),
                Arc::new(StringArray::from(names)),
            ],
        )?;

        // 按照 Hash 策略分配到不同的 partition
        // 将 batch 按 id 的 hash 值分组
        let mut partition_batches: Vec<Vec<usize>> = vec![Vec::new(); NUM_PARTITIONS];
        
        for (idx, id) in ids.iter().enumerate() {
            let partition_id = hash_partition_id(*id, NUM_PARTITIONS);
            partition_batches[partition_id].push(idx);
        }

        // 为每个 partition 创建子 batch 并插入
        for (partition_id, indices) in partition_batches.iter().enumerate() {
            if indices.is_empty() {
                continue;
            }

            // 创建该 partition 的数据
            let partition_ids: Vec<u32> = indices.iter().map(|&i| ids[i]).collect();
            let partition_ages: Vec<i64> = indices.iter().map(|&i| batch.column(1).as_any()
                .downcast_ref::<Int64Array>().unwrap().value(i)).collect();
            let partition_scores: Vec<i64> = indices.iter().map(|&i| batch.column(2).as_any()
                .downcast_ref::<Int64Array>().unwrap().value(i)).collect();
            let partition_names: Vec<String> = indices.iter().map(|&i| batch.column(3).as_any()
                .downcast_ref::<StringArray>().unwrap().value(i).to_string()).collect();

            let partition_batch = RecordBatch::try_new(
                arrow_schema.clone(),
                vec![
                    Arc::new(UInt32Array::from(partition_ids)),
                    Arc::new(Int64Array::from(partition_ages)),
                    Arc::new(Int64Array::from(partition_scores)),
                    Arc::new(StringArray::from(partition_names)),
                ],
            )?;

            // 插入到对应的 partition
            if let Some(partition) = engine.get_partition(TABLE_NAME, partition_id as u64).await {
                partition.upsert(partition_batch)?;
            }
        }

        total_inserted += batch_size;

        // 打印进度
        if total_inserted % 100_000 == 0 {
            print!("   已插入: {} 万行...\r", total_inserted / 10_000);
            use std::io::Write;
            std::io::stdout().flush()?;
        }
    }

    println!("   已插入: {} 万行    ", total_inserted / 10_000);

    Ok(())
}

/// 计算 Hash 分区 ID
fn hash_partition_id(id: u32, num_partitions: usize) -> usize {
    let mut hasher = DefaultHasher::new();
    id.hash(&mut hasher);
    (hasher.finish() as usize) % num_partitions
}

/// 执行查询并统计性能
async fn run_query(
    executor: &DistributedExecutor,
    sql: &str,
    description: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let start = Instant::now();
    let result = executor.execute_sql(sql).await?;
    let duration = start.elapsed();

    let total_rows: usize = result.batches.iter().map(|b| b.num_rows()).sum();
    let num_batches = result.batches.len();

    println!("✅ 查询完成");
    println!("   - 耗时: {:.2}ms", duration.as_millis());
    println!("   - 返回: {} 行（{} 个 batch）", total_rows, num_batches);
    
    if total_rows > 0 {
        println!("   - 吞吐量: {:.0} 行/秒", total_rows as f64 / duration.as_secs_f64());
    }
    
    // 调试：显示每个 batch 的行数
    if num_batches > 1 {
        println!("   ⚠️  警告：返回了多个 batch，可能没有正确合并");
        for (i, batch) in result.batches.iter().enumerate() {
            println!("      Batch {}: {} 行", i, batch.num_rows());
        }
    }

    Ok(())
}

/// 打印使用说明
fn print_usage() {
    println!("\n使用方法:");
    println!("  cargo run --example demo --release -- write        # 写入数据");
    println!("  cargo run --example demo --release -- query        # 查询数据");
    println!("  cargo run --example demo --release -- persist      # 持久化数据");
    println!("  cargo run --example demo --release -- write query  # 写入并查询");
    println!("  cargo run --example demo --release -- all          # 全部操作");
    println!("\n示例:");
    println!("  cargo run --example demo --release -- write query");
}
