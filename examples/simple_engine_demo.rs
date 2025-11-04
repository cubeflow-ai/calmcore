/// 简单的 Engine Demo
///
/// 运行方式：
/// ```bash
/// cargo run --example simple_engine_demo
/// ```
use calm::engine::{Engine, EngineConfig};
use calm::schema::{field::FieldOption, PersistPolicy, Schema};
use std::time::Duration;

#[tokio::main]
async fn main() {
    println!("\n🚀 Simple Engine Demo\n");
    println!("{}", "=".repeat(60));

    // ========== 1. 创建 Engine ==========
    println!("\n[1] 创建 Engine");
    let engine = Engine::new(EngineConfig {
        data_dir: "./simple_demo_data".into(),
        persist_check_interval_secs: 5,
        max_concurrent_persists: 2,
        check_after_flush: true,
    });
    println!("    ✅ Engine 已创建");

    // ========== 2. 定义 Schema ==========
    println!("\n[2] 定义 Schema");
    let schema = create_simple_schema();
    println!("    ✅ Schema 已创建: 3 个字段");

    // ========== 3. 创建 Partition ==========
    println!("\n[3] 创建 Partition");
    let partition_id = 1;
    let partition = engine.create_partition(partition_id, schema.clone()).await;
    println!("    ✅ Partition {} 已创建", partition_id);

    // ========== 4. 写入数据 ==========
    println!("\n[4] 写入数据");
    if let Err(e) = write_simple_data(&partition) {
        println!("    ❌ 写入失败: {:?}", e);
    }

    // ========== 5. 查看统计 ==========
    println!("\n[5] 查看统计信息");
    println!("    Total Docs: {}", partition.total_count());
    println!("    Frozen Segments: {}", partition.frozen_count());

    // ========== 6. 手动 Flush ==========
    println!("\n[6] 手动 Flush (移动到 frozen segments)");
    match partition.flush(false) {
        Ok(flushed_id) => {
            if flushed_id > 0 {
                println!("    ✅ Flushed segment {}", flushed_id);
            } else {
                println!("    ℹ️  No data to flush");
            }
        }
        Err(e) => println!("    ❌ Flush 失败: {:?}", e),
    }

    // ========== 7. 触发持久化 ==========
    println!("\n[7] 触发持久化");
    engine.trigger_persist(partition_id as u64);
    println!("    ✅ 已发送持久化请求");

    // 等待持久化
    tokio::time::sleep(Duration::from_secs(2)).await;

    // ========== 8. 查看 Engine 统计 ==========
    println!("\n[8] 查看 Engine 统计信息");
    let stats = engine.stats().await;
    println!("    Partitions: {}", stats.partition_count);
    println!("    Total Docs: {}", stats.total_doc_count);
    println!("    Frozen Segments: {}", stats.total_frozen_segments);
    println!(
        "    Unpersisted Segments: {}",
        stats.total_unpersisted_segments
    );

    // ========== 9. 模拟重启：加载数据 ==========
    println!("\n[9] 模拟重启：从磁盘加载 Partition");
    tokio::time::sleep(Duration::from_secs(1)).await;

    match engine.load_partition(partition_id, schema.clone()).await {
        Ok(loaded) => {
            println!("    ✅ 加载成功");
            println!("       - 总文档数: {}", loaded.total_count());
            println!("       - Frozen Segments: {}", loaded.frozen_count());
        }
        Err(e) => {
            println!("    ⚠️  加载失败: {:?}", e);
            println!("       (可能还未持久化到磁盘)");
        }
    }

    // ========== 10. 关闭 Engine ==========
    println!("\n[10] 关闭 Engine");
    engine.shutdown().await;
    println!("    ✅ Engine 已关闭");

    println!("\n{}", "=".repeat(60));
    println!("✨ Demo 完成！\n");
}

/// 创建简单的 Schema（只使用 Keyword 字段）
fn create_simple_schema() -> Schema {
    Schema {
        name: "demo_schema".to_string(),
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
                name: "score".to_string(),
                index: true,
                is_array: false,
                persist_option: None,
                case_sensitive: true,
            },
        ],
        persist_policy: PersistPolicy {
            max_docs_per_segment: 100,
            max_segment_age: Duration::from_secs(30),
            check_on_flush: true,
        },
    }
}

/// 写入简单数据（所有字段都是 Keyword/String 类型）
fn write_simple_data(
    partition: &std::sync::Arc<calm::partition::Partition>,
) -> Result<(), Box<dyn std::error::Error>> {
    use arrow::array::{RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use std::sync::Arc;

    let num_records = 50;

    // 构造数据（所有字段都转为字符串）
    let ids: Vec<String> = (0..num_records).map(|i| i.to_string()).collect();
    let names: Vec<String> = (0..num_records).map(|i| format!("User_{}", i)).collect();
    let scores: Vec<String> = (0..num_records).map(|i| (i % 100).to_string()).collect();

    // 构造 Arrow Schema（所有字段都是 Utf8）
    let arrow_schema = ArrowSchema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("score", DataType::Utf8, false),
    ]);

    // 构造 RecordBatch
    let batch = RecordBatch::try_new(
        Arc::new(arrow_schema),
        vec![
            Arc::new(StringArray::from(ids)),
            Arc::new(StringArray::from(names)),
            Arc::new(StringArray::from(scores)),
        ],
    )?;

    // 写入（使用 upsert）
    let row_ids = partition.upsert(batch)?;
    println!("    ✅ 写入 {} 条记录", row_ids.len());

    Ok(())
}
