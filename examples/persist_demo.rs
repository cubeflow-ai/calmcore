/// 演示 Engine 持久化接口的简洁用法
///
/// 展示两个核心接口：
/// 1. engine.persist_partition(id) - 同步持久化指定 partition
/// 2. engine.stop() - 停止 engine 并持久化所有数据
use calm::engine::{Engine, EngineConfig};
use calm::schema::field::FieldOption;
use calm::schema::{PersistPolicy, Schema};
use serde_json::json;
use std::path::PathBuf;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("╔════════════════════════════════════════════════════╗");
    println!("║       Calm Engine 持久化接口演示                    ║");
    println!("╚════════════════════════════════════════════════════╝\n");

    // 1. 创建 Engine
    let config = EngineConfig {
        data_dir: PathBuf::from("/tmp/calm_persist_demo"),
        persist_check_interval_secs: 60,
        ..Default::default()
    };

    let engine = Engine::new(config);
    println!("✅ Engine 创建成功\n");

    // 2. 创建 Schema（简单的用户表）
    let schema = Schema {
        name: "users".to_string(),
        primary_key: None, // 简化演示，不使用主键
        store_source: false,
        fields: vec![
            FieldOption::I64 {
                name: "id".to_string(),
                index: true,
            },
            FieldOption::Keyword {
                name: "name".to_string(),
                index: true,
                is_array: false,
                persist_option: None,
                case_sensitive: true,
            },
            FieldOption::I64 {
                name: "age".to_string(),
                index: true,
            },
        ],
        persist_policy: PersistPolicy {
            max_docs_per_segment: 10_000, // 演示用，设小一点
            ..Default::default()
        },
    };

    // 3. 创建两个 Partition
    let partition1 = engine.create_partition(1, schema.clone()).await;
    let partition2 = engine.create_partition(2, schema.clone()).await;
    println!("✅ 创建了 2 个 Partition\n");

    // 4. 写入数据到 partition 1
    println!("📝 向 Partition 1 写入 15,000 条数据...");
    for i in 0..15_000 {
        let doc = json!({
            "id": i,
            "name": format!("user_{}", i),
            "age": 20 + (i % 50),
        });

        partition1.upsert_json(&[doc])?;
    }
    println!("✅ Partition 1 写入完成 (15,000 条)\n");

    // 5. 写入数据到 partition 2
    println!("📝 向 Partition 2 写入 8,000 条数据...");
    for i in 0..8_000 {
        let doc = json!({
            "id": i + 100_000,
            "name": format!("admin_{}", i),
            "age": 25 + (i % 40),
        });

        partition2.upsert_json(&[doc])?;
    }
    println!("✅ Partition 2 写入完成 (8,000 条)\n");

    // 6. 手动持久化 Partition 1（同步操作）
    println!("💾 手动持久化 Partition 1...");
    engine.persist_partition(1).await?;
    println!("✅ Partition 1 持久化完成（所有数据已写入磁盘）\n");

    // 7. 继续写入更多数据到 Partition 2
    println!("📝 继续向 Partition 2 写入 5,000 条数据...");
    for i in 8_000..13_000 {
        let doc = json!({
            "id": i + 100_000,
            "name": format!("admin_{}", i),
            "age": 25 + (i % 40),
        });

        partition2.upsert_json(&[doc])?;
    }
    println!("✅ Partition 2 继续写入完成 (现在总共 13,000 条)\n");

    // 8. 停止 Engine（会自动持久化所有剩余数据）
    println!("🛑 停止 Engine（自动持久化所有数据）...\n");
    engine.stop().await?;

    println!("\n╔════════════════════════════════════════════════════╗");
    println!("║                演示完成                            ║");
    println!("╠════════════════════════════════════════════════════╣");
    println!("║ • Partition 1: 手动持久化 (persist_partition)      ║");
    println!("║ • Partition 2: 自动持久化 (engine.stop)            ║");
    println!("║ • 所有数据已安全写入磁盘                           ║");
    println!("╚════════════════════════════════════════════════════╝\n");

    println!("数据目录: /tmp/calm_persist_demo");
    println!("你可以检查该目录下的 partition-1 和 partition-2 文件夹");

    Ok(())
}
