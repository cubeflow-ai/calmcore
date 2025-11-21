/// Custom 分区 load_segment 功能演示
/// 
/// 这个示例展示如何使用 Custom 分区策略加载外部文件
use calm::catalog::PartitionStrategy;
use calm::engine::{Engine, EngineConfig};
use calm::schema::field::FieldOption;
use calm::schema::{PersistPolicy, Schema};
use calm::segment_loader::FileHandlerType;
use std::path::PathBuf;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化日志
    env_logger::init();

    println!("=== Custom Partition Load Segment Demo ===\n");

    // 1. 创建 Engine
    println!("Step 1: Creating Engine...");
    let config = EngineConfig {
        data_dir: PathBuf::from("./data_custom_demo"),
        persist_check_interval_secs: 60,
        max_concurrent_persists: 4,
        check_after_flush: true,
    };
    let engine = Engine::new(config)?;
    println!("✓ Engine created\n");

    // 2. 创建使用 Custom 分区策略的表
    println!("Step 2: Creating table with Custom partition strategy...");
    let schema = Schema {
        name: "access_logs".to_string(),
        primary_key: Some("log_id".to_string()),
        store_source: true,
        fields: vec![
            FieldOption::Keyword {
                name: "log_id".to_string(),
                index: true,
                is_array: false,
                persist_option: None,
                case_sensitive: true,
            },
            FieldOption::I64 {
                name: "timestamp".to_string(),
                index: true,
            },
            FieldOption::Keyword {
                name: "ip".to_string(),
                index: true,
                is_array: false,
                persist_option: None,
                case_sensitive: false,
            },
            FieldOption::Keyword {
                name: "method".to_string(),
                index: true,
                is_array: false,
                persist_option: None,
                case_sensitive: false,
            },
            FieldOption::Keyword {
                name: "path".to_string(),
                index: true,
                is_array: false,
                persist_option: None,
                case_sensitive: false,
            },
            FieldOption::I32 {
                name: "status".to_string(),
                index: true,
            },
        ],
        persist_policy: PersistPolicy::default(),
    };

    engine
        .create_table(
            "access_logs",
            schema,
            PartitionStrategy::Custom,
            1, // Custom 分区不需要预定义数量
        )
        .await?;
    println!("✓ Table 'access_logs' created with Custom partition strategy\n");

    // 3. 准备测试数据文件
    println!("Step 3: Preparing test data files...");
    let temp_dir = PathBuf::from("./temp_data");
    std::fs::create_dir_all(&temp_dir)?;

    // 创建第一个数据文件（2024-01）
    let file1_path = temp_dir.join("logs_2024_01.jsonl");
    let data1 = r#"{"log_id": "log_001", "timestamp": 1704067200, "ip": "192.168.1.100", "method": "GET", "path": "/api/users", "status": 200}
{"log_id": "log_002", "timestamp": 1704067201, "ip": "192.168.1.101", "method": "POST", "path": "/api/login", "status": 200}
{"log_id": "log_003", "timestamp": 1704067202, "ip": "192.168.1.102", "method": "GET", "path": "/api/products", "status": 404}
{"log_id": "log_004", "timestamp": 1704067203, "ip": "192.168.1.100", "method": "DELETE", "path": "/api/users/123", "status": 403}
"#;
    std::fs::write(&file1_path, data1)?;
    println!("✓ Created test file: {:?}", file1_path);

    // 创建第二个数据文件（2024-02）
    let file2_path = temp_dir.join("logs_2024_02.jsonl");
    let data2 = r#"{"log_id": "log_101", "timestamp": 1706745600, "ip": "10.0.0.50", "method": "GET", "path": "/api/orders", "status": 200}
{"log_id": "log_102", "timestamp": 1706745601, "ip": "10.0.0.51", "method": "POST", "path": "/api/checkout", "status": 500}
{"log_id": "log_103", "timestamp": 1706745602, "ip": "10.0.0.52", "method": "GET", "path": "/api/status", "status": 200}
"#;
    std::fs::write(&file2_path, data2)?;
    println!("✓ Created test file: {:?}\n", file2_path);

    // 4. 使用 load_segment 加载第一个文件
    println!("Step 4: Loading first file (2024-01)...");
    let doc_count1 = engine
        .load_segment(
            "access_logs",
            None,
            Some("2024-01".to_string()),
            file1_path.clone(),
            FileHandlerType::Copy,
        )
        .await?;
    println!("✓ Loaded {} documents to partition '2024-01'\n", doc_count1);

    // 5. 使用 load_segment 加载第二个文件
    println!("Step 5: Loading second file (2024-02)...");
    let doc_count2 = engine
        .load_segment(
            "access_logs",
            None,
            Some("2024-02".to_string()),
            file2_path.clone(),
            FileHandlerType::Copy,
        )
        .await?;
    println!("✓ Loaded {} documents to partition '2024-02'\n", doc_count2);

    // 6. 查询所有数据
    println!("Step 6: Querying all data...");
    let result = engine.execute_sql("SELECT * FROM access_logs").await?;
    println!(
        "✓ Total documents in table: {}\n",
        result.batch.num_rows()
    );

    // 7. 按条件查询
    println!("Step 7: Querying with conditions...");

    // 查询所有 GET 请求
    let get_result = engine
        .execute_sql("SELECT * FROM access_logs WHERE method = 'GET'")
        .await?;
    println!("✓ GET requests: {}", get_result.batch.num_rows());

    // 查询所有错误状态（4xx, 5xx）
    let error_result = engine
        .execute_sql("SELECT * FROM access_logs WHERE status >= 400")
        .await?;
    println!("✓ Error responses (4xx/5xx): {}", error_result.batch.num_rows());

    // 查询特定 IP
    let ip_result = engine
        .execute_sql("SELECT * FROM access_logs WHERE ip = '192.168.1.100'")
        .await?;
    println!(
        "✓ Requests from 192.168.1.100: {}\n",
        ip_result.batch.num_rows()
    );

    // 8. 列出所有 partition
    println!("Step 8: Listing partitions...");
    let partitions = engine.list_partitions("access_logs").await;
    println!("✓ Partitions in table: {:?}\n", partitions);

    // 9. 持久化数据
    println!("Step 9: Persisting data...");
    engine.flush_table("access_logs").await?;
    println!("✓ All data persisted to disk\n");

    // 10. 清理
    println!("Step 10: Cleaning up...");
    engine.stop().await?;
    std::fs::remove_dir_all(&temp_dir)?;
    println!("✓ Cleanup completed\n");

    println!("=== Demo completed successfully! ===");
    println!("\nSummary:");
    println!("- Created table with Custom partition strategy");
    println!("- Loaded {} documents from file 1", doc_count1);
    println!("- Loaded {} documents from file 2", doc_count2);
    println!(
        "- Total documents: {}",
        doc_count1 + doc_count2
    );
    println!("- Created {} partitions", partitions.len());

    Ok(())
}
