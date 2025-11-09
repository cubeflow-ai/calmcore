/// Table Catalog 功能演示
///
/// 演示如何使用 Engine 的 create_table API 创建表，
/// 包括不同的分区策略和数据插入查询。
use calm::catalog::PartitionStrategy;
use calm::engine::{Engine, EngineConfig};
use calm::schema::field::FieldOption;
use calm::schema::Schema;
use std::path::PathBuf;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║          Table Catalog 功能演示                              ║");
    println!("╚══════════════════════════════════════════════════════════════╝\n");

    // 1. 创建 Engine
    let config = EngineConfig {
        data_dir: PathBuf::from("/tmp/catalog_demo"),
        persist_check_interval_secs: 60,
        max_concurrent_persists: 4,
        check_after_flush: true,
    };

    let engine = Engine::new(config)?;

    // 2. 创建 Schema（用户表）
    let user_schema = Schema {
        name: "users".to_string(),
        primary_key: Some("user_id".to_string()),
        store_source: true,
        persist_policy: Default::default(),
        fields: vec![
            FieldOption::U64 {
                name: "user_id".to_string(),
                index: true,
            },
            FieldOption::Keyword {
                name: "username".to_string(),
                index: true,
                is_array: false,
                persist_option: None,
                case_sensitive: true,
            },
            FieldOption::I32 {
                name: "age".to_string(),
                index: true,
            },
            FieldOption::Keyword {
                name: "city".to_string(),
                index: true,
                is_array: false,
                persist_option: None,
                case_sensitive: true,
            },
        ],
    };

    // 3. 创建表 - Hash 分区策略
    println!("📊 Creating table 'users' with Hash partition strategy...");
    engine
        .create_table(
            "users",
            user_schema.clone(),
            PartitionStrategy::Hash {
                field: "user_id".to_string(),
                num_partitions: 4,
            },
            4,
        )
        .await?;

    // 4. 创建 Schema（订单表）
    let order_schema = Schema {
        name: "orders".to_string(),
        primary_key: Some("order_id".to_string()),
        store_source: true,
        persist_policy: Default::default(),
        fields: vec![
            FieldOption::U64 {
                name: "order_id".to_string(),
                index: true,
            },
            FieldOption::U64 {
                name: "user_id".to_string(),
                index: true,
            },
            FieldOption::F64 {
                name: "amount".to_string(),
                index: true,
            },
            FieldOption::I64 {
                name: "timestamp".to_string(),
                index: true,
            },
        ],
    };

    // 5. 创建表 - Range 分区策略（按时间范围）
    println!("📊 Creating table 'orders' with Range partition strategy...");

    use calm::catalog::{PartitionValue, RangePartition};

    engine
        .create_table(
            "orders",
            order_schema.clone(),
            PartitionStrategy::Range {
                field: "timestamp".to_string(),
                ranges: vec![
                    RangePartition {
                        start: PartitionValue::Int64(0),
                        end: PartitionValue::Int64(1000000),
                        partition_id: 0,
                    },
                    RangePartition {
                        start: PartitionValue::Int64(1000000),
                        end: PartitionValue::Int64(2000000),
                        partition_id: 1,
                    },
                    RangePartition {
                        start: PartitionValue::Int64(2000000),
                        end: PartitionValue::MaxValue,
                        partition_id: 2,
                    },
                ],
            },
            3,
        )
        .await?;

    // 6. 创建 Schema（产品表）
    let product_schema = Schema {
        name: "products".to_string(),
        primary_key: Some("product_id".to_string()),
        store_source: true,
        persist_policy: Default::default(),
        fields: vec![
            FieldOption::U64 {
                name: "product_id".to_string(),
                index: true,
            },
            FieldOption::Keyword {
                name: "category".to_string(),
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
            FieldOption::F64 {
                name: "price".to_string(),
                index: true,
            },
        ],
    };

    // 7. 创建表 - List 分区策略（按类别）
    println!("📊 Creating table 'products' with List partition strategy...");

    use std::collections::HashMap;
    let mut category_map = HashMap::new();
    category_map.insert("electronics".to_string(), 0);
    category_map.insert("books".to_string(), 1);
    category_map.insert("clothing".to_string(), 2);

    engine
        .create_table(
            "products",
            product_schema.clone(),
            PartitionStrategy::List {
                field: "category".to_string(),
                values: category_map,
            },
            3,
        )
        .await?;

    // 8. 列出所有表
    println!("\n📋 List all tables:");
    let tables = engine.list_tables();
    for table_name in &tables {
        println!("  ✓ {}", table_name);
    }

    // 9. 查看表元数据
    println!("\n📄 Table metadata:");
    for table_name in &tables {
        let meta = engine.get_table_meta(table_name)?;
        println!("\n  Table: {}", meta.table_name);
        println!("  Schema: {}", meta.schema.name);
        println!("  Fields: {}", meta.schema.fields.len());
        println!("  Partitions: {}", meta.parallel_workers);
        println!("  Strategy: {:?}", meta.partition_strategy);
    }

    // 10. 测试分区路由
    println!("\n🔀 Testing partition routing:");

    // Hash 分区路由测试
    println!("\n  Hash partition (users):");
    for user_id in &["1001", "1002", "1003", "1004"] {
        let partition_id = engine.route_partition("users", user_id)?;
        println!("    user_id {} -> partition {}", user_id, partition_id);
    }

    // Range 分区路由测试
    println!("\n  Range partition (orders):");
    for timestamp in &["500000", "1500000", "2500000"] {
        let partition_id = engine.route_partition("orders", timestamp)?;
        println!("    timestamp {} -> partition {}", timestamp, partition_id);
    }

    // List 分区路由测试
    println!("\n  List partition (products):");
    for category in &["electronics", "books", "clothing", "other"] {
        let partition_id = engine.route_partition("products", category)?;
        println!("    category '{}' -> partition {}", category, partition_id);
    }

    // 11. 获取 Engine 统计信息
    println!("\n");
    engine.print_stats().await;

    // 12. 清理：删除一个表
    println!("🗑️  Dropping table 'products'...");
    engine.drop_table("products").await?;

    println!("\n📋 Remaining tables after drop:");
    let remaining_tables = engine.list_tables();
    for table_name in &remaining_tables {
        println!("  ✓ {}", table_name);
    }

    // 13. 关闭 Engine
    println!("\n🔒 Shutting down engine...");
    engine.stop().await?;

    println!("\n✅ Demo completed successfully!");
    println!("\n💡 Check the directory structure:");
    println!("   /tmp/catalog_demo/tables/");
    println!("   ├── users/");
    println!("   │   ├── meta.json");
    println!("   │   └── partitions/");
    println!("   │       ├── partition-0/");
    println!("   │       ├── partition-1/");
    println!("   │       ├── partition-2/");
    println!("   │       └── partition-3/");
    println!("   └── orders/");
    println!("       ├── meta.json");
    println!("       └── partitions/");
    println!("           ├── partition-0/");
    println!("           ├── partition-1/");
    println!("           └── partition-2/");

    Ok(())
}
