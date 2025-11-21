// GraphQL 查询接口演示
// 演示如何通过 GraphQL 查看表、分区和段的详细信息

use calm::{
    catalog::{PartitionStrategy, PartitionValue, RangePartition},
    engine::{Engine, EngineConfig},
    protocol::graphql::GraphQLServer,
    schema::{field::FieldOption, PersistPolicy, Schema},
};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== GraphQL 查询接口演示 ===\n");

    // 1. 创建 Engine
    let config = EngineConfig {
        data_dir: "./data".into(),
        ..Default::default()
    };
    let engine = Engine::new(config)?;

    // 2. 创建测试表（Hash 分区策略）
    println!("📋 创建测试表 'users'...");
    let schema = Schema {
        name: "users".to_string(),
        primary_key: Some("user_id".to_string()),
        store_source: true,
        fields: vec![
            FieldOption::Keyword {
                name: "user_id".to_string(),
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
            FieldOption::I32 {
                name: "age".to_string(),
                index: true,
            },
            FieldOption::Keyword {
                name: "email".to_string(),
                index: true,
                is_array: false,
                persist_option: None,
                case_sensitive: false,
            },
        ],
        persist_policy: PersistPolicy::default(),
    };

    engine
        .create_table(
            "users",
            schema,
            PartitionStrategy::Hash {
                field: "user_id".to_string(),
                num_partitions: 4,
            },
            4,
        )
        .await?;

    println!("✅ 表 'users' 创建成功\n");

    // 3. 插入测试数据
    println!("📝 插入测试数据...");
    let test_data = vec![
        json!({"user_id": "user001", "name": "Alice", "age": 25, "email": "alice@example.com"}),
        json!({"user_id": "user002", "name": "Bob", "age": 30, "email": "bob@example.com"}),
        json!({"user_id": "user003", "name": "Charlie", "age": 35, "email": "charlie@example.com"}),
        json!({"user_id": "user004", "name": "David", "age": 28, "email": "david@example.com"}),
        json!({"user_id": "user005", "name": "Eve", "age": 32, "email": "eve@example.com"}),
        json!({"user_id": "user006", "name": "Frank", "age": 27, "email": "frank@example.com"}),
        json!({"user_id": "user007", "name": "Grace", "age": 29, "email": "grace@example.com"}),
        json!({"user_id": "user008", "name": "Henry", "age": 31, "email": "henry@example.com"}),
    ];

    for doc in &test_data {
        let user_id = doc["user_id"].as_str().unwrap();
        let partition_id = engine.route_partition("users", user_id)?;
        let partition = engine
            .get_partition("users", &partition_id)
            .await
            .ok_or("Partition not found")?;
        partition.upsert_json(&[doc.clone()])?;
    }

    println!("✅ 插入 {} 条数据\n", test_data.len());

    // 4. 创建第二个表（Range 分区策略）
    println!("📋 创建测试表 'products'...");
    let schema2 = Schema {
        name: "products".to_string(),
        primary_key: Some("product_id".to_string()),
        store_source: true,
        fields: vec![
            FieldOption::I64 {
                name: "product_id".to_string(),
                index: true,
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
        persist_policy: PersistPolicy::default(),
    };

    engine
        .create_table(
            "products",
            schema2,
            PartitionStrategy::Range {
                field: "product_id".to_string(),
                ranges: vec![
                    RangePartition {
                        start: PartitionValue::Int64(0),
                        end: PartitionValue::Int64(100),
                        partition_id: 0,
                    },
                    RangePartition {
                        start: PartitionValue::Int64(100),
                        end: PartitionValue::Int64(200),
                        partition_id: 1,
                    },
                    RangePartition {
                        start: PartitionValue::Int64(200),
                        end: PartitionValue::Int64(300),
                        partition_id: 2,
                    },
                ],
            },
            3,
        )
        .await?;

    println!("✅ 表 'products' 创建成功\n");

    // 5. 向 products 插入数据
    let product_data = vec![
        json!({"product_id": 50, "name": "Laptop", "price": 999.99}),
        json!({"product_id": 150, "name": "Phone", "price": 599.99}),
        json!({"product_id": 250, "name": "Tablet", "price": 399.99}),
    ];

    for doc in &product_data {
        let product_id = doc["product_id"].to_string();
        let partition_id = engine.route_partition("products", &product_id)?;
        let partition = engine
            .get_partition("products", &partition_id)
            .await
            .ok_or("Partition not found")?;
        partition.upsert_json(&[doc.clone()])?;
    }

    println!("✅ 插入 {} 条产品数据\n", product_data.len());

    // 6. 显示如何通过代码查询分区和段信息
    println!("=== 通过 Engine API 查询信息 ===\n");

    // 列出所有表
    let tables = engine.list_tables();
    println!("📊 所有表: {:?}\n", tables);

    // 查询 users 表的分区信息
    println!("📊 表 'users' 的分区信息:");
    let user_partitions = engine.list_partitions("users").await;
    for partition_id in &user_partitions {
        if let Some(partition) = engine.get_partition("users", partition_id).await {
            let frozen_segments = partition.get_frozen_segments();
            let current_segment = partition.get_current_segment();

            println!("  分区 ID: {}", partition_id);
            println!("    冻结段数量: {}", frozen_segments.len());
            println!("    当前段文档数: {}", current_segment.doc_count());

            // 释放读锁
            drop(frozen_segments);
            drop(current_segment);
        }
    }
    println!();

    // 查询 products 表的分区信息
    println!("📊 表 'products' 的分区信息:");
    let product_partitions = engine.list_partitions("products").await;
    for partition_id in &product_partitions {
        if let Some(partition) = engine.get_partition("products", partition_id).await {
            let current_segment = partition.get_current_segment();
            println!("  分区 ID: {}", partition_id);
            println!("    当前段文档数: {}", current_segment.doc_count());
            drop(current_segment);
        }
    }
    println!();

    // 7. 启动 GraphQL 服务器
    println!("🚀 启动 GraphQL 服务器...");
    println!("📍 服务地址: http://127.0.0.1:8080");
    println!("📍 GraphQL 端点: http://127.0.0.1:8080/graphql");
    println!("📍 Playground: http://127.0.0.1:8080/playground");
    println!();
    println!("=== GraphQL 查询示例 ===\n");

    println!("1️⃣ 列出所有表:");
    println!(
        r#"
query {{
  tables
}}
"#
    );

    println!("2️⃣ 获取表的基本信息:");
    println!(
        r#"
query {{
  table(name: "users") {{
    name
    partitionCount
    primaryKey
    fields {{
      name
      fieldType
      indexed
    }}
  }}
}}
"#
    );

    println!("3️⃣ 获取表的所有分区和段信息:");
    println!(
        r#"
query {{
  partitions(table: "users") {{
    partitionId
    segmentCount
    segments {{
      segmentId
      docCount
      deletedCount
      isPersisted
      basePath
    }}
  }}
}}
"#
    );

    println!("4️⃣ 获取表的完整详情（包括统计信息）:");
    println!(
        r#"
query {{
  tableDetail(name: "users") {{
    name
    partitionCount
    totalSegments
    totalDocuments
    primaryKey
    fields {{
      name
      fieldType
    }}
    partitions {{
      partitionId
      segmentCount
      segments {{
        segmentId
        docCount
        isPersisted
      }}
    }}
  }}
}}
"#
    );

    println!("5️⃣ 查看 Range 分区策略的表:");
    println!(
        r#"
query {{
  tableDetail(name: "products") {{
    name
    partitionCount
    totalDocuments
    partitions {{
      partitionId
      segments {{
        docCount
      }}
    }}
  }}
}}
"#
    );

    println!("\n⏳ 按 Ctrl+C 停止服务器...\n");

    // 启动服务器
    let server = GraphQLServer::new(engine.clone());
    server.start("127.0.0.1:8080").await?;

    Ok(())
}
