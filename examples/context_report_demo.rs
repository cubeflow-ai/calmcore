use async_graphql_poem::GraphQL;
use calm::catalog::PartitionStrategy;
use calm::engine::{Engine, EngineConfig};
use calm::protocol::graphql::GraphQLServer;
use calm::protocol::mysql::MysqlServer;
use calm::schema::field::FieldOption;
use calm::schema::Schema;
use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use datafusion::arrow::record_batch::RecordBatch;
use serde_json::Value;
use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. 初始化 Engine
    let config = EngineConfig::default();
    let engine = Engine::new(config)?;

    // 加载已有的表
    engine.load_existing_tables().await?;

    println!("=== Context Report Demo ===\n");

    // 2. 创建 Schema
    let schema = Schema {
        name: "context_report".to_string(),
        primary_key: None, // 不需要主键
        store_source: true,
        fields: vec![
            // Keyword 字段
            FieldOption::Keyword {
                name: "userBasicInfo_extMap".to_string(),
                index: true,
                is_array: false,
                persist_option: None,
                case_sensitive: true,
            },
            FieldOption::Keyword {
                name: "appClient_clientName".to_string(),
                index: true,
                is_array: false,
                persist_option: None,
                case_sensitive: true,
            },
            FieldOption::Keyword {
                name: "trafficTag_functionId".to_string(),
                index: true,
                is_array: false,
                persist_option: None,
                case_sensitive: true,
            },
            FieldOption::Keyword {
                name: "envSetting_timeZone".to_string(),
                index: true,
                is_array: false,
                persist_option: None,
                case_sensitive: true,
            },
            FieldOption::Keyword {
                name: "appClient_ip".to_string(),
                index: true,
                is_array: false,
                persist_option: None,
                case_sensitive: true,
            },
            FieldOption::Keyword {
                name: "appClient_model".to_string(),
                index: true,
                is_array: false,
                persist_option: None,
                case_sensitive: true,
            },
            FieldOption::Keyword {
                name: "traceFrom_preFrom".to_string(),
                index: true,
                is_array: false,
                persist_option: None,
                case_sensitive: true,
            },
            // Integer 字段
            FieldOption::I32 {
                name: "contextSize".to_string(),
                index: true,
            },
            FieldOption::I32 {
                name: "action".to_string(),
                index: true,
            },
            // 更多 Keyword 字段
            FieldOption::Keyword {
                name: "trafficTag_forcebot".to_string(),
                index: true,
                is_array: false,
                persist_option: None,
                case_sensitive: true,
            },
            FieldOption::Keyword {
                name: "trafficTag_trafficName".to_string(),
                index: true,
                is_array: false,
                persist_option: None,
                case_sensitive: true,
            },
            FieldOption::Keyword {
                name: "traceFrom_api".to_string(),
                index: true,
                is_array: false,
                persist_option: None,
                case_sensitive: true,
            },
            FieldOption::Keyword {
                name: "instanceId".to_string(),
                index: true,
                is_array: false,
                persist_option: None,
                case_sensitive: true,
            },
            FieldOption::Keyword {
                name: "trafficTag_requestId".to_string(),
                index: true,
                is_array: false,
                persist_option: None,
                case_sensitive: true,
            },
            FieldOption::Keyword {
                name: "trafficTag_trafficId".to_string(),
                index: true,
                is_array: false,
                persist_option: None,
                case_sensitive: true,
            },
            FieldOption::Keyword {
                name: "abTag_extMap".to_string(),
                index: true,
                is_array: false,
                persist_option: None,
                case_sensitive: true,
            },
            FieldOption::Keyword {
                name: "appClient_extMap".to_string(),
                index: true,
                is_array: false,
                persist_option: None,
                case_sensitive: true,
            },
            FieldOption::Keyword {
                name: "traceFrom_from".to_string(),
                index: true,
                is_array: false,
                persist_option: None,
                case_sensitive: true,
            },
            FieldOption::Keyword {
                name: "envSetting_extMap".to_string(),
                index: true,
                is_array: false,
                persist_option: None,
                case_sensitive: true,
            },
            FieldOption::Keyword {
                name: "traceFrom_requestId".to_string(),
                index: true,
                is_array: false,
                persist_option: None,
                case_sensitive: true,
            },
            FieldOption::Keyword {
                name: "abTag_expInfo".to_string(),
                index: true,
                is_array: false,
                persist_option: None,
                case_sensitive: true,
            },
            FieldOption::Keyword {
                name: "app".to_string(),
                index: true,
                is_array: false,
                persist_option: None,
                case_sensitive: true,
            },
            FieldOption::Keyword {
                name: "envSetting_currency".to_string(),
                index: true,
                is_array: false,
                persist_option: None,
                case_sensitive: true,
            },
            FieldOption::Keyword {
                name: "envSetting_language".to_string(),
                index: true,
                is_array: false,
                persist_option: None,
                case_sensitive: true,
            },
            FieldOption::Keyword {
                name: "appClient_openudid".to_string(),
                index: true,
                is_array: false,
                persist_option: None,
                case_sensitive: true,
            },
            FieldOption::Keyword {
                name: "appClient_clientVersion".to_string(),
                index: true,
                is_array: false,
                persist_option: None,
                case_sensitive: true,
            },
            FieldOption::Keyword {
                name: "jdosEnv".to_string(),
                index: true,
                is_array: false,
                persist_option: None,
                case_sensitive: true,
            },
            FieldOption::Keyword {
                name: "userBasicInfo_pin".to_string(),
                index: true,
                is_array: false,
                persist_option: None,
                case_sensitive: true,
            },
            FieldOption::Keyword {
                name: "ip".to_string(),
                index: true,
                is_array: false,
                persist_option: None,
                case_sensitive: true,
            },
            FieldOption::Keyword {
                name: "pfinderTraceId".to_string(),
                index: true,
                is_array: false,
                persist_option: None,
                case_sensitive: true,
            },
            FieldOption::Keyword {
                name: "appClient_uuid".to_string(),
                index: true,
                is_array: false,
                persist_option: None,
                case_sensitive: true,
            },
            FieldOption::I64 {
                name: "updateTime".to_string(),
                index: true,
            },
            FieldOption::Keyword {
                name: "trafficTag_extMap".to_string(),
                index: true,
                is_array: false,
                persist_option: None,
                case_sensitive: true,
            },
            FieldOption::Keyword {
                name: "appClient_osVersion".to_string(),
                index: true,
                is_array: false,
                persist_option: None,
                case_sensitive: true,
            },
            FieldOption::Boolean {
                name: "traceFrom_sampling".to_string(),
                index: true,
            },
            FieldOption::Keyword {
                name: "appClient_eid".to_string(),
                index: true,
                is_array: false,
                persist_option: None,
                case_sensitive: true,
            },
            FieldOption::Keyword {
                name: "appClient_brand".to_string(),
                index: true,
                is_array: false,
                persist_option: None,
                case_sensitive: true,
            },
            FieldOption::Keyword {
                name: "appClient_areaParam".to_string(),
                index: true,
                is_array: false,
                persist_option: None,
                case_sensitive: true,
            },
            FieldOption::Keyword {
                name: "appClient_appId".to_string(),
                index: true,
                is_array: false,
                persist_option: None,
                case_sensitive: true,
            },
            FieldOption::Keyword {
                name: "jdosGroup".to_string(),
                index: true,
                is_array: false,
                persist_option: None,
                case_sensitive: true,
            },
        ],
        persist_policy: Default::default(),
    };

    // 3. 创建表 (使用 Hash 策略，基于 instanceId 字段)
    let table_name = "context_report";

    // 检查表是否已存在
    let table_exists = engine.list_tables().contains(&table_name.to_string());

    if !table_exists {
        println!("✓ Creating table '{}'...", table_name);
        engine
            .create_table(
                table_name,
                schema.clone(),
                PartitionStrategy::Hash {
                    field: "instanceId".to_string(),
                    num_partitions: 3,
                },
                3, // 3 个 partitions
            )
            .await?;
        println!("✓ Table '{}' created with 3 partitions\n", table_name);

        // 4. 读取 JSON 文件并插入数据 (仅在表不存在时)
        let json_file_path = "/Users/sunjian11/javaworkspace/jmqbykafka/context_report.json";

        println!("  ℹ Not a JSON array, trying NDJSON format...");
        let mut json_data: Vec<Value> = Vec::new();
        let file = File::open(json_file_path)?;
        let reader = BufReader::new(file);
        use std::io::BufRead;

        for line in reader.lines() {
            let line = line?;
            if !line.trim().is_empty() {
                if let Ok(obj) = serde_json::from_str::<Value>(&line) {
                    json_data.push(obj);
                }
            }
        }

        println!("✓ Found {} records in JSON file", json_data.len());

        // 5. 转换 JSON 数据为 Arrow RecordBatch 并批量插入
        if !json_data.is_empty() {
            println!("✓ Converting JSON to Arrow batches...");

            // 批量处理数据
            let batch_size = 1000;
            let mut total_inserted = 0;

            for chunk in json_data.chunks(batch_size) {
                let record_batch = json_to_record_batch(chunk, &schema)?;

                // 使用 Hash 策略，根据 instanceId 字段路由到不同的 partition
                // 这里我们简化处理：批量插入到所有 partition
                for partition_id in 0..3 {
                    let partition = engine
                        .get_partition(table_name, partition_id)
                        .await
                        .ok_or("Partition not found")?;

                    // 只插入属于该 partition 的数据
                    // 简化处理：将整个 batch 插入到第一个 partition
                    if partition_id == 0 {
                        partition.upsert(record_batch.clone())?;
                        total_inserted += chunk.len();
                        println!(
                            "  ✓ Inserted {} records (total: {})",
                            chunk.len(),
                            total_inserted
                        );
                        break;
                    }
                }
            }

            println!("\n✓ Successfully inserted {} records\n", total_inserted);

            // // 🔥 重要：手动 flush 所有 partition，确保数据移到 frozen_segments
            // println!("✓ Flushing all partitions...");
            // for partition_id in 0..3 {
            //     if let Some(partition) = engine.get_partition(table_name, partition_id).await {
            //         match partition.flush(false) {
            //             // force flush
            //             Ok(seg_id) => {
            //                 if seg_id > 0 {
            //                     println!(
            //                         "  ✓ Partition {} flushed (segment {})",
            //                         partition_id, seg_id
            //                     );
            //                 }
            //             }
            //             Err(e) => eprintln!("  ✗ Partition {} flush failed: {:?}", partition_id, e),
            //         }
            //     }
            // }

            // // 🔥 重要：等待持久化完成（给后台任务时间完成）
            // println!("✓ Waiting for background persist to complete...");
            // tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
            // println!("✓ Data persisted to disk\n");
        }
    } else {
        println!(
            "✓ Table '{}' already exists, skipping data import\n",
            table_name
        );
    }

    // 6. 启动 MySQL 服务器
    println!("=== Starting MySQL Server ===\n");
    println!("✓ MySQL Server starting on 127.0.0.1:3307");
    println!("\n💡 Connect using:");
    println!("   mysql -h 127.0.0.1 -P 3307 -u root\n");
    println!("📚 Example Queries:");
    println!("   SELECT COUNT(*) FROM context_report;");
    println!("   SELECT app, COUNT(*) FROM context_report GROUP BY app;");
    println!("   SELECT * FROM context_report WHERE action = 1 LIMIT 10;");
    println!("   SELECT * FROM context_report WHERE \"envSetting_language\" = 'en_US' LIMIT 10;");
    println!("   SELECT \"appClient_clientName\", ip FROM context_report WHERE \"traceFrom_sampling\" = true LIMIT 10;");
    println!("\n⚠️  Note: Mixed-case field names require double quotes (e.g., \\\"envSetting_language\\\")\n");

    let graphql_handle = tokio::spawn({
        let engine = engine.clone();
        async move {
            let graphql_server = GraphQLServer::new(engine);
            graphql_server.start("127.0.0.1:8080").await
        }
    });

    let mysql_result = MysqlServer::new(engine).start("127.0.0.1:3307").await;

    // 等待 GraphQL 服务器结束(通常不会)
    let _ = graphql_handle.await;

    mysql_result
}

/// 将 JSON 数据转换为 Arrow RecordBatch
fn json_to_record_batch(
    json_records: &[Value],
    schema: &Schema,
) -> Result<RecordBatch, Box<dyn std::error::Error>> {
    let mut columns: Vec<Arc<dyn datafusion::arrow::array::Array>> = Vec::new();
    let mut arrow_fields: Vec<Field> = Vec::new();

    for field in &schema.fields {
        match field {
            FieldOption::Keyword { name, .. } => {
                let values: Vec<Option<String>> = json_records
                    .iter()
                    .map(|record| {
                        record
                            .get(name)
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string())
                    })
                    .collect();

                columns.push(Arc::new(StringArray::from(values)));
                arrow_fields.push(Field::new(name, DataType::Utf8, true));
            }
            FieldOption::I32 { name, .. } => {
                let values: Vec<Option<i32>> = json_records
                    .iter()
                    .map(|record| record.get(name).and_then(|v| v.as_i64()).map(|i| i as i32))
                    .collect();

                columns.push(Arc::new(Int32Array::from(values)));
                arrow_fields.push(Field::new(name, DataType::Int32, true));
            }
            FieldOption::I64 { name, .. } => {
                let values: Vec<Option<i64>> = json_records
                    .iter()
                    .map(|record| record.get(name).and_then(|v| v.as_i64()))
                    .collect();

                columns.push(Arc::new(Int64Array::from(values)));
                arrow_fields.push(Field::new(name, DataType::Int64, true));
            }
            FieldOption::Boolean { name, .. } => {
                let values: Vec<Option<bool>> = json_records
                    .iter()
                    .map(|record| record.get(name).and_then(|v| v.as_bool()))
                    .collect();

                columns.push(Arc::new(BooleanArray::from(values)));
                arrow_fields.push(Field::new(name, DataType::Boolean, true));
            }
            _ => {}
        }
    }

    let arrow_schema = Arc::new(ArrowSchema::new(arrow_fields));
    let batch = RecordBatch::try_new(arrow_schema, columns)?;

    Ok(batch)
}
