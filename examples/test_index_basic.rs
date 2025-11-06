use calm::{
    partition::Partition,
    schema::{field::FieldOption, Schema},
};
use datafusion::arrow::{
    array::{Int64Array, StringArray},
    datatypes::{DataType, Field, Schema as ArrowSchema},
    record_batch::RecordBatch,
};
use std::{path::PathBuf, sync::Arc};
use tokio::sync::mpsc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Basic Index Test ===\n");

    // 1. 创建简单的 Schema
    let schema = Schema {
        name: "test".to_string(),
        primary_key: None,
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
        ],
        persist_policy: Default::default(),
    };

    let (persist_tx, mut persist_rx) = mpsc::unbounded_channel();

    tokio::spawn(async move {
        while let Some(partition_id) = persist_rx.recv().await {
            println!(
                "  [Background] Partition {} needs persistence",
                partition_id
            );
        }
    });

    let partition = Arc::new(Partition::new(
        1,
        PathBuf::from("/tmp/calm_index_test"),
        schema.clone(),
        persist_tx,
    ));

    // 2. 准备 Arrow Schema
    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
    ]));

    // 3. 插入 10 条测试数据
    println!("Step 1: Inserting 10 test records...");

    let ids = vec![1i64, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    let names = vec![
        "Alice", "Bob", "Charlie", "David", "Eve", "Frank", "Grace", "Henry", "Ivy", "Jack",
    ];

    let batch = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![
            Arc::new(Int64Array::from(ids.clone())),
            Arc::new(StringArray::from(
                names.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
            )),
        ],
    )?;

    partition.upsert(batch)?;

    println!("✓ Inserted 10 records");
    println!(
        "  Current segment doc_count: {}",
        partition.get_current_segment().doc_count()
    );

    // 4. 直接测试 Segment 的索引查询
    println!("\nStep 2: Testing index queries directly on Segment...");

    let segment = partition.get_current_segment();

    // 测试 id 字段查询
    println!("\nTest A: Query id=5");
    use datafusion::scalar::ScalarValue;
    if let Some(bitmap) = segment.query_field("id", &ScalarValue::Int64(Some(5))) {
        println!("  ✓ Found {} matching docs", bitmap.len());
        println!("  Doc IDs: {:?}", bitmap.iter().collect::<Vec<_>>());
    } else {
        println!("  ✗ Query returned None!");
    }

    // 测试 name 字段查询
    println!("\nTest B: Query name='Alice'");
    if let Some(bitmap) = segment.query_field("name", &ScalarValue::Utf8(Some("Alice".to_string())))
    {
        println!("  ✓ Found {} matching docs", bitmap.len());
        println!("  Doc IDs: {:?}", bitmap.iter().collect::<Vec<_>>());
    } else {
        println!("  ✗ Query returned None!");
    }

    // 测试不存在的值
    println!("\nTest C: Query id=999 (should not exist)");
    if let Some(bitmap) = segment.query_field("id", &ScalarValue::Int64(Some(999))) {
        println!("  Found {} matching docs", bitmap.len());
        if bitmap.is_empty() {
            println!("  ✓ Correctly returned empty bitmap");
        } else {
            println!("  ✗ Should be empty but has {} docs", bitmap.len());
        }
    } else {
        println!("  ✗ Query returned None (should return empty bitmap)!");
    }

    // 5. 测试 IndexReader
    println!("\nStep 3: Testing IndexReader interface...");
    let index_readers = segment.get_index_readers();
    println!(
        "  Available index readers: {:?}",
        index_readers.keys().collect::<Vec<_>>()
    );

    if let Some(id_reader) = index_readers.get("id") {
        println!("\n  Testing id IndexReader:");
        println!("    Field name: {}", id_reader.name());
        println!("    Field type: {:?}", id_reader.field_type());

        if let Some(bitmap) = id_reader.query(&ScalarValue::Int64(Some(5))) {
            println!("    Query id=5: Found {} docs", bitmap.len());
        } else {
            println!("    Query id=5: Returned None!");
        }
    } else {
        println!("  ✗ No index reader for 'id' field!");
    }

    if let Some(name_reader) = index_readers.get("name") {
        println!("\n  Testing name IndexReader:");
        println!("    Field name: {}", name_reader.name());
        println!("    Field type: {:?}", name_reader.field_type());

        if let Some(bitmap) = name_reader.query(&ScalarValue::Utf8(Some("Bob".to_string()))) {
            println!("    Query name='Bob': Found {} docs", bitmap.len());
        } else {
            println!("    Query name='Bob': Returned None!");
        }
    } else {
        println!("  ✗ No index reader for 'name' field!");
    }

    // 6. 测试 RowDataStore
    println!("\nStep 4: Testing RowDataStore...");
    let row_data = segment.get_row_data();

    for doc_id in 0..5 {
        if let Some((batch_start, batch)) = row_data.floor(&doc_id) {
            let row_idx = (doc_id - batch_start) as usize;
            if row_idx < batch.num_rows() {
                println!(
                    "  Doc {}: Found in batch starting at {}, row index {}",
                    doc_id, batch_start, row_idx
                );
            }
        } else {
            println!("  Doc {}: Not found!", doc_id);
        }
    }

    println!("\n=== Test Completed ===");
    Ok(())
}
