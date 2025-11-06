use calm::partition::Partition;
use calm::schema::field::FieldOption;
use calm::schema::Schema;
use datafusion::arrow::array::{Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use std::sync::Arc;

fn main() -> CoreResult<()> {
    println!("=== Test Index Persistence and Loading ===\n");

    // Step 1: Create schema
    let schema = Arc::new(Schema {
        name: "test_schema".to_string(),
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
        persist_policy: calm::schema::PersistPolicy::default(),
        primary_key: None,
        store_source: false,
    });

    // Step 2: Create in-memory segment and insert data
    println!("Step 1: Creating in-memory segment and inserting data...");
    let segment = Segment::new(0, schema.clone());

    let id_array = Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]));
    let name_array = Arc::new(StringArray::from(vec![
        "Alice", "Bob", "Carol", "Dave", "Eve", "Frank", "Grace", "Henry", "Ivy", "Jack",
    ]));

    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let batch = RecordBatch::try_new(arrow_schema, vec![id_array, name_array]).unwrap();
    segment.upsert(&batch)?;

    println!("  ✓ Inserted 10 records");
    println!("  Current doc_count: {}\n", segment.doc_count());

    // Step 3: Test query on in-memory segment
    println!("Step 2: Testing query on in-memory segment...");
    let query_result = segment.query_field("id", &5i64);
    match query_result {
        Some(bitmap) => {
            println!("  ✓ Query id=5 found {} docs", bitmap.len());
            println!("  Doc IDs: {:?}\n", bitmap.iter().collect::<Vec<_>>());
        }
        None => {
            println!("  ✗ Query id=5 returned None!\n");
        }
    }

    // Step 4: Persist segment to disk
    println!("Step 3: Persisting segment to disk...");
    let base_dir = "/tmp/calm_test_persist";
    std::fs::create_dir_all(base_dir).ok();

    // Clear existing files
    let segment_path = format!("{}/segment-0-9", base_dir);
    if std::path::Path::new(&segment_path).exists() {
        std::fs::remove_dir_all(&segment_path).ok();
    }

    let frozen = segment.freeze(base_dir)?;
    println!("  ✓ Segment frozen to: {}\n", segment_path);

    // Step 5: Load segment from disk
    println!("Step 4: Loading segment from disk...");
    let loaded_segment = Segment::load_frozen(base_dir, 0, 9, schema.clone())?;
    println!("  ✓ Segment loaded");
    println!("  Doc count: {}\n", loaded_segment.doc_count());

    // Step 6: Test query on loaded segment
    println!("Step 5: Testing query on loaded segment...");
    let query_result2 = loaded_segment.query_field("id", &5i64);
    match query_result2 {
        Some(bitmap) => {
            println!("  ✓ Query id=5 found {} docs", bitmap.len());
            println!("  Doc IDs: {:?}", bitmap.iter().collect::<Vec<_>>());
        }
        None => {
            println!("  ✗ Query id=5 returned None!");
            println!("  THIS IS THE BUG!");
        }
    }

    // Step 7: Test using IndexReader interface
    println!("\nStep 6: Testing IndexReader interface on loaded segment...");
    let index_readers = loaded_segment.get_index_readers();
    println!(
        "  Available index readers: {:?}",
        index_readers.keys().collect::<Vec<_>>()
    );

    if let Some(id_reader) = index_readers.get("id") {
        use datafusion::scalar::ScalarValue;
        let result = id_reader.query(&ScalarValue::Int64(Some(5)));
        match result {
            Some(bitmap) => {
                println!("  ✓ IndexReader.query(id=5) found {} docs", bitmap.len());
                println!("  Doc IDs: {:?}", bitmap.iter().collect::<Vec<_>>());
            }
            None => {
                println!("  ✗ IndexReader.query(id=5) returned None!");
                println!("  THIS IS THE ROOT CAUSE!");
            }
        }
    }

    // Step 8: Check if index files exist
    println!("\nStep 7: Checking index files...");
    let id_index_path = format!("{}/field-id", segment_path);
    if std::path::Path::new(&id_index_path).exists() {
        println!("  ✓ Index file exists: {}", id_index_path);

        // Check file size
        if let Ok(metadata) = std::fs::metadata(&id_index_path) {
            println!("  File size: {} bytes", metadata.len());

            if metadata.len() == 0 {
                println!("  ✗ WARNING: Index file is EMPTY!");
            }
        }
    } else {
        println!("  ✗ Index file NOT found: {}", id_index_path);
    }

    println!("\n=== Test Completed ===");
    Ok(())
}
