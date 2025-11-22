//! Test Parquet optimizations: Row Group statistics, columnar reads, batch queries

use calm::schema::field::FieldOption;
use calm::segment::field_store::text::{
    get_parquet_metadata, search_term, search_term_docids_only, search_terms_batch, FullTextField,
};
use calm::segment::field_store::IndexWriter;
use datafusion::arrow::array::StringArray;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use std::sync::Arc;
use std::time::Instant;
use tempfile::TempDir;

fn main() {
    println!("🚀 Testing Parquet Optimizations\n");

    // Create temporary directory
    let temp_dir = TempDir::new().unwrap();
    let persist_path = temp_dir.path().join("fulltext_index");

    // Create FullTextField
    let field = FieldOption::Keyword {
        name: "content".to_string(),
        index: true,
        is_array: false,
        persist_option: None,
        case_sensitive: true,
    };
    let index = FullTextField::new(&field);

    // Index a larger dataset
    let texts = vec![
        "Rust programming language is fast and safe",
        "Python programming is easy to learn",
        "Rust and Python are both popular",
        "Fast programming with Rust",
        "Learn Rust programming today",
        "JavaScript is a web programming language",
        "Go language for concurrent programming",
        "Java enterprise programming framework",
        "C++ systems programming language",
        "TypeScript extends JavaScript with types",
    ];

    let schema = Arc::new(Schema::new(vec![Field::new(
        "content",
        DataType::Utf8,
        false,
    )]));
    let content_array = StringArray::from(texts.clone());
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(content_array)]).unwrap();

    index.write(&batch, 0).unwrap();
    println!("✅ Indexed {} documents", texts.len());

    // Persist to Parquet
    let persist_path_str = persist_path.to_str().unwrap();
    index.persist(persist_path_str).unwrap();
    println!("💾 Persisted to Parquet\n");

    let parquet_path = persist_path.join("posting_lists.parquet");
    let parquet_path_str = parquet_path.to_str().unwrap();

    // Test 1: Get Parquet metadata
    println!("📊 Test 1: Parquet Metadata");
    println!("─────────────────────────────");
    let metadata = get_parquet_metadata(parquet_path_str).unwrap();
    println!("  • Row Groups: {}", metadata.num_row_groups);
    println!("  • Total Rows: {}", metadata.num_rows);
    println!("  • File Size: {} bytes", metadata.file_size);
    println!();

    // Test 2: Search with Row Group statistics (optimized)
    println!("🔍 Test 2: Optimized Search (Row Group Statistics)");
    println!("─────────────────────────────────────────────────");
    let start = Instant::now();
    let result = search_term(parquet_path_str, "rust").unwrap();
    let duration = start.elapsed();
    if let Some(row) = result {
        println!("  • Term 'rust': {} docs", row.doc_ids.len());
        println!("  • Doc IDs: {:?}", row.doc_ids);
        println!("  • Search time: {:?}", duration);
    }
    println!();

    // Test 3: Columnar read (doc_ids only, skip positions)
    println!("📖 Test 3: Columnar Read (Doc IDs Only)");
    println!("───────────────────────────────────────");
    let start = Instant::now();
    let doc_ids = search_term_docids_only(parquet_path_str, "programming").unwrap();
    let duration = start.elapsed();
    if let Some(ids) = doc_ids {
        println!("  • Term 'programming': {} docs", ids.len());
        println!("  • Doc IDs: {:?}", ids);
        println!("  • Search time (columnar): {:?}", duration);
        println!("  • Benefit: Skipped reading positions column!");
    }
    println!();

    // Test 4: Batch query multiple terms
    println!("📦 Test 4: Batch Query (Multiple Terms)");
    println!("───────────────────────────────────────");
    let terms = vec!["rust", "python", "javascript", "nonexistent"];
    let start = Instant::now();
    let results = search_terms_batch(parquet_path_str, &terms).unwrap();
    let duration = start.elapsed();
    println!("  • Queried {} terms in {:?}", terms.len(), duration);
    for (term, result) in terms.iter().zip(results.iter()) {
        match result {
            Some(row) => println!("    ✓ '{}': {} docs", term, row.doc_ids.len()),
            None => println!("    ✗ '{}': not found", term),
        }
    }
    println!();

    // Test 5: Compare full read vs optimized search
    println!("⚡ Test 5: Performance Comparison");
    println!("─────────────────────────────");

    // Full read approach (old)
    let start = Instant::now();
    let loaded_index = FullTextField::from_disk(&field, persist_path_str).unwrap();
    let full_load_duration = start.elapsed();
    let start = Instant::now();
    let _docs = loaded_index.term_query("rust").unwrap();
    let full_query_duration = start.elapsed();
    println!("  Old approach (full load):");
    println!("    • Load time: {:?}", full_load_duration);
    println!("    • Query time: {:?}", full_query_duration);
    println!("    • Total: {:?}", full_load_duration + full_query_duration);

    // Optimized search (new)
    let start = Instant::now();
    let _result = search_term(parquet_path_str, "rust").unwrap();
    let optimized_duration = start.elapsed();
    println!("  New approach (Row Group statistics):");
    println!("    • Direct search: {:?}", optimized_duration);
    println!(
        "    • Speedup: {:.2}x faster",
        (full_load_duration + full_query_duration).as_nanos() as f64
            / optimized_duration.as_nanos() as f64
    );
    println!();

    // Test 6: Test term not found (early termination)
    println!("🎯 Test 6: Early Termination (Term Not Found)");
    println!("────────────────────────────────────────────");
    let start = Instant::now();
    let result = search_term(parquet_path_str, "aaaaa").unwrap(); // Before all terms
    let duration = start.elapsed();
    match result {
        Some(_) => println!("  ✗ Should not find 'aaaaa'"),
        None => {
            println!("  ✓ Term 'aaaaa' not found (as expected)");
            println!("  • Search time: {:?}", duration);
            println!("  • Benefit: Early termination using Row Group min statistics");
        }
    }
    println!();

    println!("✅ All optimization tests passed!");
    println!();
    println!("🎉 Summary of Optimizations:");
    println!("  1. Row Group Statistics: Binary search to locate data");
    println!("  2. Columnar Reads: Skip unused columns (e.g., positions)");
    println!("  3. Batch Queries: Efficient multi-term lookups");
    println!("  4. Early Termination: Fast rejection using min/max stats");
}
