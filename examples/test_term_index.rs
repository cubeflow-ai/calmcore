//! Test term index for O(1) lookup performance

use calm::schema::field::FieldOption;
use calm::segment::field_store::text::{
    build_term_index, load_term_index, save_term_index, search_term, search_with_term_index,
    FullTextField,
};
use calm::segment::field_store::IndexWriter;
use datafusion::arrow::array::StringArray;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use std::sync::Arc;
use std::time::Instant;
use tempfile::TempDir;

fn main() {
    println!("🎯 Testing Term Index for O(1) Lookups\n");

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

    // Index documents
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
        "Ruby dynamic programming language",
        "PHP web development language",
        "Swift for iOS development",
        "Kotlin for Android development",
        "Scala functional programming on JVM",
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
    println!("💾 Persisted to Parquet");

    let parquet_path = persist_path.join("posting_lists.parquet");
    let parquet_path_str = parquet_path.to_str().unwrap();

    // Build term index
    println!("\n🔨 Building Term Index...");
    let start = Instant::now();
    let term_index = build_term_index(parquet_path_str).unwrap();
    let build_duration = start.elapsed();
    println!("✅ Built index with {} terms in {:?}", term_index.len(), build_duration);

    // Save term index
    let index_path = persist_path.join("term_index.json");
    let index_path_str = index_path.to_str().unwrap();
    save_term_index(&term_index, index_path_str).unwrap();
    println!("💾 Saved term index to {}", index_path_str);

    // Show some index entries
    println!("\n📋 Sample Index Entries:");
    for entry in term_index.iter().take(5) {
        println!(
            "  • '{}' → Row Group {}, Offset {}",
            entry.term, entry.row_group_id, entry.row_offset
        );
    }

    // Load term index
    println!("\n📂 Loading Term Index...");
    let start = Instant::now();
    let loaded_index = load_term_index(index_path_str).unwrap();
    let load_duration = start.elapsed();
    println!("✅ Loaded {} entries in {:?}", loaded_index.len(), load_duration);

    // Test 1: Compare search performance
    println!("\n⚡ Performance Comparison");
    println!("─────────────────────────");

    let test_terms = vec!["rust", "programming", "language", "development"];

    // Without term index (Row Group statistics)
    let start = Instant::now();
    for term in &test_terms {
        let _ = search_term(parquet_path_str, term).unwrap();
    }
    let without_index_duration = start.elapsed();
    println!("  Without index (Row Group stats): {:?}", without_index_duration);

    // With term index
    let start = Instant::now();
    for term in &test_terms {
        let _ = search_with_term_index(parquet_path_str, term, &loaded_index).unwrap();
    }
    let with_index_duration = start.elapsed();
    println!("  With term index (O(1) lookup):   {:?}", with_index_duration);

    if without_index_duration > with_index_duration {
        println!(
            "  🎉 Speedup: {:.2}x faster with term index",
            without_index_duration.as_nanos() as f64 / with_index_duration.as_nanos() as f64
        );
    } else {
        println!("  ℹ️  Note: For small datasets, overhead may dominate");
    }

    // Test 2: Verify correctness
    println!("\n✅ Correctness Test");
    println!("───────────────────");
    for term in &test_terms {
        let result1 = search_term(parquet_path_str, term).unwrap();
        let result2 = search_with_term_index(parquet_path_str, term, &loaded_index).unwrap();

        let match_result = match (result1, result2) {
            (Some(r1), Some(r2)) => {
                if r1.term == r2.term && r1.doc_ids == r2.doc_ids {
                    format!("✓ Both found {} docs", r1.doc_ids.len())
                } else {
                    "✗ Results mismatch!".to_string()
                }
            }
            (None, None) => "✓ Both not found".to_string(),
            _ => "✗ One found, one not found!".to_string(),
        };

        println!("  '{}': {}", term, match_result);
    }

    // Test 3: Memory usage comparison
    println!("\n💾 Memory Usage");
    println!("───────────────");
    let parquet_size = std::fs::metadata(parquet_path_str)
        .map(|m| m.len())
        .unwrap_or(0);
    let index_size = std::fs::metadata(index_path_str)
        .map(|m| m.len())
        .unwrap_or(0);

    println!("  Parquet file:  {} bytes", parquet_size);
    println!("  Term index:    {} bytes", index_size);
    println!(
        "  Index overhead: {:.1}% of Parquet size",
        (index_size as f64 / parquet_size as f64) * 100.0
    );

    println!("\n🎉 Summary:");
    println!("  • Term index enables O(1) lookup for point queries");
    println!("  • Best for: Many single-term queries");
    println!("  • Trade-off: ~{:.0}% extra storage for index", (index_size as f64 / parquet_size as f64) * 100.0);
    println!("  • Alternative: Row Group statistics (no extra storage)");
}
