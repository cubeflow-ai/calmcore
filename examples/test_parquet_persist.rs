//! Test Parquet persistence for full-text index

use calm::schema::field::FieldOption;
use calm::segment::field_store::text::FullTextField;
use calm::segment::field_store::IndexWriter;
use datafusion::arrow::array::StringArray;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use std::sync::Arc;
use tempfile::TempDir;

fn main() {
    println!("🧪 Testing Parquet Persistence for FullTextField\n");

    // Create temporary directory
    let temp_dir = TempDir::new().unwrap();
    let persist_path = temp_dir.path().join("fulltext_index");
    println!("📁 Using temp directory: {}", persist_path.display());

    // Create FullTextField
    let field = FieldOption::Keyword {
        name: "content".to_string(),
        index: true,
        is_array: false,
        persist_option: None,
        case_sensitive: true,
    };
    let index = FullTextField::new(&field);

    // Index some documents
    let texts = vec![
        "Rust programming language is fast and safe",
        "Python programming is easy to learn",
        "Rust and Python are both popular",
        "Fast programming with Rust",
        "Learn Rust programming today",
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

    // Test queries before persist
    let rust_docs = index.term_query("rust").unwrap();
    println!("🔍 Before persist: 'rust' matches {} docs", rust_docs.len());

    let phrase_docs = index.phrase_query(&["rust", "programming"], 0).unwrap();
    println!(
        "🔍 Before persist: 'rust programming' matches {} docs",
        phrase_docs.len()
    );

    // Get statistics
    let stats = index.get_field_stats();
    println!(
        "📊 Field stats: {} docs, avg_length={:.2}",
        stats.num_docs, stats.avg_field_length
    );

    // Persist to disk
    println!("\n💾 Persisting to: {}", persist_path.display());
    index.persist(persist_path.to_str().unwrap()).unwrap();

    // Check file sizes
    let posting_file = persist_path.join("posting_lists.parquet");
    let stats_file = persist_path.join("field_stats.json");

    if posting_file.exists() {
        let size = std::fs::metadata(&posting_file).unwrap().len();
        println!("📦 posting_lists.parquet: {} bytes", size);
    }

    if stats_file.exists() {
        let size = std::fs::metadata(&stats_file).unwrap().len();
        println!("📦 field_stats.json: {} bytes", size);
    }

    // Load from disk
    println!("\n📂 Loading from disk...");
    let loaded_index = FullTextField::from_disk(&field, persist_path.to_str().unwrap()).unwrap();

    // Test queries after load
    let rust_docs_loaded = loaded_index.term_query("rust").unwrap();
    println!(
        "🔍 After load: 'rust' matches {} docs",
        rust_docs_loaded.len()
    );
    assert_eq!(rust_docs.len(), rust_docs_loaded.len());

    let phrase_docs_loaded = loaded_index
        .phrase_query(&["rust", "programming"], 0)
        .unwrap();
    println!(
        "🔍 After load: 'rust programming' matches {} docs",
        phrase_docs_loaded.len()
    );
    assert_eq!(phrase_docs.len(), phrase_docs_loaded.len());

    // Verify statistics
    let stats_loaded = loaded_index.get_field_stats();
    println!(
        "📊 Loaded stats: {} docs, avg_length={:.2}",
        stats_loaded.num_docs, stats_loaded.avg_field_length
    );
    assert_eq!(stats.num_docs, stats_loaded.num_docs);
    assert_eq!(stats.avg_field_length, stats_loaded.avg_field_length);

    // Test term statistics
    let term_stats = loaded_index.get_term_stats("rust").unwrap();
    println!(
        "📊 Term 'rust': doc_freq={}, total_freq={}",
        term_stats.doc_freq, term_stats.total_freq
    );

    // Test posting list details
    let postings = loaded_index.get_postings("rust").unwrap();
    println!("📊 Posting list for 'rust': {} entries", postings.len());
    for (i, entry) in postings.iter().take(3).enumerate() {
        println!(
            "  Entry {}: doc_id={}, term_freq={}, positions={:?}",
            i, entry.doc_id, entry.term_freq, entry.positions
        );
    }

    println!("\n✅ All tests passed!");
    println!("🎉 Parquet persistence working correctly!");
}
