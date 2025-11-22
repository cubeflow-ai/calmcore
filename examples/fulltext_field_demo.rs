//! FullTextField demo - following KeywordField pattern
//!
//! Architecture:
//! - InvertedIndex<String>: term -> Vec<doc_id> (memory) or RoaringBitmap (disk)
//! - Position tracking: BTree for phrase queries
//! - TreeWriter persistence: Same as KeywordField
//! - Statistics: Will be stored in Parquet (TODO)

use calm::schema::field::{FieldOption, FieldType};
use calm::segment::field_store::text::FullTextField;
use calm::segment::field_store::IndexWriter;
use datafusion::arrow::array::{RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

fn main() {
    println!("\n🎯 FullTextField Demo - New Architecture");
    println!("========================================\n");

    // 1. Create field (use Keyword as base for full-text)
    let field_opt = FieldOption::Keyword {
        name: "content".to_string(),
        index: true,
        is_array: false,
        persist_option: None,
        case_sensitive: true,
    };
    
    let index = FullTextField::new(&field_opt);
    println!("✅ Created FullTextField");

    // 2. Prepare test data
    let schema = Schema::new(vec![Field::new("content", DataType::Utf8, true)]);
    
    let documents = vec![
        "Rust programming language is fast and safe",
        "Python programming is easy to learn",
        "Rust and Python are both popular",
        "Fast programming with Rust",
        "Learn Rust programming today",
    ];
    
    let content = StringArray::from(documents.clone());
    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![Arc::new(content)],
    ).unwrap();

    // 3. Index documents (using IndexWriter trait)
    index.write(&batch, 0).unwrap();
    println!("✅ Indexed {} documents\n", documents.len());

    // 4. Term queries
    println!("📝 Term Queries:");
    println!("----------------");
    
    let queries = vec!["rust", "python", "programming", "fast"];
    for query in queries {
        if let Some(bitmap) = index.term_query(query) {
            let doc_ids: Vec<u32> = bitmap.iter().collect();
            println!("  '{}': {} docs → {:?}", query, bitmap.len(), doc_ids);
            
            for doc_id in doc_ids.iter().take(2) {
                if let Some(doc) = documents.get(*doc_id as usize) {
                    println!("    - [{}] {}", doc_id, doc);
                }
            }
        } else {
            println!("  '{}': No matches", query);
        }
    }

    // 5. Phrase queries
    println!("\n📚 Phrase Queries (exact match):");
    println!("---------------------------------");
    
    let phrases = vec![
        vec!["rust", "programming"],
        vec!["programming", "language"],
        vec!["fast", "safe"],
    ];
    
    for phrase in &phrases {
        if let Some(bitmap) = index.phrase_query(phrase, 0) {
            let doc_ids: Vec<u32> = bitmap.iter().collect();
            println!("  '{}': {} docs → {:?}", phrase.join(" "), bitmap.len(), doc_ids);
            
            for doc_id in doc_ids.iter() {
                if let Some(doc) = documents.get(*doc_id as usize) {
                    println!("    - [{}] {}", doc_id, doc);
                }
            }
        } else {
            println!("  '{}': No matches", phrase.join(" "));
        }
    }

    // 6. Phrase queries with slop
    println!("\n📚 Phrase Queries (with slop=1):");
    println!("---------------------------------");
    
    if let Some(bitmap) = index.phrase_query(&["rust", "safe"], 1) {
        let doc_ids: Vec<u32> = bitmap.iter().collect();
        println!("  'rust ... safe' (slop=1): {} docs → {:?}", bitmap.len(), doc_ids);
        
        for doc_id in doc_ids.iter() {
            if let Some(doc) = documents.get(*doc_id as usize) {
                println!("    - [{}] {}", doc_id, doc);
            }
        }
    }

    // 7. Persistence test
    println!("\n💾 Persistence Test:");
    println!("--------------------");
    
    use tempfile::TempDir;
    let temp_dir = TempDir::new().unwrap();
    let index_path = temp_dir.path().join("fulltext_index");
    
    println!("  Persisting to: {:?}", index_path);
    let persisted = index.persist(index_path.to_str().unwrap()).unwrap();
    println!("  ✅ Persisted (Vec<u32> → RoaringBitmap)");
    
    // Query from disk
    if let Some(bitmap) = persisted.term_query("rust") {
        println!("  ✅ Loaded from disk: {} docs match 'rust'", bitmap.len());
    }

    println!("\n🎉 Demo complete! Architecture follows KeywordField pattern.");
    println!("\n📊 Key Features:");
    println!("  ✓ InvertedIndex<String> (Vec in memory, Bitmap on disk)");
    println!("  ✓ Position tracking for phrase queries");
    println!("  ✓ TreeWriter persistence");
    println!("  ✓ BM25 scorer integration");
    println!("  ✓ IndexWriter/IndexReader traits");
    println!("\n📝 TODO:");
    println!("  - Store metadata in Parquet (user suggestion)");
    println!("  - Persist position index");
    println!("  - Add boolean query support");
}
