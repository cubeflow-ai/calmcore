//! FullTextField Advanced Demo
//!
//! Demonstrates:
//! - Boolean queries (AND/OR/NOT)
//! - Term statistics (for BM25)
//! - Field statistics
//! - Posting lists
//! 
//! Architecture inspired by Tantivy/Lucene

use calm::schema::field::FieldOption;
use calm::segment::field_store::text::FullTextField;
use calm::segment::field_store::IndexWriter;
use datafusion::arrow::array::{RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

fn main() {
    println!("\n🚀 FullTextField Advanced Demo");
    println!("================================\n");

    // Create field
    let field_opt = FieldOption::Keyword {
        name: "content".to_string(),
        index: true,
        is_array: false,
        persist_option: None,
        case_sensitive: true,
    };
    
    let index = FullTextField::new(&field_opt);

    // Index documents
    let schema = Schema::new(vec![Field::new("content", DataType::Utf8, true)]);
    
    let documents = vec![
        "Rust is a systems programming language",
        "Python is great for machine learning",
        "Rust and Python are both popular",
        "Systems programming with Rust is safe",
        "Machine learning with Python and TensorFlow",
        "Rust programming language is fast and safe",
    ];
    
    let content = StringArray::from(documents.clone());
    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![Arc::new(content)],
    ).unwrap();

    index.write(&batch, 0).unwrap();
    println!("✅ Indexed {} documents\n", documents.len());

    // 1. Boolean Queries
    println!("🔍 Boolean Queries:");
    println!("-------------------");
    
    // AND: documents with both "rust" and "programming"
    println!("\n1. MUST: rust AND programming");
    if let Some(bitmap) = index.boolean_query(&["rust", "programming"], &[], &[]) {
        println!("   Found {} docs:", bitmap.len());
        for doc_id in bitmap.iter() {
            println!("   [{}] {}", doc_id, documents[doc_id as usize]);
        }
    }

    // OR: documents with "machine" or "systems"
    println!("\n2. SHOULD: machine OR systems");
    if let Some(bitmap) = index.boolean_query(&[], &["machine", "systems"], &[]) {
        println!("   Found {} docs:", bitmap.len());
        for doc_id in bitmap.iter() {
            println!("   [{}] {}", doc_id, documents[doc_id as usize]);
        }
    }

    // NOT: "programming" but NOT "python"
    println!("\n3. MUST + MUST_NOT: programming AND NOT python");
    if let Some(bitmap) = index.boolean_query(&["programming"], &[], &["python"]) {
        println!("   Found {} docs:", bitmap.len());
        for doc_id in bitmap.iter() {
            println!("   [{}] {}", doc_id, documents[doc_id as usize]);
        }
    }

    // Complex: (rust OR python) AND programming
    println!("\n4. Complex: (rust OR python) AND programming");
    if let Some(should_bitmap) = index.boolean_query(&[], &["rust", "python"], &[]) {
        if let Some(must_bitmap) = index.term_query("programming") {
            let result = should_bitmap & must_bitmap;
            println!("   Found {} docs:", result.len());
            for doc_id in result.iter() {
                println!("   [{}] {}", doc_id, documents[doc_id as usize]);
            }
        }
    }

    // 2. Term Statistics
    println!("\n\n📊 Term Statistics (for BM25):");
    println!("-------------------------------");
    
    let terms = ["rust", "python", "programming", "language"];
    for &term in &terms {
        if let Some(stats) = index.get_term_stats(term) {
            println!("  '{}': doc_freq={}, total_freq={}", 
                term, stats.doc_freq, stats.total_freq);
        } else {
            println!("  '{}': not found", term);
        }
    }

    // 3. Field Statistics
    println!("\n📈 Field Statistics:");
    println!("--------------------");
    let field_stats = index.get_field_stats();
    println!("  Total documents: {}", field_stats.num_docs);
    println!("  Sum of doc frequencies: {}", field_stats.sum_doc_freq);
    println!("  Sum of total term frequencies: {}", field_stats.sum_total_term_freq);
    println!("  Average field length: {:.2}", field_stats.avg_field_length);

    // 4. Posting Lists
    println!("\n📋 Posting Lists (with positions):");
    println!("------------------------------------");
    
    if let Some(postings) = index.get_postings("rust") {
        println!("  Term 'rust': {} postings", postings.len());
        for (i, posting) in postings.iter().take(3).enumerate() {
            println!("    [{}] doc={}, tf={}, positions={:?}", 
                i, posting.doc_id, posting.term_freq, posting.positions);
            println!("        Text: {}", documents[posting.doc_id as usize]);
        }
    }

    // 5. Phrase with Slop
    println!("\n🔤 Phrase Queries with Slop:");
    println!("----------------------------");
    
    let phrases = [
        ("rust programming", 0),
        ("rust programming", 1),
        ("rust safe", 3),
    ];
    
    for (phrase, slop) in phrases {
        let terms: Vec<&str> = phrase.split_whitespace().collect();
        if let Some(bitmap) = index.phrase_query(&terms, slop) {
            println!("  '{}' (slop={}): {} docs", phrase, slop, bitmap.len());
            for doc_id in bitmap.iter().take(2) {
                println!("    [{}] {}", doc_id, documents[doc_id as usize]);
            }
        } else {
            println!("  '{}' (slop={}): no matches", phrase, slop);
        }
    }

    println!("\n✨ Demo complete!");
    println!("\n🎯 Architecture Notes:");
    println!("  • InvertedIndex<String>: Uses existing codebase pattern");
    println!("  • Position tracking: Separate BTree for phrase queries");
    println!("  • Statistics: Inspired by Lucene's TermStatistics & CollectionStatistics");
    println!("  • Posting lists: Similar to Tantivy's design");
    println!("  • No IndexReader implementation: Full-text has its own query interface");
}
