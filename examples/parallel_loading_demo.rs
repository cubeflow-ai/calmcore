//! Parallel Row Group loading benchmark and demo
//!
//! Demonstrates the performance improvement of parallel Row Group loading
//! using rayon for building term index and batch queries.

use calm::schema::field::FieldOption;
use calm::segment::field_store::text::{build_term_index, search_terms_batch, FullTextField};
use calm::segment::field_store::IndexWriter;
use datafusion::arrow::array::StringArray;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use std::sync::Arc;
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Parallel Row Group Loading Demo\n");

    // 1. Create test data with multiple documents
    println!("📝 Step 1: Creating test dataset...");
    let docs = generate_test_documents(50000); // 50K documents for larger file
    println!("✅ Created {} documents\n", docs.len());

    // 2. Build full-text index
    println!("🔨 Step 2: Building full-text index...");
    let field = FieldOption::Keyword {
        name: "content".to_string(),
        index: true,
        is_array: false,
        persist_option: None,
        case_sensitive: false,
    };

    let index = FullTextField::new(&field);
    let schema = Arc::new(Schema::new(vec![Field::new(
        "content",
        DataType::Utf8,
        false,
    )]));

    let content_array = StringArray::from(docs.clone());
    let batch = RecordBatch::try_new(schema, vec![Arc::new(content_array)])?;

    let start = Instant::now();
    index.write(&batch, 0)?;
    let write_time = start.elapsed();
    println!("✅ Index built in {:?}", write_time);

    // Persist to Parquet
    let base_path = "data/parallel_loading_demo/postings.parquet";
    println!("💾 Persisting to {}...", base_path);
    let start = Instant::now();
    index.persist(base_path)?;
    let persist_time = start.elapsed();
    println!("✅ Persisted in {:?}\n", persist_time);

    // 3. Build term index with parallel loading
    println!("📊 Step 3: Building term index (parallel Row Group loading)...");
    let parquet_path = format!("{}/posting_lists.parquet", base_path);

    let start = Instant::now();
    let term_index = build_term_index(&parquet_path)?;
    let build_time = start.elapsed();

    println!("✅ Term index built in {:?}", build_time);
    println!("   Terms indexed: {}", term_index.len());
    println!(
        "   Speed: {:.0} terms/sec\n",
        term_index.len() as f64 / build_time.as_secs_f64()
    );

    // 4. Verify index with sample queries
    println!("🔍 Step 4: Testing sample queries...\n");

    let test_terms: Vec<&str> = vec![
        "rust",
        "async",
        "await",
        "programming",
        "systems",
        "performance",
        "memory",
        "safety",
        "concurrency",
        "parallelism",
        "tokio",
        "rayon",
        "futures",
        "channels",
        "threads",
        "database",
        "query",
        "index",
        "search",
        "fulltext",
        "optimization",
        "algorithm",
        "data",
        "structure",
        "vector",
        "hash",
        "map",
        "set",
        "tree",
        "graph",
        "network",
        "protocol",
        "http",
        "tcp",
        "udp",
        "file",
        "io",
        "stream",
        "buffer",
        "cache",
        "lock",
        "mutex",
        "semaphore",
        "atomic",
        "barrier",
        "heap",
        "stack",
        "allocator",
        "garbage",
        "collector",
        "compile",
        "runtime",
        "benchmark",
        "profiling",
        "tracing",
        "error",
        "result",
        "option",
        "panic",
        "unwrap",
        "trait",
        "generic",
        "lifetime",
        "borrow",
        "ownership",
        "macro",
        "derive",
        "attribute",
        "annotation",
        "reflection",
        "module",
        "crate",
        "package",
        "cargo",
        "rustup",
        "test",
        "debug",
        "release",
        "build",
        "deploy",
        "docker",
        "kubernetes",
        "cloud",
        "server",
        "client",
        "json",
        "xml",
        "yaml",
        "toml",
        "config",
        "log",
        "metric",
        "monitor",
        "alert",
        "dashboard",
        "user",
        "authentication",
        "authorization",
        "security",
        "encryption",
    ];

    // Batch search
    println!("  🔍 Batch search ({} terms):", test_terms.len());
    let start = Instant::now();
    let results = search_terms_batch(&parquet_path, &test_terms)?;
    let search_time = start.elapsed();
    let found = results.iter().filter(|r| r.is_some()).count();
    println!("     Time: {:?}", search_time);
    println!("     Found: {}/{} terms", found, test_terms.len());
    println!(
        "     Speed: {:.0} terms/sec",
        test_terms.len() as f64 / search_time.as_secs_f64()
    );

    // 5. Summary
    println!("\n✅ Demo completed!\n");
    println!("📊 Performance Summary:");
    println!("  • Document count: {}", docs.len());
    println!("  • Index build: {:?}", write_time);
    println!("  • Persist time: {:?}", persist_time);
    println!(
        "  • Term index build (parallel): {:?} ({} terms)",
        build_time,
        term_index.len()
    );
    println!(
        "  • Index build speed: {:.0} terms/sec",
        term_index.len() as f64 / build_time.as_secs_f64()
    );
    println!("  • Batch search: {:?}", search_time);
    println!("\n💡 Key Insights:");
    println!("  • ✅ Parallel Row Group loading in build_term_index()");
    println!("  • Each thread processes independent Row Groups");
    println!("  • Speedup scales with number of Row Groups");
    println!("  • Best for: index building, not individual queries");
    println!(
        "  • Index build speed: ~{}M terms/sec",
        (term_index.len() as f64 / build_time.as_secs_f64() / 1_000_000.0) as i32
    );

    Ok(())
}

/// Generate test documents with various terms
fn generate_test_documents(count: usize) -> Vec<String> {
    let keywords = vec![
        "rust",
        "programming",
        "async",
        "await",
        "systems",
        "memory",
        "safety",
        "performance",
        "concurrency",
        "parallelism",
        "tokio",
        "rayon",
        "futures",
        "channels",
        "threads",
        "database",
        "query",
        "index",
        "search",
        "fulltext",
        "optimization",
        "algorithm",
        "data",
        "structure",
        "vector",
        "hash",
        "map",
        "set",
        "tree",
        "graph",
    ];

    (0..count)
        .map(|i| {
            let mut doc = format!("Document {}: ", i);
            // Add 5-10 random keywords
            let num_keywords = 5 + (i % 6);
            for j in 0..num_keywords {
                let keyword = keywords[(i + j * 7) % keywords.len()];
                doc.push_str(keyword);
                doc.push(' ');
            }
            doc
        })
        .collect()
}
