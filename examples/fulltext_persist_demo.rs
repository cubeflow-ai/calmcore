//! Demonstration of hybrid inverted index
//!
//! This demo shows the advantage of using Vec<u32> for writes vs RoaringBitmap
//! Key insight: Vec append is O(1), RoaringBitmap insert is O(log n)
//!
//! Design principle from user:
//! - Memory mode: Use Vec<u32> for fast append-only writes
//! - Disk mode: Convert to RoaringBitmap for space-efficient storage
//! - Hybrid approach: Best of both worlds

use std::sync::Arc;
use std::time::Instant;

use calm::segment::field_store::text::{InvertedIndex, PostingEntry, SimpleAnalyzer};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Hybrid Inverted Index Demo\n");
    println!("Demonstrating: Vec<u32> (memory) vs RoaringBitmap (disk)\n");

    let analyzer = Arc::new(SimpleAnalyzer::new());

    // ============================================================
    // Phase 1: Create memory-based index
    // ============================================================
    println!("📝 Phase 1: Creating memory-based index (Vec<u32> for fast writes)...\n");

    let index = InvertedIndex::new_memory(128);

    // Index sample documents
    let docs = vec![
        (1, "The quick brown fox jumps over the lazy dog"),
        (2, "The lazy cat sleeps on the warm mat"),
        (3, "A quick brown dog runs through the forest"),
        (4, "The fox and the cat are both quick animals"),
        (5, "Lazy dogs and cats sleep all day long"),
        (6, "The brown fox hunts in the dark forest"),
        (7, "Quick reflexes help the cat catch mice"),
        (8, "The dog barks at the lazy mailman"),
    ];

    println!("Indexing {} documents...", docs.len());
    let index_start = Instant::now();
    
    for (doc_id, text) in &docs {
        let tokens = analyzer.analyzer_index(text);
        for token in tokens {
            index.insert(&token.name, *doc_id)?;
        }
    }
    
    let index_duration = index_start.elapsed();
    println!(
        "✅ Indexed in {:.3}ms ({:.1} docs/sec)\n",
        index_duration.as_secs_f64() * 1000.0,
        docs.len() as f64 / index_duration.as_secs_f64()
    );

    println!("📊 Index statistics:");
    println!("  Total unique terms: {}", index.term_count());
    println!("  Storage mode: Memory (Vec<u32>)\n");

    // ============================================================
    // Phase 2: Query the index
    // ============================================================
    println!("🔍 Phase 2: Querying index...\n");

    // Test 1: Simple term query
    println!("Test 1: Term query 'quick'");
    if let Some(posting) = index.get("quick") {
        println!("  Document frequency: {}", posting.len());
        println!("  Documents: {:?}", posting.iter().collect::<Vec<_>>());
    }
    println!();

    // Test 2: Boolean AND (intersection)
    println!("Test 2: Boolean AND query 'lazy' AND 'dog'");
    if let (Some(lazy_bitmap), Some(dog_bitmap)) =
        (index.get_bitmap("lazy"), index.get_bitmap("dog"))
    {
        let intersection = &lazy_bitmap & &dog_bitmap;
        println!("  Found {} documents", intersection.len());
        println!("  Documents: {:?}", intersection.iter().collect::<Vec<_>>());
    }
    println!();

    // Test 3: Boolean OR (union)
    println!("Test 3: Boolean OR query 'cat' OR 'fox'");
    if let (Some(cat_bitmap), Some(fox_bitmap)) =
        (index.get_bitmap("cat"), index.get_bitmap("fox"))
    {
        let union = &cat_bitmap | &fox_bitmap;
        println!("  Found {} documents", union.len());
        println!("  Documents: {:?}", union.iter().collect::<Vec<_>>());
    }
    println!();

    // Test 4: Boolean NOT (difference)
    println!("Test 4: Boolean NOT query 'dog' AND NOT 'lazy'");
    if let (Some(dog_bitmap), Some(lazy_bitmap)) =
        (index.get_bitmap("dog"), index.get_bitmap("lazy"))
    {
        let difference = &dog_bitmap - &lazy_bitmap;
        println!("  Found {} documents", difference.len());
        println!("  Documents: {:?}", difference.iter().collect::<Vec<_>>());
    }
    println!();

    // ============================================================
    // Phase 3: Performance comparison (Vec vs Bitmap)
    // ============================================================
    println!("⚡ Phase 3: Performance comparison...\n");

    let test_term = "test_term";
    let write_count = 10000;

    // Test Vec<u32> performance (memory mode)
    println!("Testing Vec<u32> writes ({} insertions)...", write_count);
    let vec_start = Instant::now();
    for doc_id in 0..write_count {
        index.insert(test_term, doc_id)?;
    }
    let vec_duration = vec_start.elapsed();
    let vec_throughput = write_count as f64 / vec_duration.as_secs_f64();

    println!(
        "  Vec<u32>: {:.3}ms ({:.0} writes/sec)",
        vec_duration.as_secs_f64() * 1000.0,
        vec_throughput
    );

    // Test RoaringBitmap performance (simulate disk mode)
    println!("\nTesting RoaringBitmap writes ({} insertions)...", write_count);
    let mut bitmap = roaring::RoaringBitmap::new();
    let bitmap_start = Instant::now();
    for doc_id in 0..write_count {
        bitmap.insert(doc_id);
    }
    let bitmap_duration = bitmap_start.elapsed();
    let bitmap_throughput = write_count as f64 / bitmap_duration.as_secs_f64();

    println!(
        "  RoaringBitmap: {:.3}ms ({:.0} writes/sec)",
        bitmap_duration.as_secs_f64() * 1000.0,
        bitmap_throughput
    );

    println!("\n📈 Performance comparison:");
    println!(
        "  Vec<u32> is {:.1}x faster for writes",
        vec_throughput / bitmap_throughput
    );
    println!(
        "  Reason: Vec append is O(1), Bitmap insert is O(log n)\n"
    );

    // ============================================================
    // Phase 4: Space efficiency (compression)
    // ============================================================
    println!("💾 Phase 4: Space efficiency...\n");

    if let Some(posting) = index.get(test_term) {
        // Estimate Vec<u32> size
        let vec_size = posting.len() * 4; // 4 bytes per u32

        // Get RoaringBitmap size
        let bitmap = posting.to_bitmap();
        let bitmap_size = bitmap.serialized_size();

        println!("Storage comparison for {} document IDs:", posting.len());
        println!("  Vec<u32>: {} bytes", vec_size);
        println!("  RoaringBitmap: {} bytes", bitmap_size);
        println!(
            "  Compression ratio: {:.1}x\n",
            vec_size as f64 / bitmap_size as f64
        );
    }

    // ============================================================
    // Phase 5: Demonstrate optimization
    // ============================================================
    println!("🔧 Phase 5: Demonstrating hybrid optimization...\n");

    // Create a posting entry in memory mode
    let mut entry = PostingEntry::new_memory();
    for doc_id in &[1, 5, 3, 1, 5, 10, 3] {
        // Duplicates and unsorted
        entry.insert(*doc_id);
    }

    println!("Memory mode (Vec<u32>):");
    println!("  Length: {} (with duplicates)", entry.len());
    println!("  Storage: Vec<u32>");

    // Optimize to disk format
    entry.optimize();

    println!("\nDisk mode (RoaringBitmap):");
    println!("  Length: {} (deduplicated)", entry.len());
    println!("  Storage: RoaringBitmap (compressed)");
    println!("  Sorted: {:?}\n", entry.iter().collect::<Vec<_>>());

    // ============================================================
    // Summary
    // ============================================================
    println!("📋 Summary: Hybrid Index Advantages\n");
    println!("✅ Fast writes: Vec<u32> for append-only operations");
    println!("✅ Space efficient: RoaringBitmap for disk storage");
    println!("✅ Query performance: Fast set operations with bitmap");
    println!("✅ Flexibility: Convert between modes as needed\n");

    println!("💡 Design principle:");
    println!("   Use Vec<u32> in memory, convert to RoaringBitmap for persistence");
    println!("   This gives you the best of both worlds!\n");

    println!("✅ Demo completed successfully!");

    Ok(())
}
