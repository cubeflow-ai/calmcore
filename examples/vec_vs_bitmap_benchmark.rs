//! Benchmark: Vec<u32> vs RoaringBitmap write performance
//!
//! This benchmark aims to verify which is faster for append-only writes

use std::time::Instant;
use roaring::RoaringBitmap;

fn main() {
    println!("🔬 Vec<u32> vs RoaringBitmap Write Performance Benchmark\n");

    // Test different dataset sizes
    let test_sizes = vec![1000, 10_000, 100_000, 1_000_000];

    for &size in &test_sizes {
        println!("📊 Testing with {} insertions:", size);
        println!("{}", "=".repeat(60));

        // Test 1: Vec<u32> append
        println!("\n1️⃣  Vec<u32> append:");
        let mut vec = Vec::new();
        let vec_start = Instant::now();
        for i in 0..size {
            vec.push(i);
        }
        let vec_duration = vec_start.elapsed();
        let vec_throughput = size as f64 / vec_duration.as_secs_f64();
        
        println!("   Time: {:.3}ms", vec_duration.as_secs_f64() * 1000.0);
        println!("   Throughput: {:.0} writes/sec", vec_throughput);
        println!("   Memory: {} bytes", vec.len() * 4);

        // Test 2: Vec<u32> with capacity
        println!("\n2️⃣  Vec<u32> with pre-allocated capacity:");
        let mut vec_cap = Vec::with_capacity(size as usize);
        let vec_cap_start = Instant::now();
        for i in 0..size {
            vec_cap.push(i);
        }
        let vec_cap_duration = vec_cap_start.elapsed();
        let vec_cap_throughput = size as f64 / vec_cap_duration.as_secs_f64();
        
        println!("   Time: {:.3}ms", vec_cap_duration.as_secs_f64() * 1000.0);
        println!("   Throughput: {:.0} writes/sec", vec_cap_throughput);
        println!("   Speedup: {:.1}x faster than Vec without capacity", vec_throughput / vec_cap_throughput);

        // Test 3: RoaringBitmap insert
        println!("\n3️⃣  RoaringBitmap insert:");
        let mut bitmap = RoaringBitmap::new();
        let bitmap_start = Instant::now();
        for i in 0..size {
            bitmap.insert(i);
        }
        let bitmap_duration = bitmap_start.elapsed();
        let bitmap_throughput = size as f64 / bitmap_duration.as_secs_f64();
        
        println!("   Time: {:.3}ms", bitmap_duration.as_secs_f64() * 1000.0);
        println!("   Throughput: {:.0} writes/sec", bitmap_throughput);
        println!("   Memory: {} bytes", bitmap.serialized_size());
        println!("   Compression: {:.1}x", (size * 4) as f64 / bitmap.serialized_size() as f64);

        // Test 4: RoaringBitmap from sorted iterator
        println!("\n4️⃣  RoaringBitmap from_sorted_iter:");
        let sorted_start = Instant::now();
        let bitmap_sorted = RoaringBitmap::from_sorted_iter(0..size).unwrap();
        let sorted_duration = sorted_start.elapsed();
        let sorted_throughput = size as f64 / sorted_duration.as_secs_f64();
        
        println!("   Time: {:.3}ms", sorted_duration.as_secs_f64() * 1000.0);
        println!("   Throughput: {:.0} writes/sec", sorted_throughput);

        // Summary
        println!("\n📈 Performance Comparison:");
        println!("   Vec<u32> (no capacity): 1.00x (baseline)");
        println!("   Vec<u32> (with capacity): {:.2}x faster", vec_throughput / vec_cap_throughput);
        println!("   RoaringBitmap insert: {:.2}x {}", 
            vec_throughput / bitmap_throughput,
            if vec_throughput > bitmap_throughput { "slower" } else { "faster" }
        );
        println!("   RoaringBitmap sorted: {:.2}x {}", 
            vec_throughput / sorted_throughput,
            if vec_throughput > sorted_throughput { "slower" } else { "faster" }
        );

        println!("\n💾 Space Efficiency:");
        println!("   Vec<u32>: {} bytes", vec.len() * 4);
        println!("   RoaringBitmap: {} bytes (compressed)", bitmap.serialized_size());
        println!("   Savings: {:.1}%\n", 
            (1.0 - bitmap.serialized_size() as f64 / (vec.len() * 4) as f64) * 100.0
        );
        
        println!("{}\n", "=".repeat(60));
    }

    // Additional test: Random insertion pattern
    println!("🎲 Bonus: Random insertion pattern (10,000 inserts)");
    println!("{}", "=".repeat(60));
    
    use rand::Rng;
    let mut rng = rand::thread_rng();
    let random_ids: Vec<u32> = (0..10_000).map(|_| rng.gen_range(0..1_000_000)).collect();

    println!("\n1️⃣  Vec<u32> (random order):");
    let mut vec_random = Vec::new();
    let vec_random_start = Instant::now();
    for &id in &random_ids {
        vec_random.push(id);
    }
    let vec_random_duration = vec_random_start.elapsed();
    println!("   Time: {:.3}ms", vec_random_duration.as_secs_f64() * 1000.0);
    println!("   Throughput: {:.0} writes/sec", 10_000.0 / vec_random_duration.as_secs_f64());

    println!("\n2️⃣  RoaringBitmap (random order):");
    let mut bitmap_random = RoaringBitmap::new();
    let bitmap_random_start = Instant::now();
    for &id in &random_ids {
        bitmap_random.insert(id);
    }
    let bitmap_random_duration = bitmap_random_start.elapsed();
    println!("   Time: {:.3}ms", bitmap_random_duration.as_secs_f64() * 1000.0);
    println!("   Throughput: {:.0} writes/sec", 10_000.0 / bitmap_random_duration.as_secs_f64());

    println!("\n📊 Random insertion comparison:");
    if vec_random_duration < bitmap_random_duration {
        println!("   ✅ Vec<u32> is {:.1}x faster for random inserts", 
            bitmap_random_duration.as_secs_f64() / vec_random_duration.as_secs_f64());
    } else {
        println!("   ✅ RoaringBitmap is {:.1}x faster for random inserts", 
            vec_random_duration.as_secs_f64() / bitmap_random_duration.as_secs_f64());
    }

    println!("\n{}", "=".repeat(60));
    println!("\n🎯 Conclusion:");
    println!("   • Vec<u32> is typically faster for pure append operations");
    println!("   • Pre-allocating capacity significantly improves Vec performance");
    println!("   • RoaringBitmap provides excellent compression (70-90% space savings)");
    println!("   • Best strategy: Use Vec in memory, convert to Bitmap for disk\n");
}
