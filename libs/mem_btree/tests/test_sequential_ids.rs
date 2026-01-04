/// Test case for sequential ID persistence bug
/// Reproduces the issue where 256 keys are lost at position 65792 (chunk 257)
use mem_btree::persist::num_ser::i64_coder;
use mem_btree::persist::{ReadSerializer, TreeReader, TreeWriter, WriteSerializer};
use mem_btree::BTree;
use roaring::RoaringBitmap;
use std::borrow::Cow;
use std::error::Error;
use std::fs;

struct BitmapSerializer;

impl WriteSerializer<u64, RoaringBitmap> for BitmapSerializer {
    fn serialize_keys<'a>(&self, keys: &'a Vec<u64>) -> Cow<'a, [u8]> {
        let mut buf = Vec::with_capacity(keys.len() * 8);
        let i64_keys: Vec<i64> = keys.iter().map(|&k| k as i64).collect();
        i64_coder::write_delta(&mut buf, &i64_keys).unwrap();
        Cow::Owned(buf)
    }

    fn serialize_value<'a>(&self, value: &'a RoaringBitmap) -> Cow<'a, [u8]> {
        let mut buf = Vec::new();
        value.serialize_into(&mut buf).unwrap();
        Cow::Owned(buf)
    }
}

impl ReadSerializer<u64, RoaringBitmap> for BitmapSerializer {
    fn deserialize_keys<'a>(&self, data: &'a [u8]) -> Vec<u64> {
        i64_coder::read_delta(&data)
            .iter()
            .map(|&k| k as u64)
            .collect()
    }

    fn deserialize_value<'a>(&self, data: &'a [u8]) -> Result<RoaringBitmap, Box<dyn Error>> {
        Ok(RoaringBitmap::deserialize_from(data)?)
    }
}

#[test]
fn test_sequential_ids_no_loss() {
    // Setup temp directory
    let temp_dir = std::env::temp_dir().join("test_sequential_ids");
    if temp_dir.exists() {
        fs::remove_dir_all(&temp_dir).unwrap();
    }
    fs::create_dir_all(&temp_dir).unwrap();

    // Insert sequential IDs: 0, 1, 2, ..., 100000 into BTree
    // This will create 391 leaf chunks (100001 / 256 ≈ 391)
    // Chunk 257 starts at position 65792
    let total_count = 100_001u64;
    let chunk_size = 256;
    println!("📝 Creating BTree with {} sequential IDs...", total_count);

    let mut btree = BTree::<u64, RoaringBitmap>::new(chunk_size);

    for id in 0..total_count {
        let mut bitmap = RoaringBitmap::new();
        bitmap.insert(id as u32);
        btree.put(id, bitmap);

        // Progress indicator
        if id % 10000 == 0 {
            println!("  Inserted {} keys", id);
        }
    }

    println!("✅ BTree created with {} keys", btree.len());
    assert_eq!(btree.len(), total_count as usize);

    // Persist to disk using TreeWriter
    println!("💾 Persisting to disk...");
    let serializer = BitmapSerializer;
    let writer = TreeWriter::new(temp_dir.clone(), chunk_size);

    writer
        .persist::<u64, RoaringBitmap, RoaringBitmap>(
            btree.len(),
            Box::new(serializer),
            None, // 不使用 union_leaf 优化
            btree.iter(),
        )
        .unwrap();

    println!("✅ Persist completed");

    // Read from disk using TreeReader
    println!("📂 Loading from disk...");
    let serializer = BitmapSerializer;
    let reader = TreeReader::new(&temp_dir, Box::new(serializer)).unwrap();
    let disk_len = reader.len();
    println!("✅ Loaded from disk, len={}", disk_len);

    // Critical check: length should match
    assert_eq!(
        disk_len, total_count as usize,
        "Disk TreeReader length mismatch! Expected {}, got {}",
        total_count, disk_len
    );

    // First, check specific boundary keys
    println!("🔍 Checking specific boundary keys...");
    let test_keys = vec![65535, 65536, 65791, 65792, 66047, 66048];
    for &key in &test_keys {
        let exists = reader.get(&key).is_some();
        println!(
            "  Key {}: {}",
            key,
            if exists { "✅ EXISTS" } else { "❌ MISSING" }
        );
    }

    // Verify ALL keys can be retrieved from disk
    println!("🔍 Verifying all keys from disk...");
    let mut missing_keys = Vec::new();

    for id in 0..total_count {
        if reader.get(&id).is_none() {
            missing_keys.push(id);
        }

        // Progress indicator
        if id % 10000 == 0 {
            println!("  Verified {} keys", id);
        }
    }

    if !missing_keys.is_empty() {
        println!("❌ Found {} missing keys:", missing_keys.len());

        // Analyze missing key pattern
        let first_missing = *missing_keys.first().unwrap();
        let last_missing = *missing_keys.last().unwrap();

        println!(
            "   First missing: {} (position={})",
            first_missing, first_missing
        );
        println!(
            "   Last missing: {} (position={})",
            last_missing, last_missing
        );
        println!(
            "   Missing range span: {}",
            last_missing - first_missing + 1
        );
        println!(
            "   First missing / 256 = {} remainder {}",
            first_missing / 256,
            first_missing % 256
        );
        println!(
            "   Last missing / 256 = {} remainder {}",
            last_missing / 256,
            last_missing % 256
        );

        // Show first 20 missing keys
        println!("   First 20 missing keys:");
        for key in missing_keys.iter().take(20) {
            println!("     - {} (chunk {})", key, key / 256);
        }

        panic!("Missing {} keys from disk!", missing_keys.len());
    }

    println!(
        "✅ All {} keys verified successfully from disk!",
        total_count
    );
    println!("🎉 Test PASSED - No data loss!");

    // Cleanup
    fs::remove_dir_all(&temp_dir).ok();
}

#[test]
fn test_chunk_257_boundary() {
    // Focused test specifically for chunk 257
    let temp_dir = std::env::temp_dir().join("test_chunk_257");
    if temp_dir.exists() {
        fs::remove_dir_all(&temp_dir).unwrap();
    }
    fs::create_dir_all(&temp_dir).unwrap();

    // Insert exactly up to and including chunk 257
    // Chunk 257 spans positions 65792-66047 (256 keys)
    // So we need at least 66048 keys (257 * 256)
    let count = 66_048u64;
    let chunk_size = 256;

    println!("📝 Creating BTree with {} keys (257 full chunks)...", count);

    let mut btree = BTree::<u64, RoaringBitmap>::new(chunk_size);

    for id in 0..count {
        let mut bitmap = RoaringBitmap::new();
        bitmap.insert(id as u32);
        btree.put(id, bitmap);
    }

    println!("💾 Persisting...");
    let serializer = BitmapSerializer;
    let writer = TreeWriter::new(temp_dir.clone(), chunk_size);

    writer
        .persist::<u64, RoaringBitmap, RoaringBitmap>(
            btree.len(),
            Box::new(serializer),
            None,
            btree.iter(),
        )
        .unwrap();

    println!("📂 Loading from disk...");
    let serializer = BitmapSerializer;
    let reader = TreeReader::new(&temp_dir, Box::new(serializer)).unwrap();

    // Focus on chunk 257: keys 65792-66047
    println!("🔍 Checking chunk 257 (positions 65792-66047)...");
    let mut missing_in_chunk_257 = Vec::new();

    for id in 65792..66048 {
        if reader.get(&id).is_none() {
            missing_in_chunk_257.push(id);
        }
    }

    if !missing_in_chunk_257.is_empty() {
        println!(
            "❌ Chunk 257 has {} missing keys:",
            missing_in_chunk_257.len()
        );
        for key in missing_in_chunk_257.iter().take(10) {
            println!("     - {}", key);
        }
        panic!("Chunk 257 data loss detected!");
    }

    println!("✅ Chunk 257 verified - all 256 keys present!");

    // Cleanup
    fs::remove_dir_all(&temp_dir).ok();
}
