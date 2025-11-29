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

/// 测试使用真实世界的非连续 key 模式
/// 模拟 String hash 或者时间戳等真实数据
#[test]
fn test_non_sequential_keys_realistic() {
    let temp_dir = std::env::temp_dir().join("test_non_sequential_realistic");
    if temp_dir.exists() {
        fs::remove_dir_all(&temp_dir).unwrap();
    }
    fs::create_dir_all(&temp_dir).unwrap();

    // 模拟真实的非连续 key 分布
    // 这些 key 可能是 String 的 hash 值，分布不均匀
    let mut tree = BTree::new(256);
    let mut expected_data = Vec::new();

    println!("📝 Creating realistic non-sequential keys...");

    // 生成 70,000 个非连续的 key（会跨越多个 chunk 边界）
    // 使用质数步长来模拟真实的 hash 分布
    let total_keys = 70_000u64;
    let step = 97u64; // 质数步长，产生非连续分布

    for i in 0..total_keys {
        // 生成非连续的 key
        let key = (i * step) % 1_000_000; // 模拟 hash 值

        let mut bitmap = RoaringBitmap::new();
        bitmap.insert(i as u32); // doc_id 是连续的，但 key 不连续

        tree.put(key, bitmap.clone());
        expected_data.push((key, bitmap));
    }

    println!("✅ Created {} non-sequential keys", expected_data.len());

    // 持久化
    println!("💾 Persisting to disk...");
    let serializer = BitmapSerializer;
    TreeWriter::new(temp_dir.clone(), 256)
        .persist::<u64, RoaringBitmap, RoaringBitmap>(
            tree.len(),
            Box::new(serializer),
            None,
            tree.iter(),
        )
        .unwrap();

    println!("📂 Loading from disk...");
    let reader = TreeReader::new(&temp_dir, Box::new(BitmapSerializer)).unwrap();

    println!("✅ Loaded from disk, len={}", reader.len());
    assert_eq!(
        reader.len(),
        expected_data.len(),
        "TreeReader length mismatch!"
    );

    // 验证每个 key 都能读到
    println!("🔍 Verifying all keys are accessible...");
    let mut missing_keys = Vec::new();
    let mut missing_at_boundaries = Vec::new();

    for (idx, (key, expected_bitmap)) in expected_data.iter().enumerate() {
        match reader.get(key) {
            Some(actual_bitmap) => {
                if &actual_bitmap != expected_bitmap {
                    println!(
                        "❌ Key {} at index {}: bitmap mismatch! expected {} docs, got {} docs",
                        key,
                        idx,
                        expected_bitmap.len(),
                        actual_bitmap.len()
                    );
                }
            }
            None => {
                missing_keys.push(*key);

                // 检查是否在 chunk 边界附近
                let chunk_num = idx / 256;
                let pos_in_chunk = idx % 256;
                if pos_in_chunk < 5 || pos_in_chunk > 251 {
                    missing_at_boundaries.push((idx, chunk_num, pos_in_chunk, *key));
                    println!(
                        "❌ BOUNDARY LOSS: index={}, chunk={}, pos_in_chunk={}, key={}",
                        idx, chunk_num, pos_in_chunk, key
                    );
                }
            }
        }

        if (idx + 1) % 10000 == 0 {
            println!("  Verified {} keys", idx + 1);
        }
    }

    // 报告结果
    if !missing_keys.is_empty() {
        println!("\n❌ Found {} missing keys:", missing_keys.len());
        println!(
            "   First 10 missing keys: {:?}",
            &missing_keys[..missing_keys.len().min(10)]
        );

        if !missing_at_boundaries.is_empty() {
            println!(
                "\n🔍 Missing keys at chunk boundaries: {}",
                missing_at_boundaries.len()
            );
            for (idx, chunk, pos, key) in missing_at_boundaries.iter().take(10) {
                println!(
                    "   - index={}, chunk={}, pos_in_chunk={}, key={}",
                    idx, chunk, pos, key
                );
            }
        }

        panic!("Missing {} keys from disk!", missing_keys.len());
    }

    println!("✅ All {} keys verified successfully!", expected_data.len());
    println!("🎉 Non-sequential key test PASSED!");

    // Cleanup
    fs::remove_dir_all(&temp_dir).ok();
}

/// 测试特定的边界条件：key 在 chunk 257-258 边界
#[test]
fn test_chunk_boundary_257_258_non_sequential() {
    let temp_dir = std::env::temp_dir().join("test_boundary_257_258_non_seq");
    if temp_dir.exists() {
        fs::remove_dir_all(&temp_dir).unwrap();
    }
    fs::create_dir_all(&temp_dir).unwrap();

    let mut tree = BTree::new(256);
    let mut expected_data = Vec::new();

    println!("📝 Creating keys around chunk 257-258 boundary...");

    // 创建恰好 66,000 个 key（会触发 chunk 257/258 边界）
    // 但使用非连续的 key 值
    let total_keys = 66_000u64;

    for i in 0..total_keys {
        // 使用大质数生成非连续 key
        let key = (i * 31337) % 5_000_000;

        let mut bitmap = RoaringBitmap::new();
        bitmap.insert(i as u32);

        tree.put(key, bitmap.clone());
        expected_data.push((key, bitmap));
    }

    println!(
        "✅ Created {} keys (spans {} chunks)",
        expected_data.len(),
        (expected_data.len() + 255) / 256
    );

    println!("💾 Persisting...");
    let serializer = BitmapSerializer;
    TreeWriter::new(temp_dir.clone(), 256)
        .persist::<u64, RoaringBitmap, RoaringBitmap>(
            tree.len(),
            Box::new(serializer),
            None,
            tree.iter(),
        )
        .unwrap();

    println!("📂 Loading...");
    let reader = TreeReader::new(&temp_dir, Box::new(BitmapSerializer)).unwrap();

    assert_eq!(
        reader.len(),
        expected_data.len(),
        "Length mismatch after persist!"
    );

    println!("🔍 Verifying chunk boundaries 256-260...");

    // 重点检查 chunk 256-260 边界的 key
    let boundary_indices = vec![
        65535, 65536, // chunk 256 boundary
        65790, 65791, 65792, 65793, 65794, // chunk 257-258 boundary
        66047, 66048, // chunk 258-259 boundary
    ];

    for &idx in &boundary_indices {
        if idx < expected_data.len() {
            let (key, expected_bitmap) = &expected_data[idx];
            match reader.get(key) {
                Some(actual_bitmap) => {
                    assert_eq!(
                        actual_bitmap, *expected_bitmap,
                        "Bitmap mismatch at index {} (key={})",
                        idx, key
                    );
                    println!("  ✅ index={}, key={} - OK", idx, key);
                }
                None => {
                    panic!(
                        "❌ Key not found at index {} (key={}, chunk={})",
                        idx,
                        key,
                        idx / 256
                    );
                }
            }
        }
    }

    println!("✅ All boundary keys verified!");
    println!("🎉 Boundary test PASSED!");

    fs::remove_dir_all(&temp_dir).ok();
}
