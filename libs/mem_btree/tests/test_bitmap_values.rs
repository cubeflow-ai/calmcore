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
fn test_bitmap_values_no_loss() {
    let temp_dir = std::env::temp_dir().join("test_bitmap_values");
    if temp_dir.exists() {
        fs::remove_dir_all(&temp_dir).unwrap();
    }
    fs::create_dir_all(&temp_dir).unwrap();

    // 模拟真实场景：每个 key (term) 对应一个包含多个 doc_id 的 bitmap
    // 我们创建 1000 个 terms，每个 term 关联 10-100 个随机 doc_ids
    let mut tree = BTree::new(256);
    let mut expected_data = Vec::new();

    println!("📝 Creating test data...");
    for term_id in 0..1000u64 {
        let mut bitmap = RoaringBitmap::new();

        // 每个 term 关联不同数量的 doc_ids (模拟真实的倒排索引)
        let doc_count = 10 + (term_id % 90) as u32; // 10-100 个 docs
        for i in 0..doc_count {
            let doc_id = term_id * 1000 + i as u64; // 生成唯一的 doc_id
            bitmap.insert(doc_id as u32);
        }

        tree.put(term_id, bitmap.clone());
        expected_data.push((term_id, bitmap));
    }

    let total_terms = expected_data.len();
    let total_docs: u64 = expected_data.iter().map(|(_, bm)| bm.len()).sum();
    println!(
        "✅ Created {} terms with {} total doc_ids",
        total_terms, total_docs
    );

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
    let serializer = BitmapSerializer;
    let reader = TreeReader::new(&temp_dir, Box::new(serializer)).unwrap();

    println!("✅ Loaded from disk, len={}", reader.len());
    assert_eq!(reader.len(), total_terms, "TreeReader length mismatch!");

    // 验证每个 term 的 bitmap 内容
    println!("🔍 Verifying bitmap contents...");
    let mut missing_terms = Vec::new();
    let mut corrupted_bitmaps = Vec::new();

    for (term_id, expected_bitmap) in &expected_data {
        match reader.get(term_id) {
            Some(actual_bitmap) => {
                // 验证 bitmap 内容是否一致
                if &actual_bitmap != expected_bitmap {
                    let expected_docs: Vec<u32> = expected_bitmap.iter().collect();
                    let actual_docs: Vec<u32> = actual_bitmap.iter().collect();

                    corrupted_bitmaps.push((
                        *term_id,
                        expected_bitmap.len(),
                        actual_bitmap.len(),
                        expected_docs.clone(),
                        actual_docs.clone(),
                    ));
                }
            }
            None => {
                missing_terms.push(*term_id);
            }
        }

        if (term_id + 1) % 100 == 0 {
            println!("  Verified {} terms", term_id + 1);
        }
    }

    // 报告结果
    if !missing_terms.is_empty() {
        println!("❌ Found {} missing terms:", missing_terms.len());
        for term in missing_terms.iter().take(10) {
            println!("   - Term {}", term);
        }
        panic!("Missing {} terms from disk!", missing_terms.len());
    }

    if !corrupted_bitmaps.is_empty() {
        println!("❌ Found {} corrupted bitmaps:", corrupted_bitmaps.len());
        for (term_id, expected_len, actual_len, expected_docs, actual_docs) in
            corrupted_bitmaps.iter().take(5)
        {
            println!(
                "   Term {}: expected {} docs, got {} docs",
                term_id, expected_len, actual_len
            );
            if expected_len != actual_len {
                println!(
                    "      Expected first 10: {:?}",
                    &expected_docs[..expected_docs.len().min(10)]
                );
                println!(
                    "      Actual first 10:   {:?}",
                    &actual_docs[..actual_docs.len().min(10)]
                );
            }
        }
        panic!("Found {} corrupted bitmaps!", corrupted_bitmaps.len());
    }

    println!(
        "✅ All {} terms verified with correct bitmap contents!",
        total_terms
    );
    println!("✅ Total {} doc_ids verified!", total_docs);
    println!("🎉 Test PASSED - No data loss!");

    // Cleanup
    fs::remove_dir_all(&temp_dir).ok();
}

#[test]
fn test_large_bitmap_values() {
    // 测试包含大量 doc_ids 的 bitmap（每个 bitmap 有几千个 doc_ids）
    let temp_dir = std::env::temp_dir().join("test_large_bitmaps");
    if temp_dir.exists() {
        fs::remove_dir_all(&temp_dir).unwrap();
    }
    fs::create_dir_all(&temp_dir).unwrap();

    let mut tree = BTree::new(256);
    let mut expected_data = Vec::new();

    println!("📝 Creating large bitmap test data...");
    // 创建 500 个 terms，每个包含 1000-5000 个 doc_ids
    for term_id in 0..500u64 {
        let mut bitmap = RoaringBitmap::new();
        let doc_count = 1000 + (term_id as u32 % 4000);

        for i in 0..doc_count {
            bitmap.insert(term_id as u32 * 10000 + i);
        }

        tree.put(term_id, bitmap.clone());
        expected_data.push((term_id, bitmap));
    }

    let total_docs: u64 = expected_data.iter().map(|(_, bm)| bm.len()).sum();
    println!("✅ Created 500 terms with {} total doc_ids", total_docs);

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
    assert_eq!(reader.len(), 500);

    println!("🔍 Verifying large bitmaps...");
    let mut total_verified_docs = 0u64;

    for (term_id, expected_bitmap) in &expected_data {
        let actual_bitmap = reader
            .get(term_id)
            .expect(&format!("Term {} not found!", term_id));

        assert_eq!(
            actual_bitmap.len(),
            expected_bitmap.len(),
            "Bitmap size mismatch for term {}",
            term_id
        );

        // 验证每个 doc_id
        for doc_id in expected_bitmap.iter() {
            assert!(
                actual_bitmap.contains(doc_id),
                "Term {}: missing doc_id {}",
                term_id,
                doc_id
            );
        }

        total_verified_docs += actual_bitmap.len();
    }

    println!("✅ All 500 terms verified!");
    println!("✅ Total {} doc_ids verified!", total_verified_docs);
    println!("🎉 Large bitmap test PASSED!");

    fs::remove_dir_all(&temp_dir).ok();
}
