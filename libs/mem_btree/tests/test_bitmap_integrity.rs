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

/// 测试重点：验证 bitmap 中的每个 doc_id 都被正确保存和读取
/// 模拟真实场景：多个 key，每个 key 对应一个包含多个 doc_id 的 bitmap
#[test]
fn test_bitmap_doc_ids_integrity() {
    let temp_dir = std::env::temp_dir().join("test_bitmap_doc_ids_integrity");
    if temp_dir.exists() {
        fs::remove_dir_all(&temp_dir).unwrap();
    }
    fs::create_dir_all(&temp_dir).unwrap();

    let mut tree = BTree::new(256);
    let mut expected_data = Vec::new();

    println!("📝 Creating test data with detailed doc_id tracking...");

    // 创建 1000 个 term，每个 term 对应不同数量的 doc_ids
    // 总共约 100,000 个 doc_ids
    let total_terms = 1000u64;
    let mut global_doc_id = 0u32;

    for term_id in 0..total_terms {
        let mut bitmap = RoaringBitmap::new();

        // 每个 term 关联不同数量的 doc_ids (50-150)
        let doc_count = 50 + (term_id % 100) as u32;
        let doc_ids_for_this_term: Vec<u32> = (0..doc_count)
            .map(|_| {
                let id = global_doc_id;
                global_doc_id += 1;
                id
            })
            .collect();

        for doc_id in &doc_ids_for_this_term {
            bitmap.insert(*doc_id);
        }

        tree.put(term_id, bitmap.clone());
        expected_data.push((term_id, bitmap, doc_ids_for_this_term));
    }

    let total_doc_ids: u64 = expected_data.iter().map(|(_, bm, _)| bm.len()).sum();
    println!(
        "✅ Created {} terms with {} total doc_ids",
        total_terms, total_doc_ids
    );
    println!("   Doc ID range: 0 to {}", global_doc_id - 1);

    // 持久化
    println!("💾 Persisting to disk...");
    TreeWriter::new(temp_dir.clone(), 256)
        .persist::<u64, RoaringBitmap, RoaringBitmap>(
            tree.len(),
            Box::new(BitmapSerializer),
            None,
            tree.iter(),
        )
        .unwrap();

    println!("📂 Loading from disk...");
    let reader = TreeReader::new(&temp_dir, Box::new(BitmapSerializer)).unwrap();

    assert_eq!(
        reader.len(),
        expected_data.len(),
        "TreeReader length mismatch!"
    );

    // 🔍 重点验证：每个 bitmap 中的每个 doc_id 都能读出来
    println!("🔍 Verifying bitmap doc_ids integrity...");
    let mut total_missing_docs = 0u64;
    let mut total_verified_docs = 0u64;
    let mut terms_with_missing_docs = Vec::new();

    for (term_id, expected_bitmap, expected_doc_ids) in &expected_data {
        match reader.get(term_id) {
            Some(actual_bitmap) => {
                let expected_len = expected_bitmap.len();
                let actual_len = actual_bitmap.len();

                if expected_len != actual_len {
                    println!(
                        "❌ Term {}: expected {} doc_ids, got {} doc_ids (LOST {} doc_ids)",
                        term_id,
                        expected_len,
                        actual_len,
                        expected_len - actual_len
                    );
                    total_missing_docs += (expected_len - actual_len) as u64;
                    terms_with_missing_docs.push(*term_id);

                    // 详细检查哪些 doc_id 丢失了
                    let mut missing_doc_ids = Vec::new();
                    for doc_id in expected_doc_ids {
                        if !actual_bitmap.contains(*doc_id) {
                            missing_doc_ids.push(*doc_id);
                        }
                    }

                    if !missing_doc_ids.is_empty() {
                        println!(
                            "   Missing doc_ids (first 10): {:?}",
                            &missing_doc_ids[..missing_doc_ids.len().min(10)]
                        );
                    }

                    // 检查是否有多余的 doc_id
                    let mut extra_doc_ids = Vec::new();
                    for doc_id in actual_bitmap.iter() {
                        if !expected_bitmap.contains(doc_id) {
                            extra_doc_ids.push(doc_id);
                        }
                    }

                    if !extra_doc_ids.is_empty() {
                        println!(
                            "   Extra doc_ids (first 10): {:?}",
                            &extra_doc_ids[..extra_doc_ids.len().min(10)]
                        );
                    }
                } else {
                    // 即使长度相同，也要验证每个 doc_id
                    for doc_id in expected_doc_ids {
                        if !actual_bitmap.contains(*doc_id) {
                            println!(
                                "❌ Term {}: doc_id {} is missing (but length matches!)",
                                term_id, doc_id
                            );
                            total_missing_docs += 1;
                            terms_with_missing_docs.push(*term_id);
                            break;
                        }
                    }
                    total_verified_docs += expected_len as u64;
                }
            }
            None => {
                panic!("❌ Term {} not found in disk index!", term_id);
            }
        }

        if (term_id + 1) % 100 == 0 {
            println!("  Verified {} terms", term_id + 1);
        }
    }

    // 报告结果
    println!("\n📊 Verification Summary:");
    println!("   Total terms: {}", total_terms);
    println!("   Total expected doc_ids: {}", total_doc_ids);
    println!("   Total verified doc_ids: {}", total_verified_docs);

    if total_missing_docs > 0 {
        println!("   ❌ MISSING doc_ids: {}", total_missing_docs);
        println!(
            "   ❌ Terms with missing docs: {} (first 10: {:?})",
            terms_with_missing_docs.len(),
            &terms_with_missing_docs[..terms_with_missing_docs.len().min(10)]
        );
        panic!("Found {} missing doc_ids in bitmaps!", total_missing_docs);
    }

    println!("✅ All {} doc_ids verified successfully!", total_doc_ids);
    println!("🎉 Bitmap doc_ids integrity test PASSED!");

    fs::remove_dir_all(&temp_dir).ok();
}

/// 测试边界情况：大量 doc_ids 在同一个 bitmap 中
#[test]
fn test_large_bitmap_doc_ids_integrity() {
    let temp_dir = std::env::temp_dir().join("test_large_bitmap_doc_ids");
    if temp_dir.exists() {
        fs::remove_dir_all(&temp_dir).unwrap();
    }
    fs::create_dir_all(&temp_dir).unwrap();

    let mut tree = BTree::new(256);
    let mut expected_data = Vec::new();

    println!("📝 Creating test data with large bitmaps...");

    // 创建 300 个 term，每个包含 1000-2000 个 doc_ids
    let total_terms = 300u64;
    let mut global_doc_id = 0u32;

    for term_id in 0..total_terms {
        let mut bitmap = RoaringBitmap::new();

        let doc_count = 1000 + (term_id as u32 % 1000);
        let doc_ids: Vec<u32> = (0..doc_count)
            .map(|_| {
                let id = global_doc_id;
                global_doc_id += 1;
                id
            })
            .collect();

        for doc_id in &doc_ids {
            bitmap.insert(*doc_id);
        }

        tree.put(term_id, bitmap.clone());
        expected_data.push((term_id, bitmap, doc_ids));
    }

    let total_doc_ids: u64 = expected_data.iter().map(|(_, bm, _)| bm.len()).sum();
    println!(
        "✅ Created {} terms with {} total doc_ids",
        total_terms, total_doc_ids
    );

    println!("💾 Persisting...");
    TreeWriter::new(temp_dir.clone(), 256)
        .persist::<u64, RoaringBitmap, RoaringBitmap>(
            tree.len(),
            Box::new(BitmapSerializer),
            None,
            tree.iter(),
        )
        .unwrap();

    println!("📂 Loading...");
    let reader = TreeReader::new(&temp_dir, Box::new(BitmapSerializer)).unwrap();

    println!("🔍 Verifying large bitmaps...");
    let mut total_verified = 0u64;

    for (term_id, expected_bitmap, expected_doc_ids) in &expected_data {
        let actual_bitmap = reader
            .get(term_id)
            .expect(&format!("Term {} not found!", term_id));

        assert_eq!(
            actual_bitmap.len(),
            expected_bitmap.len(),
            "Term {}: bitmap size mismatch",
            term_id
        );

        // 验证每个 doc_id
        for doc_id in expected_doc_ids {
            assert!(
                actual_bitmap.contains(*doc_id),
                "Term {}: missing doc_id {}",
                term_id,
                doc_id
            );
        }

        total_verified += actual_bitmap.len();
    }

    println!(
        "✅ Verified {} doc_ids across {} terms!",
        total_verified, total_terms
    );
    println!("🎉 Large bitmap integrity test PASSED!");

    fs::remove_dir_all(&temp_dir).ok();
}

/// 测试关键边界：chunk 257-258 边界附近的 bitmap doc_ids
#[test]
fn test_chunk_boundary_bitmap_integrity() {
    let temp_dir = std::env::temp_dir().join("test_chunk_boundary_bitmap");
    if temp_dir.exists() {
        fs::remove_dir_all(&temp_dir).unwrap();
    }
    fs::create_dir_all(&temp_dir).unwrap();

    let mut tree = BTree::new(256);
    let mut expected_data = Vec::new();

    println!("📝 Creating data around chunk 257-258 boundary...");

    // 创建恰好 66,000 个 term（会跨越 chunk 257/258 边界）
    let total_terms = 66_000u64;
    let mut global_doc_id = 0u32;

    for term_id in 0..total_terms {
        let mut bitmap = RoaringBitmap::new();

        // 每个 term 只有 1 个 doc_id，简化验证
        bitmap.insert(global_doc_id);
        let doc_ids = vec![global_doc_id];
        global_doc_id += 1;

        tree.put(term_id, bitmap.clone());
        expected_data.push((term_id, bitmap, doc_ids));
    }

    println!(
        "✅ Created {} terms (spans {} chunks)",
        total_terms,
        (total_terms + 255) / 256
    );

    println!("💾 Persisting...");
    TreeWriter::new(temp_dir.clone(), 256)
        .persist::<u64, RoaringBitmap, RoaringBitmap>(
            tree.len(),
            Box::new(BitmapSerializer),
            None,
            tree.iter(),
        )
        .unwrap();

    println!("📂 Loading...");
    let reader = TreeReader::new(&temp_dir, Box::new(BitmapSerializer)).unwrap();

    println!("🔍 Checking boundary terms (65790-65795)...");

    let boundary_terms = vec![65790, 65791, 65792, 65793, 65794, 65795];
    for term_id in boundary_terms {
        if term_id < expected_data.len() as u64 {
            let (_, expected_bitmap, expected_doc_ids) = &expected_data[term_id as usize];
            let actual_bitmap = reader
                .get(&term_id)
                .expect(&format!("Term {} not found!", term_id));

            assert_eq!(
                actual_bitmap.len(),
                expected_bitmap.len(),
                "Term {}: bitmap size mismatch",
                term_id
            );

            for doc_id in expected_doc_ids {
                assert!(
                    actual_bitmap.contains(*doc_id),
                    "Term {}: missing doc_id {}",
                    term_id,
                    doc_id
                );
            }

            println!(
                "  ✅ Term {}: doc_id {} verified",
                term_id, expected_doc_ids[0]
            );
        }
    }

    println!("✅ All boundary terms verified!");
    println!("🎉 Boundary bitmap integrity test PASSED!");

    fs::remove_dir_all(&temp_dir).ok();
}
