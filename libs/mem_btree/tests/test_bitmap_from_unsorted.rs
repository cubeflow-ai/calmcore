/// 测试验证从无序 Vec<u32> 构建 bitmap 的情况
/// 模拟真实场景：并发写入可能导致 Vec 无序
use roaring::RoaringBitmap;

#[test]
fn test_unsorted_vec_to_bitmap() {
    // 无序的 doc_ids
    let unsorted_ids = vec![10u32, 5, 20, 3, 15, 1, 25, 8];
    let expected_ids: Vec<u32> = vec![1, 3, 5, 8, 10, 15, 20, 25];
    
    println!("📝 Testing unsorted vec to bitmap conversion");
    println!("   Unsorted: {:?}", unsorted_ids);
    println!("   Expected: {:?}", expected_ids);
    
    // 使用 from_iter (正确)
    let bitmap_from_iter = RoaringBitmap::from_iter(unsorted_ids.iter().copied());
    println!("   from_iter result: {} items", bitmap_from_iter.len());
    
    // 验证所有元素都在
    for id in &expected_ids {
        assert!(
            bitmap_from_iter.contains(*id),
            "from_iter: missing id {}",
            id
        );
    }
    
    assert_eq!(bitmap_from_iter.len(), expected_ids.len() as u64);
    println!("   ✅ from_iter: All {} elements present", expected_ids.len());
    
    // 测试 from_sorted_iter (可能有问题)
    match RoaringBitmap::from_sorted_iter(unsorted_ids.iter().copied()) {
        Ok(bitmap_from_sorted) => {
            println!("   from_sorted_iter result: {} items", bitmap_from_sorted.len());
            
            let mut missing = Vec::new();
            for id in &expected_ids {
                if !bitmap_from_sorted.contains(*id) {
                    missing.push(*id);
                }
            }
            
            if !missing.is_empty() {
                println!("   ❌ from_sorted_iter: LOST {} elements: {:?}",
                         missing.len(), missing);
                println!("   ⚠️  This demonstrates why from_sorted_iter is DANGEROUS with unsorted data!");
            } else if bitmap_from_sorted.len() != expected_ids.len() as u64 {
                println!("   ❌ from_sorted_iter: Length mismatch! expected {}, got {}",
                         expected_ids.len(), bitmap_from_sorted.len());
            } else {
                println!("   ⚠️  from_sorted_iter happened to work (but not guaranteed!)");
            }
        }
        Err(e) => {
            println!("   ✅ from_sorted_iter correctly rejected unsorted input: {:?}", e);
        }
    }
    
    println!("🎉 Test completed - from_iter is the safe choice!");
}

#[test]
fn test_sorted_vec_to_bitmap() {
    // 已排序的 doc_ids
    let sorted_ids = vec![1u32, 3, 5, 8, 10, 15, 20, 25];
    
    println!("📝 Testing sorted vec to bitmap conversion");
    println!("   Sorted: {:?}", sorted_ids);
    
    let bitmap_from_iter = RoaringBitmap::from_iter(sorted_ids.iter().copied());
    let bitmap_from_sorted = RoaringBitmap::from_sorted_iter(sorted_ids.iter().copied()).unwrap();
    
    assert_eq!(bitmap_from_iter.len(), sorted_ids.len() as u64);
    assert_eq!(bitmap_from_sorted.len(), sorted_ids.len() as u64);
    
    // 验证两种方法结果相同
    for id in &sorted_ids {
        assert!(bitmap_from_iter.contains(*id));
        assert!(bitmap_from_sorted.contains(*id));
    }
    
    println!("   ✅ Both methods work correctly with sorted input");
    println!("   ✅ from_iter: {} items", bitmap_from_iter.len());
    println!("   ✅ from_sorted_iter: {} items", bitmap_from_sorted.len());
}

#[test]
fn test_large_unsorted_vec_to_bitmap() {
    println!("📝 Testing large unsorted vec (simulating real workload)");
    
    // 模拟真实场景：100,000 个 doc_ids，随机顺序
    use std::collections::HashSet;
    let mut ids = Vec::new();
    let mut expected_set = HashSet::new();
    
    // 生成 100,000 个唯一 ID，但顺序打乱
    for i in 0..100_000u32 {
        let id = (i * 31337) % 500_000; // 使用质数生成伪随机 ID
        if expected_set.insert(id) {
            ids.push(id);
        }
    }
    
    println!("   Generated {} unique IDs", ids.len());
    
    // 使用 from_iter
    let bitmap = RoaringBitmap::from_iter(ids.iter().copied());
    
    println!("   Bitmap contains {} items", bitmap.len());
    assert_eq!(bitmap.len(), ids.len() as u64, "Lost doc_ids!");
    
    // 验证所有 ID 都在
    let mut missing_count = 0;
    for id in &ids {
        if !bitmap.contains(*id) {
            missing_count += 1;
        }
    }
    
    assert_eq!(missing_count, 0, "Missing {} doc_ids!", missing_count);
    println!("   ✅ All {} doc_ids verified!", ids.len());
}
