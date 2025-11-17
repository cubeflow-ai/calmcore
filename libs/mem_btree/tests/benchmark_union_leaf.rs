use mem_btree::persist::{
    ReadSerializer, TreeReader, TreeWriter, UnionLeafSerializer, WriteSerializer,
};
use mem_btree::BTree;
use roaring::RoaringBitmap;
use std::borrow::Cow;
use std::ops::BitOr;
use std::path::PathBuf;
use std::time::Instant;

/// Bitmap serializer for key-value pairs
struct BitmapSerializer;

impl WriteSerializer<u32, RoaringBitmap> for BitmapSerializer {
    fn serialize_keys<'a>(&self, keys: &'a Vec<u32>) -> Cow<'a, [u8]> {
        let mut buf = Vec::with_capacity(keys.len() * 4);
        for &key in keys {
            buf.extend_from_slice(&key.to_be_bytes());
        }
        Cow::Owned(buf)
    }

    fn serialize_value<'a>(&self, value: &'a RoaringBitmap) -> Cow<'a, [u8]> {
        let mut buf = Vec::new();
        value.serialize_into(&mut buf).unwrap();
        Cow::Owned(buf)
    }
}

impl ReadSerializer<u32, RoaringBitmap> for BitmapSerializer {
    fn deserialize_keys<'a>(&self, data: &'a [u8]) -> Vec<u32> {
        data.chunks_exact(4)
            .map(|chunk| u32::from_be_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
            .collect()
    }

    fn deserialize_value<'a>(
        &self,
        data: &'a [u8],
    ) -> std::result::Result<RoaringBitmap, Box<dyn std::error::Error>> {
        Ok(RoaringBitmap::deserialize_from(data)?)
    }

    /// Check if union_leaf intersects with query range [start_key, end_key)
    /// NOTE: For bitmap values, union_leaf stores the union of all VALUE bitmaps in chunk,
    /// NOT the key range. So this optimization only works for value-based filtering,
    /// not key-based range queries.
    ///
    /// For pure key-range queries (like "give me all keys in [10000, 20000)"),
    /// union_leaf provides NO optimization since we need to scan all matching keys anyway.
    ///
    /// Union_leaf is useful for:
    /// 1. Aggregation: "Which chunks contain doc_id=5000?"
    /// 2. Filtered range: "In range [10000,20000), which chunks have bitmap containing doc_id=5000?"
    fn union_intersects_range(&self, _union_data: &[u8], _start_key: &u32, _end_key: &u32) -> bool {
        // For pure key-range queries, we cannot skip chunks based on value union
        // Return true to disable filtering (= no optimization for this use case)
        true
    }
}

/// Union leaf for bitmap aggregation - stores combined bitmap of all values in chunk
struct BitmapUnionLeaf {
    union_bitmap: std::sync::Mutex<RoaringBitmap>,
}

impl BitmapUnionLeaf {
    fn new() -> Self {
        Self {
            union_bitmap: std::sync::Mutex::new(RoaringBitmap::new()),
        }
    }
}

impl UnionLeafSerializer<RoaringBitmap> for BitmapUnionLeaf {
    fn add_value<'a>(&self, value: &'a RoaringBitmap) {
        let mut guard = self.union_bitmap.lock().unwrap();
        *guard = &*guard | value;
    }

    fn release<'a>(&self) -> RoaringBitmap {
        let mut guard = self.union_bitmap.lock().unwrap();
        std::mem::replace(&mut *guard, RoaringBitmap::new())
    }
}

fn get_dir_size(path: &PathBuf) -> u64 {
    let mut total = 0;
    if let Ok(entries) = std::fs::read_dir(path) {
        for entry in entries.flatten() {
            if let Ok(metadata) = entry.metadata() {
                total += metadata.len();
            }
        }
    }
    total
}

fn format_size(bytes: u64) -> String {
    if bytes < 1024 {
        format!("{} B", bytes)
    } else if bytes < 1024 * 1024 {
        format!("{:.2} KB", bytes as f64 / 1024.0)
    } else if bytes < 1024 * 1024 * 1024 {
        format!("{:.2} MB", bytes as f64 / (1024.0 * 1024.0))
    } else {
        format!("{:.2} GB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
    }
}

#[test]
fn benchmark_union_leaf_large_dataset() {
    println!("\n{:=^80}", "");
    println!("{:^80}", "Union_leaf 大数据集性能测试");
    println!("{:=^80}\n", "");

    // 测试参数
    const NUM_KEYS: u32 = 100_000; // 10万个键
    const BITMAP_DENSITY: f32 = 0.1; // 每个 bitmap 包含 10% 的数据
    const BITMAP_MAX_VALUE: u32 = 10_000; // bitmap 值域范围
    const CHUNK_SIZE: usize = 128; // chunk 大小

    println!("📊 测试参数:");
    println!("  - 键数量: {}", NUM_KEYS);
    println!("  - Chunk大小: {}", CHUNK_SIZE);
    println!("  - Bitmap值域: 0-{}", BITMAP_MAX_VALUE);
    println!("  - Bitmap密度: {:.0}%", BITMAP_DENSITY * 100.0);
    println!();

    // 准备测试数据
    println!("🔧 生成测试数据...");
    let start = Instant::now();
    let mut tree = BTree::new(32);

    for key in 0..NUM_KEYS {
        let mut bitmap = RoaringBitmap::new();
        let num_values = (BITMAP_MAX_VALUE as f32 * BITMAP_DENSITY) as u32;

        // 确定性生成 bitmap 数据
        for i in 0..num_values {
            let value = (key * 7 + i * 13) % BITMAP_MAX_VALUE;
            bitmap.insert(value);
        }

        tree.put(key, bitmap);
    }

    println!("  ✓ 数据生成完成: {:?}", start.elapsed());
    println!();

    // ============ 测试1: 不启用 union_leaf ============
    println!("🚀 测试1: 标准B树 (无union_leaf)");
    println!("{:-^80}", "");

    let dir_no_union = PathBuf::from("/tmp/bench_bitmap_no_union");
    std::fs::remove_dir_all(&dir_no_union).ok();
    std::fs::create_dir_all(&dir_no_union).unwrap();

    // 写入性能
    let start = Instant::now();
    TreeWriter::new(dir_no_union.clone(), CHUNK_SIZE)
        .persist::<u32, RoaringBitmap, RoaringBitmap>(
            tree.len(),
            Box::new(BitmapSerializer),
            None, // 不启用 union_leaf
            tree.iter(),
        )
        .unwrap();
    let write_time_no_union = start.elapsed();

    let size_no_union = get_dir_size(&dir_no_union);

    println!("  写入时间: {:?}", write_time_no_union);
    println!("  磁盘占用: {}", format_size(size_no_union));

    // 读取性能测试
    let reader_no_union = TreeReader::new(&dir_no_union, Box::new(BitmapSerializer)).unwrap();

    // 单点查询
    let start = Instant::now();
    for i in (0..NUM_KEYS).step_by(1000) {
        let _ = reader_no_union.get(&i);
    }
    let point_query_time = start.elapsed();
    println!("  单点查询100次: {:?}", point_query_time);

    // Range 查询小范围
    let start = Instant::now();
    let mut count = 0;
    for item in reader_no_union.range(&10000, &10100) {
        count += item.1.len();
    }
    let small_range_time = start.elapsed();
    println!(
        "  Range查询[10000,10100): {:?} (扫描{}个bitmap值)",
        small_range_time, count
    );

    // Range 查询中等范围
    let start = Instant::now();
    let mut count = 0;
    for item in reader_no_union.range(&20000, &25000) {
        count += item.1.len();
    }
    let medium_range_time = start.elapsed();
    println!(
        "  Range查询[20000,25000): {:?} (扫描{}个bitmap值)",
        medium_range_time, count
    );

    // Range 查询大范围
    let start = Instant::now();
    let mut count = 0;
    for item in reader_no_union.range(&0, &50000) {
        count += item.1.len();
    }
    let large_range_time = start.elapsed();
    println!(
        "  Range查询[0,50000): {:?} (扫描{}个bitmap值)",
        large_range_time, count
    );

    println!();

    // ============ 测试2: 启用 union_leaf ============
    println!("🚀 测试2: 优化B树 (启用union_leaf)");
    println!("{:-^80}", "");

    let dir_with_union = PathBuf::from("/tmp/bench_bitmap_with_union");
    std::fs::remove_dir_all(&dir_with_union).ok();
    std::fs::create_dir_all(&dir_with_union).unwrap();

    // 写入性能
    let start = Instant::now();
    TreeWriter::new(dir_with_union.clone(), CHUNK_SIZE)
        .persist::<u32, RoaringBitmap, RoaringBitmap>(
            tree.len(),
            Box::new(BitmapSerializer),
            Some(Box::new(BitmapUnionLeaf::new())), // 启用 union_leaf
            tree.iter(),
        )
        .unwrap();
    let write_time_with_union = start.elapsed();

    let size_with_union = get_dir_size(&dir_with_union);

    println!("  写入时间: {:?}", write_time_with_union);
    println!("  磁盘占用: {}", format_size(size_with_union));

    // 读取性能测试
    let reader_with_union = TreeReader::new(&dir_with_union, Box::new(BitmapSerializer)).unwrap();
    assert!(reader_with_union.has_union_leaf(), "应该启用了union_leaf");

    // 单点查询
    let start = Instant::now();
    for i in (0..NUM_KEYS).step_by(1000) {
        let _ = reader_with_union.get(&i);
    }
    let point_query_time_union = start.elapsed();
    println!("  单点查询100次: {:?}", point_query_time_union);

    // Range 查询小范围
    let start = Instant::now();
    let mut count = 0;
    for item in reader_with_union.range(&10000, &10100) {
        count += item.1.len();
    }
    let small_range_time_union = start.elapsed();
    println!(
        "  Range查询[10000,10100): {:?} (扫描{}个bitmap值)",
        small_range_time_union, count
    );

    // Range 查询中等范围
    let start = Instant::now();
    let mut count = 0;
    for item in reader_with_union.range(&20000, &25000) {
        count += item.1.len();
    }
    let medium_range_time_union = start.elapsed();
    println!(
        "  Range查询[20000,25000): {:?} (扫描{}个bitmap值)",
        medium_range_time_union, count
    );

    // Range 查询大范围
    let start = Instant::now();
    let mut count = 0;
    for item in reader_with_union.range(&0, &50000) {
        count += item.1.len();
    }
    let large_range_time_union = start.elapsed();
    println!(
        "  Range查询[0,50000): {:?} (扫描{}个bitmap值)",
        large_range_time_union, count
    );

    println!();

    // ============ 性能对比总结 ============
    println!("\n{:=^80}", "");
    println!("{:^80}", "性能对比总结");
    println!("{:=^80}\n", "");

    println!("📝 磁盘占用:");
    println!("  无union_leaf:  {}", format_size(size_no_union));
    println!("  有union_leaf:  {}", format_size(size_with_union));
    let size_overhead = size_with_union as i64 - size_no_union as i64;
    let size_overhead_pct = (size_overhead as f64 / size_no_union as f64) * 100.0;
    if size_overhead >= 0 {
        println!(
            "  额外开销:      {} ({:+.2}%)",
            format_size(size_overhead as u64),
            size_overhead_pct
        );
    } else {
        println!(
            "  节省空间:      {} ({:.2}%)",
            format_size((-size_overhead) as u64),
            -size_overhead_pct
        );
    }
    println!();

    println!("⚡ 写入性能:");
    println!("  无union_leaf:  {:?}", write_time_no_union);
    println!("  有union_leaf:  {:?}", write_time_with_union);
    let write_overhead =
        write_time_with_union.as_micros() as f64 / write_time_no_union.as_micros() as f64;
    println!("  性能比:        {:.2}x", write_overhead);
    println!();

    println!("🔍 单点查询性能 (100次):");
    println!("  无union_leaf:  {:?}", point_query_time);
    println!("  有union_leaf:  {:?}", point_query_time_union);
    let point_ratio =
        point_query_time_union.as_micros() as f64 / point_query_time.as_micros() as f64;
    println!("  性能比:        {:.2}x", point_ratio);
    println!();

    println!("📊 Range查询性能:");
    println!("  小范围[100个key]:");
    println!("    无union_leaf: {:?}", small_range_time);
    println!("    有union_leaf: {:?}", small_range_time_union);
    println!(
        "    性能比:       {:.2}x",
        small_range_time_union.as_micros() as f64 / small_range_time.as_micros() as f64
    );
    println!();

    println!("  中范围[5000个key]:");
    println!("    无union_leaf: {:?}", medium_range_time);
    println!("    有union_leaf: {:?}", medium_range_time_union);
    println!(
        "    性能比:       {:.2}x",
        medium_range_time_union.as_micros() as f64 / medium_range_time.as_micros() as f64
    );
    println!();

    println!("  大范围[50000个key]:");
    println!("    无union_leaf: {:?}", large_range_time);
    println!("    有union_leaf: {:?}", large_range_time_union);
    println!(
        "    性能比:       {:.2}x",
        large_range_time_union.as_micros() as f64 / large_range_time.as_micros() as f64
    );
    println!();

    // ============ 结论分析 ============
    println!("{:=^80}", "");
    println!("{:^80}", "结论分析");
    println!("{:=^80}\n", "");

    println!("✅ Union_leaf 优势:");
    println!("  1. 每个chunk存储了union bitmap,可用于快速过滤");
    println!("  2. 支持未来的聚合查询优化 (如:快速判断chunk是否包含某值)");
    println!("  3. 可用于统计信息查询,无需扫描所有数据");
    println!();

    println!("⚠️  Union_leaf 成本:");
    println!("  1. 磁盘空间增加 ~{:.1}%", size_overhead_pct);
    println!("  2. 写入时需要额外计算union bitmap");
    println!("  3. 当前reader未充分利用union_leaf (待优化)");
    println!();

    println!("💡 优化建议:");
    println!("  1. 在reader中利用union_leaf进行chunk过滤");
    println!("  2. 实现聚合查询: range_contains(key, value)");
    println!("  3. 实现快速统计: range_union() 返回范围内所有bitmap的并集");
    println!("  4. 考虑压缩union_leaf数据以减少磁盘开销");
    println!();

    println!("{:=^80}\n", "");
}

#[test]
fn test_union_leaf_correctness() {
    println!("\n🧪 Union_leaf 正确性测试");
    println!("{:-^60}", "");

    let dir = PathBuf::from("/tmp/test_union_leaf_correctness");
    std::fs::remove_dir_all(&dir).ok();
    std::fs::create_dir_all(&dir).unwrap();

    // 创建测试数据 - 增加数据量以确保有多个chunk
    let mut tree = BTree::new(32);
    for key in 0..5000u32 {
        // 增加到5000个键
        let mut bitmap = RoaringBitmap::new();
        // 每个key的bitmap包含 key, key+1, key+2
        bitmap.insert(key);
        bitmap.insert(key + 1);
        bitmap.insert(key + 2);
        tree.put(key, bitmap);
    }

    // 写入并启用 union_leaf
    TreeWriter::new(dir.clone(), 128) // 使用128的chunk size
        .persist::<u32, RoaringBitmap, RoaringBitmap>(
            tree.len(),
            Box::new(BitmapSerializer),
            Some(Box::new(BitmapUnionLeaf::new())),
            tree.iter(),
        )
        .unwrap();

    // 读取并验证
    let reader = TreeReader::new(&dir, Box::new(BitmapSerializer)).unwrap();

    assert!(reader.has_union_leaf(), "应该启用union_leaf");
    assert_eq!(reader.len(), 5000, "应该有5000个键");

    println!("  ✓ Reader创建成功: len={}, has_union_leaf={}", reader.len(), reader.has_union_leaf());
    
    // 先测试前几个key
    println!("  测试前10个key:");
    for key in 0..10u32 {
        match reader.get(&key) {
            Some(bm) => println!("    Key {}: ✓ bitmap.len()={}", key, bm.len()),
            None => println!("    Key {}: ✗ NOT FOUND", key),
        }
    }

    // 验证数据完整性 - 抽样检查 (测试所有能被100整除的key)
    println!("  测试抽样keys:");
    let test_keys = vec![0, 100, 200, 300, 400, 500, 1000, 2000, 3000, 4000, 4900];
    for &key in &test_keys {
        let bitmap = reader
            .get(&key)
            .expect(&format!("应该能找到key {}", key));
        assert!(
            bitmap.contains(key),
            "Key {} 的bitmap应该包含key本身",
            key
        );
        assert!(
            bitmap.contains(key + 1),
            "Key {} 的bitmap应该包含key+1",
            key
        );
        assert!(
            bitmap.contains(key + 2),
            "Key {} 的bitmap应该包含key+2",
            key
        );
        assert_eq!(bitmap.len(), 3, "Key {} 的每个bitmap应该有3个元素", key);
    }

    // 验证 range 查询
    let range_result: Vec<_> = reader.range(&1000, &1010).collect();
    assert_eq!(range_result.len(), 10, "range [1000,1010) 应该返回10个元素");

    for (idx, item) in range_result.iter().enumerate() {
        let key = 1000 + idx as u32;
        assert_eq!(item.0, key, "键应该按顺序");
        assert_eq!(item.1.len(), 3, "每个bitmap应该有3个元素");
    }

    println!("  ✓ 所有正确性测试通过!");
    println!();
}
