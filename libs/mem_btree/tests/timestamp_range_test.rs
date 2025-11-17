use mem_btree::persist::num_ser::i64_coder;
use mem_btree::persist::{
    ReadSerializer, TreeReader, TreeWriter, UnionLeafSerializer, WriteSerializer,
};
use mem_btree::BTree;
use roaring::RoaringBitmap;
use std::borrow::Cow;
use std::error::Error;
use std::path::PathBuf;
use std::sync::Mutex;

// 模拟时间序列数据：timestamp -> RoaringBitmap (文档ID集合)
// 场景：10小时的日志数据，每毫秒1-2条记录

const HOURS: u64 = 10;
const MS_PER_HOUR: u64 = 3600 * 1000;
const TOTAL_MS: u64 = HOURS * MS_PER_HOUR; // 36,000,000 毫秒
const DOC_ID_RANGE: u32 = 100_000; // 文档ID范围

struct BitmapSerializer;

// 用于累积union数据的包装器
struct BitmapUnionAccumulator {
    current_union: Mutex<RoaringBitmap>,
}

impl BitmapUnionAccumulator {
    fn new() -> Self {
        Self {
            current_union: Mutex::new(RoaringBitmap::new()),
        }
    }
}

impl WriteSerializer<u64, RoaringBitmap> for BitmapSerializer {
    fn serialize_keys<'a>(&self, keys: &'a Vec<u64>) -> Cow<'a, [u8]> {
        let mut buf = Vec::with_capacity(keys.len() * 8);
        // Convert u64 to i64 for delta encoding
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
        // Convert i64 back to u64
        i64_coder::read_delta(&data)
            .iter()
            .map(|&k| k as u64)
            .collect()
    }

    fn deserialize_value<'a>(&self, data: &'a [u8]) -> Result<RoaringBitmap, Box<dyn Error>> {
        Ok(RoaringBitmap::deserialize_from(data)?)
    }
}

impl UnionLeafSerializer<RoaringBitmap> for BitmapUnionAccumulator {
    fn add_value(&self, value: &RoaringBitmap) {
        *self.current_union.lock().unwrap() |= value;
    }

    fn release(&self) -> RoaringBitmap {
        std::mem::replace(
            &mut *self.current_union.lock().unwrap(),
            RoaringBitmap::new(),
        )
    }
}

#[test]
fn test_timestamp_range_query() {
    println!("\n╔══════════════════════════════════════════════════════════════════════════════╗");
    println!("║              时间序列数据 Range 查询测试 (10小时数据)                        ║");
    println!("╚══════════════════════════════════════════════════════════════════════════════╝\n");

    println!("📊 测试场景:");
    println!("  - 数据类型: 时间戳 -> 文档ID集合 (RoaringBitmap)");
    println!("  - 时间跨度: {} 小时", HOURS);
    println!("  - 总记录数: ~{} 条 (每毫秒1-2条)", TOTAL_MS * 3 / 2);
    println!("  - 文档ID范围: 0-{}", DOC_ID_RANGE);
    println!();

    // 生成测试数据：每毫秒1-2条记录
    println!("🔄 生成测试数据...");
    let mut tree = BTree::new(32);
    let mut expected_data = std::collections::HashMap::new();

    let start_time = std::time::Instant::now();

    // 使用简单的伪随机数生成器，确保可重现
    let mut seed = 12345u64;
    let mut total_records = 0;

    for ms in 0..TOTAL_MS {
        // 每毫秒生成1-2条记录
        let records_per_ms = 1 + (seed % 2) as usize;
        seed = seed.wrapping_mul(1103515245).wrapping_add(12345);

        for _ in 0..records_per_ms {
            let timestamp = ms;

            // 为这个时间戳生成一个包含5-10个文档ID的bitmap
            let mut bitmap = RoaringBitmap::new();
            let num_docs = 5 + (seed % 6) as u32;
            seed = seed.wrapping_mul(1103515245).wrapping_add(12345);

            for _ in 0..num_docs {
                let doc_id = (seed % DOC_ID_RANGE as u64) as u32;
                bitmap.insert(doc_id);
                seed = seed.wrapping_mul(1103515245).wrapping_add(12345);
            }

            // 存储到tree和expected_data
            tree.put(timestamp, bitmap.clone());
            expected_data
                .entry(timestamp)
                .and_modify(|e: &mut RoaringBitmap| *e |= bitmap.clone())
                .or_insert(bitmap);

            total_records += 1;
        }

        // 每小时打印一次进度
        if ms > 0 && ms % MS_PER_HOUR == 0 {
            println!(
                "  ✓ 已生成 {} 小时数据，共 {} 条记录",
                ms / MS_PER_HOUR,
                total_records
            );
        }
    }

    let gen_duration = start_time.elapsed();
    println!("  ✅ 数据生成完成！");
    println!("     - 总记录数: {}", total_records);
    println!("     - 不同时间戳: {}", expected_data.len());
    println!("     - 内存树大小: {}", tree.len());
    println!("     - 生成耗时: {:?}", gen_duration);
    println!();

    // 持久化数据
    println!("💾 持久化数据到磁盘...");
    let dir = PathBuf::from("/tmp/timestamp_range_test");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let persist_start = std::time::Instant::now();
    TreeWriter::new(dir.clone(), 128)
        .persist::<u64, RoaringBitmap, RoaringBitmap>(
            tree.len(),
            Box::new(BitmapSerializer),
            Some(Box::new(BitmapUnionAccumulator::new())),
            tree.iter(),
        )
        .unwrap();
    let persist_duration = persist_start.elapsed();
    println!("  ✅ 持久化完成，耗时: {:?}", persist_duration);
    println!();

    drop(tree); // 释放内存树

    // 读取数据
    let reader = TreeReader::new(&dir, Box::new(BitmapSerializer)).unwrap();
    println!("📖 数据读取统计:");
    println!("  - Reader中的记录数: {}", reader.len());
    println!("  - 支持 union_leaf: {}", reader.has_union_leaf());
    println!();

    // 验证数据完整性
    println!("🔍 验证数据完整性...");
    let full_count = reader.range(&0, &TOTAL_MS).count();
    println!("  - range(0, {}) 返回: {} 条", TOTAL_MS, full_count);
    println!("  - 期望: {} 条", expected_data.len());
    assert_eq!(full_count, expected_data.len(), "数据完整性检查失败！");
    println!("  ✅ 数据完整性验证通过！");
    println!();

    // 测试不同范围的查询
    println!("╔══════════════════════════════════════════════════════════════════════════════╗");
    println!("║                           Range 查询准确性测试                                ║");
    println!("╚══════════════════════════════════════════════════════════════════════════════╝\n");

    let test_cases = vec![
        ("1秒", 0, 1000),
        ("10秒", 0, 10_000),
        ("1分钟", 0, 60_000),
        ("10分钟", 0, 600_000),
        ("1小时", 0, MS_PER_HOUR),
        ("5小时", 0, 5 * MS_PER_HOUR),
        ("全部10小时", 0, TOTAL_MS),
    ];

    println!("测试场景                起始时间戳          结束时间戳          期望记录数      实际记录数      状态");
    println!(
        "─────────────────────────────────────────────────────────────────────────────────────"
    );

    for (name, start, end) in test_cases {
        // 计算期望的记录数
        let expected_count = expected_data
            .keys()
            .filter(|&&ts| ts >= start && ts < end)
            .count();

        // 实际查询
        let actual_count = reader.range(&start, &end).count();

        // 验证
        let status = if actual_count == expected_count {
            "✅"
        } else {
            "❌"
        };
        println!(
            "{:<20} {:>15} {:>15} {:>15} {:>15}      {}",
            name, start, end, expected_count, actual_count, status
        );

        assert_eq!(
            actual_count, expected_count,
            "范围 [{}, {}) 查询结果不匹配！期望 {}, 实际 {}",
            start, end, expected_count, actual_count
        );
    }

    println!();
    println!("✅ 所有范围查询测试通过！");
    println!();

    // 性能对比测试
    println!("╔══════════════════════════════════════════════════════════════════════════════╗");
    println!("║                      Range Union 性能对比测试                                 ║");
    println!("╚══════════════════════════════════════════════════════════════════════════════╝\n");

    let perf_cases = vec![
        ("1秒", 0, 1000),
        ("1分钟", 0, 60_000),
        ("10分钟", 0, 600_000),
        ("1小时", 0, MS_PER_HOUR),
        ("5小时", 0, 5 * MS_PER_HOUR),
    ];

    println!("范围           迭代+手动union      range_union优化     加速比");
    println!("───────────────────────────────────────────────────────────");

    for (name, start, end) in perf_cases {
        // 方法1: range() + 手动 union
        let iter_start = std::time::Instant::now();
        let mut result1 = RoaringBitmap::new();
        let mut count1 = 0;
        for item in reader.range(&start, &end) {
            result1 |= item.1.clone();
            count1 += 1;
        }
        let iter_duration = iter_start.elapsed();

        // 方法2: range_union 优化
        let union_start = std::time::Instant::now();
        let result2 = reader.range_union(&start, &end).unwrap();
        let union_duration = union_start.elapsed();

        // 验证结果一致性
        eprintln!(
            "[{}] range迭代了{}条记录, 手动union doc_ids={}, range_union doc_ids={}",
            name,
            count1,
            result1.len(),
            result2.len()
        );

        assert_eq!(
            result1.len(),
            result2.len(),
            "range_union 结果与手动union不一致！range: {} vs union: {}",
            result1.len(),
            result2.len()
        );

        let speedup = iter_duration.as_nanos() as f64 / union_duration.as_nanos() as f64;
        println!(
            "{:<12} {:>15} {:>15}     {:>6.1}x",
            name,
            format!("{:?}", iter_duration),
            format!("{:?}", union_duration),
            speedup
        );
    }

    println!();
    println!("✅ 所有性能测试通过！");
    println!();

    // 边界情况测试
    println!("╔══════════════════════════════════════════════════════════════════════════════╗");
    println!("║                           边界情况测试                                        ║");
    println!("╚══════════════════════════════════════════════════════════════════════════════╝\n");

    // 空范围
    let empty_count = reader.range(&100, &100).count();
    assert_eq!(empty_count, 0, "空范围应该返回0条记录");
    println!("  ✅ 空范围查询: range(100, 100) = {} 条", empty_count);

    // 单个时间戳
    let single_count = reader.range(&1000, &1001).count();
    println!("  ✅ 单时间戳查询: range(1000, 1001) = {} 条", single_count);

    // 超出范围
    let beyond_end = TOTAL_MS + 1000;
    let beyond_count = reader.range(&TOTAL_MS, &beyond_end).count();
    assert_eq!(beyond_count, 0, "超出范围应该返回0条记录");
    println!(
        "  ✅ 超出范围查询: range({}, {}) = {} 条",
        TOTAL_MS, beyond_end, beyond_count
    );

    // 反向范围（start > end）
    let reverse_count = reader.range(&1000, &100).count();
    assert_eq!(reverse_count, 0, "反向范围应该返回0条记录");
    println!("  ✅ 反向范围查询: range(1000, 100) = {} 条", reverse_count);

    println!();
    println!("✅ 所有边界测试通过！");
    println!();

    println!("╔══════════════════════════════════════════════════════════════════════════════╗");
    println!("║                          🎉 所有测试通过！                                    ║");
    println!("╚══════════════════════════════════════════════════════════════════════════════╝");
}
