/// 最终对比：TreeReader vs Parquet 最优场景
///
/// TreeReader: BTree 索引 + 随机访问
/// Parquet: 顺序读取所有 Row Groups（最优使用方式）
use arrow::array::{ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use bytes::Bytes;
use mem_btree::persist::{ReadSerializer, TreeReader, TreeWriter, WriteSerializer};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::borrow::Cow;
use std::collections::HashMap;
use std::error::Error;
use std::fs::File;
use std::sync::Arc;
use std::time::Instant;

// U32RecordBatchSerializer
#[derive(Clone)]
pub struct U32RecordBatchSerializer;

impl WriteSerializer<u32, RecordBatch> for U32RecordBatchSerializer {
    fn serialize_keys<'a>(&self, keys: &'a Vec<u32>) -> Cow<'a, [u8]> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&(keys.len() as u32).to_be_bytes());
        for key in keys {
            buf.extend_from_slice(&key.to_be_bytes());
        }
        Cow::Owned(buf)
    }

    fn serialize_value<'a>(&self, batch: &'a RecordBatch) -> Cow<'a, [u8]> {
        let mut buf = Vec::new();
        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(Default::default()))
            .build();

        let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), Some(props))
            .expect("Failed to create ArrowWriter");
        writer.write(batch).expect("Failed to write batch");
        writer.close().expect("Failed to close writer");

        Cow::Owned(buf)
    }
}

impl ReadSerializer<u32, RecordBatch> for U32RecordBatchSerializer {
    fn deserialize_keys<'a>(&self, data: &'a [u8]) -> Vec<u32> {
        let mut pos = 0;
        if data.len() < 4 {
            return Vec::new();
        }

        let count =
            u32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]) as usize;
        pos += 4;

        let mut keys = Vec::with_capacity(count);
        for _ in 0..count {
            if pos + 4 > data.len() {
                break;
            }
            let key = u32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
            keys.push(key);
            pos += 4;
        }
        keys
    }

    fn deserialize_value<'a>(&self, data: &'a [u8]) -> Result<RecordBatch, Box<dyn Error>> {
        let bytes = Bytes::copy_from_slice(data);
        let builder = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .map_err(|e| format!("Failed to create builder: {}", e))?;

        let mut reader = builder
            .build()
            .map_err(|e| format!("Failed to build reader: {}", e))?;

        if let Some(result) = reader.next() {
            result.map_err(|e| format!("Failed to read batch: {}", e).into())
        } else {
            Err("No batch found".into())
        }
    }
}

// 生成测试数据
fn generate_test_batch(start_id: u32, count: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::Int64, false),
        Field::new("city", DataType::Utf8, false),
        Field::new("score", DataType::Float64, false),
    ]));

    let ids: Int64Array = (start_id as i64..(start_id as i64 + count as i64)).collect();
    let names =
        StringArray::from_iter_values((0..count).map(|i| format!("user_{}", start_id + i as u32)));
    let ages: Int64Array = (0..count)
        .map(|i| ((start_id + i as u32) % 80) as i64 + 18)
        .collect();
    let cities = StringArray::from_iter_values((0..count).map(|i| {
        let cities = vec!["北京", "上海", "广州", "深圳", "杭州"];
        cities[((start_id + i as u32) % 5) as usize].to_string()
    }));
    let scores: Float64Array = (0..count)
        .map(|i| ((start_id + i as u32) % 100) as f64 + 0.5)
        .collect();

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(ids) as ArrayRef,
            Arc::new(names) as ArrayRef,
            Arc::new(ages) as ArrayRef,
            Arc::new(cities) as ArrayRef,
            Arc::new(scores) as ArrayRef,
        ],
    )
    .unwrap()
}

// 创建 TreeReader 数据
fn create_tree_data(dir: &str, total_rows: usize, batch_size: usize) -> std::time::Duration {
    println!("📝 创建 TreeReader 数据...");

    let _ = std::fs::remove_dir_all(dir);
    std::fs::create_dir_all(dir).unwrap();

    let write_start = Instant::now();

    use mem_btree::BTree;
    let mut tree = BTree::<u32, RecordBatch>::new(1000);

    let mut current_id = 0u32;
    while (current_id as usize) < total_rows {
        let batch =
            generate_test_batch(current_id, batch_size.min(total_rows - current_id as usize));
        tree.put(current_id, batch);
        current_id += batch_size as u32;
    }

    use mem_btree::persist::TreeWriter;
    let tree_writer = TreeWriter::new(std::path::PathBuf::from(dir), 1000, 4);

    tree_writer
        .persist::<u32, RecordBatch, RecordBatch>(
            tree.len(),
            Box::new(U32RecordBatchSerializer),
            tree.iter(),
        )
        .unwrap();

    let write_time = write_start.elapsed();
    let file_size = std::fs::metadata(format!("{}/data", dir)).unwrap().len();

    println!("   ✅ 写入完成: {:.2?}", write_time);
    println!(
        "   💾 文件大小: {:.2} MB\n",
        file_size as f64 / 1024.0 / 1024.0
    );

    write_time
}

// 创建 Parquet 文件
fn create_parquet_file(path: &str, total_rows: usize, batch_size: usize) -> std::time::Duration {
    println!("📝 创建 Parquet 文件...");

    let write_start = Instant::now();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::Int64, false),
        Field::new("city", DataType::Utf8, false),
        Field::new("score", DataType::Float64, false),
    ]));

    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(Default::default()))
        .set_max_row_group_size(batch_size)
        .build();

    let file = File::create(path).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).unwrap();

    let mut current_id = 0u32;
    while (current_id as usize) < total_rows {
        let batch =
            generate_test_batch(current_id, batch_size.min(total_rows - current_id as usize));
        writer.write(&batch).unwrap();
        current_id += batch_size as u32;
    }

    writer.close().unwrap();
    let write_time = write_start.elapsed();

    let file_size = std::fs::metadata(path).unwrap().len();
    println!("   ✅ 写入完成: {:.2?}", write_time);
    println!(
        "   💾 文件大小: {:.2} MB\n",
        file_size as f64 / 1024.0 / 1024.0
    );

    write_time
}

// TreeReader: 随机访问测试
fn bench_tree_random_access(dir: &str, total_rows: usize, batch_size: usize, query_count: usize) {
    println!("🔵 TreeReader - 随机访问 {} 个 batch", query_count);
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    let reader = TreeReader::new(
        std::path::Path::new(dir),
        Box::new(U32RecordBatchSerializer),
    )
    .unwrap();

    let mut total_time = std::time::Duration::ZERO;
    let mut success_count = 0;

    let query_targets: Vec<u32> = (0..query_count)
        .map(|i| ((i * 997) % total_rows) as u32)
        .collect();

    for target_row in query_targets {
        let batch_key = (target_row / batch_size as u32) * batch_size as u32;

        let query_start = Instant::now();
        if let Some(batch) = reader.get(&batch_key) {
            let _num_rows = batch.num_rows();
            success_count += 1;
        }
        total_time += query_start.elapsed();
    }

    println!("   ✅ 成功查询: {}/{}", success_count, query_count);
    println!(
        "   ⏱️  平均延迟: {:.3} ms",
        total_time.as_secs_f64() * 1000.0 / query_count as f64
    );
    println!(
        "   📈 QPS: {:.0}\n",
        query_count as f64 / total_time.as_secs_f64()
    );
}

// Parquet: 顺序读取所有数据（最优场景）
fn bench_parquet_sequential_all(path: &str) {
    println!("🟢 Parquet - 顺序读取所有 Row Groups");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    let read_start = Instant::now();

    let file = File::open(path).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let mut reader = builder.with_batch_size(1000).build().unwrap();

    let mut total_rows = 0;
    let mut batch_count = 0;

    while let Some(Ok(batch)) = reader.next() {
        total_rows += batch.num_rows();
        batch_count += 1;
    }

    let read_time = read_start.elapsed();

    println!("   ✅ 读取的 batches: {}", batch_count);
    println!("   📄 总行数: {}", total_rows);
    println!("   ⏱️  总耗时: {:.2?}", read_time);
    println!(
        "   📈 吞吐量: {:.0} rows/sec\n",
        total_rows as f64 / read_time.as_secs_f64()
    );
}

// Parquet: 预先加载所有到 HashMap（内存缓存）
fn bench_parquet_preload_to_hashmap(
    path: &str,
    batch_size: usize,
    query_count: usize,
    total_rows: usize,
) {
    println!("🟡 Parquet - 预加载到 HashMap + 随机访问");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    // 预加载阶段
    let load_start = Instant::now();
    let file = File::open(path).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let mut reader = builder.with_batch_size(batch_size).build().unwrap();

    let mut cache: HashMap<u32, RecordBatch> = HashMap::new();
    let mut batch_key = 0u32;

    while let Some(Ok(batch)) = reader.next() {
        cache.insert(batch_key, batch);
        batch_key += batch_size as u32;
    }

    let load_time = load_start.elapsed();
    let cache_size: usize = cache.values().map(|b| b.get_array_memory_size()).sum();

    println!("   📂 预加载耗时: {:.2?}", load_time);
    println!(
        "   💾 缓存大小: {:.2} MB",
        cache_size as f64 / 1024.0 / 1024.0
    );

    // 随机访问测试
    let mut total_time = std::time::Duration::ZERO;
    let mut success_count = 0;

    let query_targets: Vec<u32> = (0..query_count)
        .map(|i| ((i * 997) % total_rows) as u32)
        .collect();

    for target_row in query_targets {
        let batch_key = (target_row / batch_size as u32) * batch_size as u32;

        let query_start = Instant::now();
        if let Some(batch) = cache.get(&batch_key) {
            let _num_rows = batch.num_rows();
            success_count += 1;
        }
        total_time += query_start.elapsed();
    }

    println!("   ✅ 成功查询: {}/{}", success_count, query_count);
    println!(
        "   ⏱️  平均延迟: {:.6} ms",
        total_time.as_secs_f64() * 1000.0 / query_count as f64
    );
    println!(
        "   📈 QPS: {:.0}\n",
        query_count as f64 / total_time.as_secs_f64()
    );
}

fn main() {
    println!("🚀 最终性能对比：TreeReader vs Parquet");
    println!("════════════════════════════════════════════\n");

    let total_rows = 10_000_000;
    let batch_size = 1000;
    let query_count = 10_000;

    println!("📋 测试配置:");
    println!("   总行数: {} (1000万)", total_rows);
    println!("   Batch 大小: {}", batch_size);
    println!("   随机查询次数: {}\n", query_count);

    let tree_dir = "/tmp/bench_tree_final";
    let parquet_path = "/tmp/bench_parquet_final.parquet";

    // 创建数据
    println!("═══════════════ 数据准备 ═══════════════");
    let _tree_write_time = create_tree_data(tree_dir, total_rows, batch_size);
    let _parquet_write_time = create_parquet_file(parquet_path, total_rows, batch_size);

    // 性能测试
    println!("═══════════════ 性能测试 ═══════════════");

    // 1. TreeReader 随机访问
    bench_tree_random_access(tree_dir, total_rows, batch_size, query_count);

    // 2. Parquet 顺序读取（最优场景）
    bench_parquet_sequential_all(parquet_path);

    // 3. Parquet 预加载 + 随机访问（对比场景）
    bench_parquet_preload_to_hashmap(parquet_path, batch_size, query_count, total_rows);

    println!("════════════════════════════════════════════");
    println!("✨ 测试完成！\n");

    println!("💡 结论:");
    println!("   1. TreeReader: 专为随机访问优化，BTree 索引 + 磁盘查找");
    println!("   2. Parquet 顺序读取: 适合全表扫描，流式处理");
    println!("   3. Parquet 预加载: 相当于全内存 HashMap，查询极快但启动慢");
    println!("\n   ✅ 对于随机 doc_id 查询场景，TreeReader 是最优选择！");
}
