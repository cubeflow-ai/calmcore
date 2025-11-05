/// Benchmark: TreeReader (BTree 索引) 查询性能测试
///
/// 测试 TreeReader 的随机查询性能，作为 Parquet 方案的对照组
use arrow::array::{ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use bytes::Bytes;
use mem_btree::persist::{ReadSerializer, TreeReader, WriteSerializer};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::borrow::Cow;
use std::error::Error;
use std::sync::Arc;
use std::time::Instant;

// U32RecordBatchSerializer - 复用 segment 中的实现
#[derive(Clone)]
pub struct U32RecordBatchSerializer;

impl U32RecordBatchSerializer {
    pub fn new() -> Self {
        Self
    }
}

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
fn create_tree_data(dir: &str, total_rows: usize, batch_size: usize) {
    println!("\n📝 创建 TreeReader 数据: {}", dir);
    println!("   总行数: {}, Batch 大小: {}", total_rows, batch_size);

    let _ = std::fs::remove_dir_all(dir);
    std::fs::create_dir_all(dir).unwrap();

    let write_start = Instant::now();

    // 使用 BTree 收集数据
    use mem_btree::BTree;
    let mut tree = BTree::<u32, RecordBatch>::new(1000);

    let mut current_id = 0u32;
    while (current_id as usize) < total_rows {
        let batch =
            generate_test_batch(current_id, batch_size.min(total_rows - current_id as usize));
        tree.put(current_id, batch);
        current_id += batch_size as u32;
    }

    // 持久化 - 使用 TreeWriter
    use mem_btree::persist::TreeWriter;
    let persist_start = Instant::now();

    let tree_writer = TreeWriter::new(
        std::path::PathBuf::from(dir),
        1000, // chunk_size
        4,    // key_len (u32 = 4 bytes)
    );

    tree_writer
        .persist::<u32, RecordBatch, RecordBatch>(
            tree.len(),
            Box::new(U32RecordBatchSerializer::new()),
            tree.iter(),
        )
        .unwrap();

    let persist_time = persist_start.elapsed();
    let write_time = write_start.elapsed();

    let file_size = std::fs::metadata(format!("{}/data", dir)).unwrap().len();
    println!(
        "   ✅ 写入完成: {:.2?} (持久化: {:.2?})",
        write_time, persist_time
    );
    println!(
        "   💾 文件大小: {:.2} MB",
        file_size as f64 / 1024.0 / 1024.0
    );
}

// TreeReader 查询测试
fn bench_tree_query(dir: &str, total_rows: usize, batch_size: usize, query_count: usize) {
    println!("\n🔵 测试 TreeReader 查询性能");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    // 打开 TreeReader
    let reader = TreeReader::new(
        std::path::Path::new(dir),
        Box::new(U32RecordBatchSerializer::new()),
    )
    .unwrap();

    let mut total_time = std::time::Duration::ZERO;
    let mut success_count = 0;

    // 使用固定种子生成查询目标
    let query_targets: Vec<u32> = (0..query_count)
        .map(|i| ((i * 997) % total_rows) as u32)
        .collect();

    println!("🔍 执行 {} 次随机行号查询...", query_count);

    for target_row in query_targets {
        let batch_key = (target_row / batch_size as u32) * batch_size as u32;

        let query_start = Instant::now();
        if let Some(batch) = reader.get(&batch_key) {
            // 模拟在 batch 内查找具体行
            let _row_in_batch = (target_row - batch_key) as usize;
            if _row_in_batch < batch.num_rows() {
                success_count += 1;
            }
        }
        total_time += query_start.elapsed();
    }

    println!("\n📊 查询结果:");
    println!("   ✅ 成功: {}/{}", success_count, query_count);
    println!(
        "   ⏱️  平均延迟: {:.3} ms",
        total_time.as_secs_f64() * 1000.0 / query_count as f64
    );
    println!(
        "   📈 QPS: {:.0}",
        query_count as f64 / total_time.as_secs_f64()
    );
}

fn main() {
    println!("🚀 TreeReader 查询性能测试");
    println!("════════════════════════════════════════════");

    // 测试参数 - 1000万行数据
    let total_rows = 10_000_000; // 1000万行
    let batch_size = 1000; // 每个 batch 1000 行
    let query_count = 10_000; // 查询 1万次

    println!("\n📋 测试配置:");
    println!("   总行数: {}", total_rows);
    println!("   Batch 大小: {}", batch_size);
    println!("   查询次数: {}", query_count);

    let tree_dir = "/tmp/bench_tree_reader";

    // 创建 TreeReader 数据
    create_tree_data(tree_dir, total_rows, batch_size);

    // 测试查询性能
    bench_tree_query(tree_dir, total_rows, batch_size, query_count);

    println!("\n════════════════════════════════════════════");
    println!("✨ 测试完成!");
    println!("\n💡 说明:");
    println!("   - TreeReader 使用 BTree 索引快速定位批次");
    println!("   - 每个批次存储为压缩的 RecordBatch (Parquet 格式)");
    println!("   - 可与 Pure Parquet (Row Group 索引) 方案对比");
}
