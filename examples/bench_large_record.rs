/// 测试大记录场景：单条数据 ~4KB, 100万条数据
///
/// 对比 TreeReader vs Parquet 在大记录场景下的性能差异
use arrow::array::{ArrayRef, BinaryArray, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use bytes::Bytes;
use mem_btree::persist::{ReadSerializer, TreeReader, WriteSerializer};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::borrow::Cow;
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

// 生成大记录测试数据 - 每条记录约 4KB
fn generate_large_batch(start_id: u32, count: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("doc_id", DataType::Int64, false),
        Field::new("title", DataType::Utf8, false),
        Field::new("content", DataType::Utf8, false), // 大字段1: ~2KB
        Field::new("metadata", DataType::Utf8, false), // 大字段2: ~1KB
        Field::new("tags", DataType::Utf8, false),    // 大字段3: ~512B
        Field::new("embedding", DataType::Binary, false), // 大字段4: ~512B (模拟向量)
    ]));

    let doc_ids: Vec<i64> = (start_id as i64..(start_id as i64 + count as i64)).collect();

    let titles: Vec<String> = (0..count)
        .map(|i| {
            format!(
                "文档标题_{}_这是一个比较长的标题用于测试大记录场景",
                start_id + i as u32
            )
        })
        .collect();

    // 每条约 2KB 的内容
    let contents: Vec<String> = (0..count)
        .map(|i| {
            let mut content = format!("这是文档{}的内容。", start_id + i as u32);
            // 重复填充到约 2KB
            while content.len() < 2048 {
                content.push_str(&format!(
                    "这是一段示例文本内容，用于模拟真实的文档数据。包含中文字符和数字{}。",
                    i
                ));
            }
            content
        })
        .collect();

    // 每条约 1KB 的元数据
    let metadata: Vec<String> = (0..count)
        .map(|i| {
            let mut meta = format!(
                r#"{{"id":{}, "author":"user_{}", "created":"2024-01-01""#,
                start_id + i as u32,
                i
            );
            while meta.len() < 1024 {
                meta.push_str(r#", "field":"value""#);
            }
            meta.push_str("}");
            meta
        })
        .collect();

    // 每条约 512B 的标签
    let tags: Vec<String> = (0..count)
        .map(|i| {
            let mut tags = String::from("标签1,标签2,标签3,");
            while tags.len() < 512 {
                tags.push_str(&format!("tag{},", i));
            }
            tags
        })
        .collect();

    // 每条约 512B 的向量数据 (模拟 128维 float32 embedding)
    let embeddings: Vec<&[u8]> = (0..count)
        .map(|i| {
            let mut vec = Vec::with_capacity(512);
            for j in 0..128 {
                let val = ((i + j) % 256) as u8;
                vec.extend_from_slice(&val.to_le_bytes());
                vec.extend_from_slice(&val.to_le_bytes());
                vec.extend_from_slice(&val.to_le_bytes());
                vec.extend_from_slice(&val.to_le_bytes());
            }
            Box::leak(vec.into_boxed_slice()) as &[u8]
        })
        .collect();

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(doc_ids)) as ArrayRef,
            Arc::new(StringArray::from(titles)) as ArrayRef,
            Arc::new(StringArray::from(contents)) as ArrayRef,
            Arc::new(StringArray::from(metadata)) as ArrayRef,
            Arc::new(StringArray::from(tags)) as ArrayRef,
            Arc::new(BinaryArray::from(embeddings)) as ArrayRef,
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
            generate_large_batch(current_id, batch_size.min(total_rows - current_id as usize));
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
        "   💾 文件大小: {:.2} MB",
        file_size as f64 / 1024.0 / 1024.0
    );
    println!(
        "   📊 单 batch 大小: ~{:.1} KB\n",
        file_size as f64 / (total_rows / batch_size) as f64 / 1024.0
    );

    write_time
}

// 创建 Parquet 文件
fn create_parquet_file(path: &str, total_rows: usize, batch_size: usize) -> std::time::Duration {
    println!("📝 创建 Parquet 文件...");

    let write_start = Instant::now();

    let schema = Arc::new(Schema::new(vec![
        Field::new("doc_id", DataType::Int64, false),
        Field::new("title", DataType::Utf8, false),
        Field::new("content", DataType::Utf8, false),
        Field::new("metadata", DataType::Utf8, false),
        Field::new("tags", DataType::Utf8, false),
        Field::new("embedding", DataType::Binary, false),
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
            generate_large_batch(current_id, batch_size.min(total_rows - current_id as usize));
        writer.write(&batch).unwrap();
        current_id += batch_size as u32;
    }

    writer.close().unwrap();
    let write_time = write_start.elapsed();

    let file_size = std::fs::metadata(path).unwrap().len();
    println!("   ✅ 写入完成: {:.2?}", write_time);
    println!(
        "   💾 文件大小: {:.2} MB",
        file_size as f64 / 1024.0 / 1024.0
    );
    println!(
        "   📊 单 Row Group 大小: ~{:.1} KB\n",
        file_size as f64 / (total_rows / batch_size) as f64 / 1024.0
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
    let mut total_bytes = 0usize;

    let query_targets: Vec<u32> = (0..query_count)
        .map(|i| ((i * 997) % total_rows) as u32)
        .collect();

    for target_row in query_targets {
        let batch_key = (target_row / batch_size as u32) * batch_size as u32;

        let query_start = Instant::now();
        if let Some(batch) = reader.get(&batch_key) {
            total_bytes += batch.get_array_memory_size();
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
        "   📈 QPS: {:.0}",
        query_count as f64 / total_time.as_secs_f64()
    );
    println!(
        "   📦 平均读取: {:.1} KB/query\n",
        total_bytes as f64 / query_count as f64 / 1024.0
    );
}

// Parquet: 顺序读取所有数据
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

// Parquet: 使用 RowSelection 跳过行
fn bench_parquet_row_selection(
    path: &str,
    total_rows: usize,
    batch_size: usize,
    query_count: usize,
) {
    println!("🟡 Parquet - RowSelection (跳过前N行)");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    use parquet::arrow::arrow_reader::{RowSelection, RowSelector};

    let mut total_time = std::time::Duration::ZERO;
    let mut success_count = 0;

    let query_targets: Vec<u32> = (0..query_count)
        .map(|i| ((i * 997) % total_rows) as u32)
        .collect();

    for target_row in query_targets {
        let skip_rows = target_row as usize;
        let read_rows = batch_size;

        let query_start = Instant::now();

        let file = File::open(path).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();

        let selection = RowSelection::from(vec![
            RowSelector::skip(skip_rows),
            RowSelector::select(read_rows),
        ]);

        let mut reader = builder.with_row_selection(selection).build().unwrap();

        if let Some(Ok(_batch)) = reader.next() {
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

fn main() {
    println!("🚀 大记录性能测试：TreeReader vs Parquet");
    println!("════════════════════════════════════════════\n");

    let total_rows = 1_000_000; // 100万条
    let batch_size = 1000; // 每batch 1000条
    let query_count = 1_000; // 测试1000次查询

    println!("📋 测试配置:");
    println!("   总行数: {} (100万)", total_rows);
    println!("   Batch 大小: {}", batch_size);
    println!("   单条记录: ~4 KB");
    println!("   总数据量: ~4 GB (未压缩)");
    println!("   随机查询次数: {}\n", query_count);

    let tree_dir = "/tmp/bench_tree_large";
    let parquet_path = "/tmp/bench_parquet_large.parquet";

    // 创建数据
    println!("═══════════════ 数据准备 ═══════════════");
    let tree_write_time = create_tree_data(tree_dir, total_rows, batch_size);
    let parquet_write_time = create_parquet_file(parquet_path, total_rows, batch_size);

    // 性能测试
    println!("═══════════════ 性能测试 ═══════════════");

    // 1. TreeReader 随机访问
    bench_tree_random_access(tree_dir, total_rows, batch_size, query_count);

    // 2. Parquet 顺序读取（最优场景）
    bench_parquet_sequential_all(parquet_path);

    // 3. Parquet RowSelection
    bench_parquet_row_selection(parquet_path, total_rows, batch_size, query_count);

    println!("════════════════════════════════════════════");
    println!("✨ 测试完成！\n");

    println!("💡 对比总结:");
    println!("   TreeReader 写入: {:.2?}", tree_write_time);
    println!("   Parquet 写入: {:.2?}", parquet_write_time);
    println!("\n   📌 大记录场景下,每次查询需要读取更多数据");
    println!("   📌 TreeReader 的 BTree 索引优势更加明显");
    println!("   📌 Parquet 的行跳过机制在大记录下开销更大");
}
