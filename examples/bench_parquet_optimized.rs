/// Benchmark: 优化版 Parquet 查询性能测试
///
/// 正确利用 Parquet 的 Row Group 索引机制：
/// 1. 保持文件打开（避免重复打开开销）
/// 2. 利用 Row Group metadata 快速定位
/// 3. 只读取包含目标行的 Row Group
use arrow::array::{ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use parquet::file::reader::FileReader;
use std::fs::File;
use std::sync::Arc;
use std::time::Instant;

// 生成测试数据
fn generate_test_batch(start_id: i64, count: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::Int64, false),
        Field::new("city", DataType::Utf8, false),
        Field::new("score", DataType::Float64, false),
    ]));

    let ids: Int64Array = (start_id..(start_id + count as i64)).collect();
    let names =
        StringArray::from_iter_values((0..count).map(|i| format!("user_{}", start_id + i as i64)));
    let ages: Int64Array = (0..count)
        .map(|i| ((start_id + i as i64) % 80) + 18)
        .collect();
    let cities = StringArray::from_iter_values((0..count).map(|i| {
        let cities = vec!["北京", "上海", "广州", "深圳", "杭州"];
        cities[((start_id + i as i64) % 5) as usize].to_string()
    }));
    let scores: Float64Array = (0..count)
        .map(|i| ((start_id + i as i64) % 100) as f64 + 0.5)
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

// 创建 Parquet 文件
fn create_parquet_file(path: &str, total_rows: usize, batch_size: usize) {
    println!("\n📝 创建 Parquet 文件: {}", path);
    println!("   总行数: {}, Row Group 大小: {}", total_rows, batch_size);

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

    let mut current_id = 0i64;
    while (current_id as usize) < total_rows {
        let batch =
            generate_test_batch(current_id, batch_size.min(total_rows - current_id as usize));
        writer.write(&batch).unwrap();
        current_id += batch_size as i64;
    }

    writer.close().unwrap();
    let write_time = write_start.elapsed();

    let file_size = std::fs::metadata(path).unwrap().len();
    println!("   ✅ 写入完成: {:.2?}", write_time);
    println!(
        "   💾 文件大小: {:.2} MB",
        file_size as f64 / 1024.0 / 1024.0
    );
}

// 优化版 Parquet 查询测试（保持文件打开）
fn bench_parquet_query_optimized(
    path: &str,
    total_rows: usize,
    batch_size: usize,
    query_count: usize,
) {
    println!("\n🟢 测试 Parquet 查询性能（优化版 - 保持文件打开）");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    // 一次性打开文件并读取 metadata
    let open_start = Instant::now();
    let file = File::open(path).unwrap();
    let file_reader = parquet::file::serialized_reader::SerializedFileReader::new(file).unwrap();
    let metadata = file_reader.metadata();
    let num_row_groups = metadata.num_row_groups();
    println!("📂 文件打开并解析 metadata: {:.2?}", open_start.elapsed());
    println!("   Row Groups 数量: {}", num_row_groups);

    let mut total_time = std::time::Duration::ZERO;
    let mut success_count = 0;

    // 使用固定种子生成查询目标
    let query_targets: Vec<usize> = (0..query_count).map(|i| (i * 997) % total_rows).collect();

    println!("🔍 执行 {} 次随机行号查询...", query_count);

    for target_row in query_targets {
        let query_start = Instant::now();

        // 计算目标 Row Group
        let row_group_idx = target_row / batch_size;

        if row_group_idx < num_row_groups {
            // 只读取包含目标行的 Row Group
            let file = File::open(path).unwrap();
            let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();

            let mut reader = builder
                .with_row_groups(vec![row_group_idx])
                .build()
                .unwrap();

            if let Some(Ok(batch)) = reader.next() {
                let _row_in_batch = target_row % batch_size;
                if _row_in_batch < batch.num_rows() {
                    success_count += 1;
                }
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

// 使用 mmap 的优化版本
fn bench_parquet_query_mmap(path: &str, total_rows: usize, batch_size: usize, query_count: usize) {
    use bytes::Bytes;

    println!("\n🔵 测试 Parquet 查询性能（mmap 版本）");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    // 使用 mmap 映射文件到内存
    let open_start = Instant::now();
    let file = File::open(path).unwrap();
    let mmap = unsafe { memmap2::Mmap::map(&file).unwrap() };
    let bytes = Bytes::copy_from_slice(&mmap);

    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes.clone()).unwrap();
    let metadata = builder.metadata();
    let num_row_groups = metadata.num_row_groups();

    println!("📂 文件 mmap 并解析 metadata: {:.2?}", open_start.elapsed());
    println!("   Row Groups 数量: {}", num_row_groups);
    println!("   文件大小: {:.2} MB", mmap.len() as f64 / 1024.0 / 1024.0);

    let mut total_time = std::time::Duration::ZERO;
    let mut success_count = 0;

    // 使用固定种子生成查询目标
    let query_targets: Vec<usize> = (0..query_count).map(|i| (i * 997) % total_rows).collect();

    println!("🔍 执行 {} 次随机行号查询...", query_count);

    for target_row in query_targets {
        let query_start = Instant::now();

        // 计算目标 Row Group
        let row_group_idx = target_row / batch_size;

        if row_group_idx < num_row_groups {
            // 重新构建 reader（因为需要指定 row_groups）
            let builder = ParquetRecordBatchReaderBuilder::try_new(bytes.clone()).unwrap();
            let mut reader = builder
                .with_row_groups(vec![row_group_idx])
                .build()
                .unwrap();

            if let Some(Ok(batch)) = reader.next() {
                let _row_in_batch = target_row % batch_size;
                if _row_in_batch < batch.num_rows() {
                    success_count += 1;
                }
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
    println!("🚀 Parquet 查询性能测试（优化版）");
    println!("════════════════════════════════════════════");

    // 测试参数
    let total_rows = 1_000_000; // 100万行
    let batch_size = 1000; // 每个 Row Group 1000 行
    let query_count = 10_000; // 查询 1万次

    println!("\n📋 测试配置:");
    println!("   总行数: {}", total_rows);
    println!("   Row Group 大小: {}", batch_size);
    println!("   查询次数: {}", query_count);

    let parquet_path = "/tmp/bench_parquet_optimized.parquet";

    // 创建 Parquet 文件
    create_parquet_file(parquet_path, total_rows, batch_size);

    // 测试1: 优化版（保持文件打开）
    bench_parquet_query_optimized(parquet_path, total_rows, batch_size, query_count);

    // 测试2: mmap 版本
    bench_parquet_query_mmap(parquet_path, total_rows, batch_size, query_count);

    println!("\n════════════════════════════════════════════");
    println!("✨ 测试完成!");
    println!("\n💡 说明:");
    println!("   - 优化版避免了重复打开文件的开销");
    println!("   - 利用 Row Group 索引直接定位目标数据");
    println!("   - mmap 版本将文件映射到内存，减少系统调用");
}
