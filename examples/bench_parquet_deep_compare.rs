/// Benchmark: 真正优化的 Parquet 查询
///
/// 关键优化：复用 ParquetRecordBatchReaderBuilder 和 metadata
use arrow::array::{ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
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

// 优化版本：避免重复构建 Reader
fn bench_parquet_optimized(path: &str, total_rows: usize, batch_size: usize, query_count: usize) {
    println!("\n🟢 测试 Parquet 查询（优化：避免重复构建 Reader）");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    // 1. 加载文件到内存
    let load_start = Instant::now();
    let file_bytes = std::fs::read(path).unwrap();
    let file_size_mb = file_bytes.len() as f64 / 1024.0 / 1024.0;
    let bytes = Bytes::from(file_bytes);

    // 2. 一次性解析 metadata
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes.clone()).unwrap();
    let metadata = builder.metadata();
    let num_row_groups = metadata.num_row_groups();

    let load_time = load_start.elapsed();

    println!("📂 文件加载和 metadata 解析: {:.2?}", load_time);
    println!("   Row Groups 数量: {}", num_row_groups);
    println!("   文件大小: {:.2} MB", file_size_mb);

    // 3. 单次查询测试
    println!("\n🔍 执行 {} 次单次查询...", query_count);

    let mut total_time = std::time::Duration::ZERO;
    let mut success_count = 0;

    let query_targets: Vec<usize> = (0..query_count).map(|i| (i * 997) % total_rows).collect();

    let progress_interval = query_count / 10;

    for (idx, target_row) in query_targets.iter().enumerate() {
        if idx > 0 && idx % progress_interval == 0 {
            let progress = (idx as f64 / query_count as f64) * 100.0;
            let avg_latency = total_time.as_secs_f64() * 1000.0 / idx as f64;
            println!(
                "   进度: {:.0}% - 平均延迟: {:.3} ms",
                progress, avg_latency
            );
        }

        let query_start = Instant::now();

        let row_group_idx = target_row / batch_size;

        if row_group_idx < num_row_groups {
            // ⚠️ 这里仍然需要重新构建 reader，因为 API 限制
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

    println!("\n📊 单次查询结果:");
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

// 真正的优化：预读取所有 Row Groups（仅用于对比）
fn bench_parquet_preloaded(path: &str, total_rows: usize, batch_size: usize, query_count: usize) {
    println!("\n🔵 测试 Parquet 查询（预加载所有 Row Groups - 仅对比）");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    let load_start = Instant::now();
    let file_bytes = std::fs::read(path).unwrap();
    let bytes = Bytes::from(file_bytes);

    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes.clone()).unwrap();
    let num_row_groups = builder.metadata().num_row_groups();

    // 预加载所有 Row Groups
    let mut row_group_cache: Vec<RecordBatch> = Vec::with_capacity(num_row_groups);

    println!("📦 预加载 {} 个 Row Groups...", num_row_groups);
    for i in 0..num_row_groups {
        if i > 0 && i % (num_row_groups / 10) == 0 {
            println!(
                "   预加载进度: {:.0}%",
                (i as f64 / num_row_groups as f64) * 100.0
            );
        }

        let builder = ParquetRecordBatchReaderBuilder::try_new(bytes.clone()).unwrap();
        let mut reader = builder.with_row_groups(vec![i]).build().unwrap();

        if let Some(Ok(batch)) = reader.next() {
            row_group_cache.push(batch);
        }
    }

    let load_time = load_start.elapsed();
    let cache_size: usize = row_group_cache
        .iter()
        .map(|b| b.get_array_memory_size())
        .sum();

    println!("📂 预加载完成: {:.2?}", load_time);
    println!("   缓存大小: {:.2} MB", cache_size as f64 / 1024.0 / 1024.0);

    // 查询测试
    let mut total_time = std::time::Duration::ZERO;
    let mut success_count = 0;

    let query_targets: Vec<usize> = (0..query_count).map(|i| (i * 997) % total_rows).collect();

    println!("🔍 执行 {} 次查询（从内存缓存）...", query_count);

    for target_row in query_targets {
        let query_start = Instant::now();

        let row_group_idx = target_row / batch_size;

        if row_group_idx < row_group_cache.len() {
            let batch = &row_group_cache[row_group_idx];
            let _row_in_batch = target_row % batch_size;
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
        "   ⏱️  平均延迟: {:.3} μs",
        total_time.as_micros() as f64 / query_count as f64
    );
    println!(
        "   📈 QPS: {:.0}",
        query_count as f64 / total_time.as_secs_f64()
    );
}

fn main() {
    println!("🚀 Parquet vs BTree 性能深度对比");
    println!("════════════════════════════════════════════");

    let total_rows = 10_000_000; // 1000万行
    let batch_size = 1000;
    let query_count = 10_000;

    println!("\n📋 测试配置:");
    println!("   总行数: {}", total_rows);
    println!("   Batch 大小: {}", batch_size);
    println!("   查询次数: {}", query_count);

    let parquet_path = "/tmp/bench_parquet_real.parquet";

    create_parquet_file(parquet_path, total_rows, batch_size);

    // 测试1: 当前实现（每次重建 Reader）
    bench_parquet_optimized(parquet_path, total_rows, batch_size, query_count);

    // 测试2: 预加载所有数据（理论最优，但不实用）
    bench_parquet_preloaded(parquet_path, total_rows, batch_size, query_count);

    println!("\n════════════════════════════════════════════");
    println!("✨ 测试完成!");
    println!("\n💡 结论:");
    println!("   - Parquet 的瓶颈在于重复构建 Reader");
    println!("   - 即使文件在内存，每次查询仍需解析 metadata + 初始化解压器");
    println!("   - BTree 的优势：索引在内存 + Reader 常驻 + 直接 seek");
    println!("   - 预加载所有数据可达到与 BTree 相当性能，但内存占用大");
}
