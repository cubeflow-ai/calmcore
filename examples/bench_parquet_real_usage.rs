/// Benchmark: 真实的 Parquet 使用场景
///
/// 正确理解：
/// 1. Parquet 文件写入时就分好了 Row Groups
/// 2. Reader 打开一次，可以顺序读取所有 Row Groups
/// 3. 但如果要随机访问指定的 Row Group，需要用 with_row_groups()
/// 4. 不幸的是，每次 with_row_groups() 都需要重新构建 Reader
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

// 场景1: 顺序读取所有 Row Groups（Parquet 的最佳使用场景）
fn bench_parquet_sequential(path: &str, batch_size: usize) {
    println!("\n🟢 场景1: 顺序读取所有数据（Parquet 最优场景）");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    let read_start = Instant::now();

    // 打开文件一次
    let file = File::open(path).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let metadata = builder.metadata();
    let num_row_groups = metadata.num_row_groups();

    println!("   Row Groups 数量: {}", num_row_groups);

    // 构建 reader，顺序读取所有数据
    let mut reader = builder.with_batch_size(batch_size).build().unwrap();

    let mut batch_count = 0;
    let mut total_rows = 0;

    while let Some(Ok(batch)) = reader.next() {
        total_rows += batch.num_rows();
        batch_count += 1;
    }

    let read_time = read_start.elapsed();

    println!("   ✅ 读取完成");
    println!("   总行数: {}", total_rows);
    println!("   Batch 数量: {}", batch_count);
    println!("   耗时: {:.2?}", read_time);
    println!(
        "   吞吐量: {:.0} 行/秒",
        total_rows as f64 / read_time.as_secs_f64()
    );
}

// 场景2: 随机访问指定 Row Groups（需要重新构建 Reader）
fn bench_parquet_random_access(
    path: &str,
    total_rows: usize,
    batch_size: usize,
    query_count: usize,
) {
    println!("\n🔴 场景2: 随机访问指定 Row Groups（性能差）");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    // 预加载文件到内存
    let file_bytes = std::fs::read(path).unwrap();
    let bytes = Bytes::from(file_bytes);

    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes.clone()).unwrap();
    let num_row_groups = builder.metadata().num_row_groups();

    println!("   Row Groups 数量: {}", num_row_groups);
    println!("   ⚠️  每次查询都需要重新构建 Reader");

    let mut total_time = std::time::Duration::ZERO;
    let mut success_count = 0;

    let query_targets: Vec<usize> = (0..query_count).map(|i| (i * 997) % total_rows).collect();

    println!("🔍 执行 {} 次随机 Row Group 访问...", query_count);

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
            // 🔴 关键问题：每次都要重新构建 Reader！
            let builder = ParquetRecordBatchReaderBuilder::try_new(bytes.clone()).unwrap();
            let mut reader = builder
                .with_row_groups(vec![row_group_idx]) // 指定读取哪个 Row Group
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

// 场景3: 预加载所有 Row Groups 到内存（模拟 BTree 的方式）
fn bench_parquet_preloaded(path: &str, total_rows: usize, batch_size: usize, query_count: usize) {
    println!("\n🔵 场景3: 预加载所有 Row Groups（模拟 BTree）");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    let load_start = Instant::now();
    let file_bytes = std::fs::read(path).unwrap();
    let bytes = Bytes::from(file_bytes);

    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes.clone()).unwrap();
    let num_row_groups = builder.metadata().num_row_groups();

    // 预先读取所有 Row Groups
    let mut row_group_cache: Vec<RecordBatch> = Vec::with_capacity(num_row_groups);

    println!("📦 预加载 {} 个 Row Groups...", num_row_groups);

    for i in 0..num_row_groups {
        if i > 0 && i % (num_row_groups / 10).max(1) == 0 {
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

    println!("✅ 预加载完成: {:.2?}", load_time);
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
        "   ⏱️  平均延迟: {:.6} ms",
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
    println!("🚀 Parquet 真实使用场景测试");
    println!("════════════════════════════════════════════");

    let total_rows = 10_000_000; // 1000万行
    let batch_size = 1000;
    let query_count = 10_000;

    println!("\n📋 测试配置:");
    println!("   总行数: {}", total_rows);
    println!("   Row Group 大小: {}", batch_size);
    println!("   查询次数: {}", query_count);

    let parquet_path = "/tmp/bench_parquet_real_usage.parquet";

    create_parquet_file(parquet_path, total_rows, batch_size);

    // 场景1: 顺序读取（Parquet 的设计初衷）
    bench_parquet_sequential(parquet_path, batch_size);

    // 场景2: 随机访问（每次重建 Reader，性能差）
    bench_parquet_random_access(parquet_path, total_rows, batch_size, query_count);

    // 场景3: 预加载（与 BTree 类似，但内存占用大）
    bench_parquet_preloaded(parquet_path, total_rows, batch_size, query_count);

    println!("\n════════════════════════════════════════════");
    println!("✨ 测试完成!");
    println!("\n💡 关键结论:");
    println!("   1. Parquet 文件写入时就分好了 Row Groups");
    println!("   2. 顺序读取性能优秀（场景1）");
    println!("   3. 随机访问需要重建 Reader，性能差（场景2）");
    println!("   4. 预加载可达到 BTree 性能，但内存占用大（场景3）");
    println!("\n🎯 为什么 Parquet 比 BTree 慢？");
    println!("   - Parquet 设计用于顺序扫描，不是随机访问");
    println!("   - 每次随机访问都要重建 Reader（解析 metadata + 初始化解压器）");
    println!("   - BTree 索引常驻内存，Reader 保持打开");
}
