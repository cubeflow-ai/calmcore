use arrow::array::{Array, Int32Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use parquet::basic::{BrotliLevel, Compression, GzipLevel, ZstdLevel};
use parquet::file::properties::WriterProperties;
use std::fs;
use std::sync::Arc;
use std::time::Instant;

#[test]
fn test_parquet_with_90_percent_nulls() {
    let total_rows = 1_000_000;

    // 创建 schema
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("value", DataType::Int32, true),
        Field::new("description", DataType::Utf8, true),
    ]));

    // 生成数据：90% 为空，但空值不连续分布
    let mut id_data = Vec::with_capacity(total_rows);
    let mut name_data = Vec::with_capacity(total_rows);
    let mut value_data = Vec::with_capacity(total_rows);
    let mut desc_data = Vec::with_capacity(total_rows);

    // 使用简单的取模方式让空值和非空值交替分布
    // 每10行中有1行非空，9行为空

    // 每10行中第1行有数据
    let i = 0;
    id_data.push(Some(i as i32));
    name_data.push(Some(format!("name_{}", i)));
    value_data.push(Some(i as i32 * 100));
    desc_data.push(Some(format!("description for item {}", i)));
    for i in 0..total_rows {
        if i % 10 == 0 {
            // 每10行中第1行有数据
            id_data.push(Some(i as i32));
            name_data.push(Some(format!("name_{}", i)));
            value_data.push(Some(i as i32 * 100));
            desc_data.push(Some(format!("description for item {}", i)));
        } else {
            // 其余9行为空
            id_data.push(None);
            name_data.push(None);
            value_data.push(None);
            desc_data.push(None);
        }
    }

    // 创建 Arrow Arrays
    let id_array = Arc::new(Int32Array::from(id_data)) as arrow::array::ArrayRef;
    let name_array = Arc::new(StringArray::from(name_data)) as arrow::array::ArrayRef;
    let value_array = Arc::new(Int32Array::from(value_data)) as arrow::array::ArrayRef;
    let desc_array = Arc::new(StringArray::from(desc_data)) as arrow::array::ArrayRef;

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![id_array, name_array, value_array, desc_array],
    )
    .unwrap();

    // 测试不同压缩算法
    let compressions = vec![
        ("uncompressed", Compression::UNCOMPRESSED),
        ("snappy", Compression::SNAPPY),
        ("gzip", Compression::GZIP(GzipLevel::try_new(3).unwrap())),
        (
            "brotli",
            Compression::BROTLI(BrotliLevel::try_new(3).unwrap()),
        ),
        ("lz4", Compression::LZ4),
        ("zstd", Compression::ZSTD(ZstdLevel::try_new(6).unwrap())),
    ];

    println!("\n=== Parquet 空值压缩测试 (100万行, 90%空值) ===\n");

    for (name, compression) in compressions {
        let file_path = format!("/tmp/test_parquet_{}.parquet", name);

        // 开始计时
        let start = Instant::now();

        // 写入 Parquet
        let file = fs::File::create(&file_path).unwrap();
        let props = WriterProperties::builder()
            .set_compression(compression)
            .build();

        let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        // 计算耗时
        let duration = start.elapsed();

        // 获取文件大小
        let metadata = fs::metadata(&file_path).unwrap();
        let size_mb = metadata.len() as f64 / (1024.0 * 1024.0);

        println!(
            "压缩算法: {:12} | 文件大小: {:6.2} MB | 耗时: {:7.2} ms",
            name,
            size_mb,
            duration.as_secs_f64() * 1000.0
        );

        // 清理测试文件
        fs::remove_file(&file_path).unwrap();
    }

    println!("\n提示: 空值在 Parquet 中占用空间极小");
}

#[test]
fn test_recordbatch_null_count() {
    let total_rows = 1_000_000;
    let non_null_rows = 100_000;

    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int32,
        true,
    )]));

    let mut value_data = Vec::with_capacity(total_rows);
    for i in 0..total_rows {
        // 每10行中有1行非空
        if i % 10 == 0 {
            value_data.push(Some(i as i32));
        } else {
            value_data.push(None);
        }
    }

    let value_array = Arc::new(Int32Array::from(value_data));
    let batch =
        RecordBatch::try_new(schema, vec![value_array.clone() as arrow::array::ArrayRef]).unwrap();

    println!("\n=== RecordBatch 空值统计 ===");
    println!("总行数: {}", batch.num_rows());
    println!("空值数量: {}", value_array.null_count());
    println!(
        "空值比例: {:.1}%",
        (value_array.null_count() as f64 / batch.num_rows() as f64) * 100.0
    );
}
