/// 测试 Parquet 索引机制能否用于行号随机访问
///
/// Parquet 有以下索引机制:
/// 1. Page Index (Column Index + Offset Index) - Parquet 2.0 特性
/// 2. Row Group Index - 基于 Row Group 的粗粒度索引
/// 3. Statistics - 每个 Row Group/Page 的统计信息
use arrow::array::{ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::sync::Arc;
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🔍 测试 Parquet 索引机制\n");

    let path = "/tmp/test_parquet_index.parquet";
    let total_rows = 1_000_000;
    let batch_size = 1000;

    // 1. 写入测试数据
    println!("📝 写入 {} 行数据...", total_rows);
    create_parquet_file(path, total_rows, batch_size)?;

    // 2. 测试不同的查询方式
    println!("\n═══════════════ 索引机制测试 ═══════════════\n");

    // 方式1: 使用 RowSelection 跳过不需要的行
    test_row_selection(path, total_rows, batch_size)?;

    // 方式2: 使用 Page Index (如果支持)
    test_page_index(path)?;

    // 方式3: 直接跳过 Row Groups
    test_skip_row_groups(path, total_rows, batch_size)?;

    Ok(())
}

fn create_parquet_file(
    path: &str,
    total_rows: usize,
    batch_size: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("row_id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]));

    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(Default::default()))
        .set_max_row_group_size(batch_size) // 每个 Row Group 1000 行
        .build();

    let file = File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))?;

    let mut current = 0;
    while current < total_rows {
        let count = batch_size.min(total_rows - current);
        let batch = generate_batch(current as i64, count);
        writer.write(&batch)?;
        current += count;
    }

    writer.close()?;
    println!("   ✅ 写入完成");

    Ok(())
}

fn generate_batch(start_id: i64, count: usize) -> RecordBatch {
    let row_ids: Vec<i64> = (start_id..start_id + count as i64).collect();
    let names: Vec<String> = (0..count)
        .map(|i| format!("row_{}", start_id + i as i64))
        .collect();
    let values: Vec<f64> = (0..count)
        .map(|i| (start_id + i as i64) as f64 * 1.5)
        .collect();

    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("row_id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ])),
        vec![
            Arc::new(Int64Array::from(row_ids)) as ArrayRef,
            Arc::new(StringArray::from(names)) as ArrayRef,
            Arc::new(Float64Array::from(values)) as ArrayRef,
        ],
    )
    .unwrap()
}

fn test_row_selection(
    path: &str,
    total_rows: usize,
    batch_size: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("🔵 方式1: 使用 RowSelection API");
    println!("   说明: Arrow-RS 提供的 RowSelection 可以跳过不需要的行\n");

    use parquet::arrow::arrow_reader::RowSelection;
    use parquet::arrow::arrow_reader::RowSelector;

    let query_count = 100;
    let mut total_time = std::time::Duration::ZERO;

    for i in 0..query_count {
        let target_row = ((i * 9973) % total_rows) as usize; // 随机行号
        let target_row_group = target_row / batch_size;
        let offset_in_group = target_row % batch_size;

        let start = Instant::now();

        let file = File::open(path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;

        // 创建 RowSelection: 跳过前面的行,只读取目标行
        let skip_rows = target_row;
        let read_rows = 1; // 只读1行

        let selection = RowSelection::from(vec![
            RowSelector::skip(skip_rows),
            RowSelector::select(read_rows),
        ]);

        let mut reader = builder.with_row_selection(selection).build()?;

        if let Some(_batch) = reader.next() {
            // 成功读取
        }

        total_time += start.elapsed();
    }

    let avg = total_time.as_secs_f64() / query_count as f64;
    println!("   ⏱️  平均延迟: {:.3} ms", avg * 1000.0);
    println!(
        "   📈 QPS: {:.0}\n",
        query_count as f64 / total_time.as_secs_f64()
    );

    Ok(())
}

fn test_page_index(path: &str) -> Result<(), Box<dyn std::error::Error>> {
    println!("🟢 方式2: Page Index (Column Index + Offset Index)");
    println!("   说明: Parquet 2.0 的 Page-level 索引,可以跳过 Page\n");

    use parquet::file::reader::FileReader;

    let file = File::open(path)?;
    let reader = parquet::file::serialized_reader::SerializedFileReader::new(file)?;
    let metadata = reader.metadata();

    // 检查是否有 Column Index
    let has_column_index = metadata.row_groups().iter().any(|rg| {
        rg.columns().iter().any(|col| {
            // 检查是否有 column_index_offset
            col.column_index_offset().is_some()
        })
    });

    // 检查是否有 Offset Index
    let has_offset_index = metadata.row_groups().iter().any(|rg| {
        rg.columns()
            .iter()
            .any(|col| col.offset_index_offset().is_some())
    });

    println!(
        "   📊 Column Index: {}",
        if has_column_index {
            "✅ 支持"
        } else {
            "❌ 不支持"
        }
    );
    println!(
        "   📊 Offset Index: {}",
        if has_offset_index {
            "✅ 支持"
        } else {
            "❌ 不支持"
        }
    );

    if !has_column_index && !has_offset_index {
        println!("   ⚠️  当前文件没有启用 Page Index\n");
        println!("   💡 Page Index 主要用于谓词下推(predicate pushdown),不是为行号查询设计\n");
    }

    Ok(())
}

fn test_skip_row_groups(
    path: &str,
    total_rows: usize,
    batch_size: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("🟡 方式3: 直接跳过 Row Groups");
    println!("   说明: 利用 Row Group 粒度的跳过,但仍需读取整个 Row Group\n");

    let query_count = 100;
    let mut total_time = std::time::Duration::ZERO;

    for i in 0..query_count {
        let target_row = ((i * 9973) % total_rows) as usize;
        let target_row_group = target_row / batch_size;

        let start = Instant::now();

        let file = File::open(path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;

        // 只读取包含目标行的 Row Group
        let mut reader = builder.with_row_groups(vec![target_row_group]).build()?;

        // 需要遍历整个 Row Group 才能找到目标行
        if let Some(_batch) = reader.next() {
            // Row Group 有 1000 行,我们只需要其中1行
            // 但无法避免读取整个 Row Group
        }

        total_time += start.elapsed();
    }

    let avg = total_time.as_secs_f64() / query_count as f64;
    println!("   ⏱️  平均延迟: {:.3} ms", avg * 1000.0);
    println!(
        "   📈 QPS: {:.0}\n",
        query_count as f64 / total_time.as_secs_f64()
    );

    Ok(())
}
