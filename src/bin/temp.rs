use datafusion::arrow::array::{Array, StringArray, UInt64Array};
use datafusion::arrow::compute;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::basic::Compression;
use datafusion::parquet::file::properties::WriterProperties;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;

const MAX_FILE_SIZE: u64 = 1024 * 1024 * 1024; // 1GB
const ROWS_PER_GROUP: usize = 1000;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 3 {
        eprintln!("Usage: temp <parquet_directory> <output_directory>");
        std::process::exit(1);
    }

    let parquet_dir = &args[1];
    let output_dir = &args[2];

    // 创建输出目录
    fs::create_dir_all(output_dir)?;

    // 递归收集所有 parquet 文件
    let parquet_files = collect_parquet_files(Path::new(parquet_dir))?;
    println!("Found {} parquet files", parquet_files.len());

    let mut current_writer: Option<ArrowWriter<File>> = None;
    let mut current_output_path: Option<String> = None;
    let mut file_counter = 0;
    let mut total_rows_read = 0u64;
    let mut total_rows_written = 0u64;
    let mut total_rows_filtered = 0u64;

    for parquet_file in parquet_files {
        println!("Processing: {:?}", parquet_file);

        let file = File::open(&parquet_file)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let schema = builder.schema().clone();
        let reader = builder.build()?;

        // 检查是否有 data_path 列
        let data_path_idx = schema.fields().iter().position(|f| f.name() == "data_path");

        for batch_result in reader {
            let batch = batch_result?;
            let original_rows = batch.num_rows();
            total_rows_read += original_rows as u64;

            // 过滤掉 data_path = 'com.jd.jpos.jingbean.rpc.jsf.JingBeanJsfFacade#balanceBeans' 的行
            let filtered_batch = if let Some(idx) = data_path_idx {
                filter_batch(&batch, idx)?
            } else {
                batch
            };

            if filtered_batch.num_rows() == 0 {
                continue;
            }

            total_rows_filtered += (original_rows - filtered_batch.num_rows()) as u64;
            total_rows_written += filtered_batch.num_rows() as u64;

            // 检查当前文件大小是否超过限制
            let need_new_file = if let Some(path) = &current_output_path {
                if let Ok(metadata) = fs::metadata(path) {
                    metadata.len() >= MAX_FILE_SIZE
                } else {
                    false
                }
            } else {
                true
            };

            // 检查是否需要创建新文件
            if current_writer.is_none() || need_new_file {
                if let Some(writer) = current_writer.take() {
                    writer.close()?;
                    if let Some(path) = &current_output_path {
                        let size = fs::metadata(path)?.len();
                        println!("Closed file: {} (size: {} MB)", path, size / 1024 / 1024);
                    }
                }

                file_counter += 1;
                let output_path = format!("{}/filtered_{:04}.parquet", output_dir, file_counter);
                println!("Creating new output file: {}", output_path);

                let output_file = File::create(&output_path)?;
                let props = WriterProperties::builder()
                    .set_max_row_group_size(ROWS_PER_GROUP)
                    .set_compression(Compression::SNAPPY)
                    .build();
                let writer =
                    ArrowWriter::try_new(output_file, filtered_batch.schema(), Some(props))?;
                current_writer = Some(writer);
                current_output_path = Some(output_path);
            }

            // 写入数据
            if let Some(ref mut writer) = current_writer {
                writer.write(&filtered_batch)?;
            }
        }
    }

    // 关闭最后一个文件
    if let Some(writer) = current_writer {
        writer.close()?;
    }

    println!("\n=== Summary ===");
    println!("Total rows read: {}", total_rows_read);
    println!("Total rows filtered: {}", total_rows_filtered);
    println!("Total rows written: {}", total_rows_written);
    println!("Output files created: {}", file_counter);

    Ok(())
}

/// 递归收集所有 parquet 文件，排除 _tmp.parquet 结尾的文件
fn collect_parquet_files(dir: &Path) -> Result<Vec<PathBuf>, Box<dyn std::error::Error>> {
    let mut parquet_files = Vec::new();

    if dir.is_dir() {
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();

            if path.is_dir() {
                parquet_files.extend(collect_parquet_files(&path)?);
            } else if path.is_file() {
                if let Some(file_name) = path.file_name().and_then(|n| n.to_str()) {
                    if file_name.ends_with(".parquet") && !file_name.ends_with("_tmp.parquet") {
                        parquet_files.push(path);
                    }
                }
            }
        }
    }

    Ok(parquet_files)
}

/// 过滤掉 data_path 匹配特定值的行
fn filter_batch(
    batch: &RecordBatch,
    data_path_idx: usize,
) -> Result<RecordBatch, Box<dyn std::error::Error>> {
    let data_path_col = batch.column(data_path_idx);
    let data_path_array = data_path_col
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or("data_path column is not a string array")?;

    // 创建过滤索引
    let mut keep_indices = Vec::new();
    for i in 0..data_path_array.len() {
        if data_path_array.is_null(i) {
            // null 值保留
            keep_indices.push(i);
        } else {
            let value = data_path_array.value(i);
            if value != "com.jd.jpos.jingbean.rpc.jsf.JingBeanJsfFacade#balanceBeans" {
                keep_indices.push(i);
            }
        }
    }

    // 如果没有需要过滤的行，直接返回原 batch
    if keep_indices.len() == batch.num_rows() {
        return Ok(batch.clone());
    }

    // 创建新的列数组
    let filtered_columns: Result<Vec<Arc<dyn Array>>, _> = batch
        .columns()
        .iter()
        .map(|col| {
            let indices =
                UInt64Array::from(keep_indices.iter().map(|&i| i as u64).collect::<Vec<_>>());
            compute::take(col.as_ref(), &indices, None)
        })
        .collect();

    let filtered_columns = filtered_columns?;
    Ok(RecordBatch::try_new(batch.schema(), filtered_columns)?)
}
