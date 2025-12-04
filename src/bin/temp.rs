use datafusion::arrow::array::{Array, StringArray, UInt64Array};
use datafusion::arrow::compute;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::basic::Compression;
use datafusion::parquet::file::properties::WriterProperties;
use rayon::prelude::*;
use std::collections::HashSet;
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

const MAX_FILE_SIZE: u64 = 1024 * 1024 * 1024; // 1GB
const ROWS_PER_GROUP: usize = 1000;
const MAX_THREADS: usize = 5;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 3 {
        eprintln!("Usage: temp <parquet_directory> <output_directory>");
        std::process::exit(1);
    }

    let parquet_dir = &args[1];
    let output_dir = &args[2];

    // 设置线程池为 5 个线程
    rayon::ThreadPoolBuilder::new()
        .num_threads(MAX_THREADS)
        .build_global()
        .unwrap();

    // 创建输出目录
    fs::create_dir_all(output_dir)?;

    // 进度文件路径
    let progress_file = format!("{}/.progress.txt", output_dir);

    // 读取已处理的文件列表
    let processed_files = Arc::new(Mutex::new(load_processed_files(&progress_file)?));

    // 递归收集所有 parquet 文件
    let all_files = collect_parquet_files(Path::new(parquet_dir))?;
    println!("Found {} parquet files", all_files.len());

    // 过滤出未处理的文件
    let processed = processed_files.lock().unwrap();
    let parquet_files: Vec<_> = all_files
        .into_iter()
        .filter(|f| !processed.contains(&f.to_string_lossy().to_string()))
        .collect();
    drop(processed);

    println!(
        "Need to process {} files (already processed: {})",
        parquet_files.len(),
        processed_files.lock().unwrap().len()
    );

    if parquet_files.is_empty() {
        println!("All files already processed!");
        return Ok(());
    }

    // 使用共享的文件计数器和统计信息
    let file_counter = Arc::new(Mutex::new(0usize));
    let total_rows_read = Arc::new(Mutex::new(0u64));
    let total_rows_written = Arc::new(Mutex::new(0u64));
    let total_rows_filtered = Arc::new(Mutex::new(0u64));

    // 5个并发处理
    let results: Vec<Result<(), String>> = parquet_files
        .par_iter()
        .map(|parquet_file| {
            match process_single_file(
                parquet_file,
                output_dir,
                file_counter.clone(),
                total_rows_read.clone(),
                total_rows_written.clone(),
                total_rows_filtered.clone(),
            ) {
                Ok(_) => {
                    // 记录成功处理的文件
                    if let Err(e) = mark_file_processed(&progress_file, parquet_file) {
                        eprintln!("Failed to mark file as processed: {}", e);
                    }
                    Ok(())
                }
                Err(e) => Err(format!("Error processing {:?}: {}", parquet_file, e)),
            }
        })
        .collect();

    // 检查是否有错误
    for result in results {
        if let Err(e) = result {
            eprintln!("Error: {}", e);
        }
    }

    println!("\n=== Summary ===");
    println!("Total rows read: {}", *total_rows_read.lock().unwrap());
    println!(
        "Total rows filtered: {}",
        *total_rows_filtered.lock().unwrap()
    );
    println!(
        "Total rows written: {}",
        *total_rows_written.lock().unwrap()
    );
    println!("Output files created: {}", *file_counter.lock().unwrap());

    Ok(())
}

/// 处理单个 parquet 文件
fn process_single_file(
    parquet_file: &PathBuf,
    output_dir: &str,
    file_counter: Arc<Mutex<usize>>,
    total_rows_read: Arc<Mutex<u64>>,
    total_rows_written: Arc<Mutex<u64>>,
    total_rows_filtered: Arc<Mutex<u64>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use datafusion::parquet::arrow::ArrowWriter;
    use datafusion::parquet::basic::Compression;
    use datafusion::parquet::file::properties::WriterProperties;
    use std::fs::File;

    println!("Processing: {:?}", parquet_file);

    let file = File::open(parquet_file)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let schema = builder.schema().clone();
    let reader = builder.build()?;

    // 检查是否有 data_path 列
    let data_path_idx = schema.fields().iter().position(|f| f.name() == "data_path");

    let mut current_writer: Option<ArrowWriter<File>> = None;
    let mut current_output_path: Option<String> = None;

    for batch_result in reader {
        let batch = batch_result?;
        let original_rows = batch.num_rows();
        *total_rows_read.lock().unwrap() += original_rows as u64;

        // 过滤掉 data_path = 'com.jd.jpos.jingbean.rpc.jsf.JingBeanJsfFacade#balanceBeans' 的行
        let filtered_batch = if let Some(idx) = data_path_idx {
            filter_batch(&batch, idx)?
        } else {
            batch
        };

        if filtered_batch.num_rows() == 0 {
            continue;
        }

        *total_rows_filtered.lock().unwrap() += (original_rows - filtered_batch.num_rows()) as u64;
        *total_rows_written.lock().unwrap() += filtered_batch.num_rows() as u64;

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

            let counter = {
                let mut c = file_counter.lock().unwrap();
                *c += 1;
                *c
            };

            let output_path = format!("{}/filtered_{:04}.parquet", output_dir, counter);
            println!("Creating new output file: {}", output_path);

            let output_file = File::create(&output_path)?;
            let props = WriterProperties::builder()
                .set_max_row_group_size(ROWS_PER_GROUP)
                .set_compression(Compression::SNAPPY)
                .build();
            let writer = ArrowWriter::try_new(output_file, filtered_batch.schema(), Some(props))?;
            current_writer = Some(writer);
            current_output_path = Some(output_path);
        }

        // 写入数据
        if let Some(ref mut writer) = current_writer {
            writer.write(&filtered_batch)?;
        }
    }

    // 关闭最后一个文件
    if let Some(writer) = current_writer {
        writer.close()?;
    }

    Ok(())
}

/// 加载已处理的文件列表
fn load_processed_files(
    progress_file: &str,
) -> Result<HashSet<String>, Box<dyn std::error::Error>> {
    use std::io::BufRead;

    let mut processed = HashSet::new();

    if let Ok(file) = File::open(progress_file) {
        let reader = BufReader::new(file);
        for line in reader.lines() {
            if let Ok(line) = line {
                processed.insert(line);
            }
        }
    }

    Ok(processed)
}

/// 标记文件为已处理
fn mark_file_processed(
    progress_file: &str,
    file_path: &PathBuf,
) -> Result<(), Box<dyn std::error::Error>> {
    use std::io::Write;

    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(progress_file)?;

    writeln!(file, "{}", file_path.to_string_lossy())?;
    file.flush()?;

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
) -> Result<RecordBatch, Box<dyn std::error::Error + Send + Sync>> {
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
