/// 验证索引完整性的工具
///
/// 通过读取 Parquet 文件和倒排索引，验证：
/// 1. Parquet 中每行的值是否在对应的倒排索引中
/// 2. 倒排索引中的 doc_id 是否指向正确的 Parquet 行
///
/// 使用方法：
///   cargo run --bin verify_index <table_name> [partition_name] [segment_name]
use calm::segment::field_store::{
    F64RoaringSerializer, I64RoaringSerializer, OrderedF64, StringRoaringSerializer,
};
use datafusion::arrow::array::{Array, ArrayRef};
use datafusion::arrow::datatypes::DataType;
use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use mem_btree::persist;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::path::{Path, PathBuf};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!(
            "使用方法: verify_index <table_name> [data_path] [partition_name] [segment_name]"
        );
        eprintln!("\n示例:");
        eprintln!("  verify_index taxi_trips");
        eprintln!("  verify_index taxi_trips ./data");
        eprintln!("  verify_index taxi_trips ./data partition-0000000000000000000");
        eprintln!("  verify_index taxi_trips ./data partition-0000000000000000000 segment-0-50402");
        std::process::exit(1);
    }

    let table_name = &args[1];

    // 检测第二个参数是 data_path 还是 partition_name
    let (data_path, partition_filter, segment_filter) =
        if args.len() >= 3 && !args[2].starts_with("partition-") {
            // 第二个参数是 data_path
            (
                args[2].clone(),
                args.get(3).map(|s| s.as_str()),
                args.get(4).map(|s| s.as_str()),
            )
        } else {
            // 第二个参数是 partition_name 或没有额外参数
            (
                "data".to_string(),
                args.get(2).map(|s| s.as_str()),
                args.get(3).map(|s| s.as_str()),
            )
        };

    println!("╔═══════════════════════════════════════════════════════════════════════╗");
    println!("║                    索引完整性验证工具                                   ║");
    println!("╚═══════════════════════════════════════════════════════════════════════╝");
    println!("📊 表名: {}", table_name);
    println!("📁 数据路径: {}", data_path);
    if let Some(p) = partition_filter {
        println!("📂 Partition: {}", p);
    }
    if let Some(s) = segment_filter {
        println!("📦 Segment: {}", s);
    }
    println!();

    // 查找表目录
    let table_path = PathBuf::from(format!("{}/tables/{}", data_path, table_name));
    if !table_path.exists() {
        eprintln!("❌ 表目录不存在: {:?}", table_path);
        std::process::exit(1);
    }

    let partitions_path = table_path.join("partitions");
    if !partitions_path.exists() {
        eprintln!("❌ partitions 目录不存在: {:?}", partitions_path);
        std::process::exit(1);
    }

    // 遍历所有 partitions
    let mut partition_dirs: Vec<_> = std::fs::read_dir(&partitions_path)?
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().ok().map(|t| t.is_dir()).unwrap_or(false))
        .filter(|e| e.file_name().to_string_lossy().starts_with("partition-"))
        .collect();
    partition_dirs.sort_by_key(|e| e.file_name());

    let mut total_segments = 0;
    let mut total_verified = 0;
    let mut total_errors = 0;

    for partition_entry in partition_dirs {
        let partition_name = partition_entry.file_name();
        let partition_name_str = partition_name.to_string_lossy();

        // 过滤 partition
        if let Some(filter) = partition_filter {
            if partition_name_str != filter {
                continue;
            }
        }

        println!("\n📂 验证 Partition: {}", partition_name_str);
        println!("{}", "─".repeat(80));

        let partition_path = partition_entry.path();

        // 遍历该 partition 下的所有 segments
        let mut segment_dirs: Vec<_> = std::fs::read_dir(&partition_path)?
            .filter_map(|e| e.ok())
            .filter(|e| e.file_type().ok().map(|t| t.is_dir()).unwrap_or(false))
            .filter(|e| e.file_name().to_string_lossy().starts_with("segment-"))
            .collect();
        segment_dirs.sort_by_key(|e| e.file_name());

        for segment_entry in segment_dirs {
            let segment_name = segment_entry.file_name();
            let segment_name_str = segment_name.to_string_lossy();

            // 过滤 segment
            if let Some(filter) = segment_filter {
                if segment_name_str != filter {
                    continue;
                }
            }

            total_segments += 1;
            let segment_path = segment_entry.path();

            println!("\n  📦 Segment: {}", segment_name_str);

            match verify_segment(&segment_path, table_name).await {
                Ok(stats) => {
                    total_verified += stats.verified_docs;
                    total_errors += stats.errors;

                    if stats.errors == 0 {
                        println!("     ✅ 验证通过: {} 行数据", stats.total_docs);
                    } else {
                        println!(
                            "     ❌ 发现 {} 个错误 ({} 行数据)",
                            stats.errors, stats.total_docs
                        );
                    }
                }
                Err(e) => {
                    println!("     ❌ 验证失败: {}", e);
                    total_errors += 1;
                }
            }
        }
    }

    // 最终统计
    println!("\n");
    println!("╔═══════════════════════════════════════════════════════════════════════╗");
    println!("║                          验证总结                                      ║");
    println!("╚═══════════════════════════════════════════════════════════════════════╝");
    println!("  📊 验证的 Segment 数量: {}", total_segments);
    println!("  📄 验证的文档数量: {}", total_verified);
    if total_errors == 0 {
        println!("  ✅ 状态: 全部通过");
    } else {
        println!("  ❌ 错误数量: {}", total_errors);
    }
    println!();

    if total_errors > 0 {
        std::process::exit(1);
    }

    Ok(())
}

#[derive(Debug, Default)]
struct VerificationStats {
    total_docs: usize,
    verified_docs: usize,
    errors: usize,
}

async fn verify_segment(
    segment_path: &Path,
    _table_name: &str,
) -> Result<VerificationStats, Box<dyn std::error::Error>> {
    let mut stats = VerificationStats::default();

    // 1. 读取 Parquet 文件
    let rowdata_path = segment_path.join("rowdata").join("rowdata.parquet");
    if !rowdata_path.exists() {
        return Err(format!("Parquet 文件不存在: {:?}", rowdata_path).into());
    }

    let file = File::open(&rowdata_path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let mut reader = builder.build()?;

    let mut all_batches = Vec::new();
    while let Some(batch) = reader.next() {
        all_batches.push(batch?);
    }

    if all_batches.is_empty() {
        return Ok(stats);
    }

    let first_batch = &all_batches[0];
    let schema = first_batch.schema();
    stats.total_docs = all_batches.iter().map(|b| b.num_rows()).sum();

    println!(
        "     📄 Parquet: {} 行, {} 列",
        stats.total_docs,
        schema.fields().len()
    );

    // 2. 收集所有有索引的字段
    let mut fields_to_verify: Vec<(String, DataType)> = Vec::new();
    for field in schema.fields() {
        let field_name = field.name();
        let data_type = field.data_type();

        // 检查索引文件是否存在
        let index_path = segment_path.join(format!("field-{}", field_name));
        if index_path.exists() {
            fields_to_verify.push((field_name.clone(), data_type.clone()));
        }
    }

    if fields_to_verify.is_empty() {
        println!("     ⚠️  没有找到可验证的字段");
        return Ok(stats);
    }

    println!("     🔍 验证 {} 个字段", fields_to_verify.len());

    // 3. 验证每个字段
    for (field_name, data_type) in fields_to_verify {
        let field_stats = verify_field(segment_path, &field_name, &data_type, &all_batches)?;

        stats.errors += field_stats.errors;

        if field_stats.errors == 0 {
            println!("        ✅ {} ({:?}): 通过", field_name, data_type);
        } else {
            println!(
                "        ❌ {} ({:?}): {} 个错误",
                field_name, data_type, field_stats.errors
            );
        }
    }

    stats.verified_docs = stats.total_docs;

    Ok(stats)
}

#[derive(Debug, Default)]
struct FieldVerificationStats {
    errors: usize,
}

fn verify_field(
    segment_path: &Path,
    field_name: &str,
    data_type: &DataType,
    all_batches: &[datafusion::arrow::record_batch::RecordBatch],
) -> Result<FieldVerificationStats, Box<dyn std::error::Error>> {
    // 根据数据类型分发到不同的验证函数
    match data_type {
        DataType::Utf8 | DataType::LargeUtf8 => {
            verify_string_field(segment_path, field_name, all_batches)
        }
        DataType::Float64 => verify_float64_field(segment_path, field_name, all_batches),
        DataType::Int64
        | DataType::UInt64
        | DataType::Int32
        | DataType::UInt32
        | DataType::Int16
        | DataType::UInt16
        | DataType::Int8
        | DataType::UInt8
        | DataType::Timestamp(_, _) => verify_int64_field(segment_path, field_name, all_batches),
        _ => Err(format!("不支持的数据类型: {:?}", data_type).into()),
    }
}

// String 字段验证
fn verify_string_field(
    segment_path: &Path,
    field_name: &str,
    all_batches: &[datafusion::arrow::record_batch::RecordBatch],
) -> Result<FieldVerificationStats, Box<dyn std::error::Error>> {
    use datafusion::arrow::array::StringArray;

    let mut stats = FieldVerificationStats::default();
    let mut value_to_doc_ids: HashMap<String, Vec<u32>> = HashMap::new();
    let mut doc_id = 0u32;

    // 收集 Parquet 中的值
    for batch in all_batches {
        let column = batch.column_by_name(field_name).ok_or("字段不存在")?;
        let string_array = column
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or("无法转换为 StringArray")?;

        for row_idx in 0..string_array.len() {
            if !string_array.is_null(row_idx) {
                let value = string_array.value(row_idx).to_string();
                value_to_doc_ids.entry(value).or_default().push(doc_id);
            }
            doc_id += 1;
        }
    }

    // 读取索引
    let index_path = segment_path.join(format!("field-{}", field_name));
    let serializer = StringRoaringSerializer::new(3);
    let tree_reader = persist::TreeReader::new(&index_path, Box::new(serializer))?;

    verify_index_match(&tree_reader, &value_to_doc_ids, &mut stats)
}

// Float64 字段验证
fn verify_float64_field(
    segment_path: &Path,
    field_name: &str,
    all_batches: &[datafusion::arrow::record_batch::RecordBatch],
) -> Result<FieldVerificationStats, Box<dyn std::error::Error>> {
    use datafusion::arrow::array::Float64Array;

    let mut stats = FieldVerificationStats::default();
    let mut value_to_doc_ids: HashMap<OrderedF64, Vec<u32>> = HashMap::new();
    let mut doc_id = 0u32;

    // 收集 Parquet 中的值
    for batch in all_batches {
        let column = batch.column_by_name(field_name).ok_or("字段不存在")?;
        let float_array = column
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or("无法转换为 Float64Array")?;

        for row_idx in 0..float_array.len() {
            if !float_array.is_null(row_idx) {
                let value = OrderedF64::from(float_array.value(row_idx));
                value_to_doc_ids.entry(value).or_default().push(doc_id);
            }
            doc_id += 1;
        }
    }

    // 读取索引
    let index_path = segment_path.join(format!("field-{}", field_name));
    let serializer = F64RoaringSerializer::new(3);
    let tree_reader = persist::TreeReader::new(&index_path, Box::new(serializer))?;

    verify_index_match_no_display(&tree_reader, &value_to_doc_ids, &mut stats)
}

// Int64 字段验证
fn verify_int64_field(
    segment_path: &Path,
    field_name: &str,
    all_batches: &[datafusion::arrow::record_batch::RecordBatch],
) -> Result<FieldVerificationStats, Box<dyn std::error::Error>> {
    let mut stats = FieldVerificationStats::default();
    let mut value_to_doc_ids: HashMap<i64, Vec<u32>> = HashMap::new();
    let mut doc_id = 0u32;

    // 收集 Parquet 中的值
    for batch in all_batches {
        let column = batch.column_by_name(field_name).ok_or("字段不存在")?;

        for row_idx in 0..column.len() {
            if !column.is_null(row_idx) {
                let value = extract_i64_value(column, row_idx)?;
                value_to_doc_ids.entry(value).or_default().push(doc_id);
            }
            doc_id += 1;
        }
    }

    // 读取索引
    let index_path = segment_path.join(format!("field-{}", field_name));
    let serializer = I64RoaringSerializer::new(3);
    let tree_reader = persist::TreeReader::new(&index_path, Box::new(serializer))?;

    verify_index_match(&tree_reader, &value_to_doc_ids, &mut stats)
}

// 通用的索引匹配验证（带 Display trait）
fn verify_index_match<K>(
    tree_reader: &persist::TreeReader<K, roaring::RoaringBitmap>,
    value_to_doc_ids: &HashMap<K, Vec<u32>>,
    stats: &mut FieldVerificationStats,
) -> Result<FieldVerificationStats, Box<dyn std::error::Error>>
where
    K: Clone + Ord + std::hash::Hash + Eq + std::fmt::Display,
{
    let mut missing_values = 0;
    let mut mismatch_values = 0;
    let mut total_missing_docs = 0;
    let mut total_extra_docs = 0;

    for (value, expected_doc_ids) in value_to_doc_ids {
        match tree_reader.get(value) {
            Some(bitmap) => {
                let expected_set: HashSet<u32> = expected_doc_ids.iter().copied().collect();
                let actual_set: HashSet<u32> = bitmap.iter().collect();

                // 如果集合不完全相等，算一个错误
                if expected_set != actual_set {
                    mismatch_values += 1;

                    let missing: Vec<u32> = expected_set.difference(&actual_set).copied().collect();
                    let extra: Vec<u32> = actual_set.difference(&expected_set).copied().collect();

                    total_missing_docs += missing.len();
                    total_extra_docs += extra.len();

                    // 只打印前几个样例
                    if mismatch_values <= 3 {
                        println!(
                            "           样例 {}: value={}, expected_docs={}, actual_docs={}, missing={}, extra={}",
                            mismatch_values,
                            value,
                            expected_set.len(),
                            actual_set.len(),
                            missing.len(),
                            extra.len()
                        );
                    }
                }
            }
            None => {
                missing_values += 1;
            }
        }
    }

    // 反向验证：检查每个 doc_id 是否都能在索引中找到
    let mut all_expected_docs: HashSet<u32> = HashSet::new();
    let mut all_indexed_docs: HashSet<u32> = HashSet::new();

    for expected_doc_ids in value_to_doc_ids.values() {
        all_expected_docs.extend(expected_doc_ids.iter().copied());
    }

    for (value, _) in value_to_doc_ids {
        if let Some(bitmap) = tree_reader.get(value) {
            all_indexed_docs.extend(bitmap.iter());
        }
    }

    let docs_not_indexed: Vec<u32> = all_expected_docs
        .difference(&all_indexed_docs)
        .copied()
        .collect();
    let docs_extra_indexed: Vec<u32> = all_indexed_docs
        .difference(&all_expected_docs)
        .copied()
        .collect();

    stats.errors = missing_values + mismatch_values;

    if stats.errors > 0 || !docs_not_indexed.is_empty() || !docs_extra_indexed.is_empty() {
        println!(
            "           缺失值: {}, 不匹配值: {}, 缺失docs: {}, 多余docs: {}",
            missing_values, mismatch_values, total_missing_docs, total_extra_docs
        );
        if !docs_not_indexed.is_empty() {
            println!(
                "           ⚠️  有 {} 个 doc_id 在 Parquet 中但不在索引中",
                docs_not_indexed.len()
            );
            if docs_not_indexed.len() <= 10 {
                println!("              doc_ids: {:?}", docs_not_indexed);
            }
        }
        if !docs_extra_indexed.is_empty() {
            println!(
                "           ⚠️  有 {} 个 doc_id 在索引中但不在 Parquet 中",
                docs_extra_indexed.len()
            );
            if docs_extra_indexed.len() <= 10 {
                println!("              doc_ids: {:?}", docs_extra_indexed);
            }
        }
    }

    Ok(FieldVerificationStats {
        errors: stats.errors,
    })
}

// 不需要 Display trait 的版本（用于 OrderedF64）
fn verify_index_match_no_display<K>(
    tree_reader: &persist::TreeReader<K, roaring::RoaringBitmap>,
    value_to_doc_ids: &HashMap<K, Vec<u32>>,
    stats: &mut FieldVerificationStats,
) -> Result<FieldVerificationStats, Box<dyn std::error::Error>>
where
    K: Clone + Ord + std::hash::Hash + Eq + std::fmt::Debug,
{
    let mut missing_values = 0;
    let mut mismatch_values = 0;
    let mut total_missing_docs = 0;
    let mut total_extra_docs = 0;

    for (value, expected_doc_ids) in value_to_doc_ids {
        match tree_reader.get(value) {
            Some(bitmap) => {
                let expected_set: HashSet<u32> = expected_doc_ids.iter().copied().collect();
                let actual_set: HashSet<u32> = bitmap.iter().collect();

                if expected_set != actual_set {
                    mismatch_values += 1;
                    let missing: Vec<u32> = expected_set.difference(&actual_set).copied().collect();
                    let extra: Vec<u32> = actual_set.difference(&expected_set).copied().collect();
                    total_missing_docs += missing.len();
                    total_extra_docs += extra.len();

                    if mismatch_values <= 3 {
                        println!(
                            "           样例 {}: value={:?}, expected_docs={}, actual_docs={}, missing={}, extra={}",
                            mismatch_values,
                            value,
                            expected_set.len(),
                            actual_set.len(),
                            missing.len(),
                            extra.len()
                        );
                    }
                }
            }
            None => {
                missing_values += 1;
            }
        }
    }

    // 反向验证：检查每个 doc_id 是否都能在索引中找到
    let mut all_expected_docs: HashSet<u32> = HashSet::new();
    let mut all_indexed_docs: HashSet<u32> = HashSet::new();

    for expected_doc_ids in value_to_doc_ids.values() {
        all_expected_docs.extend(expected_doc_ids.iter().copied());
    }

    for (value, _) in value_to_doc_ids {
        if let Some(bitmap) = tree_reader.get(value) {
            all_indexed_docs.extend(bitmap.iter());
        }
    }

    let docs_not_indexed: Vec<u32> = all_expected_docs
        .difference(&all_indexed_docs)
        .copied()
        .collect();
    let docs_extra_indexed: Vec<u32> = all_indexed_docs
        .difference(&all_expected_docs)
        .copied()
        .collect();

    stats.errors = missing_values + mismatch_values;

    if stats.errors > 0 || !docs_not_indexed.is_empty() || !docs_extra_indexed.is_empty() {
        println!(
            "           缺失值: {}, 不匹配值: {}, 缺失docs: {}, 多余docs: {}",
            missing_values, mismatch_values, total_missing_docs, total_extra_docs
        );
        if !docs_not_indexed.is_empty() {
            println!(
                "           ⚠️  有 {} 个 doc_id 在 Parquet 中但不在索引中",
                docs_not_indexed.len()
            );
            if docs_not_indexed.len() <= 10 {
                println!("              doc_ids: {:?}", docs_not_indexed);
            }
        }
        if !docs_extra_indexed.is_empty() {
            println!(
                "           ⚠️  有 {} 个 doc_id 在索引中但不在 Parquet 中",
                docs_extra_indexed.len()
            );
            if docs_extra_indexed.len() <= 10 {
                println!("              doc_ids: {:?}", docs_extra_indexed);
            }
        }
    }

    Ok(FieldVerificationStats {
        errors: stats.errors,
    })
}

fn extract_i64_value(array: &ArrayRef, idx: usize) -> Result<i64, Box<dyn std::error::Error>> {
    use datafusion::arrow::array::*;

    let value = match array.data_type() {
        DataType::Int8 => array
            .as_any()
            .downcast_ref::<Int8Array>()
            .unwrap()
            .value(idx) as i64,
        DataType::Int16 => array
            .as_any()
            .downcast_ref::<Int16Array>()
            .unwrap()
            .value(idx) as i64,
        DataType::Int32 => array
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(idx) as i64,
        DataType::Int64 => array
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(idx),
        DataType::UInt8 => array
            .as_any()
            .downcast_ref::<UInt8Array>()
            .unwrap()
            .value(idx) as i64,
        DataType::UInt16 => array
            .as_any()
            .downcast_ref::<UInt16Array>()
            .unwrap()
            .value(idx) as i64,
        DataType::UInt32 => array
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap()
            .value(idx) as i64,
        DataType::UInt64 => array
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .value(idx) as i64,
        DataType::Float32 => {
            let val = array
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap()
                .value(idx);
            (val * 1000.0) as i64 // 转换为毫单位
        }
        DataType::Float64 => {
            let val = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(idx);
            (val * 1000.0) as i64 // 转换为毫单位
        }
        DataType::Timestamp(_, _) => array
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .map(|arr| arr.value(idx))
            .or_else(|| {
                array
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .map(|arr| arr.value(idx) / 1000)
            })
            .or_else(|| {
                array
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .map(|arr| arr.value(idx) / 1_000_000)
            })
            .or_else(|| {
                array
                    .as_any()
                    .downcast_ref::<TimestampSecondArray>()
                    .map(|arr| arr.value(idx) * 1000)
            })
            .ok_or("无法解析 Timestamp")?,
        DataType::Utf8 => {
            // 对于字符串，使用哈希值
            let s = array
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(idx);
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};
            let mut hasher = DefaultHasher::new();
            s.hash(&mut hasher);
            hasher.finish() as i64
        }
        DataType::LargeUtf8 => {
            // 对于大字符串，使用哈希值
            let s = array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .unwrap()
                .value(idx);
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};
            let mut hasher = DefaultHasher::new();
            s.hash(&mut hasher);
            hasher.finish() as i64
        }
        _ => return Err(format!("不支持的数据类型: {:?}", array.data_type()).into()),
    };

    Ok(value)
}
