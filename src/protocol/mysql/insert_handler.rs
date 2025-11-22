/// 高性能 INSERT 语句处理器 - 使用字节级优化解析

use crate::catalog::TableMeta;
use crate::engine::Engine;
use crate::schema::field::FieldOption;
use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema, TimeUnit};
use msql_srv::*;
use std::io;
use std::sync::Arc;

/// 高性能 INSERT 处理
pub async fn handle_insert<W: io::Read + io::Write>(
    engine: Arc<Engine>,
    query: &str,
    results: QueryResultWriter<'_, W>,
) -> io::Result<()> {
    let start_time = std::time::Instant::now();
    eprintln!("⏱️  [INSERT] Starting, SQL length: {} bytes", query.len());

    let parse_start = std::time::Instant::now();
    
    // 快速解析 INSERT 语句
    let (table_name, columns, all_rows) = parse_insert_fast(query)?;
    
    eprintln!(
        "⏱️  [INSERT] Parsed {} rows in {:?}",
        all_rows.len(),
        parse_start.elapsed()
    );

    // 获取表元数据
    let meta = engine.get_table_meta(&table_name).map_err(|e| {
        io::Error::new(
            io::ErrorKind::NotFound,
            format!("Table not found: {}", e),
        )
    })?;

    // 按 partition 分组数据
    let route_start = std::time::Instant::now();
    use std::collections::HashMap;
    let mut partition_data: HashMap<String, Vec<Vec<String>>> = HashMap::new();

    let pk_field = meta.schema.primary_key.as_ref().ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidInput, "Table has no primary key")
    })?;

    let pk_idx = columns
        .iter()
        .position(|c| c.eq_ignore_ascii_case(pk_field))
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "Primary key not in INSERT"))?;

    // 路由每一行到对应的 partition
    for row in all_rows {
        let pk_value = &row[pk_idx];
        let partition_id = engine
            .route_partition(&table_name, pk_value)
            .map_err(|e| io::Error::other(format!("Partition routing failed: {}", e)))?;

        partition_data.entry(partition_id).or_default().push(row);
    }

    eprintln!(
        "⏱️  [INSERT] Routed to {} partitions in {:?}",
        partition_data.len(),
        route_start.elapsed()
    );

    // 对每个 partition 批量插入
    let insert_start = std::time::Instant::now();
    let mut total_inserted = 0u64;

    for (partition_id, rows) in partition_data {
        let batch_build_start = std::time::Instant::now();
        let batch = build_record_batch(&meta, &columns, &rows)?;
        eprintln!(
            "⏱️  [INSERT] Built RecordBatch for partition {} ({} rows) in {:?}",
            partition_id,
            rows.len(),
            batch_build_start.elapsed()
        );

        let partition_get_start = std::time::Instant::now();
        let partition = engine
            .get_partition(&table_name, &partition_id)
            .await
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "Partition not found"))?;
        eprintln!(
            "⏱️  [INSERT] Got partition {} in {:?}",
            partition_id,
            partition_get_start.elapsed()
        );

        let upsert_start = std::time::Instant::now();
        partition
            .upsert(batch)
            .map_err(|e| io::Error::other(format!("Insert failed: {}", e)))?;
        eprintln!(
            "⏱️  [INSERT] Upserted {} rows to partition {} in {:?}",
            rows.len(),
            partition_id,
            upsert_start.elapsed()
        );

        total_inserted += rows.len() as u64;
    }

    eprintln!(
        "⏱️  [INSERT] Total insert time: {:?}, {} rows, {:.0} rows/sec",
        insert_start.elapsed(),
        total_inserted,
        total_inserted as f64 / insert_start.elapsed().as_secs_f64()
    );
    eprintln!("⏱️  [INSERT] Overall time: {:?}\n", start_time.elapsed());

    results.completed(total_inserted, 0)
}

/// 快速解析 INSERT 语句 (字节级优化，避免逐字符迭代)
fn parse_insert_fast(query: &str) -> io::Result<(String, Vec<String>, Vec<Vec<String>>)> {
    let query_bytes = query.as_bytes();
    let len = query_bytes.len();
    
    // 1. 找到表名 - 跳过 "INSERT INTO "
    let mut pos = 0;
    while pos < len - 6 {
        if query_bytes[pos..pos + 6].eq_ignore_ascii_case(b"INSERT") {
            pos += 6;
            break;
        }
        pos += 1;
    }
    
    while pos < len && query_bytes[pos].is_ascii_whitespace() {
        pos += 1;
    }
    
    if pos + 4 <= len && query_bytes[pos..pos + 4].eq_ignore_ascii_case(b"INTO") {
        pos += 4;
    }
    
    while pos < len && query_bytes[pos].is_ascii_whitespace() {
        pos += 1;
    }
    
    let table_start = pos;
    while pos < len && query_bytes[pos] != b'(' && !query_bytes[pos].is_ascii_whitespace() {
        pos += 1;
    }
    let table_name = String::from_utf8_lossy(&query_bytes[table_start..pos]).trim().to_string();
    
    // 2. 找到列名
    while pos < len && query_bytes[pos] != b'(' {
        pos += 1;
    }
    pos += 1; // 跳过 '('
    
    let cols_start = pos;
    let mut depth = 1;
    while pos < len && depth > 0 {
        if query_bytes[pos] == b'(' {
            depth += 1;
        } else if query_bytes[pos] == b')' {
            depth -= 1;
        }
        if depth > 0 {
            pos += 1;
        }
    }
    
    let cols_str = String::from_utf8_lossy(&query_bytes[cols_start..pos]);
    let columns: Vec<String> = cols_str
        .split(',')
        .map(|s| s.trim().to_string())
        .collect();
    
    pos += 1; // 跳过 ')'
    
    // 3. 找到 VALUES
    while pos < len - 6 {
        if query_bytes[pos..pos + 6].eq_ignore_ascii_case(b"VALUES") {
            pos += 6;
            break;
        }
        pos += 1;
    }
    
    while pos < len && query_bytes[pos].is_ascii_whitespace() {
        pos += 1;
    }
    
    // 4. 解析所有行 (优化：使用字节切片)
    let mut all_rows = Vec::new();
    
    while pos < len {
        // 跳过空白和逗号
        while pos < len && (query_bytes[pos].is_ascii_whitespace() || query_bytes[pos] == b',') {
            pos += 1;
        }
        
        if pos >= len || query_bytes[pos] == b';' {
            break;
        }
        
        if query_bytes[pos] != b'(' {
            break;
        }
        pos += 1;
        
        let row_start = pos;
        
        // 找到对应的 ')' - 处理引号
        let mut depth = 1;
        while pos < len && depth > 0 {
            match query_bytes[pos] {
                b'(' => depth += 1,
                b')' => depth -= 1,
                b'\'' | b'"' => {
                    let quote = query_bytes[pos];
                    pos += 1;
                    while pos < len {
                        if query_bytes[pos] == b'\\' && pos + 1 < len {
                            pos += 2;
                        } else if query_bytes[pos] == quote {
                            pos += 1;
                            break;
                        } else {
                            pos += 1;
                        }
                    }
                    continue;
                }
                _ => {}
            }
            if depth > 0 {
                pos += 1;
            }
        }
        
        let row_str = String::from_utf8_lossy(&query_bytes[row_start..pos]);
        let row_values = parse_row_values(&row_str)?;
        
        if row_values.len() != columns.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "Column count ({}) doesn't match value count ({})",
                    columns.len(),
                    row_values.len()
                ),
            ));
        }
        
        all_rows.push(row_values);
        pos += 1;
    }
    
    if all_rows.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "No values found in INSERT",
        ));
    }
    
    Ok((table_name, columns, all_rows))
}

/// 解析一行的值 (优化版本 - 字节级处理)
fn parse_row_values(row_str: &str) -> io::Result<Vec<String>> {
    let mut values = Vec::new();
    let bytes = row_str.as_bytes();
    let len = bytes.len();
    let mut pos = 0;
    let mut current = Vec::new();
    
    while pos < len {
        while pos < len && bytes[pos].is_ascii_whitespace() {
            pos += 1;
        }
        
        if pos >= len {
            break;
        }
        
        if bytes[pos] == b'\'' || bytes[pos] == b'"' {
            let quote = bytes[pos];
            pos += 1;
            while pos < len {
                if bytes[pos] == b'\\' && pos + 1 < len {
                    current.push(bytes[pos + 1]);
                    pos += 2;
                } else if bytes[pos] == quote {
                    pos += 1;
                    break;
                } else {
                    current.push(bytes[pos]);
                    pos += 1;
                }
            }
        } else {
            while pos < len && bytes[pos] != b',' {
                if !bytes[pos].is_ascii_whitespace() {
                    current.push(bytes[pos]);
                }
                pos += 1;
            }
        }
        
        values.push(String::from_utf8_lossy(&current).trim().to_string());
        current.clear();
        
        while pos < len && (bytes[pos] == b',' || bytes[pos].is_ascii_whitespace()) {
            if bytes[pos] == b',' {
                pos += 1;
                break;
            }
            pos += 1;
        }
    }
    
    Ok(values)
}

/// 构建 RecordBatch
fn build_record_batch(
    meta: &TableMeta,
    columns: &[String],
    rows: &[Vec<String>],
) -> io::Result<RecordBatch> {
    let mut arrays: Vec<ArrayRef> = Vec::new();
    let mut arrow_fields: Vec<Field> = Vec::new();

    for col_name in columns {
        let field_opt = meta
            .schema
            .fields
            .iter()
            .find(|f| f.name().eq_ignore_ascii_case(col_name))
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("Column '{}' not found in schema", col_name),
                )
            })?;

        let col_idx = columns.iter().position(|c| c == col_name).unwrap();

        let (array, arrow_field) = match field_opt {
            FieldOption::I32 { name, .. } => {
                let vals: Result<Vec<i32>, _> = rows
                    .iter()
                    .map(|row| {
                        row[col_idx].parse().map_err(|e| {
                            io::Error::new(
                                io::ErrorKind::InvalidInput,
                                format!("Invalid I32: {}", e),
                            )
                        })
                    })
                    .collect();
                (
                    Arc::new(Int32Array::from(vals?)) as ArrayRef,
                    Field::new(name, DataType::Int32, false),
                )
            }
            FieldOption::I64 { name, .. } => {
                let vals: Result<Vec<i64>, _> = rows
                    .iter()
                    .map(|row| {
                        row[col_idx].parse().map_err(|e| {
                            io::Error::new(
                                io::ErrorKind::InvalidInput,
                                format!("Invalid I64: {}", e),
                            )
                        })
                    })
                    .collect();
                (
                    Arc::new(Int64Array::from(vals?)) as ArrayRef,
                    Field::new(name, DataType::Int64, false),
                )
            }
            FieldOption::U64 { name, .. } => {
                let vals: Result<Vec<u64>, _> = rows
                    .iter()
                    .map(|row| {
                        row[col_idx].parse().map_err(|e| {
                            io::Error::new(
                                io::ErrorKind::InvalidInput,
                                format!("Invalid U64: {}", e),
                            )
                        })
                    })
                    .collect();
                (
                    Arc::new(UInt64Array::from(vals?)) as ArrayRef,
                    Field::new(name, DataType::UInt64, false),
                )
            }
            FieldOption::F32 { name, .. } => {
                let vals: Result<Vec<f32>, _> = rows
                    .iter()
                    .map(|row| {
                        row[col_idx].parse().map_err(|e| {
                            io::Error::new(
                                io::ErrorKind::InvalidInput,
                                format!("Invalid F32: {}", e),
                            )
                        })
                    })
                    .collect();
                (
                    Arc::new(Float32Array::from(vals?)) as ArrayRef,
                    Field::new(name, DataType::Float32, false),
                )
            }
            FieldOption::F64 { name, .. } => {
                let vals: Result<Vec<f64>, _> = rows
                    .iter()
                    .map(|row| {
                        row[col_idx].parse().map_err(|e| {
                            io::Error::new(
                                io::ErrorKind::InvalidInput,
                                format!("Invalid F64: {}", e),
                            )
                        })
                    })
                    .collect();
                (
                    Arc::new(Float64Array::from(vals?)) as ArrayRef,
                    Field::new(name, DataType::Float64, false),
                )
            }
            FieldOption::Boolean { name, .. } => {
                let vals: Result<Vec<bool>, _> = rows
                    .iter()
                    .map(|row| match row[col_idx].to_lowercase().as_str() {
                        "true" | "1" => Ok(true),
                        "false" | "0" => Ok(false),
                        _ => Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!("Invalid boolean value: {}", row[col_idx]),
                        )),
                    })
                    .collect();
                (
                    Arc::new(BooleanArray::from(vals?)) as ArrayRef,
                    Field::new(name, DataType::Boolean, false),
                )
            }
            FieldOption::Keyword { name, .. } => {
                let vals: Vec<&str> = rows.iter().map(|row| row[col_idx].as_str()).collect();
                (
                    Arc::new(StringArray::from(vals)) as ArrayRef,
                    Field::new(name, DataType::Utf8, false),
                )
            }
            FieldOption::Timestamp { name, .. } => {
                let vals: Result<Vec<i64>, _> = rows
                    .iter()
                    .map(|row| {
                        row[col_idx].parse().map_err(|e| {
                            io::Error::new(
                                io::ErrorKind::InvalidInput,
                                format!("Invalid Timestamp: {}", e),
                            )
                        })
                    })
                    .collect();
                (
                    Arc::new(TimestampMillisecondArray::from(vals?)) as ArrayRef,
                    Field::new(
                        name,
                        DataType::Timestamp(TimeUnit::Millisecond, None),
                        false,
                    ),
                )
            }
            _ => {
                return Err(io::Error::other(format!(
                    "Unsupported field type for INSERT: {:?}",
                    field_opt
                )));
            }
        };

        arrays.push(array);
        arrow_fields.push(arrow_field);
    }

    let schema = Arc::new(ArrowSchema::new(arrow_fields));
    RecordBatch::try_new(schema, arrays)
        .map_err(|e| io::Error::other(format!("Failed to create batch: {}", e)))
}
