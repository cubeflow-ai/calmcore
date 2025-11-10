use crate::compute::PartitionTableProvider;
use crate::engine::Engine;
use crate::schema::field::FieldOption;
use crate::schema::Schema;
use datafusion::arrow::array::{
    ArrayRef, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array, RecordBatch,
    StringArray, UInt64Array,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema, SchemaRef};
use datafusion::prelude::*;
use msql_srv::*;
use std::io;
use std::sync::Arc;

pub struct MysqlServer {
    engine: Arc<Engine>,
}

impl MysqlServer {
    pub fn new(engine: Arc<Engine>) -> Self {
        Self { engine }
    }

    pub async fn start(self, addr: &str) -> Result<(), Box<dyn std::error::Error>> {
        let listener = std::net::TcpListener::bind(addr)?;
        println!("MySQL server listening on {}", addr);

        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    let engine = self.engine.clone();
                    tokio::spawn(async move {
                        let backend = CalmBackend { engine };
                        if let Err(e) = MysqlIntermediary::run_on_tcp(backend, stream) {
                            eprintln!("Error handling client: {}", e);
                        }
                    });
                }
                Err(e) => {
                    eprintln!("Connection failed: {}", e);
                }
            }
        }

        Ok(())
    }
}

struct CalmBackend {
    engine: Arc<Engine>,
}

impl<W: io::Read + io::Write> MysqlShim<W> for CalmBackend {
    type Error = io::Error;

    fn on_prepare(&mut self, _query: &str, info: StatementMetaWriter<W>) -> io::Result<()> {
        info.reply(42, &[], &[])
    }

    fn on_execute(
        &mut self,
        _id: u32,
        _params: ParamParser,
        results: QueryResultWriter<W>,
    ) -> io::Result<()> {
        results.completed(0, 0)
    }

    fn on_close(&mut self, _stmt: u32) {}

    fn on_query(&mut self, query: &str, results: QueryResultWriter<W>) -> io::Result<()> {
        let query_lower = query.trim().to_lowercase();

        // INSERT 语句
        if query_lower.starts_with("insert") {
            return tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(handle_insert(
                    self.engine.clone(),
                    query,
                    results,
                ))
            });
        }

        // DELETE 语句
        if query_lower.starts_with("delete") {
            return tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(handle_delete(
                    self.engine.clone(),
                    query,
                    results,
                ))
            });
        }

        // CREATE TABLE
        if query_lower.starts_with("create table") {
            return tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(async move {
                    match handle_create_table(&self.engine, query).await {
                        Ok((schema, batches)) => write_query_result(results, &schema, &batches),
                        Err(e) => {
                            let msg = format!("CREATE TABLE failed: {}", e);
                            results.error(ErrorKind::ER_UNKNOWN_ERROR, msg.as_bytes())
                        }
                    }
                })
            });
        }

        // DROP TABLE
        if query_lower.starts_with("drop table") {
            return tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(async move {
                    match handle_drop_table(&self.engine, query).await {
                        Ok((schema, batches)) => write_query_result(results, &schema, &batches),
                        Err(e) => {
                            let msg = format!("DROP TABLE failed: {}", e);
                            results.error(ErrorKind::ER_UNKNOWN_ERROR, msg.as_bytes())
                        }
                    }
                })
            });
        }

        // SHOW DATABASES
        if query_lower == "show databases" {
            let schema = Arc::new(ArrowSchema::new(vec![Field::new(
                "Database",
                DataType::Utf8,
                false,
            )]));

            let databases = StringArray::from(vec!["calm"]);
            let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(databases)])
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

            return write_query_result(results, &schema, &[batch]);
        }

        // USE database
        if query_lower.starts_with("use ") {
            return results.completed(0, 0);
        }

        // SHOW TABLES
        if query_lower == "show tables" || query_lower == "show tables;" {
            let schema = Arc::new(ArrowSchema::new(vec![Field::new(
                "Tables_in_calm",
                DataType::Utf8,
                false,
            )]));

            let table_names = self.engine.list_tables();
            let tables: Vec<&str> = table_names.iter().map(|s| s.as_str()).collect();

            let tables_array = StringArray::from(tables);
            let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(tables_array)])
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

            return write_query_result(results, &schema, &[batch]);
        }

        // DESCRIBE / DESC table
        if query_lower.starts_with("describe ") || query_lower.starts_with("desc ") {
            return tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(async move {
                    match handle_describe(&self.engine, query).await {
                        Ok((schema, batches)) => write_query_result(results, &schema, &batches),
                        Err(e) => {
                            let msg = format!("DESCRIBE failed: {}", e);
                            results.error(ErrorKind::ER_UNKNOWN_ERROR, msg.as_bytes())
                        }
                    }
                })
            });
        }

        // SELECT 查询
        if query_lower.starts_with("select") {
            return tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(execute_query(
                    self.engine.clone(),
                    query,
                    results,
                ))
            });
        }

        results.error(
            ErrorKind::ER_NOT_SUPPORTED_YET,
            b"Only SELECT/INSERT/DELETE/CREATE TABLE/DROP TABLE/SHOW TABLES/SHOW DATABASES/DESCRIBE supported",
        )
    }
}

/// 执行 SELECT 查询
async fn execute_query<W: io::Read + io::Write>(
    engine: Arc<Engine>,
    query: &str,
    results: QueryResultWriter<'_, W>,
) -> io::Result<()> {
    let ctx = SessionContext::new();

    // 提取表名 (简单实现)
    let query_lower = query.to_lowercase();
    let table_names = engine.list_tables();

    let mut found_table: Option<String> = None;
    for table_name in &table_names {
        if query_lower.contains(&format!("from {}", table_name.to_lowercase()))
            || query_lower.contains(&format!("from `{}`", table_name.to_lowercase()))
        {
            found_table = Some(table_name.clone());
            break;
        }
    }

    let table_name = match found_table {
        Some(name) => name,
        None => {
            let msg = format!(
                "Table not found in query. Available tables: {}",
                table_names.join(", ")
            );
            return results.error(ErrorKind::ER_NO_SUCH_TABLE, msg.as_bytes());
        }
    };

    // 获取表元数据 (验证表存在)
    let _meta = match engine.get_table_meta(&table_name) {
        Ok(meta) => meta,
        Err(e) => {
            let msg = format!("Failed to get table metadata: {}", e);
            return results.error(ErrorKind::ER_NO_SUCH_TABLE, msg.as_bytes());
        }
    };

    // 注册所有 partition (简化版: 只使用第一个)
    // 未来可以用 UNION ALL 合并多个 partition
    if let Some(partition) = engine.get_partition(&table_name, 0).await {
        eprintln!(
            "🔍 [Partition Info] current segment doc_count: {}",
            partition.get_current_segment().doc_count()
        );
        eprintln!(
            "🔍 [Partition Info] frozen segments count: {}",
            partition.get_frozen_segments().len()
        );

        let provider = Arc::new(PartitionTableProvider::new(partition));
        if let Err(e) = ctx.register_table(&table_name, provider) {
            let msg = format!("Failed to register table: {}", e);
            return results.error(ErrorKind::ER_UNKNOWN_ERROR, msg.as_bytes());
        }
    } else {
        let msg = format!("Partition 0 not found for table '{}'", table_name);
        return results.error(ErrorKind::ER_NO_SUCH_TABLE, msg.as_bytes());
    }

    // 执行查询
    eprintln!("🔍 [SQL Query] {}", query);
    let df = match ctx.sql(query).await {
        Ok(df) => {
            eprintln!("🔍 [SQL] Query parsed successfully");
            df
        }
        Err(e) => {
            let msg = format!("Query parse error: {}", e);
            eprintln!("❌ [SQL] Parse error: {}", msg);
            return results.error(ErrorKind::ER_PARSE_ERROR, msg.as_bytes());
        }
    };

    let batches = match df.collect().await {
        Ok(batches) => {
            eprintln!("🔍 [SQL] Query executed successfully");
            batches
        }
        Err(e) => {
            let msg = format!("Query execution error: {}", e);
            eprintln!("❌ [SQL] Execution error: {}", msg);
            return results.error(ErrorKind::ER_UNKNOWN_ERROR, msg.as_bytes());
        }
    };

    // 调试信息
    eprintln!("🔍 [Query Result] batches.len() = {}", batches.len());
    for (i, batch) in batches.iter().enumerate() {
        eprintln!(
            "  Batch {}: {} rows, {} columns",
            i,
            batch.num_rows(),
            batch.num_columns()
        );
    }

    // 检查是否有数据
    if batches.is_empty() {
        // 没有任何 batch，返回空结果
        return results.completed(0, 0);
    }

    // 有 batch 但可能没有行，仍然需要返回 schema
    let schema = batches[0].schema();
    write_query_result(results, &schema, &batches)
}

/// CREATE TABLE 处理
async fn handle_create_table(
    engine: &Arc<Engine>,
    query: &str,
) -> Result<(SchemaRef, Vec<RecordBatch>), String> {
    // 解析 CREATE TABLE 语句
    // 格式: CREATE TABLE table_name (col1 TYPE, col2 TYPE, ...);

    let query_clean = query
        .trim()
        .trim_end_matches(';')
        .replace("  ", " ")
        .replace("\n", " ");

    // 提取表名
    let parts: Vec<&str> = query_clean.split('(').collect();
    if parts.len() < 2 {
        return Err("Invalid CREATE TABLE syntax".to_string());
    }

    let header = parts[0];
    let table_name = header
        .trim()
        .strip_prefix("CREATE TABLE ")
        .or_else(|| header.trim().strip_prefix("create table "))
        .ok_or("Invalid CREATE TABLE syntax")?
        .trim()
        .to_string();

    // 提取列定义
    let columns_part = parts[1..].join("(");
    let columns_part = columns_part.trim_end_matches(')').trim();

    let column_defs: Vec<&str> = columns_part.split(',').collect();

    let mut fields = Vec::new();
    let mut primary_key: Option<String> = None;

    for col_def in column_defs {
        let col_def = col_def.trim();
        let parts: Vec<&str> = col_def.split_whitespace().collect();

        if parts.len() < 2 {
            continue;
        }

        let col_name = parts[0].to_string();
        let col_type = parts[1].to_uppercase();

        let is_primary_key = parts.iter().any(|&p| p.to_uppercase() == "PRIMARY");
        if is_primary_key {
            primary_key = Some(col_name.clone());
        }

        let field = match col_type.as_str() {
            "INT" | "INTEGER" | "INT32" => FieldOption::I32 {
                name: col_name,
                index: true,
            },
            "BIGINT" | "INT64" => FieldOption::I64 {
                name: col_name,
                index: true,
            },
            "FLOAT" | "FLOAT32" => FieldOption::F32 {
                name: col_name,
                index: true,
            },
            "DOUBLE" | "FLOAT64" => FieldOption::F64 {
                name: col_name,
                index: true,
            },
            "BOOL" | "BOOLEAN" => FieldOption::Boolean {
                name: col_name,
                index: true,
            },
            "TEXT" | "STRING" | "VARCHAR" => FieldOption::Keyword {
                name: col_name,
                index: true,
                is_array: false,
                persist_option: None,
                case_sensitive: true,
            },
            _ => {
                return Err(format!("Unsupported column type: {}", col_type));
            }
        };

        fields.push(field);
    }

    if fields.is_empty() {
        return Err("No valid columns defined".to_string());
    }

    // 创建 Schema
    let schema = Schema {
        name: table_name.clone(),
        primary_key,
        store_source: true,
        fields,
        persist_policy: Default::default(),
    };

    // 创建表 (使用 Hash 分区策略)
    use crate::catalog::PartitionStrategy;

    engine
        .create_table(
            &table_name,
            schema,
            PartitionStrategy::Hash {
                field: "id".to_string(), // 默认使用 id 字段做 hash
                num_partitions: 4,
            },
            4, // 4 个 partition
        )
        .await
        .map_err(|e| format!("Failed to create table: {}", e))?;

    // 返回成功消息
    let schema = Arc::new(ArrowSchema::new(vec![Field::new(
        "Result",
        DataType::Utf8,
        false,
    )]));

    let message = StringArray::from(vec![format!("Table '{}' created successfully", table_name)]);
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(message)])
        .map_err(|e| format!("Failed to create result batch: {}", e))?;

    Ok((schema, vec![batch]))
}

/// DROP TABLE 处理
async fn handle_drop_table(
    engine: &Arc<Engine>,
    query: &str,
) -> Result<(SchemaRef, Vec<RecordBatch>), String> {
    // 解析 DROP TABLE 语句
    // 格式: DROP TABLE table_name;

    let query_clean = query.trim().trim_end_matches(';');

    let table_name = query_clean
        .strip_prefix("DROP TABLE ")
        .or_else(|| query_clean.strip_prefix("drop table "))
        .ok_or("Invalid DROP TABLE syntax")?
        .trim()
        .to_string();

    // 删除表
    engine
        .drop_table(&table_name)
        .await
        .map_err(|e| format!("Failed to drop table: {}", e))?;

    // 返回成功消息
    let schema = Arc::new(ArrowSchema::new(vec![Field::new(
        "Result",
        DataType::Utf8,
        false,
    )]));

    let message = StringArray::from(vec![format!("Table '{}' dropped successfully", table_name)]);
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(message)])
        .map_err(|e| format!("Failed to create result batch: {}", e))?;

    Ok((schema, vec![batch]))
}

/// INSERT 语句处理
async fn handle_insert<W: io::Read + io::Write>(
    engine: Arc<Engine>,
    query: &str,
    results: QueryResultWriter<'_, W>,
) -> io::Result<()> {
    // 解析 INSERT 语句
    // 格式: INSERT INTO table (col1, col2) VALUES (val1, val2);

    let query_clean = query.trim().trim_end_matches(';');

    // 提取表名
    let table_name_start = query_clean
        .to_lowercase()
        .find("into ")
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "Missing 'INTO'"))?
        + 5;

    let table_name_end = query_clean[table_name_start..]
        .find('(')
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "Missing '('"))?;

    let table_name = query_clean[table_name_start..table_name_start + table_name_end]
        .trim()
        .to_string();

    // 获取表元数据
    let meta = engine
        .get_table_meta(&table_name)
        .map_err(|e| io::Error::new(io::ErrorKind::NotFound, format!("Table not found: {}", e)))?;

    // 提取列名
    let columns_start = table_name_start + table_name_end + 1;
    let columns_end = query_clean[columns_start..]
        .find(')')
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "Missing ')' after columns"))?
        + columns_start;

    let columns_str = &query_clean[columns_start..columns_end];
    let columns: Vec<String> = columns_str
        .split(',')
        .map(|s| s.trim().to_string())
        .collect();

    // 提取值
    let values_start = query_clean
        .to_lowercase()
        .find("values")
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "Missing 'VALUES'"))?
        + 6;

    let values_start = query_clean[values_start..]
        .find('(')
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "Missing '(' after VALUES"))?
        + values_start
        + 1;

    let values_end = query_clean[values_start..]
        .rfind(')')
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "Missing ')' after values"))?
        + values_start;

    let values_str = &query_clean[values_start..values_end];
    let values: Vec<String> = values_str
        .split(',')
        .map(|s| {
            let s = s.trim();
            // 去掉引号
            if (s.starts_with('\'') && s.ends_with('\''))
                || (s.starts_with('"') && s.ends_with('"'))
            {
                s[1..s.len() - 1].to_string()
            } else {
                s.to_string()
            }
        })
        .collect();

    if columns.len() != values.len() {
        return results.error(
            ErrorKind::ER_WRONG_VALUE_COUNT_ON_ROW,
            b"Column count doesn't match value count",
        );
    }

    // 构建 Arrow 数组
    let mut arrays: Vec<ArrayRef> = Vec::new();
    let mut arrow_fields: Vec<Field> = Vec::new();

    for (col_name, value) in columns.iter().zip(values.iter()) {
        // 在 schema 中查找字段
        let field_opt = meta
            .schema
            .fields
            .iter()
            .find(|f| f.name() == col_name)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("Column '{}' not found in schema", col_name),
                )
            })?;

        let (array, arrow_field) = match field_opt {
            FieldOption::I32 { name, .. } => {
                let val: i32 = value.parse().map_err(|e| {
                    io::Error::new(io::ErrorKind::InvalidInput, format!("Invalid I32: {}", e))
                })?;
                (
                    Arc::new(Int32Array::from(vec![val])) as ArrayRef,
                    Field::new(name, DataType::Int32, false),
                )
            }
            FieldOption::I64 { name, .. } => {
                let val: i64 = value.parse().map_err(|e| {
                    io::Error::new(io::ErrorKind::InvalidInput, format!("Invalid I64: {}", e))
                })?;
                (
                    Arc::new(Int64Array::from(vec![val])) as ArrayRef,
                    Field::new(name, DataType::Int64, false),
                )
            }
            FieldOption::U64 { name, .. } => {
                let val: u64 = value.parse().map_err(|e| {
                    io::Error::new(io::ErrorKind::InvalidInput, format!("Invalid U64: {}", e))
                })?;
                (
                    Arc::new(UInt64Array::from(vec![val])) as ArrayRef,
                    Field::new(name, DataType::UInt64, false),
                )
            }
            FieldOption::F32 { name, .. } => {
                let val: f32 = value.parse().map_err(|e| {
                    io::Error::new(io::ErrorKind::InvalidInput, format!("Invalid F32: {}", e))
                })?;
                (
                    Arc::new(Float32Array::from(vec![val])) as ArrayRef,
                    Field::new(name, DataType::Float32, false),
                )
            }
            FieldOption::F64 { name, .. } => {
                let val: f64 = value.parse().map_err(|e| {
                    io::Error::new(io::ErrorKind::InvalidInput, format!("Invalid F64: {}", e))
                })?;
                (
                    Arc::new(Float64Array::from(vec![val])) as ArrayRef,
                    Field::new(name, DataType::Float64, false),
                )
            }
            FieldOption::Boolean { name, .. } => {
                let val: bool = match value.to_lowercase().as_str() {
                    "true" | "1" => true,
                    "false" | "0" => false,
                    _ => {
                        return results.error(
                            ErrorKind::ER_WRONG_VALUE,
                            format!("Invalid boolean value: {}", value).as_bytes(),
                        );
                    }
                };
                (
                    Arc::new(BooleanArray::from(vec![val])) as ArrayRef,
                    Field::new(name, DataType::Boolean, false),
                )
            }
            FieldOption::Keyword { name, .. } => (
                Arc::new(StringArray::from(vec![value.as_str()])) as ArrayRef,
                Field::new(name, DataType::Utf8, false),
            ),
            _ => {
                return results.error(
                    ErrorKind::ER_NOT_SUPPORTED_YET,
                    format!("Unsupported field type for INSERT: {:?}", field_opt).as_bytes(),
                );
            }
        };

        arrays.push(array);
        arrow_fields.push(arrow_field);
    }

    let schema = Arc::new(ArrowSchema::new(arrow_fields));
    let batch = RecordBatch::try_new(schema, arrays).map_err(|e| {
        io::Error::new(
            io::ErrorKind::Other,
            format!("Failed to create batch: {}", e),
        )
    })?;

    // 路由到正确的 partition (使用主键)
    let pk_field =
        meta.schema.primary_key.as_ref().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "Table has no primary key")
        })?;

    let pk_value_idx = columns
        .iter()
        .position(|c| c == pk_field)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "Primary key not in INSERT"))?;

    let pk_value = &values[pk_value_idx];

    let partition_id = engine.route_partition(&table_name, pk_value).map_err(|e| {
        io::Error::new(
            io::ErrorKind::Other,
            format!("Partition routing failed: {}", e),
        )
    })?;

    // 获取 partition 并插入
    let partition = engine
        .get_partition(&table_name, partition_id)
        .await
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "Partition not found"))?;

    partition
        .upsert(batch)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Insert failed: {}", e)))?;

    // 返回成功
    results.completed(1, 0)
}

/// DELETE 语句处理
async fn handle_delete<W: io::Read + io::Write>(
    engine: Arc<Engine>,
    query: &str,
    results: QueryResultWriter<'_, W>,
) -> io::Result<()> {
    // 解析 DELETE 语句
    // 格式: DELETE FROM table WHERE condition;

    let query_clean = query.trim().trim_end_matches(';');
    let query_lower = query_clean.to_lowercase();

    // 提取表名
    let from_pos = query_lower
        .find("from ")
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "Missing 'FROM'"))?
        + 5;

    let where_pos = query_lower.find("where ");

    let table_name = if let Some(where_pos) = where_pos {
        query_clean[from_pos..where_pos].trim().to_string()
    } else {
        return results.error(
            ErrorKind::ER_PARSE_ERROR,
            b"DELETE without WHERE is not supported for safety",
        );
    };

    // 获取表元数据
    let meta = engine
        .get_table_meta(&table_name)
        .map_err(|e| io::Error::new(io::ErrorKind::NotFound, format!("Table not found: {}", e)))?;

    // 构建 SELECT 查询来找到要删除的记录
    let where_clause = &query_clean[where_pos.unwrap() + 6..];
    let select_query = format!("SELECT * FROM {} WHERE {}", table_name, where_clause);

    // 创建 DataFusion context 并注册表
    let ctx = SessionContext::new();

    // 注册所有 partition (简化: 只用第一个)
    if let Some(partition) = engine.get_partition(&table_name, 0).await {
        let provider = Arc::new(PartitionTableProvider::new(partition));
        ctx.register_table(&table_name, provider).map_err(|e| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("Register table failed: {}", e),
            )
        })?;
    } else {
        return results.error(ErrorKind::ER_NO_SUCH_TABLE, b"Partition not found");
    }

    // 执行查询找到要删除的记录
    let df = ctx.sql(&select_query).await.map_err(|e| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("Query parse error: {}", e),
        )
    })?;

    let batches = df
        .collect()
        .await
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Query failed: {}", e)))?;

    if batches.is_empty() {
        return results.completed(0, 0);
    }

    // 提取主键值
    let pk_field =
        meta.schema.primary_key.as_ref().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "Table has no primary key")
        })?;

    let mut total_deleted = 0u64;

    for batch in batches {
        let pk_array = batch.column_by_name(pk_field).ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotFound, "Primary key column not found")
        })?;

        // 对每个主键值进行路由和删除
        for i in 0..pk_array.len() {
            let pk_value = format_arrow_value(pk_array, i);

            let partition_id = engine
                .route_partition(&table_name, &pk_value)
                .map_err(|e| {
                    io::Error::new(io::ErrorKind::Other, format!("Routing failed: {}", e))
                })?;

            let partition = engine
                .get_partition(&table_name, partition_id)
                .await
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "Partition not found"))?;

            // 创建单个值的数组用于删除
            let pk_array_single = match pk_array.data_type() {
                DataType::Int64 => {
                    let typed = pk_array.as_any().downcast_ref::<Int64Array>().unwrap();
                    Arc::new(Int64Array::from(vec![typed.value(i)])) as ArrayRef
                }
                DataType::UInt64 => {
                    let typed = pk_array.as_any().downcast_ref::<UInt64Array>().unwrap();
                    Arc::new(UInt64Array::from(vec![typed.value(i)])) as ArrayRef
                }
                DataType::Utf8 => {
                    let typed = pk_array.as_any().downcast_ref::<StringArray>().unwrap();
                    Arc::new(StringArray::from(vec![typed.value(i)])) as ArrayRef
                }
                _ => {
                    return results.error(
                        ErrorKind::ER_NOT_SUPPORTED_YET,
                        b"Unsupported primary key type for DELETE",
                    );
                }
            };

            let deleted = partition.delete_by_pk(&pk_array_single).map_err(|e| {
                io::Error::new(io::ErrorKind::Other, format!("Delete failed: {}", e))
            })?;

            total_deleted += deleted;
        }
    }

    results.completed(total_deleted, 0)
}

/// 写入查询结果
fn write_query_result<W: io::Read + io::Write>(
    results: QueryResultWriter<'_, W>,
    schema: &SchemaRef,
    batches: &[RecordBatch],
) -> io::Result<()> {
    let columns: Vec<msql_srv::Column> = schema
        .fields()
        .iter()
        .map(|field| {
            let col_type = get_arrow_type(field.data_type());
            msql_srv::Column {
                table: "".to_string(),
                column: field.name().clone(),
                coltype: col_type,
                colflags: ColumnFlags::empty(),
            }
        })
        .collect();

    let mut row_writer = results.start(&columns)?;

    for batch in batches {
        for row_idx in 0..batch.num_rows() {
            for col_idx in 0..batch.num_columns() {
                let array = batch.column(col_idx);
                let value = format_arrow_value(array, row_idx);
                row_writer.write_col(value)?;
            }
            row_writer.end_row()?;
        }
    }

    row_writer.finish()
}

/// 格式化 Arrow 数组值为字符串
fn format_arrow_value(array: &ArrayRef, index: usize) -> String {
    use datafusion::arrow::array::*;

    if array.is_null(index) {
        return "NULL".to_string();
    }

    match array.data_type() {
        DataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
            arr.value(index).to_string()
        }
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
            arr.value(index).to_string()
        }
        DataType::UInt64 => {
            let arr = array.as_any().downcast_ref::<UInt64Array>().unwrap();
            arr.value(index).to_string()
        }
        DataType::Float32 => {
            let arr = array.as_any().downcast_ref::<Float32Array>().unwrap();
            arr.value(index).to_string()
        }
        DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
            arr.value(index).to_string()
        }
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
            arr.value(index).to_string()
        }
        DataType::Boolean => {
            let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            arr.value(index).to_string()
        }
        _ => format!("{:?}", array),
    }
}

/// DESCRIBE 处理
async fn handle_describe(
    engine: &Arc<Engine>,
    query: &str,
) -> Result<(SchemaRef, Vec<RecordBatch>), String> {
    // 解析表名
    let query_lower = query.trim().to_lowercase();
    let table_name = if query_lower.starts_with("describe ") {
        query.trim()[9..].trim()
    } else if query_lower.starts_with("desc ") {
        query.trim()[5..].trim()
    } else {
        return Err("Invalid DESCRIBE syntax".to_string());
    };

    // 移除分号
    let table_name = table_name.trim_end_matches(';').trim();

    // 获取表元数据
    let table_meta = engine
        .get_table_meta(table_name)
        .map_err(|e| format!("Table '{}' not found: {}", table_name, e))?;

    // 构建 DESCRIBE 结果
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("Field", DataType::Utf8, false),
        Field::new("Type", DataType::Utf8, false),
        Field::new("Null", DataType::Utf8, false),
        Field::new("Key", DataType::Utf8, false),
        Field::new("Default", DataType::Utf8, true),
        Field::new("Extra", DataType::Utf8, true),
    ]));

    let mut field_names = Vec::new();
    let mut field_types = Vec::new();
    let mut field_nulls = Vec::new();
    let mut field_keys = Vec::new();
    let mut field_defaults: Vec<Option<String>> = Vec::new();
    let mut field_extras: Vec<Option<String>> = Vec::new();

    for field in &table_meta.schema.fields {
        field_names.push(field.name().to_string());

        // 字段类型
        let field_type = match field {
            FieldOption::Keyword { .. } => "varchar(255)",
            FieldOption::I8 { .. } => "tinyint",
            FieldOption::I16 { .. } => "smallint",
            FieldOption::I32 { .. } => "int",
            FieldOption::I64 { .. } => "bigint",
            FieldOption::U8 { .. } => "tinyint unsigned",
            FieldOption::U16 { .. } => "smallint unsigned",
            FieldOption::U32 { .. } => "int unsigned",
            FieldOption::U64 { .. } => "bigint unsigned",
            FieldOption::F32 { .. } => "float",
            FieldOption::F64 { .. } => "double",
            FieldOption::Boolean { .. } => "tinyint(1)",
        };
        field_types.push(field_type.to_string());

        // Null 约束
        field_nulls.push("YES".to_string());

        // Key (主键标识)
        let is_primary = table_meta
            .schema
            .primary_key
            .as_ref()
            .map(|pk| pk == field.name())
            .unwrap_or(false);
        field_keys.push(if is_primary {
            "PRI".to_string()
        } else {
            "".to_string()
        });

        // Default
        field_defaults.push(None);

        // Extra (索引标识)
        let extra = if field.is_index() {
            Some("indexed".to_string())
        } else {
            None
        };
        field_extras.push(extra);
    }

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(field_names)),
            Arc::new(StringArray::from(field_types)),
            Arc::new(StringArray::from(field_nulls)),
            Arc::new(StringArray::from(field_keys)),
            Arc::new(StringArray::from(field_defaults)),
            Arc::new(StringArray::from(field_extras)),
        ],
    )
    .map_err(|e| format!("Failed to create result batch: {}", e))?;

    Ok((schema, vec![batch]))
}

/// 将 Arrow DataType 映射到 MySQL ColumnType
fn get_arrow_type(data_type: &DataType) -> ColumnType {
    match data_type {
        DataType::Int8 | DataType::Int16 | DataType::Int32 => ColumnType::MYSQL_TYPE_LONG,
        DataType::Int64 => ColumnType::MYSQL_TYPE_LONGLONG,
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 => ColumnType::MYSQL_TYPE_LONG,
        DataType::UInt64 => ColumnType::MYSQL_TYPE_LONGLONG,
        DataType::Float32 => ColumnType::MYSQL_TYPE_FLOAT,
        DataType::Float64 => ColumnType::MYSQL_TYPE_DOUBLE,
        DataType::Utf8 | DataType::LargeUtf8 => ColumnType::MYSQL_TYPE_VAR_STRING,
        DataType::Boolean => ColumnType::MYSQL_TYPE_TINY,
        DataType::Date32 | DataType::Date64 => ColumnType::MYSQL_TYPE_DATE,
        DataType::Timestamp(_, _) => ColumnType::MYSQL_TYPE_TIMESTAMP,
        _ => ColumnType::MYSQL_TYPE_VAR_STRING,
    }
}
