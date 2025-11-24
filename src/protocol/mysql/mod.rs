mod insert_handler;

use crate::engine::Engine;
use crate::schema::field::FieldOption;
use crate::schema::Schema;
use datafusion::arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray, UInt64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema, SchemaRef};
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
        let listener = tokio::net::TcpListener::bind(addr).await?;

        loop {
            match listener.accept().await {
                Ok((stream, addr)) => {
                    println!("MySQL client connected from: {}", addr);
                    let engine = self.engine.clone();

                    // 将 tokio TcpStream 转换为 std TcpStream 并设置为阻塞模式
                    // msql_srv 需要阻塞式的 std::net::TcpStream
                    tokio::task::spawn_blocking(move || match stream.into_std() {
                        Ok(std_stream) => {
                            // 设置为阻塞模式，msql_srv 期望阻塞 I/O
                            if let Err(e) = std_stream.set_nonblocking(false) {
                                eprintln!("Failed to set blocking mode: {}", e);
                                return;
                            }

                            let backend = CalmBackend { engine };
                            if let Err(e) = MysqlIntermediary::run_on_tcp(backend, std_stream) {
                                eprintln!("Error handling MySQL client: {}", e);
                            }
                        }
                        Err(e) => {
                            eprintln!("Failed to convert stream: {}", e);
                        }
                    });
                }
                Err(e) => {
                    eprintln!("MySQL connection failed: {}", e);
                }
            }
        }
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
        let query_trimmed = query.trim();

        // 规范化前缀用于判断语句类型（将多个空格合并为一个）
        // 只处理前 100 个字符，避免大型 INSERT 语句的性能问题
        let prefix_len = query_trimmed.len().min(100);
        let prefix = &query_trimmed[..prefix_len];

        // 将连续空格替换为单个空格，然后转小写
        let query_lower = prefix
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
            .to_lowercase();

        // 忽略客户端初始化命令和事务命令
        if query_lower.starts_with("set ")
            || query_lower.starts_with("select @@")
            || query_lower.starts_with("select version()")
            || query_lower == "select 1"
            || query_lower.starts_with("show variables")
            || query_lower.starts_with("show session")
            || query_lower.starts_with("show collation")
            || query_lower.starts_with("show charset")
            || query_lower == "commit"
            || query_lower == "rollback"
            || query_lower == "begin"
            || query_lower.starts_with("start transaction")
        {
            // 返回空结果或默认值
            if query_lower.starts_with("select @@") || query_lower.starts_with("select version()") {
                let schema = Arc::new(ArrowSchema::new(vec![Field::new(
                    "result",
                    DataType::Utf8,
                    false,
                )]));
                let result = StringArray::from(vec!["calm-0.1.0"]);
                let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(result)])
                    .map_err(io::Error::other)?;
                return write_query_result(results, &schema, &[batch]);
            }
            // 事务命令和其他初始化命令返回成功
            return results.completed(0, 0);
        }

        // flush tables 命令
        if query_lower.starts_with("flush tables") {
            return tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(async {
                    // 解析表名 (如果有指定)
                    let parts: Vec<&str> = query_trimmed.split_whitespace().collect();

                    if parts.len() == 2 {
                        // FLUSH TABLES (刷新所有表)
                        let tables = self.engine.list_tables();
                        log::info!("💾 Flushing all {} tables", tables.len());

                        let mut success_count = 0;
                        let mut failed_tables = Vec::new();

                        for table_name in tables {
                            match self.engine.flush_table(&table_name).await {
                                Ok(_) => {
                                    success_count += 1;
                                    log::info!("✅ Flushed table '{}'", table_name);
                                }
                                Err(e) => {
                                    log::error!("❌ Failed to flush table '{}': {}", table_name, e);
                                    failed_tables.push(table_name);
                                }
                            }
                        }

                        if !failed_tables.is_empty() {
                            let error_msg =
                                format!("Failed to flush tables: {}", failed_tables.join(", "));
                            return results
                                .error(ErrorKind::ER_UNKNOWN_ERROR, error_msg.as_bytes());
                        }

                        log::info!("✅ Successfully flushed {} tables", success_count);
                        return results.completed(0, 0);
                    } else if parts.len() >= 3 {
                        // FLUSH TABLES table_name [, table_name2, ...]
                        // 提取所有表名 (跳过 FLUSH TABLES)
                        let table_names: Vec<String> = parts[2..]
                            .iter()
                            .filter(|s| !s.is_empty() && **s != ",")
                            .map(|s| s.trim_end_matches(',').to_string())
                            .collect();

                        if table_names.is_empty() {
                            return results.error(
                                ErrorKind::ER_PARSE_ERROR,
                                b"No table name specified for FLUSH TABLES",
                            );
                        }

                        log::info!("💾 Flushing tables: {:?}", table_names);

                        let mut failed_tables = Vec::new();
                        for table_name in &table_names {
                            match self.engine.flush_table(table_name).await {
                                Ok(_) => {
                                    log::info!("✅ Flushed table '{}'", table_name);
                                }
                                Err(e) => {
                                    log::error!("❌ Failed to flush table '{}': {}", table_name, e);
                                    failed_tables.push(table_name.clone());
                                }
                            }
                        }

                        if !failed_tables.is_empty() {
                            let error_msg =
                                format!("Failed to flush tables: {}", failed_tables.join(", "));
                            return results
                                .error(ErrorKind::ER_UNKNOWN_ERROR, error_msg.as_bytes());
                        }

                        log::info!("✅ Successfully flushed {} tables", table_names.len());
                        return results.completed(0, 0);
                    } else {
                        return results
                            .error(ErrorKind::ER_PARSE_ERROR, b"Invalid FLUSH TABLES syntax");
                    }
                })
            });
        }

        // INSERT 语句
        if query_lower.starts_with("insert") {
            return tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(insert_handler::handle_insert(
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
                .map_err(io::Error::other)?;

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
                .map_err(io::Error::other)?;

            return write_query_result(results, &schema, &[batch]);
        }

        // SHOW PARTITIONS [FROM|IN] table
        if query_lower.starts_with("show partitions") {
            // 解析表名
            let base = query_lower
                .strip_suffix(';')
                .unwrap_or(query_lower.as_str())
                .trim();
            let rest = base.trim_start_matches("show partitions").trim();

            let table_name_opt = if rest.is_empty() {
                None
            } else if let Some(after_from) = rest.strip_prefix("from ") {
                Some(after_from.trim().to_string())
            } else if let Some(after_in) = rest.strip_prefix("in ") {
                Some(after_in.trim().to_string())
            } else {
                None
            };

            let schema = Arc::new(ArrowSchema::new(vec![
                Field::new("Table", DataType::Utf8, false),
                Field::new("Partition", DataType::Utf8, false),
                Field::new("Segment", DataType::Utf8, false),
                Field::new("SegmentType", DataType::Utf8, false),
                Field::new("CreatedAt", DataType::Utf8, false),
                Field::new("DocCount", DataType::Utf8, false),
            ]));

            return tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(async {
                    let mut tables = Vec::new();
                    let mut partitions = Vec::new();
                    let mut segments = Vec::new();
                    let mut segment_types = Vec::new();
                    let mut created_list = Vec::new();
                    let mut doc_counts = Vec::new();

                    if let Some(table_name) = table_name_opt {
                        let part_names = self.engine.list_partitions(&table_name).await;
                        for p in part_names {
                            let seg_infos = self.engine.list_segments(&table_name, &p).await;
                            if seg_infos.is_empty() {
                                tables.push(table_name.clone());
                                partitions.push(p.clone());
                                segments.push(String::from("-"));
                                segment_types.push(String::from("-"));
                                created_list.push(String::from("-"));
                                doc_counts.push(String::from("-"));
                            } else {
                                for (seg_id, doc_count, created_ts_ms, is_current) in seg_infos {
                                    tables.push(table_name.clone());
                                    partitions.push(p.clone());
                                    segments.push(seg_id.to_string());
                                    let seg_type = if is_current { "current" } else { "frozen" };
                                    segment_types.push(seg_type.to_string());
                                    // 格式化时间戳为本地时区 2025-11-24 11:24:33.006
                                    let dt = chrono::DateTime::from_timestamp_millis(
                                        created_ts_ms as i64,
                                    )
                                    .unwrap_or_else(|| {
                                        chrono::DateTime::from_timestamp(0, 0).unwrap()
                                    })
                                    .with_timezone(&chrono::Local);
                                    let formatted = dt.format("%Y-%m-%d %H:%M:%S%.3f").to_string();
                                    created_list.push(formatted);
                                    doc_counts.push(doc_count.to_string());
                                }
                            }
                        }
                    } else {
                        let all_keys = self.engine.list_all_partition_keys().await;
                        for (t, p) in all_keys {
                            let seg_infos = self.engine.list_segments(&t, &p).await;
                            if seg_infos.is_empty() {
                                tables.push(t.clone());
                                partitions.push(p.clone());
                                segments.push(String::from("-"));
                                segment_types.push(String::from("-"));
                                created_list.push(String::from("-"));
                                doc_counts.push(String::from("-"));
                            } else {
                                for (seg_id, doc_count, created_ts_ms, is_current) in seg_infos {
                                    tables.push(t.clone());
                                    partitions.push(p.clone());
                                    segments.push(seg_id.to_string());
                                    let seg_type = if is_current { "current" } else { "frozen" };
                                    segment_types.push(seg_type.to_string());
                                    let dt = chrono::DateTime::from_timestamp_millis(
                                        created_ts_ms as i64,
                                    )
                                    .unwrap_or_else(|| {
                                        chrono::DateTime::from_timestamp(0, 0).unwrap()
                                    })
                                    .with_timezone(&chrono::Local);
                                    let formatted = dt.format("%Y-%m-%d %H:%M:%S%.3f").to_string();
                                    created_list.push(formatted);
                                    doc_counts.push(doc_count.to_string());
                                }
                            }
                        }
                    }

                    let table_array = StringArray::from(tables);
                    let partition_array = StringArray::from(partitions);
                    let segment_array = StringArray::from(segments);
                    let segment_type_array = StringArray::from(segment_types);
                    let created_array = StringArray::from(created_list);
                    let doc_count_array = StringArray::from(doc_counts);

                    let batch = RecordBatch::try_new(
                        schema.clone(),
                        vec![
                            Arc::new(table_array),
                            Arc::new(partition_array),
                            Arc::new(segment_array),
                            Arc::new(segment_type_array),
                            Arc::new(created_array),
                            Arc::new(doc_count_array),
                        ],
                    )
                    .map_err(io::Error::other)?;

                    write_query_result(results, &schema, &[batch])
                })
            });
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

        log::error!("unsupport sql:{:?}", query);

        results.error(
            ErrorKind::ER_NOT_SUPPORTED_YET,
            b"Only SELECT/INSERT/DELETE/CREATE TABLE/DROP TABLE/SHOW TABLES/SHOW DATABASES/SHOW PARTITIONS/DESCRIBE supported",
        )
    }
}

/// 执行 SELECT 查询
async fn execute_query<W: io::Read + io::Write>(
    engine: Arc<Engine>,
    query: &str,
    results: QueryResultWriter<'_, W>,
) -> io::Result<()> {
    let result = match engine.clone().execute_sql(query).await {
        Ok(result) => result,
        Err(e) => {
            let msg = format!("SQL execution failed: {}", e);
            eprintln!("❌ [SQL] Error: {}", msg);
            return results.error(ErrorKind::ER_PARSE_ERROR, msg.as_bytes());
        }
    };

    eprintln!("🔍 [MySQL Query] {}", query);
    eprintln!(
        "🔍 [MySQL Result] matched_docs={}, result_rows={}",
        result.matched_docs,
        result.batch.num_rows()
    );

    // 检查是否有数据
    if result.batch.num_rows() == 0 {
        eprintln!("⚠️  [MySQL Query] No rows returned - query returned empty result");
        return results.completed(0, 0);
    }

    eprintln!(
        "✅ [MySQL Query] Returning {} rows (matched {} docs)",
        result.batch.num_rows(),
        result.matched_docs
    );

    // 返回单个 batch
    let schema = result.batch.schema();
    write_query_result(results, &schema, &[result.batch])
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
            "TINYINT" | "INT8" => FieldOption::I8 {
                name: col_name,
                index: true,
            },
            "SMALLINT" | "INT16" => FieldOption::I16 {
                name: col_name,
                index: true,
            },
            "BIGINT" | "INT64" => FieldOption::I64 {
                name: col_name,
                index: true,
            },
            "TINYINT UNSIGNED" | "UINT8" => FieldOption::U8 {
                name: col_name,
                index: true,
            },
            "SMALLINT UNSIGNED" | "UINT16" => FieldOption::U16 {
                name: col_name,
                index: true,
            },
            "INT UNSIGNED" | "UINT32" => FieldOption::U32 {
                name: col_name,
                index: true,
            },
            "BIGINT UNSIGNED" | "UINT64" => FieldOption::U64 {
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
            "TIMESTAMP" | "DATETIME" => FieldOption::Timestamp {
                name: col_name,
                index: true,
                format: Some("iso8601".to_string()),
            },
            "TEXT" | "STRING" | "VARCHAR" | "CHAR" => FieldOption::Keyword {
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

    let result = match engine.clone().execute_sql(&select_query).await {
        Ok(result) => result,
        Err(e) => {
            let msg = format!("Query execution failed: {}", e);
            return results.error(ErrorKind::ER_UNKNOWN_ERROR, msg.as_bytes());
        }
    };

    if result.batch.num_rows() == 0 {
        return results.completed(0, 0);
    }

    // 提取主键值
    let pk_field =
        meta.schema.primary_key.as_ref().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "Table has no primary key")
        })?;

    let mut total_deleted = 0u64;

    let batch = result.batch;
    let pk_array = batch
        .column_by_name(pk_field)
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "Primary key column not found"))?;

    // 对每个主键值进行路由和删除
    for i in 0..pk_array.len() {
        let pk_value = format_arrow_value(pk_array, i);

        let partition_id = engine
            .route_partition(&table_name, &pk_value)
            .map_err(|e| io::Error::other(format!("Routing failed: {}", e)))?;

        let partition = engine
            .get_partition(&table_name, &partition_id)
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

        let deleted = partition
            .delete_by_pk(&pk_array_single)
            .map_err(|e| io::Error::other(format!("Delete failed: {}", e)))?;

        total_deleted += deleted;
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
        DataType::UInt32 => {
            let arr = array.as_any().downcast_ref::<UInt32Array>().unwrap();
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
        DataType::Timestamp(unit, _) => {
            // 处理时间戳类型，转换为本地时区显示
            use chrono::{TimeZone, Utc};
            use datafusion::arrow::array::PrimitiveArray;

            match unit {
                datafusion::arrow::datatypes::TimeUnit::Millisecond => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<PrimitiveArray<datafusion::arrow::datatypes::TimestampMillisecondType>>()
                        .unwrap();
                    let timestamp_ms = arr.value(index);
                    let dt = Utc.timestamp_millis_opt(timestamp_ms).unwrap();
                    dt.format("%Y-%m-%d %H:%M:%S%.3f").to_string()
                }
                datafusion::arrow::datatypes::TimeUnit::Second => {
                    let arr = array.as_any().downcast_ref::<PrimitiveArray<datafusion::arrow::datatypes::TimestampSecondType>>().unwrap();
                    let timestamp_s = arr.value(index);
                    let dt = Utc.timestamp_opt(timestamp_s, 0).unwrap();
                    dt.format("%Y-%m-%d %H:%M:%S").to_string()
                }
                datafusion::arrow::datatypes::TimeUnit::Microsecond => {
                    let arr = array.as_any().downcast_ref::<PrimitiveArray<datafusion::arrow::datatypes::TimestampMicrosecondType>>().unwrap();
                    let timestamp_us = arr.value(index);
                    let dt = Utc.timestamp_micros(timestamp_us).unwrap();
                    dt.format("%Y-%m-%d %H:%M:%S%.6f").to_string()
                }
                datafusion::arrow::datatypes::TimeUnit::Nanosecond => {
                    let arr = array.as_any().downcast_ref::<PrimitiveArray<datafusion::arrow::datatypes::TimestampNanosecondType>>().unwrap();
                    let timestamp_ns = arr.value(index);
                    let dt = Utc.timestamp_nanos(timestamp_ns);
                    dt.format("%Y-%m-%d %H:%M:%S%.9f").to_string()
                }
            }
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
            FieldOption::Timestamp { .. } => "timestamp",
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
