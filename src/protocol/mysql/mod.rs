mod insert_handler;

use crate::engine::Engine;
use crate::schema::field::FieldOption;
use crate::schema::Schema;
use datafusion::arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray, UInt64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema, SchemaRef};
use msql_srv::*;
use sha1::{Digest, Sha1};
use std::io;
use std::sync::Arc;

pub struct MysqlServer {
    engine: Arc<Engine>,
    username: String,
    password: String,
}

impl MysqlServer {
    pub fn new(engine: Arc<Engine>, username: String, password: String) -> Self {
        Self {
            engine,
            username,
            password,
        }
    }

    pub async fn start(self, addr: &str) -> Result<(), Box<dyn std::error::Error>> {
        let listener = tokio::net::TcpListener::bind(addr).await?;

        loop {
            match listener.accept().await {
                Ok((stream, addr)) => {
                    log::info!("MySQL client connected from: {}", addr);
                    let engine = self.engine.clone();
                    let username = self.username.clone();
                    let password = self.password.clone();

                    // 将 tokio TcpStream 转换为 std TcpStream 并设置为阻塞模式
                    // msql_srv 需要阻塞式的 std::net::TcpStream
                    tokio::task::spawn_blocking(move || {
                        let peer_addr = addr.to_string();

                        match stream.into_std() {
                            Ok(std_stream) => {
                                // 设置为阻塞模式，msql_srv 期望阻塞 I/O
                                if let Err(e) = std_stream.set_nonblocking(false) {
                                    log::error!("Failed to set blocking mode: {}", e);
                                    return;
                                }

                                // 禁用 Nagle 算法，减少小包延迟（跨平台）
                                if let Err(e) = std_stream.set_nodelay(true) {
                                    log::warn!("Failed to set TCP_NODELAY: {}", e);
                                }

                                // 设置读写超时，避免无响应连接长时间占用资源（跨平台）
                                // 增加到 8 小时，避免交互式客户端频繁断开
                                let timeout = std::time::Duration::from_secs(300);
                                if let Err(e) = std_stream.set_read_timeout(Some(timeout)) {
                                    log::warn!("Failed to set read timeout: {}", e);
                                }
                                if let Err(e) = std_stream.set_write_timeout(Some(timeout)) {
                                    log::warn!("Failed to set write timeout: {}", e);
                                }

                                let backend = CalmBackend {
                                    engine,
                                    username,
                                    password,
                                    prepared_stmts: std::collections::HashMap::new(),
                                };

                                // 处理连接
                                match MysqlIntermediary::run_on_tcp(backend, std_stream) {
                                    Ok(_) => {
                                        log::info!(
                                            "MySQL client {} disconnected normally",
                                            peer_addr
                                        );
                                    }
                                    Err(e) => {
                                        log::error!("MySQL client {} error: {}", peer_addr, e);
                                    }
                                }

                                // 连接结束,资源应该被释放
                                log::debug!("Cleaning up MySQL connection from {}", peer_addr);
                            }
                            Err(e) => {
                                log::error!("Failed to convert stream: {}", e);
                            }
                        }
                    });
                }
                Err(e) => {
                    log::error!("MySQL connection failed: {}", e);
                }
            }
        }
    }
}

struct CalmBackend {
    engine: Arc<Engine>,
    username: String,
    password: String,
    // 存储 prepared statement 的 SQL（用于流式游标支持）
    prepared_stmts: std::collections::HashMap<u32, String>,
}

/// MySQL 密码验证 - mysql_native_password 插件
///
/// MySQL 5.x/8.x 使用的标准认证方式:
/// 1. Server 发送 20 字节的 scramble (salt)
/// 2. Client 计算: XOR(SHA1(password), SHA1(scramble + SHA1(SHA1(password))))
/// 3. Server 验证: 计算相同的值并比较
fn verify_mysql_native_password(password: &str, auth_response: &[u8], scramble: &[u8]) -> bool {
    if auth_response.is_empty() {
        // 空密码的情况
        return password.is_empty();
    }

    if scramble.len() != 20 {
        log::error!("Invalid scramble length: {}", scramble.len());
        return false;
    }

    if auth_response.len() != 20 {
        log::error!("Invalid auth_response length: {}", auth_response.len());
        return false;
    }

    // 1. SHA1(password)
    let mut hasher = Sha1::new();
    hasher.update(password.as_bytes());
    let stage1 = hasher.finalize();

    // 2. SHA1(SHA1(password))
    let mut hasher = Sha1::new();
    hasher.update(&stage1);
    let stage2 = hasher.finalize();

    // 3. SHA1(scramble + SHA1(SHA1(password)))
    let mut hasher = Sha1::new();
    hasher.update(scramble);
    hasher.update(&stage2);
    let stage3 = hasher.finalize();

    // 4. XOR(SHA1(password), SHA1(scramble + SHA1(SHA1(password))))
    let mut expected = [0u8; 20];
    for i in 0..20 {
        expected[i] = stage1[i] ^ stage3[i];
    }

    // 5. 比较结果
    expected == auth_response
}

impl<W: io::Read + io::Write> MysqlShim<W> for CalmBackend {
    type Error = io::Error;

    fn after_authentication(&mut self, context: &AuthenticationContext<'_>) -> io::Result<()> {
        // 验证用户名
        if let Some(client_username) = &context.username {
            let client_username_str = String::from_utf8_lossy(client_username);

            // 检查用户名是否匹配
            if client_username_str != self.username {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    format!("Access denied for user '{}'", client_username_str),
                ));
            }

            // 验证密码
            if let (Some(auth_response), Some(scramble)) =
                (&context.auth_response, &context.scramble)
            {
                if !verify_mysql_native_password(&self.password, auth_response, scramble) {
                    return Err(io::Error::new(
                        io::ErrorKind::PermissionDenied,
                        format!(
                            "Access denied for user '{}' (using password: YES)",
                            client_username_str
                        ),
                    ));
                }
            } else {
                // 没有密码数据的情况（不应该发生）
                if !self.password.is_empty() {
                    return Err(io::Error::new(
                        io::ErrorKind::PermissionDenied,
                        format!(
                            "Access denied for user '{}' (no auth data)",
                            client_username_str
                        ),
                    ));
                }
            }
        }

        Ok(())
    }

    fn on_prepare(&mut self, query: &str, info: StatementMetaWriter<W>) -> io::Result<()> {
        // 生成一个简单的 statement ID
        let stmt_id = self.prepared_stmts.len() as u32;

        // 保存 SQL 用于后续流式执行
        self.prepared_stmts.insert(stmt_id, query.to_string());

        log::debug!("📝 [MySQL] Prepared statement {}: {}", stmt_id, query);

        // 返回 statement metadata（暂时不指定参数和列）
        info.reply(stmt_id, &[], &[])
    }

    fn on_execute(
        &mut self,
        id: u32,
        flags: u8,
        _params: ParamParser,
        results: QueryResultWriter<W>,
    ) -> io::Result<()> {
        const CURSOR_TYPE_READ_ONLY: u8 = 1;

        // 如果客户端请求流式游标，直接走 natural_order_executor（不通过 on_query）
        if flags == CURSOR_TYPE_READ_ONLY {
            log::info!(
                "🌊 [MySQL] Client requested streaming cursor (flags={})",
                flags
            );

            // 获取之前准备的 SQL
            let sql = match self.prepared_stmts.get(&id) {
                Some(sql) => sql.clone(),
                None => {
                    return results.error(
                        ErrorKind::ER_UNKNOWN_STMT_HANDLER,
                        format!("Unknown statement ID: {}", id).as_bytes(),
                    );
                }
            };

            log::info!("🌿 [MySQL] Streaming execution (direct path): {}", sql);

            // 直接调用 natural_order_executor 的流式版本
            // 使用 channel 传输数据，避免全部加载到内存
            return tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(async {
                    use crate::compute::natural_order_executor::NaturalOrderExecutor;

                    let executor = NaturalOrderExecutor::new(self.engine.clone());

                    match executor.execute_natural_cursor(&sql).await {
                        Ok(mut stream_result) => {
                            let schema = stream_result.schema.clone();

                            // 准备列定义
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

                            let mut row_writer = match results.start(&columns) {
                                Ok(w) => w,
                                Err(e) => {
                                    log::error!("Failed to start row writer: {}", e);
                                    return Err(e);
                                }
                            };

                            let mut total_rows = 0;
                            const FLUSH_INTERVAL: usize = 10000;

                            // 从 channel 逐批次接收数据
                            while let Some(batch_result) = stream_result.receiver.recv().await {
                                let batch = match batch_result {
                                    Ok(b) => b,
                                    Err(e) => {
                                        log::error!("❌ [MySQL] Stream error: {}", e);
                                        return row_writer.finish();
                                    }
                                };

                                log::debug!(
                                    "📦 [MySQL] Received batch with {} rows",
                                    batch.num_rows()
                                );

                                // 写入这个 batch 的所有行
                                for row_idx in 0..batch.num_rows() {
                                    for col_idx in 0..batch.num_columns() {
                                        let array = batch.column(col_idx);
                                        let value = format_arrow_value(array, row_idx);

                                        if let Err(e) = row_writer.write_col(value) {
                                            log::debug!("Client disconnected: {}", e);
                                            return Err(e);
                                        }
                                    }

                                    if let Err(e) = row_writer.end_row() {
                                        log::debug!(
                                            "Client disconnected at row {}: {}",
                                            total_rows,
                                            e
                                        );
                                        return Err(e);
                                    }

                                    total_rows += 1;

                                    // 每 10000 行记录进度
                                    if total_rows % FLUSH_INTERVAL == 0 {
                                        log::debug!("🌊 [MySQL] Streamed {} rows", total_rows);
                                    }
                                }
                            }

                            log::info!("✅ [MySQL] Stream completed: {} rows sent", total_rows);
                            row_writer.finish()
                        }
                        Err(e) => {
                            let msg = format!("Streaming query failed: {}", e);
                            log::error!("{}", msg);
                            results.error(ErrorKind::ER_UNKNOWN_ERROR, msg.as_bytes())
                        }
                    }
                })
            });
        } else {
            log::debug!("📦 [MySQL] Standard execute (flags={})", flags);
            results.error(
                ErrorKind::ER_NOT_SUPPORTED_YET,
                b"Non-streaming prepared statements not supported. Use text queries with LIMIT/OFFSET.",
            )
        }
    }

    fn on_close(&mut self, stmt: u32) {
        // 清理 prepared statement
        if self.prepared_stmts.remove(&stmt).is_some() {
            log::debug!("🗑️  [MySQL] Closed prepared statement {}", stmt);
        }
    }

    fn on_query(&mut self, query: &str, results: QueryResultWriter<W>) -> io::Result<()> {
        let query_trimmed = query.trim();

        // 去除 MySQL 注释 (/* ... */)
        let query_without_comment = if query_trimmed.starts_with("/*") {
            if let Some(end_pos) = query_trimmed.find("*/") {
                query_trimmed[end_pos + 2..].trim()
            } else {
                query_trimmed
            }
        } else {
            query_trimmed
        };

        // 规范化前缀用于判断语句类型（将多个空格合并为一个）
        // 只处理前 100 个字符，避免大型 INSERT 语句的性能问题
        let prefix_len = query_without_comment.len().min(100);
        let prefix = &query_without_comment[..prefix_len];

        // 将连续空格替换为单个空格，然后转小写
        let query_lower = prefix
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
            .to_lowercase();

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

        // 忽略客户端初始化命令和事务命令
        if query_lower.starts_with("set ")
            || query_lower.starts_with("select @@")
            || query_lower.starts_with("select version()")
            || query_lower.starts_with("select database()")
            || query_lower.starts_with("select $$")  // MySQL 客户端初始化查询
            || query_lower == "select 1"
            || query_lower.starts_with("show variables")
            || query_lower.starts_with("show session")
            || query_lower.starts_with("show collation")
            || query_lower.starts_with("show charset")
            || query_lower.starts_with("show warnings")
            || query_lower.starts_with("show errors")
            || query_lower.starts_with("show status")
            || query_lower.starts_with("show engines")
            || query_lower.starts_with("show plugins")
            || query_lower.starts_with("show processlist")
            || query_lower == "commit"
            || query_lower == "rollback"
            || query_lower == "begin"
            || query_lower.starts_with("start transaction")
        {
            // 特殊处理: SET TRACING 命令
            if query_lower.starts_with("set tracing") {
                use crate::utils::tracing::{disable_tracing, enable_tracing, toggle_tracing};

                if query_lower.contains("on")
                    || query_lower.contains("= 1")
                    || query_lower.contains("=1")
                {
                    enable_tracing();
                    return results.completed(0, 0);
                } else if query_lower.contains("off")
                    || query_lower.contains("= 0")
                    || query_lower.contains("=0")
                {
                    disable_tracing();
                    return results.completed(0, 0);
                } else if query_lower == "set tracing" || query_lower == "set tracing;" {
                    toggle_tracing();
                    return results.completed(0, 0);
                }
            }

            // 返回空结果或默认值
            if query_lower.starts_with("select @@")
                || query_lower.starts_with("select version()")
                || query_lower.starts_with("select database()")
            {
                // 处理多列的 @@variable 查询 (JDBC 初始化查询)
                return self.handle_session_variables_query(query_without_comment, results);
            }

            // 处理 SHOW VARIABLES 查询
            if query_lower.starts_with("show variables") {
                return self.handle_show_variables(query_without_comment, results);
            }

            // select $$ 和其他初始化命令直接返回成功
            // 避免发送复杂的结果集导致协议问题
            return results.completed(0, 0);
        }

        // flush tables 命令
        if query_lower.starts_with("flush tables") {
            log::info!("📨 [MySQL] Received query: {}", query_trimmed);
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
            // 🔧 特殊处理: SELECT @@variable 查询应该走 handle_session_variables_query
            // 不要走普通的 execute_query,因为 DataFusion 不支持 @@ 语法
            if query_without_comment.trim().to_lowercase().contains("@@") {
                log::info!("🔧 [MySQL] Redirecting @@variable query to session handler");
                return self.handle_session_variables_query(query_without_comment, results);
            }

            return tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(execute_query(
                    self.engine.clone(),
                    query,
                    results,
                ))
            });
        }

        // KILL QUERY 命令 (JDBC 超时取消)
        if query_lower.starts_with("kill query") || query_lower.starts_with("kill") {
            log::info!("📝 [MySQL] Received KILL command: {}", query_trimmed);
            // 在单连接场景下,KILL 命令意义不大,直接返回成功
            // 实际的查询取消由客户端断开连接来实现
            return results.completed(0, 0);
        }

        // 未识别的查询,打印日志
        log::warn!("Unsupported SQL query: {}", query_trimmed);

        results.error(
            ErrorKind::ER_NOT_SUPPORTED_YET,
            b"Only SELECT/INSERT/DELETE/CREATE TABLE/DROP TABLE/SHOW TABLES/SHOW DATABASES/SHOW PARTITIONS/DESCRIBE supported. Use LIMIT/OFFSET for pagination.",
        )
    }
}

impl CalmBackend {
    /// 处理 SELECT @@variable 查询 (JDBC 初始化)
    /// 解析查询中的所有列名并返回默认值
    fn handle_session_variables_query<W: io::Read + io::Write>(
        &self,
        query: &str,
        results: QueryResultWriter<'_, W>,
    ) -> io::Result<()> {
        // 解析 SELECT 和 FROM 之间的列定义
        let query_upper = query.to_uppercase();
        let select_start = query_upper.find("SELECT").unwrap_or(0) + 6;
        let from_pos = query_upper.find(" FROM ");

        let columns_str = if let Some(from_pos) = from_pos {
            &query[select_start..from_pos]
        } else {
            &query[select_start..]
        };

        // 解析列名和别名
        let mut fields = Vec::new();
        let mut values = Vec::new();

        for column_def in columns_str.split(',') {
            let column_def = column_def.trim();

            // 提取变量名和别名
            // 格式: @@session.variable_name AS alias 或 @@variable_name
            let (var_expr, alias) = if let Some(as_pos) = column_def.to_uppercase().rfind(" AS ") {
                let var_part = column_def[..as_pos].trim();
                let alias_part = column_def[as_pos + 4..].trim();
                (var_part, alias_part)
            } else {
                // 没有 AS,使用整个定义
                (
                    column_def,
                    column_def.split_whitespace().last().unwrap_or("value"),
                )
            };

            // 从变量表达式中提取变量名 (去除 @@ 前缀和 @@session./@@global. 前缀)
            let var_name = var_expr
                .trim_start_matches("@@")
                .trim_start_matches("session.")
                .trim_start_matches("global.")
                .to_lowercase();

            // 使用别名作为字段名
            fields.push(Field::new(alias, DataType::Utf8, false));

            log::debug!(
                "📊 [Session Var] '{}' -> var_name: '{}', alias: '{}'",
                column_def,
                var_name,
                alias
            );

            // 根据变量名(不是别名)返回值
            let default_value = match var_name.as_str() {
                "auto_increment_increment" => "1",
                "character_set_client"
                | "character_set_connection"
                | "character_set_results"
                | "character_set_server" => "utf8mb4",
                "collation_server" | "collation_connection" => "utf8mb4_general_ci",
                "init_connect" => "",
                "interactive_timeout" | "wait_timeout" => "28800",
                "language" => "/usr/share/mysql/english/",
                "license" => "MIT",
                "lower_case_table_names" => "0",
                "max_allowed_packet" => "67108864",
                "net_write_timeout" => "60",
                "performance_schema" => "0",
                "query_cache_size" => "0",
                "query_cache_type" => "OFF",
                "sql_mode" => "STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION",
                "system_time_zone" => "UTC",
                "time_zone" => "SYSTEM",
                "transaction_isolation" | "tx_isolation" | "transaction_read_only" => {
                    if var_name == "tx_isolation" {
                        log::warn!("⚠️  Deprecated variable 'tx_isolation' requested, returning compatible value");
                    }
                    "REPEATABLE-READ"
                }
                "version" | "version_comment" => {
                    // 返回 calm-{branch}-{commit} 格式
                    use std::process::Command;

                    let branch = Command::new("git")
                        .args(["rev-parse", "--abbrev-ref", "HEAD"])
                        .output()
                        .ok()
                        .and_then(|o| String::from_utf8(o.stdout).ok())
                        .map(|s| s.trim().to_string())
                        .unwrap_or_else(|| "unknown".to_string());

                    let commit = Command::new("git")
                        .args(["rev-parse", "--short", "HEAD"])
                        .output()
                        .ok()
                        .and_then(|o| String::from_utf8(o.stdout).ok())
                        .map(|s| s.trim().to_string())
                        .unwrap_or_else(|| "unknown".to_string());

                    // 静态分配，避免生命周期问题
                    Box::leak(format!("calm-{}-{}", branch, commit).into_boxed_str())
                }
                "autocommit" => "1",
                "auto_commit" => "1",
                _ => {
                    log::warn!(
                        "⚠️  Unknown session variable: '{}', returning default",
                        var_name
                    );
                    "0"
                }
            };

            values.push(default_value);
        }

        log::info!(
            "📊 [Session Var Result] Returning {} fields, {} values",
            fields.len(),
            values.len()
        );

        let schema = Arc::new(ArrowSchema::new(fields));
        let columns: Vec<ArrayRef> = values
            .iter()
            .map(|v| Arc::new(StringArray::from(vec![*v])) as ArrayRef)
            .collect();

        let batch = RecordBatch::try_new(schema.clone(), columns).map_err(io::Error::other)?;

        log::info!(
            "📊 [Session Var Result] RecordBatch: {} rows, {} columns",
            batch.num_rows(),
            batch.num_columns()
        );

        write_query_result(results, &schema, &[batch])
    }

    /// 处理 SHOW VARIABLES 查询
    /// 格式: SHOW VARIABLES LIKE 'pattern' 或 SHOW VARIABLES
    fn handle_show_variables<W: io::Read + io::Write>(
        &self,
        query: &str,
        results: QueryResultWriter<'_, W>,
    ) -> io::Result<()> {
        let query_upper = query.to_uppercase();

        // 提取 LIKE 子句中的模式
        let pattern = if let Some(like_pos) = query_upper.find(" LIKE ") {
            let pattern_part = &query[like_pos + 6..].trim();
            // 去除引号
            pattern_part
                .trim_matches('\'')
                .trim_matches('"')
                .to_lowercase()
        } else {
            // 没有 LIKE,返回所有变量
            "*".to_string()
        };

        log::debug!("📊 [SHOW VARIABLES] Pattern: '{}'", pattern);

        // 创建结果字段: Variable_name, Value
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("Variable_name", DataType::Utf8, false),
            Field::new("Value", DataType::Utf8, false),
        ]));

        let mut names: Vec<String> = Vec::new();
        let mut values: Vec<String> = Vec::new();

        // 根据模式匹配变量
        let mut add_var = |n: &str, v: &str| {
            if pattern == "*" || n.contains(&pattern) {
                names.push(n.to_string());
                values.push(v.to_string());
            }
        };

        // 添加所有支持的变量
        add_var("auto_increment_increment", "1");
        add_var("character_set_client", "utf8mb4");
        add_var("character_set_connection", "utf8mb4");
        add_var("character_set_results", "utf8mb4");
        add_var("character_set_server", "utf8mb4");
        add_var("collation_server", "utf8mb4_general_ci");
        add_var("collation_connection", "utf8mb4_general_ci");
        add_var("init_connect", "");
        add_var("interactive_timeout", "28800");
        add_var("language", "/usr/share/mysql/english/");
        add_var("license", "MIT");
        add_var("lower_case_table_names", "0");
        add_var("max_allowed_packet", "67108864");
        add_var("net_write_timeout", "60");
        add_var("performance_schema", "0");
        add_var("query_cache_size", "0");
        add_var("query_cache_type", "OFF");
        add_var("sql_mode", "STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION");
        add_var("system_time_zone", "UTC");
        add_var("time_zone", "SYSTEM");
        add_var("transaction_isolation", "REPEATABLE-READ");
        add_var("transaction_read_only", "OFF");
        add_var("wait_timeout", "28800");
        add_var("version", "8.0.32-calm");
        add_var("version_comment", "Calm Database");
        add_var("autocommit", "1");

        log::info!("📊 [SHOW VARIABLES] Returning {} variables", names.len());

        let name_array = Arc::new(StringArray::from(names)) as ArrayRef;
        let value_array = Arc::new(StringArray::from(values)) as ArrayRef;

        let batch = RecordBatch::try_new(schema.clone(), vec![name_array, value_array])
            .map_err(io::Error::other)?;

        write_query_result(results, &schema, &[batch])
    }
}

/// 执行 SELECT 查询
async fn execute_query<W: io::Read + io::Write>(
    engine: Arc<Engine>,
    query: &str,
    results: QueryResultWriter<'_, W>,
) -> io::Result<()> {
    use crate::utils::tracing::{is_tracing_enabled, TraceContext};

    // 创建追踪上下文
    let trace_ctx = if is_tracing_enabled() {
        Some(TraceContext::new(format!(
            "query-{}",
            query.chars().take(50).collect::<String>()
        )))
    } else {
        None
    };

    // SQL 解析和执行（流式版本）
    use futures::StreamExt;

    let mut stream = {
        let _span = trace_ctx.as_ref().map(|ctx| {
            let guard = ctx.span("SQL Execution");
            guard.metadata("query", query);
            guard
        });

        match engine.clone().execute_sql_stream(query).await {
            Ok(stream) => stream,
            Err(e) => {
                let msg = format!("SQL execution failed: {}", e);
                log::error!("SQL Error: {}", msg);
                return results.error(ErrorKind::ER_PARSE_ERROR, msg.as_bytes());
            }
        }
    };

    log::debug!("MySQL Query (streaming): {}", query);

    // 🌊 流式发送结果
    let mut total_rows = 0;
    let mut batches = Vec::new();

    while let Some(batch_result) = stream.next().await {
        let batch = match batch_result {
            Ok(b) => b,
            Err(e) => {
                let msg = format!("Stream error: {}", e);
                log::error!("{}", msg);
                return results.error(ErrorKind::ER_UNKNOWN_ERROR, msg.as_bytes());
            }
        };

        total_rows += batch.num_rows();
        batches.push(batch);

        // 每 100 个 batch 记录一次进度
        if batches.len() % 100 == 0 {
            log::debug!(
                "🌊 [MySQL Stream] Received {} batches, {} total rows",
                batches.len(),
                total_rows
            );
        }
    }

    log::debug!(
        "✅ [MySQL Stream] Completed: {} batches, {} total rows",
        batches.len(),
        total_rows
    );

    // 序列化结果
    let schema = if batches.is_empty() {
        use datafusion::arrow::datatypes::Schema;
        std::sync::Arc::new(Schema::empty())
    } else {
        batches[0].schema()
    };

    let write_result = {
        let _span = trace_ctx.as_ref().map(|ctx| {
            let guard = ctx.span("Write Result");
            guard.metadata("rows", total_rows.to_string());
            guard.metadata("batches", batches.len().to_string());
            guard
        });

        write_query_result(results, &schema, &batches)
    };

    // 打印追踪报告
    if let Some(ctx) = trace_ctx {
        ctx.report();
    }

    write_result
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
                description: None,
                default_value: None,
                nullable: true,
            },
            "TINYINT" | "INT8" => FieldOption::I8 {
                name: col_name,
                index: true,
                description: None,
                default_value: None,
                nullable: true,
            },
            "SMALLINT" | "INT16" => FieldOption::I16 {
                name: col_name,
                index: true,
                description: None,
                default_value: None,
                nullable: true,
            },
            "BIGINT" | "INT64" => FieldOption::I64 {
                name: col_name,
                index: true,
                description: None,
                default_value: None,
                nullable: true,
            },
            "TINYINT UNSIGNED" | "UINT8" => FieldOption::U8 {
                name: col_name,
                index: true,
                description: None,
                default_value: None,
                nullable: true,
            },
            "SMALLINT UNSIGNED" | "UINT16" => FieldOption::U16 {
                name: col_name,
                index: true,
                description: None,
                default_value: None,
                nullable: true,
            },
            "INT UNSIGNED" | "UINT32" => FieldOption::U32 {
                name: col_name,
                index: true,
                description: None,
                default_value: None,
                nullable: true,
            },
            "BIGINT UNSIGNED" | "UINT64" => FieldOption::U64 {
                name: col_name,
                index: true,
                description: None,
                default_value: None,
                nullable: true,
            },
            "FLOAT" | "FLOAT32" => FieldOption::F32 {
                name: col_name,
                index: true,
                description: None,
                default_value: None,
                nullable: true,
            },
            "DOUBLE" | "FLOAT64" => FieldOption::F64 {
                name: col_name,
                index: true,
                description: None,
                default_value: None,
                nullable: true,
            },
            "BOOL" | "BOOLEAN" => FieldOption::Boolean {
                name: col_name,
                index: true,
                description: None,
                default_value: None,
                nullable: true,
            },
            "TIMESTAMP" | "DATETIME" => FieldOption::Timestamp {
                name: col_name,
                index: true,
                format: Some("iso8601".to_string()),
                description: None,
                default_value: None,
                nullable: true,
            },
            "TEXT" | "STRING" | "VARCHAR" | "CHAR" => FieldOption::Keyword {
                name: col_name,
                index: true,
                is_array: false,
                persist_option: None,
                case_sensitive: true,
                description: None,
                default_value: None,
                nullable: true,
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
    let schema = Schema::new(
        table_name.clone(),
        primary_key,
        true,
        fields,
        Default::default(),
        None, // description
    );

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

    // 使用流式查询然后 collect
    use futures::StreamExt;
    let mut stream = match engine.clone().execute_sql_stream(&select_query).await {
        Ok(s) => s,
        Err(e) => {
            let msg = format!("Query execution failed: {}", e);
            return results.error(ErrorKind::ER_UNKNOWN_ERROR, msg.as_bytes());
        }
    };

    let mut batches = Vec::new();
    while let Some(batch_result) = stream.next().await {
        let batch = match batch_result {
            Ok(b) => b,
            Err(e) => {
                let msg = format!("Stream error: {}", e);
                return results.error(ErrorKind::ER_UNKNOWN_ERROR, msg.as_bytes());
            }
        };
        batches.push(batch);
    }

    if batches.is_empty() {
        return results.completed(0, 0);
    }

    // 合并 batches
    use datafusion::arrow::compute::concat_batches;
    let result = if batches.len() == 1 {
        batches.into_iter().next().unwrap()
    } else {
        let schema = batches[0].schema();
        match concat_batches(&schema, &batches) {
            Ok(b) => b,
            Err(e) => {
                let msg = format!("Failed to concat batches: {}", e);
                return results.error(ErrorKind::ER_UNKNOWN_ERROR, msg.as_bytes());
            }
        }
    };

    if result.num_rows() == 0 {
        return results.completed(0, 0);
    }

    // 提取主键值
    let pk_field =
        meta.schema.primary_key.as_ref().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "Table has no primary key")
        })?;

    let mut total_deleted = 0u64;

    let batch = result;
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
    log::debug!(
        "write_query_result: {} columns, {} batches",
        schema.fields().len(),
        batches.len()
    );

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

    const FLUSH_INTERVAL: usize = 1000; // 每 1000 行 flush 一次,实现背压
    let mut rows_written = 0;

    for batch in batches {
        for row_idx in 0..batch.num_rows() {
            // 写入一行的所有列
            for col_idx in 0..batch.num_columns() {
                let array = batch.column(col_idx);
                let value = format_arrow_value(array, row_idx);

                // 检查写入是否成功 - 客户端断开会返回错误
                if let Err(e) = row_writer.write_col(value) {
                    log::debug!("Client disconnected or write failed: {}", e);
                    return Err(e);
                }
            }

            // 结束当前行
            if let Err(e) = row_writer.end_row() {
                log::debug!("Client disconnected at row {}: {}", rows_written, e);
                return Err(e);
            }

            rows_written += 1;

            // 每 FLUSH_INTERVAL 行强制 flush,实现 TCP 背压
            // 如果客户端消费慢,这里会阻塞
            if rows_written % FLUSH_INTERVAL == 0 {
                // msql_srv 的 RowWriter 没有 flush 方法,但 end_row 会写入
                // TCP socket 会自动实现背压
                log::debug!("Sent {} rows (may block if client is slow)", rows_written);
            }
        }
    }

    log::debug!("Finished sending {} rows", rows_written);
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
            // 转换为数字字符串 "1" 或 "0"，与 MySQL TINYINT(1) 行为一致
            if arr.value(index) { "1" } else { "0" }.to_string()
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
