use std::io;
use std::net::TcpListener;
use std::sync::mpsc;
use std::sync::Arc;

use datafusion::prelude::SessionContext;
use msql_srv::{
    Column as MysqlColumn, ColumnFlags, ColumnType, ErrorKind, MysqlIntermediary, MysqlShim,
    ParamParser, QueryResultWriter, StatementMetaWriter,
};

use crate::partition::Partition;
use crate::schema::compute::PartitionTableProvider;

/// MySQL协议服务器
pub struct MysqlServer {
    partition: Arc<Partition>,
}

impl MysqlServer {
    pub fn new(partition: Arc<Partition>) -> Self {
        Self { partition }
    }

    /// 启动MySQL服务器
    pub async fn start(&self, addr: &str) -> io::Result<()> {
        let listener = TcpListener::bind(addr)?;
        println!("MySQL server listening on {}", addr);

        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    let partition = self.partition.clone();
                    tokio::spawn(async move {
                        let backend = CalmBackend::new(partition);
                        if let Err(e) = MysqlIntermediary::run_on_tcp(backend, stream) {
                            eprintln!("MySQL connection error: {}", e);
                        }
                    });
                }
                Err(e) => {
                    eprintln!("Connection error: {}", e);
                }
            }
        }

        Ok(())
    }
}

/// Calm数据库后端实现
struct CalmBackend {
    partition: Arc<Partition>,
}

impl CalmBackend {
    fn new(partition: Arc<Partition>) -> Self {
        Self { partition }
    }
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
        // 处理特殊的MySQL命令
        let query_lower = query.trim().to_lowercase();

        // 处理USE database命令
        if query_lower.starts_with("use ") {
            return results.completed(0, 0);
        }

        // 处理SHOW DATABASES
        if query_lower == "show databases" {
            let cols = vec![MysqlColumn {
                table: "".to_string(),
                column: "Database".to_string(),
                coltype: ColumnType::MYSQL_TYPE_VAR_STRING,
                colflags: ColumnFlags::empty(),
            }];

            let mut writer = results.start(&cols)?;
            writer.write_row(&["calm"])?;
            return writer.finish();
        }

        // 处理SHOW TABLES
        if query_lower == "show tables" {
            let cols = vec![MysqlColumn {
                table: "".to_string(),
                column: "Tables_in_calm".to_string(),
                coltype: ColumnType::MYSQL_TYPE_VAR_STRING,
                colflags: ColumnFlags::empty(),
            }];

            let mut writer = results.start(&cols)?;
            writer.write_row(&["data"])?;
            return writer.finish();
        }

        // 检查是否是INSERT/UPDATE/DELETE语句
        if query_lower.starts_with("insert") {
            return results.error(
                ErrorKind::ER_NOT_SUPPORTED_YET,
                b"INSERT is not yet supported. Please use Partition::upsert() API directly.",
            );
        }

        if query_lower.starts_with("update") {
            return results.error(
                ErrorKind::ER_NOT_SUPPORTED_YET,
                b"UPDATE is not yet supported. Please use Partition::upsert() API directly.",
            );
        }

        if query_lower.starts_with("delete") {
            return results.error(
                ErrorKind::ER_NOT_SUPPORTED_YET,
                b"DELETE is not yet supported. Please use Partition::delete() API directly.",
            );
        }

        // 执行SQL查询
        let partition = self.partition.clone();
        let query_str = query.to_string();

        // 使用channel来传递结果
        let (tx, rx) = mpsc::channel();

        std::thread::spawn(move || {
            // 创建新的runtime来执行查询
            let rt = tokio::runtime::Runtime::new().unwrap();
            let result = rt.block_on(async { execute_query(partition, &query_str).await });
            let _ = tx.send(result);
        });

        // 接收结果
        match rx.recv() {
            Ok(Ok((schema, batches))) => write_query_result(results, schema, batches),
            Ok(Err(e)) => {
                // 提供更友好的错误消息
                let error_msg = if e.contains("table") && e.contains("not found") {
                    "Table 'data' not found. Available tables: data".to_string()
                } else if e.contains("column") {
                    format!("Column error: {}. Please check your column names.", e)
                } else if e.contains("syntax") || e.contains("SQL") {
                    format!("SQL syntax error: {}", e)
                } else {
                    format!("Query execution failed: {}", e)
                };
                results.error(ErrorKind::ER_UNKNOWN_ERROR, error_msg.as_bytes())
            }
            Err(_) => results.error(
                ErrorKind::ER_UNKNOWN_ERROR,
                b"Internal error: Query execution thread failed",
            ),
        }
    }
}

/// 执行SQL查询
async fn execute_query(
    partition: Arc<Partition>,
    query: &str,
) -> Result<
    (
        datafusion::arrow::datatypes::SchemaRef,
        Vec<datafusion::arrow::record_batch::RecordBatch>,
    ),
    String,
> {
    // 创建DataFusion上下文
    let ctx = SessionContext::new();

    // 注册Partition为表
    let provider = Arc::new(PartitionTableProvider::new(partition));
    ctx.register_table("data", provider)
        .map_err(|e| format!("Failed to register table: {}", e))?;

    // 执行查询
    let df = ctx
        .sql(query)
        .await
        .map_err(|e| format!("SQL error: {}", e))?;
    let batches = df
        .collect()
        .await
        .map_err(|e| format!("Query execution error: {}", e))?;

    let schema = if !batches.is_empty() {
        batches[0].schema()
    } else {
        Arc::new(datafusion::arrow::datatypes::Schema::empty())
    };

    Ok((schema, batches))
}

/// 将查询结果写入MySQL响应
fn write_query_result<W: io::Read + io::Write>(
    results: QueryResultWriter<W>,
    schema: datafusion::arrow::datatypes::SchemaRef,
    batches: Vec<datafusion::arrow::record_batch::RecordBatch>,
) -> io::Result<()> {
    use datafusion::arrow::datatypes::DataType;

    // 转换Arrow schema为MySQL columns
    let cols: Vec<MysqlColumn> = schema
        .fields()
        .iter()
        .map(|field| {
            let coltype = match field.data_type() {
                DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                    ColumnType::MYSQL_TYPE_LONGLONG
                }
                DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
                    ColumnType::MYSQL_TYPE_LONGLONG
                }
                DataType::Float32 | DataType::Float64 => ColumnType::MYSQL_TYPE_DOUBLE,
                DataType::Utf8 | DataType::LargeUtf8 => ColumnType::MYSQL_TYPE_VAR_STRING,
                DataType::Boolean => ColumnType::MYSQL_TYPE_TINY,
                _ => ColumnType::MYSQL_TYPE_VAR_STRING,
            };

            MysqlColumn {
                table: "data".to_string(),
                column: field.name().clone(),
                coltype,
                colflags: ColumnFlags::empty(),
            }
        })
        .collect();

    let mut writer = results.start(&cols)?;

    // 写入数据行
    for batch in batches {
        let num_rows = batch.num_rows();

        for row_idx in 0..num_rows {
            let mut row_values: Vec<String> = Vec::new();

            for col_idx in 0..batch.num_columns() {
                let column = batch.column(col_idx);
                let value = format_arrow_value(column, row_idx);
                row_values.push(value);
            }

            // 转换为&str切片
            let row_refs: Vec<&str> = row_values.iter().map(|s| s.as_str()).collect();
            writer.write_row(row_refs.as_slice())?;
        }
    }

    writer.finish()
}

/// 格式化Arrow值为字符串
fn format_arrow_value(column: &datafusion::arrow::array::ArrayRef, row_idx: usize) -> String {
    use datafusion::arrow::array::*;
    use datafusion::arrow::datatypes::DataType;

    if column.is_null(row_idx) {
        return "NULL".to_string();
    }

    match column.data_type() {
        DataType::Int8 => {
            let array = column.as_any().downcast_ref::<Int8Array>().unwrap();
            array.value(row_idx).to_string()
        }
        DataType::Int16 => {
            let array = column.as_any().downcast_ref::<Int16Array>().unwrap();
            array.value(row_idx).to_string()
        }
        DataType::Int32 => {
            let array = column.as_any().downcast_ref::<Int32Array>().unwrap();
            array.value(row_idx).to_string()
        }
        DataType::Int64 => {
            let array = column.as_any().downcast_ref::<Int64Array>().unwrap();
            array.value(row_idx).to_string()
        }
        DataType::UInt8 => {
            let array = column.as_any().downcast_ref::<UInt8Array>().unwrap();
            array.value(row_idx).to_string()
        }
        DataType::UInt16 => {
            let array = column.as_any().downcast_ref::<UInt16Array>().unwrap();
            array.value(row_idx).to_string()
        }
        DataType::UInt32 => {
            let array = column.as_any().downcast_ref::<UInt32Array>().unwrap();
            array.value(row_idx).to_string()
        }
        DataType::UInt64 => {
            let array = column.as_any().downcast_ref::<UInt64Array>().unwrap();
            array.value(row_idx).to_string()
        }
        DataType::Float32 => {
            let array = column.as_any().downcast_ref::<Float32Array>().unwrap();
            array.value(row_idx).to_string()
        }
        DataType::Float64 => {
            let array = column.as_any().downcast_ref::<Float64Array>().unwrap();
            array.value(row_idx).to_string()
        }
        DataType::Utf8 => {
            let array = column.as_any().downcast_ref::<StringArray>().unwrap();
            array.value(row_idx).to_string()
        }
        DataType::Boolean => {
            let array = column.as_any().downcast_ref::<BooleanArray>().unwrap();
            if array.value(row_idx) {
                "1".to_string()
            } else {
                "0".to_string()
            }
        }
        _ => "NULL".to_string(),
    }
}
