//! INFORMATION_SCHEMA 虚拟表查询执行器
//!
//! 专门处理 INFORMATION_SCHEMA.TABLES 等元数据查询
//! 完全独立的逻辑，不影响现有的查询执行流程

use datafusion::arrow::array::{
    Int32Array, Int64Array, RecordBatch, StringArray, TimestampSecondArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::prelude::*;
use futures::stream;
use std::sync::Arc;

use crate::catalog::Catalog;
use crate::compute::udf::fulltext_udf::register_fulltext_udfs;
use crate::engine::Engine;
use crate::utils::error::{CoreError, CoreResult};

pub struct InformationSchemaExecutor {
    engine: Arc<Engine>,
    catalog: Option<Arc<Catalog>>,
}

impl InformationSchemaExecutor {
    pub fn new(engine: Arc<Engine>, catalog: Arc<Catalog>) -> Self {
        Self {
            engine,
            catalog: Some(catalog),
        }
    }

    pub fn new_simple(engine: Arc<Engine>) -> Self {
        Self {
            engine,
            catalog: None,
        }
    }

    /// 执行 INFORMATION_SCHEMA 查询（流式）
    pub async fn execute_stream(&self, sql: &str) -> CoreResult<SendableRecordBatchStream> {
        log::info!("🔍 [INFORMATION_SCHEMA] Executing query");

        // 🎯 特殊处理：JDBC 元数据查询（避免 DataFusion 的 CASE/HAVING bug）
        let sql_upper = sql.to_uppercase();
        let is_tables_query = sql_upper.contains("FROM INFORMATION_SCHEMA.TABLES");
        let is_columns_query = sql_upper.contains("FROM INFORMATION_SCHEMA.COLUMNS");
        let is_statistics_query = sql_upper.contains("FROM INFORMATION_SCHEMA.STATISTICS");
        let has_jdbc_fields = (sql_upper.contains("TABLE_CAT")
            || sql_upper.contains("TABLE_SCHEM"))
            && (sql_upper.contains("REMARKS") || sql_upper.contains("REF_GENERATION"));
        let has_case = sql_upper.contains("CASE WHEN");
        let has_having = sql_upper.contains(" HAVING ");

        // JDBC getTables() 快速路径
        if is_tables_query && (has_jdbc_fields || has_case || has_having) {
            log::info!("🎯 [INFORMATION_SCHEMA] Using JDBC getTables() fast path");
            let result = self.execute_jdbc_get_tables(sql).await?;
            // 转换为 Stream
            use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
            let schema = result.schema();
            let stream = stream::once(async move { Ok(result) });
            return Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)));
        }

        // JDBC getColumns() 快速路径 - 返回完整列信息
        // 检测条件：查询 COLUMNS 表（无论是否有 CASE）
        if is_columns_query {
            log::info!("🎯 [INFORMATION_SCHEMA] Using JDBC getColumns() fast path");
            let result = self.execute_jdbc_get_columns_empty().await?;
            use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
            let schema = result.schema();
            let stream = stream::once(async move { Ok(result) });
            return Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)));
        }

        // JDBC getPrimaryKeys() 快速路径 - 返回空结果
        // TODO: 实现完整的主键信息支持
        if is_statistics_query {
            log::info!(
                "🎯 [INFORMATION_SCHEMA] Using JDBC getPrimaryKeys() fast path (empty result)"
            );
            let result = self.execute_jdbc_get_primary_keys_empty().await?;
            use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
            let schema = result.schema();
            let stream = stream::once(async move { Ok(result) });
            return Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)));
        }

        // 提取数据库名
        let db_name = Self::extract_database_name(sql);
        log::info!("🔍 [INFORMATION_SCHEMA] Database: {}", db_name);

        // 创建独立的 SessionContext
        let config = SessionConfig::new().with_information_schema(true);
        let ctx = SessionContext::new_with_config(config);
        let _fulltext_context = register_fulltext_udfs(&ctx);

        // 注册虚拟表
        self.register_tables(&ctx, &db_name).await?;

        // SQL 重写
        let rewritten_sql = self.rewrite_sql(sql);
        log::info!("🔄 [INFORMATION_SCHEMA] Rewritten SQL: {}", rewritten_sql);

        // 执行查询
        let df = ctx
            .sql(&rewritten_sql)
            .await
            .map_err(|e| CoreError::Internal(format!("Query execution error: {}", e)))?;

        // 🔑 返回 Stream
        let stream = df
            .execute_stream()
            .await
            .map_err(|e| CoreError::Internal(format!("Failed to execute stream: {}", e)))?;

        log::info!("✅ [INFORMATION_SCHEMA] Stream ready");
        Ok(stream)
    }

    /// 注册虚拟表
    async fn register_tables(&self, ctx: &SessionContext, db_name: &str) -> CoreResult<()> {
        let tables_schema = Arc::new(Schema::new(vec![
            Field::new("table_catalog", DataType::Utf8, true),
            Field::new("table_schema", DataType::Utf8, false),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("table_type", DataType::Utf8, false),
            Field::new("engine", DataType::Utf8, true),
            Field::new("version", DataType::Int64, true),
            Field::new("row_format", DataType::Utf8, true),
            Field::new("table_rows", DataType::Int64, true),
            Field::new("avg_row_length", DataType::Int64, true),
            Field::new("data_length", DataType::Int64, true),
            Field::new("max_data_length", DataType::Int64, true),
            Field::new("index_length", DataType::Int64, true),
            Field::new("data_free", DataType::Int64, true),
            Field::new("auto_increment", DataType::Int64, true),
            Field::new(
                "create_time",
                DataType::Timestamp(TimeUnit::Second, None),
                true,
            ),
            Field::new(
                "update_time",
                DataType::Timestamp(TimeUnit::Second, None),
                true,
            ),
            Field::new(
                "check_time",
                DataType::Timestamp(TimeUnit::Second, None),
                true,
            ),
            Field::new("table_collation", DataType::Utf8, true),
            Field::new("checksum", DataType::Int64, true),
            Field::new("create_options", DataType::Utf8, true),
            Field::new("table_comment", DataType::Utf8, true),
        ]));

        // 使用 catalog 的 list_tables 方法
        let table_names = if let Some(catalog) = &self.catalog {
            catalog.list_tables().await
        } else {
            vec![]
        };
        let row_count = table_names.len();

        let tables_batch = RecordBatch::try_new(
            tables_schema.clone(),
            vec![
                Arc::new(StringArray::from(vec![Some("def"); row_count])),
                Arc::new(StringArray::from(vec![db_name; row_count])),
                Arc::new(StringArray::from(table_names)),
                Arc::new(StringArray::from(vec!["BASE TABLE"; row_count])),
                Arc::new(StringArray::from(vec![Some("CalmCore"); row_count])),
                Arc::new(Int64Array::from(vec![Some(10i64); row_count])),
                Arc::new(StringArray::from(vec![Some("Dynamic"); row_count])),
                Arc::new(Int64Array::from(vec![None::<i64>; row_count])),
                Arc::new(Int64Array::from(vec![None::<i64>; row_count])),
                Arc::new(Int64Array::from(vec![None::<i64>; row_count])),
                Arc::new(Int64Array::from(vec![None::<i64>; row_count])),
                Arc::new(Int64Array::from(vec![None::<i64>; row_count])),
                Arc::new(Int64Array::from(vec![None::<i64>; row_count])),
                Arc::new(Int64Array::from(vec![None::<i64>; row_count])),
                Arc::new(TimestampSecondArray::from(vec![None::<i64>; row_count])),
                Arc::new(TimestampSecondArray::from(vec![None::<i64>; row_count])),
                Arc::new(TimestampSecondArray::from(vec![None::<i64>; row_count])),
                Arc::new(StringArray::from(vec![
                    Some("utf8mb4_general_ci");
                    row_count
                ])),
                Arc::new(Int64Array::from(vec![None::<i64>; row_count])),
                Arc::new(StringArray::from(vec![None::<String>; row_count])),
                Arc::new(StringArray::from(vec![None::<String>; row_count])),
            ],
        )
        .map_err(|e| CoreError::Internal(format!("Failed to create TABLES batch: {}", e)))?;

        let mem_table =
            datafusion::datasource::MemTable::try_new(tables_schema, vec![vec![tables_batch]])
                .map_err(|e| CoreError::Internal(format!("Failed to create MemTable: {}", e)))?;

        ctx.register_table("information_schema_tables", Arc::new(mem_table))
            .map_err(|e| {
                CoreError::Internal(format!(
                    "Failed to register information_schema_tables: {}",
                    e
                ))
            })?;

        log::info!("✅ [INFORMATION_SCHEMA] Registered {} tables", row_count);
        Ok(())
    }

    /// SQL 重写：处理字段名、HAVING 子句等
    fn rewrite_sql(&self, sql: &str) -> String {
        let mut rewritten = sql
            .replace("INFORMATION_SCHEMA.TABLES", "information_schema_tables")
            .replace("information_schema.tables", "information_schema_tables");

        // 字段名转小写
        let fields = vec![
            "TABLE_CATALOG",
            "TABLE_SCHEMA",
            "TABLE_NAME",
            "TABLE_TYPE",
            "ENGINE",
            "VERSION",
            "ROW_FORMAT",
            "TABLE_ROWS",
            "AVG_ROW_LENGTH",
            "DATA_LENGTH",
            "MAX_DATA_LENGTH",
            "INDEX_LENGTH",
            "DATA_FREE",
            "AUTO_INCREMENT",
            "CREATE_TIME",
            "UPDATE_TIME",
            "CHECK_TIME",
            "TABLE_COLLATION",
            "CHECKSUM",
            "CREATE_OPTIONS",
            "TABLE_COMMENT",
        ];

        for field in fields {
            rewritten = rewritten.replace(field, &field.to_lowercase());
        }

        // 修复 NULL 类型：DataFusion 需要类型提示
        // NULL AS column_name → CAST(NULL AS VARCHAR) AS column_name
        rewritten = rewritten.replace(" NULL AS ", " CAST(NULL AS VARCHAR) AS ");
        rewritten = rewritten.replace(", NULL,", ", CAST(NULL AS VARCHAR),");

        // 处理 HAVING 子句
        if rewritten.contains(" HAVING ") && !rewritten.contains("GROUP BY") {
            rewritten = self.rewrite_having_clause(&rewritten);
        }

        rewritten
    }

    /// 重写 HAVING 子句：简单地转换为 AND 条件
    /// MySQL 允许 HAVING 不配合 GROUP BY 使用，相当于 WHERE 的附加条件
    fn rewrite_having_clause(&self, sql: &str) -> String {
        sql.replace(" HAVING ", " AND ")
    }

    /// 提取数据库名（从 WHERE 子句中提取）
    fn extract_database_name(sql: &str) -> String {
        let sql_upper = sql.to_uppercase();

        // 找到 WHERE 子句的位置
        if let Some(where_pos) = sql_upper.find(" WHERE ") {
            // 只在 WHERE 子句后面查找
            let after_where = &sql[where_pos + 7..];
            let after_where_upper = &sql_upper[where_pos + 7..];

            // 找到第一个 TABLE_SCHEMA = '...'
            let patterns = [("TABLE_SCHEMA = '", 16), ("TABLE_SCHEMA='", 15)];

            for (pattern, pattern_len) in &patterns {
                if let Some(pos) = after_where_upper.find(pattern) {
                    let after_pattern = &after_where[pos + pattern_len..];
                    if let Some(end_pos) = after_pattern.find('\'') {
                        let db_name = &after_pattern[..end_pos];
                        return db_name.to_string();
                    }
                }
            }
        }
        "calm".to_string()
    }

    /// 快速路径：直接处理 JDBC getTables() 查询
    /// 避免 DataFusion 嵌套 CASE 表达式的 bug
    async fn execute_jdbc_get_tables(&self, sql: &str) -> CoreResult<RecordBatch> {
        // 提取数据库名（schema）
        let db_name = Self::extract_database_name(sql);
        log::info!("🎯 [JDBC getTables] Schema: {}", db_name);

        // 获取所有表名
        let table_names = if let Some(catalog) = &self.catalog {
            catalog.list_tables().await
        } else {
            vec![] // fallback
        };

        // 过滤：只返回匹配 schema 的表
        // 注意：这里假设 table_name 本身就是 schema 名（taxi_trips）
        // 或者 db_name 是表名的一部分
        let filtered_tables: Vec<String> = if db_name == "calm" {
            // 返回所有表
            table_names
        } else {
            // 只返回匹配的表
            table_names
                .into_iter()
                .filter(|t| t == &db_name || t.contains(&db_name))
                .collect()
        };

        log::info!("🎯 [JDBC getTables] Found {} tables", filtered_tables.len());

        // 构建 JDBC 标准结果
        let row_count = filtered_tables.len();

        // 创建 Schema：JDBC getTables() 标准字段
        let schema = Arc::new(Schema::new(vec![
            Field::new("table_cat", DataType::Utf8, true), // TABLE_SCHEMA AS TABLE_CAT
            Field::new("table_schem", DataType::Utf8, true), // NULL AS TABLE_SCHEM
            Field::new("table_name", DataType::Utf8, false), // TABLE_NAME
            Field::new("table_type", DataType::Utf8, false), // 'TABLE'
            Field::new("remarks", DataType::Utf8, true),   // TABLE_COMMENT AS REMARKS
            Field::new("type_cat", DataType::Utf8, true),  // NULL AS TYPE_CAT
            Field::new("type_schem", DataType::Utf8, true), // NULL AS TYPE_SCHEM
            Field::new("type_name", DataType::Utf8, true), // NULL AS TYPE_NAME
            Field::new("self_referencing_col_name", DataType::Utf8, true), // NULL
            Field::new("ref_generation", DataType::Utf8, true), // NULL
        ]));

        // 构建数据
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                // TABLE_CAT: 使用数据库名
                Arc::new(StringArray::from(vec![db_name.as_str(); row_count])),
                // TABLE_SCHEM: NULL
                Arc::new(StringArray::from(vec![None::<String>; row_count])),
                // TABLE_NAME: 实际表名
                Arc::new(StringArray::from(filtered_tables)),
                // TABLE_TYPE: 全部返回 'TABLE' (因为 HAVING 过滤了 TABLE_TYPE IN ('TABLE','VIEW'))
                Arc::new(StringArray::from(vec!["TABLE"; row_count])),
                // REMARKS: NULL
                Arc::new(StringArray::from(vec![None::<String>; row_count])),
                // TYPE_CAT: NULL
                Arc::new(StringArray::from(vec![None::<String>; row_count])),
                // TYPE_SCHEM: NULL
                Arc::new(StringArray::from(vec![None::<String>; row_count])),
                // TYPE_NAME: NULL
                Arc::new(StringArray::from(vec![None::<String>; row_count])),
                // SELF_REFERENCING_COL_NAME: NULL
                Arc::new(StringArray::from(vec![None::<String>; row_count])),
                // REF_GENERATION: NULL
                Arc::new(StringArray::from(vec![None::<String>; row_count])),
            ],
        )
        .map_err(|e| CoreError::Internal(format!("Failed to create result batch: {}", e)))?;

        log::info!("✅ [JDBC getTables] Returning {} rows", row_count);

        Ok(batch)
    }

    /// 快速路径：返回 JDBC getColumns() 结果，包含真实的列信息
    async fn execute_jdbc_get_columns_empty(&self) -> CoreResult<RecordBatch> {
        log::info!("🎯 [JDBC getColumns] Fetching column information");

        // 从 Catalog 获取所有表的 schema
        let tables = if let Some(catalog) = &self.catalog {
            catalog.list_tables().await
        } else {
            vec![]
        };

        // 准备结果数据
        let mut table_cats: Vec<Option<String>> = Vec::new();
        let mut table_schems: Vec<Option<String>> = Vec::new();
        let mut table_names: Vec<String> = Vec::new();
        let mut column_names: Vec<String> = Vec::new();
        let mut data_types: Vec<i32> = Vec::new();
        let mut type_names: Vec<String> = Vec::new();
        let mut column_sizes: Vec<Option<i32>> = Vec::new();
        let mut buffer_lengths: Vec<Option<i32>> = Vec::new();
        let mut decimal_digits_vec: Vec<Option<i32>> = Vec::new();
        let mut num_prec_radixes: Vec<Option<i32>> = Vec::new();
        let mut nullables: Vec<i32> = Vec::new();
        let mut remarks_vec: Vec<Option<String>> = Vec::new();
        let mut column_defs: Vec<Option<String>> = Vec::new();
        let mut sql_data_types: Vec<Option<i32>> = Vec::new();
        let mut sql_datetime_subs: Vec<Option<i32>> = Vec::new();
        let mut char_octet_lengths: Vec<Option<i32>> = Vec::new();
        let mut ordinal_positions: Vec<i32> = Vec::new();
        let mut is_nullables: Vec<String> = Vec::new();
        let mut scope_catalogs: Vec<Option<String>> = Vec::new();
        let mut scope_schemas: Vec<Option<String>> = Vec::new();
        let mut scope_tables: Vec<Option<String>> = Vec::new();
        let mut source_data_types: Vec<Option<i32>> = Vec::new();
        let mut is_autoincrements: Vec<String> = Vec::new();
        let mut is_generatedcolumns: Vec<String> = Vec::new();
        let mut column_comments: Vec<Option<String>> = Vec::new();
        let mut extras: Vec<String> = Vec::new();

        // 遍历所有表并获取列信息
        for table_name in &tables {
            // 获取表的 schema
            let table = if let Some(catalog) = &self.catalog {
                match catalog.get_or_load_table(table_name).await {
                    Ok(t) => t,
                    Err(_) => continue,
                }
            } else {
                continue;
            };

            let schema = &table.table.schema;

            // 遍历所有字段
            for (ordinal, field) in schema.fields.iter().enumerate() {
                // 使用表名作为数据库名（CalmCore 是单数据库系统）
                let db_name = table_name.to_string();

                table_cats.push(Some(db_name));
                table_schems.push(None); // MySQL 不使用 schema
                table_names.push(table_name.to_string());
                column_names.push(field.name().to_string());

                // 映射 CalmCore FieldType 到 JDBC SQL Types
                let (sql_type, type_name, column_size, decimal_digits) =
                    self.map_field_type_to_jdbc(&field.field_type());

                data_types.push(sql_type);
                type_names.push(type_name);
                column_sizes.push(column_size);
                buffer_lengths.push(None);
                decimal_digits_vec.push(decimal_digits);
                num_prec_radixes.push(Some(10)); // 数值类型使用十进制

                // Nullable: 1 = nullable, 0 = not null
                let is_nullable = field.nullable();
                nullables.push(if is_nullable { 1 } else { 0 });
                is_nullables.push(if is_nullable {
                    "YES".to_string()
                } else {
                    "NO".to_string()
                });

                remarks_vec.push(None);
                column_defs.push(None);
                sql_data_types.push(None);
                sql_datetime_subs.push(None);

                // 字符类型的字节长度
                char_octet_lengths.push(match field.field_type() {
                    crate::schema::field::FieldType::Keyword => Some(65535),
                    _ => None,
                });

                ordinal_positions.push((ordinal + 1) as i32);
                scope_catalogs.push(None);
                scope_schemas.push(None);
                scope_tables.push(None);
                source_data_types.push(None);
                is_autoincrements.push("NO".to_string());
                is_generatedcolumns.push("NO".to_string());
                column_comments.push(None);
                extras.push("".to_string()); // 空字符串，表示没有特殊属性
            }
        }

        // 构建结果 schema - 使用小写字段名匹配 INFORMATION_SCHEMA 标准
        let schema = Arc::new(Schema::new(vec![
            Field::new("table_schema", DataType::Utf8, true),
            Field::new("table_catalog", DataType::Utf8, true),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("column_name", DataType::Utf8, false),
            Field::new("data_type", DataType::Int32, false),
            Field::new("type_name", DataType::Utf8, false),
            Field::new("column_size", DataType::Int32, true),
            Field::new("buffer_length", DataType::Int32, true),
            Field::new("decimal_digits", DataType::Int32, true),
            Field::new("num_prec_radix", DataType::Int32, true),
            Field::new("nullable", DataType::Int32, false),
            Field::new("remarks", DataType::Utf8, true),
            Field::new("column_def", DataType::Utf8, true),
            Field::new("sql_data_type", DataType::Int32, true),
            Field::new("sql_datetime_sub", DataType::Int32, true),
            Field::new("char_octet_length", DataType::Int32, true),
            Field::new("ordinal_position", DataType::Int32, false),
            Field::new("is_nullable", DataType::Utf8, false),
            Field::new("scope_catalog", DataType::Utf8, true),
            Field::new("scope_schema", DataType::Utf8, true),
            Field::new("scope_table", DataType::Utf8, true),
            Field::new("source_data_type", DataType::Int32, true),
            Field::new("is_autoincrement", DataType::Utf8, false),
            Field::new("is_generatedcolumn", DataType::Utf8, false),
            Field::new("column_comment", DataType::Utf8, true),
            Field::new("extra", DataType::Utf8, false),
        ]));

        let row_count = column_names.len();

        // 构建 RecordBatch
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(table_cats)),
                Arc::new(StringArray::from(table_schems)),
                Arc::new(StringArray::from(table_names)),
                Arc::new(StringArray::from(column_names)),
                Arc::new(Int32Array::from(data_types)),
                Arc::new(StringArray::from(type_names)),
                Arc::new(Int32Array::from(column_sizes)),
                Arc::new(Int32Array::from(buffer_lengths)),
                Arc::new(Int32Array::from(decimal_digits_vec)),
                Arc::new(Int32Array::from(num_prec_radixes)),
                Arc::new(Int32Array::from(nullables)),
                Arc::new(StringArray::from(remarks_vec)),
                Arc::new(StringArray::from(column_defs)),
                Arc::new(Int32Array::from(sql_data_types)),
                Arc::new(Int32Array::from(sql_datetime_subs)),
                Arc::new(Int32Array::from(char_octet_lengths)),
                Arc::new(Int32Array::from(ordinal_positions)),
                Arc::new(StringArray::from(is_nullables)),
                Arc::new(StringArray::from(scope_catalogs)),
                Arc::new(StringArray::from(scope_schemas)),
                Arc::new(StringArray::from(scope_tables)),
                Arc::new(Int32Array::from(source_data_types)),
                Arc::new(StringArray::from(is_autoincrements)),
                Arc::new(StringArray::from(is_generatedcolumns)),
                Arc::new(StringArray::from(column_comments)),
                Arc::new(StringArray::from(extras)),
            ],
        )
        .map_err(|e| CoreError::Internal(format!("Failed to create columns batch: {}", e)))?;

        log::info!("✅ [JDBC getColumns] Returning {} rows", row_count);

        Ok(batch)
    }

    /// 映射 CalmCore FieldType 到 JDBC SQL Types
    /// 返回: (sql_type_code, type_name, column_size, decimal_digits)
    ///
    /// JDBC SQL Type 常量参考 java.sql.Types:
    /// CHAR=1, NUMERIC=2, DECIMAL=3, INTEGER=4, SMALLINT=5, FLOAT=6, REAL=7, DOUBLE=8,
    /// VARCHAR=12, BOOLEAN=16, TINYINT=-6, BIGINT=-5, VARBINARY=-2, TIMESTAMP=93
    fn map_field_type_to_jdbc(
        &self,
        field_type: &crate::schema::field::FieldType,
    ) -> (i32, String, Option<i32>, Option<i32>) {
        use crate::schema::field::FieldType;

        match field_type {
            // 字符串类型
            FieldType::Keyword => (12, "VARCHAR".to_string(), Some(65535), None),

            // 整数类型
            FieldType::I8 => (-6, "TINYINT".to_string(), Some(3), Some(0)),
            FieldType::I16 => (5, "SMALLINT".to_string(), Some(5), Some(0)),
            FieldType::I32 => (4, "INTEGER".to_string(), Some(10), Some(0)),
            FieldType::I64 => (-5, "BIGINT".to_string(), Some(19), Some(0)),

            // 无符号整数
            FieldType::U8 => (-6, "TINYINT UNSIGNED".to_string(), Some(3), Some(0)),
            FieldType::U16 => (5, "SMALLINT UNSIGNED".to_string(), Some(5), Some(0)),
            FieldType::U32 => (4, "INTEGER UNSIGNED".to_string(), Some(10), Some(0)),
            FieldType::U64 => (-5, "BIGINT UNSIGNED".to_string(), Some(20), Some(0)),

            // 浮点类型
            FieldType::F32 => (7, "REAL".to_string(), Some(7), Some(31)),
            FieldType::F64 => (8, "DOUBLE".to_string(), Some(15), Some(31)),

            // 布尔类型
            FieldType::Boolean => (16, "BOOLEAN".to_string(), Some(1), None),

            // 时间戳类型（存储为 i64 毫秒）
            FieldType::Timestamp => (93, "TIMESTAMP".to_string(), Some(23), Some(3)),
        }
    }

    /// 快速路径：返回空的 JDBC getPrimaryKeys() 结果
    /// TODO: 实现完整的主键信息查询
    async fn execute_jdbc_get_primary_keys_empty(&self) -> CoreResult<RecordBatch> {
        log::info!("🎯 [JDBC getPrimaryKeys] Returning empty result (not implemented yet)");

        // JDBC getPrimaryKeys() 标准字段（对应 INFORMATION_SCHEMA.STATISTICS）
        let schema = Arc::new(Schema::new(vec![
            Field::new("TABLE_CAT", DataType::Utf8, true), // TABLE_SCHEMA
            Field::new("TABLE_SCHEM", DataType::Utf8, true), // NULL
            Field::new("TABLE_NAME", DataType::Utf8, false), // TABLE_NAME
            Field::new("COLUMN_NAME", DataType::Utf8, false), // COLUMN_NAME
            Field::new("KEY_SEQ", DataType::Int32, false), // SEQ_IN_INDEX
            Field::new("PK_NAME", DataType::Utf8, true),   // 'PRIMARY'
        ]));

        // 返回空结果集（0行）
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(Vec::<Option<String>>::new())),
                Arc::new(StringArray::from(Vec::<Option<String>>::new())),
                Arc::new(StringArray::from(Vec::<String>::new())),
                Arc::new(StringArray::from(Vec::<String>::new())),
                Arc::new(Int32Array::from(Vec::<i32>::new())),
                Arc::new(StringArray::from(Vec::<Option<String>>::new())),
            ],
        )
        .map_err(|e| {
            CoreError::Internal(format!("Failed to create empty primary keys batch: {}", e))
        })?;

        log::info!("✅ [JDBC getPrimaryKeys] Returning 0 rows");

        Ok(batch)
    }
}
