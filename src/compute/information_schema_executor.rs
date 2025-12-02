//! INFORMATION_SCHEMA 虚拟表查询执行器
//!
//! 专门处理 INFORMATION_SCHEMA.TABLES 等元数据查询
//! 完全独立的逻辑，不影响现有的查询执行流程

use datafusion::arrow::array::{Int64Array, RecordBatch, StringArray, TimestampSecondArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::prelude::*;
use futures::stream;
use std::sync::Arc;

use crate::engine::Engine;
use crate::utils::error::{CoreError, CoreResult};

pub struct InformationSchemaExecutor {
    engine: Arc<Engine>,
}

impl InformationSchemaExecutor {
    pub fn new(engine: Arc<Engine>) -> Self {
        Self { engine }
    }

    /// 执行 INFORMATION_SCHEMA 查询（流式）
    pub async fn execute_stream(&self, sql: &str) -> CoreResult<SendableRecordBatchStream> {
        log::info!("🔍 [INFORMATION_SCHEMA] Executing query");

        // 🎯 特殊处理：JDBC getTables() 查询（避免 DataFusion 的 CASE/HAVING bug）
        // 使用快速路径的条件（满足任一即可）：
        // 1. 包含 JDBC 标准字段（TABLE_CAT, TABLE_SCHEM, REMARKS 等）
        // 2. 包含 CASE 表达式（嵌套 CASE 会导致 DataFusion 挂起）
        // 3. 包含 HAVING 子句（HAVING 不带 GROUP BY 是 MySQL 非标准语法）
        let sql_upper = sql.to_uppercase();
        let has_jdbc_fields = (sql_upper.contains("TABLE_CAT")
            || sql_upper.contains("TABLE_SCHEM"))
            && (sql_upper.contains("REMARKS") || sql_upper.contains("REF_GENERATION"));
        let has_case = sql_upper.contains("CASE WHEN");
        let has_having = sql_upper.contains(" HAVING ");

        if has_jdbc_fields || has_case || has_having {
            log::info!("🎯 [INFORMATION_SCHEMA] Using JDBC fast path");
            let result = self.execute_jdbc_get_tables(sql).await?;
            // 转换为 Stream
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

        // 注册虚拟表
        self.register_tables(&ctx, &db_name)?;

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
    fn register_tables(&self, ctx: &SessionContext, db_name: &str) -> CoreResult<()> {
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

        let table_names = self.engine.list_tables();
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
        let table_names = self.engine.list_tables();

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
}
