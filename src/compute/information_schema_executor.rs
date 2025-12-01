//! INFORMATION_SCHEMA 虚拟表查询执行器
//!
//! 专门处理 INFORMATION_SCHEMA.TABLES 等元数据查询
//! 完全独立的逻辑，不影响现有的查询执行流程

use datafusion::arrow::array::{Int64Array, RecordBatch, StringArray, TimestampSecondArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::prelude::*;
use std::sync::Arc;

use crate::compute::QueryResult;
use crate::engine::Engine;
use crate::utils::error::{CoreError, CoreResult};

pub struct InformationSchemaExecutor {
    engine: Arc<Engine>,
}

impl InformationSchemaExecutor {
    pub fn new(engine: Arc<Engine>) -> Self {
        Self { engine }
    }

    /// 执行 INFORMATION_SCHEMA 查询
    pub async fn execute(&self, sql: &str) -> CoreResult<QueryResult> {
        log::info!("🔍 [INFORMATION_SCHEMA] Executing query");
        log::info!("🔍 [INFORMATION_SCHEMA] Original SQL: {}", sql);

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

        let batches = df
            .collect()
            .await
            .map_err(|e| CoreError::Internal(format!("Failed to collect results: {}", e)))?;

        // 合并结果
        let batch = if batches.is_empty() {
            RecordBatch::new_empty(Arc::new(Schema::empty()))
        } else if batches.len() == 1 {
            batches.into_iter().next().unwrap()
        } else {
            use datafusion::arrow::compute::concat_batches;
            let schema = batches[0].schema();
            concat_batches(&schema, &batches)
                .map_err(|e| CoreError::Internal(format!("Failed to concat batches: {}", e)))?
        };

        let matched_docs = batch.num_rows();
        Ok(QueryResult {
            batch,
            matched_docs,
        })
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

    /// 提取数据库名
    fn extract_database_name(sql: &str) -> String {
        let patterns = [
            "table_schema = '",
            "table_schema='",
            "TABLE_SCHEMA = '",
            "TABLE_SCHEMA='",
        ];

        for pattern in &patterns {
            if let Some(pos) = sql.find(pattern) {
                let after = &sql[pos + pattern.len()..];
                if let Some(end_pos) = after.find('\'') {
                    return after[..end_pos].to_string();
                }
            }
        }

        "calm".to_string()
    }
}
