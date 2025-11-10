use std::sync::Arc;

use async_graphql::{Context, EmptySubscription, Object, Result, Schema, SimpleObject};
use poem::{
    get, handler, listener::TcpListener, middleware::Cors, post, EndpointExt, Route, Server,
};
use serde_json::Value as JsonValue;

use crate::{
    catalog::PartitionStrategy,
    engine::Engine,
    schema::{field::FieldOption, PersistPolicy, Schema as CalmSchema},
    utils::arrow_utils,
};

/// GraphQL Schema for database operations
pub type CalmGraphQLSchema = Schema<QueryRoot, MutationRoot, EmptySubscription>;

/// 创建 GraphQL Schema
pub fn create_schema(engine: Arc<Engine>) -> CalmGraphQLSchema {
    Schema::build(QueryRoot, MutationRoot, EmptySubscription)
        .data(engine)
        .finish()
}

// ===== 类型定义 =====

#[derive(SimpleObject)]
pub struct Table {
    pub name: String,
    pub partition_count: u64,
    pub fields: Vec<Field>,
    pub primary_key: Option<String>,
}

#[derive(SimpleObject)]
pub struct Field {
    pub name: String,
    pub field_type: String,
    pub indexed: bool,
}

#[derive(async_graphql::InputObject)]
pub struct CreateTableInput {
    pub name: String,
    pub primary_key: Option<String>,
    pub partition_count: Option<u64>,
    pub fields: Vec<FieldInput>,
}

#[derive(async_graphql::InputObject)]
pub struct FieldInput {
    pub name: String,
    pub field_type: String,
    pub indexed: Option<bool>,
    pub case_sensitive: Option<bool>,
}

#[derive(SimpleObject)]
pub struct InsertResult {
    pub success: bool,
    pub rows_inserted: usize,
    pub message: String,
}

#[derive(SimpleObject)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<JsonValue>,
    pub total_rows: usize,
}

#[derive(async_graphql::InputObject)]
pub struct InsertDataInput {
    pub table: String,
    pub data: Vec<JsonValue>,
}

// ===== Query Root =====

pub struct QueryRoot;

#[Object]
impl QueryRoot {
    /// 列出所有表
    async fn tables(&self, ctx: &Context<'_>) -> Result<Vec<String>> {
        let engine = ctx.data::<Arc<Engine>>()?;
        Ok(engine.list_tables())
    }

    /// 获取表信息
    async fn table(&self, ctx: &Context<'_>, name: String) -> Result<Option<Table>> {
        let engine = ctx.data::<Arc<Engine>>()?;

        let meta = match engine.get_table_meta(&name) {
            Ok(meta) => meta,
            Err(_) => return Ok(None),
        };

        let fields = meta
            .schema
            .fields
            .iter()
            .map(|f| {
                let field_type = match f {
                    FieldOption::Keyword { .. } => "keyword",
                    FieldOption::I64 { .. } => "i64",
                    FieldOption::F64 { .. } => "f64",
                    FieldOption::Boolean { .. } => "boolean",
                    FieldOption::I32 { .. } => "i32",
                    FieldOption::F32 { .. } => "f32",
                    _ => "unknown",
                };

                Field {
                    name: f.name().to_string(),
                    field_type: field_type.to_string(),
                    indexed: f.is_index(),
                }
            })
            .collect();

        Ok(Some(Table {
            name: meta.schema.name.clone(),
            partition_count: meta.parallel_workers as u64,
            fields,
            primary_key: meta.schema.primary_key.clone(),
        }))
    }

    /// 执行 SQL 查询
    async fn query(&self, ctx: &Context<'_>, sql: String) -> Result<QueryResult> {
        let engine = ctx.data::<Arc<Engine>>()?;

        // 解析 SQL 获取表名 (简单实现: 从 FROM 后提取)
        let sql_upper = sql.to_uppercase();
        let table_name = if let Some(from_pos) = sql_upper.find(" FROM ") {
            let after_from = &sql[from_pos + 6..];
            after_from
                .split_whitespace()
                .next()
                .unwrap_or("unknown")
                .to_string()
        } else {
            return Err(async_graphql::Error::new(
                "Cannot parse table name from SQL",
            ));
        };

        // 创建 DataFusion context
        use datafusion::prelude::*;
        let datafusion_ctx = SessionContext::new();

        // 注册所有 partition
        let meta = engine
            .get_table_meta(&table_name)
            .map_err(|e| async_graphql::Error::new(format!("Table not found: {}", e)))?;

        for partition_id in 0..meta.parallel_workers {
            if let Some(partition) = engine.get_partition(&table_name, partition_id as u64).await {
                use crate::compute::PartitionTableProvider;
                let provider = Arc::new(PartitionTableProvider::new(partition));
                datafusion_ctx
                    .register_table(&table_name, provider)
                    .map_err(|e| {
                        async_graphql::Error::new(format!("Register table failed: {}", e))
                    })?;
                break; // 简化: 只注册第一个 partition
            }
        }

        // 执行查询
        let df = datafusion_ctx
            .sql(&sql)
            .await
            .map_err(|e| async_graphql::Error::new(format!("SQL parse error: {}", e)))?;

        let batches = df
            .collect()
            .await
            .map_err(|e| async_graphql::Error::new(format!("Query failed: {}", e)))?;

        if batches.is_empty() {
            return Ok(QueryResult {
                columns: vec![],
                rows: vec![],
                total_rows: 0,
            });
        }

        // 获取列名
        let columns: Vec<String> = batches[0]
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();

        // 转换所有批次为 JSON
        let mut all_rows = Vec::new();
        for batch in batches {
            let rows = arrow_utils::record_batch_to_json(&batch).map_err(|e| {
                async_graphql::Error::new(format!("Failed to convert to JSON: {}", e))
            })?;
            all_rows.extend(rows);
        }

        let total_rows = all_rows.len();

        Ok(QueryResult {
            columns,
            rows: all_rows,
            total_rows,
        })
    }
}

// ===== Mutation Root =====

pub struct MutationRoot;

#[Object]
impl MutationRoot {
    /// 创建表
    async fn create_table(&self, ctx: &Context<'_>, input: CreateTableInput) -> Result<Table> {
        let engine = ctx.data::<Arc<Engine>>()?;

        // 构建字段
        let mut fields = Vec::new();
        for field_input in input.fields {
            let indexed = field_input.indexed.unwrap_or(true);
            let case_sensitive = field_input.case_sensitive.unwrap_or(true);

            let field = match field_input.field_type.as_str() {
                "keyword" | "text" => FieldOption::Keyword {
                    name: field_input.name.clone(),
                    index: indexed,
                    is_array: false,
                    persist_option: None,
                    case_sensitive,
                },
                "i64" | "long" => FieldOption::I64 {
                    name: field_input.name.clone(),
                    index: indexed,
                },
                "f64" | "double" => FieldOption::F64 {
                    name: field_input.name.clone(),
                    index: indexed,
                },
                "boolean" | "bool" => FieldOption::Boolean {
                    name: field_input.name.clone(),
                    index: indexed,
                },
                "i32" | "integer" => FieldOption::I32 {
                    name: field_input.name.clone(),
                    index: indexed,
                },
                "f32" | "float" => FieldOption::F32 {
                    name: field_input.name.clone(),
                    index: indexed,
                },
                _ => {
                    return Err(async_graphql::Error::new(format!(
                        "Unsupported field type: {}",
                        field_input.field_type
                    )));
                }
            };

            fields.push(field);
        }

        // 创建 Schema
        let schema = CalmSchema {
            name: input.name.clone(),
            primary_key: input.primary_key.clone(),
            store_source: true,
            fields: fields.clone(),
            persist_policy: PersistPolicy::default(),
        };

        // 确定主键字段用于分区策略
        let partition_field = input.primary_key.clone().unwrap_or_else(|| {
            fields
                .first()
                .map(|f| f.name().to_string())
                .unwrap_or_default()
        });

        let partition_count = input.partition_count.unwrap_or(1);
        let partition_count_usize: usize = partition_count.try_into().unwrap();

        // 创建表
        engine
            .create_table(
                &input.name,
                schema,
                PartitionStrategy::Hash {
                    field: partition_field,
                    num_partitions: partition_count_usize,
                },
                partition_count_usize,
            )
            .await
            .map_err(|e| async_graphql::Error::new(e.to_string()))?;

        // 返回创建的表信息
        let field_info: Vec<Field> = fields
            .iter()
            .map(|f| {
                let field_type = match f {
                    FieldOption::Keyword { .. } => "keyword",
                    FieldOption::I64 { .. } => "i64",
                    FieldOption::F64 { .. } => "f64",
                    FieldOption::Boolean { .. } => "boolean",
                    FieldOption::I32 { .. } => "i32",
                    FieldOption::F32 { .. } => "f32",
                    _ => "unknown",
                };

                Field {
                    name: f.name().to_string(),
                    field_type: field_type.to_string(),
                    indexed: f.is_index(),
                }
            })
            .collect();

        Ok(Table {
            name: input.name,
            partition_count,
            fields: field_info,
            primary_key: input.primary_key,
        })
    }

    /// 删除表
    async fn drop_table(&self, ctx: &Context<'_>, name: String) -> Result<bool> {
        let engine = ctx.data::<Arc<Engine>>()?;

        engine
            .drop_table(&name)
            .await
            .map_err(|e| async_graphql::Error::new(e.to_string()))?;

        Ok(true)
    }

    /// 持久化表（强制将表的所有数据写入磁盘）
    async fn flush_table(&self, ctx: &Context<'_>, name: String) -> Result<bool> {
        let engine = ctx.data::<Arc<Engine>>()?;

        engine
            .flush_table(&name)
            .await
            .map_err(|e| async_graphql::Error::new(format!("Failed to flush table: {}", e)))?;

        Ok(true)
    }

    /// 插入数据
    async fn insert_data(&self, ctx: &Context<'_>, input: InsertDataInput) -> Result<InsertResult> {
        let engine = ctx.data::<Arc<Engine>>()?;

        // 获取表的元数据
        let meta = engine
            .get_table_meta(&input.table)
            .map_err(|e| async_graphql::Error::new(format!("Table not found: {}", e)))?;

        // 获取主键字段
        let pk_field = meta
            .schema
            .primary_key
            .as_ref()
            .ok_or_else(|| async_graphql::Error::new("Table has no primary key"))?;

        let mut total_inserted = 0;

        // 对每条数据进行路由和插入
        for doc in &input.data {
            // 1. 提取主键值
            let pk_value = doc.get(pk_field).and_then(|v| v.as_str()).ok_or_else(|| {
                async_graphql::Error::new(format!("Missing or invalid primary key: {}", pk_field))
            })?;

            // 2. 根据路由策略计算 partition_id
            let partition_id = engine
                .route_partition(&input.table, pk_value)
                .map_err(|e| async_graphql::Error::new(format!("Route failed: {}", e)))?;

            // 3. 获取 partition
            let partition = engine
                .get_partition(&input.table, partition_id)
                .await
                .ok_or_else(|| {
                    async_graphql::Error::new(format!(
                        "Partition {} not found for table {}",
                        partition_id, input.table
                    ))
                })?;

            // 4. 插入单条数据
            partition
                .upsert_json(&[doc.clone()])
                .map_err(|e| async_graphql::Error::new(format!("Insert failed: {}", e)))?;

            total_inserted += 1;
        }

        Ok(InsertResult {
            success: true,
            rows_inserted: total_inserted,
            message: format!("Successfully inserted {} rows", total_inserted),
        })
    }
}

// ===== GraphQL Server =====

/// GraphQL 服务器
pub struct GraphQLServer {
    engine: Arc<Engine>,
}

impl GraphQLServer {
    pub fn new(engine: Arc<Engine>) -> Self {
        Self { engine }
    }

    /// 启动 GraphQL 服务器
    pub async fn start(self, addr: &str) -> Result<(), std::io::Error> {
        println!("=== Calm Database - GraphQL Server ===");
        println!();
        println!("✓ Engine initialized");
        println!();

        // 创建 GraphQL Schema
        let graphql_schema = create_schema(self.engine.clone());

        // GraphQL endpoint
        let graphql_endpoint = async_graphql_poem::GraphQL::new(graphql_schema);

        // 构建路由
        let app = Route::new()
            .at("/", get(root))
            .at("/health", get(health))
            .at("/graphql", post(graphql_endpoint))
            .at(
                "/playground",
                get(poem::endpoint::make_sync(move |_| {
                    poem::web::Html(
                        r#"
                        <!DOCTYPE html>
                        <html>
                        <head>
                            <title>GraphQL Playground</title>
                            <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/graphql-playground-react/build/static/css/index.css" />
                            <script src="https://cdn.jsdelivr.net/npm/graphql-playground-react/build/static/js/middleware.js"></script>
                        </head>
                        <body>
                            <div id="root"></div>
                            <script>
                                window.addEventListener('load', function() {
                                    GraphQLPlayground.init(document.getElementById('root'), {
                                        endpoint: '/graphql'
                                    })
                                })
                            </script>
                        </body>
                        </html>
                        "#.to_string()
                    )
                })),
            )
            .with(Cors::new());

        println!("🚀 GraphQL Server starting on http://{}", addr);
        println!();
        println!("📍 Endpoints:");
        println!("   GraphQL API:        http://{}/graphql", addr);
        println!("   GraphQL Playground: http://{}/playground", addr);
        println!();
        println!("📚 Available Operations:");
        println!();
        println!("  Queries:");
        println!("    - tables: [String!]!");
        println!("    - table(name: String!): Table");
        println!("    - query(sql: String!): QueryResult!");
        println!();
        println!("  Mutations:");
        println!("    - createTable(input: CreateTableInput!): Table!");
        println!("    - dropTable(name: String!): Boolean!");
        println!("    - flushTable(name: String!): Boolean!");
        println!("    - insertData(input: InsertDataInput!): InsertResult!");
        println!();
        println!("📖 Example Usage:");
        println!();
        println!("  1️⃣  Create a table:");
        println!(
            r#"
mutation {{
  createTable(input: {{
    name: "users"
    primaryKey: "id"
    partitionCount: 1
    fields: [
      {{ name: "id", fieldType: "keyword" }}
      {{ name: "name", fieldType: "keyword" }}
      {{ name: "age", fieldType: "i64" }}
      {{ name: "email", fieldType: "keyword" }}
    ]
  }}) {{
    name
    partitionCount
    fields {{
      name
      fieldType
      indexed
    }}
  }}
}}
"#
        );
        println!();
        println!("  2️⃣  Insert data:");
        println!(
            r#"
mutation {{
  insertData(input: {{
    table: "users"
    data: [
      {{id: "1", name: "Alice", age: 30, email: "alice@example.com"}},
      {{id: "2", name: "Bob", age: 25, email: "bob@example.com"}}
    ]
  }}) {{
    success
    rowsInserted
    message
  }}
}}
"#
        );
        println!();
        println!("  3️⃣  Query with SQL:");
        println!(
            r#"
query {{
  query(sql: "SELECT * FROM users WHERE age > 20") {{
    columns
    totalRows
    rows
  }}
}}
"#
        );
        println!();
        println!("  4️⃣  Flush table (persist to disk):");
        println!(
            r#"
mutation {{
  flushTable(name: "users")
}}
"#
        );
        println!();
        println!("Press Ctrl+C to stop the server");
        println!();

        Server::new(TcpListener::bind(addr)).run(app).await
    }
}

/// 健康检查
#[handler]
async fn health() -> &'static str {
    "OK"
}

/// 根路径
#[handler]
async fn root() -> poem::web::Json<serde_json::Value> {
    poem::web::Json(serde_json::json!({
        "name": "Calm Database - GraphQL Server",
        "version": "0.1.0",
        "graphql_endpoint": "/graphql",
        "playground": "/playground"
    }))
}
