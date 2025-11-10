use std::sync::Arc;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{delete, get, post, put},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::{
    catalog::PartitionStrategy,
    engine::Engine,
    schema::{field::FieldOption, Schema},
    utils::error::CoreError,
};

/// Elasticsearch API 服务器
pub struct ElasticsearchServer {
    engine: Arc<Engine>,
}

impl ElasticsearchServer {
    pub fn new(engine: Arc<Engine>) -> Self {
        Self { engine }
    }

    /// 创建 Axum Router
    pub fn router(self) -> Router {
        let state = Arc::new(self);

        Router::new()
            // 健康检查
            .route("/", get(root))
            .route("/_cluster/health", get(cluster_health))
            // 索引管理
            .route("/:index", put(create_index))
            .route("/:index", delete(delete_index))
            .route("/:index", get(get_index))
            .route("/_cat/indices", get(list_indices))
            // 文档操作
            .route("/:index/_doc/:id", put(index_document_with_id))
            .route("/:index/_doc", post(index_document))
            .route("/:index/_doc/:id", get(get_document))
            .route("/:index/_doc/:id", delete(delete_document))
            // 批量操作
            .route("/:index/_bulk", post(bulk_operation))
            .route("/_bulk", post(bulk_operation_global))
            // 搜索
            .route("/:index/_search", post(search_documents))
            .route("/:index/_search", get(search_documents_get))
            .with_state(state)
    }

    /// 启动 HTTP 服务器
    pub async fn start(self, addr: &str) -> Result<(), Box<dyn std::error::Error>> {
        let listener = tokio::net::TcpListener::bind(addr).await?;
        println!("🔍 Elasticsearch API server listening on http://{}", addr);
        println!();
        println!("API endpoints:");
        println!("  PUT    /:index                    - Create index");
        println!("  DELETE /:index                    - Delete index");
        println!("  GET    /:index                    - Get index info");
        println!("  GET    /_cat/indices              - List all indices");
        println!("  PUT    /:index/_doc/:id           - Index document with ID");
        println!("  POST   /:index/_doc               - Index document (auto ID)");
        println!("  GET    /:index/_doc/:id           - Get document");
        println!("  DELETE /:index/_doc/:id           - Delete document");
        println!("  POST   /:index/_bulk              - Bulk operations");
        println!("  POST   /:index/_search            - Search documents");
        println!();

        axum::serve(listener, self.router()).await?;
        Ok(())
    }
}

// ===== 请求/响应结构体 =====

#[derive(Debug, Deserialize)]
struct CreateIndexRequest {
    mappings: Option<Mappings>,
    settings: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct Mappings {
    properties: std::collections::HashMap<String, PropertyMapping>,
}

#[derive(Debug, Deserialize)]
struct PropertyMapping {
    #[serde(rename = "type")]
    field_type: String,
    #[serde(default)]
    index: Option<bool>,
}

#[derive(Debug, Deserialize, Serialize)]
struct IndexDocumentRequest {
    #[serde(flatten)]
    document: Value,
}

#[derive(Debug, Deserialize)]
struct SearchRequest {
    query: Option<Value>,
    size: Option<usize>,
    from: Option<usize>,
}

#[derive(Debug, Serialize)]
struct ErrorResponse {
    error: ErrorDetail,
    status: u16,
}

#[derive(Debug, Serialize)]
struct ErrorDetail {
    #[serde(rename = "type")]
    error_type: String,
    reason: String,
}

// ===== 处理器函数 =====

/// 根路径 - 返回集群信息
async fn root() -> Json<Value> {
    Json(json!({
        "name": "calm-node-1",
        "cluster_name": "calm-cluster",
        "version": {
            "number": "8.0.0-calm",
            "build_flavor": "default",
            "build_type": "tar",
            "build_hash": "calm",
            "lucene_version": "9.0.0"
        },
        "tagline": "You Know, for Search"
    }))
}

/// 集群健康检查
async fn cluster_health() -> Json<Value> {
    Json(json!({
        "cluster_name": "calm-cluster",
        "status": "green",
        "timed_out": false,
        "number_of_nodes": 1,
        "number_of_data_nodes": 1,
        "active_primary_shards": 0,
        "active_shards": 0,
        "relocating_shards": 0,
        "initializing_shards": 0,
        "unassigned_shards": 0
    }))
}

/// 创建索引
async fn create_index(
    State(server): State<Arc<ElasticsearchServer>>,
    Path(index): Path<String>,
    Json(payload): Json<CreateIndexRequest>,
) -> Result<Json<Value>, AppError> {
    // 构建 Schema
    let mut fields = Vec::new();

    if let Some(mappings) = payload.mappings {
        for (field_name, mapping) in mappings.properties {
            let should_index = mapping.index.unwrap_or(true);

            let field = match mapping.field_type.as_str() {
                "text" | "keyword" => FieldOption::Keyword {
                    name: field_name,
                    index: should_index,
                    is_array: false,
                    persist_option: None,
                    case_sensitive: true,
                },
                "long" | "integer" => FieldOption::I64 {
                    name: field_name,
                    index: should_index,
                },
                "float" | "double" => FieldOption::F64 {
                    name: field_name,
                    index: should_index,
                },
                "boolean" => FieldOption::Boolean {
                    name: field_name,
                    index: should_index,
                },
                _ => FieldOption::Keyword {
                    name: field_name,
                    index: should_index,
                    is_array: false,
                    persist_option: None,
                    case_sensitive: true,
                },
            };

            fields.push(field);
        }
    }

    // 如果没有指定 mappings，添加默认的 _id 字段
    if fields.is_empty() {
        fields.push(FieldOption::Keyword {
            name: "_id".to_string(),
            index: true,
            is_array: false,
            persist_option: None,
            case_sensitive: true,
        });
    }

    // 创建 Schema，使用 _id 作为主键
    let schema = Schema {
        name: index.clone(),
        primary_key: Some("_id".to_string()),
        store_source: true,
        fields,
        persist_policy: crate::schema::PersistPolicy::default(),
    };

    // 创建表（索引）
    server
        .engine
        .create_table(
            &index,
            schema,
            PartitionStrategy::Hash {
                field: "_id".to_string(),
                num_partitions: 1,
            },
            1, // 默认 1 个分区
        )
        .await
        .map_err(|e| AppError::Internal(e.to_string()))?;

    Ok(Json(json!({
        "acknowledged": true,
        "shards_acknowledged": true,
        "index": index
    })))
}

/// 删除索引
async fn delete_index(
    State(server): State<Arc<ElasticsearchServer>>,
    Path(index): Path<String>,
) -> Result<Json<Value>, AppError> {
    server
        .engine
        .drop_table(&index)
        .await
        .map_err(|e| AppError::NotFound(format!("Index not found: {}", e)))?;

    Ok(Json(json!({
        "acknowledged": true
    })))
}

/// 获取索引信息
async fn get_index(
    State(server): State<Arc<ElasticsearchServer>>,
    Path(index): Path<String>,
) -> Result<Json<Value>, AppError> {
    let meta = server
        .engine
        .get_table_meta(&index)
        .map_err(|_| AppError::NotFound(format!("Index '{}' not found", index)))?;

    // 构建 mappings 响应
    let mut properties = serde_json::Map::new();
    for field in &meta.schema.fields {
        let field_type = match field {
            FieldOption::Keyword { .. } => "keyword",
            FieldOption::I64 { .. } => "long",
            FieldOption::F64 { .. } => "double",
            FieldOption::Boolean { .. } => "boolean",
            FieldOption::I32 { .. } => "integer",
            FieldOption::F32 { .. } => "float",
            _ => "keyword",
        };

        properties.insert(
            field.name().to_string(),
            json!({
                "type": field_type
            }),
        );
    }

    Ok(Json(json!({
        index: {
            "mappings": {
                "properties": properties
            },
            "settings": {
                "index": {
                    "number_of_shards": meta.parallel_workers,
                    "number_of_replicas": 0
                }
            }
        }
    })))
}

/// 列出所有索引
async fn list_indices(State(server): State<Arc<ElasticsearchServer>>) -> Result<String, AppError> {
    let tables = server.engine.list_tables();

    let mut output = String::new();
    output.push_str("health status index    uuid                   pri rep docs.count docs.deleted store.size pri.store.size\n");

    for (i, table) in tables.iter().enumerate() {
        output.push_str(&format!(
            "green  open   {:8} {:22} 1   0          0            0       0b            0b\n",
            table,
            format!("calm-{}", i)
        ));
    }

    Ok(output)
}

/// 索引文档（指定 ID）
async fn index_document_with_id(
    State(server): State<Arc<ElasticsearchServer>>,
    Path((index, id)): Path<(String, String)>,
    Json(document): Json<Value>,
) -> Result<Json<Value>, AppError> {
    // 添加 _id 字段到文档
    let mut doc = document;
    if let Value::Object(ref mut map) = doc {
        map.insert("_id".to_string(), Value::String(id.clone()));
    }

    // 获取 partition
    let partition_id = server
        .engine
        .route_partition(&index, &id)
        .map_err(|e| AppError::NotFound(e.to_string()))?;

    let partition = server
        .engine
        .get_partition(partition_id)
        .await
        .ok_or_else(|| AppError::Internal("Partition not found".to_string()))?;

    // 插入文档
    let result_ids = partition
        .upsert_json(&[doc])
        .map_err(|e| AppError::Internal(e.to_string()))?;

    Ok(Json(json!({
        "_index": index,
        "_id": id,
        "_version": 1,
        "result": "created",
        "_shards": {
            "total": 1,
            "successful": 1,
            "failed": 0
        },
        "_seq_no": result_ids.get(0).unwrap_or(&0),
        "_primary_term": 1
    })))
}

/// 索引文档（自动生成 ID）
async fn index_document(
    State(server): State<Arc<ElasticsearchServer>>,
    Path(index): Path<String>,
    Json(document): Json<Value>,
) -> Result<Json<Value>, AppError> {
    // 生成唯一 ID
    let id = uuid::Uuid::new_v4().to_string();

    // 添加 _id 字段到文档
    let mut doc = document;
    if let Value::Object(ref mut map) = doc {
        map.insert("_id".to_string(), Value::String(id.clone()));
    }

    // 获取 partition
    let partition_id = server
        .engine
        .route_partition(&index, &id)
        .map_err(|e| AppError::NotFound(e.to_string()))?;

    let partition = server
        .engine
        .get_partition(partition_id)
        .await
        .ok_or_else(|| AppError::Internal("Partition not found".to_string()))?;

    // 插入文档
    let result_ids = partition
        .upsert_json(&[doc])
        .map_err(|e| AppError::Internal(e.to_string()))?;

    Ok(Json(json!({
        "_index": index,
        "_id": id,
        "_version": 1,
        "result": "created",
        "_shards": {
            "total": 1,
            "successful": 1,
            "failed": 0
        },
        "_seq_no": result_ids.get(0).unwrap_or(&0),
        "_primary_term": 1
    })))
}

/// 获取文档
async fn get_document(
    State(server): State<Arc<ElasticsearchServer>>,
    Path((index, id)): Path<(String, String)>,
) -> Result<Json<Value>, AppError> {
    // 获取 partition
    let partition_id = server
        .engine
        .route_partition(&index, &id)
        .map_err(|e| AppError::NotFound(e.to_string()))?;

    let partition = server
        .engine
        .get_partition(partition_id)
        .await
        .ok_or_else(|| AppError::Internal("Partition not found".to_string()))?;

    // 查询文档
    let batch = partition
        .get_by_pk(&[&id])
        .map_err(|e| AppError::Internal(e.to_string()))?;

    if let Some(batch) = batch {
        if batch.num_rows() > 0 {
            // 转换 RecordBatch 为 JSON
            let json_docs = crate::utils::arrow_utils::record_batch_to_json(&batch)
                .map_err(|e| AppError::Internal(e.to_string()))?;

            if let Some(doc) = json_docs.first() {
                return Ok(Json(json!({
                    "_index": index,
                    "_id": id,
                    "_version": 1,
                    "found": true,
                    "_source": doc
                })));
            }
        }
    }

    Ok(Json(json!({
        "_index": index,
        "_id": id,
        "found": false
    })))
}

/// 删除文档
async fn delete_document(
    State(_server): State<Arc<ElasticsearchServer>>,
    Path((index, id)): Path<(String, String)>,
) -> Result<Json<Value>, AppError> {
    // TODO: 实现删除逻辑
    Ok(Json(json!({
        "_index": index,
        "_id": id,
        "_version": 2,
        "result": "deleted",
        "_shards": {
            "total": 1,
            "successful": 1,
            "failed": 0
        },
        "_seq_no": 1,
        "_primary_term": 1
    })))
}

/// 批量操作
async fn bulk_operation(
    State(server): State<Arc<ElasticsearchServer>>,
    Path(index): Path<String>,
    body: String,
) -> Result<Json<Value>, AppError> {
    bulk_operation_impl(server, Some(index), body).await
}

/// 批量操作（全局）
async fn bulk_operation_global(
    State(server): State<Arc<ElasticsearchServer>>,
    body: String,
) -> Result<Json<Value>, AppError> {
    bulk_operation_impl(server, None, body).await
}

/// 批量操作实现
async fn bulk_operation_impl(
    server: Arc<ElasticsearchServer>,
    default_index: Option<String>,
    body: String,
) -> Result<Json<Value>, AppError> {
    let mut items = Vec::new();
    let mut errors = false;

    // 解析 NDJSON 格式
    let lines: Vec<&str> = body.lines().collect();
    let mut i = 0;

    while i < lines.len() {
        if lines[i].trim().is_empty() {
            i += 1;
            continue;
        }

        // 解析操作行
        let action: Value = serde_json::from_str(lines[i])
            .map_err(|e| AppError::BadRequest(format!("Invalid JSON in action line: {}", e)))?;

        i += 1;

        // 获取操作类型
        let (action_type, action_meta) = if let Some(index_action) = action.get("index") {
            ("index", index_action)
        } else if let Some(create_action) = action.get("create") {
            ("create", create_action)
        } else if let Some(update_action) = action.get("update") {
            ("update", update_action)
        } else if let Some(delete_action) = action.get("delete") {
            ("delete", delete_action)
        } else {
            return Err(AppError::BadRequest("Unknown action type".to_string()));
        };

        // 获取索引和 ID
        let index_name = action_meta
            .get("_index")
            .and_then(|v| v.as_str())
            .or(default_index.as_deref())
            .ok_or_else(|| AppError::BadRequest("Missing _index".to_string()))?;

        let doc_id = action_meta
            .get("_id")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

        // 处理不同操作类型
        match action_type {
            "index" | "create" => {
                if i >= lines.len() {
                    return Err(AppError::BadRequest(
                        "Missing document after action".to_string(),
                    ));
                }

                // 解析文档
                let mut doc: Value = serde_json::from_str(lines[i]).map_err(|e| {
                    AppError::BadRequest(format!("Invalid JSON in document line: {}", e))
                })?;

                if let Value::Object(ref mut map) = doc {
                    map.insert("_id".to_string(), Value::String(doc_id.clone()));
                }

                i += 1;

                // 插入文档
                match insert_document(&server, index_name, &doc_id, doc).await {
                    Ok(_) => {
                        items.push(json!({
                            action_type: {
                                "_index": index_name,
                                "_id": doc_id,
                                "_version": 1,
                                "result": "created",
                                "status": 201
                            }
                        }));
                    }
                    Err(e) => {
                        errors = true;
                        items.push(json!({
                            action_type: {
                                "_index": index_name,
                                "_id": doc_id,
                                "status": 400,
                                "error": {
                                    "type": "mapper_parsing_exception",
                                    "reason": e.to_string()
                                }
                            }
                        }));
                    }
                }
            }
            "delete" => {
                items.push(json!({
                    "delete": {
                        "_index": index_name,
                        "_id": doc_id,
                        "_version": 2,
                        "result": "deleted",
                        "status": 200
                    }
                }));
            }
            _ => {
                errors = true;
                items.push(json!({
                    action_type: {
                        "_index": index_name,
                        "_id": doc_id,
                        "status": 400,
                        "error": {
                            "type": "action_request_validation_exception",
                            "reason": format!("Unsupported action: {}", action_type)
                        }
                    }
                }));
            }
        }
    }

    Ok(Json(json!({
        "took": 10,
        "errors": errors,
        "items": items
    })))
}

/// 搜索文档（POST）
async fn search_documents(
    State(server): State<Arc<ElasticsearchServer>>,
    Path(index): Path<String>,
    Json(search_req): Json<SearchRequest>,
) -> Result<Json<Value>, AppError> {
    search_impl(server, index, search_req).await
}

/// 搜索文档（GET）
async fn search_documents_get(
    State(server): State<Arc<ElasticsearchServer>>,
    Path(index): Path<String>,
) -> Result<Json<Value>, AppError> {
    // 默认搜索请求
    let search_req = SearchRequest {
        query: None,
        size: Some(10),
        from: Some(0),
    };
    search_impl(server, index, search_req).await
}

/// 搜索实现
async fn search_impl(
    server: Arc<ElasticsearchServer>,
    index: String,
    search_req: SearchRequest,
) -> Result<Json<Value>, AppError> {
    // 获取表的所有 partition
    let meta = server
        .engine
        .get_table_meta(&index)
        .map_err(|_| AppError::NotFound(format!("Index '{}' not found", index)))?;

    let mut all_docs = Vec::new();

    // 查询所有 partition
    // TODO: 实现完整的搜索功能，目前返回空结果
    // 后续可以通过 DataFusion SQL 或直接扫描 segments 实现
    for partition_id in 0..meta.parallel_workers {
        if let Some(_partition) = server.engine.get_partition(partition_id as u64).await {
            // 暂时不实现全表扫描，返回空结果
            // 实际应该扫描所有 segments 的数据
        }
    }

    // 分页
    let from = search_req.from.unwrap_or(0);
    let size = search_req.size.unwrap_or(10);
    let total = all_docs.len();

    let hits: Vec<Value> = all_docs
        .into_iter()
        .skip(from)
        .take(size)
        .map(|doc: Value| {
            let id = doc
                .get("_id")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown")
                .to_string();

            json!({
                "_index": index,
                "_id": id,
                "_score": 1.0,
                "_source": doc
            })
        })
        .collect();

    Ok(Json(json!({
        "took": 5,
        "timed_out": false,
        "_shards": {
            "total": meta.parallel_workers,
            "successful": meta.parallel_workers,
            "skipped": 0,
            "failed": 0
        },
        "hits": {
            "total": {
                "value": total,
                "relation": "eq"
            },
            "max_score": 1.0,
            "hits": hits
        }
    })))
}

// ===== 辅助函数 =====

/// 插入单个文档
async fn insert_document(
    server: &Arc<ElasticsearchServer>,
    index: &str,
    id: &str,
    doc: Value,
) -> Result<(), AppError> {
    let partition_id = server
        .engine
        .route_partition(index, id)
        .map_err(|e| AppError::NotFound(e.to_string()))?;

    let partition = server
        .engine
        .get_partition(partition_id)
        .await
        .ok_or_else(|| AppError::Internal("Partition not found".to_string()))?;

    partition
        .upsert_json(&[doc])
        .map_err(|e| AppError::Internal(e.to_string()))?;

    Ok(())
}

// ===== 错误处理 =====

#[derive(Debug)]
enum AppError {
    NotFound(String),
    BadRequest(String),
    Internal(String),
}

impl std::fmt::Display for AppError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AppError::NotFound(msg) => write!(f, "Not found: {}", msg),
            AppError::BadRequest(msg) => write!(f, "Bad request: {}", msg),
            AppError::Internal(msg) => write!(f, "Internal error: {}", msg),
        }
    }
}

impl std::error::Error for AppError {}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, error_type, reason) = match self {
            AppError::NotFound(msg) => (StatusCode::NOT_FOUND, "index_not_found_exception", msg),
            AppError::BadRequest(msg) => (
                StatusCode::BAD_REQUEST,
                "action_request_validation_exception",
                msg,
            ),
            AppError::Internal(msg) => (StatusCode::INTERNAL_SERVER_ERROR, "internal_error", msg),
        };

        let body = Json(ErrorResponse {
            error: ErrorDetail {
                error_type: error_type.to_string(),
                reason,
            },
            status: status.as_u16(),
        });

        (status, body).into_response()
    }
}

impl From<CoreError> for AppError {
    fn from(err: CoreError) -> Self {
        AppError::Internal(err.to_string())
    }
}
