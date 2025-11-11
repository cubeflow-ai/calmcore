use std::sync::Arc;

use poem::{
    error::ResponseError,
    handler,
    http::StatusCode,
    middleware::AddData,
    web::{Data, Json, Path},
    EndpointExt, IntoResponse, Response, Route, Server,
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

    /// 创建 Poem Route
    pub fn app(self) -> impl poem::IntoEndpoint {
        let state = Arc::new(self);

        Route::new()
            // 健康检查
            .at("/", poem::get(root))
            .at("/_cluster/health", poem::get(cluster_health))
            .at("/_cat/indices", poem::get(list_indices))
            // 索引管理 - 标准 Elasticsearch API 路径
            .at("/:index", poem::put(create_index).get(get_index).delete(delete_index))
            // 文档操作 - 合并相同路径的不同 HTTP 方法
            .at("/:index/_doc/:id", poem::put(index_document_with_id).get(get_document).delete(delete_document))
            .at("/:index/_doc", poem::post(index_document))
            // 批量操作
            .at("/:index/_bulk", poem::post(bulk_operation))
            .at("/_bulk", poem::post(bulk_operation_global))
            // 搜索
            .at("/:index/_search", poem::post(search_documents).get(search_documents_get))
            .with(AddData::new(state))
    }

    /// 启动 HTTP 服务器
    pub async fn start(self, addr: &str) -> Result<(), Box<dyn std::error::Error>> {
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

        Server::new(poem::listener::TcpListener::bind(addr))
            .run(self.app())
            .await?;
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
#[handler]
async fn root() -> Json<Value> {
    poem::web::Json(serde_json::json!({
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
#[handler]
async fn cluster_health() -> Json<Value> {
    poem::web::Json(serde_json::json!({
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

#[handler]
async fn create_index(
    Data(server): Data<&Arc<ElasticsearchServer>>,
    Path(index): Path<String>,
    Json(payload): Json<CreateIndexRequest>,
) -> Result<poem::web::Json<serde_json::Value>, poem::Error> {
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

    Ok(poem::web::Json(serde_json::json!({
        "acknowledged": true,
        "shards_acknowledged": true,
        "index": index
    })))
}

/// 删除索引

#[handler]
async fn delete_index(
    Data(server): Data<&Arc<ElasticsearchServer>>,
    Path(index): Path<String>,
) -> Result<poem::web::Json<serde_json::Value>, poem::Error> {
    server
        .engine
        .drop_table(&index)
        .await
        .map_err(|e| AppError::NotFound(format!("Index not found: {}", e)))?;

    Ok(poem::web::Json(serde_json::json!({
        "acknowledged": true
    })))
}

/// 获取索引信息

#[handler]
async fn get_index(
    Data(server): Data<&Arc<ElasticsearchServer>>,
    Path(index): Path<String>,
) -> Result<poem::web::Json<serde_json::Value>, poem::Error> {
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

    Ok(poem::web::Json(serde_json::json!({
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

#[handler]
async fn list_indices(
    Data(server): Data<&Arc<ElasticsearchServer>>,
) -> Result<String, poem::Error> {
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

#[handler]
async fn index_document_with_id(
    Data(server): Data<&Arc<ElasticsearchServer>>,
    Path((index, id)): Path<(String, String)>,
    Json(document): Json<Value>,
) -> Result<poem::web::Json<serde_json::Value>, poem::Error> {
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
        .get_partition(&index, partition_id)
        .await
        .ok_or_else(|| AppError::Internal("Partition not found".to_string()))?;

    // 插入文档
    let result_ids = partition
        .upsert_json(&[doc])
        .map_err(|e| AppError::Internal(e.to_string()))?;

    Ok(poem::web::Json(serde_json::json!({
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

#[handler]
async fn index_document(
    Data(server): Data<&Arc<ElasticsearchServer>>,
    Path(index): Path<String>,
    Json(document): Json<Value>,
) -> Result<poem::web::Json<serde_json::Value>, poem::Error> {
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
        .get_partition(&index, partition_id)
        .await
        .ok_or_else(|| AppError::Internal("Partition not found".to_string()))?;

    // 插入文档
    let result_ids = partition
        .upsert_json(&[doc])
        .map_err(|e| AppError::Internal(e.to_string()))?;

    Ok(poem::web::Json(serde_json::json!({
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

#[handler]
async fn get_document(
    Data(server): Data<&Arc<ElasticsearchServer>>,
    Path((index, id)): Path<(String, String)>,
) -> Result<poem::web::Json<serde_json::Value>, poem::Error> {
    // 获取 partition
    let partition_id = server
        .engine
        .route_partition(&index, &id)
        .map_err(|e| AppError::NotFound(e.to_string()))?;

    let partition = server
        .engine
        .get_partition(&index, partition_id)
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
                return Ok(poem::web::Json(serde_json::json!({
                    "_index": index,
                    "_id": id,
                    "_version": 1,
                    "found": true,
                    "_source": doc
                })));
            }
        }
    }

    Ok(poem::web::Json(serde_json::json!({
        "_index": index,
        "_id": id,
        "found": false
    })))
}

/// 删除文档

#[handler]
async fn delete_document(
    Data(_server): Data<&Arc<ElasticsearchServer>>,
    Path((index, id)): Path<(String, String)>,
) -> Result<poem::web::Json<serde_json::Value>, poem::Error> {
    // TODO: 实现删除逻辑
    Ok(poem::web::Json(serde_json::json!({
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

#[handler]
async fn bulk_operation(
    Data(server): Data<&Arc<ElasticsearchServer>>,
    Path(index): Path<String>,
    body: String,
) -> Result<poem::web::Json<serde_json::Value>, poem::Error> {
    bulk_operation_impl(server.clone(), Some(index), body).await
}

/// 批量操作（全局）

#[handler]
async fn bulk_operation_global(
    Data(server): Data<&Arc<ElasticsearchServer>>,
    body: String,
) -> Result<poem::web::Json<serde_json::Value>, poem::Error> {
    bulk_operation_impl(server.clone(), None, body).await
}

/// 批量操作实现
async fn bulk_operation_impl(
    server: Arc<ElasticsearchServer>,
    default_index: Option<String>,
    body: String,
) -> Result<poem::web::Json<serde_json::Value>, poem::Error> {
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
            return Err(AppError::BadRequest("Unknown action type".to_string()).into());
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
                    return Err(
                        AppError::BadRequest("Missing document after action".to_string()).into(),
                    );
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

    Ok(poem::web::Json(serde_json::json!({
        "took": 10,
        "errors": errors,
        "items": items
    })))
}

/// 搜索文档（POST）

#[handler]
async fn search_documents(
    Data(server): Data<&Arc<ElasticsearchServer>>,
    Path(index): Path<String>,
    Json(search_req): Json<SearchRequest>,
) -> Result<poem::web::Json<serde_json::Value>, poem::Error> {
    search_impl(server.clone(), index, search_req).await
}

/// 搜索文档（GET）

#[handler]
async fn search_documents_get(
    Data(server): Data<&Arc<ElasticsearchServer>>,
    Path(index): Path<String>,
) -> Result<poem::web::Json<serde_json::Value>, poem::Error> {
    // 默认搜索请求
    let search_req = SearchRequest {
        query: None,
        size: Some(10),
        from: Some(0),
    };
    search_impl(server.clone(), index, search_req).await
}

/// 搜索实现
async fn search_impl(
    server: Arc<ElasticsearchServer>,
    index: String,
    search_req: SearchRequest,
) -> Result<poem::web::Json<serde_json::Value>, poem::Error> {
    let start_time = std::time::Instant::now();

    // 获取表的所有 partition
    let meta = server
        .engine
        .get_table_meta(&index)
        .map_err(|_| AppError::NotFound(format!("Index '{}' not found", index)))?;

    let mut all_docs = Vec::new();

    // 查询所有 partition
    for partition_id in 0..meta.parallel_workers {
        if let Some(partition) = server
            .engine
            .get_partition(&index, partition_id as u64)
            .await
        {
            // 扫描当前 segment
            let current_segment = partition.get_current_segment();
            if let Ok(batches) = current_segment.scan_documents() {
                for batch in batches {
                    if let Ok(docs) = crate::utils::arrow_utils::record_batch_to_json(&batch) {
                        all_docs.extend(docs);
                    }
                }
            }

            // 扫描所有 frozen segments
            let frozen_segments = partition.get_frozen_segments();
            for (_, segment) in frozen_segments.iter() {
                if let Ok(batches) = segment.scan_documents() {
                    for batch in batches {
                        if let Ok(docs) = crate::utils::arrow_utils::record_batch_to_json(&batch) {
                            all_docs.extend(docs);
                        }
                    }
                }
            }
        }
    }

    // 应用查询过滤（简单的 match_all 或 term 查询）
    let filtered_docs = if let Some(query) = &search_req.query {
        apply_query_filter(&all_docs, query)
    } else {
        all_docs // 如果没有查询条件，返回所有文档
    };

    // 分页
    let from = search_req.from.unwrap_or(0);
    let size = search_req.size.unwrap_or(10);
    let total = filtered_docs.len();

    let hits: Vec<Value> = filtered_docs
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

    let took = start_time.elapsed().as_millis() as u64;

    Ok(poem::web::Json(serde_json::json!({
        "took": took,
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

/// 应用查询过滤
fn apply_query_filter(docs: &[Value], query: &Value) -> Vec<Value> {
    // 简单的查询实现，支持 match_all 和 term 查询
    if let Some(query_obj) = query.as_object() {
        if query_obj.contains_key("match_all") {
            return docs.to_vec();
        }
        
        if let Some(term) = query_obj.get("term") {
            if let Some(term_obj) = term.as_object() {
                let mut filtered = Vec::new();
                
                for doc in docs {
                    let mut matches = true;
                    
                    for (field, value) in term_obj {
                        if !check_field_match(doc, field, value) {
                            matches = false;
                            break;
                        }
                    }
                    
                    if matches {
                        filtered.push(doc.clone());
                    }
                }
                
                return filtered;
            }
        }
        
        // 支持简单的 match 查询
        if let Some(match_query) = query_obj.get("match") {
            if let Some(match_obj) = match_query.as_object() {
                let mut filtered = Vec::new();
                
                for doc in docs {
                    let mut matches = true;
                    
                    for (field, value) in match_obj {
                        if !check_field_match(doc, field, value) {
                            matches = false;
                            break;
                        }
                    }
                    
                    if matches {
                        filtered.push(doc.clone());
                    }
                }
                
                return filtered;
            }
        }
    }
    
    // 默认返回所有文档
    docs.to_vec()
}

/// 检查字段是否匹配
fn check_field_match(doc: &Value, field: &str, expected_value: &Value) -> bool {
    if let Some(doc_obj) = doc.as_object() {
        if let Some(field_value) = doc_obj.get(field) {
            return field_value == expected_value;
        }
    }
    false
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
        .get_partition(&index, partition_id)
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

impl ResponseError for AppError {
    fn status(&self) -> StatusCode {
        match self {
            AppError::NotFound(_) => StatusCode::NOT_FOUND,
            AppError::BadRequest(_) => StatusCode::BAD_REQUEST,
            AppError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    fn as_response(&self) -> Response {
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
                reason: reason.clone(),
            },
            status: status.as_u16(),
        });

        (status, body).into_response()
    }
}

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
