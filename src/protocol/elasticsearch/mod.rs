mod aggregation;

use std::sync::Arc;

use poem::{
    error::ResponseError,
    handler,
    http::StatusCode,
    middleware::{AddData, SetHeader},
    web::{Data, Json, Path},
    EndpointExt, Response, Route, Server,
};
use poem_openapi::types::ToJSON;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::{
    catalog::PartitionStrategy,
    engine::Engine,
    schema::{field::FieldOption, Schema},
    utils::error::CoreError,
};

use aggregation::{
    build_aggregation_sql, convert_aggregations_to_sql, format_aggregation_response, Aggregations,
};

// 错误辅助函数
fn not_found(msg: impl Into<String>) -> CoreError {
    CoreError::NotExisted(msg.into())
}

fn bad_request(msg: impl Into<String>) -> CoreError {
    CoreError::InvalidParam(msg.into())
}

fn internal_error(msg: impl Into<String>) -> CoreError {
    CoreError::Internal(msg.into())
}

/// 将 CoreError 转换为 ResponseError
impl ResponseError for CoreError {
    fn status(&self) -> StatusCode {
        StatusCode::INTERNAL_SERVER_ERROR
    }
}

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
            .at(
                "/:index",
                poem::put(create_index).get(get_index).delete(delete_index),
            )
            // 文档操作 - 合并相同路径的不同 HTTP 方法
            .at(
                "/:index/_doc/:id",
                poem::put(index_document_with_id)
                    .get(get_document)
                    .delete(delete_document),
            )
            .at("/:index/_doc", poem::post(index_document))
            // 批量操作
            .at("/:index/_bulk", poem::post(bulk_operation))
            .at("/_bulk", poem::post(bulk_operation_global))
            // 搜索
            .at(
                "/:index/_search",
                poem::post(search_documents).get(search_documents_get),
            )
            .with(AddData::new(state))
            .with(SetHeader::new().appending("X-Elastic-Product", "Elasticsearch"))
    }

    /// 启动 HTTP 服务器
    pub async fn start(self, addr: &str) -> Result<(), Box<dyn std::error::Error>> {
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
    #[allow(dead_code)]
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

#[allow(dead_code)]
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
    sort: Option<Value>,
    /// 聚合查询
    #[serde(skip_serializing_if = "Option::is_none")]
    aggregations: Option<Aggregations>,
    /// 简写形式
    #[serde(skip_serializing_if = "Option::is_none")]
    aggs: Option<Aggregations>,
}

#[allow(dead_code)]
#[derive(Debug, Serialize)]
struct ErrorResponse {
    error: ErrorDetail,
    status: u16,
}

#[allow(dead_code)]
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
        "cluster_uuid": "calmcluster-uuid-0000-0000-000000000000",
        "version": {
            "number": "8.0.0",
            "build_flavor": "default",
            "build_type": "tar",
            "build_hash": "calm0000000000000000000000000000000000",
            "build_date": "2024-01-01T00:00:00Z",
            "build_snapshot": false,
            "lucene_version": "9.0.0",
            "minimum_wire_compatibility_version": "7.17.0",
            "minimum_index_compatibility_version": "7.0.0"
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
    let schema = Schema::new(
        index.clone(),
        Some("_id".to_string()),
        true,
        fields,
        crate::schema::PersistPolicy::default(),
    );

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
        .map_err(|e| internal_error(e.to_string()))?;

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
        .map_err(|e| not_found(format!("Index not found: {}", e)))?;

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
        .map_err(|_| not_found(format!("Index '{}' not found", index)))?;

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
        .map_err(|e| not_found(e.to_string()))?;

    let partition = server
        .engine
        .get_partition(&index, &partition_id)
        .await
        .ok_or_else(|| internal_error("Partition not found".to_string()))?;

    // 插入文档
    let result_ids = partition
        .upsert_json(&[doc])
        .map_err(|e| internal_error(e.to_string()))?;

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
        "_seq_no": result_ids.first().unwrap_or(&0),
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
        .map_err(|e| not_found(e.to_string()))?;

    let partition = server
        .engine
        .get_partition(&index, &partition_id)
        .await
        .ok_or_else(|| internal_error("Partition not found".to_string()))?;

    // 插入文档
    let result_ids = partition
        .upsert_json(&[doc])
        .map_err(|e| internal_error(e.to_string()))?;

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
        "_seq_no": result_ids.first().unwrap_or(&0),
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
        .map_err(|e| not_found(e.to_string()))?;

    let partition = server
        .engine
        .get_partition(&index, &partition_id)
        .await
        .ok_or_else(|| internal_error("Partition not found".to_string()))?;

    // 查询文档
    let batch = partition
        .get_by_pk(&[&id])
        .map_err(|e| internal_error(e.to_string()))?;

    if let Some(batch) = batch {
        if batch.num_rows() > 0 {
            // 转换 RecordBatch 为 JSON
            let json_docs = crate::utils::arrow_utils::record_batch_to_json(&batch)
                .map_err(|e| internal_error(e.to_string()))?;

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
) -> Result<Response, poem::Error> {
    bulk_operation_impl(server.clone(), Some(index), body).await
}

/// 批量操作（全局）

#[handler]
async fn bulk_operation_global(
    Data(server): Data<&Arc<ElasticsearchServer>>,
    body: String,
) -> Result<Response, poem::Error> {
    bulk_operation_impl(server.clone(), None, body).await
}

/// 批量操作实现
async fn bulk_operation_impl(
    server: Arc<ElasticsearchServer>,
    default_index: Option<String>,
    body: String,
) -> Result<Response, poem::Error> {
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
            .map_err(|e| bad_request(format!("Invalid JSON in action line: {}", e)))?;

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
            return Err(bad_request("Unknown action type".to_string()).into());
        };

        // 获取索引和 ID
        let index_name = action_meta
            .get("_index")
            .and_then(|v| v.as_str())
            .or(default_index.as_deref())
            .ok_or_else(|| bad_request("Missing _index".to_string()))?;

        let doc_id = action_meta
            .get("_id")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

        // 处理不同操作类型
        match action_type {
            "index" | "create" => {
                if i >= lines.len() {
                    return Err(bad_request("Missing document after action".to_string()).into());
                }

                // 解析文档
                let mut doc: Value = serde_json::from_str(lines[i])
                    .map_err(|e| bad_request(format!("Invalid JSON in document line: {}", e)))?;

                if let Value::Object(ref mut map) = doc {
                    map.insert("_id".to_string(), Value::String(doc_id.clone()));
                }

                i += 1;

                // 插入文档
                match insert_document(&server, index_name, &doc_id, doc).await {
                    Ok(_) => {
                        // 严格按照 ES 8.x 响应格式，包含 _type 字段（虽然已废弃但客户端需要）
                        items.push(json!({
                            action_type: {
                                "_index": index_name,
                                "_type": "_doc",  // 添加 _type 字段，使用默认值 "_doc"
                                "_id": doc_id,
                                "_version": 1,
                                "result": "created",
                                "_shards": {
                                    "total": 2,
                                    "successful": 1,
                                    "failed": 0
                                },
                                "_seq_no": 0,
                                "_primary_term": 1,
                                "status": 201
                            }
                        }));
                    }
                    Err(e) => {
                        errors = true;
                        items.push(json!({
                            action_type: {
                                "_index": index_name,
                                "_type": "_doc",  // 错误响应也需要 _type
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
                        "_type": "_doc",  // 添加 _type 字段
                        "_id": doc_id,
                        "_version": 2,
                        "result": "deleted",
                        "_shards": {
                            "total": 2,
                            "successful": 1,
                            "failed": 0
                        },
                        "_seq_no": 1,
                        "_primary_term": 1,
                        "status": 200
                    }
                }));
            }
            _ => {
                errors = true;
                items.push(json!({
                    action_type: {
                        "_index": index_name,
                        "_type": "_doc",  // 添加 _type 字段
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

    // 创建响应，明确设置 Content-Type 为 application/json（不带 charset）
    let response_body = serde_json::json!({
        "took": 10,
        "errors": errors,
        "items": items
    });

    let json_string = serde_json::to_string(&response_body)
        .map_err(|e| internal_error(format!("Failed to serialize response: {}", e)))?;

    Ok(Response::builder()
        .content_type("application/json")
        .body(json_string))
}

/// 搜索文档（POST）

#[handler]
async fn search_documents(
    Data(server): Data<&Arc<ElasticsearchServer>>,
    Path(index): Path<String>,
    poem::web::Query(params): poem::web::Query<std::collections::HashMap<String, String>>,
    Json(search_req): Json<SearchRequest>,
) -> Result<Response, poem::Error> {
    let typed_keys = params
        .get("typed_keys")
        .map(|v| v == "true")
        .unwrap_or(false);
    search_impl(server.clone(), index, search_req, typed_keys).await
}

/// 处理聚合查询
async fn handle_aggregation_search(
    server: Arc<ElasticsearchServer>,
    index: String,
    search_req: SearchRequest,
    aggregations: Aggregations,
    schema: Arc<Schema>,
    typed_keys: bool,
) -> Result<Response, poem::Error> {
    let start_time = std::time::Instant::now();

    // 转换聚合查询为 SQL
    let agg_sql = convert_aggregations_to_sql(&aggregations, &schema)
        .map_err(|e| bad_request(format!("Failed to convert aggregation: {}", e)))?;

    // 构建 WHERE 子句
    let mut where_clause = String::new();
    if let Some(query) = &search_req.query {
        if let Some(where_sql) = convert_es_query_to_sql(query, &schema) {
            where_clause = format!(" WHERE {}", where_sql);
        }
    }

    // 构建完整的聚合 SQL
    let sql = build_aggregation_sql(&index, &where_clause, &agg_sql);

    log::info!("🔍 [ES Agg] Generated SQL: {}", sql);

    // 执行聚合查询
    let result = server
        .engine
        .clone()
        .execute_sql(&sql)
        .await
        .map_err(|e| internal_error(format!("Aggregation query failed: {}", e)))?;

    // 先获取总计数
    let count_sql = format!("SELECT COUNT(*) FROM {}{}", index, where_clause);
    let total_count = match server.engine.clone().execute_sql(&count_sql).await {
        Ok(result) if result.batch.num_rows() > 0 => {
            use datafusion::arrow::array::*;
            let batch = &result.batch;
            if batch.num_columns() > 0 && batch.num_rows() > 0 {
                let column = batch.column(0);
                if let Some(int64_array) = column.as_any().downcast_ref::<Int64Array>() {
                    int64_array.value(0) as usize
                } else if let Some(uint64_array) = column.as_any().downcast_ref::<UInt64Array>() {
                    uint64_array.value(0) as usize
                } else {
                    0
                }
            } else {
                0
            }
        }
        _ => 0,
    };

    // 转换结果为 JSON
    let records = crate::utils::arrow_utils::record_batch_to_json(&result.batch)
        .map_err(|e| internal_error(format!("Failed to convert results: {}", e)))?;

    // 转换为 Map 格式
    let record_maps: Vec<serde_json::Map<String, Value>> = records
        .into_iter()
        .filter_map(|v| v.as_object().cloned())
        .collect();

    // 获取第一个聚合的名称
    let agg_name = aggregations
        .aggs
        .keys()
        .next()
        .ok_or_else(|| bad_request("No aggregation name found"))?;

    // 格式化为 ES 响应格式
    let agg_result = format_aggregation_response(agg_name, record_maps, typed_keys);

    let took = start_time.elapsed().as_millis() as u64;

    // 构建响应
    let response_body = json!({
        "took": took,
        "timed_out": false,
        "_shards": {
            "total": 1,
            "successful": 1,
            "skipped": 0,
            "failed": 0
        },
        "hits": {
            "total": {
                "value": total_count,
                "relation": "eq"
            },
            "max_score": null,
            "hits": []  // size=0 时不返回文档
        },
        "aggregations": agg_result
    });

    let json_string = serde_json::to_string(&response_body)
        .map_err(|e| internal_error(format!("Failed to serialize response: {}", e)))?;

    Ok(Response::builder()
        .content_type("application/json")
        .body(json_string))
}

/// 搜索文档（GET）

#[handler]
async fn search_documents_get(
    Data(server): Data<&Arc<ElasticsearchServer>>,
    Path(index): Path<String>,
    poem::web::Query(params): poem::web::Query<std::collections::HashMap<String, String>>,
) -> Result<Response, poem::Error> {
    // 默认搜索请求
    let search_req = SearchRequest {
        query: None,
        size: Some(10),
        from: Some(0),
        sort: None,
        aggregations: None,
        aggs: None,
    };
    let typed_keys = params
        .get("typed_keys")
        .map(|v| v == "true")
        .unwrap_or(false);
    search_impl(server.clone(), index, search_req, typed_keys).await
}

/// 搜索实现
async fn search_impl(
    server: Arc<ElasticsearchServer>,
    index: String,
    search_req: SearchRequest,
    typed_keys: bool,
) -> Result<Response, poem::Error> {
    let start_time = std::time::Instant::now();

    // 获取表元数据
    let meta = server
        .engine
        .get_table_meta(&index)
        .map_err(|_| not_found(format!("Index '{}' not found", index)))?;

    // 🔧 检查是否是聚合查询
    let aggs = search_req
        .aggregations
        .as_ref()
        .or(search_req.aggs.as_ref());
    if let Some(aggregations) = aggs {
        log::info!("🔍 [ES Agg] Processing aggregation query");
        let schema_arc = Arc::new(meta.schema.clone());
        let aggregations_owned = aggregations.clone();
        return handle_aggregation_search(
            server,
            index,
            search_req,
            aggregations_owned,
            schema_arc,
            typed_keys,
        )
        .await;
    }

    // 构建 SQL 查询
    let mut sql = format!("SELECT * FROM {}", index);
    let mut where_clause = String::new();

    // 转换 ES DSL 查询为 SQL WHERE 条件
    if let Some(query) = &search_req.query {
        log::info!("🔍 [ES Search] Input query: {:?}", query);
        if let Some(where_sql) = convert_es_query_to_sql(query, &meta.schema) {
            where_clause = format!(" WHERE {}", where_sql);
            sql.push_str(&where_clause);
            log::debug!("✅ [ES Search] Generated WHERE clause: {}", where_clause);
        } else {
            log::warn!("⚠️  [ES Search] convert_es_query_to_sql returned None");
        }
    }

    // 添加排序
    if let Some(sort) = &search_req.sort {
        if let Some(order_by) = convert_es_sort_to_sql(sort) {
            if !order_by.is_empty() {
                sql.push_str(&format!(" ORDER BY {}", order_by));
                log::debug!("🔍 [ES Search] Added ORDER BY: {}", order_by);
            }
        }
    }

    // 添加 LIMIT (ES 的 from + size)
    let from = search_req.from.unwrap_or(0);
    let size = search_req.size.unwrap_or(10);
    if from > 0 {
        sql.push_str(&format!(" LIMIT {} OFFSET {}", size, from));
    } else {
        sql.push_str(&format!(" LIMIT {}", size));
    }

    log::debug!("🔍 [ES Search] Generated SQL: {}", sql);

    // 先执行 COUNT 查询获取总数
    let count_sql = format!("SELECT COUNT(*) FROM {}{}", index, where_clause);
    let total_count = match server.engine.clone().execute_sql(&count_sql).await {
        Ok(result) if result.batch.num_rows() > 0 => {
            use datafusion::arrow::array::*;
            let batch = &result.batch;
            if batch.num_columns() > 0 && batch.num_rows() > 0 {
                let column = batch.column(0);
                if let Some(int64_array) = column.as_any().downcast_ref::<Int64Array>() {
                    int64_array.value(0) as usize
                } else if let Some(uint64_array) = column.as_any().downcast_ref::<UInt64Array>() {
                    uint64_array.value(0) as usize
                } else {
                    0
                }
            } else {
                0
            }
        }
        _ => 0,
    };

    // 执行实际查询
    let result = match server.engine.clone().execute_sql(&sql).await {
        Ok(result) => result,
        Err(e) => {
            let msg = format!("Query execution failed: {}", e);
            log::error!("❌ [ES Search] Error: {}", msg);
            return Err(internal_error(msg).into());
        }
    };

    // 将 RecordBatch 转换为 JSON
    let all_docs =
        crate::utils::arrow_utils::record_batch_to_json(&result.batch).unwrap_or_else(|_| vec![]);

    // 提取排序字段名称（如果有排序）
    let sort_fields: Vec<String> = if let Some(sort) = &search_req.sort {
        if let Some(arr) = sort.as_array() {
            arr.iter()
                .filter_map(|s| {
                    if let Some(obj) = s.as_object() {
                        obj.keys().next().map(|k| k.to_string())
                    } else {
                        None
                    }
                })
                .collect()
        } else {
            vec![]
        }
    } else {
        vec![]
    };

    if !sort_fields.is_empty() {
        log::debug!("🔍 [ES Sort] Extracted sort fields: {:?}", sort_fields);
    }

    // 注意: LIMIT 和 OFFSET 已经在 SQL 中处理了,这里不需要再分页
    let hits: Vec<Value> = all_docs
        .into_iter()
        .map(|doc: Value| {
            // 处理 _source：如果是数组，取第一个元素；否则直接使用
            let source = if doc.is_array() {
                doc.as_array()
                    .and_then(|arr| arr.first())
                    .cloned()
                    .unwrap_or(doc.clone())
            } else {
                doc.clone()
            };

            let id = source
                .get("_id")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown")
                .to_string();

            // 提取排序值
            let sort_values: Vec<Value> = sort_fields
                .iter()
                .filter_map(|field| source.get(field).cloned())
                .collect();

            if !sort_values.is_empty() {
                log::debug!(
                    "🔍 [ES Sort] Document id={}, sort values={:?}",
                    id,
                    sort_values
                );
            }

            let mut hit = json!({
                "_index": index,
                "_type": "_doc",
                "_id": id,
                "_score": 1.0,
                "_source": source
            });

            // 如果有排序，添加 sort 字段
            if !sort_values.is_empty() {
                if let Some(obj) = hit.as_object_mut() {
                    obj.insert("sort".to_string(), json!(sort_values));
                }
            }

            hit
        })
        .collect();

    let took = start_time.elapsed().as_millis() as u64;

    let response_body = serde_json::json!({
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
                "value": total_count,
                "relation": "eq"
            },
            "max_score": 1.0,
            "hits": hits
        }
    });

    let json_string = serde_json::to_string(&response_body)
        .map_err(|e| internal_error(format!("Failed to serialize response: {}", e)))?;

    Ok(Response::builder()
        .content_type("application/json")
        .body(json_string))
}

/// 将 ES DSL 查询转换为 SQL WHERE 条件
fn convert_es_query_to_sql(query: &Value, schema: &crate::schema::Schema) -> Option<String> {
    use crate::query_rewriter::QueryRewriter;

    let rewriter = QueryRewriter::new(schema);

    if let Some(query_obj) = query.as_object() {
        // match_all 查询 - 不需要 WHERE 条件
        if query_obj.contains_key("match_all") {
            return None;
        }

        // 处理 bool 查询
        if let Some(bool_query) = query_obj.get("bool") {
            if let Some(bool_obj) = bool_query.as_object() {
                let mut conditions = Vec::new();

                // 处理 must 子句（AND 条件）
                if let Some(must) = bool_obj.get("must") {
                    if let Some(must_array) = must.as_array() {
                        for sub_query in must_array {
                            // 递归调用处理每个子查询
                            if let Some(sub_where) = convert_es_query_to_sql(sub_query, schema) {
                                conditions.push(format!("({})", sub_where));
                            }
                        }
                    }
                }

                // 处理 filter 子句（AND 条件，但不影响评分）
                if let Some(filter) = bool_obj.get("filter") {
                    if let Some(filter_array) = filter.as_array() {
                        for sub_query in filter_array {
                            if let Some(sub_where) = convert_es_query_to_sql(sub_query, schema) {
                                conditions.push(format!("({})", sub_where));
                            }
                        }
                    }
                }

                // 处理 should 子句（OR 条件）
                if let Some(should) = bool_obj.get("should") {
                    if let Some(should_array) = should.as_array() {
                        let mut should_conditions = Vec::new();
                        for sub_query in should_array {
                            if let Some(sub_where) = convert_es_query_to_sql(sub_query, schema) {
                                should_conditions.push(sub_where);
                            }
                        }
                        if !should_conditions.is_empty() {
                            conditions.push(format!("({})", should_conditions.join(" OR ")));
                        }
                    }
                }

                // 处理 must_not 子句（NOT 条件）
                if let Some(must_not) = bool_obj.get("must_not") {
                    if let Some(must_not_array) = must_not.as_array() {
                        for sub_query in must_not_array {
                            if let Some(sub_where) = convert_es_query_to_sql(sub_query, schema) {
                                conditions.push(format!("NOT ({})", sub_where));
                            }
                        }
                    }
                }

                if !conditions.is_empty() {
                    return Some(conditions.join(" AND "));
                } else {
                    return None;
                }
            }
        }

        // term 查询 - 精确匹配
        if let Some(term) = query_obj.get("term") {
            if let Some(term_obj) = term.as_object() {
                let conditions: Vec<String> = term_obj
                    .iter()
                    .filter_map(|(field, value)| {
                        // term 查询支持两种格式:
                        // 1. {"term": {"field": "value"}}
                        // 2. {"term": {"field": {"value": "value", "boost": 1.0}}}
                        let actual_value = if let Some(obj) = value.as_object() {
                            // 对象格式: 提取 "value" 字段
                            obj.get("value").unwrap_or(value)
                        } else {
                            // 简单格式: 直接使用值
                            value
                        };

                        // 跳过空字符串(通常表示不过滤此字段)
                        if let Some(s) = actual_value.as_str() {
                            if s.is_empty() {
                                return None;
                            }
                        }

                        // 使用改写器处理值
                        let rewritten = rewriter.rewrite_value(field, actual_value);
                        let val_str = match &rewritten {
                            Value::String(s) => format!("'{}'", s.replace("'", "''")),
                            Value::Number(n) => n.to_string(),
                            Value::Bool(b) => b.to_string(),
                            _ => return None,
                        };
                        Some(format!("{} = {}", field, val_str))
                    })
                    .collect();

                if !conditions.is_empty() {
                    return Some(conditions.join(" AND "));
                }
            }
        }

        // match 查询 - 文本匹配（简化为相等比较）
        if let Some(match_query) = query_obj.get("match") {
            if let Some(match_obj) = match_query.as_object() {
                let conditions: Vec<String> = match_obj
                    .iter()
                    .filter_map(|(field, value)| {
                        // match 查询也支持对象格式: {"match": {"field": {"query": "value", "boost": 1.0}}}
                        let actual_value = if let Some(obj) = value.as_object() {
                            // 对象格式: 提取 "query" 或 "value" 字段
                            obj.get("query")
                                .or_else(|| obj.get("value"))
                                .unwrap_or(value)
                        } else {
                            value
                        };

                        // 跳过空字符串
                        if let Some(s) = actual_value.as_str() {
                            if s.is_empty() {
                                return None;
                            }
                        }

                        // 使用改写器处理值
                        let rewritten = rewriter.rewrite_value(field, actual_value);
                        let val_str = match &rewritten {
                            Value::String(s) => format!("'{}'", s.replace("'", "''")),
                            Value::Number(n) => n.to_string(),
                            Value::Bool(b) => b.to_string(),
                            _ => return None,
                        };
                        Some(format!("{} = {}", field, val_str))
                    })
                    .collect();

                if !conditions.is_empty() {
                    return Some(conditions.join(" AND "));
                }
            }
        }

        // wildcard 查询 - 通配符匹配
        if let Some(wildcard) = query_obj.get("wildcard") {
            if let Some(wildcard_obj) = wildcard.as_object() {
                let conditions: Vec<String> = wildcard_obj
                    .iter()
                    .filter_map(|(field, value)| {
                        // wildcard可能是字符串或对象格式
                        let pattern = if let Some(s) = value.as_str() {
                            s.to_string()
                        } else if let Some(obj) = value.as_object() {
                            // 支持 {"field": {"wildcard": "*pattern*", "boost": 1.0}} 格式
                            if let Some(wildcard_str) = obj.get("wildcard") {
                                wildcard_str.as_str()?.to_string()
                            } else if let Some(value_str) = obj.get("value") {
                                // 支持 {"field": {"value": "*pattern*"}} 格式
                                value_str.as_str()?.to_string()
                            } else {
                                return None;
                            }
                        } else {
                            return None;
                        };

                        // 转换ES wildcard pattern到SQL LIKE pattern
                        // ES: * = 任意字符, ? = 单个字符
                        // SQL: % = 任意字符, _ = 单个字符
                        let sql_pattern = pattern
                            .replace("\\", "\\\\") // 先转义反斜杠
                            .replace("%", "\\%") // 转义SQL中的%
                            .replace("_", "\\_") // 转义SQL中的_
                            .replace("*", "%") // ES * -> SQL %
                            .replace("?", "_") // ES ? -> SQL _
                            .replace("'", "''"); // 转义单引号

                        Some(format!("{} LIKE '{}'", field, sql_pattern))
                    })
                    .collect();

                if !conditions.is_empty() {
                    let result = conditions.join(" AND ");
                    return Some(result);
                }
            }
        }

        // prefix 查询 - 前缀匹配
        if let Some(prefix) = query_obj.get("prefix") {
            if let Some(prefix_obj) = prefix.as_object() {
                let conditions: Vec<String> = prefix_obj
                    .iter()
                    .filter_map(|(field, value)| {
                        // prefix可能是字符串或对象格式
                        let prefix_val = if let Some(s) = value.as_str() {
                            s.to_string()
                        } else if let Some(obj) = value.as_object() {
                            obj.get("value")?.as_str()?.to_string()
                        } else {
                            return None;
                        };

                        // 转义SQL特殊字符
                        let sql_prefix = prefix_val
                            .replace("\\", "\\\\")
                            .replace("%", "\\%")
                            .replace("_", "\\_")
                            .replace("'", "''");

                        // 前缀匹配转换为 LIKE 'prefix%'
                        Some(format!("{} LIKE '{}%'", field, sql_prefix))
                    })
                    .collect();

                if !conditions.is_empty() {
                    let result = conditions.join(" AND ");
                    return Some(result);
                }
            }
        }

        // range 查询
        if let Some(range) = query_obj.get("range") {
            if let Some(range_obj) = range.as_object() {
                let conditions: Vec<String> = range_obj
                    .iter()
                    .filter_map(|(field, range_spec)| {
                        if let Some(spec_obj) = range_spec.as_object() {
                            let mut parts = Vec::new();

                            // 处理 gte (greater than or equal)
                            if let Some(gte) = spec_obj.get("gte") {
                                let rewritten = rewriter.rewrite_value(field, gte);
                                let val = format_sql_value(&rewritten)?;
                                parts.push(format!("{} >= {}", field, val));
                            }

                            // 处理 gt (greater than)
                            if let Some(gt) = spec_obj.get("gt") {
                                let rewritten = rewriter.rewrite_value(field, gt);
                                let val = format_sql_value(&rewritten)?;
                                parts.push(format!("{} > {}", field, val));
                            }

                            // 处理 lte (less than or equal)
                            if let Some(lte) = spec_obj.get("lte") {
                                let rewritten = rewriter.rewrite_value(field, lte);
                                let val = format_sql_value(&rewritten)?;
                                parts.push(format!("{} <= {}", field, val));
                            }

                            // 处理 lt (less than)
                            if let Some(lt) = spec_obj.get("lt") {
                                let rewritten = rewriter.rewrite_value(field, lt);
                                let val = format_sql_value(&rewritten)?;
                                parts.push(format!("{} < {}", field, val));
                            }

                            // 处理 from 和 to (另一种 range 语法)
                            if let Some(from) = spec_obj.get("from") {
                                let rewritten = rewriter.rewrite_value(field, from);
                                let val = format_sql_value(&rewritten)?;
                                // 检查 include_lower，默认为 true
                                let include_lower = spec_obj
                                    .get("include_lower")
                                    .and_then(|v| v.as_bool())
                                    .unwrap_or(true);
                                let op = if include_lower { ">=" } else { ">" };
                                parts.push(format!("{} {} {}", field, op, val));
                            }

                            if let Some(to) = spec_obj.get("to") {
                                let rewritten = rewriter.rewrite_value(field, to);
                                let val = format_sql_value(&rewritten)?;
                                // 检查 include_upper，默认为 true
                                let include_upper = spec_obj
                                    .get("include_upper")
                                    .and_then(|v| v.as_bool())
                                    .unwrap_or(true);
                                let op = if include_upper { "<=" } else { "<" };
                                parts.push(format!("{} {} {}", field, op, val));
                            }

                            if parts.is_empty() {
                                None
                            } else {
                                Some(parts.join(" AND "))
                            }
                        } else {
                            None
                        }
                    })
                    .collect();

                if !conditions.is_empty() {
                    return Some(conditions.join(" AND "));
                }
            }
        }

        // bool 查询
        if let Some(bool_query) = query_obj.get("bool") {
            if let Some(bool_obj) = bool_query.as_object() {
                let mut all_conditions = Vec::new();

                // must 子句 (AND)
                if let Some(must) = bool_obj.get("must") {
                    if let Some(must_array) = must.as_array() {
                        let must_conditions: Vec<String> = must_array
                            .iter()
                            .filter_map(|q| convert_es_query_to_sql(q, schema))
                            .collect();
                        if !must_conditions.is_empty() {
                            all_conditions.push(format!("({})", must_conditions.join(" AND ")));
                        }
                    }
                }

                // should 子句 (OR)
                if let Some(should) = bool_obj.get("should") {
                    if let Some(should_array) = should.as_array() {
                        let should_conditions: Vec<String> = should_array
                            .iter()
                            .filter_map(|q| convert_es_query_to_sql(q, schema))
                            .collect();
                        if !should_conditions.is_empty() {
                            all_conditions.push(format!("({})", should_conditions.join(" OR ")));
                        }
                    }
                }

                // must_not 子句 (NOT)
                if let Some(must_not) = bool_obj.get("must_not") {
                    if let Some(must_not_array) = must_not.as_array() {
                        let must_not_conditions: Vec<String> = must_not_array
                            .iter()
                            .filter_map(|q| convert_es_query_to_sql(q, schema))
                            .map(|c| format!("NOT ({})", c))
                            .collect();
                        if !must_not_conditions.is_empty() {
                            all_conditions.extend(must_not_conditions);
                        }
                    }
                }

                if !all_conditions.is_empty() {
                    return Some(all_conditions.join(" AND "));
                }
            }
        }
    }

    None
}

/// 将 ES sort 转换为 SQL ORDER BY
fn convert_es_sort_to_sql(sort: &Value) -> Option<String> {
    let mut order_clauses = Vec::new();

    // sort 可以是数组或对象
    if let Some(sort_array) = sort.as_array() {
        for sort_item in sort_array {
            if let Some(field_name) = sort_item.as_str() {
                // 简单格式: ["field1", "field2"]
                order_clauses.push(format!("{} ASC", field_name));
            } else if let Some(sort_obj) = sort_item.as_object() {
                // 对象格式: [{"field": {"order": "desc"}}]
                for (field, order_spec) in sort_obj {
                    let order = if let Some(spec_obj) = order_spec.as_object() {
                        spec_obj
                            .get("order")
                            .and_then(|v| v.as_str())
                            .unwrap_or("asc")
                    } else {
                        order_spec.as_str().unwrap_or("asc")
                    };

                    let order_upper = order.to_uppercase();
                    if order_upper == "ASC" || order_upper == "DESC" {
                        order_clauses.push(format!("{} {}", field, order_upper));
                    }
                }
            }
        }
    } else if let Some(sort_obj) = sort.as_object() {
        // 单个对象格式: {"field": "asc"} 或 {"field": {"order": "desc"}}
        for (field, order_spec) in sort_obj {
            let order = if let Some(spec_obj) = order_spec.as_object() {
                spec_obj
                    .get("order")
                    .and_then(|v| v.as_str())
                    .unwrap_or("asc")
            } else {
                order_spec.as_str().unwrap_or("asc")
            };

            let order_upper = order.to_uppercase();
            if order_upper == "ASC" || order_upper == "DESC" {
                order_clauses.push(format!("{} {}", field, order_upper));
            }
        }
    } else {
        log::error!("⚠️  [convert_es_sort_to_sql] Sort is neither array nor object");
    }

    let result = if order_clauses.is_empty() {
        None
    } else {
        let result_str = order_clauses.join(", ");
        Some(result_str)
    };

    result
}

/// 格式化 SQL 值
fn format_sql_value(value: &Value) -> Option<String> {
    match value {
        Value::String(s) => Some(format!("'{}'", s.replace("'", "''"))),
        Value::Number(n) => Some(n.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

/// 应用查询过滤
#[allow(dead_code)]
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
#[allow(dead_code)]
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
    _id: &str,
    doc: Value,
) -> Result<(), CoreError> {
    // 获取表的元数据以确定分区策略
    let table_meta = server
        .engine
        .get_table_meta(index)
        .map_err(|e| not_found(e.to_string()))?;

    // 根据分区策略提取分区字段的值

    let partition_value = table_meta
        .partition_strategy
        .router_field()
        .map(|f| doc.get(f).to_json_string())
        .unwrap_or_default();

    // 使用分区字段的值来路由
    let partition_id = server
        .engine
        .route_partition(index, &partition_value)
        .map_err(|e| not_found(e.to_string()))?;

    let partition = server
        .engine
        .get_partition(index, &partition_id)
        .await
        .ok_or_else(|| internal_error("Partition not found".to_string()))?;

    partition
        .upsert_json(&[doc])
        .map_err(|e| internal_error(e.to_string()))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    #[test]
    fn test() {
        // 创建包含不同类型的JSON文档
        let doc = json!({
            "integer": 42,
            "float": 3.14,
            "boolean_true": true,
            "boolean_false": false,
            "string": "hello",
            "array": [1, 2, 3],
            "object": {"key": "value"}
        });

        // 测试整数
        let integer_field = "integer";
        let integer_json = doc.get(integer_field).unwrap().to_string();
        println!("整数 {} 转换结果: '{}'", doc[integer_field], integer_json);

        // 测试浮点数
        let float_field = "float";
        let float_json = doc.get(float_field).unwrap().to_string();
        println!("浮点数 {} 转换结果: '{}'", doc[float_field], float_json);

        // 测试布尔值 true
        let bool_true_field = "boolean_true";
        let bool_true_json = doc.get(bool_true_field).unwrap().to_string();
        println!(
            "布尔值 {} 转换结果: '{}'",
            doc[bool_true_field], bool_true_json
        );

        // 测试布尔值 false
        let bool_false_field = "boolean_false";
        let bool_false_json = doc.get(bool_false_field).unwrap().to_string();
        println!(
            "布尔值 {} 转换结果: '{}'",
            doc[bool_false_field], bool_false_json
        );

        // 测试字符串
        let string_field = "string";
        let string_json = doc.get(string_field).unwrap().to_string();
        println!("字符串 {} 转换结果: '{}'", doc[string_field], string_json);

        // 测试Some包装
        let some_integer = Some(doc.get(integer_field).unwrap().to_string());
        println!("Some包装的整数结果: {:?}", some_integer);

        let some_bool = Some(doc.get(bool_true_field).unwrap().to_string());
        println!("Some包装的布尔值结果: {:?}", some_bool);
    }
}
