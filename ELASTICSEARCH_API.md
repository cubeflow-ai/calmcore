# Calm Elasticsearch API

Calm 数据库的 Elasticsearch 兼容 API 实现。

## 功能特性

✅ **索引管理**
- 创建索引（PUT /:index）
- 删除索引（DELETE /:index）
- 获取索引信息（GET /:index）
- 列出所有索引（GET /_cat/indices）

✅ **文档操作**
- 插入文档（指定ID）- PUT /:index/_doc/:id
- 插入文档（自动ID）- POST /:index/_doc
- 获取文档 - GET /:index/_doc/:id
- 删除文档 - DELETE /:index/_doc/:id

✅ **批量操作**
- 批量插入/更新/删除 - POST /:index/_bulk

✅ **搜索功能**
- 基础搜索 - GET/POST /:index/_search
- 分页支持（from/size）

## 快速开始

### 1. 启动服务器

```bash
cargo run --release --example elasticsearch_server
```

服务器将在 `http://127.0.0.1:9200` 启动

### 2. 创建索引

```bash
curl -X PUT "http://localhost:9200/products" -H 'Content-Type: application/json' -d'{
  "mappings": {
    "properties": {
      "_id": { "type": "keyword" },
      "name": { "type": "text" },
      "price": { "type": "double" },
      "category": { "type": "keyword" },
      "in_stock": { "type": "boolean" }
    }
  }
}'
```

**响应：**
```json
{
  "acknowledged": true,
  "shards_acknowledged": true,
  "index": "products"
}
```

### 3. 插入文档

**指定ID：**
```bash
curl -X PUT "http://localhost:9200/products/_doc/1" -H 'Content-Type: application/json' -d'{
  "name": "Laptop",
  "price": 999.99,
  "category": "electronics",
  "in_stock": true
}'
```

**自动生成ID：**
```bash
curl -X POST "http://localhost:9200/products/_doc" -H 'Content-Type: application/json' -d'{
  "name": "Mouse",
  "price": 29.99,
  "category": "electronics",
  "in_stock": true
}'
```

### 4. 批量插入

```bash
curl -X POST "http://localhost:9200/products/_bulk" -H 'Content-Type: application/x-ndjson' -d'
{"index":{"_id":"2"}}
{"name":"Keyboard","price":79.99,"category":"electronics","in_stock":true}
{"index":{"_id":"3"}}
{"name":"Monitor","price":299.99,"category":"electronics","in_stock":false}
'
```

**响应：**
```json
{
  "took": 10,
  "errors": false,
  "items": [
    {
      "index": {
        "_index": "products",
        "_id": "2",
        "_version": 1,
        "result": "created",
        "status": 201
      }
    },
    {
      "index": {
        "_index": "products",
        "_id": "3",
        "_version": 1,
        "result": "created",
        "status": 201
      }
    }
  ]
}
```

### 5. 获取文档

```bash
curl -X GET "http://localhost:9200/products/_doc/1"
```

**响应：**
```json
{
  "_index": "products",
  "_id": "1",
  "_version": 1,
  "found": true,
  "_source": {
    "_id": "1",
    "name": "Laptop",
    "price": 999.99,
    "category": "electronics",
    "in_stock": true
  }
}
```

### 6. 搜索文档

```bash
curl -X GET "http://localhost:9200/products/_search"
```

**带参数搜索：**
```bash
curl -X POST "http://localhost:9200/products/_search" -H 'Content-Type: application/json' -d'{
  "size": 10,
  "from": 0
}'
```

### 7. 列出所有索引

```bash
curl -X GET "http://localhost:9200/_cat/indices"
```

### 8. 删除索引

```bash
curl -X DELETE "http://localhost:9200/products"
```

## 完整测试

运行测试脚本查看所有功能：

```bash
./test_es_api.sh
```

## 数据类型映射

| Elasticsearch 类型 | Calm 内部类型 |
|-------------------|--------------|
| keyword           | Keyword      |
| text              | Keyword      |
| long/integer      | I64          |
| float/double      | F64          |
| boolean           | Boolean      |

## API 兼容性

本实现兼容 Elasticsearch 的基础 API，可以使用：
- ✅ curl
- ✅ Elasticsearch 官方客户端（基础功能）
- ✅ Kibana（部分功能）
- ✅ Postman

## 性能特点

- 🚀 **高性能写入** - 基于 Arrow 列存储
- 💾 **内存索引** - 倒排索引常驻内存
- 📦 **自动持久化** - 后台异步持久化到磁盘
- 🔍 **快速查询** - 主键查询 O(1) 复杂度

## 限制

当前版本的限制：
- 搜索功能暂时返回空结果（正在开发中）
- 不支持复杂的 DSL 查询
- 不支持聚合查询（aggregations）
- 不支持更新操作（update）

## 下一步计划

- [ ] 实现完整的全文搜索
- [ ] 支持 Elasticsearch Query DSL
- [ ] 添加聚合功能
- [ ] 实现 update 操作
- [ ] 添加更多字段类型支持

## 架构

```
┌─────────────────────┐
│  Elasticsearch API  │
│   (HTTP/JSON)       │
└──────────┬──────────┘
           │
           v
┌─────────────────────┐
│   Axum Router       │
│  (REST Handlers)    │
└──────────┬──────────┘
           │
           v
┌─────────────────────┐
│      Engine         │
│  (Catalog/Routing)  │
└──────────┬──────────┘
           │
           v
┌─────────────────────┐
│    Partitions       │
│  (Sharding Layer)   │
└──────────┬──────────┘
           │
           v
┌─────────────────────┐
│     Segments        │
│ (Storage/Indexing)  │
└─────────────────────┘
```

## 示例代码

### Rust 客户端

```rust
use reqwest::Client;
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::new();
    
    // 创建索引
    client.put("http://localhost:9200/myindex")
        .json(&json!({
            "mappings": {
                "properties": {
                    "_id": { "type": "keyword" },
                    "title": { "type": "text" }
                }
            }
        }))
        .send()
        .await?;
    
    // 插入文档
    client.put("http://localhost:9200/myindex/_doc/1")
        .json(&json!({
            "title": "Hello World"
        }))
        .send()
        .await?;
    
    // 获取文档
    let doc = client.get("http://localhost:9200/myindex/_doc/1")
        .send()
        .await?
        .json::<serde_json::Value>()
        .await?;
    
    println!("{}", serde_json::to_string_pretty(&doc)?);
    
    Ok(())
}
```

### Python 客户端

```python
from elasticsearch import Elasticsearch

# 连接到 Calm
es = Elasticsearch(['http://localhost:9200'])

# 创建索引
es.indices.create(
    index='myindex',
    body={
        'mappings': {
            'properties': {
                '_id': {'type': 'keyword'},
                'title': {'type': 'text'}
            }
        }
    }
)

# 插入文档
es.index(
    index='myindex',
    id='1',
    body={'title': 'Hello World'}
)

# 获取文档
doc = es.get(index='myindex', id='1')
print(doc)
```

## 许可证

MIT License
