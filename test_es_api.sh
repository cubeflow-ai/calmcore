#!/bin/bash

# Elasticsearch API 测试脚本

BASE_URL="http://localhost:9200"

echo "=== Elasticsearch API 测试 ==="
echo

# 1. 健康检查
echo "1. 健康检查"
curl -s "$BASE_URL/" | jq .
echo -e "\n"

# 2. 创建索引
echo "2. 创建 products 索引"
curl -s -X PUT "$BASE_URL/products" -H 'Content-Type: application/json' -d'{
  "mappings": {
    "properties": {
      "_id": { "type": "keyword" },
      "name": { "type": "text" },
      "price": { "type": "double" },
      "category": { "type": "keyword" },
      "in_stock": { "type": "boolean" }
    }
  }
}' | jq .
echo -e "\n"

# 3. 插入文档（指定ID）
echo "3. 插入文档 (ID=1)"
curl -s -X PUT "$BASE_URL/products/_doc/1" -H 'Content-Type: application/json' -d'{
  "name": "Laptop",
  "price": 999.99,
  "category": "electronics",
  "in_stock": true
}' | jq .
echo -e "\n"

# 4. 插入文档（自动生成ID）
echo "4. 插入文档 (自动生成ID)"
curl -s -X POST "$BASE_URL/products/_doc" -H 'Content-Type: application/json' -d'{
  "name": "Mouse",
  "price": 29.99,
  "category": "electronics",
  "in_stock": true
}' | jq .
echo -e "\n"

# 5. 批量插入
echo "5. 批量插入文档"
curl -s -X POST "$BASE_URL/products/_bulk" -H 'Content-Type: application/x-ndjson' --data-binary @- << EOF | jq .
{"index":{"_id":"2"}}
{"name":"Keyboard","price":79.99,"category":"electronics","in_stock":true}
{"index":{"_id":"3"}}
{"name":"Monitor","price":299.99,"category":"electronics","in_stock":false}
{"index":{"_id":"4"}}
{"name":"Webcam","price":59.99,"category":"electronics","in_stock":true}
EOF
echo -e "\n"

# 6. 获取文档
echo "6. 获取文档 (ID=1)"
curl -s -X GET "$BASE_URL/products/_doc/1" | jq .
echo -e "\n"

# 7. 搜索文档
echo "7. 搜索所有文档"
curl -s -X GET "$BASE_URL/products/_search" | jq .
echo -e "\n"

# 8. 获取索引信息
echo "8. 获取索引信息"
curl -s -X GET "$BASE_URL/products" | jq .
echo -e "\n"

# 9. 列出所有索引
echo "9. 列出所有索引"
curl -s -X GET "$BASE_URL/_cat/indices"
echo -e "\n"

echo "=== 测试完成 ==="
