#!/bin/bash

# ES 插入查询测试脚本
# 通过 GraphQL 创建表，通过 ES 插入和查询数据

set -e

GRAPHQL_URL="http://localhost:9567/graphql"
ES_URL="http://localhost:9200"
TABLE_NAME="test_products"

echo "======================================"
echo "Elasticsearch 插入查询测试"
echo "======================================"

# 1. 通过 GraphQL 创建表
echo ""
echo "步骤 1: 创建表 '$TABLE_NAME'"
echo "--------------------------------------"

curl -s -X POST "$GRAPHQL_URL" \
  -H "Content-Type: application/json" \
  -d "{\"query\":\"mutation { createTable(input: { name: \\\"$TABLE_NAME\\\", description: \\\"产品信息表\\\", primaryKey: \\\"product_id\\\", partitionStrategy: { pkHash: { numPartitions: 4 } }, fields: [ { name: \\\"product_id\\\", fieldType: U64, description: \\\"产品ID\\\", nullable: false, indexed: true }, { name: \\\"name\\\", fieldType: KEYWORD, description: \\\"产品名称\\\", nullable: false, indexed: true, caseSensitive: false }, { name: \\\"category\\\", fieldType: KEYWORD, description: \\\"分类\\\", nullable: true, indexed: true }, { name: \\\"price\\\", fieldType: F64, description: \\\"价格\\\", nullable: false, indexed: true }, { name: \\\"stock\\\", fieldType: I32, description: \\\"库存\\\", nullable: false, indexed: true, defaultValue: \\\"0\\\" }, { name: \\\"active\\\", fieldType: BOOLEAN, description: \\\"是否上架\\\", nullable: false, indexed: true, defaultValue: \\\"true\\\" }, { name: \\\"created_at\\\", fieldType: TIMESTAMP, description: \\\"创建时间\\\", nullable: false, indexed: true, format: \\\"iso8601\\\" } ] }) { name partitionCount } }\"}" | jq .

echo ""
echo "✅ 表创建成功"
sleep 1

# 2. 通过 Elasticsearch API 插入数据
echo ""
echo "步骤 2: 插入测试数据"
echo "--------------------------------------"

echo "2.1 插入单条数据（ID=1）"
curl -s -X PUT "$ES_URL/$TABLE_NAME/_doc/1" \
  -H "Content-Type: application/json" \
  -d '{"name":"iPhone 15 Pro","category":"手机","price":7999.0,"stock":100,"active":true,"created_at":1704067200000}' | jq .

echo ""
echo "2.2 批量插入数据"
curl -s -X POST "$ES_URL/$TABLE_NAME/_bulk" \
  -H "Content-Type: application/x-ndjson" \
  -d '{"index":{"_id":"2"}}
{"name":"MacBook Pro","category":"电脑","price":12999.0,"stock":50,"active":true,"created_at":1704067200000}
{"index":{"_id":"3"}}
{"name":"AirPods Pro","category":"耳机","price":1999.0,"stock":200,"active":true,"created_at":1704067200000}
{"index":{"_id":"4"}}
{"name":"iPad Air","category":"平板","price":4799.0,"stock":80,"active":true,"created_at":1704067200000}
{"index":{"_id":"5"}}
{"name":"Apple Watch","category":"手表","price":2999.0,"stock":150,"active":false,"created_at":1704067200000}
' | jq .

echo ""
echo "✅ 数据插入成功"
sleep 1

# 3. 通过 Elasticsearch API 查询数据
echo ""
echo "步骤 3: 查询数据"
echo "--------------------------------------"

echo "3.1 查询所有产品（前 10 条）"
curl -s -X POST "$ES_URL/$TABLE_NAME/_search" \
  -H "Content-Type: application/json" \
  -d '{"query":{"match_all":{}},"size":10}' | jq .

echo ""
echo "3.2 根据 ID 查询（ID=1）"
curl -s -X GET "$ES_URL/$TABLE_NAME/_doc/1" | jq .

echo ""
echo "3.3 term 查询（category=手机）"
curl -s -X POST "$ES_URL/$TABLE_NAME/_search" \
  -H "Content-Type: application/json" \
  -d '{"query":{"term":{"category":"手机"}}}' | jq .

echo ""
echo "3.4 range 查询（价格 2000-8000）"
curl -s -X POST "$ES_URL/$TABLE_NAME/_search" \
  -H "Content-Type: application/json" \
  -d '{"query":{"range":{"price":{"gte":2000,"lte":8000}}},"sort":[{"price":{"order":"asc"}}]}' | jq .

echo ""
echo "======================================"
echo "✅ 测试完成"
echo "======================================"
echo ""
echo "清理提示："
echo "  如需删除测试表，请运行："
echo "  curl -X DELETE \"$ES_URL/$TABLE_NAME\""
echo ""
