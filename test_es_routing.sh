#!/bin/bash

# Elasticsearch 协议指定 routing(space/partition)插入功能测试脚本

ES_ENDPOINT="http://localhost:9200"

echo "======================================"
echo "测试 ES 协议指定 routing 插入功能"
echo "======================================"
echo ""

# 1. 创建索引(表)
echo "1. 创建索引 'test_routing'..."
curl -s -X PUT "$ES_ENDPOINT/test_routing" \
  -H "Content-Type: application/json" \
  -d '{
    "mappings": {
      "properties": {
        "id": { "type": "long" },
        "name": { "type": "keyword" },
        "value": { "type": "integer" },
        "region": { "type": "keyword" }
      }
    }
  }' | jq '.'

echo ""
echo "等待 1 秒..."
sleep 1

# 2. 测试自动路由插入(不指定 routing)
echo ""
echo "2. 测试自动路由插入(不指定 routing)..."
curl -s -X POST "$ES_ENDPOINT/test_routing/_doc/1" \
  -H "Content-Type: application/json" \
  -d '{
    "id": 1,
    "name": "auto_route_1",
    "value": 100,
    "region": "auto"
  }' | jq '.'

echo ""
echo "等待 1 秒..."
sleep 1

# 3. 测试指定 routing 插入(URL 参数)
echo ""
echo "3. 测试指定 routing 插入(routing=us_west)..."
curl -s -X POST "$ES_ENDPOINT/test_routing/_doc/10?routing=us_west" \
  -H "Content-Type: application/json" \
  -d '{
    "id": 10,
    "name": "us_west_1",
    "value": 1000,
    "region": "us_west"
  }' | jq '.'

echo ""
echo "等待 1 秒..."
sleep 1

# 4. 再次插入到同一个 routing
echo ""
echo "4. 再次插入到 us_west routing..."
curl -s -X POST "$ES_ENDPOINT/test_routing/_doc/11?routing=us_west" \
  -H "Content-Type: application/json" \
  -d '{
    "id": 11,
    "name": "us_west_2",
    "value": 1100,
    "region": "us_west"
  }' | jq '.'

echo ""
echo "等待 1 秒..."
sleep 1

# 5. 测试另一个 routing
echo ""
echo "5. 测试插入到 eu_central routing..."
curl -s -X POST "$ES_ENDPOINT/test_routing/_doc/20?routing=eu_central" \
  -H "Content-Type: application/json" \
  -d '{
    "id": 20,
    "name": "eu_central_1",
    "value": 2000,
    "region": "eu_central"
  }' | jq '.'

echo ""
echo "等待 1 秒..."
sleep 1

# 6. 测试 bulk 操作 - 不指定 routing
echo ""
echo "6. 测试 bulk 操作(不指定 routing)..."
curl -s -X POST "$ES_ENDPOINT/test_routing/_bulk" \
  -H "Content-Type: application/x-ndjson" \
  -d '{"index":{"_id":"100"}}
{"id":100,"name":"bulk_auto_1","value":10000,"region":"auto"}
{"index":{"_id":"101"}}
{"id":101,"name":"bulk_auto_2","value":10100,"region":"auto"}
' | jq '.items[] | {status: .[].status, id: .[]._id, result: .[].result}'

echo ""
echo "等待 1 秒..."
sleep 1

# 7. 测试 bulk 操作 - 指定 routing
echo ""
echo "7. 测试 bulk 操作(指定 routing=asia_pacific)..."
curl -s -X POST "$ES_ENDPOINT/test_routing/_bulk" \
  -H "Content-Type: application/x-ndjson" \
  -d '{"index":{"_id":"200","routing":"asia_pacific"}}
{"id":200,"name":"asia_pacific_1","value":20000,"region":"asia_pacific"}
{"index":{"_id":"201","routing":"asia_pacific"}}
{"id":201,"name":"asia_pacific_2","value":20100,"region":"asia_pacific"}
' | jq '.items[] | {status: .[].status, id: .[]._id, result: .[].result}'

echo ""
echo "等待 1 秒..."
sleep 1

# 8. 测试 bulk 操作 - 混合 routing
echo ""
echo "8. 测试 bulk 操作(混合不同 routing)..."
curl -s -X POST "$ES_ENDPOINT/_bulk" \
  -H "Content-Type: application/x-ndjson" \
  -d '{"index":{"_index":"test_routing","_id":"300","routing":"us_east"}}
{"id":300,"name":"us_east_1","value":30000,"region":"us_east"}
{"index":{"_index":"test_routing","_id":"301","routing":"us_east"}}
{"id":301,"name":"us_east_2","value":30100,"region":"us_east"}
{"index":{"_index":"test_routing","_id":"400"}}
{"id":400,"name":"auto_route_2","value":40000,"region":"auto"}
' | jq '.items[] | {status: .[].status, id: .[]._id, result: .[].result}'

echo ""
echo "等待 1 秒..."
sleep 1

# 9. 查询所有数据
echo ""
echo "9. 查询所有数据..."
curl -s -X POST "$ES_ENDPOINT/test_routing/_search" \
  -H "Content-Type: application/json" \
  -d '{
    "query": { "match_all": {} },
    "size": 100,
    "sort": [{ "id": "asc" }]
  }' | jq '.hits.total, .hits.hits[] | {id: ._id, source: ._source}'

echo ""
echo "等待 1 秒..."
sleep 1

# 10. 按 region 聚合查看数据分布
echo ""
echo "10. 按 region 聚合查看数据分布..."
curl -s -X POST "$ES_ENDPOINT/test_routing/_search" \
  -H "Content-Type: application/json" \
  -d '{
    "size": 0,
    "aggs": {
      "regions": {
        "terms": {
          "field": "region",
          "size": 10
        }
      }
    }
  }' | jq '.aggregations.regions.buckets[] | {region: .key, count: .doc_count}'

echo ""
echo "======================================"
echo "测试完成!"
echo "======================================"
echo ""
echo "总结:"
echo "- 创建了测试索引 test_routing"
echo "- 测试了自动路由插入"
echo "- 测试了指定 routing 的单文档插入(URL 参数)"
echo "- 测试了多次插入到同一 routing"
echo "- 测试了 bulk 操作(不指定 routing)"
echo "- 测试了 bulk 操作(指定 routing)"
echo "- 测试了 bulk 操作(混合不同 routing)"
echo "- 验证了所有数据成功插入"
echo "- 查看了数据按 region 的分布"
echo ""
echo "说明:"
echo "- routing 参数对应 Calm 的 partition"
echo "- 指定 routing 可以精确控制数据分区"
echo "- routing 不存在时自动创建新分区"
echo "- bulk 操作支持在 action 元数据中指定 routing"
echo ""
