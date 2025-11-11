#!/bin/bash

echo "=== Elasticsearch 搜索功能测试 ==="
echo

# 1. 测试基本的 match_all 查询
echo "1. 测试 match_all 查询 (应该返回空结果):"
curl -s -X POST "http://localhost:9200/products/_search" -H "Content-Type: application/json" -d '{
    "query": {
        "match_all": {}
    }
}' | jq '.hits'
echo

# 2. 测试带分页的查询
echo "2. 测试带分页的查询:"
curl -s -X POST "http://localhost:9200/products/_search" -H "Content-Type: application/json" -d '{
    "query": {
        "match_all": {}
    },
    "from": 0,
    "size": 5
}' | jq '.hits.total, .hits.hits'
echo

# 3. 测试 term 查询（目前应该没有结果）
echo "3. 测试 term 查询:"
curl -s -X POST "http://localhost:9200/products/_search" -H "Content-Type: application/json" -d '{
    "query": {
        "term": {
            "category": "electronics"
        }
    }
}' | jq '.hits.total'
echo

# 4. 测试 match 查询
echo "4. 测试 match 查询:"
curl -s -X POST "http://localhost:9200/products/_search" -H "Content-Type: application/json" -d '{
    "query": {
        "match": {
            "name": "mouse"
        }
    }
}' | jq '.hits.total'
echo

# 5. 测试索引信息
echo "5. 测试获取索引信息:"
curl -s -X GET "http://localhost:9200/products" | jq '.'
echo

# 6. 测试列出所有索引
echo "6. 测试列出所有索引:"
curl -s -X GET "http://localhost:9200/_cat/indices" | grep -E "(products|demo_index)"
echo

echo "=== 测试完成 ==="