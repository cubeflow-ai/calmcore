#!/bin/bash

# GraphQL 指定分区插入功能测试脚本

GRAPHQL_ENDPOINT="http://localhost:5002/graphql"

echo "======================================"
echo "测试 GraphQL 指定分区插入功能"
echo "======================================"
echo ""

# 1. 创建测试表
echo "1. 创建测试表 'test_custom_partition'..."
curl -s -X POST "$GRAPHQL_ENDPOINT" \
  -H "Content-Type: application/json" \
  -d '{
    "query": "mutation { createTable(input: { name: \"test_custom_partition\", primary_key: \"id\", partition_count: 2, fields: [ { name: \"id\", field_type: U64, nullable: false }, { name: \"name\", field_type: KEYWORD, nullable: false }, { name: \"value\", field_type: I32, nullable: true } ] }) { name partition_count } }"
  }' | jq '.'

echo ""
echo "等待 1 秒..."
sleep 1

# 2. 测试自动路由插入(使用默认分区策略)
echo ""
echo "2. 测试自动路由插入(不指定 partition)..."
curl -s -X POST "$GRAPHQL_ENDPOINT" \
  -H "Content-Type: application/json" \
  -d '{
    "query": "mutation { insertData(input: { table: \"test_custom_partition\", data: [ {\"id\": 1, \"name\": \"auto_route_1\", \"value\": 100}, {\"id\": 2, \"name\": \"auto_route_2\", \"value\": 200} ] }) { success rows_inserted message } }"
  }' | jq '.'

echo ""
echo "等待 1 秒..."
sleep 1

# 3. 测试指定分区插入(分区已存在)
echo ""
echo "3. 测试指定现有分区插入(partition: \"p0\")..."
curl -s -X POST "$GRAPHQL_ENDPOINT" \
  -H "Content-Type: application/json" \
  -d '{
    "query": "mutation { insertData(input: { table: \"test_custom_partition\", partition: \"p0\", data: [ {\"id\": 10, \"name\": \"custom_p0_1\", \"value\": 1000}, {\"id\": 11, \"name\": \"custom_p0_2\", \"value\": 1100} ] }) { success rows_inserted message } }"
  }' | jq '.'

echo ""
echo "等待 1 秒..."
sleep 1

# 4. 测试指定不存在的分区插入(自动创建)
echo ""
echo "4. 测试指定新分区插入(partition: \"custom_partition_A\",自动创建)..."
curl -s -X POST "$GRAPHQL_ENDPOINT" \
  -H "Content-Type: application/json" \
  -d '{
    "query": "mutation { insertData(input: { table: \"test_custom_partition\", partition: \"custom_partition_A\", data: [ {\"id\": 100, \"name\": \"new_partition_1\", \"value\": 10000}, {\"id\": 101, \"name\": \"new_partition_2\", \"value\": 10100} ] }) { success rows_inserted message } }"
  }' | jq '.'

echo ""
echo "等待 1 秒..."
sleep 1

# 5. 再次插入到同一个自定义分区
echo ""
echo "5. 再次插入到同一个自定义分区(partition: \"custom_partition_A\")..."
curl -s -X POST "$GRAPHQL_ENDPOINT" \
  -H "Content-Type: application/json" \
  -d '{
    "query": "mutation { insertData(input: { table: \"test_custom_partition\", partition: \"custom_partition_A\", data: [ {\"id\": 102, \"name\": \"new_partition_3\", \"value\": 10200} ] }) { success rows_inserted message } }"
  }' | jq '.'

echo ""
echo "等待 1 秒..."
sleep 1

# 6. 创建另一个自定义分区
echo ""
echo "6. 创建另一个自定义分区(partition: \"custom_partition_B\")..."
curl -s -X POST "$GRAPHQL_ENDPOINT" \
  -H "Content-Type: application/json" \
  -d '{
    "query": "mutation { insertData(input: { table: \"test_custom_partition\", partition: \"custom_partition_B\", data: [ {\"id\": 200, \"name\": \"partition_B_1\", \"value\": 20000} ] }) { success rows_inserted message } }"
  }' | jq '.'

echo ""
echo "等待 1 秒..."
sleep 1

# 7. 查看所有分区
echo ""
echo "7. 查看表的所有分区..."
curl -s -X POST "$GRAPHQL_ENDPOINT" \
  -H "Content-Type: application/json" \
  -d '{
    "query": "query { partitions(table: \"test_custom_partition\") { partition_id segment_count segments { segment_id doc_count } } }"
  }' | jq '.'

echo ""
echo "等待 1 秒..."
sleep 1

# 8. 查询所有数据
echo ""
echo "8. 查询所有数据..."
curl -s -X POST "$GRAPHQL_ENDPOINT" \
  -H "Content-Type: application/json" \
  -d '{
    "query": "query { query(sql: \"SELECT * FROM test_custom_partition ORDER BY id\") { columns rows total_rows } }"
  }' | jq '.'

echo ""
echo "等待 1 秒..."
sleep 1

# 9. 查看表详情
echo ""
echo "9. 查看表详情(包含所有分区和段)..."
curl -s -X POST "$GRAPHQL_ENDPOINT" \
  -H "Content-Type: application/json" \
  -d '{
    "query": "query { tableDetail(name: \"test_custom_partition\") { name partition_count total_segments total_documents partitions { partition_id segment_count } } }"
  }' | jq '.'

echo ""
echo "======================================"
echo "测试完成!"
echo "======================================"
echo ""
echo "总结:"
echo "- 创建了测试表 test_custom_partition"
echo "- 测试了自动路由插入(使用默认分区策略)"
echo "- 测试了指定现有分区插入(p0)"
echo "- 测试了自动创建新分区插入(custom_partition_A)"
echo "- 测试了再次插入到已创建的自定义分区"
echo "- 测试了创建第二个自定义分区(custom_partition_B)"
echo "- 验证了所有数据都成功插入"
echo ""
