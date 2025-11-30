#!/bin/bash

# GraphQL DDL 功能验证脚本

echo "🧪 验证 GraphQL DDL 增强功能"
echo "================================"
echo ""

# 启动 Calm (假设已经在运行)
GRAPHQL_URL="http://localhost:9567/graphql"

echo "📋 测试 1: 创建表(带 description、default_value、nullable)"
echo "---------------------------------------------------"

curl -s -X POST "$GRAPHQL_URL" \
  -H "Content-Type: application/json" \
  -d '{
    "query": "mutation { createTable(input: { name: \"test_ddl_demo\", description: \"DDL 功能测试表\", primary_key: \"id\", partition_count: 1, fields: [ { name: \"id\", field_type: U64, description: \"主键 ID\", nullable: false }, { name: \"name\", field_type: KEYWORD, description: \"名称\", default_value: \"\\\"unknown\\\"\", nullable: true }, { name: \"age\", field_type: I8, description: \"年龄\", default_value: \"0\", nullable: true }, { name: \"active\", field_type: BOOLEAN, description: \"是否激活\", default_value: \"true\", nullable: false } ] }) { name partition_count fields { name field_type indexed } } }"
  }' | jq '.'

echo ""
echo "✅ 测试完成!"
echo ""
echo "📖 详细文档: docs/graphql_ddl_complete_example.md"
echo "🎮 GraphQL Playground: http://localhost:9567/playground"
