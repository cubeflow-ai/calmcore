#!/bin/bash

# 创建 r2api 表
# 使用方法: ./create_r2api_table.sh [graphql_url]

GRAPHQL_URL="${1:-http://127.0.0.1:9567/graphql}"

echo "Creating table 'r2api' at $GRAPHQL_URL..."

curl -X POST "$GRAPHQL_URL" \
  -H "Content-Type: application/json" \
  -d '{
    "query": "mutation { createTable(input: { name: \"r2api\", partitionCount: 3, fields: [ { name: \"trace_id\", fieldType: KEYWORD, indexed: true, caseSensitive: true }, { name: \"data_path\", fieldType: KEYWORD, indexed: true, caseSensitive: true }, { name: \"app_name\", fieldType: KEYWORD, indexed: true, caseSensitive: true }, { name: \"data\", fieldType: KEYWORD, indexed: false, caseSensitive: false } ], partitionStrategy: { strategyType: CUSTOM } }) { name partitionCount primaryKey fields { name fieldType indexed } } }"
  }' | python3 -m json.tool

echo ""
echo "Done!"
