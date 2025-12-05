#!/bin/bash

# 测试 OneofObject 分区策略配置

BASE_URL="http://localhost:9567/graphql"

echo "=== 测试 1: PKHash 分区 (最简单) ==="
curl -s -X POST $BASE_URL \
  -H "Content-Type: application/json" \
  -d '{
    "query": "mutation { createTable(input: { name: \"test_pkhash\", primaryKey: \"id\", partitionStrategy: { pkHash: { numPartitions: 4 } }, fields: [{ name: \"id\", fieldType: U64 }, { name: \"name\", fieldType: KEYWORD }] }) { name partitionCount } }"
  }' | python3 -m json.tool

echo -e "\n=== 测试 2: Hash 分区 (指定字段) ==="
curl -s -X POST $BASE_URL \
  -H "Content-Type: application/json" \
  -d '{
    "query": "mutation { createTable(input: { name: \"test_hash\", partitionStrategy: { hash: { field: \"user_id\", numPartitions: 8 } }, fields: [{ name: \"user_id\", fieldType: U64 }, { name: \"event\", fieldType: KEYWORD }] }) { name partitionCount } }"
  }' | python3 -m json.tool

echo -e "\n=== 测试 3: DatetimeRange 分区 (时序数据) ==="
curl -s -X POST $BASE_URL \
  -H "Content-Type: application/json" \
  -d '{
    "query": "mutation { createTable(input: { name: \"test_datetime\", primaryKey: \"log_id\", partitionStrategy: { datetimeRange: { field: \"timestamp\", granularity: DAY, timezone: \"UTC\", parallelism: 2 } }, fields: [{ name: \"log_id\", fieldType: U64 }, { name: \"timestamp\", fieldType: TIMESTAMP }, { name: \"message\", fieldType: KEYWORD }] }) { name partitionCount } }"
  }' | python3 -m json.tool

echo -e "\n=== 测试 4: Range 分区 (手动范围) ==="
curl -s -X POST $BASE_URL \
  -H "Content-Type: application/json" \
  -d '{
    "query": "mutation { createTable(input: { name: \"test_range\", primaryKey: \"order_id\", partitionStrategy: { range: { field: \"created_at\", start: 1704067200000, step: 86400000, numPartitions: 30, parallelism: 1 } }, fields: [{ name: \"order_id\", fieldType: U64 }, { name: \"created_at\", fieldType: TIMESTAMP }, { name: \"amount\", fieldType: F64 }] }) { name partitionCount } }"
  }' | python3 -m json.tool

echo -e "\n=== 测试 5: None 分区 (小表) ==="
curl -s -X POST $BASE_URL \
  -H "Content-Type: application/json" \
  -d '{
    "query": "mutation { createTable(input: { name: \"test_none\", primaryKey: \"config_id\", partitionStrategy: { none: { enabled: true } }, fields: [{ name: \"config_id\", fieldType: U64 }, { name: \"key\", fieldType: KEYWORD }, { name: \"value\", fieldType: KEYWORD }] }) { name partitionCount } }"
  }' | python3 -m json.tool

echo -e "\n=== 清理测试表 ==="
for table in test_pkhash test_hash test_datetime test_range test_none; do
  curl -s -X POST $BASE_URL \
    -H "Content-Type: application/json" \
    -d "{\"query\": \"mutation { dropTable(name: \\\"$table\\\") }\"}" > /dev/null
  echo "已删除: $table"
done

echo -e "\n=== 测试完成 ==="
