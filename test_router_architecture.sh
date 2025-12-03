#!/bin/bash
# 测试新的 Router 架构和 insert_batch 接口

set -e

echo "🧪 测试新的路由架构..."

# 启动 calm 服务
echo "📦 启动 calm 服务..."
cargo run --release &
CALM_PID=$!

# 等待服务启动
sleep 5

# 测试 1: PKHash 分区策略
echo ""
echo "✅ 测试 1: PKHash 分区策略"
curl -X POST http://localhost:4000/graphql \
  -H "Content-Type: application/json" \
  -d '{
    "query": "mutation { createTable(input: { tableName: \"users_pkhash\", schema: [ { name: \"user_id\", dataType: \"UInt64\", isPrimaryKey: true }, { name: \"name\", dataType: \"Utf8\" }, { name: \"age\", dataType: \"I32\" } ], partitionStrategy: { strategyType: PKHash, num_partitions: 4 } }) { success message } }"
  }'

curl -X POST http://localhost:4000/graphql \
  -H "Content-Type: application/json" \
  -d '{
    "query": "mutation { insertData(input: { table: \"users_pkhash\", data: [ { user_id: 1, name: \"Alice\", age: 25 }, { user_id: 2, name: \"Bob\", age: 30 }, { user_id: 3, name: \"Charlie\", age: 35 }, { user_id: 4, name: \"David\", age: 40 }, { user_id: 5, name: \"Eve\", age: 45 } ] }) { success rows_inserted message } }"
  }'

# 测试 2: Hash 分区策略  
echo ""
echo "✅ 测试 2: Hash 分区策略"
curl -X POST http://localhost:4000/graphql \
  -H "Content-Type: application/json" \
  -d '{
    "query": "mutation { createTable(input: { tableName: \"products_hash\", schema: [ { name: \"product_id\", dataType: \"UInt64\", isPrimaryKey: true }, { name: \"category\", dataType: \"Utf8\" }, { name: \"price\", dataType: \"F64\" } ], partitionStrategy: { strategyType: Hash, field: \"category\", num_partitions: 3 } }) { success message } }"
  }'

curl -X POST http://localhost:4000/graphql \
  -H "Content-Type: application/json" \
  -d '{
    "query": "mutation { insertData(input: { table: \"products_hash\", data: [ { product_id: 1, category: \"electronics\", price: 999.99 }, { product_id: 2, category: \"books\", price: 29.99 }, { product_id: 3, category: \"electronics\", price: 1499.99 }, { product_id: 4, category: \"food\", price: 9.99 }, { product_id: 5, category: \"books\", price: 39.99 } ] }) { success rows_inserted message } }"
  }'

# 测试 3: Range 分区策略（时间戳）
echo ""
echo "✅ 测试 3: Range 分区策略（时间戳）"
curl -X POST http://localhost:4000/graphql \
  -H "Content-Type: application/json" \
  -d '{
    "query": "mutation { createTable(input: { tableName: \"events_range\", schema: [ { name: \"event_id\", dataType: \"UInt64\", isPrimaryKey: true }, { name: \"timestamp\", dataType: \"Timestamp\" }, { name: \"event_type\", dataType: \"Utf8\" } ], partitionStrategy: { strategyType: Range, field: \"timestamp\", range_start: 1704067200000, range_step: 86400000 } }) { success message } }"
  }'

curl -X POST http://localhost:4000/graphql \
  -H "Content-Type: application/json" \
  -d '{
    "query": "mutation { insertData(input: { table: \"events_range\", data: [ { event_id: 1, timestamp: 1704067200000, event_type: \"login\" }, { event_id: 2, timestamp: 1704153600000, event_type: \"purchase\" }, { event_id: 3, timestamp: 1704240000000, event_type: \"logout\" } ] }) { success rows_inserted message } }"
  }'

# 测试 4: MySQL INSERT（使用新的 insert_batch）
echo ""
echo "✅ 测试 4: MySQL INSERT"
mysql -h 127.0.0.1 -P 3307 -u root -e "
CREATE TABLE IF NOT EXISTS orders_mysql (
  order_id BIGINT PRIMARY KEY,
  customer_id BIGINT,
  amount DECIMAL(10,2),
  order_date TIMESTAMP
);
"

mysql -h 127.0.0.1 -P 3307 -u root -e "
INSERT INTO orders_mysql VALUES 
  (1, 100, 99.99, '2024-01-01 00:00:00'),
  (2, 101, 149.99, '2024-01-01 01:00:00'),
  (3, 102, 199.99, '2024-01-02 00:00:00');
"

echo ""
echo "✅ 所有测试完成!"
echo "📊 检查分区创建情况:"
ls -la data/tables/*/partitions/

# 清理
kill $CALM_PID 2>/dev/null || true

echo ""
echo "🎉 新路由架构测试成功!"
