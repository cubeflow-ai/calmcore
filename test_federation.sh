#!/bin/bash
# 测试 Federation 分布式查询功能

set -e

echo "=========================================="
echo "测试 1: 单机模式（无 ClusterManager）"
echo "=========================================="

# 启动单机模式
cargo run --bin calm -- --config deploy/standalone.toml &
CALM_PID=$!
sleep 3

echo "创建测试表..."
curl -X POST http://localhost:9567/graphql \
  -H "Content-Type: application/json" \
  -d '{
    "query": "mutation { createTable(input: { name: \"test_table\", primaryKey: \"id\", partitionStrategy: { pkHash: { numPartitions: 4 } }, fields: [{ name: \"id\", fieldType: U64, nullable: false }, { name: \"name\", fieldType: KEYWORD }, { name: \"value\", fieldType: I64 }] }) { name partitionCount } }"
  }'

echo -e "\n\n插入数据..."
mysql -h127.0.0.1 -P3307 -uroot -pcalm -e "
USE calm;
INSERT INTO test_table (id, name, value) VALUES (1, 'alice', 100), (2, 'bob', 200), (3, 'charlie', 300), (4, 'david', 400);
"

echo -e "\n查询数据（应该走 fallback 单机模式）..."
mysql -h127.0.0.1 -P3307 -uroot -pcalm -e "
USE calm;
SELECT * FROM test_table WHERE value > 150 ORDER BY id;
"

# 清理
kill $CALM_PID
sleep 1

echo -e "\n=========================================="
echo "测试 2: 集群模式（2节点）"
echo "=========================================="

# 启动两节点集群
./examples/cluster_test.sh start

sleep 5

echo "创建分区表..."
curl -X POST http://localhost:9567/graphql \
  -H "Content-Type: application/json" \
  -d '{
    "query": "mutation { createTable(input: { name: \"events\", primaryKey: \"event_id\", partitionStrategy: { pkHash: { numPartitions: 8 } }, fields: [{ name: \"event_id\", fieldType: U64, nullable: false }, { name: \"user_id\", fieldType: U64, indexed: true }, { name: \"event_type\", fieldType: KEYWORD }, { name: \"value\", fieldType: I64 }] }) { name partitionCount } }"
  }'

echo -e "\n\n插入数据（会分布到两个节点）..."
mysql -h127.0.0.1 -P3307 -uroot -pcalm -e "
USE calm;
INSERT INTO events (event_id, user_id, event_type, value) VALUES 
  (1, 100, 'click', 10),
  (2, 101, 'view', 20),
  (3, 102, 'click', 30),
  (4, 103, 'purchase', 100),
  (5, 104, 'view', 15),
  (6, 105, 'click', 25),
  (7, 106, 'purchase', 200),
  (8, 107, 'view', 5);
"

echo -e "\n查询数据（应该走 FederatedQueryExecutor）..."
mysql -h127.0.0.1 -P3307 -uroot -pcalm -e "
USE calm;
SELECT event_id, event_type, value FROM events WHERE value > 20 ORDER BY event_id;
"

echo -e "\n测试 Filter Pushdown..."
mysql -h127.0.0.1 -P3307 -uroot -pcalm -e "
USE calm;
SELECT * FROM events WHERE event_type = 'purchase' ORDER BY event_id;
"

echo -e "\n测试 Projection Pushdown..."
mysql -h127.0.0.1 -P3307 -uroot -pcalm -e "
USE calm;
SELECT event_id, value FROM events WHERE user_id > 103 ORDER BY event_id;
"

# 清理
./examples/cluster_test.sh stop

echo -e "\n=========================================="
echo "✅ Federation 测试完成！"
echo "=========================================="
