#!/bin/bash
# 测试多节点分区查询 - 验证修复

set -e

echo "🧪 Testing multi-node partition query fix..."

# 清理旧数据和日志
echo "📁 Cleaning old data..."
rm -rf /tmp/calm_cluster_test/{node1,node2} logs/calm.log 2>/dev/null || true
mkdir -p /tmp/calm_cluster_test/{node1,node2}

# 启动集群
echo "🚀 Starting cluster..."
./examples/cluster_test.sh start

# 等待服务启动
echo "⏳ Waiting for services to start..."
sleep 5

# 创建测试表 (通过 node1 的 GraphQL)
echo "📋 Creating test table..."
curl -s -X POST http://127.0.0.1:9567/graphql \
  -H "Content-Type: application/json" \
  -d '{
    "query": "mutation { createTable(input: { name: \"test_multi_node\", primaryKey: \"id\", partitionStrategy: { pkHash: { numPartitions: 4 } }, fields: [ { name: \"id\", fieldType: U64, nullable: false }, { name: \"name\", fieldType: TEXT }, { name: \"value\", fieldType: I32 } ] }) { name partitionCount } }"
  }' | jq .

echo ""

# 插入测试数据 (通过 node1 的 MySQL)
echo "📝 Inserting test data..."
mysql -h 127.0.0.1 -P 3307 -u root -e "
  INSERT INTO test_multi_node (id, name, value) VALUES 
    (1, 'record1', 100),
    (2, 'record2', 200),
    (3, 'record3', 300),
    (4, 'record4', 400),
    (5, 'record5', 500);
"

# 等待数据同步
sleep 2

# 测试查询 (带 projection - 这会触发原来的 bug)
echo "🔍 Testing query with projection..."
mysql -h 127.0.0.1 -P 3307 -u root -e "SELECT id, value FROM test_multi_node ORDER BY id;" || {
  echo "❌ Query failed!"
  ./examples/cluster_test.sh logs
  ./examples/cluster_test.sh stop
  exit 1
}

echo ""
echo "🔍 Testing query with WHERE clause..."
mysql -h 127.0.0.1 -P 3307 -u root -e "SELECT name FROM test_multi_node WHERE value > 200;" || {
  echo "❌ Query with WHERE failed!"
  ./examples/cluster_test.sh logs
  ./examples/cluster_test.sh stop
  exit 1
}

echo ""
echo "🔍 Testing aggregation..."
mysql -h 127.0.0.1 -P 3307 -u root -e "SELECT COUNT(*) as total FROM test_multi_node;" || {
  echo "❌ Aggregation failed!"
  ./examples/cluster_test.sh logs
  ./examples/cluster_test.sh stop
  exit 1
}

echo ""
echo "✅ All tests passed!"

# 停止集群
echo "🛑 Stopping cluster..."
./examples/cluster_test.sh stop

echo "✨ Test completed successfully!"
