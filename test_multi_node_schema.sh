#!/bin/bash

# 测试多节点 schema 不一致问题

echo "=== 测试多节点 Schema 不一致问题 ==="

# 等待编译完成
echo "等待编译完成..."
cd /home/ansj/rustworkspace/calmcore
cargo build --release 2>&1 | tail -3

# 复制二进制文件
cp target/release/calm deploy/cluster/

# 停止已有的节点
echo "停止已有节点..."
pkill -f "calm --config.*node"
sleep 2

# 清理数据目录
echo "清理数据目录..."
rm -rf deploy/cluster/data/tables/*

# 启动节点1
echo "启动节点1..."
cd deploy/cluster
./calm --config ./node1.toml > node1.log 2>&1 &
NODE1_PID=$!
echo "节点1 PID: $NODE1_PID"
sleep 3

# 启动节点2
echo "启动节点2..."
./calm --config ./node2.toml > node2.log 2>&1 &
NODE2_PID=$!
echo "节点2 PID: $NODE2_PID"
sleep 5

echo "=== 节点已启动，等待 gossip 同步 ==="
sleep 5

# 创建测试表
echo "=== 创建测试表 test_events ==="
curl -s -X POST http://localhost:9567/graphql \
  -H "Content-Type: application/json" \
  -d '{
    "query": "mutation { createTable(input: { name: \"test_events\", primaryKey: \"event_id\", partitionStrategy: { pkHash: { numPartitions: 4 } }, fields: [ { name: \"event_id\", fieldType: U64, nullable: false }, { name: \"user_id\", fieldType: U64, indexed: true }, { name: \"event_name\", fieldType: STRING }, { name: \"created_at\", fieldType: TIMESTAMP, format: \"ms\" } ] }) { name partitionCount } }"
  }' | jq .

sleep 2

# 插入测试数据
echo "=== 插入测试数据 ==="
mysql -h 127.0.0.1 -P 3307 -u root -proot123 -e "
INSERT INTO test_events (event_id, user_id, event_name, created_at) VALUES 
(1, 100, 'login', 1703750400000),
(2, 100, 'view_page', 1703750500000),
(3, 200, 'click_button', 1703750600000),
(4, 200, 'logout', 1703750700000),
(5, 300, 'login', 1703750800000),
(6, 300, 'purchase', 1703750900000),
(7, 400, 'view_page', 1703751000000),
(8, 400, 'logout', 1703751100000);
"

sleep 2

# 测试查询 - 只选择部分列（这会触发 projection）
echo "=== 测试查询（projection）==="
echo "查询 1: SELECT event_id FROM test_events"
mysql -h 127.0.0.1 -P 3307 -u root -proot123 -e "SELECT event_id FROM test_events LIMIT 10;" 2>&1

echo ""
echo "查询 2: SELECT event_name FROM test_events"
mysql -h 127.0.0.1 -P 3307 -u root -proot123 -e "SELECT event_name FROM test_events LIMIT 10;" 2>&1

echo ""
echo "查询 3: SELECT user_id, event_name FROM test_events"
mysql -h 127.0.0.1 -P 3307 -u root -proot123 -e "SELECT user_id, event_name FROM test_events LIMIT 10;" 2>&1

echo ""
echo "=== 检查日志中的 schema 信息 ==="
echo "--- Node1 Schema 日志 ---"
grep -A 2 "LazyPartitionExec.*schema" node1.log | tail -20

echo ""
echo "--- Node2 Schema 日志 ---"
grep -A 2 "LazyPartitionExec.*schema" node2.log | tail -20

echo ""
echo "=== 检查是否有 panic ==="
grep -i "panic\|index out of bounds" node1.log node2.log | tail -10

echo ""
echo "测试完成！"
echo "查看完整日志: tail -f deploy/cluster/node1.log"
echo "              tail -f deploy/cluster/node2.log"
