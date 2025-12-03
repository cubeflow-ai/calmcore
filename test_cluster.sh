#!/bin/bash
# Phase 1 集群功能测试
# 启动 3 个节点的 CalmCore 集群

set -e

echo "🚀 CalmCore Cluster Phase 1 - Test Script"
echo "========================================="
echo ""

# 清理旧进程
pkill -f "cluster_test" || true
sleep 1

# 启动节点 1（种子节点）
echo "Starting Node 1 (Seed)..."
CALM_NODE_ID="node-1" \
CALM_CLUSTER_ID="test-cluster" \
CALM_GOSSIP_ADDR="127.0.0.1:7946" \
CALM_SEED_NODES="" \
RUST_LOG=info \
cargo run --example cluster_test > /tmp/node1.log 2>&1 &
NODE1_PID=$!

sleep 2

# 启动节点 2
echo "Starting Node 2..."
CALM_NODE_ID="node-2" \
CALM_CLUSTER_ID="test-cluster" \
CALM_GOSSIP_ADDR="127.0.0.1:7947" \
CALM_SEED_NODES="127.0.0.1:7946" \
RUST_LOG=info \
cargo run --example cluster_test > /tmp/node2.log 2>&1 &
NODE2_PID=$!

sleep 2

# 启动节点 3
echo "Starting Node 3..."
CALM_NODE_ID="node-3" \
CALM_CLUSTER_ID="test-cluster" \
CALM_GOSSIP_ADDR="127.0.0.1:7948" \
CALM_SEED_NODES="127.0.0.1:7946" \
RUST_LOG=info \
cargo run --example cluster_test > /tmp/node3.log 2>&1 &
NODE3_PID=$!

echo ""
echo "✅ All nodes started!"
echo "  Node 1 PID: $NODE1_PID (logs: /tmp/node1.log)"
echo "  Node 2 PID: $NODE2_PID (logs: /tmp/node2.log)"
echo "  Node 3 PID: $NODE3_PID (logs: /tmp/node3.log)"
echo ""
echo "Press Ctrl+C to stop all nodes"
echo ""

# 等待用户中断
trap "echo ''; echo 'Stopping all nodes...'; kill $NODE1_PID $NODE2_PID $NODE3_PID 2>/dev/null; exit" INT TERM

# 显示日志
tail -f /tmp/node1.log
