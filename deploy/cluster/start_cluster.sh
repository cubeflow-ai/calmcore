#!/bin/bash
# 启动 CalmCore 双节点集群
# 用法: ./start_cluster.sh

set -e

echo "🚀 启动 CalmCore 双节点集群"
echo ""

# 检查 calm 二进制是否存在
if ! command -v cargo &> /dev/null; then
    echo "❌ 错误: 找不到 cargo 命令"
    exit 1
fi

# 创建数据和日志目录
mkdir -p data/node1 data/node2
mkdir -p logs/node1 logs/node2

echo "📂 数据目录创建完成"
echo ""

# 启动 Node 1
echo "🟢 启动 Node 1 (端口: GraphQL=9567, ES=9200, MySQL=3307, Gossip=7946)"
RUST_LOG=info cargo run --bin calm -- --config deploy/cluster/node1.toml &
NODE1_PID=$!
echo "   进程 PID: $NODE1_PID"
echo ""

# 等待 Node 1 启动
sleep 3

# 启动 Node 2
echo "🟢 启动 Node 2 (端口: GraphQL=9568, ES=9201, MySQL=3308, Gossip=7947)"
RUST_LOG=info cargo run --bin calm -- --config deploy/cluster/node2.toml &
NODE2_PID=$!
echo "   进程 PID: $NODE2_PID"
echo ""

# 保存 PID 到文件
echo $NODE1_PID > /tmp/calmcore_node1.pid
echo $NODE2_PID > /tmp/calmcore_node2.pid

echo "✅ 集群启动完成！"
echo ""
echo "查看状态:"
echo "  Node 1: http://localhost:9567"
echo "  Node 2: http://localhost:9568"
echo ""
echo "停止集群:"
echo "  ./stop_cluster.sh"
echo ""
echo "查看日志:"
echo "  tail -f logs/node1/calm.log"
echo "  tail -f logs/node2/calm.log"
