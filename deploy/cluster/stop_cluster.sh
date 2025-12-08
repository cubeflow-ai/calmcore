#!/bin/bash
# 停止 CalmCore 双节点集群
# 用法: ./stop_cluster.sh

set -e

echo "🛑 停止 CalmCore 集群"
echo ""

# 停止 Node 1
if [ -f /tmp/calmcore_node1.pid ]; then
    NODE1_PID=$(cat /tmp/calmcore_node1.pid)
    if kill -0 $NODE1_PID 2>/dev/null; then
        echo "🔴 停止 Node 1 (PID: $NODE1_PID)"
        kill $NODE1_PID
        rm /tmp/calmcore_node1.pid
    else
        echo "⚠️  Node 1 进程不存在"
        rm /tmp/calmcore_node1.pid
    fi
else
    echo "⚠️  找不到 Node 1 PID 文件"
fi

# 停止 Node 2
if [ -f /tmp/calmcore_node2.pid ]; then
    NODE2_PID=$(cat /tmp/calmcore_node2.pid)
    if kill -0 $NODE2_PID 2>/dev/null; then
        echo "🔴 停止 Node 2 (PID: $NODE2_PID)"
        kill $NODE2_PID
        rm /tmp/calmcore_node2.pid
    else
        echo "⚠️  Node 2 进程不存在"
        rm /tmp/calmcore_node2.pid
    fi
else
    echo "⚠️  找不到 Node 2 PID 文件"
fi

echo ""
echo "✅ 集群已停止"
