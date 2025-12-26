#!/bin/bash

# 测试统一的 gRPC 架构

set -e

echo "🧹 清理旧数据..."
rm -rf data/node1 data/node2 logs/node1 logs/node2
mkdir -p data/node1 data/node2 logs/node1 logs/node2

echo ""
echo "🚀 启动节点 1 (种子节点)..."
cargo run --bin calm -- --config examples/node1.toml > logs/node1/startup.log 2>&1 &
NODE1_PID=$!
echo "   PID: $NODE1_PID"

sleep 5

echo ""
echo "📋 检查节点 1 的 gRPC 端口分配..."
grep -E "gRPC.*(port|allocated)" logs/node1/startup.log || echo "   (未找到日志)"
grep "Arrow Flight" logs/node1/startup.log || echo "   (未找到 Flight 服务日志)"

echo ""
echo "🚀 启动节点 2..."
cargo run --bin calm -- --config examples/node2.toml > logs/node2/startup.log 2>&1 &
NODE2_PID=$!
echo "   PID: $NODE2_PID"

sleep 5

echo ""
echo "📋 检查节点 2 的 gRPC 端口分配..."
grep -E "gRPC.*(port|allocated)" logs/node2/startup.log || echo "   (未找到日志)"
grep "Arrow Flight" logs/node2/startup.log || echo "   (未找到 Flight 服务日志)"

echo ""
echo "📡 检查集群发现..."
sleep 3
grep "Found coordinator" logs/node1/startup.log logs/node2/startup.log || echo "   (节点还在寻找协调者)"

echo ""
echo "✅ 测试完成！"
echo ""
echo "查看节点 1 日志: tail -f logs/node1/startup.log"
echo "查看节点 2 日志: tail -f logs/node2/startup.log"
echo ""
echo "停止节点: kill $NODE1_PID $NODE2_PID"
echo ""
echo "保存这些 PID: "
echo "NODE1_PID=$NODE1_PID" > logs/pids.sh
echo "NODE2_PID=$NODE2_PID" >> logs/pids.sh
echo "   (执行 source logs/pids.sh 来恢复)"
