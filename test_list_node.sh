#!/bin/bash

# 测试 list_node 和 node_info 接口
# list_node: 获取所有存活节点的状态（仅协调节点）
# node_info: 获取当前节点的状态

set -e

GRAPHQL_ENDPOINT="http://127.0.0.1:9567"

echo "🧪 Testing node status GraphQL interfaces..."
echo ""

# 1. 查询当前节点状态
echo "📊 Query 1: nodeInfo (current node status)..."
RESPONSE=$(curl -s -X POST "$GRAPHQL_ENDPOINT" \
  -H "Content-Type: application/json" \
  -d '{
    "query": "query { nodeInfo }"
  }')

echo "nodeInfo Response:"
echo "$RESPONSE" | jq '.'
echo ""

# 检查是否成功
if echo "$RESPONSE" | grep -q '"node_id"'; then
    echo "✅ Current node info retrieved successfully!"
    
    # 提取关键信息
    NODE_ID=$(echo "$RESPONSE" | jq -r '.data.nodeInfo.node_id')
    PARTITION_COUNT=$(echo "$RESPONSE" | jq -r '.data.nodeInfo.partition_count')
    CPU_USAGE=$(echo "$RESPONSE" | jq -r '.data.nodeInfo.cpu_usage')
    MEMORY_USAGE=$(echo "$RESPONSE" | jq -r '.data.nodeInfo.memory_usage')
    LOAD_AVG=$(echo "$RESPONSE" | jq -r '.data.nodeInfo.load_avg_1min')
    
    echo ""
    echo "📊 Current Node Status:"
    echo "   - Node ID: $NODE_ID"
    echo "   - Partition Count: $PARTITION_COUNT"
    echo "   - CPU Usage: ${CPU_USAGE}%"
    echo "   - Memory Usage: ${MEMORY_USAGE}%"
    echo "   - Load Average (1min): $LOAD_AVG"
else
    echo "❌ Failed to retrieve current node info!"
    exit 1
fi

echo ""
echo "================================================"
echo ""

# 2. 查询所有存活节点状态（仅协调节点可调用）
echo "📊 Query 2: listNode (all alive nodes status)..."
RESPONSE2=$(curl -s -X POST "$GRAPHQL_ENDPOINT" \
  -H "Content-Type: application/json" \
  -d '{
    "query": "query { listNode }"
  }')

echo "listNode Response:"
echo "$RESPONSE2" | jq '.'
echo ""

# 检查是否成功
if echo "$RESPONSE2" | grep -q '\[\]' || echo "$RESPONSE2" | grep -q '"node_id"'; then
    echo "✅ All alive nodes info retrieved successfully!"
    
    # 统计节点数量
    NODE_COUNT=$(echo "$RESPONSE2" | jq '.data.listNode | length')
    echo ""
    echo "📊 Cluster Summary:"
    echo "   - Total Alive Nodes: $NODE_COUNT"
    
    # 如果有节点数据，显示详情
    if [ "$NODE_COUNT" -gt 0 ]; then
        echo "   - Node Details:"
        echo "$RESPONSE2" | jq -r '.data.listNode[] | "     * \(.node_id): \(.partition_count) partitions, CPU: \(.cpu_usage)%, Memory: \(.memory_usage)%, Load: \(.load_avg_1min)"'
    fi
else
    echo "⚠️  Failed to retrieve all nodes info (might not be coordinator node)"
fi

echo ""
echo "🎉 Test completed!"
