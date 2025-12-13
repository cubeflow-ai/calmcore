#!/bin/bash

# 集群测试启动脚本
# 
# 用法：
#   ./examples/cluster_test.sh start   - 启动两个节点
#   ./examples/cluster_test.sh stop    - 停止所有节点
#   ./examples/cluster_test.sh restart - 重启所有节点
#   ./examples/cluster_test.sh status  - 查看节点状态

set -e

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_DIR"

# 日志目录
mkdir -p logs

# PID 文件
PID_FILE_1="logs/node1.pid"
PID_FILE_2="logs/node2.pid"

# 日志文件
LOG_FILE_1="logs/node1.log"
LOG_FILE_2="logs/node2.log"

# 启动节点函数
start_node1() {
    echo "🚀 Starting Node 1..."
    cargo build --bin calm --quiet
    cargo run --bin calm -- --config examples/node1.toml > "$LOG_FILE_1" 2>&1 &
    echo $! > "$PID_FILE_1"
    echo "✅ Node 1 started (PID: $(cat $PID_FILE_1))"
    echo "📋 Config: examples/node1.toml"
    echo "📋 Log file: $LOG_FILE_1"
    echo "📋 GraphQL: http://127.0.0.1:9567"
}

start_node2() {
    echo "🚀 Starting Node 2..."
    cargo build --bin calm --quiet
    cargo run --bin calm -- --config examples/node2.toml > "$LOG_FILE_2" 2>&1 &
    echo $! > "$PID_FILE_2"
    echo "✅ Node 2 started (PID: $(cat $PID_FILE_2))"
    echo "📋 Config: examples/node2.toml"
    echo "📋 Log file: $LOG_FILE_2"
    echo "📋 GraphQL: http://127.0.0.1:9568"
}

# 停止节点函数
stop_node() {
    local pid_file=$1
    local node_name=$2
    
    if [ -f "$pid_file" ]; then
        local pid=$(cat "$pid_file")
        if kill -0 "$pid" 2>/dev/null; then
            echo "🛑 Stopping $node_name (PID: $pid)..."
            kill "$pid"
            sleep 2
            if kill -0 "$pid" 2>/dev/null; then
                echo "⚠️  Force killing $node_name..."
                kill -9 "$pid"
            fi
        else
            echo "⚠️  $node_name process not found"
        fi
        rm -f "$pid_file"
    else
        echo "⚠️  $node_name PID file not found"
    fi
}

# 检查节点状态
check_status() {
    echo "📊 Cluster Status:"
    echo ""
    
    # 检查节点1
    if [ -f "$PID_FILE_1" ]; then
        local pid1=$(cat "$PID_FILE_1")
        if kill -0 "$pid1" 2>/dev/null; then
            echo "✅ Node 1: RUNNING (PID: $pid1)"
            echo "   Config: examples/node1.toml"
            echo "   Log: $LOG_FILE_1"
            echo "   GraphQL: http://127.0.0.1:9567"
            echo "   Gossip: 0.0.0.0:7946"
            echo "   RPC: 0.0.0.0:7947"
        else
            echo "❌ Node 1: STOPPED (stale PID file)"
        fi
    else
        echo "❌ Node 1: NOT STARTED"
    fi
    
    echo ""
    
    # 检查节点2
    if [ -f "$PID_FILE_2" ]; then
        local pid2=$(cat "$PID_FILE_2")
        if kill -0 "$pid2" 2>/dev/null; then
            echo "✅ Node 2: RUNNING (PID: $pid2)"
            echo "   Config: examples/node2.toml"
            echo "   Log: $LOG_FILE_2"
            echo "   GraphQL: http://127.0.0.1:9568"
            echo "   Gossip: 0.0.0.0:7948"
            echo "   RPC: 0.0.0.0:7949"
        else
            echo "❌ Node 2: STOPPED (stale PID file)"
        fi
    else
        echo "❌ Node 2: NOT STARTED"
    fi
    
    echo ""
    echo "📁 Data Directory: ./data"
    echo "📁 Logs Directory: ./logs"
}

# 查看日志
tail_logs() {
    echo "📜 Tailing cluster logs (Ctrl+C to stop)..."
    echo ""
    tail -f "$LOG_FILE_1" "$LOG_FILE_2" 2>/dev/null || echo "⚠️  Log files not found"
}

# 主命令处理
case "$1" in
    start)
        echo "🌟 Starting Calm Cluster Test Environment..."
        echo ""
        start_node1
        echo ""
        echo "⏳ Waiting 3 seconds for Node 1 to initialize..."
        sleep 3
        echo ""
        start_node2
        echo ""
        echo "⏳ Waiting 3 seconds for Node 2 to join cluster..."
        sleep 3
        echo ""
        check_status
        echo ""
        echo "💡 Tips:"
        echo "   - Check status: ./examples/cluster_test.sh status"
        echo "   - View logs: ./examples/cluster_test.sh logs"
        echo "   - Stop cluster: ./examples/cluster_test.sh stop"
        echo "   - Node 1 GraphQL: http://127.0.0.1:9567"
        echo "   - Node 2 GraphQL: http://127.0.0.1:9568"
        ;;
        
    stop)
        echo "🛑 Stopping Calm Cluster..."
        echo ""
        stop_node "$PID_FILE_1" "Node 1"
        stop_node "$PID_FILE_2" "Node 2"
        echo ""
        echo "✅ Cluster stopped"
        ;;
        
    restart)
        echo "🔄 Restarting Calm Cluster..."
        echo ""
        $0 stop
        echo ""
        sleep 2
        $0 start
        ;;
        
    status)
        check_status
        ;;
        
    logs)
        tail_logs
        ;;
        
    clean)
        echo "🧹 Cleaning up..."
        $0 stop
        echo ""
        echo "🗑️  Removing logs..."
        rm -f "$LOG_FILE_1" "$LOG_FILE_2"
        echo "✅ Cleanup complete"
        ;;
        
    *)
        echo "Usage: $0 {start|stop|restart|status|logs|clean}"
        echo ""
        echo "Commands:"
        echo "  start   - Start both cluster nodes"
        echo "  stop    - Stop all cluster nodes"
        echo "  restart - Restart the cluster"
        echo "  status  - Show cluster status"
        echo "  logs    - Tail cluster logs"
        echo "  clean   - Stop cluster and remove logs"
        exit 1
        ;;
esac
