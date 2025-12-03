#!/bin/bash

# 测试流式查询的内存使用
# 监控 calm 进程的内存占用

echo "🧪 测试流式查询内存占用"
echo "================================"

# 查找 calm 进程 PID
CALM_PID=$(pgrep -f "target/release/calm")

if [ -z "$CALM_PID" ]; then
    echo "❌ 未找到 calm 进程,请先启动服务"
    echo "提示: 在另一个终端运行 'cargo run --release'"
    exit 1
fi

echo "📍 Calm PID: $CALM_PID"

# 记录初始内存
INIT_MEM=$(ps -o rss= -p $CALM_PID)
echo "📊 初始内存: $(echo "scale=2; $INIT_MEM / 1024" | bc) MB"

echo ""
echo "🚀 开始执行大查询..."
echo "查询: SELECT '11.3.83.3','r2api','r2api',app_name FROM r2api"

# 在后台监控内存
(
    while true; do
        CURRENT_MEM=$(ps -o rss= -p $CALM_PID 2>/dev/null)
        if [ -z "$CURRENT_MEM" ]; then
            break
        fi
        CURRENT_MB=$(echo "scale=2; $CURRENT_MEM / 1024" | bc)
        DELTA=$(echo "scale=2; ($CURRENT_MEM - $INIT_MEM) / 1024" | bc)
        echo "📈 当前内存: ${CURRENT_MB} MB (增长: ${DELTA} MB)"
        sleep 2
    done
) &
MONITOR_PID=$!

# 执行查询到本地 calm (3307端口)
mysql -h127.0.0.1 -P3307 -uroot -pcalm -e "select '11.3.83.3','r2api','r2api',app_name,data_path,app_name,data from r2api" > /dev/null 2>&1

# 停止监控
kill $MONITOR_PID 2>/dev/null

# 最终内存
FINAL_MEM=$(ps -o rss= -p $CALM_PID)
FINAL_MB=$(echo "scale=2; $FINAL_MEM / 1024" | bc)
TOTAL_DELTA=$(echo "scale=2; ($FINAL_MEM - $INIT_MEM) / 1024" | bc)

echo ""
echo "================================"
echo "📊 测试结果:"
echo "  初始内存: $(echo "scale=2; $INIT_MEM / 1024" | bc) MB"
echo "  最终内存: ${FINAL_MB} MB"
echo "  内存增长: ${TOTAL_DELTA} MB"
echo ""

if (( $(echo "$TOTAL_DELTA < 100" | bc -l) )); then
    echo "✅ 内存控制良好 (< 100MB增长)"
else
    echo "⚠️  内存增长较大 (> 100MB增长)"
fi
