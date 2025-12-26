#!/bin/bash

echo "=========================================="
echo "🔄 重启 MySQL 服务 (包含 JDBC 兼容性修复)"
echo "=========================================="

# 停止旧进程
pkill -9 -f "calm mysql" 2>/dev/null
sleep 1

# 启动新服务
cd /Users/sunjian11/rustworkspace/calmcore
echo "🚀 启动服务..."
./target/release/calm --mysql-port 3307 --no-graphql --no-es > /tmp/calm_mysql.log 2>&1 &

sleep 2

# 检查服务状态
if pgrep -f "calm mysql" > /dev/null; then
    echo "✅ MySQL 服务已启动在 3307 端口"
    echo ""
    echo "📋 修复内容:"
    echo "  1. CLIENT_SECURE_CONNECTION (0x8000) ✅"
    echo "  2. CLIENT_PLUGIN_AUTH (0x00080000) ✅"
    echo "  3. JDBC 初始化查询支持 (SELECT @@variable) ✅"
    echo "  4. MySQL 注释处理 (/* ... */) ✅"
    echo ""
    echo "🧪 运行测试:"
    echo "  java -cp .:mysql-connector-java-8.0.33.jar MysqlCursor"
    echo ""
    echo "📊 服务日志: tail -f /tmp/calm_mysql.log"
else
    echo "❌ 服务启动失败"
    echo "查看日志: tail -20 /tmp/calm_mysql.log"
    exit 1
fi
