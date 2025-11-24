#!/bin/bash

echo "🔧 重启 MySQL 服务..."
pkill -9 -f "calm mysql" 2>/dev/null
sleep 1

echo "🚀 启动新编译的 MySQL 服务..."
cd /Users/sunjian11/rustworkspace/calmcore
./target/release/calm mysql --port 3307 > /tmp/calm_mysql.log 2>&1 &

sleep 2

if pgrep -f "calm mysql" > /dev/null; then
    echo "✅ MySQL 服务已启动在 3307 端口"
    echo ""
    echo "📊 Capability Flags 修改:"
    echo "  - CLIENT_SECURE_CONNECTION (0x8000) ✅"
    echo "  - CLIENT_PLUGIN_AUTH (0x00080000) ✅"
    echo ""
    echo "现在运行你的 Java 测试程序:"
    echo "  java -cp .:mysql-connector-java-*.jar MysqlCursor"
else
    echo "❌ 服务启动失败,查看日志: tail /tmp/calm_mysql.log"
    exit 1
fi
