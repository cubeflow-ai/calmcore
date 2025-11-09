#!/bin/bash

# MySQL CREATE TABLE 功能测试脚本
# 
# 用法: ./test_create_table.sh
#
# 要求:
# 1. 已编译 mysql_create_table_demo: cargo build --example mysql_create_table_demo
# 2. 已安装 mysql 客户端

set -e

echo "╔══════════════════════════════════════════════════════════════╗"
echo "║       MySQL CREATE TABLE 功能测试                            ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""

# 1. 启动服务器 (后台)
echo "📌 Step 1: 启动 MySQL 服务器..."
cargo run --example mysql_create_table_demo > /tmp/mysql_server.log 2>&1 &
SERVER_PID=$!
echo "   服务器 PID: $SERVER_PID"

# 等待服务器启动
echo "⏳ 等待服务器启动..."
sleep 3

# 检查服务器是否运行
if ! kill -0 $SERVER_PID 2>/dev/null; then
    echo "❌ 服务器启动失败!"
    cat /tmp/mysql_server.log
    exit 1
fi

echo "✅ 服务器启动成功"
echo ""

# 2. 执行测试
echo "📌 Step 2: 执行 SQL 测试..."
echo ""

# 测试 SHOW TABLES (应该为空)
echo "🔍 Test 1: SHOW TABLES (初始状态)"
mysql -h 127.0.0.1 -P 3307 -e "SHOW TABLES;" || echo "❌ 连接失败"
echo ""

# 测试 CREATE TABLE
echo "🔍 Test 2: CREATE TABLE users"
mysql -h 127.0.0.1 -P 3307 -e "CREATE TABLE users (id BIGINT PRIMARY KEY, name TEXT, age INT);" && echo "✅ 创建成功" || echo "❌ 创建失败"
echo ""

# 测试 SHOW TABLES (应该有 users)
echo "🔍 Test 3: SHOW TABLES (应该有 users)"
mysql -h 127.0.0.1 -P 3307 -e "SHOW TABLES;"
echo ""

# 创建更多表
echo "🔍 Test 4: CREATE TABLE products"
mysql -h 127.0.0.1 -P 3307 -e "CREATE TABLE products (id INT PRIMARY KEY, title TEXT, price DOUBLE);" && echo "✅ 创建成功" || echo "❌ 创建失败"
echo ""

echo "🔍 Test 5: CREATE TABLE orders"
mysql -h 127.0.0.1 -P 3307 -e "CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, amount DOUBLE);" && echo "✅ 创建成功" || echo "❌ 创建失败"
echo ""

# 查看所有表
echo "🔍 Test 6: SHOW TABLES (应该有 3 个表)"
mysql -h 127.0.0.1 -P 3307 -e "SHOW TABLES;"
echo ""

# 删除一个表
echo "🔍 Test 7: DROP TABLE users"
mysql -h 127.0.0.1 -P 3307 -e "DROP TABLE users;" && echo "✅ 删除成功" || echo "❌ 删除失败"
echo ""

# 再次查看
echo "🔍 Test 8: SHOW TABLES (应该剩 2 个表)"
mysql -h 127.0.0.1 -P 3307 -e "SHOW TABLES;"
echo ""

# 3. 停止服务器
echo "📌 Step 3: 停止服务器..."
kill $SERVER_PID 2>/dev/null || true
wait $SERVER_PID 2>/dev/null || true
echo "✅ 服务器已停止"
echo ""

echo "╔══════════════════════════════════════════════════════════════╗"
echo "║              测试完成                                        ║"
echo "╚══════════════════════════════════════════════════════════════╝"
