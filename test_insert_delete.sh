#!/bin/bash

# MySQL INSERT/DELETE 功能测试脚本
# 
# 用法: ./test_insert_delete.sh

set -e

echo "╔══════════════════════════════════════════════════════════════╗"
echo "║       MySQL INSERT/DELETE 功能测试                           ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""

# 1. 启动服务器 (后台)
echo "📌 Step 1: 启动 MySQL 服务器..."
cargo run --example mysql_insert_delete_demo > /tmp/mysql_insert_delete_server.log 2>&1 &
SERVER_PID=$!
echo "   服务器 PID: $SERVER_PID"

# 等待服务器启动
echo "⏳ 等待服务器启动..."
sleep 3

# 检查服务器是否运行
if ! kill -0 $SERVER_PID 2>/dev/null; then
    echo "❌ 服务器启动失败!"
    cat /tmp/mysql_insert_delete_server.log
    exit 1
fi

echo "✅ 服务器启动成功"
echo ""

# 2. 执行测试
echo "📌 Step 2: 执行 SQL 测试..."
echo ""

# 测试 CREATE TABLE
echo "🔍 Test 1: CREATE TABLE"
mysql -h 127.0.0.1 -P 3308 -e "CREATE TABLE users (id BIGINT PRIMARY KEY, name TEXT, age INT);" && echo "✅ 创建表成功" || echo "❌ 创建表失败"
echo ""

# 测试 INSERT - 单条
echo "🔍 Test 2: INSERT 单条记录"
mysql -h 127.0.0.1 -P 3308 -e "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30);" && echo "✅ 插入成功" || echo "❌ 插入失败"
echo ""

# 测试 INSERT - 多条
echo "🔍 Test 3: INSERT 多条记录"
mysql -h 127.0.0.1 -P 3308 -e "INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25);"
mysql -h 127.0.0.1 -P 3308 -e "INSERT INTO users (id, name, age) VALUES (3, 'Charlie', 35);"
mysql -h 127.0.0.1 -P 3308 -e "INSERT INTO users (id, name, age) VALUES (4, 'David', 28);"
echo "✅ 多条插入成功"
echo ""

# 测试 SELECT - 查询所有
echo "🔍 Test 4: SELECT * FROM users (应该有 4 条记录)"
mysql -h 127.0.0.1 -P 3308 -e "SELECT * FROM users;"
echo ""

# 测试 SELECT - 条件查询
echo "🔍 Test 5: SELECT * FROM users WHERE age > 25"
mysql -h 127.0.0.1 -P 3308 -e "SELECT * FROM users WHERE age > 25;"
echo ""

# 测试 DELETE - 单条
echo "🔍 Test 6: DELETE FROM users WHERE id = 1"
mysql -h 127.0.0.1 -P 3308 -e "DELETE FROM users WHERE id = 1;" && echo "✅ 删除成功" || echo "❌ 删除失败"
echo ""

# 查询验证删除
echo "🔍 Test 7: SELECT * FROM users (应该剩 3 条记录,不包含 Alice)"
mysql -h 127.0.0.1 -P 3308 -e "SELECT * FROM users;"
echo ""

# 测试 DELETE - 条件删除
echo "🔍 Test 8: DELETE FROM users WHERE age < 30"
mysql -h 127.0.0.1 -P 3308 -e "DELETE FROM users WHERE age < 30;" && echo "✅ 条件删除成功" || echo "❌ 条件删除失败"
echo ""

# 最终验证
echo "🔍 Test 9: SELECT * FROM users (应该只剩 Charlie, age=35)"
mysql -h 127.0.0.1 -P 3308 -e "SELECT * FROM users;"
echo ""

# 测试 COUNT
echo "🔍 Test 10: SELECT COUNT(*) FROM users"
mysql -h 127.0.0.1 -P 3308 -e "SELECT COUNT(*) as total FROM users;"
echo ""

# 再插入一些数据测试
echo "🔍 Test 11: 再次插入数据并测试"
mysql -h 127.0.0.1 -P 3308 -e "INSERT INTO users (id, name, age) VALUES (10, 'Eve', 32);"
mysql -h 127.0.0.1 -P 3308 -e "INSERT INTO users (id, name, age) VALUES (11, 'Frank', 45);"
mysql -h 127.0.0.1 -P 3308 -e "SELECT * FROM users ORDER BY age;"
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
