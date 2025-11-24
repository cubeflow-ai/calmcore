#!/bin/bash

# MySQL 游标功能测试脚本

MYSQL_HOST="127.0.0.1"
MYSQL_PORT="3307"

echo "=========================================="
echo "  CalmCore MySQL Cursor 功能测试"
echo "=========================================="
echo ""

# 测试连接
echo "1️⃣  测试连接..."
mysql -h $MYSQL_HOST -P $MYSQL_PORT -u root -e "SELECT 'Connected successfully' as status;" 2>/dev/null
if [ $? -ne 0 ]; then
    echo "❌ 无法连接到 MySQL 服务器"
    echo "   请先启动服务: cargo run --release -- --mysql-port 3307"
    exit 1
fi
echo "✅ 连接成功"
echo ""

# 测试 Prepared Statement
echo "2️⃣  测试 Prepared Statement..."
mysql -h $MYSQL_HOST -P $MYSQL_PORT -u root <<EOF
-- 显示所有表
SHOW TABLES;
EOF
echo ""

# 检查是否有测试数据
echo "3️⃣  检查测试数据..."
TABLE_EXISTS=$(mysql -h $MYSQL_HOST -P $MYSQL_PORT -u root -N -e "SHOW TABLES;" | grep -c "r2api")

if [ "$TABLE_EXISTS" -eq 0 ]; then
    echo "⚠️  没有找到 r2api 表,将跳过游标测试"
    echo "   请先创建表并插入数据"
    exit 0
fi

# 查看表数据量
echo "📊 查看 r2api 表数据量..."
mysql -h $MYSQL_HOST -P $MYSQL_PORT -u root -e "SELECT COUNT(*) as total_rows FROM r2api;" 2>/dev/null
echo ""

# 测试游标 - 基本用法
echo "4️⃣  测试游标基本功能..."
echo "===================="
echo ""

echo "📌 声明游标..."
mysql -h $MYSQL_HOST -P $MYSQL_PORT -u root <<'EOF'
DECLARE test_cursor CURSOR FOR SELECT * FROM r2api LIMIT 20;
EOF

if [ $? -eq 0 ]; then
    echo "✅ 游标声明成功"
else
    echo "❌ 游标声明失败"
    exit 1
fi
echo ""

echo "🔍 第一次 FETCH (获取 5 行)..."
mysql -h $MYSQL_HOST -P $MYSQL_PORT -u root <<'EOF'
FETCH 5 FROM test_cursor;
EOF
echo ""

echo "🔍 第二次 FETCH (获取 5 行)..."
mysql -h $MYSQL_HOST -P $MYSQL_PORT -u root <<'EOF'
FETCH 5 FROM test_cursor;
EOF
echo ""

echo "🔍 第三次 FETCH (获取 10 行)..."
mysql -h $MYSQL_HOST -P $MYSQL_PORT -u root <<'EOF'
FETCH 10 FROM test_cursor;
EOF
echo ""

echo "🗑️  关闭游标..."
mysql -h $MYSQL_HOST -P $MYSQL_PORT -u root <<'EOF'
CLOSE test_cursor;
EOF

if [ $? -eq 0 ]; then
    echo "✅ 游标关闭成功"
else
    echo "❌ 游标关闭失败"
fi
echo ""

# 测试游标 - 不同语法
echo "5️⃣  测试不同的 FETCH 语法..."
echo "===================="
echo ""

echo "📌 声明新游标..."
mysql -h $MYSQL_HOST -P $MYSQL_PORT -u root <<'EOF'
DECLARE syntax_cursor CURSOR FOR SELECT id, api_url FROM r2api LIMIT 15;
EOF
echo ""

echo "🔍 FETCH FORWARD 3..."
mysql -h $MYSQL_HOST -P $MYSQL_PORT -u root <<'EOF'
FETCH FORWARD 3 FROM syntax_cursor;
EOF
echo ""

echo "🔍 FETCH (默认 1 行)..."
mysql -h $MYSQL_HOST -P $MYSQL_PORT -u root <<'EOF'
FETCH FROM syntax_cursor;
EOF
echo ""

echo "🗑️  关闭游标..."
mysql -h $MYSQL_HOST -P $MYSQL_PORT -u root <<'EOF'
CLOSE syntax_cursor;
EOF
echo ""

# 测试错误处理
echo "6️⃣  测试错误处理..."
echo "===================="
echo ""

echo "❌ 尝试从不存在的游标获取数据..."
mysql -h $MYSQL_HOST -P $MYSQL_PORT -u root <<'EOF'
FETCH FROM non_existent_cursor;
EOF
echo ""

echo "❌ 尝试关闭不存在的游标..."
mysql -h $MYSQL_HOST -P $MYSQL_PORT -u root <<'EOF'
CLOSE non_existent_cursor;
EOF
echo ""

# 测试游标耗尽
echo "7️⃣  测试游标耗尽场景..."
echo "===================="
echo ""

echo "📌 声明小游标 (只有 3 行)..."
mysql -h $MYSQL_HOST -P $MYSQL_PORT -u root <<'EOF'
DECLARE small_cursor CURSOR FOR SELECT * FROM r2api LIMIT 3;
EOF
echo ""

echo "🔍 FETCH 3 行 (全部数据)..."
mysql -h $MYSQL_HOST -P $MYSQL_PORT -u root <<'EOF'
FETCH 3 FROM small_cursor;
EOF
echo ""

echo "🔍 再次 FETCH (应该返回 0 行)..."
mysql -h $MYSQL_HOST -P $MYSQL_PORT -u root <<'EOF'
FETCH 5 FROM small_cursor;
EOF
echo ""

echo "🗑️  关闭游标..."
mysql -h $MYSQL_HOST -P $MYSQL_PORT -u root <<'EOF'
CLOSE small_cursor;
EOF
echo ""

echo "=========================================="
echo "  ✅ 所有测试完成!"
echo "=========================================="
echo ""
echo "💡 提示:"
echo "   - 查看服务器日志可以看到详细的游标操作记录"
echo "   - 游标会在内存中保存完整结果集"
echo "   - 记得及时关闭不再使用的游标"
echo ""
