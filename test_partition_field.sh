#!/bin/bash

# 测试 _partition 虚拟字段过滤功能

MYSQL_PORT=3307
MYSQL_USER="root"
MYSQL_PASS=""
MYSQL_DB="calm"

echo "🧪 测试 _partition 虚拟字段过滤功能"
echo "=================================="

# 1. 测试精确匹配
echo ""
echo "1️⃣  测试 _partition = '具体分区名'"
mysql -h127.0.0.1 -P${MYSQL_PORT} -u${MYSQL_USER} -e "SELECT COUNT(*) as count FROM events WHERE _partition = '20240116_1'" ${MYSQL_DB}

# 2. 测试 LIKE 模式匹配
echo ""
echo "2️⃣  测试 _partition LIKE '20240116%'"
mysql -h127.0.0.1 -P${MYSQL_PORT} -u${MYSQL_USER} -e "SELECT COUNT(*) as count FROM events WHERE _partition LIKE '20240116%'" ${MYSQL_DB}

# 3. 测试 IN 列表匹配
echo ""
echo "3️⃣  测试 _partition IN ('20240116_1', '20240116_2')"
mysql -h127.0.0.1 -P${MYSQL_PORT} -u${MYSQL_USER} -e "SELECT COUNT(*) as count FROM events WHERE _partition IN ('20240116_1', '20240116_2')" ${MYSQL_DB}

# 4. 测试组合条件
echo ""
echo "4️⃣  测试 _partition LIKE '2024%' AND event_type = 'click'"
mysql -h127.0.0.1 -P${MYSQL_PORT} -u${MYSQL_USER} -e "SELECT COUNT(*) as count FROM events WHERE _partition LIKE '2024%' AND event_type = 'click'" ${MYSQL_DB}

# 5. 测试获取前10条数据
echo ""
echo "5️⃣  测试获取前10条数据（带 _partition 过滤）"
mysql -h127.0.0.1 -P${MYSQL_PORT} -u${MYSQL_USER} -e "SELECT event_id, event_type FROM events WHERE _partition LIKE '20240116%' LIMIT 10" ${MYSQL_DB}

echo ""
echo "✅ 测试完成"
