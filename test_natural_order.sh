#!/bin/bash

# 测试 ORDER BY _nature 的自然顺序查询

echo "🌿 测试 ORDER BY _nature (Natural Order Query)"
echo "==========================================="

# 1. 查询前 10 行 (OFFSET=0, LIMIT=10)
echo ""
echo "1️⃣  测试: SELECT * FROM r2api ORDER BY _nature LIMIT 10"
curl -s -X POST http://127.0.0.1:3000/api/mysql/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "SELECT * FROM r2api ORDER BY _nature LIMIT 10"
  }' | jq '.matched_docs, .execution_time_ms, .batch.num_rows'

# 2. 跳过前 100 行，读取 20 行 (OFFSET=100, LIMIT=20)
echo ""
echo "2️⃣  测试: SELECT * FROM r2api ORDER BY _nature LIMIT 100, 20"
curl -s -X POST http://127.0.0.1:3000/api/mysql/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "SELECT * FROM r2api ORDER BY _nature LIMIT 100, 20"
  }' | jq '.matched_docs, .execution_time_ms, .batch.num_rows'

# 3. 深度分页测试: OFFSET=10000, LIMIT=50
echo ""
echo "3️⃣  测试: SELECT * FROM r2api ORDER BY _nature LIMIT 10000, 50 (深度分页)"
curl -s -X POST http://127.0.0.1:3000/api/mysql/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "SELECT * FROM r2api ORDER BY _nature LIMIT 10000, 50"
  }' | jq '.matched_docs, .execution_time_ms, .batch.num_rows'

# 4. 带 WHERE 条件的自然顺序查询
echo ""
echo "4️⃣  测试: SELECT * FROM r2api WHERE region='us-west' ORDER BY _nature LIMIT 10"
curl -s -X POST http://127.0.0.1:3000/api/mysql/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "SELECT * FROM r2api WHERE region='\''us-west'\'' ORDER BY _nature LIMIT 10"
  }' | jq '.matched_docs, .execution_time_ms, .batch.num_rows'

echo ""
echo "✅ 测试完成！"
echo ""
echo "🔍 查看日志中的 [NaturalOrder] 标记来确认执行路径"
