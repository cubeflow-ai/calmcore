#!/bin/bash

# Elasticsearch搜索功能测试脚本

echo "🧪 测试Elasticsearch搜索功能"
echo "================================"

# 设置基础URL
BASE_URL="http://localhost:9200"
INDEX="test_index"

# 函数：检查服务是否运行
check_service() {
    echo "🔍 检查Elasticsearch服务状态..."
    if curl -s "$BASE_URL/_cluster/health" > /dev/null; then
        echo "✅ 服务运行正常"
        return 0
    else
        echo "❌ 服务未运行，请先启动服务"
        return 1
    fi
}

# 函数：创建测试索引
create_index() {
    echo "📋 创建测试索引..."
    curl -s -X PUT "$BASE_URL/$INDEX" \
        -H "Content-Type: application/json" \
        -d '{
            "mappings": {
                "properties": {
                    "name": {"type": "keyword"},
                    "age": {"type": "integer"},
                    "city": {"type": "keyword"}
                }
            }
        }' | jq .
}

# 函数：插入测试文档
insert_documents() {
    echo "📝 插入测试文档..."
    
    # 文档1
    curl -s -X POST "$BASE_URL/$INDEX/_doc/1" \
        -H "Content-Type: application/json" \
        -d '{
            "name": "张三",
            "age": 25,
            "city": "北京"
        }' | jq .
    
    # 文档2
    curl -s -X POST "$BASE_URL/$INDEX/_doc/2" \
        -H "Content-Type: application/json" \
        -d '{
            "name": "李四",
            "age": 30,
            "city": "上海"
        }' | jq .
    
    # 文档3
    curl -s -X POST "$BASE_URL/$INDEX/_doc/3" \
        -H "Content-Type: application/json" \
        -d '{
            "name": "王五",
            "age": 25,
            "city": "广州"
        }' | jq .
}

# 函数：测试搜索功能
test_search() {
    echo "🔍 测试搜索功能..."
    
    echo "1️⃣ 搜索所有文档 (match_all):"
    curl -s -X GET "$BASE_URL/$INDEX/_search" \
        -H "Content-Type: application/json" \
        -d '{
            "query": {
                "match_all": {}
            }
        }' | jq .
    
    echo ""
    echo "2️⃣ 按城市搜索 (term查询):"
    curl -s -X GET "$BASE_URL/$INDEX/_search" \
        -H "Content-Type: application/json" \
        -d '{
            "query": {
                "term": {
                    "city": "北京"
                }
            }
        }' | jq .
    
    echo ""
    echo "3️⃣ 按年龄搜索 (match查询):"
    curl -s -X GET "$BASE_URL/$INDEX/_search" \
        -H "Content-Type: application/json" \
        -d '{
            "query": {
                "match": {
                    "age": 25
                }
            }
        }' | jq .
    
    echo ""
    echo "4️⃣ 分页测试 (from=0, size=2):"
    curl -s -X GET "$BASE_URL/$INDEX/_search" \
        -H "Content-Type: application/json" \
        -d '{
            "query": {
                "match_all": {}
            },
            "from": 0,
            "size": 2
        }' | jq .
}

# 函数：清理测试数据
cleanup() {
    echo "🧹 清理测试数据..."
    curl -s -X DELETE "$BASE_URL/$INDEX" | jq .
}

# 主流程
main() {
    if ! check_service; then
        echo "请先在另一个终端运行: cargo run --example elasticsearch_server"
        exit 1
    fi
    
    echo ""
    create_index
    echo ""
    insert_documents
    echo ""
    sleep 2  # 等待数据写入
    test_search
    echo ""
    cleanup
    
    echo "✅ 测试完成！"
}

# 运行主流程
main