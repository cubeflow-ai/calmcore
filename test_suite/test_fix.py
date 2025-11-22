#!/usr/bin/env python3
"""测试 timestamp 类型修复"""

import requests
import json

# 测试 SQL (之前报错的查询)
sql = "SELECT * FROM taxi_trips WHERE pickup_datetime >= 1704067200000 AND pickup_datetime < 1704153600000 ORDER BY pickup_datetime DESC LIMIT 10"

print("=" * 60)
print("测试 SQL Normalizer 的 Timestamp 类型修复")
print("=" * 60)
print(f"\n原始 SQL:\n{sql}\n")

try:
    response = requests.post(
        "http://localhost:3307/query",
        headers={"Content-Type": "application/json"},
        json={"sql": sql},
        timeout=10,
    )

    print(f"响应状态码: {response.status_code}")
    print("-" * 60)

    if response.status_code == 200:
        print("✅ 查询成功!")
        print("\n返回数据 (前500字符):")
        print(response.text[:500])
    else:
        print("❌ 查询失败")
        print(f"\n错误信息:\n{response.text}")

except requests.exceptions.ConnectionError:
    print("❌ 连接失败! 服务未启动或端口错误")
    print("   请先启动服务: ./target/release/calm")
except Exception as e:
    print(f"❌ 请求异常: {e}")
