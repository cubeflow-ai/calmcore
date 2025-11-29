#!/usr/bin/env python3
"""
简单检查测试失败的原因
"""
import sys
import os

# 添加当前目录到路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from generate_test_cases import generate_all_test_cases

# 生成测试用例
tests = generate_all_test_cases("taxi_trips")

print("=" * 80)
print("检查测试用例")
print("=" * 80)

# 按类别分组
categories = {}
for category, query in tests:
    if category not in categories:
        categories[category] = []
    categories[category].append(query)

# 检查可能有问题的查询
problem_queries = []

for category, queries in categories.items():
    print(f"\n{category}: {len(queries)} 条")
    for i, query in enumerate(queries[:3], 1):  # 只显示前3条
        print(f"  {i}. {query[:70]}...")

        # 检查可能的问题
        query_lower = query.lower()

        # Calm 可能不支持的功能
        if "nullif" in query_lower:
            problem_queries.append((category, query, "NULLIF 函数可能不支持"))
        if "case when" in query_lower:
            problem_queries.append((category, query, "CASE WHEN 可能不完全支持"))
        if "union" in query_lower:
            problem_queries.append((category, query, "UNION 可能不支持"))
        if "like" in query_lower:
            problem_queries.append((category, query, "LIKE 可能不支持"))

print("\n" + "=" * 80)
print("可能有问题的查询:")
print("=" * 80)

if problem_queries:
    for category, query, reason in problem_queries:
        print(f"\n[{category}] {reason}")
        print(f"  SQL: {query[:100]}...")
else:
    print("未发现明显问题")

print(f"\n总计: {len(tests)} 条测试")
print(f"潜在问题: {len(problem_queries)} 条")
