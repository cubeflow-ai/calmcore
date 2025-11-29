#!/usr/bin/env python3
"""
测试 COUNT 查询并分析每个 segment 的 bitmap 大小
"""

import pymysql
import sys


def test_count_query():
    """执行 COUNT 查询"""
    try:
        conn = pymysql.connect(
            host="127.0.0.1",
            port=3306,
            user="root",
            password="",
            database="test",
            charset="utf8mb4",
        )

        cursor = conn.cursor()

        # 执行查询
        print("🔍 执行查询: SELECT COUNT(*) FROM taxi_trips WHERE passenger_count > 2")
        cursor.execute("SELECT COUNT(*) FROM taxi_trips WHERE passenger_count > 2")
        result = cursor.fetchone()

        print(f"\n✅ 查询结果: {result[0]}")
        print(f"期望结果: 199,155")
        print(f"差异: {199155 - result[0]} 行")

        cursor.close()
        conn.close()

        return result[0]

    except Exception as e:
        print(f"❌ 错误: {e}")
        import traceback

        traceback.print_exc()
        return None


if __name__ == "__main__":
    count = test_count_query()
    if count is None:
        sys.exit(1)

    if count != 199155:
        print(f"\n❌ 测试失败! 返回 {count},期望 199155")
        sys.exit(1)
    else:
        print(f"\n✅ 测试通过!")
        sys.exit(0)
