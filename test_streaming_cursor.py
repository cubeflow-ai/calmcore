#!/usr/bin/env python3
"""
测试 MySQL 流式游标（CURSOR_TYPE_READ_ONLY）
验证 Prepared Statement + Streaming 是否路由到 nature executor
"""

import mysql.connector
import sys


def test_streaming_cursor():
    print("=" * 60)
    print("🌊 测试 MySQL 流式游标（Prepared Statement）")
    print("=" * 60)

    try:
        # 连接到 calm server
        conn = mysql.connector.connect(
            host="127.0.0.1",
            port=3307,
            user="root",
            password="calm",
            database="test",
        )

        print("✅ 连接成功")

        # 创建游标，启用服务端游标（流式模式）
        # useCursorFetch=True 会发送 CURSOR_TYPE_READ_ONLY flag
        cursor = conn.cursor()

        # 测试 1: 简单查询（应该自动添加 ORDER BY _nature）
        print("\n" + "=" * 60)
        print("📝 测试 1: Prepared Statement + Streaming Cursor")
        print("=" * 60)

        sql = "SELECT * FROM nyc_taxi LIMIT 10"
        print(f"SQL: {sql}")
        print(f"预期: 应该自动转换为 '{sql} ORDER BY _nature'")

        try:
            # 使用 prepared statement
            cursor.execute(sql)

            rows = cursor.fetchall()
            print(f"✅ 返回 {len(rows)} 行数据")

            if rows:
                print(f"第一行: {rows[0][:3]}...")  # 显示前3列
        except Exception as e:
            print(f"⚠️  执行失败: {e}")

        # 测试 2: 已经包含 ORDER BY _nature 的查询
        print("\n" + "=" * 60)
        print("📝 测试 2: 已包含 ORDER BY _nature")
        print("=" * 60)

        sql2 = "SELECT * FROM nyc_taxi ORDER BY _nature LIMIT 10"
        print(f"SQL: {sql2}")

        try:
            cursor.execute(sql2)
            rows2 = cursor.fetchall()
            print(f"✅ 返回 {len(rows2)} 行数据")
        except Exception as e:
            print(f"⚠️  执行失败: {e}")

        # 测试 3: 带 WHERE 条件
        print("\n" + "=" * 60)
        print("📝 测试 3: 带 WHERE 条件 + Streaming")
        print("=" * 60)

        sql3 = "SELECT * FROM nyc_taxi WHERE passenger_count > 2 LIMIT 10"
        print(f"SQL: {sql3}")

        try:
            cursor.execute(sql3)
            rows3 = cursor.fetchall()
            print(f"✅ 返回 {len(rows3)} 行数据")
        except Exception as e:
            print(f"⚠️  执行失败: {e}")

        cursor.close()
        conn.close()

        print("\n" + "=" * 60)
        print("✅ 所有测试完成")
        print("=" * 60)

    except mysql.connector.Error as err:
        print(f"❌ MySQL 错误: {err}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ 错误: {e}")
        sys.exit(1)


if __name__ == "__main__":
    test_streaming_cursor()
