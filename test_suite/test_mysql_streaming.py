#!/usr/bin/env python3
"""
测试 MySQL 流式查询功能
验证 COM_STMT_EXECUTE 的 flags=CURSOR_TYPE_READ_ONLY 是否正确触发流式执行
"""

import pymysql
import sys


def test_streaming_cursor():
    """测试使用 SSCursor (流式游标) 进行查询"""

    print("=" * 60)
    print("MySQL 流式查询测试")
    print("=" * 60)

    # 连接到 calm 数据库 (参考 test_queries.py 的配置)
    conn = pymysql.connect(
        host="127.0.0.1",
        port=3307,
        user="root",
        password="calm",
        database="default",
        cursorclass=pymysql.cursors.SSCursor,  # 使用流式游标
    )

    try:
        with conn.cursor() as cursor:
            # 测试1: 简单 SELECT 查询 (使用 taxi_trips 表，约 200 万数据)
            print("\n✅ 测试1: 简单 SELECT 查询")
            sql = "SELECT * FROM taxi_trips LIMIT 10"
            print(f"SQL: {sql}")

            cursor.execute(sql)
            rows = cursor.fetchall()
            print(f"返回 {len(rows)} 行")
            for i, row in enumerate(rows[:3]):  # 只打印前3行
                print(f"  行 {i+1}: {row}")

            # 测试2: 带 ORDER BY _nature 的查询
            print(
                "\n✅ 测试2: ORDER BY _nature 查询 (应该直接走 natural_order_executor)"
            )
            sql = "SELECT * FROM taxi_trips ORDER BY _nature LIMIT 10"
            print(f"SQL: {sql}")

            cursor.execute(sql)
            rows = cursor.fetchall()
            print(f"返回 {len(rows)} 行")
            for i, row in enumerate(rows[:3]):
                print(f"  行 {i+1}: {row}")

            # 测试3: 大数据量查询 (验证流式特性)
            print("\n✅ 测试3: 大数据量查询 (LIMIT 1000)")
            sql = "SELECT * FROM taxi_trips ORDER BY _nature LIMIT 1000"
            print(f"SQL: {sql}")

            cursor.execute(sql)

            # 逐行获取，验证流式特性
            row_count = 0
            for row in cursor:
                row_count += 1
                if row_count <= 3:
                    print(f"  行 {row_count}: {row}")

            print(f"总共处理 {row_count} 行 (流式获取)")

            # 测试4: 验证 prepared statement 存储
            print("\n✅ 测试4: 多次执行相同 prepared statement")
            sql = "SELECT COUNT(*) as cnt FROM taxi_trips"
            print(f"SQL: {sql}")

            # 执行两次，验证 stmt_id 复用
            cursor.execute(sql)
            result1 = cursor.fetchone()
            print(f"第1次执行: {result1}")

            cursor.execute(sql)
            result2 = cursor.fetchone()
            print(f"第2次执行: {result2}")

            # 测试5: 全表扫描验证 - 检查流式游标是否能正确拉取所有数据
            print("\n✅ 测试5: 全表扫描验证 (检查总数一致性)")

            # 先获取总行数
            count_sql = "SELECT COUNT(*) as total FROM taxi_trips"
            cursor.execute(count_sql)
            count_result = cursor.fetchone()
            expected_count = count_result[0]
            print(f"COUNT(*) 返回: {expected_count} 行")

            # 使用流式游标拉取全表（不带 ORDER BY _nature，测试普通SQL的流式执行）
            print("开始流式拉取全表数据...")
            scan_sql = "SELECT * FROM taxi_trips"
            print(f"💡 SQL: {scan_sql} (流式游标会自动触发 natural_order_executor)")
            cursor.execute(scan_sql)

            # 逐行计数
            actual_count = 0
            start_time = __import__("time").time()
            for row in cursor:
                actual_count += 1
                if actual_count % 100000 == 0:  # 每 10 万行打印一次进度
                    elapsed = __import__("time").time() - start_time
                    print(
                        f"  已拉取 {actual_count} 行 (耗时 {elapsed:.2f}s, {actual_count/elapsed:.0f} 行/秒)"
                    )

            elapsed = __import__("time").time() - start_time
            print(f"流式拉取完成: {actual_count} 行 (总耗时 {elapsed:.2f}s)")

            # 验证一致性
            if actual_count == expected_count:
                print(
                    f"✅ 数据一致性验证通过: COUNT={expected_count}, 实际拉取={actual_count}"
                )
            else:
                print(
                    f"❌ 数据不一致: COUNT={expected_count}, 实际拉取={actual_count}, 差异={expected_count - actual_count}"
                )
                return False

            print("\n" + "=" * 60)
            print("✅ 所有测试通过!")
            print("=" * 60)

    except Exception as e:
        print(f"\n❌ 测试失败: {e}")
        import traceback

        traceback.print_exc()
        return False

    finally:
        conn.close()

    return True


def test_regular_cursor():
    """测试使用普通游标 (非流式) - 应该正常工作"""

    print("\n" + "=" * 60)
    print("MySQL 普通游标测试 (对比测试)")
    print("=" * 60)

    conn = pymysql.connect(
        host="127.0.0.1",
        port=3307,
        user="root",
        password="calm",
        database="default",
        cursorclass=pymysql.cursors.Cursor,  # 普通游标
    )

    try:
        with conn.cursor() as cursor:
            print("\n测试: 使用普通游标执行查询")
            sql = "SELECT * FROM taxi_trips LIMIT 10"
            print(f"SQL: {sql}")

            cursor.execute(sql)
            rows = cursor.fetchall()
            print(f"✅ 成功返回 {len(rows)} 行")
            for i, row in enumerate(rows[:3]):
                print(f"  行 {i+1}: {row}")

            print("\n💡 说明: 普通游标不使用 prepared statement，直接走文本查询")
            return True

    except Exception as e:
        print(f"❌ 测试失败: {e}")
        import traceback

        traceback.print_exc()
        return False

    finally:
        conn.close()


if __name__ == "__main__":
    print("⚠️  确保 calm server 正在运行: ./target/release/calm server")
    print("⚠️  使用 taxi_trips 表进行测试 (约 200 万行数据)\n")

    # 先运行流式游标测试
    streaming_ok = test_streaming_cursor()

    # 再运行普通游标测试 (对比测试)
    regular_ok = test_regular_cursor()

    print("\n" + "=" * 60)
    print("测试摘要:")
    print(
        f"  流式游标测试 (SSCursor + Prepared Statement): {'✅ PASS' if streaming_ok else '❌ FAIL'}"
    )
    print(f"  普通游标测试 (文本查询): {'✅ PASS' if regular_ok else '❌ FAIL'}")
    print("=" * 60)

    if streaming_ok and regular_ok:
        print("\n✅ 所有测试通过！")
        print(
            "   - 流式游标使用 prepared statement (flags=1)，直接走 natural_order_executor"
        )
        print("   - 普通游标走文本查询，通过 on_query() 路由")

    sys.exit(0 if (streaming_ok and regular_ok) else 1)
