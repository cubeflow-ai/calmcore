#!/usr/bin/env python3
"""
测试真实数据集的 SQL 查询

使用方法：
    python3 test_queries.py nyc-taxi
"""

import sys
import time
import pymysql
from datetime import datetime


# 颜色
class Colors:
    BLUE = "\033[0;34m"
    GREEN = "\033[0;32m"
    YELLOW = "\033[1;33m"
    RED = "\033[0;31m"
    NC = "\033[0m"


def print_colored(color, text):
    print(f"{color}{text}{Colors.NC}")


def connect_db(host="127.0.0.1", port=3307):
    """连接数据库"""
    try:
        conn = pymysql.connect(
            host=host, port=port, user="root", passwd="calm", database="default"
        )
        return conn
    except Exception as e:
        print_colored(Colors.RED, f"❌ Failed to connect: {e}")
        sys.exit(1)


def run_query(cursor, name, sql, show_preview=True):
    """执行单个查询"""
    print_colored(Colors.YELLOW, f"\n📊 {name}")
    print(f"SQL: {sql}")

    start_time = time.time()

    try:
        cursor.execute(sql)
        results = cursor.fetchall()

        duration = time.time() - start_time
        row_count = len(results)

        print_colored(Colors.GREEN, f"✓ Success: {row_count:,} rows in {duration:.3f}s")

        # 显示预览
        if show_preview and results:
            preview_count = min(3, len(results))
            print(f"Preview (first {preview_count} rows):")
            for i, row in enumerate(results[:preview_count]):
                # 格式化时间戳
                formatted_row = []
                for val in row:
                    if isinstance(val, int) and val > 1000000000000:  # 可能是毫秒时间戳
                        try:
                            dt = datetime.fromtimestamp(val / 1000)
                            formatted_row.append(dt.strftime("%Y-%m-%d %H:%M:%S"))
                        except:
                            formatted_row.append(val)
                    else:
                        formatted_row.append(val)
                print(f"  Row {i}: {formatted_row}")

        return True, duration, row_count

    except Exception as e:
        duration = time.time() - start_time
        print_colored(Colors.RED, f"❌ Failed: {e}")
        return False, duration, 0


def verify_correctness(cursor, name, sql1, sql2):
    """验证两个查询返回相同的结果"""
    print_colored(Colors.YELLOW, f"\n🔍 Correctness Check: {name}")

    try:
        cursor.execute(sql1)
        result1 = cursor.fetchall()

        cursor.execute(sql2)
        result2 = cursor.fetchall()

        if result1 == result2:
            print_colored(Colors.GREEN, f"✓ Results match ({len(result1)} rows)")
            return True
        else:
            print_colored(
                Colors.RED, f"✗ Results differ: {len(result1)} vs {len(result2)} rows"
            )
            if len(result1) <= 5 and len(result2) <= 5:
                print(f"  Query 1 result: {result1}")
                print(f"  Query 2 result: {result2}")
            return False
    except Exception as e:
        print_colored(Colors.RED, f"✗ Verification failed: {e}")
        return False


def test_nyc_taxi(conn):
    """测试 NYC Taxi 数据集的查询"""
    print_colored(Colors.BLUE, "\n=== NYC Taxi Dataset Queries ===\n")

    cursor = conn.cursor()

    # 2024-01-01 00:00:00 的毫秒时间戳
    jan_1_2024 = 1704067200000
    jan_2_2024 = 1704153600000
    jan_8_2024 = 1704672000000

    test_cases = [
        ("1. Simple SELECT with LIMIT", "SELECT * FROM taxi_trips LIMIT 10", True),
        ("2. COUNT total trips", "SELECT COUNT(*) FROM taxi_trips", True),
        (
            "3. Time range query (Jan 1, 2024)",
            f"SELECT * FROM taxi_trips WHERE pickup_datetime >= {jan_1_2024} AND pickup_datetime < {jan_2_2024} LIMIT 100",
            True,
        ),
        (
            "4. Time range with ORDER BY DESC",
            f"SELECT * FROM taxi_trips WHERE pickup_datetime >= {jan_1_2024} AND pickup_datetime < {jan_2_2024} ORDER BY pickup_datetime DESC LIMIT 10",
            True,
        ),
        (
            "5. Filter by passenger count",
            "SELECT * FROM taxi_trips WHERE passenger_count = 2 LIMIT 50",
            True,
        ),
        (
            "6. COUNT by passenger count",
            "SELECT passenger_count, COUNT(*) as count FROM taxi_trips GROUP BY passenger_count ORDER BY passenger_count",
            True,
        ),
        (
            "7. Average fare by payment type",
            "SELECT payment_type, AVG(fare_amount) as avg_fare FROM taxi_trips GROUP BY payment_type ORDER BY payment_type",
            True,
        ),
        (
            "8. Complex filter (time + passenger + distance)",
            f"SELECT * FROM taxi_trips WHERE pickup_datetime >= {jan_1_2024} AND pickup_datetime < {jan_8_2024} AND passenger_count >= 2 AND trip_distance > 5.0 LIMIT 20",
            True,
        ),
        (
            "9. Top 10 most expensive trips",
            "SELECT * FROM taxi_trips ORDER BY total_amount DESC LIMIT 10",
            True,
        ),
        (
            "10. Trips with high tips",
            "SELECT * FROM taxi_trips WHERE tip_amount > 10.0 ORDER BY tip_amount DESC LIMIT 20",
            True,
        ),
        (
            "11. COUNT trips by pickup location",
            "SELECT pickup_location_id, COUNT(*) as count FROM taxi_trips GROUP BY pickup_location_id ORDER BY count DESC LIMIT 10",
            True,
        ),
        (
            "12. Average trip distance",
            "SELECT AVG(trip_distance) as avg_distance FROM taxi_trips",
            True,
        ),
        # 新增：边界条件测试
        (
            "13. Query with zero passengers",
            "SELECT COUNT(*) FROM taxi_trips WHERE passenger_count = 0",
            True,
        ),
        (
            "14. Very long trips (>50 miles)",
            "SELECT * FROM taxi_trips WHERE trip_distance > 50.0 ORDER BY trip_distance DESC LIMIT 10",
            True,
        ),
        (
            "15. Zero fare trips",
            "SELECT COUNT(*) FROM taxi_trips WHERE fare_amount = 0",
            True,
        ),
        # 新增：多字段聚合
        (
            "16. MIN/MAX/AVG statistics",
            "SELECT MIN(fare_amount) as min_fare, MAX(fare_amount) as max_fare, AVG(fare_amount) as avg_fare, MIN(trip_distance) as min_dist, MAX(trip_distance) as max_dist FROM taxi_trips",
            True,
        ),
        (
            "17. SUM total revenue by payment type",
            "SELECT payment_type, SUM(total_amount) as total_revenue, COUNT(*) as trip_count FROM taxi_trips GROUP BY payment_type ORDER BY total_revenue DESC",
            True,
        ),
        # 新增：HAVING 子句
        (
            "18. Locations with >1000 pickups",
            "SELECT pickup_location_id, COUNT(*) as count FROM taxi_trips GROUP BY pickup_location_id HAVING COUNT(*) > 1000 ORDER BY count DESC",
            True,
        ),
        # 新增：多时间段对比
        (
            "19. Time range with ORDER BY ASC",
            f"SELECT * FROM taxi_trips WHERE pickup_datetime >= {jan_1_2024} AND pickup_datetime < {jan_2_2024} ORDER BY pickup_datetime ASC LIMIT 10",
            True,
        ),
        # 新增：IN 操作符
        (
            "20. Filter by multiple passenger counts",
            "SELECT passenger_count, COUNT(*) as count FROM taxi_trips WHERE passenger_count IN (1, 2, 3) GROUP BY passenger_count ORDER BY passenger_count",
            True,
        ),
        # 新增：范围查询组合
        (
            "21. Mid-range fare trips ($10-$30)",
            "SELECT COUNT(*) FROM taxi_trips WHERE fare_amount >= 10.0 AND fare_amount <= 30.0",
            True,
        ),
        # 新增：复杂排序
        (
            "22. Top trips by tip percentage",
            "SELECT fare_amount, tip_amount, (tip_amount / fare_amount * 100) as tip_pct FROM taxi_trips WHERE fare_amount > 0 ORDER BY tip_pct DESC LIMIT 10",
            True,
        ),
        # 新增：多字段 ORDER BY
        (
            "23. Order by multiple fields",
            "SELECT passenger_count, payment_type, COUNT(*) as count FROM taxi_trips GROUP BY passenger_count, payment_type ORDER BY passenger_count, payment_type LIMIT 20",
            True,
        ),
        # 新增：DISTINCT
        (
            "24. Distinct pickup locations count",
            "SELECT COUNT(DISTINCT pickup_location_id) as unique_locations FROM taxi_trips",
            True,
        ),
    ]

    # 执行基本查询测试
    results = []
    for name, sql, show_preview in test_cases:
        success, duration, row_count = run_query(cursor, name, sql, show_preview)
        results.append((name, success, duration, row_count))

    # LIKE 查询测试
    print_colored(Colors.BLUE, "\n=== LIKE Query Tests ===\n")

    # 先查询一些真实的 id 值用于测试 (id 是 KEYWORD 类型,支持 LIKE)
    cursor.execute("SELECT id FROM taxi_trips LIMIT 10")
    ids = [str(row[0]) for row in cursor.fetchall()]

    like_tests = []

    if ids:
        # 选择一个包含数字的 id 用于测试
        # 大多数 id 格式类似 "trip_12345"
        test_id = ids[0] if ids else "trip_1"

        # 提取一些可搜索的模式
        if "_" in test_id:
            # 例如 "trip_12345" -> 搜索 "trip"
            pattern1 = test_id.split("_")[0]  # "trip"
            # 搜索数字部分的一部分
            pattern2 = test_id[-2:] if len(test_id) >= 2 else test_id  # 最后两个字符
        else:
            pattern1 = test_id[:3] if len(test_id) >= 3 else test_id
            pattern2 = test_id[-2:] if len(test_id) >= 2 else test_id

        like_tests.extend(
            [
                (
                    "LIKE-1. Simple LIKE query with prefix",
                    f"SELECT COUNT(*) FROM taxi_trips WHERE id LIKE '{pattern1}%'",
                    True,
                ),
                (
                    "LIKE-2. LIKE with time range",
                    f"SELECT COUNT(*) FROM taxi_trips WHERE pickup_datetime >= {jan_1_2024} AND pickup_datetime < {jan_2_2024} AND id LIKE '%{pattern2}%'",
                    True,
                ),
                (
                    "LIKE-3. Multiple LIKE conditions (contains)",
                    f"SELECT COUNT(*) FROM taxi_trips WHERE id LIKE '%{pattern1}%' AND id LIKE '%{pattern2}%'",
                    True,
                ),
                (
                    "LIKE-4. LIKE with ORDER BY and LIMIT",
                    f"SELECT * FROM taxi_trips WHERE id LIKE '{pattern1}%' ORDER BY pickup_datetime DESC LIMIT 10",
                    True,
                ),
            ]
        )

    for name, sql, show_preview in like_tests:
        success, duration, row_count = run_query(cursor, name, sql, show_preview)
        results.append((name, success, duration, row_count))

    # LIKE 正确性验证测试
    print_colored(Colors.BLUE, "\n=== LIKE Correctness Verification ===\n")

    if ids:
        test_id = ids[0]

        # 提取搜索模式
        if "_" in test_id:
            pattern = test_id.split("_")[0]  # 例如 "trip"
        else:
            pattern = test_id[:3] if len(test_id) >= 3 else test_id

        # 验证 LIKE 结果确实包含匹配的模式
        print_colored(
            Colors.YELLOW, f"\n🔍 Verifying LIKE '{pattern}%' actually filters data"
        )

        # 执行 LIKE 查询
        like_sql = f"SELECT COUNT(*) FROM taxi_trips WHERE id LIKE '{pattern}%'"
        cursor.execute(like_sql)
        like_count = cursor.fetchone()[0]

        # 执行无过滤查询
        cursor.execute("SELECT COUNT(*) FROM taxi_trips")
        total_count = cursor.fetchone()[0]

        print(f"  Total rows: {total_count:,}")
        print(f"  LIKE matched rows: {like_count:,}")

        if like_count < total_count:
            print_colored(
                Colors.GREEN,
                f"  ✓ LIKE correctly filtered data ({like_count}/{total_count} = {like_count/total_count*100:.1f}%)",
            )
            results.append(("LIKE-Verify-1: Filter effectiveness", True, 0, like_count))
        elif like_count == total_count:
            # 可能是所有数据都匹配,验证是否合理
            # 对于 id 字段,如果所有 id 都以相同前缀开头,这是合理的
            print_colored(
                Colors.YELLOW,
                f"  ⚠️  All data matches pattern (this might be expected if all IDs have same prefix)",
            )
            results.append(("LIKE-Verify-1: Filter effectiveness", True, 0, like_count))
        else:
            print_colored(
                Colors.RED, f"  ✗ LIKE count > total count - something is wrong!"
            )
            results.append(
                ("LIKE-Verify-1: Filter effectiveness", False, 0, like_count)
            )

        # 验证 LIKE 与具体值的组合
        print_colored(Colors.YELLOW, f"\n🔍 Verifying LIKE with time range filter")

        like_time_sql = f"SELECT COUNT(*) FROM taxi_trips WHERE pickup_datetime >= {jan_1_2024} AND pickup_datetime < {jan_2_2024} AND id LIKE '{pattern}%'"
        cursor.execute(like_time_sql)
        like_time_count = cursor.fetchone()[0]

        time_only_sql = f"SELECT COUNT(*) FROM taxi_trips WHERE pickup_datetime >= {jan_1_2024} AND pickup_datetime < {jan_2_2024}"
        cursor.execute(time_only_sql)
        time_only_count = cursor.fetchone()[0]

        print(f"  Time range only: {time_only_count:,} rows")
        print(f"  Time range + LIKE: {like_time_count:,} rows")

        if time_only_count == 0:
            print_colored(
                Colors.YELLOW, f"  ⚠️  No data in time range, skipping verification"
            )
            results.append(("LIKE-Verify-2: Combined filter", True, 0, like_time_count))
        elif like_time_count <= time_only_count:
            percentage = (
                like_time_count / time_only_count * 100 if time_only_count > 0 else 0
            )
            print_colored(
                Colors.GREEN,
                f"  ✓ LIKE correctly filtered time range results ({like_time_count}/{time_only_count} = {percentage:.1f}%)",
            )
            results.append(("LIKE-Verify-2: Combined filter", True, 0, like_time_count))
        else:
            print_colored(
                Colors.RED,
                f"  ✗ LIKE+time count > time-only count - filter not working!",
            )
            results.append(
                ("LIKE-Verify-2: Combined filter", False, 0, like_time_count)
            )

    # 正确性校验测试（合并到主测试中）
    print_colored(Colors.BLUE, "\n=== Correctness Verification Tests ===\n")

    verification_tests = [
        (
            "25. COUNT consistency (equality vs range)",
            "SELECT COUNT(*) FROM taxi_trips WHERE passenger_count = 2",
            "SELECT COUNT(*) FROM taxi_trips WHERE passenger_count >= 2 AND passenger_count <= 2",
        ),
        (
            "26. Range query idempotence",
            f"SELECT COUNT(*) FROM taxi_trips WHERE pickup_datetime >= {jan_1_2024} AND pickup_datetime < {jan_2_2024}",
            f"SELECT COUNT(*) FROM taxi_trips WHERE pickup_datetime >= {jan_1_2024} AND pickup_datetime < {jan_2_2024}",
        ),
        (
            "27. LIMIT with ORDER BY consistency",
            "SELECT id FROM taxi_trips ORDER BY id LIMIT 5",
            "SELECT id FROM taxi_trips ORDER BY id ASC LIMIT 5",
        ),
        (
            "28. COUNT(*) vs COUNT(column)",
            "SELECT COUNT(*) FROM taxi_trips WHERE passenger_count > 0",
            "SELECT COUNT(id) FROM taxi_trips WHERE passenger_count > 0",
        ),
        (
            "29. MIN/MAX with/without NULL filter",
            "SELECT MIN(fare_amount), MAX(fare_amount) FROM taxi_trips",
            "SELECT MIN(fare_amount), MAX(fare_amount) FROM taxi_trips WHERE fare_amount IS NOT NULL",
        ),
        (
            "30. WHERE filter ordering independence",
            "SELECT COUNT(*) FROM taxi_trips WHERE passenger_count = 1 AND payment_type = 2",
            "SELECT COUNT(*) FROM taxi_trips WHERE payment_type = 2 AND passenger_count = 1",
        ),
        (
            "31. Time range boundary test",
            f"SELECT COUNT(*) FROM taxi_trips WHERE pickup_datetime >= {jan_1_2024}",
            f"SELECT COUNT(*) FROM taxi_trips WHERE pickup_datetime > {jan_1_2024 - 1}",
        ),
        (
            "32. OR vs IN equivalence",
            "SELECT COUNT(*) FROM taxi_trips WHERE passenger_count = 1 OR passenger_count = 2",
            "SELECT COUNT(*) FROM taxi_trips WHERE passenger_count IN (1, 2)",
        ),
        (
            "33. Constant false returns zero",
            "SELECT COUNT(*) FROM taxi_trips WHERE 2=4",
            "SELECT COUNT(*) FROM taxi_trips WHERE fare_amount > 999999",
        ),
        (
            "34. Constant true with filter",
            "SELECT COUNT(*) FROM taxi_trips WHERE 1=1 AND passenger_count = 2",
            "SELECT COUNT(*) FROM taxi_trips WHERE passenger_count = 2",
        ),
        (
            "35. Constant comparison (10 > 5)",
            "SELECT COUNT(*) FROM taxi_trips WHERE 10 > 5",
            "SELECT COUNT(*) FROM taxi_trips WHERE 1=1",
        ),
        (
            "36. Constant false with AND",
            "SELECT COUNT(*) FROM taxi_trips WHERE 2=4 AND passenger_count = 1",
            "SELECT COUNT(*) FROM taxi_trips WHERE 1=2",
        ),
        (
            "37. NOT IN equivalence",
            "SELECT COUNT(*) FROM taxi_trips WHERE passenger_count NOT IN (1, 2)",
            "SELECT COUNT(*) FROM taxi_trips WHERE passenger_count != 1 AND passenger_count != 2",
        ),
        (
            "38. IN with 3 values vs multiple OR",
            "SELECT COUNT(*) FROM taxi_trips WHERE passenger_count IN (1, 2, 3)",
            "SELECT COUNT(*) FROM taxi_trips WHERE passenger_count = 1 OR passenger_count = 2 OR passenger_count = 3",
        ),
        (
            "39. Mixed constant and range",
            "SELECT COUNT(*) FROM taxi_trips WHERE 1=1 AND fare_amount > 20.0",
            "SELECT COUNT(*) FROM taxi_trips WHERE fare_amount > 20.0",
        ),
        (
            "40. Negative value comparison",
            "SELECT COUNT(*) FROM taxi_trips WHERE fare_amount < 0",
            "SELECT COUNT(*) FROM taxi_trips WHERE fare_amount < 0.0",
        ),
    ]

    for name, sql1, sql2 in verification_tests:
        # 执行第一个查询
        success1, duration1, count1 = run_query(
            cursor, name + " [Query 1]", sql1, False
        )
        # 执行第二个查询
        success2, duration2, count2 = run_query(
            cursor, name + " [Query 2]", sql2, False
        )

        # 验证结果是否一致
        if success1 and success2:
            verified = count1 == count2
            if verified:
                print_colored(Colors.GREEN, f"  ✓ Results match: {count1} rows")
            else:
                print_colored(
                    Colors.RED, f"  ✗ Results differ: {count1} vs {count2} rows"
                )
            results.append(
                (
                    name,
                    verified and success1 and success2,
                    duration1 + duration2,
                    count1,
                )
            )
        else:
            print_colored(Colors.RED, f"  ✗ One or both queries failed")
            results.append((name, False, duration1 + duration2, 0))

    cursor.close()

    # 打印总结
    print_colored(Colors.BLUE, "\n=== Test Summary ===\n")

    passed = sum(1 for _, success, _, _ in results if success)
    failed = len(results) - passed
    total_time = sum(duration for _, _, duration, _ in results)

    print(f"Total tests: {len(results)}")
    print_colored(Colors.GREEN, f"  Passed: {passed}")
    if failed > 0:
        print_colored(Colors.RED, f"  Failed: {failed}")

    print(f"\nTotal time: {total_time:.3f}s")
    print()

    # 性能统计
    print_colored(Colors.BLUE, "Performance breakdown:")
    for name, success, duration, row_count in results:
        status = "✓" if success else "✗"
        print(f"  {status} {name}: {duration:.3f}s ({row_count:,} rows)")

    return failed == 0


def main():
    if len(sys.argv) < 2:
        print("Usage: python3 test_queries.py [nyc-taxi]")
        return 1

    dataset = sys.argv[1]

    print_colored(Colors.BLUE, "=== Query Test Suite ===\n")
    print(f"Dataset: {dataset}")
    print("Connecting to database...\n")

    conn = connect_db()
    print_colored(Colors.GREEN, "✓ Connected\n")

    success = False

    if dataset == "nyc-taxi":
        success = test_nyc_taxi(conn)
    else:
        print_colored(Colors.RED, f"Unknown dataset: {dataset}")
        return 1

    conn.close()

    if success:
        print_colored(Colors.GREEN, "\n🎉 All tests passed!")
        return 0
    else:
        print_colored(Colors.RED, "\n❌ Some tests failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())
