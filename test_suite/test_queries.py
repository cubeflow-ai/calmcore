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
        conn = pymysql.connect(host=host, port=port, user="root", database="default")
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
            "SELECT passenger_count, COUNT(*) as count FROM taxi_trips GROUP BY passenger_count",
            True,
        ),
        (
            "7. Average fare by payment type",
            "SELECT payment_type, AVG(fare_amount) as avg_fare FROM taxi_trips GROUP BY payment_type",
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
    ]

    results = []

    for name, sql, show_preview in test_cases:
        success, duration, row_count = run_query(cursor, name, sql, show_preview)
        results.append((name, success, duration, row_count))

    cursor.close()

    # 打印总结
    print_colored(Colors.BLUE, "\n=== Test Summary ===\n")

    passed = sum(1 for _, success, _, _ in results if success)
    failed = len(results) - passed
    total_time = sum(duration for _, _, duration, _ in results)

    print(f"Total tests: {len(results)}")
    print_colored(Colors.GREEN, f"Passed: {passed}")
    if failed > 0:
        print_colored(Colors.RED, f"Failed: {failed}")
    print(f"Total time: {total_time:.3f}s")
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
