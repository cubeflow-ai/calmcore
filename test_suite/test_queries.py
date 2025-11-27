#!/usr/bin/env python3
"""
对比测试 Calm 和原生 MySQL 的查询结果

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
    CYAN = "\033[0;36m"
    MAGENTA = "\033[0;35m"
    NC = "\033[0m"


def print_colored(color, text):
    print(f"{color}{text}{Colors.NC}")


def connect_calm(host="127.0.0.1", port=3307):
    """连接 Calm 数据库"""
    try:
        conn = pymysql.connect(
            host=host, port=port, user="root", passwd="calm", database="default"
        )
        return conn
    except Exception as e:
        print_colored(Colors.RED, f"❌ Failed to connect to Calm: {e}")
        sys.exit(1)


def connect_mysql(host="127.0.0.1", port=3306, database="calm_test"):
    """连接原生 MySQL 数据库"""
    try:
        conn = pymysql.connect(
            host=host, port=port, user="root", passwd="ansjsun", database=database
        )
        return conn
    except Exception as e:
        print_colored(Colors.RED, f"❌ Failed to connect to MySQL: {e}")
        sys.exit(1)


def format_value(val):
    """格式化值用于显示"""
    if isinstance(val, int) and val > 1000000000000:  # 可能是毫秒时间戳
        try:
            dt = datetime.fromtimestamp(val / 1000)
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except:
            return val
    elif isinstance(val, float):
        return f"{val:.2f}"
    return val


def normalize_results(results):
    """标准化查询结果用于对比（处理浮点数精度）"""
    normalized = []
    for row in results:
        normalized_row = []
        for val in row:
            if isinstance(val, float):
                # 四舍五入到小数点后2位
                normalized_row.append(round(val, 2))
            else:
                normalized_row.append(val)
        normalized.append(tuple(normalized_row))
    return normalized


def compare_results(calm_results, mysql_results, name):
    """对比两个数据库的查询结果"""
    calm_normalized = normalize_results(calm_results)
    mysql_normalized = normalize_results(mysql_results)
    
    if len(calm_normalized) != len(mysql_normalized):
        print_colored(Colors.RED, f"    ❌ Row count mismatch: Calm={len(calm_results)}, MySQL={len(mysql_results)}")
        return False
    
    # 对于聚合查询，结果顺序可能不同，需要排序后对比
    calm_sorted = sorted(calm_normalized)
    mysql_sorted = sorted(mysql_normalized)
    
    if calm_sorted == mysql_sorted:
        print_colored(Colors.GREEN, f"    ✓ Results match ({len(calm_results)} rows)")
        return True
    else:
        print_colored(Colors.RED, f"    ❌ Results differ!")
        
        # 显示前几行差异
        max_show = 5
        print_colored(Colors.YELLOW, f"    First {max_show} rows from each:")
        print_colored(Colors.CYAN, "    Calm:")
        for i, row in enumerate(calm_results[:max_show]):
            formatted = [format_value(v) for v in row]
            print(f"      [{i}] {formatted}")
        print_colored(Colors.MAGENTA, "    MySQL:")
        for i, row in enumerate(mysql_results[:max_show]):
            formatted = [format_value(v) for v in row]
            print(f"      [{i}] {formatted}")
        
        return False


def run_comparison_query(calm_cursor, mysql_cursor, name, sql):
    """在两个数据库上执行相同的查询并对比结果"""
    print_colored(Colors.YELLOW, f"\n{'='*80}")
    print_colored(Colors.YELLOW, f"📊 {name}")
    print(f"SQL: {sql}")
    print()
    
    # 执行 Calm 查询
    calm_success = False
    calm_duration = 0
    calm_results = []
    
    print_colored(Colors.CYAN, "  [Calm] Executing...")
    start_time = time.time()
    try:
        calm_cursor.execute(sql)
        calm_results = calm_cursor.fetchall()
        calm_duration = time.time() - start_time
        calm_success = True
        print_colored(Colors.GREEN, f"  [Calm] ✓ {len(calm_results):,} rows in {calm_duration:.3f}s")
    except Exception as e:
        calm_duration = time.time() - start_time
        print_colored(Colors.RED, f"  [Calm] ❌ Failed: {e}")
    
    # 执行 MySQL 查询
    mysql_success = False
    mysql_duration = 0
    mysql_results = []
    
    print_colored(Colors.MAGENTA, "  [MySQL] Executing...")
    start_time = time.time()
    try:
        mysql_cursor.execute(sql)
        mysql_results = mysql_cursor.fetchall()
        mysql_duration = time.time() - start_time
        mysql_success = True
        print_colored(Colors.GREEN, f"  [MySQL] ✓ {len(mysql_results):,} rows in {mysql_duration:.3f}s")
    except Exception as e:
        mysql_duration = time.time() - start_time
        print_colored(Colors.RED, f"  [MySQL] ❌ Failed: {e}")
    
    # 对比结果
    print()
    if calm_success and mysql_success:
        match = compare_results(calm_results, mysql_results, name)
        
        # 性能对比
        if calm_duration > 0 and mysql_duration > 0:
            speedup = mysql_duration / calm_duration
            if speedup > 1:
                print_colored(Colors.GREEN, f"    ⚡ Calm is {speedup:.2f}x faster")
            else:
                print_colored(Colors.YELLOW, f"    ⚡ MySQL is {1/speedup:.2f}x faster")
        
        return match
    elif calm_success:
        print_colored(Colors.YELLOW, "    ⚠️  Only Calm succeeded")
        return False
    elif mysql_success:
        print_colored(Colors.YELLOW, "    ⚠️  Only MySQL succeeded")
        return False
    else:
        print_colored(Colors.RED, "    ❌ Both queries failed")
        return False


def test_nyc_taxi(calm_conn, mysql_conn):
    """测试 NYC Taxi 数据集的查询"""
    print_colored(Colors.BLUE, "\n" + "="*80)
    print_colored(Colors.BLUE, "=== NYC Taxi Dataset - Calm vs MySQL Comparison ===")
    print_colored(Colors.BLUE, "="*80)

    calm_cursor = calm_conn.cursor()
    mysql_cursor = mysql_conn.cursor()

    # 2024-01-01 时间戳
    jan_1_2024 = 1704067200000
    jan_2_2024 = 1704153600000
    jan_8_2024 = 1704672000000

    test_cases = [
        ("1. Simple COUNT", "SELECT COUNT(*) FROM taxi_trips"),
        
        ("2. Simple SELECT with LIMIT", "SELECT * FROM taxi_trips LIMIT 10"),
        
        ("3. Time range query (Jan 1, 2024)",
         f"SELECT COUNT(*) FROM taxi_trips WHERE pickup_datetime >= {jan_1_2024} AND pickup_datetime < {jan_2_2024}"),
        
        ("4. Filter by passenger count",
         "SELECT COUNT(*) FROM taxi_trips WHERE passenger_count = 2"),
        
        ("5. COUNT by passenger count",
         "SELECT passenger_count, COUNT(*) as count FROM taxi_trips GROUP BY passenger_count ORDER BY passenger_count"),
        
        ("6. Average fare by payment type",
         "SELECT payment_type, AVG(fare_amount) as avg_fare, COUNT(*) as cnt FROM taxi_trips GROUP BY payment_type ORDER BY payment_type"),
        
        ("7. SUM by payment type",
         "SELECT payment_type, SUM(total_amount) as total FROM taxi_trips GROUP BY payment_type ORDER BY payment_type"),
        
        ("8. MIN/MAX statistics",
         "SELECT MIN(fare_amount) as min_fare, MAX(fare_amount) as max_fare FROM taxi_trips"),
        
        ("9. COUNT by pickup location (Top 10)",
         "SELECT pickup_location_id, COUNT(*) as count FROM taxi_trips GROUP BY pickup_location_id ORDER BY count DESC LIMIT 10"),
        
        ("10. Complex filter (time + passenger + distance)",
         f"SELECT COUNT(*) FROM taxi_trips WHERE pickup_datetime >= {jan_1_2024} AND pickup_datetime < {jan_8_2024} AND passenger_count >= 2 AND trip_distance > 5.0"),
        
        ("11. High tip trips",
         "SELECT COUNT(*) FROM taxi_trips WHERE tip_amount > 10.0"),
        
        ("12. Zero fare trips",
         "SELECT COUNT(*) FROM taxi_trips WHERE fare_amount = 0"),
        
        ("13. Long distance trips",
         "SELECT COUNT(*) FROM taxi_trips WHERE trip_distance > 50.0"),
        
        ("14. Multiple GROUP BY fields",
         "SELECT pickup_location_id, dropoff_location_id, COUNT(*) as cnt FROM taxi_trips GROUP BY pickup_location_id, dropoff_location_id ORDER BY cnt DESC LIMIT 10"),
        
        ("15. HAVING clause",
         "SELECT payment_type, COUNT(*) as cnt FROM taxi_trips GROUP BY payment_type HAVING cnt > 1000 ORDER BY payment_type"),
    ]

    passed = 0
    failed = 0

    for name, sql in test_cases:
        try:
            if run_comparison_query(calm_cursor, mysql_cursor, name, sql):
                passed += 1
            else:
                failed += 1
        except Exception as e:
            print_colored(Colors.RED, f"❌ Test failed with exception: {e}")
            failed += 1
        
        time.sleep(0.1)  # 短暂暂停，避免过快

    # 总结
    print_colored(Colors.BLUE, f"\n{'='*80}")
    print_colored(Colors.BLUE, "=== Test Summary ===")
    print_colored(Colors.BLUE, f"{'='*80}")
    total = passed + failed
    print(f"Total tests: {total}")
    print_colored(Colors.GREEN, f"Passed: {passed}")
    print_colored(Colors.RED, f"Failed: {failed}")
    
    if failed == 0:
        print_colored(Colors.GREEN, "\n🎉 All tests passed! Calm and MySQL results match perfectly!")
    else:
        print_colored(Colors.YELLOW, f"\n⚠️  {failed} test(s) failed. Please investigate differences.")

    calm_cursor.close()
    mysql_cursor.close()


def main():
    if len(sys.argv) < 2:
        print("Usage: python3 test_queries.py nyc-taxi")
        sys.exit(1)

    dataset = sys.argv[1]

    if dataset != "nyc-taxi":
        print_colored(Colors.RED, f"Unknown dataset: {dataset}")
        print("Supported datasets: nyc-taxi")
        sys.exit(1)

    print_colored(Colors.BLUE, "=== Database Connection Test ===\n")
    
    print("Connecting to Calm (127.0.0.1:3307)...")
    calm_conn = connect_calm()
    print_colored(Colors.GREEN, "✓ Connected to Calm\n")

    print("Connecting to MySQL (127.0.0.1:3306, database: calm_test)...")
    mysql_conn = connect_mysql()
    print_colored(Colors.GREEN, "✓ Connected to MySQL\n")

    try:
        if dataset == "nyc-taxi":
            test_nyc_taxi(calm_conn, mysql_conn)
    finally:
        calm_conn.close()
        mysql_conn.close()
        print_colored(Colors.BLUE, "\n✓ Connections closed")

    return 0


if __name__ == "__main__":
    sys.exit(main())
