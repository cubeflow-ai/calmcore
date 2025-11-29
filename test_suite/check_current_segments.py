#!/usr/bin/env python3
"""
检查是否有 current segment 中的数据被遗漏
"""
import pymysql


def check_current_segments():
    """查询时是否包含 current segment"""

    # 连接数据库
    conn = pymysql.connect(
        host="127.0.0.1", port=3307, user="root", passwd="calm", database="default"
    )
    cursor = conn.cursor()

    print("=" * 80)
    print("检查 taxi_trips 表的 segment 情况")
    print("=" * 80)
    print()

    # 1. 总行数
    cursor.execute("SELECT COUNT(*) FROM taxi_trips")
    total_count = cursor.fetchone()[0]
    print(f"总行数: {total_count:,}")

    # 2. 使用 ORDER BY _nature 查询的行数
    cursor.execute("SELECT COUNT(*) FROM taxi_trips ORDER BY _nature LIMIT 10000000")
    nature_count = cursor.fetchone()[0]
    print(f"ORDER BY _nature 返回行数: {nature_count:,}")

    # 3. 带 WHERE 条件的总数
    cursor.execute("SELECT COUNT(*) FROM taxi_trips WHERE passenger_count > 2")
    where_count = cursor.fetchone()[0]
    print(f"WHERE passenger_count > 2 总数: {where_count:,}")

    # 4. 带 WHERE + ORDER BY _nature
    cursor.execute(
        "SELECT COUNT(*) FROM taxi_trips WHERE passenger_count > 2 ORDER BY _nature LIMIT 10000000"
    )
    nature_where_count = cursor.fetchone()[0]
    print(f"WHERE + ORDER BY _nature 返回行数: {nature_where_count:,}")

    print()
    print("=" * 80)
    print("差异分析:")
    print("=" * 80)

    if total_count != nature_count:
        missing = total_count - nature_count
        print(f"❌ ORDER BY _nature 丢失了 {missing:,} 行数据!")
    else:
        print(f"✅ ORDER BY _nature 行数正确")

    if where_count != nature_where_count:
        missing = where_count - nature_where_count
        print(f"❌ WHERE + ORDER BY _nature 丢失了 {missing:,} 行数据!")
    else:
        print(f"✅ WHERE + ORDER BY _nature 行数正确")

    cursor.close()
    conn.close()


if __name__ == "__main__":
    check_current_segments()
