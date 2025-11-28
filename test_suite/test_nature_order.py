#!/usr/bin/env python3
"""
测试 ORDER BY _nature 深度分页的正确性

验证策略：
1. 使用 _nature 分页迭代所有数据
2. 使用 COUNT(*) 验证总行数
3. 使用 GROUP BY 验证数据分布
4. 确保没有重复或遗漏

使用方法：
    python3 test_nature_order.py nyc-taxi
"""

import sys
import pymysql
from collections import defaultdict


class Colors:
    GREEN = "\033[0;32m"
    YELLOW = "\033[1;33m"
    RED = "\033[0;31m"
    CYAN = "\033[0;36m"
    BLUE = "\033[0;34m"
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


def test_nature_pagination(table_name, batch_size=1000):
    """
    测试 ORDER BY _nature 分页

    返回：(总行数, 所有数据的ID列表)
    """
    print_colored(Colors.CYAN, f"\n{'='*60}")
    print_colored(
        Colors.CYAN, f"Testing ORDER BY _nature pagination on table: {table_name}"
    )
    print_colored(Colors.CYAN, f"Batch size: {batch_size}")
    print_colored(Colors.CYAN, f"{'='*60}\n")

    conn = connect_calm()
    cursor = conn.cursor()

    all_ids = []
    offset = 0
    batch_count = 0

    print_colored(Colors.BLUE, "📖 Iterating through data with ORDER BY _nature...")

    while True:
        sql = f"SELECT * FROM `{table_name}` ORDER BY _nature LIMIT {batch_size} OFFSET {offset}"

        cursor.execute(sql)
        rows = cursor.fetchall()

        if not rows:
            break

        batch_count += 1
        row_count = len(rows)

        # 收集第一列作为 ID（通常是主键）
        batch_ids = [row[0] for row in rows]
        all_ids.extend(batch_ids)

        print_colored(
            Colors.GREEN,
            f"  Batch {batch_count}: offset={offset:,}, fetched {row_count} rows",
        )

        offset += batch_size

        # 安全检查：防止无限循环
        if offset > 10_000_000:
            print_colored(Colors.RED, "⚠️  Safety limit reached (10M rows)")
            break

    cursor.close()
    conn.close()

    total_rows = len(all_ids)
    print_colored(Colors.GREEN, f"\n✅ Total rows fetched via _nature: {total_rows:,}")

    return total_rows, all_ids


def verify_count(table_name):
    """使用 COUNT(*) 验证总行数"""
    print_colored(Colors.BLUE, "\n🔢 Verifying total count with COUNT(*)...")

    conn = connect_calm()
    cursor = conn.cursor()

    sql = f"SELECT COUNT(*) FROM `{table_name}`"
    cursor.execute(sql)
    count = cursor.fetchone()[0]

    cursor.close()
    conn.close()

    print_colored(Colors.GREEN, f"✅ COUNT(*) result: {count:,}")
    return count


def verify_uniqueness(all_ids):
    """验证是否有重复的ID"""
    print_colored(Colors.BLUE, "\n🔍 Checking for duplicates...")

    unique_ids = set(all_ids)
    duplicates = len(all_ids) - len(unique_ids)

    if duplicates > 0:
        print_colored(Colors.RED, f"❌ Found {duplicates:,} duplicate IDs!")

        # 找出重复的ID
        id_counts = defaultdict(int)
        for id_val in all_ids:
            id_counts[id_val] += 1

        dup_ids = [id_val for id_val, count in id_counts.items() if count > 1]
        print_colored(Colors.RED, f"  Duplicate IDs (showing first 10): {dup_ids[:10]}")
        return False
    else:
        print_colored(
            Colors.GREEN,
            f"✅ No duplicates found! All {len(unique_ids):,} IDs are unique",
        )
        return True


def verify_distribution(table_name, group_by_field):
    """
    使用 GROUP BY 验证数据分布

    对比迭代得到的数据和 GROUP BY COUNT(*) 的结果
    """
    print_colored(
        Colors.BLUE,
        f"\n📊 Verifying data distribution with GROUP BY {group_by_field}...",
    )

    conn = connect_calm()
    cursor = conn.cursor()

    # 获取每个分组的计数
    sql = f"SELECT {group_by_field}, COUNT(*) FROM `{table_name}` GROUP BY {group_by_field}"
    cursor.execute(sql)
    group_counts = cursor.fetchall()

    cursor.close()
    conn.close()

    total_from_groupby = sum(count for _, count in group_counts)

    print_colored(Colors.GREEN, f"✅ GROUP BY result:")
    print_colored(Colors.GREEN, f"  - Total groups: {len(group_counts)}")
    print_colored(Colors.GREEN, f"  - Total rows (sum): {total_from_groupby:,}")

    # 显示前10个分组
    print_colored(Colors.CYAN, "\n  Top 10 groups:")
    for field_value, count in sorted(group_counts, key=lambda x: x[1], reverse=True)[
        :10
    ]:
        print_colored(Colors.CYAN, f"    {field_value}: {count:,} rows")

    return total_from_groupby, len(group_counts)


def run_test(table_name, batch_size=1000, group_by_field=None):
    """运行完整的测试套件"""
    print_colored(Colors.YELLOW, f"\n{'#'*70}")
    print_colored(Colors.YELLOW, f"# ORDER BY _nature Test Suite")
    print_colored(Colors.YELLOW, f"# Table: {table_name}")
    print_colored(Colors.YELLOW, f"{'#'*70}\n")

    # 1. 使用 _nature 迭代所有数据
    nature_total, all_ids = test_nature_pagination(table_name, batch_size)

    # 2. 验证总数
    count_total = verify_count(table_name)

    # 3. 验证唯一性
    is_unique = verify_uniqueness(all_ids)

    # 4. 验证数据分布
    groupby_total = None
    if group_by_field:
        groupby_total, group_count = verify_distribution(table_name, group_by_field)

    # 最终结果
    print_colored(Colors.YELLOW, f"\n{'='*70}")
    print_colored(Colors.YELLOW, "FINAL RESULTS:")
    print_colored(Colors.YELLOW, f"{'='*70}")

    print_colored(Colors.CYAN, f"  ORDER BY _nature fetched: {nature_total:,} rows")
    print_colored(Colors.CYAN, f"  COUNT(*) returned:        {count_total:,} rows")
    if groupby_total:
        print_colored(
            Colors.CYAN, f"  GROUP BY sum:             {groupby_total:,} rows"
        )
    print_colored(
        Colors.CYAN,
        f"  Uniqueness check:         {'✅ PASS' if is_unique else '❌ FAIL'}",
    )

    # 判断是否通过
    all_match = nature_total == count_total
    if groupby_total:
        all_match = all_match and (nature_total == groupby_total)
    all_match = all_match and is_unique

    print_colored(Colors.YELLOW, f"{'='*70}")

    if all_match:
        print_colored(Colors.GREEN, "\n🎉 ALL TESTS PASSED!")
        print_colored(Colors.GREEN, "   - Row counts match")
        print_colored(Colors.GREEN, "   - No duplicates found")
        print_colored(Colors.GREEN, "   - Data integrity verified\n")
        return True
    else:
        print_colored(Colors.RED, "\n❌ TESTS FAILED!")
        if nature_total != count_total:
            print_colored(
                Colors.RED,
                f"   - Row count mismatch: {nature_total:,} vs {count_total:,}",
            )
        if groupby_total and nature_total != groupby_total:
            print_colored(
                Colors.RED,
                f"   - GROUP BY mismatch: {nature_total:,} vs {groupby_total:,}",
            )
        if not is_unique:
            print_colored(Colors.RED, "   - Duplicate IDs detected")
        print()
        return False


def main():
    if len(sys.argv) < 2:
        print(
            "Usage: python3 test_nature_order.py <table_name> [batch_size] [group_by_field]"
        )
        print("\nExamples:")
        print("  python3 test_nature_order.py nyc-taxi")
        print("  python3 test_nature_order.py nyc-taxi 500")
        print("  python3 test_nature_order.py nyc-taxi 1000 passenger_count")
        sys.exit(1)

    table_name = sys.argv[1]
    batch_size = int(sys.argv[2]) if len(sys.argv) > 2 else 1000
    group_by_field = sys.argv[3] if len(sys.argv) > 3 else None

    # 默认的 group by 字段
    if group_by_field is None:
        if table_name == "nyc-taxi" or table_name == "taxi_trips":
            group_by_field = "passenger_count"
        elif table_name == "wikipedia":
            group_by_field = "namespace"

    success = run_test(table_name, batch_size, group_by_field)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
