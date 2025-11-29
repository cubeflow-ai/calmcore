#!/usr/bin/env python3
"""
生成针对 taxi_trips 表的各种查询测试用例
涵盖各种 SQL 功能和边界情况
"""


# 聚合函数测试 - 扩展版
def generate_aggregate_queries(table_name):
    return [
        # 基础聚合
        f"SELECT COUNT(*) FROM {table_name}",
        f"SELECT COUNT(DISTINCT passenger_count) FROM {table_name}",
        f"SELECT COUNT(DISTINCT payment_type) FROM {table_name}",
        f"SELECT COUNT(DISTINCT pickup_location_id) FROM {table_name}",
        f"SELECT COUNT(DISTINCT dropoff_location_id) FROM {table_name}",
        # SUM 聚合
        f"SELECT SUM(trip_distance) FROM {table_name}",
        f"SELECT SUM(fare_amount) FROM {table_name}",
        f"SELECT SUM(tip_amount) FROM {table_name}",
        f"SELECT SUM(total_amount) FROM {table_name}",
        # AVG 聚合
        f"SELECT AVG(trip_distance) FROM {table_name}",
        f"SELECT AVG(fare_amount) FROM {table_name}",
        f"SELECT AVG(tip_amount) FROM {table_name}",
        f"SELECT AVG(passenger_count) FROM {table_name}",
        # MIN/MAX
        f"SELECT MIN(trip_distance) FROM {table_name}",
        f"SELECT MAX(trip_distance) FROM {table_name}",
        f"SELECT MIN(fare_amount) FROM {table_name}",
        f"SELECT MAX(fare_amount) FROM {table_name}",
        f"SELECT MIN(passenger_count) FROM {table_name}",
        f"SELECT MAX(passenger_count) FROM {table_name}",
        # 多列聚合
        f"SELECT SUM(fare_amount), AVG(fare_amount) FROM {table_name}",
        f"SELECT COUNT(*), SUM(trip_distance), AVG(trip_distance) FROM {table_name}",
        f"SELECT MIN(fare_amount), MAX(fare_amount), AVG(fare_amount) FROM {table_name}",
        f"SELECT COUNT(*), SUM(fare_amount), SUM(tip_amount), SUM(total_amount) FROM {table_name}",
        # 条件聚合
        f"SELECT COUNT(*) FROM {table_name} WHERE trip_distance > 0",
        f"SELECT AVG(fare_amount) FROM {table_name} WHERE fare_amount > 0",
        f"SELECT SUM(tip_amount) FROM {table_name} WHERE tip_amount > 0",
        f"SELECT COUNT(*) FROM {table_name} WHERE passenger_count > 0",
    ]


# GROUP BY 查询 - 扩展版
def generate_group_by_queries(table_name):
    return [
        # 单列分组
        f"SELECT passenger_count, COUNT(*) FROM {table_name} GROUP BY passenger_count",
        f"SELECT passenger_count, COUNT(*) FROM {table_name} GROUP BY passenger_count ORDER BY passenger_count",
        f"SELECT passenger_count, COUNT(*) FROM {table_name} GROUP BY passenger_count ORDER BY COUNT(*) DESC",
        f"SELECT payment_type, COUNT(*) FROM {table_name} GROUP BY payment_type",
        f"SELECT payment_type, COUNT(*) FROM {table_name} GROUP BY payment_type ORDER BY payment_type",
        # 带聚合函数的分组
        f"SELECT passenger_count, AVG(trip_distance) FROM {table_name} GROUP BY passenger_count",
        f"SELECT passenger_count, AVG(fare_amount) FROM {table_name} GROUP BY passenger_count",
        f"SELECT passenger_count, SUM(trip_distance) FROM {table_name} GROUP BY passenger_count",
        f"SELECT passenger_count, SUM(fare_amount) FROM {table_name} GROUP BY passenger_count",
        f"SELECT passenger_count, MIN(fare_amount), MAX(fare_amount) FROM {table_name} GROUP BY passenger_count",
        f"SELECT payment_type, AVG(fare_amount) FROM {table_name} GROUP BY payment_type",
        f"SELECT payment_type, SUM(total_amount) FROM {table_name} GROUP BY payment_type",
        # 多列分组
        f"SELECT passenger_count, payment_type, COUNT(*) FROM {table_name} GROUP BY passenger_count, payment_type",
        f"SELECT passenger_count, payment_type, COUNT(*) FROM {table_name} GROUP BY passenger_count, payment_type ORDER BY passenger_count, payment_type",
        # HAVING 子句
        f"SELECT passenger_count, COUNT(*) as cnt FROM {table_name} GROUP BY passenger_count HAVING cnt > 100",
        f"SELECT passenger_count, COUNT(*) as cnt FROM {table_name} GROUP BY passenger_count HAVING cnt > 1000",
        f"SELECT passenger_count, AVG(trip_distance) as avg_dist FROM {table_name} GROUP BY passenger_count HAVING avg_dist > 2.0",
        f"SELECT payment_type, COUNT(*) as cnt FROM {table_name} GROUP BY payment_type HAVING cnt > 1000",
        # 复杂聚合
        f"SELECT passenger_count, COUNT(*) as cnt, AVG(trip_distance) as avg_dist, SUM(fare_amount) as total_fare FROM {table_name} GROUP BY passenger_count",
        f"SELECT payment_type, COUNT(*) as cnt, AVG(fare_amount) as avg_fare, SUM(total_amount) as total FROM {table_name} GROUP BY payment_type",
    ]


# WHERE 条件查询 - 扩展版
def generate_where_queries(table_name):
    return [
        # 等值查询
        f"SELECT COUNT(*) FROM {table_name} WHERE passenger_count = 1",
        f"SELECT COUNT(*) FROM {table_name} WHERE passenger_count = 2",
        f"SELECT COUNT(*) FROM {table_name} WHERE passenger_count = 3",
        f"SELECT COUNT(*) FROM {table_name} WHERE payment_type = 1",
        f"SELECT COUNT(*) FROM {table_name} WHERE payment_type = 2",
        # 比较查询
        f"SELECT COUNT(*) FROM {table_name} WHERE passenger_count > 2",
        f"SELECT COUNT(*) FROM {table_name} WHERE passenger_count >= 2",
        f"SELECT COUNT(*) FROM {table_name} WHERE passenger_count < 5",
        f"SELECT COUNT(*) FROM {table_name} WHERE passenger_count <= 3",
        f"SELECT COUNT(*) FROM {table_name} WHERE passenger_count != 1",
        f"SELECT COUNT(*) FROM {table_name} WHERE trip_distance > 5.0",
        f"SELECT COUNT(*) FROM {table_name} WHERE trip_distance >= 10.0",
        f"SELECT COUNT(*) FROM {table_name} WHERE trip_distance < 1.0",
        f"SELECT COUNT(*) FROM {table_name} WHERE fare_amount > 10.0",
        f"SELECT COUNT(*) FROM {table_name} WHERE fare_amount >= 20.0",
        f"SELECT COUNT(*) FROM {table_name} WHERE fare_amount < 5.0",
        # BETWEEN 查询
        f"SELECT COUNT(*) FROM {table_name} WHERE passenger_count BETWEEN 2 AND 4",
        f"SELECT COUNT(*) FROM {table_name} WHERE trip_distance BETWEEN 1.0 AND 5.0",
        f"SELECT COUNT(*) FROM {table_name} WHERE fare_amount BETWEEN 10.0 AND 20.0",
        # IN 查询
        f"SELECT COUNT(*) FROM {table_name} WHERE passenger_count IN (1, 2, 3)",
        f"SELECT COUNT(*) FROM {table_name} WHERE passenger_count NOT IN (1, 2)",
        f"SELECT COUNT(*) FROM {table_name} WHERE payment_type IN (1, 2)",
        # AND 组合条件
        f"SELECT COUNT(*) FROM {table_name} WHERE trip_distance > 5.0 AND fare_amount > 10.0",
        f"SELECT COUNT(*) FROM {table_name} WHERE passenger_count = 1 AND trip_distance > 5.0",
        f"SELECT COUNT(*) FROM {table_name} WHERE passenger_count >= 2 AND fare_amount >= 20.0",
        f"SELECT COUNT(*) FROM {table_name} WHERE trip_distance > 1.0 AND trip_distance < 10.0",
        f"SELECT COUNT(*) FROM {table_name} WHERE passenger_count > 0 AND trip_distance > 0 AND fare_amount > 0",
        # OR 组合条件
        f"SELECT COUNT(*) FROM {table_name} WHERE trip_distance > 10.0 OR fare_amount > 20.0",
        f"SELECT COUNT(*) FROM {table_name} WHERE passenger_count = 1 OR passenger_count = 2",
        f"SELECT COUNT(*) FROM {table_name} WHERE payment_type = 1 OR payment_type = 2",
        # NOT 条件
        f"SELECT COUNT(*) FROM {table_name} WHERE NOT (passenger_count = 1)",
        f"SELECT COUNT(*) FROM {table_name} WHERE NOT (trip_distance > 10.0)",
        # 空值检查
        f"SELECT COUNT(*) FROM {table_name} WHERE passenger_count IS NOT NULL",
        f"SELECT COUNT(*) FROM {table_name} WHERE trip_distance IS NOT NULL",
        # 零值检查
        f"SELECT COUNT(*) FROM {table_name} WHERE fare_amount = 0",
        f"SELECT COUNT(*) FROM {table_name} WHERE tip_amount = 0",
        f"SELECT COUNT(*) FROM {table_name} WHERE trip_distance = 0",
        f"SELECT COUNT(*) FROM {table_name} WHERE passenger_count = 0",
        # 大于某个阈值
        f"SELECT COUNT(*) FROM {table_name} WHERE tip_amount > 5.0",
        f"SELECT COUNT(*) FROM {table_name} WHERE tip_amount > 10.0",
        f"SELECT COUNT(*) FROM {table_name} WHERE trip_distance > 20.0",
        f"SELECT COUNT(*) FROM {table_name} WHERE trip_distance > 50.0",
        f"SELECT COUNT(*) FROM {table_name} WHERE fare_amount > 50.0",
        f"SELECT COUNT(*) FROM {table_name} WHERE fare_amount > 100.0",
    ]


# ORDER BY 和 LIMIT - 扩展版
def generate_order_limit_queries(table_name):
    return [
        # 基础 ORDER BY
        f"SELECT passenger_count, trip_distance, fare_amount FROM {table_name} ORDER BY passenger_count, id LIMIT 10",
        f"SELECT passenger_count, trip_distance, fare_amount FROM {table_name} ORDER BY passenger_count DESC, id LIMIT 10",
        f"SELECT id, trip_distance, fare_amount FROM {table_name} ORDER BY trip_distance, id LIMIT 10",
        f"SELECT id, trip_distance, fare_amount FROM {table_name} ORDER BY trip_distance DESC, id LIMIT 10",
        f"SELECT id, fare_amount, trip_distance FROM {table_name} ORDER BY fare_amount, id LIMIT 10",
        f"SELECT id, fare_amount, trip_distance FROM {table_name} ORDER BY fare_amount DESC, id LIMIT 10",
        f"SELECT id, total_amount, fare_amount FROM {table_name} ORDER BY total_amount, id LIMIT 10",
        f"SELECT id, total_amount, fare_amount FROM {table_name} ORDER BY total_amount DESC, id LIMIT 10",
        # 多列排序
        f"SELECT passenger_count, trip_distance, fare_amount FROM {table_name} ORDER BY passenger_count, trip_distance, id LIMIT 10",
        f"SELECT passenger_count, trip_distance, fare_amount FROM {table_name} ORDER BY passenger_count DESC, trip_distance DESC, id LIMIT 10",
        f"SELECT payment_type, fare_amount, trip_distance FROM {table_name} ORDER BY payment_type, fare_amount, id LIMIT 10",
        # SELECT 特定列
        f"SELECT passenger_count, trip_distance FROM {table_name} ORDER BY passenger_count, id LIMIT 20",
        f"SELECT passenger_count, fare_amount FROM {table_name} ORDER BY fare_amount DESC, id LIMIT 20",
        f"SELECT pickup_location_id, dropoff_location_id, trip_distance FROM {table_name} ORDER BY trip_distance DESC, id LIMIT 20",
        f"SELECT payment_type, total_amount FROM {table_name} ORDER BY total_amount DESC, id LIMIT 20",
        # 不同的 LIMIT 值
        f"SELECT passenger_count, trip_distance, fare_amount FROM {table_name} ORDER BY id LIMIT 5",
        f"SELECT passenger_count, trip_distance, fare_amount FROM {table_name} ORDER BY id LIMIT 20",
        f"SELECT passenger_count, trip_distance, fare_amount FROM {table_name} ORDER BY id LIMIT 50",
        f"SELECT passenger_count, trip_distance, fare_amount FROM {table_name} ORDER BY id LIMIT 100",
        f"SELECT passenger_count, trip_distance, fare_amount FROM {table_name} ORDER BY id LIMIT 500",
        f"SELECT passenger_count, trip_distance, fare_amount FROM {table_name} ORDER BY id LIMIT 1000",
        # OFFSET 分页
        f"SELECT passenger_count, trip_distance, fare_amount FROM {table_name} ORDER BY id LIMIT 10 OFFSET 0",
        f"SELECT passenger_count, trip_distance, fare_amount FROM {table_name} ORDER BY id LIMIT 10 OFFSET 10",
        f"SELECT passenger_count, trip_distance, fare_amount FROM {table_name} ORDER BY id LIMIT 10 OFFSET 20",
        f"SELECT passenger_count, trip_distance, fare_amount FROM {table_name} ORDER BY id LIMIT 100 OFFSET 50",
        f"SELECT passenger_count, trip_distance, fare_amount FROM {table_name} ORDER BY id LIMIT 50 OFFSET 100",
        f"SELECT passenger_count, trip_distance, fare_amount FROM {table_name} ORDER BY passenger_count, id LIMIT 50 OFFSET 100",
        f"SELECT id, fare_amount, trip_distance FROM {table_name} ORDER BY fare_amount DESC, id LIMIT 20 OFFSET 50",
        # 带 WHERE 的 ORDER BY
        f"SELECT passenger_count, trip_distance, fare_amount FROM {table_name} WHERE passenger_count > 0 ORDER BY trip_distance, id LIMIT 20",
        f"SELECT trip_distance, fare_amount, passenger_count FROM {table_name} WHERE trip_distance > 5.0 ORDER BY fare_amount DESC, id LIMIT 20",
        f"SELECT fare_amount, trip_distance, passenger_count FROM {table_name} WHERE fare_amount > 10.0 ORDER BY fare_amount, id LIMIT 20",
    ]


# DISTINCT 查询 - 扩展版
def generate_distinct_queries(table_name):
    return [
        # 单列 DISTINCT
        f"SELECT DISTINCT passenger_count FROM {table_name}",
        f"SELECT DISTINCT passenger_count FROM {table_name} ORDER BY passenger_count",
        f"SELECT DISTINCT payment_type FROM {table_name}",
        f"SELECT DISTINCT payment_type FROM {table_name} ORDER BY payment_type",
        # 多列 DISTINCT
        f"SELECT DISTINCT passenger_count, payment_type FROM {table_name}",
        f"SELECT DISTINCT passenger_count, payment_type FROM {table_name} ORDER BY passenger_count, payment_type",
        # COUNT DISTINCT
        f"SELECT COUNT(DISTINCT passenger_count) FROM {table_name}",
        f"SELECT COUNT(DISTINCT payment_type) FROM {table_name}",
        f"SELECT COUNT(DISTINCT pickup_location_id) FROM {table_name}",
        f"SELECT COUNT(DISTINCT dropoff_location_id) FROM {table_name}",
        # DISTINCT with WHERE
        f"SELECT DISTINCT passenger_count FROM {table_name} WHERE trip_distance > 5.0",
        f"SELECT DISTINCT payment_type FROM {table_name} WHERE fare_amount > 10.0",
        f"SELECT COUNT(DISTINCT passenger_count) FROM {table_name} WHERE trip_distance > 0",
    ]


# 复杂查询 - 实用版（去掉 Calm 可能不支持的高级特性）
def generate_complex_queries(table_name):
    return [
        # 复杂 WHERE 条件
        f"SELECT COUNT(*) FROM {table_name} WHERE (passenger_count = 1 OR passenger_count = 2) AND trip_distance > 5.0",
        f"SELECT COUNT(*) FROM {table_name} WHERE (passenger_count = 1 OR passenger_count = 2) AND fare_amount > 10.0",
        f"SELECT COUNT(*) FROM {table_name} WHERE passenger_count IN (1, 2, 3) AND fare_amount BETWEEN 10.0 AND 50.0",
        f"SELECT COUNT(*) FROM {table_name} WHERE passenger_count IN (2, 3, 4) AND trip_distance BETWEEN 1.0 AND 10.0",
        f"SELECT COUNT(*) FROM {table_name} WHERE trip_distance > 0 AND fare_amount > 0 AND tip_amount > 0",
        f"SELECT COUNT(*) FROM {table_name} WHERE passenger_count > 0 AND trip_distance > 0 AND fare_amount > 0 AND tip_amount > 0",
        # 复杂 GROUP BY + HAVING
        f"SELECT passenger_count, COUNT(*) as cnt, AVG(trip_distance) as avg_dist, SUM(fare_amount) as total_fare FROM {table_name} GROUP BY passenger_count HAVING cnt > 50 ORDER BY total_fare DESC",
        f"SELECT passenger_count, COUNT(*) as cnt, AVG(trip_distance) as avg_dist, SUM(fare_amount) as total_fare FROM {table_name} GROUP BY passenger_count HAVING cnt > 100",
        f"SELECT payment_type, COUNT(*) as cnt, AVG(fare_amount) as avg_fare, SUM(total_amount) as total FROM {table_name} GROUP BY payment_type HAVING cnt > 100 ORDER BY total DESC",
        f"SELECT payment_type, COUNT(*) as cnt, AVG(fare_amount) as avg_fare FROM {table_name} GROUP BY payment_type HAVING cnt > 1000",
        # 多列聚合 + 多条件
        f"SELECT passenger_count, payment_type, COUNT(*) as cnt, AVG(fare_amount) as avg_fare FROM {table_name} GROUP BY passenger_count, payment_type HAVING cnt > 10",
        f"SELECT passenger_count, payment_type, COUNT(*) as cnt, SUM(fare_amount) as total_fare FROM {table_name} GROUP BY passenger_count, payment_type HAVING cnt > 50",
        # 多表达式计算（不使用 NULLIF）
        f"SELECT passenger_count, SUM(fare_amount) as total_fare, COUNT(*) as trip_count FROM {table_name} GROUP BY passenger_count",
        f"SELECT passenger_count, SUM(trip_distance) as total_distance, COUNT(*) as trip_count FROM {table_name} GROUP BY passenger_count",
        f"SELECT payment_type, SUM(fare_amount) as total_fare, COUNT(*) as trip_count FROM {table_name} GROUP BY payment_type",
        # 复杂排序 + 分页
        f"SELECT passenger_count, trip_distance, fare_amount FROM {table_name} WHERE trip_distance > 10.0 ORDER BY fare_amount DESC, id LIMIT 20",
        f"SELECT passenger_count, trip_distance, fare_amount FROM {table_name} WHERE fare_amount > 50.0 ORDER BY trip_distance DESC, id LIMIT 20",
        f"SELECT passenger_count, trip_distance, fare_amount FROM {table_name} WHERE trip_distance > 20.0 ORDER BY trip_distance DESC, id LIMIT 30",
        f"SELECT payment_type, fare_amount, tip_amount FROM {table_name} WHERE tip_amount > 5.0 ORDER BY tip_amount DESC, id LIMIT 50",
    ]


def generate_all_test_cases(table_name="taxi_trips"):
    """生成所有测试用例 - 基于实际表的查询"""
    all_tests = []

    # 基于表的查询测试
    all_tests.extend([("聚合函数", q) for q in generate_aggregate_queries(table_name)])
    all_tests.extend([("GROUP BY", q) for q in generate_group_by_queries(table_name)])
    all_tests.extend([("WHERE 条件", q) for q in generate_where_queries(table_name)])
    all_tests.extend(
        [("ORDER BY & LIMIT", q) for q in generate_order_limit_queries(table_name)]
    )
    all_tests.extend([("DISTINCT", q) for q in generate_distinct_queries(table_name)])
    all_tests.extend([("复杂查询", q) for q in generate_complex_queries(table_name)])

    return all_tests


def print_test_summary():
    """打印测试用例统计"""
    tests = generate_all_test_cases()

    print("=" * 80)
    print("SQL 测试用例统计")
    print("=" * 80)

    categories = {}
    for category, query in tests:
        if category not in categories:
            categories[category] = []
        categories[category].append(query)

    total = 0
    for category, queries in categories.items():
        count = len(queries)
        total += count
        print(f"{category:20s}: {count:4d} 条")

    print("-" * 80)
    print(f"{'总计':20s}: {total:4d} 条")
    print("=" * 80)


if __name__ == "__main__":
    print_test_summary()
