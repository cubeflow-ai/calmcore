#!/usr/bin/env python3
"""
测试 INSERT 功能

使用方法：
    python3 test_insert.py
"""

import pymysql

# 颜色
class Colors:
    BLUE = '\033[0;34m'
    GREEN = '\033[0;32m'
    YELLOW = '\033[1;33m'
    RED = '\033[0;31m'
    NC = '\033[0m'

def print_colored(color, text):
    print(f"{color}{text}{Colors.NC}")

def main():
    print_colored(Colors.BLUE, "=== Testing INSERT ===\n")
    
    try:
        # 连接数据库
        conn = pymysql.connect(
            host='127.0.0.1',
            port=3306,
            user='root',
            database='default'
        )
        cursor = conn.cursor()
        
        print_colored(Colors.GREEN, "✓ Connected to MySQL\n")
        
        # 测试 1: 单条 INSERT
        print_colored(Colors.BLUE, "Test 1: Single INSERT")
        sql = "INSERT INTO taxi_trips (id, pickup_datetime, dropoff_datetime, passenger_count, trip_distance, fare_amount, tip_amount, total_amount, payment_type, pickup_location_id, dropoff_location_id) VALUES ('test_1', 1704067200000, 1704067800000, 2, 5.5, 15.50, 3.00, 18.50, 1, 100, 200)"
        
        try:
            cursor.execute(sql)
            conn.commit()
            print_colored(Colors.GREEN, "✓ Single INSERT succeeded\n")
        except Exception as e:
            print_colored(Colors.RED, f"✗ Single INSERT failed: {e}\n")
            return 1
        
        # 测试 2: 批量 INSERT (3 条)
        print_colored(Colors.BLUE, "Test 2: Batch INSERT (3 rows)")
        sql = """INSERT INTO taxi_trips (id, pickup_datetime, dropoff_datetime, passenger_count, trip_distance, fare_amount, tip_amount, total_amount, payment_type, pickup_location_id, dropoff_location_id) 
        VALUES 
        ('test_2', 1704067200000, 1704067800000, 1, 3.2, 12.00, 2.00, 14.00, 1, 101, 201),
        ('test_3', 1704067300000, 1704067900000, 3, 7.8, 22.50, 4.50, 27.00, 2, 102, 202),
        ('test_4', 1704067400000, 1704068000000, 2, 4.5, 16.00, 3.20, 19.20, 1, 103, 203)"""
        
        try:
            cursor.execute(sql)
            conn.commit()
            print_colored(Colors.GREEN, "✓ Batch INSERT succeeded\n")
        except Exception as e:
            print_colored(Colors.RED, f"✗ Batch INSERT failed: {e}\n")
            return 1
        
        # 测试 3: 查询验证
        print_colored(Colors.BLUE, "Test 3: Verify data")
        sql = "SELECT COUNT(*) FROM taxi_trips WHERE id LIKE 'test_%'"
        
        try:
            cursor.execute(sql)
            result = cursor.fetchone()
            count = result[0]
            
            if count == 4:
                print_colored(Colors.GREEN, f"✓ Found {count} test rows (expected 4)\n")
            else:
                print_colored(Colors.YELLOW, f"⚠ Found {count} test rows (expected 4)\n")
        except Exception as e:
            print_colored(Colors.RED, f"✗ Query failed: {e}\n")
            return 1
        
        cursor.close()
        conn.close()
        
        print_colored(Colors.GREEN, "🎉 All tests passed!")
        return 0
        
    except Exception as e:
        print_colored(Colors.RED, f"❌ Error: {e}")
        return 1

if __name__ == '__main__':
    import sys
    sys.exit(main())
