#!/usr/bin/env python3
"""
测试 MySQL 流式游标 (SSCursor)
验证数据是否真正流式拉取,而不是一次性加载到内存
"""

import pymysql
import time
import psutil
import os

# 数据库配置
DB_CONFIG = {
    "host": "11.3.83.3",
    "port": 443,
    "user": "root",
    "password": "r2archland",
    "database": "r2api",
    "charset": "utf8mb4",
}

# 测试 SQL
SQL = "SELECT '11.3.83.3', 'r2api' as db, 'r2api', trace_id FROM r2api"


def get_memory_mb():
    """获取当前进程内存使用(MB)"""
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / 1024 / 1024


def test_streaming_cursor():
    """流式游标 (SSCursor) - 按需从服务器拉取数据"""
    print("\n" + "=" * 80)
    print("测试: 流式游标 (SSCursor, 服务器端游标)")
    print("=" * 80)

    mem_start = get_memory_mb()
    print(f"开始内存: {mem_start:.2f} MB")

    conn = pymysql.connect(**DB_CONFIG)
    # 使用 SSCursor (Server Side Cursor) - 流式游标
    cursor = conn.cursor(pymysql.cursors.SSCursor)

    print(f"\n执行查询: {SQL}")
    start_time = time.time()

    cursor.execute(SQL)

    fetch_time = time.time()
    mem_after_execute = get_memory_mb()

    print(f"execute() 完成耗时: {fetch_time - start_time:.2f} 秒")
    print(
        f"execute() 后内存: {mem_after_execute:.2f} MB (增加 {mem_after_execute - mem_start:.2f} MB)"
    )
    print(f"说明: 流式游标在 execute() 时只发送查询,不加载数据\n")

    # 逐行读取
    row_count = 0
    print("开始逐行 fetch...")

    mem_peak = mem_after_execute

    for i, row in enumerate(cursor):
        row_count += 1

        # 每 10000 行打印一次进度
        if (i + 1) % 10000 == 0:
            current_mem = get_memory_mb()
            mem_peak = max(mem_peak, current_mem)
            elapsed = time.time() - start_time
            rate = (i + 1) / elapsed
            print(
                f"  已读取 {i + 1:,} 行, 速度: {rate:,.0f} 行/秒, 内存: {current_mem:.2f} MB, 峰值: {mem_peak:.2f} MB"
            )

    end_time = time.time()
    mem_end = get_memory_mb()

    print(f"\n总共读取: {row_count:,} 行")
    print(f"总耗时: {end_time - start_time:.2f} 秒")
    print(f"平均速度: {row_count / (end_time - start_time):,.0f} 行/秒")
    print(f"最终内存: {mem_end:.2f} MB")
    print(f"内存峰值: {mem_peak:.2f} MB")
    print(f"内存增长: {mem_peak - mem_start:.2f} MB (峰值)")

    cursor.close()
    conn.close()

    return row_count, end_time - start_time, mem_peak - mem_start


def main():
    print("\n🔍 MySQL 流式游标测试")
    print("=" * 80)
    print(f"连接: {DB_CONFIG['host']}:{DB_CONFIG['port']}")
    print(f"数据库: {DB_CONFIG['database']}")
    print(f"查询: {SQL}")
    print()

    try:
        rows, elapsed, mem_growth = test_streaming_cursor()

        print("\n" + "=" * 80)
        print("📊 测试结果")
        print("=" * 80)
        print(f"总行数: {rows:,}")
        print(f"总耗时: {elapsed:.2f} 秒")
        print(f"平均速度: {rows / elapsed:,.0f} 行/秒")
        print(f"内存增长: {mem_growth:.2f} MB")
        print()

        if mem_growth < 100:  # 如果内存增长小于 100MB
            print("✅ 流式游标生效! 内存增长很小,说明数据是流式拉取的")
        elif mem_growth < 500:
            print("⚠️  流式游标部分生效,内存增长适中")
        else:
            print("❌ 流式游标可能未生效! 内存增长过大")

    except Exception as e:
        print(f"\n❌ 错误: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    main()
