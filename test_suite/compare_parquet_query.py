#!/usr/bin/env python3
"""
对比 parquet 文件和查询结果,找出丢失的数据
"""
import pyarrow.parquet as pq
from pathlib import Path
import sys
import subprocess
import json


def get_from_parquet():
    """从 parquet 文件读取所有 passenger_count > 2 的 id"""
    tables_dir = Path("../data/tables/taxi_trips/partitions")

    all_ids = set()

    for partition_dir in sorted(tables_dir.iterdir()):
        if not partition_dir.is_dir():
            continue

        for segment_dir in sorted(partition_dir.iterdir()):
            if not segment_dir.is_dir() or not segment_dir.name.startswith("segment-"):
                continue

            parquet_file = segment_dir / "rowdata" / "rowdata.parquet"
            if not parquet_file.exists():
                continue

            table = pq.read_table(parquet_file)
            ids = table["id"].to_pylist()
            passenger_counts = table["passenger_count"].to_pylist()

            for id_val, count in zip(ids, passenger_counts):
                if count > 2:
                    all_ids.add(id_val)

    return all_ids


def get_from_query():
    """从查询获取所有 passenger_count > 2 的 id"""
    # 使用 mysql 命令行工具执行查询
    cmd = [
        "mysql",
        "-h",
        "127.0.0.1",
        "-P",
        "3307",
        "-u",
        "root",
        "-pcalm",
        "-D",
        "default",
        "-N",  # 不输出列名
        "-B",  # batch 模式
        "-e",
        "SELECT id FROM taxi_trips WHERE passenger_count > 2 ORDER BY _nature LIMIT 999999",
    ]

    result = subprocess.run(cmd, capture_output=True, text=True)

    all_ids = set()
    for line in result.stdout.strip().split("\n"):
        if line:
            all_ids.add(line.strip())

    return all_ids


def main():
    print("=" * 80)
    print("对比 Parquet 文件和查询结果")
    print("=" * 80)
    print()

    print("📖 从 Parquet 文件读取数据...")
    parquet_ids = get_from_parquet()
    print(f"✅ Parquet 中 passenger_count > 2 的行数: {len(parquet_ids):,}")

    print()
    print("📖 从查询获取数据...")
    query_ids = get_from_query()
    print(f"✅ 查询返回的行数: {len(query_ids):,}")

    print()
    print("=" * 80)
    print("差异分析")
    print("=" * 80)

    missing_ids = parquet_ids - query_ids
    extra_ids = query_ids - parquet_ids

    if missing_ids:
        print(f"❌ 查询丢失了 {len(missing_ids):,} 条数据!")
        print(f"   丢失的 ID (显示前 50 个):")
        for id_val in sorted(list(missing_ids))[:50]:
            print(f"     - {id_val}")
    else:
        print(f"✅ 没有丢失数据")

    if extra_ids:
        print(f"⚠️  查询多返回了 {len(extra_ids):,} 条数据!")
        print(f"   多余的 ID (显示前 50 个):")
        for id_val in sorted(list(extra_ids))[:50]:
            print(f"     - {id_val}")
    else:
        print(f"✅ 没有多余数据")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"❌ 错误: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
