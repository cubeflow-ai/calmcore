#!/usr/bin/env python3
"""
对比 Parquet 数据和查询结果,找出丢失的 28 行
"""

import pyarrow.parquet as pq
from pathlib import Path
import subprocess
import re


def get_parquet_ids():
    """从所有 Parquet 文件中读取 passenger_count > 2 的 ID"""
    data_dir = Path("../data/tables/taxi_trips/partitions")

    all_ids = set()

    for part_dir in sorted(data_dir.glob("partition-*")):
        for seg_dir in sorted(part_dir.glob("segment-*")):
            parquet_file = seg_dir / "rowdata" / "rowdata.parquet"

            if parquet_file.exists():
                table = pq.read_table(parquet_file, columns=["id", "passenger_count"])

                for i in range(table.num_rows):
                    passenger_count = table.column("passenger_count")[i].as_py()
                    if passenger_count is not None and passenger_count > 2:
                        trip_id = table.column("id")[i].as_py()
                        all_ids.add(trip_id)

    print(f"Parquet 中 passenger_count > 2 的行数: {len(all_ids)}")
    return all_ids


def get_query_ids():
    """从查询结果中读取所有 ID"""
    cmd = 'mysql -h 127.0.0.1 -P 3307 -uroot -pcalm test -N -e "SELECT id FROM taxi_trips WHERE passenger_count > 2" 2>/dev/null'

    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)

    ids = set()
    for line in result.stdout.strip().split("\n"):
        if line:
            ids.add(line.strip())

    print(f"查询返回的行数: {len(ids)}")
    return ids


def main():
    print("🔍 开始对比...")
    print()

    parquet_ids = get_parquet_ids()
    query_ids = get_query_ids()

    missing_ids = parquet_ids - query_ids
    extra_ids = query_ids - parquet_ids

    print()
    print(f"📊 统计:")
    print(f"  Parquet: {len(parquet_ids)} 行")
    print(f"  查询:    {len(query_ids)} 行")
    print(f"  丢失:    {len(missing_ids)} 行")
    print(f"  多余:    {len(extra_ids)} 行")

    if missing_ids:
        print()
        print(f"⚠️  丢失的 ID (前 30 个):")
        for idx, trip_id in enumerate(sorted(missing_ids)[:30], 1):
            print(f"  {idx}. {trip_id}")

    if extra_ids:
        print()
        print(f"⚠️  多余的 ID (前 30 个):")
        for idx, trip_id in enumerate(sorted(extra_ids)[:30], 1):
            print(f"  {idx}. {trip_id}")


if __name__ == "__main__":
    main()
