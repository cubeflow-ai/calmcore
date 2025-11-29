#!/usr/bin/env python3
"""
统计所有 Parquet 文件中 passenger_count > 2 的行数
"""
import pyarrow.parquet as pq
import os
from pathlib import Path


def count_passenger_gt_2(data_path):
    """遍历所有 partition 和 segment,统计 passenger_count > 2 的行数"""

    tables_dir = Path(data_path) / "tables" / "taxi_trips" / "partitions"

    if not tables_dir.exists():
        print(f"❌ 目录不存在: {tables_dir}")
        return

    total_count = 0
    total_rows = 0
    partition_details = []

    # 遍历所有 partition
    for partition_dir in sorted(tables_dir.iterdir()):
        if not partition_dir.is_dir():
            continue

        partition_name = partition_dir.name
        partition_count = 0
        partition_rows = 0
        segment_details = []

        # 遍历所有 segment
        for segment_dir in sorted(partition_dir.iterdir()):
            if not segment_dir.is_dir() or not segment_dir.name.startswith("segment-"):
                continue

            parquet_file = segment_dir / "rowdata" / "rowdata.parquet"
            if not parquet_file.exists():
                continue

            # 读取 Parquet 文件
            table = pq.read_table(parquet_file)
            passenger_count = table["passenger_count"].to_pylist()

            # 统计 passenger_count > 2
            segment_count = sum(1 for p in passenger_count if p > 2)
            segment_rows = len(passenger_count)

            partition_count += segment_count
            partition_rows += segment_rows

            segment_details.append(
                {
                    "segment": segment_dir.name,
                    "total_rows": segment_rows,
                    "gt_2_count": segment_count,
                }
            )

        total_count += partition_count
        total_rows += partition_rows

        partition_details.append(
            {
                "partition": partition_name,
                "total_rows": partition_rows,
                "gt_2_count": partition_count,
                "segments": segment_details,
            }
        )

    # 打印结果
    print("=" * 80)
    print("遍历所有 Parquet 文件统计 passenger_count > 2")
    print("=" * 80)
    print()

    for partition_info in partition_details:
        print(f"📂 {partition_info['partition']}")
        print(f"   总行数: {partition_info['total_rows']:,}")
        print(f"   passenger_count > 2: {partition_info['gt_2_count']:,}")
        print()

        for segment_info in partition_info["segments"]:
            print(f"   📦 {segment_info['segment']}")
            print(f"      总行数: {segment_info['total_rows']:,}")
            print(f"      passenger_count > 2: {segment_info['gt_2_count']:,}")
        print()

    print("=" * 80)
    print(f"✅ 总计:")
    print(f"   总行数: {total_rows:,}")
    print(f"   passenger_count > 2 的行数: {total_count:,}")
    print("=" * 80)
    print()
    print(f"结论: passenger_count > 2 的实际行数是 {total_count:,}")

    return total_count


if __name__ == "__main__":
    data_path = "../data"
    count_passenger_gt_2(data_path)
