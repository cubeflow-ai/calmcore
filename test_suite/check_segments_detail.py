#!/usr/bin/env python3
"""
详细检查每个 partition 的 segment 分布
"""
import pyarrow.parquet as pq
from pathlib import Path


def check_segments_detail():
    """检查每个 partition 的 segment 细节"""

    tables_dir = Path("../data/tables/taxi_trips/partitions")

    print("=" * 80)
    print("检查所有 Partition 的 Segment 分布")
    print("=" * 80)
    print()

    total_rows = 0
    total_gt_2 = 0

    for partition_dir in sorted(tables_dir.iterdir()):
        if not partition_dir.is_dir():
            continue

        partition_name = partition_dir.name
        print(f"\n📂 {partition_name}")
        print("-" * 80)

        segments = []
        for segment_dir in sorted(partition_dir.iterdir()):
            if not segment_dir.is_dir() or not segment_dir.name.startswith("segment-"):
                continue

            parquet_file = segment_dir / "rowdata" / "rowdata.parquet"
            if not parquet_file.exists():
                print(f"  ⚠️  {segment_dir.name}: Parquet 文件不存在")
                continue

            table = pq.read_table(parquet_file)
            passenger_count = table["passenger_count"].to_pylist()

            rows = len(passenger_count)
            gt_2 = sum(1 for p in passenger_count if p > 2)

            segments.append({"name": segment_dir.name, "rows": rows, "gt_2": gt_2})

            total_rows += rows
            total_gt_2 += gt_2

        # 按名称排序并显示
        for seg in sorted(segments, key=lambda x: x["name"]):
            print(
                f"  {seg['name']}: {seg['rows']:,} 行, {seg['gt_2']:,} 行 (passenger_count > 2)"
            )

        partition_rows = sum(s["rows"] for s in segments)
        partition_gt_2 = sum(s["gt_2"] for s in segments)
        print(
            f"  小计: {partition_rows:,} 行, {partition_gt_2:,} 行 (passenger_count > 2)"
        )

    print()
    print("=" * 80)
    print(f"✅ 总计: {total_rows:,} 行")
    print(f"✅ passenger_count > 2: {total_gt_2:,} 行")
    print("=" * 80)


if __name__ == "__main__":
    check_segments_detail()
