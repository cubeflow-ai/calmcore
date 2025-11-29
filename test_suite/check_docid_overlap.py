#!/usr/bin/env python3
"""
检查不同 partition 的 doc_id 是否有重叠
"""

import pyarrow.parquet as pq
import os
from pathlib import Path


def check_doc_id_ranges():
    """检查所有 partition 的 doc_id 范围"""

    data_dir = Path("../data/tables/taxi_trips/partitions")

    partitions = {}

    # 遍历所有 partition
    for part_dir in sorted(data_dir.glob("partition-*")):
        part_name = part_dir.name
        print(f"\n=== {part_name} ===")

        # 遍历所有 segment
        for seg_dir in sorted(part_dir.glob("segment-*")):
            seg_name = seg_dir.name
            parquet_file = seg_dir / "rowdata" / "rowdata.parquet"

            if parquet_file.exists():
                # 读取 Parquet 文件的 id 列
                table = pq.read_table(parquet_file, columns=["id"])
                ids = table.column("id").to_pylist()

                if ids:
                    min_id = min(ids)
                    max_id = max(ids)
                    print(f"  {seg_name}: id范围 {min_id} - {max_id} ({len(ids)} 行)")

                    # 记录范围
                    if part_name not in partitions:
                        partitions[part_name] = []
                    partitions[part_name].append((min_id, max_id, len(ids)))

    # 检查是否有重叠
    print("\n\n=== 检查 doc_id 重叠 ===")
    all_ranges = []
    for part_name, ranges in partitions.items():
        for min_id, max_id, count in ranges:
            all_ranges.append((part_name, min_id, max_id, count))

    # 按 min_id 排序
    all_ranges.sort(key=lambda x: x[1])

    overlaps = []
    for i in range(len(all_ranges) - 1):
        part1, min1, max1, count1 = all_ranges[i]
        part2, min2, max2, count2 = all_ranges[i + 1]

        if max1 >= min2:
            overlap_start = min2
            overlap_end = min(max1, max2)
            overlap_count = overlap_end - overlap_start + 1
            overlaps.append((part1, part2, overlap_start, overlap_end, overlap_count))
            print(f"⚠️  重叠: {part1} ({min1}-{max1}) 和 {part2} ({min2}-{max2})")
            print(
                f"   重叠范围: {overlap_start} - {overlap_end} ({overlap_count} 个 ID)"
            )

    if not overlaps:
        print("✅ 没有发现 doc_id 重叠")
    else:
        total_overlap = sum(o[4] for o in overlaps)
        print(f"\n总共 {len(overlaps)} 处重叠,重叠 ID 总数: {total_overlap}")

    return overlaps


if __name__ == "__main__":
    overlaps = check_doc_id_ranges()
