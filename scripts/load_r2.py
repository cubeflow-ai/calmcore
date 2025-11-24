"""
扫描目录中的 Parquet 文件并通过 GraphQL 加载到数据库

使用方法：
    python3 load_parquet_segments.py [--data-dir DIR] [--graphql-url URL] [--table TABLE] [--dry-run]

示例：
    python3 load_parquet_segments.py --data-dir /mnt/nvme_cfs/r2data/data --table r2api
"""

import os
import sys
import time
import argparse
from pathlib import Path
from datetime import datetime, timedelta
import requests


# 颜色
class Colors:
    BLUE = "\033[0;34m"
    GREEN = "\033[0;32m"
    YELLOW = "\033[1;33m"
    RED = "\033[0;31m"
    NC = "\033[0m"


def print_colored(color, text):
    print(f"{color}{text}{Colors.NC}")


def scan_parquet_files(data_dir, min_age_minutes=20):
    """
    扫描目录中的 parquet 文件

    Args:
        data_dir: 数据目录路径
        min_age_minutes: 文件最小年龄（分钟），只处理超过这个时间的文件

    Returns:
        list: [(partition_name, file_path), ...]
    """
    print_colored(Colors.BLUE, f"\nð Scanning directory: {data_dir}")
    print(f"   Filtering files older than {min_age_minutes} minutes")
    print(f"   Excluding *_tmp.parquet files\n")

    data_path = Path(data_dir)
    if not data_path.exists():
        print_colored(Colors.RED, f"❌ Directory not found: {data_dir}")
        return []

    current_time = time.time()
    cutoff_time = current_time - (min_age_minutes * 60)

    parquet_files = []
    skipped_tmp = 0
    skipped_recent = 0

    # 递归扫描所有子目录
    for root, dirs, files in os.walk(data_dir):
        # 获取相对于 data_dir 的路径作为 partition_name
        rel_path = os.path.relpath(root, data_dir)

        # 如果是根目录，跳过
        if rel_path == ".":
            continue

        partition_name = rel_path

        for filename in files:
            # 只处理 .parquet 文件
            if not filename.endswith(".parquet"):
                continue

            # 跳过临时文件
            if filename.endswith("_tmp.parquet"):
                skipped_tmp += 1
                continue

            file_path = os.path.join(root, filename)

            # 检查文件修改时间
            try:
                mtime = os.path.getmtime(file_path)

                # 只处理超过指定时间的文件
                if mtime > cutoff_time:
                    skipped_recent += 1
                    continue

                # 计算文件年龄
                age_minutes = (current_time - mtime) / 60

                parquet_files.append(
                    {
                        "partition_name": partition_name,
                        "file_path": file_path,
                        "filename": filename,
                        "age_minutes": age_minutes,
                        "mtime": mtime,
                    }
                )

            except OSError as e:
                print_colored(Colors.YELLOW, f"⚠️  Cannot access file: {file_path}: {e}")
                continue

    print_colored(Colors.GREEN, f"✓ Found {len(parquet_files)} parquet files")
    print(f"  Skipped {skipped_tmp} temporary files (*_tmp.parquet)")
    print(
        f"  Skipped {skipped_recent} recent files (< {min_age_minutes} minutes old)\n"
    )

    # 按 partition_name 和文件名排序
    parquet_files.sort(key=lambda x: (x["partition_name"], x["filename"]))

    return parquet_files


def load_segment_graphql(graphql_url, table, partition_name, file_path):
    """
    通过 GraphQL 加载 segment

    Args:
        graphql_url: GraphQL endpoint URL
        table: 表名
        partition_name: 分区 ID
        file_path: Parquet 文件路径

    Returns:
        dict: 响应结果
    """
    mutation = """
    mutation LoadSegmentFromFile($table: String!, $partitionName: String!, $filePath: String!) {
        loadSegment(input: {
            table: $table
            partitionName: $partitionName
            filePath: $filePath
            handlerType: REFERENCE
        }) {
            success
            documentsLoaded
            partitionName
            message
        }
    }
    """

    variables = {"table": table, "partitionName": partition_name, "filePath": file_path}

    try:
        response = requests.post(
            graphql_url, json={"query": mutation, "variables": variables}, timeout=30
        )

        if response.status_code == 200:
            result = response.json()

            if "errors" in result:
                return {"success": False, "error": result["errors"][0]["message"]}

            if "data" in result and "loadSegment" in result["data"]:
                return result["data"]["loadSegment"]

            return {"success": False, "error": "Unexpected response format"}
        else:
            return {
                "success": False,
                "error": f"HTTP {response.status_code}: {response.text}",
            }

    except requests.exceptions.Timeout:
        return {"success": False, "error": "Request timeout (30s)"}
    except Exception as e:
        return {"success": False, "error": str(e)}


def main():
    parser = argparse.ArgumentParser(
        description="Scan and load parquet segments into database",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Preview files (default mode)
  python3 load_parquet_segments.py

  # Actually load segments
  python3 load_parquet_segments.py --load

  # Custom table or directory
  python3 load_parquet_segments.py --table mytable --data-dir /other/path --load
        """,
    )

    parser.add_argument(
        "--data-dir",
        default="/mnt/nvme_cfs/r2data/data",
        help="Data directory to scan (default: /mnt/nvme_cfs/r2data/data)",
    )
    parser.add_argument("--table", default="r2api", help="Table name (default: r2api)")
    parser.add_argument(
        "--graphql-url",
        default="http://127.0.0.1:9567/graphql",
        help="GraphQL endpoint URL (default: http://127.0.0.1:9567/graphql)",
    )
    parser.add_argument(
        "--min-age",
        type=int,
        default=20,
        help="Minimum file age in minutes (default: 20)",
    )
    parser.add_argument(
        "--load",
        action="store_true",
        help="Actually load segments (default is preview only)",
    )
    parser.add_argument("--limit", type=int, help="Limit number of files to process")

    args = parser.parse_args()

    print_colored(Colors.BLUE, "=== Parquet Segment Loader ===\n")
    print(f"Data directory: {args.data_dir}")
    print(f"Table: {args.table}")
    print(f"GraphQL URL: {args.graphql_url}")
    print(f"Minimum age: {args.min_age} minutes")
    if not args.load:
        print_colored(
            Colors.YELLOW, "Mode: PREVIEW ONLY (use --load to actually load segments)"
        )
    else:
        print_colored(Colors.GREEN, "Mode: LOAD")
    if args.limit:
        print(f"Limit: {args.limit} files")

    # 扫描文件
    parquet_files = scan_parquet_files(args.data_dir, args.min_age)

    if not parquet_files:
        print_colored(Colors.YELLOW, "No files to process")
        return 0

    # 应用 limit
    if args.limit:
        parquet_files = parquet_files[: args.limit]
        print_colored(Colors.YELLOW, f"Limited to {len(parquet_files)} files\n")

    # 显示文件列表
    print_colored(Colors.BLUE, "Files to process:")
    print(f"{'Partition':<15} {'Filename':<40} {'Age (min)':<12}")
    print("-" * 70)

    for file_info in parquet_files[:10]:  # 只显示前 10 个
        print(
            f"{file_info['partition_name']:<15} {file_info['filename']:<40} {file_info['age_minutes']:<12.1f}"
        )

    if len(parquet_files) > 10:
        print(f"... and {len(parquet_files) - 10} more files")

    print()

    # 预览模式
    if not args.load:
        print_colored(
            Colors.YELLOW,
            "ð Preview complete. Use --load to actually load these segments.",
        )
        return 0

    # 确认加载
    print_colored(
        Colors.YELLOW,
        f"\n⚠️  About to load {len(parquet_files)} segments into table '{args.table}'",
    )
    try:
        response = input("Continue? [y/N]: ")
        if response.lower() not in ["y", "yes"]:
            print_colored(Colors.YELLOW, "Cancelled")
            return 0
    except KeyboardInterrupt:
        print_colored(Colors.YELLOW, "\nCancelled")
        return 0

    # 加载 segments
    print_colored(Colors.BLUE, f"\nð¥ Loading segments...\n")

    success_count = 0
    failed_count = 0
    total_docs = 0
    start_time = time.time()

    for idx, file_info in enumerate(parquet_files, 1):
        partition_name = file_info["partition_name"]
        file_path = file_info["file_path"]
        filename = file_info["filename"]

        print(
            f"[{idx}/{len(parquet_files)}] Loading {partition_name}/{filename}...",
            end=" ",
        )

        result = load_segment_graphql(
            args.graphql_url, args.table, partition_name, file_path
        )

        if result.get("success"):
            docs_loaded = result.get("documentsLoaded", 0)
            total_docs += docs_loaded
            success_count += 1
            print_colored(Colors.GREEN, f"✓ {docs_loaded} docs")
        else:
            failed_count += 1
            error = result.get("error", "Unknown error")
            print_colored(Colors.RED, f"✗ {error}")

        # 每 10 个文件显示进度
        if idx % 10 == 0:
            elapsed = time.time() - start_time
            rate = idx / elapsed if elapsed > 0 else 0
            print(
                f"  Progress: {idx}/{len(parquet_files)} files, {rate:.1f} files/sec, {total_docs:,} docs loaded\n"
            )

    # 总结
    elapsed = time.time() - start_time

    print_colored(Colors.BLUE, "\n=== Summary ===")
    print(f"Total files: {len(parquet_files)}")
    print_colored(Colors.GREEN, f"Success: {success_count}")
    if failed_count > 0:
        print_colored(Colors.RED, f"Failed: {failed_count}")
    print(f"Documents loaded: {total_docs:,}")
    print(f"Time: {elapsed:.2f}s ({len(parquet_files)/elapsed:.1f} files/sec)")

    if failed_count == 0:
        print_colored(Colors.GREEN, "\nð All segments loaded successfully!")
        return 0
    else:
        print_colored(Colors.YELLOW, f"\n⚠️  {failed_count} segments failed to load")
        return 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print_colored(Colors.YELLOW, "\n\n⚠️  Interrupted by user")
        sys.exit(130)
    except Exception as e:
        print_colored(Colors.RED, f"\n❌ Error: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
