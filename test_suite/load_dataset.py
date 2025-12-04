#!/usr/bin/env python3
"""
加载开放数据集到 Calm 数据库

支持的数据集：
- NYC Taxi (Parquet 格式)
- 自定义 CSV/JSON

依赖：
    pip install pymysql pandas pyarrow

使用方法：
    python3 load_dataset.py nyc-taxi [--limit 100000]
"""

import sys
import time
import argparse
import pymysql
import pandas as pd
from pathlib import Path


# 颜色
class Colors:
    BLUE = "\033[0;34m"
    GREEN = "\033[0;32m"
    YELLOW = "\033[1;33m"
    RED = "\033[0;31m"
    NC = "\033[0m"


def print_colored(color, text):
    print(f"{color}{text}{Colors.NC}")


def connect_db(host="127.0.0.1", port=3306, user="root", database="default"):
    """连接数据库"""
    try:
        conn = pymysql.connect(
            host=host, port=port, user=user, password="calm", database=database
        )
        return conn
    except Exception as e:
        print_colored(Colors.RED, f"❌ Failed to connect: {e}")
        sys.exit(1)


def drop_table_graphql(table_name, host="127.0.0.1", port=9567):
    """使用 GraphQL 删除表"""
    import requests

    graphql_url = f"http://{host}:{port}/graphql"

    mutation = f"""
    mutation {{
        dropTable(name: "{table_name}")
    }}
    """

    try:
        response = requests.post(graphql_url, json={"query": mutation})
        if response.status_code == 200:
            result = response.json()
            if "errors" in result:
                # 如果表不存在，忽略错误
                error_msg = result["errors"][0]["message"]
                if (
                    "not found" in error_msg.lower()
                    or "does not exist" in error_msg.lower()
                ):
                    return True
                raise Exception(error_msg)
            return True
        else:
            raise Exception(f"HTTP {response.status_code}: {response.text}")
    except Exception as e:
        # 如果是连接错误或表不存在，返回 True
        error_str = str(e).lower()
        if "not found" in error_str or "does not exist" in error_str:
            return True
        raise Exception(f"GraphQL request failed: {e}")


def create_table_graphql(host="127.0.0.1", port=9567):
    """使用 GraphQL 创建表"""
    import requests

    graphql_url = f"http://{host}:{port}/graphql"

    mutation = """
    mutation {
        createTable(input: {
            name: "taxi_trips"
            primaryKey: "id"
            partitionStrategy: {
                strategyType: PK_HASH
                numPartitions: 4
            }
            fields: [
                { name: "id", fieldType: KEYWORD, indexed: true }
                { name: "pickup_datetime", fieldType: TIMESTAMP, indexed: true }
                { name: "dropoff_datetime", fieldType: TIMESTAMP, indexed: true }
                { name: "passenger_count", fieldType: I64, indexed: true }
                { name: "trip_distance", fieldType: F64, indexed: false }
                { name: "fare_amount", fieldType: F64, indexed: false }
                { name: "tip_amount", fieldType: F64, indexed: false }
                { name: "total_amount", fieldType: F64, indexed: false }
                { name: "payment_type", fieldType: I64, indexed: true }
                { name: "pickup_location_id", fieldType: I64, indexed: true }
                { name: "dropoff_location_id", fieldType: I64, indexed: true }
            ]
        }) {
            name
            partitionCount
        }
    }
    """

    try:
        response = requests.post(graphql_url, json={"query": mutation})
        if response.status_code == 200:
            result = response.json()
            if "errors" in result:
                raise Exception(result["errors"][0]["message"])
            return True
        else:
            raise Exception(f"HTTP {response.status_code}: {response.text}")
    except Exception as e:
        raise Exception(f"GraphQL request failed: {e}")


def load_nyc_taxi(conn, limit=None, graphql_port=8000):
    """加载 NYC Taxi 数据集"""
    print_colored(Colors.BLUE, "\n📦 Loading NYC Taxi dataset...")

    # 查找 parquet 文件
    dataset_dir = Path("datasets")
    parquet_files = list(dataset_dir.glob("yellow_tripdata_*.parquet"))

    if not parquet_files:
        print_colored(Colors.RED, "❌ No parquet files found in datasets/")
        print("Run: ./download_dataset.sh nyc-taxi small")
        return False

    print(f"Found {len(parquet_files)} parquet file(s)")

    # 删除已存在的表（如果存在）
    print_colored(
        Colors.BLUE, "\n� ️  Dropping existing table 'taxi_trips' if exists..."
    )
    try:
        drop_table_graphql("taxi_trips", port=graphql_port)
        print_colored(Colors.GREEN, "✓ Table dropped (or didn't exist)")
    except Exception as e:
        print_colored(Colors.YELLOW, f"⚠️  Warning: {e}")

    # 创建表（使用 GraphQL）
    print_colored(Colors.BLUE, "\n📋 Creating table 'taxi_trips' via GraphQL...")

    try:
        create_table_graphql(port=graphql_port)
        print_colored(Colors.GREEN, "✓ Table created")
    except Exception as e:
        print_colored(Colors.RED, f"❌ Failed to create table: {e}")
        print_colored(
            Colors.YELLOW,
            "Make sure GraphQL server is running on port {}".format(graphql_port),
        )
        return False

    cursor = conn.cursor()

    # 加载数据
    print_colored(Colors.BLUE, "\n📝 Loading data...")

    total_loaded = 0
    batch_size = 5000

    start_time = time.time()

    for parquet_file in parquet_files:
        print(f"\nProcessing: {parquet_file.name}")

        # 读取 parquet 文件
        try:
            df = pd.read_parquet(parquet_file)
            print(f"  Rows in file: {len(df):,}")

            # 选择需要的列
            columns_map = {
                "tpep_pickup_datetime": "pickup_datetime",
                "tpep_dropoff_datetime": "dropoff_datetime",
                "passenger_count": "passenger_count",
                "trip_distance": "trip_distance",
                "fare_amount": "fare_amount",
                "tip_amount": "tip_amount",
                "total_amount": "total_amount",
                "payment_type": "payment_type",
                "PULocationID": "pickup_location_id",
                "DOLocationID": "dropoff_location_id",
            }

            # 检查列是否存在
            available_cols = {k: v for k, v in columns_map.items() if k in df.columns}
            df = df[list(available_cols.keys())].copy()
            df.rename(columns=available_cols, inplace=True)

            # 转换时间戳为毫秒
            # Pandas datetime64[ns] 是纳秒级，需要除以 10^6 转换为毫秒
            if "pickup_datetime" in df.columns:
                # 先转换为 datetime，再转为纳秒时间戳，最后转为毫秒
                dt_series = pd.to_datetime(df["pickup_datetime"])
                df["pickup_datetime"] = (dt_series.astype("int64") // 10**3).astype(
                    "int64"
                )

            if "dropoff_datetime" in df.columns:
                dt_series = pd.to_datetime(df["dropoff_datetime"])
                df["dropoff_datetime"] = (dt_series.astype("int64") // 10**3).astype(
                    "int64"
                )

            # 验证时间戳（应该是 13 位数字，表示 2001-2099 年之间）
            if "pickup_datetime" in df.columns and len(df) > 0:
                sample_ts = int(df["pickup_datetime"].iloc[0])
                sample_date = (
                    pd.to_datetime(df.iloc[0]["tpep_pickup_datetime"])
                    if "tpep_pickup_datetime" in df.columns
                    else None
                )
                print(
                    f"  📅 Sample: {sample_date} → {sample_ts} ms ({len(str(sample_ts))} digits)"
                )

                # 检查时间戳是否合理（2001-2099年之间）
                if sample_ts < 10**12:  # 小于 2001-09-09
                    print(
                        f"  ⚠️  ERROR: Timestamp too small! Expected 13 digits, got {len(str(sample_ts))}"
                    )
                    print(f"  ⚠️  This will result in dates around 1970!")
                elif sample_ts > 4 * 10**12:  # 大于 2096年
                    print(f"  ⚠️  Warning: Timestamp seems too large")

            # 填充缺失值
            df = df.fillna(0)

            # 添加 ID
            df["id"] = [f"trip_{total_loaded + i}" for i in range(len(df))]

            # 应用 limit
            if limit and total_loaded + len(df) > limit:
                remaining = limit - total_loaded
                df = df.head(remaining)
                print(f"  Limiting to {remaining} rows (reached limit of {limit})")

            # 批量插入（优化版）
            num_batches = (len(df) + batch_size - 1) // batch_size

            for batch_idx in range(num_batches):
                start_idx = batch_idx * batch_size
                end_idx = min(start_idx + batch_size, len(df))
                batch_df = df.iloc[start_idx:end_idx]

                # 构建 INSERT 语句（使用 itertuples 代替 iterrows，快 10-100 倍）
                values = []
                for idx, row in enumerate(batch_df.itertuples(index=False)):
                    # 调试：打印第一行的时间戳
                    if batch_idx == 0 and idx == 0:
                        print(f"  🔍 First row timestamp debug:")
                        print(
                            f"     pickup_datetime = {int(row.pickup_datetime)} ({len(str(int(row.pickup_datetime)))} digits)"
                        )
                        print(
                            f"     Expected: 13 digits for milliseconds (e.g., 1705307400000)"
                        )

                    value_str = (
                        f"('{row.id}', {int(row.pickup_datetime)}, {int(row.dropoff_datetime)}, "
                        f"{int(getattr(row, 'passenger_count', 0))}, {float(getattr(row, 'trip_distance', 0))}, "
                        f"{float(getattr(row, 'fare_amount', 0))}, {float(getattr(row, 'tip_amount', 0))}, "
                        f"{float(getattr(row, 'total_amount', 0))}, {int(getattr(row, 'payment_type', 0))}, "
                        f"{int(getattr(row, 'pickup_location_id', 0))}, {int(getattr(row, 'dropoff_location_id', 0))})"
                    )
                    values.append(value_str)

                insert_sql = (
                    f"INSERT INTO taxi_trips (id, pickup_datetime, dropoff_datetime, passenger_count, "
                    f"trip_distance, fare_amount, tip_amount, total_amount, payment_type, "
                    f"pickup_location_id, dropoff_location_id) VALUES {','.join(values)}"
                )

                try:
                    cursor.execute(insert_sql)
                    # 每 10 批 commit 一次，而不是每批都 commit（性能提升 10 倍）
                    if (batch_idx + 1) % 10 == 0:
                        conn.commit()
                except Exception as e:
                    print_colored(Colors.RED, f"  ❌ Batch {batch_idx} failed: {e}")
                    conn.rollback()
                    continue

                total_loaded += len(batch_df)

                if (batch_idx + 1) % 10 == 0:
                    elapsed = time.time() - start_time
                    rate = total_loaded / elapsed if elapsed > 0 else 0
                    print(f"  Progress: {total_loaded:,} rows ({rate:.0f} rows/sec)")

            cursor.execute("flush tables taxi_trips")
            # 最后一次 commit（确保所有数据都提交）
            conn.commit()

            # 检查是否达到 limit
            if limit and total_loaded >= limit:
                print(f"\n✓ Reached limit of {limit:,} rows")
                break

        except Exception as e:
            print_colored(Colors.RED, f"  ❌ Error processing file: {e}")
            conn.rollback()
            continue

    duration = time.time() - start_time
    rate = total_loaded / duration if duration > 0 else 0

    print_colored(
        Colors.GREEN,
        f"\n✓ Loaded {total_loaded:,} rows in {duration:.2f}s ({rate:.0f} rows/sec)",
    )

    cursor.close()
    return True


def main():
    parser = argparse.ArgumentParser(description="Load dataset into Calm database")
    parser.add_argument("dataset", choices=["nyc-taxi"], help="Dataset to load")
    parser.add_argument("--limit", type=int, help="Limit number of rows to load")
    parser.add_argument("--host", default="127.0.0.1", help="Database host")
    parser.add_argument("--port", type=int, default=3307, help="MySQL port")
    parser.add_argument(
        "--graphql-port", type=int, default=9567, help="GraphQL port for table creation"
    )

    args = parser.parse_args()

    print_colored(Colors.BLUE, "=== Dataset Loader ===\n")
    print(f"Dataset: {args.dataset}")
    if args.limit:
        print(f"Limit: {args.limit:,} rows")
    print(f"MySQL: {args.host}:{args.port}")
    print(f"GraphQL: {args.host}:{args.graphql_port}\n")

    conn = connect_db(args.host, args.port)
    print_colored(Colors.GREEN, "✓ Connected to MySQL\n")

    success = False

    if args.dataset == "nyc-taxi":
        success = load_nyc_taxi(conn, args.limit, args.graphql_port)

    conn.close()

    if success:
        print_colored(Colors.GREEN, "\n🎉 Dataset loaded successfully!")
        print("\nNext steps:")
        print("  python3 test_queries.py nyc-taxi")
    else:
        print_colored(Colors.RED, "\n❌ Failed to load dataset")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
