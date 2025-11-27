#!/usr/bin/env python3
"""
加载开放数据集到本地 MySQL 数据库

支持的数据集：
- NYC Taxi (Parquet 格式)

依赖：
    pip install pymysql pandas pyarrow

使用方法：
    python3 load_to_mysql.py nyc-taxi [--limit 100000]
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


def connect_db(
    host="127.0.0.1", port=3306, user="root", password="ansjsun", database=None
):
    """连接数据库"""
    try:
        conn = pymysql.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            charset="utf8mb4",
        )
        return conn
    except Exception as e:
        print_colored(Colors.RED, f"❌ Failed to connect: {e}")
        sys.exit(1)


def create_database_if_not_exists(conn, db_name="calm_test"):
    """创建数据库（如果不存在）"""
    cursor = conn.cursor()
    try:
        cursor.execute(
            f"CREATE DATABASE IF NOT EXISTS {db_name} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
        )
        print_colored(Colors.GREEN, f"✓ Database '{db_name}' ready")
    except Exception as e:
        print_colored(Colors.RED, f"❌ Failed to create database: {e}")
        return False
    finally:
        cursor.close()
    return True


def drop_table_if_exists(conn, table_name):
    """删除表（如果存在）"""
    cursor = conn.cursor()
    try:
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        conn.commit()
        print_colored(Colors.GREEN, f"✓ Table '{table_name}' dropped (if existed)")
    except Exception as e:
        print_colored(Colors.YELLOW, f"⚠️  Warning: {e}")
    finally:
        cursor.close()


def create_taxi_table(conn):
    """创建 taxi_trips 表"""
    cursor = conn.cursor()

    create_table_sql = """
    CREATE TABLE IF NOT EXISTS taxi_trips (
        id VARCHAR(50) PRIMARY KEY,
        pickup_datetime BIGINT NOT NULL,
        dropoff_datetime BIGINT NOT NULL,
        passenger_count INT DEFAULT 0,
        trip_distance DOUBLE DEFAULT 0,
        fare_amount DOUBLE DEFAULT 0,
        tip_amount DOUBLE DEFAULT 0,
        total_amount DOUBLE DEFAULT 0,
        payment_type INT DEFAULT 0,
        pickup_location_id INT DEFAULT 0,
        dropoff_location_id INT DEFAULT 0,
        INDEX idx_pickup_datetime (pickup_datetime),
        INDEX idx_dropoff_datetime (dropoff_datetime),
        INDEX idx_payment_type (payment_type),
        INDEX idx_pickup_location (pickup_location_id),
        INDEX idx_dropoff_location (dropoff_location_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """

    try:
        cursor.execute(create_table_sql)
        conn.commit()
        print_colored(Colors.GREEN, "✓ Table 'taxi_trips' created")
        return True
    except Exception as e:
        print_colored(Colors.RED, f"❌ Failed to create table: {e}")
        return False
    finally:
        cursor.close()


def load_nyc_taxi(conn, limit=None):
    """加载 NYC Taxi 数据集到 MySQL"""
    print_colored(Colors.BLUE, "\n📦 Loading NYC Taxi dataset to MySQL...")

    # 查找 parquet 文件
    dataset_dir = Path("datasets")
    parquet_files = list(dataset_dir.glob("yellow_tripdata_*.parquet"))

    if not parquet_files:
        print_colored(Colors.RED, "❌ No parquet files found in datasets/")
        print("Run: cd .. && ./download_dataset.sh nyc-taxi small")
        return False

    print(f"Found {len(parquet_files)} parquet file(s)")

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
            if "pickup_datetime" in df.columns:
                dt_series = pd.to_datetime(df["pickup_datetime"])
                df["pickup_datetime"] = (dt_series.astype("int64") // 10**3).astype(
                    "int64"
                )

            if "dropoff_datetime" in df.columns:
                dt_series = pd.to_datetime(df["dropoff_datetime"])
                df["dropoff_datetime"] = (dt_series.astype("int64") // 10**3).astype(
                    "int64"
                )

            # 验证时间戳
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

                if sample_ts < 10**12:
                    print(f"  ⚠️  ERROR: Timestamp too small!")
                elif sample_ts > 4 * 10**12:
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

            # 批量插入
            num_batches = (len(df) + batch_size - 1) // batch_size

            for batch_idx in range(num_batches):
                start_idx = batch_idx * batch_size
                end_idx = min(start_idx + batch_size, len(df))
                batch_df = df.iloc[start_idx:end_idx]

                # 构建 INSERT 语句
                values = []
                for idx, row in enumerate(batch_df.itertuples(index=False)):
                    # 调试：打印第一行的时间戳
                    if batch_idx == 0 and idx == 0:
                        print(f"  🔍 First row timestamp debug:")
                        print(
                            f"     pickup_datetime = {int(row.pickup_datetime)} ({len(str(int(row.pickup_datetime)))} digits)"
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
                    # 每 10 批 commit 一次
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

            # 最后一次 commit
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
    parser = argparse.ArgumentParser(
        description="Load dataset into local MySQL database"
    )
    parser.add_argument("dataset", choices=["nyc-taxi"], help="Dataset to load")
    parser.add_argument("--limit", type=int, help="Limit number of rows to load")
    parser.add_argument("--host", default="127.0.0.1", help="Database host")
    parser.add_argument("--port", type=int, default=3306, help="MySQL port")
    parser.add_argument("--user", default="root", help="MySQL user")
    parser.add_argument("--password", default="ansjsun", help="MySQL password")
    parser.add_argument("--database", default="calm_test", help="Database name")

    args = parser.parse_args()

    print_colored(Colors.BLUE, "=== Dataset Loader (MySQL) ===\n")
    print(f"Dataset: {args.dataset}")
    if args.limit:
        print(f"Limit: {args.limit:,} rows")
    print(f"MySQL: {args.host}:{args.port}")
    print(f"Database: {args.database}")
    print(f"User: {args.user}\n")

    # 先连接到 MySQL（不指定数据库）
    conn = connect_db(args.host, args.port, args.user, args.password, database=None)
    print_colored(Colors.GREEN, "✓ Connected to MySQL\n")

    # 创建数据库
    if not create_database_if_not_exists(conn, args.database):
        conn.close()
        return 1

    # 切换到目标数据库
    conn.close()
    conn = connect_db(
        args.host, args.port, args.user, args.password, database=args.database
    )
    print_colored(Colors.GREEN, f"✓ Using database '{args.database}'\n")

    success = False

    if args.dataset == "nyc-taxi":
        # 删除旧表
        print_colored(Colors.BLUE, "🗑️  Dropping existing table if exists...")
        drop_table_if_exists(conn, "taxi_trips")

        # 创建表
        print_colored(Colors.BLUE, "\n📋 Creating table 'taxi_trips'...")
        if not create_taxi_table(conn):
            conn.close()
            return 1

        # 加载数据
        success = load_nyc_taxi(conn, args.limit)

    conn.close()

    if success:
        print_colored(Colors.GREEN, "\n🎉 Dataset loaded successfully to MySQL!")
        print("\nNext steps:")
        print(
            f"  mysql -h{args.host} -P{args.port} -u{args.user} -p{args.password} {args.database}"
        )
        print(f"  mysql> SELECT COUNT(*) FROM taxi_trips;")
        print(f"  mysql> SELECT * FROM taxi_trips LIMIT 10;")
    else:
        print_colored(Colors.RED, "\n❌ Failed to load dataset")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
