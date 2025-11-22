#!/usr/bin/env python3
"""诊断 Parquet 文件中的时间戳"""

import pandas as pd
from pathlib import Path
import sys

# 查找 parquet 文件
dataset_dir = Path("datasets")
parquet_files = list(dataset_dir.glob("yellow_tripdata_*.parquet"))

if not parquet_files:
    print("❌ No parquet files found in datasets/")
    sys.exit(1)

print(f"Found {len(parquet_files)} parquet file(s)\n")

# 读取第一个文件
parquet_file = parquet_files[0]
print(f"📁 Reading: {parquet_file.name}")
print("=" * 70)

df = pd.read_parquet(parquet_file)

# 检查时间列
time_cols = [col for col in df.columns if 'datetime' in col.lower() or 'time' in col.lower()]
print(f"\n⏰ Time columns found: {time_cols}\n")

for col in time_cols:
    print(f"Column: {col}")
    print(f"  Type: {df[col].dtype}")
    print(f"  Sample values:")
    for i in range(min(3, len(df))):
        val = df[col].iloc[i]
        print(f"    [{i}] {val} (type: {type(val).__name__})")
        
        # 如果是 datetime 类型，转换为时间戳
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            ts_ns = pd.to_datetime(val).value
            ts_ms = ts_ns // 10**6
            ts_s = ts_ns // 10**9
            print(f"        → 纳秒: {ts_ns} ({len(str(ts_ns))} 位)")
            print(f"        → 毫秒: {ts_ms} ({len(str(ts_ms))} 位)")
            print(f"        → 秒: {ts_s} ({len(str(ts_s))} 位)")
    print()

# 测试转换
print("=" * 70)
print("🔄 Testing conversion:")
print("=" * 70)

if 'tpep_pickup_datetime' in df.columns:
    col = 'tpep_pickup_datetime'
    original = df[col].iloc[0]
    print(f"\n原始值: {original}")
    print(f"原始类型: {df[col].dtype}")
    
    # 转换
    dt_series = pd.to_datetime(df[col])
    ts_ms = (dt_series.astype('int64') // 10**6).astype('int64')
    
    print(f"\n转换后的毫秒时间戳: {ts_ms.iloc[0]}")
    print(f"位数: {len(str(ts_ms.iloc[0]))}")
    
    # 验证
    from datetime import datetime
    dt_back = datetime.fromtimestamp(ts_ms.iloc[0] / 1000)
    print(f"转换回日期: {dt_back}")
    
    if ts_ms.iloc[0] < 10**12:
        print("\n❌ ERROR: 时间戳太小！会显示为 1970 年代的日期")
    else:
        print("\n✅ OK: 时间戳看起来正确")
