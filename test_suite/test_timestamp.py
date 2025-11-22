#!/usr/bin/env python3
"""测试时间戳转换"""

import pandas as pd
from datetime import datetime

# 测试不同的时间戳转换
test_date = "2024-01-15 08:30:00"
print(f"原始日期字符串: {test_date}")
print()

# 方法1: Pandas 转换
dt = pd.to_datetime(test_date)
print(f"Pandas datetime: {dt}")
print(f"  类型: {type(dt)}")

# 转换为纳秒时间戳
ts_ns = dt.value  # 或 dt.astype('int64')
print(f"  纳秒时间戳: {ts_ns} ({len(str(ts_ns))} 位)")

# 转换为毫秒
ts_ms = ts_ns // 10**6
print(f"  毫秒时间戳: {ts_ms} ({len(str(ts_ms))} 位)")

# 转换为秒
ts_s = ts_ns // 10**9
print(f"  秒时间戳: {ts_s} ({len(str(ts_s))} 位)")

print()

# 验证：从毫秒时间戳转回日期
dt_from_ms = datetime.fromtimestamp(ts_ms / 1000)
print(f"从毫秒时间戳转回: {dt_from_ms}")

print()
print("=" * 60)
print("你的问题时间戳分析:")
print("=" * 60)

# 你看到的错误时间戳
wrong_ts = 1699535699  # 对应 1970-01-20 18:05:35.699
print(f"错误的时间戳: {wrong_ts} ({len(str(wrong_ts))} 位)")
wrong_date = datetime.fromtimestamp(wrong_ts / 1000)
print(f"  对应日期: {wrong_date}")

print()
print("可能的原因:")
print(f"  1. 如果原始是纳秒，除以 10^6 得到毫秒: {wrong_ts} ✓")
print(f"  2. 但这个值太小，说明原始纳秒值就不对")
print(f"  3. 正确的 2024 年毫秒时间戳应该是: {ts_ms}")
print(f"  4. 差距: {ts_ms / wrong_ts:.1f} 倍")
