#!/usr/bin/env python3
"""
加载 Wikipedia 数据集到 CalmCore 进行全文检索测试

功能：
  - 创建 wikipedia_articles 表（包含全文索引字段）
  - 批量插入 Wikipedia 文章
  - 每 100,000 条记录持久化一次
  - 显示加载进度和性能统计
"""

import json
import pymysql
import time
import sys
import os
from pathlib import Path


# 颜色
class Colors:
    BLUE = "\033[0;34m"
    GREEN = "\033[0;32m"
    YELLOW = "\033[1;33m"
    RED = "\033[0;31m"
    CYAN = "\033[0;36m"
    NC = "\033[0m"


def print_colored(color, message):
    print(f"{color}{message}{Colors.NC}")


def load_wikipedia_articles(
    data_file, batch_size=5000, commit_interval=10, persist_interval=100000
):
    """
    加载 Wikipedia 文章数据

    参数：
        data_file: JSONL 数据文件路径
        batch_size: 每批插入的行数
        commit_interval: 每 N 批 commit 一次
        persist_interval: 每 N 条记录持久化一次
    """

    print_colored(Colors.BLUE, "=== Wikipedia Articles Loader ===\n")

    # 检查文件
    if not os.path.exists(data_file):
        print_colored(Colors.RED, f"Error: File not found: {data_file}")
        print_colored(Colors.YELLOW, "Please run: ./download_wikipedia.sh first")
        return

    file_size = os.path.getsize(data_file) / (1024 * 1024)  # MB
    print_colored(Colors.CYAN, f"📁 Data file: {data_file}")
    print_colored(Colors.CYAN, f"📊 File size: {file_size:.2f} MB\n")

    # 连接数据库
    print_colored(Colors.CYAN, "🔌 Connecting to CalmCore...")
    try:
        conn = pymysql.connect(
            host="localhost",
            port=3307,
            user="root",
            password="",
            database="test",
            charset="utf8mb4",
        )
        cursor = conn.cursor()
        print_colored(Colors.GREEN, "✓ Connected\n")
    except Exception as e:
        print_colored(Colors.RED, f"❌ Connection failed: {e}")
        print_colored(Colors.YELLOW, "Make sure CalmCore server is running:")
        print_colored(Colors.YELLOW, "  cargo run --bin calm --release")
        return

    try:
        # 1. 删除旧表（如果存在）
        print_colored(Colors.CYAN, "🗑️  Dropping old table (if exists)...")
        try:
            cursor.execute("DROP TABLE IF EXISTS wikipedia_articles")
            conn.commit()
            print_colored(Colors.GREEN, "✓ Old table dropped\n")
        except Exception as e:
            print_colored(Colors.YELLOW, f"⚠️  Warning: {e}\n")

        # 2. 创建表
        print_colored(Colors.CYAN, "📋 Creating wikipedia_articles table...")
        create_table_sql = """
        CREATE TABLE wikipedia_articles (
            id VARCHAR PRIMARY KEY,
            title TEXT,
            content TEXT FULLTEXT,
            url VARCHAR,
            timestamp TIMESTAMP
        ) ENGINE=MemBTree PARTITION=HASH(id, 4)
        """
        cursor.execute(create_table_sql)
        conn.commit()
        print_colored(Colors.GREEN, "✓ Table created\n")

        # 3. 加载数据
        print_colored(Colors.CYAN, f"📥 Loading data from {data_file}...")
        print_colored(Colors.CYAN, f"   Batch size: {batch_size:,} rows")
        print_colored(
            Colors.CYAN, f"   Commit interval: every {commit_interval} batches"
        )
        print_colored(
            Colors.CYAN, f"   Persist interval: every {persist_interval:,} rows\n"
        )

        total_loaded = 0
        batch_count = 0
        last_persist = 0
        start_time = time.time()
        batch_start = time.time()

        batch = []

        with open(data_file, "r", encoding="utf-8") as f:
            for line_num, line in enumerate(f, 1):
                try:
                    article = json.loads(line.strip())

                    # 构建记录
                    record = (
                        article.get("id", str(line_num)),
                        article.get("title", ""),
                        article.get("text", ""),
                        article.get("url", ""),
                        "NOW()",  # 时间戳
                    )
                    batch.append(record)

                    # 批量插入
                    if len(batch) >= batch_size:
                        # 构建 INSERT 语句
                        values_str = ", ".join(
                            [
                                f"('{r[0]}', '{r[1].replace(chr(39), chr(39)*2)}', "
                                f"'{r[2].replace(chr(39), chr(39)*2)}', '{r[3]}', NOW())"
                                for r in batch
                            ]
                        )
                        insert_sql = f"INSERT INTO wikipedia_articles (id, title, content, url, timestamp) VALUES {values_str}"

                        try:
                            cursor.execute(insert_sql)
                            total_loaded += len(batch)
                            batch_count += 1
                            batch = []

                            # 定期 commit
                            if batch_count % commit_interval == 0:
                                conn.commit()
                                elapsed = time.time() - batch_start
                                rate = (batch_size * commit_interval) / elapsed
                                print_colored(
                                    Colors.GREEN,
                                    f"  ✓ Loaded {total_loaded:,} articles "
                                    f"({rate:.0f} articles/sec)",
                                )
                                batch_start = time.time()

                            # 定期持久化
                            if total_loaded - last_persist >= persist_interval:
                                print_colored(
                                    Colors.YELLOW,
                                    f"\n  💾 Persisting data at {total_loaded:,} articles...",
                                )
                                try:
                                    cursor.execute("FLUSH TABLES")
                                    conn.commit()
                                    last_persist = total_loaded
                                    print_colored(
                                        Colors.GREEN, f"  ✓ Persisted successfully"
                                    )
                                except Exception as e:
                                    print_colored(
                                        Colors.YELLOW, f"  ⚠️  Persist warning: {e}"
                                    )
                                print()

                        except Exception as e:
                            print_colored(
                                Colors.RED, f"❌ Insert error at line {line_num}: {e}"
                            )
                            batch = []
                            continue

                except json.JSONDecodeError as e:
                    print_colored(
                        Colors.YELLOW, f"⚠️  JSON parse error at line {line_num}: {e}"
                    )
                    continue

        # 插入剩余数据
        if batch:
            values_str = ", ".join(
                [
                    f"('{r[0]}', '{r[1].replace(chr(39), chr(39)*2)}', "
                    f"'{r[2].replace(chr(39), chr(39)*2)}', '{r[3]}', NOW())"
                    for r in batch
                ]
            )
            insert_sql = f"INSERT INTO wikipedia_articles (id, title, content, url, timestamp) VALUES {values_str}"
            cursor.execute(insert_sql)
            total_loaded += len(batch)
            conn.commit()

        # 最终持久化
        if total_loaded - last_persist > 0:
            print_colored(
                Colors.YELLOW, f"\n  💾 Final persist at {total_loaded:,} articles..."
            )
            cursor.execute("FLUSH TABLES")
            conn.commit()
            print_colored(Colors.GREEN, f"  ✓ Persisted successfully\n")

        # 4. 显示统计
        total_time = time.time() - start_time
        avg_rate = total_loaded / total_time if total_time > 0 else 0

        print_colored(Colors.GREEN, "\n✅ Loading completed!\n")
        print_colored(Colors.CYAN, "📊 Statistics:")
        print(f"   Total articles: {total_loaded:,}")
        print(f"   Total time: {total_time:.2f} seconds")
        print(f"   Average rate: {avg_rate:.0f} articles/sec")
        print()

        # 5. 验证数据
        print_colored(Colors.CYAN, "🔍 Verifying data...")
        cursor.execute("SELECT COUNT(*) FROM wikipedia_articles")
        count = cursor.fetchone()[0]
        print_colored(Colors.GREEN, f"✓ Table contains {count:,} articles\n")

        # 6. 显示示例
        print_colored(Colors.CYAN, "📄 Sample articles:")
        cursor.execute("SELECT id, title FROM wikipedia_articles LIMIT 5")
        for row in cursor.fetchall():
            print(f"   {row[0]}: {row[1][:60]}...")
        print()

        print_colored(Colors.GREEN, "🎉 Ready for full-text search testing!")
        print_colored(Colors.CYAN, "\nNext steps:")
        print("   python3 test_wikipedia.py")

    except Exception as e:
        print_colored(Colors.RED, f"\n❌ Error: {e}")
        import traceback

        traceback.print_exc()

    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    data_file = "datasets/wikipedia/wikipedia_articles.jsonl"

    if len(sys.argv) > 1:
        data_file = sys.argv[1]

    load_wikipedia_articles(data_file)
