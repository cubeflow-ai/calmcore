#!/usr/bin/env python3
"""
测试 Wikipedia 全文检索功能

测试场景：
  - 关键词搜索（text 函数）
  - 短语搜索（phrase 函数）
  - BM25 相关性排序
  - 复杂查询组合
"""

import pymysql
import time
import sys


# 颜色
class Colors:
    BLUE = "\033[0;34m"
    GREEN = "\033[0;32m"
    YELLOW = "\033[1;33m"
    RED = "\033[0;31m"
    CYAN = "\033[0;36m"
    MAGENTA = "\033[0;35m"
    NC = "\033[0m"


def print_colored(color, message):
    print(f"{color}{message}{Colors.NC}")


def execute_query(cursor, query, description):
    """执行查询并显示结果"""
    print_colored(Colors.CYAN, f"\n{'='*80}")
    print_colored(Colors.MAGENTA, f"📝 {description}")
    print_colored(Colors.BLUE, f"SQL: {query}")
    print_colored(Colors.CYAN, f"{'='*80}")

    try:
        start_time = time.time()
        cursor.execute(query)
        results = cursor.fetchall()
        elapsed = time.time() - start_time

        print_colored(
            Colors.GREEN, f"\n✓ Found {len(results)} results in {elapsed*1000:.2f}ms\n"
        )

        if results:
            # 显示前 5 条结果
            for i, row in enumerate(results[:5], 1):
                if len(row) >= 3:  # id, title, score
                    article_id, title, score = (
                        row[0],
                        row[1],
                        row[2] if len(row) > 2 else "N/A",
                    )
                    print_colored(Colors.YELLOW, f"  {i}. [{score}] {title[:70]}")
                    print(f"     ID: {article_id}")
                else:
                    print_colored(Colors.YELLOW, f"  {i}. {row}")
                print()

            if len(results) > 5:
                print_colored(Colors.CYAN, f"  ... and {len(results) - 5} more results")
        else:
            print_colored(Colors.YELLOW, "  No results found")

        return results

    except Exception as e:
        print_colored(Colors.RED, f"❌ Query failed: {e}")
        import traceback

        traceback.print_exc()
        return []


def test_wikipedia_search():
    """测试 Wikipedia 全文检索"""

    print_colored(Colors.BLUE, "\n=== Wikipedia Full-Text Search Test ===\n")

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
        return

    try:
        # 0. 检查表状态
        print_colored(Colors.CYAN, "🔍 Checking table status...")
        cursor.execute("SELECT COUNT(*) FROM wikipedia_articles")
        total_count = cursor.fetchone()[0]
        print_colored(Colors.GREEN, f"✓ Table contains {total_count:,} articles\n")

        if total_count == 0:
            print_colored(
                Colors.RED, "❌ No data in table. Please run load_wikipedia.py first"
            )
            return

        # 测试查询列表
        test_queries = [
            {
                "description": "Test 1: Search for 'artificial intelligence'",
                "sql": """
                    SELECT id, title, _score 
                    FROM wikipedia_articles 
                    WHERE content = text('artificial intelligence', 1.0) 
                    ORDER BY _score DESC 
                    LIMIT 10
                """,
            },
            {
                "description": "Test 2: Search for 'quantum physics'",
                "sql": """
                    SELECT id, title, _score 
                    FROM wikipedia_articles 
                    WHERE content = text('quantum physics', 1.0) 
                    ORDER BY _score DESC 
                    LIMIT 10
                """,
            },
            {
                "description": "Test 3: Phrase search 'machine learning'",
                "sql": """
                    SELECT id, title, _score 
                    FROM wikipedia_articles 
                    WHERE content = phrase('machine learning', 1.0, 0) 
                    ORDER BY _score DESC 
                    LIMIT 10
                """,
            },
            {
                "description": "Test 4: Search for 'python programming'",
                "sql": """
                    SELECT id, title, _score 
                    FROM wikipedia_articles 
                    WHERE content = text('python programming', 1.0) 
                    ORDER BY _score DESC 
                    LIMIT 10
                """,
            },
            {
                "description": "Test 5: Search for 'climate change'",
                "sql": """
                    SELECT id, title, _score 
                    FROM wikipedia_articles 
                    WHERE content = text('climate change', 1.0) 
                    ORDER BY _score DESC 
                    LIMIT 10
                """,
            },
            {
                "description": "Test 6: Search for 'world war'",
                "sql": """
                    SELECT id, title, _score 
                    FROM wikipedia_articles 
                    WHERE content = text('world war', 1.0) 
                    ORDER BY _score DESC 
                    LIMIT 10
                """,
            },
            {
                "description": "Test 7: Search for 'computer science'",
                "sql": """
                    SELECT id, title, _score 
                    FROM wikipedia_articles 
                    WHERE content = text('computer science', 1.0) 
                    ORDER BY _score DESC 
                    LIMIT 10
                """,
            },
        ]

        # 执行所有测试
        all_results = []
        for test in test_queries:
            results = execute_query(cursor, test["sql"], test["description"])
            all_results.append((test["description"], len(results)))
            time.sleep(0.5)  # 稍微延迟以便观察

        # 总结
        print_colored(Colors.CYAN, f"\n{'='*80}")
        print_colored(Colors.GREEN, "📊 Test Summary")
        print_colored(Colors.CYAN, f"{'='*80}\n")

        for desc, count in all_results:
            status = "✓" if count > 0 else "✗"
            color = Colors.GREEN if count > 0 else Colors.YELLOW
            print_colored(color, f"  {status} {desc}: {count} results")

        print()
        print_colored(Colors.GREEN, "🎉 Full-text search testing completed!")

    except Exception as e:
        print_colored(Colors.RED, f"\n❌ Error: {e}")
        import traceback

        traceback.print_exc()

    finally:
        cursor.close()
        conn.close()


def interactive_search():
    """交互式搜索"""
    print_colored(Colors.BLUE, "\n=== Interactive Wikipedia Search ===\n")

    conn = pymysql.connect(
        host="localhost",
        port=3307,
        user="root",
        password="",
        database="test",
        charset="utf8mb4",
    )
    cursor = conn.cursor()

    print_colored(Colors.CYAN, "Enter your search queries (or 'quit' to exit)\n")

    while True:
        try:
            query_text = input(f"{Colors.MAGENTA}🔍 Search:{Colors.NC} ").strip()

            if query_text.lower() in ["quit", "exit", "q"]:
                break

            if not query_text:
                continue

            # 构建查询
            sql = f"""
                SELECT id, title, _score 
                FROM wikipedia_articles 
                WHERE content = text('{query_text}', 1.0) 
                ORDER BY _score DESC 
                LIMIT 10
            """

            execute_query(cursor, sql, f"Search for '{query_text}'")

        except KeyboardInterrupt:
            break
        except Exception as e:
            print_colored(Colors.RED, f"Error: {e}")

    cursor.close()
    conn.close()
    print_colored(Colors.GREEN, "\nGoodbye!")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "interactive":
        interactive_search()
    else:
        test_wikipedia_search()

        print_colored(
            Colors.CYAN, "\n💡 Tip: Run with 'interactive' for interactive search mode:"
        )
        print_colored(Colors.CYAN, "   python3 test_wikipedia.py interactive")
