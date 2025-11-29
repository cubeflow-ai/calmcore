import re

with open('test_results.log', 'r') as f:
    content = f.read()

# 找到所有包含 ❌ 的测试
pattern = r'📊 \d+\. (.+?)\nSQL: (.+?)(?=\n  \[Calm\])'
matches = re.findall(pattern, content, re.DOTALL)

print(f"找到 {len(matches)} 个失败的测试:\n")
for i, (title, sql) in enumerate(matches[:30], 1):
    sql_clean = ' '.join(sql.split())
    if '❌' in content[content.find(sql):content.find(sql) + 1000]:
        print(f"{i}. {title.strip()}")
        print(f"   {sql_clean[:100]}...")
        print()
