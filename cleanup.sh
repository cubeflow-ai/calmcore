#!/bin/bash
# 快速清理和验证脚本

set -e

echo "=========================================="
echo "Calm Database 代码清理工具"
echo "=========================================="

# 1. 自动清理未使用的导入
echo -e "\n[1/5] 清理未使用的导入..."
cargo clippy --fix --allow-dirty --allow-staged -- -W unused-imports 2>&1 | tail -3

# 2. 格式化代码
echo -e "\n[2/5] 格式化代码..."
cargo fmt

# 3. 检查编译
echo -e "\n[3/5] 验证编译..."
cargo check 2>&1 | tail -3

# 4. 统计警告
echo -e "\n[4/5] 统计警告数量..."
WARNING_COUNT=$(cargo build 2>&1 | grep "warning:" | wc -l | tr -d ' ')
echo "当前警告数: $WARNING_COUNT"

# 5. 运行测试
echo -e "\n[5/5] 运行测试..."
cargo test --lib 2>&1 | tail -10

echo -e "\n=========================================="
echo "✅ 清理完成！"
echo "=========================================="
echo "警告数量: $WARNING_COUNT"
echo "下一步建议:"
echo "  1. 查看 CODE_REVIEW_AND_RECOMMENDATIONS.md"
echo "  2. 运行 ./test_federation.sh 测试分布式查询"
echo "  3. 提交代码: git add . && git commit -m 'chore: clean up unused imports'"
