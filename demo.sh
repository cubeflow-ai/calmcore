#!/bin/bash

# 查询优化器 Demo 运行脚本

echo "╔════════════════════════════════════════════════════════════╗"
echo "║          查询优化器 Demo 运行脚本                            ║"
echo "╚════════════════════════════════════════════════════════════╝"
echo ""

# 如果没有参数，显示帮助
if [ $# -eq 0 ]; then
    echo "用法："
    echo "  ./demo.sh write              - 写入数据"
    echo "  ./demo.sh query              - 查询数据"
    echo "  ./demo.sh persist            - 持久化数据"
    echo "  ./demo.sh write query        - 写入并查询"
    echo "  ./demo.sh all                - 全部操作"
    echo "  ./demo.sh clean              - 清理测试数据"
    echo ""
    echo "示例："
    echo "  ./demo.sh write query        # 写入数据并查询"
    echo "  ./demo.sh query              # 只查询（需要先写入）"
    exit 0
fi

# 检查是否是清理操作
if [ "$1" == "clean" ]; then
    echo "清理测试数据..."
    rm -rf ./demo_data
    rm -rf ./benchmark_data
    echo "✅ 清理完成"
    exit 0
fi

# 运行 demo
echo "运行操作: $@"
echo ""
RUST_LOG=info cargo run --example demo --release -- "$@"
