# SQL 测试指南

本文档介绍如何测试 Calm 数据库的 SQL 查询功能。

## 测试方案

我们提供了三种测试方式：

### 1. Rust 集成测试（推荐用于 CI/CD）

直接在 Rust 代码中测试，不需要启动独立服务器。

```bash
# 快速测试（1000 条数据）
cargo test --test test_sql_query test_sql_query_quick -- --nocapture

# 完整测试（100 万条数据，需要较长时间）
cargo