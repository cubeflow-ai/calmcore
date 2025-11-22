use calm::compute::optimizer::analyze_query;
/// 查询类型分类演示
///
/// 演示新的扁平化查询执行架构，展示不同 SQL 如何被分类到不同的执行路径
use calm::compute::sql_normalizer::SqlNormalizer;

fn main() {
    println!("🎯 查询类型分类演示 - 扁平化执行架构\n");
    println!("{}", "=".repeat(80));

    let test_queries = vec![
        // ===== 聚合查询 =====
        (
            "SELECT COUNT(*) FROM users",
            "COUNT(*) 无 GROUP BY - 快速路径",
        ),
        (
            "SELECT status, COUNT(*) FROM users GROUP BY status",
            "COUNT(*) + 单字段 GROUP BY - 索引快速路径",
        ),
        (
            "SELECT status, region, COUNT(*) FROM users GROUP BY status, region",
            "COUNT(*) + 多字段 GROUP BY - 通用聚合",
        ),
        (
            "SELECT status, COUNT(*), AVG(age) FROM users GROUP BY status",
            "多聚合函数 - 通用聚合",
        ),
        // ===== 非聚合查询 =====
        (
            "SELECT * FROM users ORDER BY age LIMIT 10",
            "ORDER BY + LIMIT - 并行 TopK",
        ),
        ("SELECT * FROM users LIMIT 100", "纯 LIMIT - 串行早停"),
        (
            "SELECT * FROM users ORDER BY age",
            "ORDER BY 无 LIMIT - 并行流式",
        ),
        (
            "SELECT * FROM users WHERE age > 20",
            "无 ORDER BY 无 LIMIT - 串行全表扫描",
        ),
        // ===== 复杂场景 =====
        (
            "SELECT * FROM users WHERE age BETWEEN 20 AND 30 ORDER BY age LIMIT 10",
            "WHERE + ORDER BY + LIMIT",
        ),
        (
            "SELECT name, age FROM users LIMIT 50 OFFSET 10",
            "LIMIT + OFFSET",
        ),
    ];

    for (sql, description) in test_queries {
        println!("\n📝 SQL: {}", sql);
        println!("   描述: {}", description);

        match SqlNormalizer::normalize(sql) {
            Ok((statement, _)) => {
                match analyze_query(statement) {
                    Some(plan) => {
                        println!("   ✅ 表: {}", plan.table_name);
                        println!("   🔍 查询类型: {:?}", plan.query_type);

                        // 输出执行提示信息
                        if plan.execution_hints.has_where {
                            println!(
                                "   🎯 WHERE 条件: {} 个过滤条件",
                                plan.execution_hints.where_conditions.len()
                            );
                            for cond in &plan.execution_hints.where_conditions {
                                println!(
                                    "      - 字段: {}, 类型: {:?}",
                                    cond.field_name, cond.condition_type
                                );
                            }
                        }
                        if plan.execution_hints.is_select_star {
                            println!("   📋 SELECT: * (所有字段)");
                        } else if !plan.execution_hints.projection_fields.is_empty() {
                            println!(
                                "   📋 SELECT: {}",
                                plan.execution_hints.projection_fields.join(", ")
                            );
                        }

                        // 根据类型输出执行策略
                        use calm::compute::optimizer::QueryType;
                        match plan.query_type {
                            QueryType::CountOnly => {
                                println!("   ⚡ 执行策略: 直接统计总行数（最快）");
                            }
                            QueryType::CountWithSingleGroupBy(_) => {
                                println!("   ⚡ 执行策略: 使用倒排索引 bitmap 计数");
                            }
                            QueryType::GeneralAggregation(_) => {
                                println!("   📊 执行策略: DataFusion 聚合 + 协调节点合并");
                            }
                            QueryType::ParallelSortLimit(_) => {
                                println!("   🔀 执行策略: 并行查询 + TopK 合并");
                            }
                            QueryType::SerialLimit(_) => {
                                println!("   ➡️  执行策略: 串行扫描 + 早停优化");
                            }
                            QueryType::ParallelSortStreaming(_) => {
                                println!("   🌊 执行策略: 并行查询 + 流式排序输出");
                            }
                            QueryType::SerialFullScan => {
                                println!("   📄 执行策略: 串行全表扫描（自然顺序）");
                            }
                        }
                    }
                    None => {
                        println!("   ⚠️  无法识别查询模式，使用 DataFusion 回退");
                    }
                }
            }
            Err(e) => {
                println!("   ❌ SQL 解析失败: {}", e);
            }
        }
    }

    println!();
    println!();
    println!("{}", "=".repeat(80));
    println!("🎉 架构优势:");
    println!("  1. ✨ 扁平化路由：在 execute_sql 中根据 QueryType 直接分发");
    println!("  2. 🚀 判断前置：所有判断逻辑在 analyze_query 中完成");
    println!("  3. 🎯 清晰明确：每种查询类型对应一个执行函数");
    println!("  4. 📊 易于扩展：新增查询类型只需添加新的 enum variant");
    println!("  5. 🔧 便于维护：执行逻辑和分析逻辑分离");
    println!("{}", "=".repeat(80));
}
