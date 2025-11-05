/// DataFusion 集成: 使用 SQL 查询 BTree RowData (简化版)
///
/// 使用 datafusion::arrow 类型,避免版本冲突
use std::sync::Arc;

// 使用 DataFusion 自带的 Arrow 类型
use datafusion::arrow::array::{ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::util::pretty::print_batches;
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 DataFusion 简单示例\n");

    // 1. 创建 SessionContext
    let ctx = SessionContext::new();

    // 2. 创建测试数据
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::Int64, false),
        Field::new("score", DataType::Float64, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])) as ArrayRef,
            Arc::new(StringArray::from(vec![
                "Alice", "Bob", "Charlie", "David", "Eve",
            ])) as ArrayRef,
            Arc::new(Int64Array::from(vec![25, 30, 35, 40, 45])) as ArrayRef,
            Arc::new(Float64Array::from(vec![85.5, 92.0, 78.5, 88.0, 95.5])) as ArrayRef,
        ],
    )?;

    // 3. 创建简单的 MemTable
    let provider = datafusion::datasource::MemTable::try_new(schema, vec![vec![batch]])?;

    // 4. 注册表
    ctx.register_table("users", Arc::new(provider))?;

    println!("📋 注册表 'users' 完成\n");

    // 5. 执行 SQL 查询
    println!("═══════════════ SQL 查询测试 ═══════════════\n");

    // 查询1: 全表查询
    println!("🔵 查询1: SELECT * FROM users");
    let df = ctx.sql("SELECT * FROM users").await?;
    let results = df.collect().await?;
    print_batches(&results)?;
    println!();

    // 查询2: 条件过滤
    println!("🟢 查询2: SELECT * FROM users WHERE age > 30");
    let df = ctx
        .sql("SELECT name, age, score FROM users WHERE age > 30")
        .await?;
    let results = df.collect().await?;
    print_batches(&results)?;
    println!();

    // 查询3: 聚合
    println!("🟡 查询3: SELECT AVG(score), MAX(age) FROM users");
    let df = ctx
        .sql("SELECT AVG(score) as avg_score, MAX(age) as max_age FROM users")
        .await?;
    let results = df.collect().await?;
    print_batches(&results)?;
    println!();

    println!("✨ 测试完成!");
    println!("\n💡 下一步:");
    println!("   - 实现自定义 TableProvider 读取 BTree RowData");
    println!("   - 需要将 BTree 中的 RecordBatch 转换为 datafusion::arrow::RecordBatch");
    println!("   - 或者修改 BTree 序列化器使用 DataFusion 的 Arrow 类型");

    Ok(())
}
