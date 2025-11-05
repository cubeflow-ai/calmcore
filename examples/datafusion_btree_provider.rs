/// DataFusion 集成: 读取 BTree RowData 并进行 SQL 查询
///
/// 演示如何将 CalmCore 的 BTree 存储集成到 DataFusion
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::util::pretty::print_batches;
use datafusion::datasource::MemTable;
use datafusion::prelude::*;
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 DataFusion + BTree TableProvider 示例\n");

    // 1. 模拟从 BTree 读取的数据
    // 实际场景中这些数据来自 Segment::scan_documents() 或 TreeReader
    let schema = Arc::new(Schema::new(vec![
        Field::new("doc_id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::Int64, false),
        Field::new("score", DataType::Float64, false),
    ]));

    // 模拟 BTree 中的多个 RecordBatch (每个 batch 可能来自不同的 BTree chunk)
    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])) as ArrayRef,
            Arc::new(Int64Array::from(vec![25, 30, 35])) as ArrayRef,
            Arc::new(Float64Array::from(vec![85.5, 92.0, 78.5])) as ArrayRef,
        ],
    )?;

    let batch2 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![4, 5, 6])) as ArrayRef,
            Arc::new(StringArray::from(vec!["David", "Eve", "Frank"])) as ArrayRef,
            Arc::new(Int64Array::from(vec![40, 45, 28])) as ArrayRef,
            Arc::new(Float64Array::from(vec![88.0, 95.5, 72.0])) as ArrayRef,
        ],
    )?;

    println!(
        "📦 模拟加载了 2 个 RecordBatch (共 {} 行)\n",
        batch1.num_rows() + batch2.num_rows()
    );

    // 2. 使用 MemTable 作为 TableProvider (DataFusion 内置的内存表)
    // MemTable 可以直接接受多个 RecordBatch
    let provider = MemTable::try_new(schema.clone(), vec![vec![batch1, batch2]])?;

    // 3. 注册到 DataFusion
    let ctx = SessionContext::new();
    ctx.register_table("documents", Arc::new(provider))?;

    println!("📋 注册表 'documents' 完成\n");
    println!("═══════════════ SQL 查询测试 ═══════════════\n");

    // 4. 执行各种 SQL 查询

    // 查询1: 全表扫描
    println!("🔵 查询1: SELECT * FROM documents");
    let df = ctx.sql("SELECT * FROM documents").await?;
    let results = df.collect().await?;
    print_batches(&results)?;
    println!();

    // 查询2: 条件过滤
    println!("🟢 查询2: WHERE 条件过滤");
    let df = ctx
        .sql("SELECT name, age, score FROM documents WHERE age >= 35")
        .await?;
    let results = df.collect().await?;
    print_batches(&results)?;
    println!();

    // 查询3: 排序
    println!("🟡 查询3: ORDER BY 排序");
    let df = ctx
        .sql("SELECT name, score FROM documents ORDER BY score DESC LIMIT 3")
        .await?;
    let results = df.collect().await?;
    print_batches(&results)?;
    println!();

    // 查询4: 聚合
    println!("🟠 查询4: GROUP BY 聚合");
    let df = ctx
        .sql("SELECT age / 10 * 10 as age_group, COUNT(*) as count, AVG(score) as avg_score FROM documents GROUP BY age_group ORDER BY age_group")
        .await?;
    let results = df.collect().await?;
    print_batches(&results)?;
    println!();

    // 查询5: 复杂查询
    println!("🔴 查询5: 复杂查询 (多条件 + 聚合)");
    let df = ctx
        .sql("SELECT COUNT(*) as total, AVG(score) as avg_score, MAX(score) as max_score, MIN(score) as min_score FROM documents WHERE age BETWEEN 30 AND 45")
        .await?;
    let results = df.collect().await?;
    print_batches(&results)?;
    println!();

    println!("✨ 测试完成!\n");
    println!("💡 集成要点:");
    println!("   ✅ 使用 datafusion::arrow 类型,避免版本冲突");
    println!("   ✅ 使用 MemTable 作为内置 TableProvider");
    println!("   ✅ BTree 的 RecordBatch 可以直接传给 MemTable");
    println!("   ✅ 支持所有标准 SQL 操作 (WHERE, ORDER BY, GROUP BY, JOIN 等)");
    println!("   ✅ 表格输出美观易读 (print_batches)");
    println!("\n🎯 实际集成步骤:");
    println!("   1. 从 Segment::scan_documents() 获取 Vec<RecordBatch>");
    println!("   2. 创建 MemTable::try_new(schema, vec![batches])?");
    println!("   3. 注册到 SessionContext: ctx.register_table(name, Arc::new(provider))?");
    println!("   4. 执行 SQL: ctx.sql(\"SELECT * FROM ...\").await?.collect().await?");
    println!("   5. 打印结果: print_batches(&results)?");
    println!("\n🚀 高级优化:");
    println!("   - 实现自定义 TableProvider 支持过滤下推 (filter pushdown)");
    println!("   - 实现懒加载,避免一次性加载所有数据");
    println!("   - 与倒排索引结合,先通过索引找到 doc_id,再通过 BTree 获取完整文档");

    Ok(())
}
