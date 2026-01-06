use datafusion::arrow::array::{Int32Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::prelude::*;
use std::sync::Arc;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // ==========================================
    // 1. 准备模拟数据 (模拟 DuckDB 和 MySQL)
    // ==========================================
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    // 模拟 DuckDB 的数据 (id: 1, 2)
    let batch_duck = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["Duck_A", "Duck_B"])),
        ],
    )?;
    let provider_duck = MemTable::try_new(schema.clone(), vec![vec![batch_duck]])?;

    // 模拟 MySQL 的数据 (id: 3, 4)
    let batch_mysql = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![3, 4])),
            Arc::new(StringArray::from(vec!["MySQL_X", "MySQL_Y"])),
        ],
    )?;
    let provider_mysql = MemTable::try_new(schema.clone(), vec![vec![batch_mysql]])?;

    // ==========================================
    // 2. 注册表到 Context
    // ==========================================
    let ctx = SessionContext::new();
    ctx.register_table("duck_table", Arc::new(provider_duck))?;
    ctx.register_table("mysql_table", Arc::new(provider_mysql))?;

    // ==========================================
    // 3. 创建 Union View (这就是你的 "Union Table")
    // ==========================================
    println!("--- 创建逻辑视图 'all_tables' ---");
    ctx.sql(
        r#"
        CREATE VIEW all_tables AS 
        SELECT * FROM duck_table 
        UNION ALL 
        SELECT * FROM mysql_table
    "#,
    )
    .await?
    .show()
    .await?;

    // ==========================================
    // 4. 测试 Case A: 简单的 Filter (WHERE id > 2)
    // 观察 Filter 是如何下推的
    // ==========================================
    println!("\n--- Case A: SELECT * FROM all_tables WHERE id > 2 ---");
    let df_filter = ctx.sql("SELECT * FROM all_tables WHERE id > 2").await?;

    // 打印数据
    println!("Result:");
    df_filter.clone().show().await?;

    // 打印执行计划 (Explain)
    println!("Execution Plan (Filter):");
    df_filter.explain(false, false)?.show().await?;

    // ==========================================
    // 5. 测试 Case B: 聚合 Count(*)
    // 观察 Count 是如何执行的
    // ==========================================
    println!("\n--- Case B: SELECT name, COUNT(*) FROM all_tables group by name---");
    let df_count = ctx
        .sql("SELECT name, COUNT(*) FROM all_tables group by name")
        .await?;

    println!("Result:");
    df_count.clone().show().await?;

    println!("Execution Plan (Count):");
    df_count.explain(false, false)?.show().await?;

    println!("\n--- Case B: SELECT COUNT(*) FROM all_tables ---");
    let df_count = ctx.sql("SELECT COUNT(*) FROM all_tables").await?;

    println!("Result:");
    df_count.clone().show().await?;

    println!("Execution Plan (Count):");
    df_count.explain(false, false)?.show().await?;

    Ok(())
}
