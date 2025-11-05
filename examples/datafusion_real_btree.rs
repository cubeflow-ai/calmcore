/// DataFusion 集成: 使用真实的 BTree RowData
///
/// 演示如何将 CalmCore 的实际 Partition 数据集成到 DataFusion 进行 SQL 查询
use std::time::Duration;

use calm::engine::{Engine, EngineConfig};
use calm::schema::field::FieldOption;
use calm::schema::{PersistPolicy, Schema};
use datafusion::arrow::util::pretty::print_batches;
use datafusion::datasource::MemTable;
use datafusion::prelude::*;
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 DataFusion + 真实 BTree RowData 集成示例\n");

    // 1. 创建 Schema
    let mut schema = Schema::new(
        vec![
            FieldOption::Keyword {
                name: "name".to_string(),
                index: false,
                is_array: false,
            },
            FieldOption::I64 {
                name: "age".to_string(),
                index: false,
            },
            FieldOption::F64 {
                name: "score".to_string(),
                index: false,
            },
        ],
        PersistPolicy {
            max_docs_per_segment: 1000,
            segment_time_threshold_secs: 3600,
        },
    );
    schema.primary_key = Some("name".to_string());

    let schema = Arc::new(schema);

    println!("📋 创建 Schema: name(keyword), age(i64), score(f64)\n");

    // 2. 创建 Segment 并写入数据
    let segment = Segment::new(0, schema.clone());

    // 准备测试数据
    let arrow_schema = Arc::new(datafusion::arrow::datatypes::Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::Int64, false),
        Field::new("score", DataType::Float64, false),
    ]));

    // 第一批数据
    let batch1 = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])) as ArrayRef,
            Arc::new(Int64Array::from(vec![25, 30, 35])) as ArrayRef,
            Arc::new(Float64Array::from(vec![85.5, 92.0, 78.5])) as ArrayRef,
        ],
    )?;

    // 第二批数据
    let batch2 = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["David", "Eve", "Frank"])) as ArrayRef,
            Arc::new(Int64Array::from(vec![40, 45, 28])) as ArrayRef,
            Arc::new(Float64Array::from(vec![88.0, 95.5, 72.0])) as ArrayRef,
        ],
    )?;

    println!("📝 写入数据到 Segment...");

    // 写入数据 (不传 pk_hash，内部会自动计算)
    let lock = std::sync::RwLock::new(());
    segment.write(&batch1, None, None, &lock)?;
    segment.write(&batch2, None, None, &lock)?;

    println!(
        "✅ 成功写入 {} 条记录\n",
        batch1.num_rows() + batch2.num_rows()
    );

    // 3. 从 Segment 读取数据（通过 scan_documents）
    println!("📖 从 BTree RowData 读取数据...");
    let batches = segment.scan_documents()?;

    println!(
        "✅ 读取到 {} 个 RecordBatch (共 {} 行)\n",
        batches.len(),
        batches.iter().map(|b| b.num_rows()).sum::<usize>()
    );

    // 4. 构建 DataFusion Schema (需要包含 _internal_id 字段)
    let df_schema = batches[0].schema();

    // 5. 创建 MemTable 并注册到 DataFusion
    let provider = MemTable::try_new(df_schema.clone(), vec![batches])?;
    let ctx = SessionContext::new();
    ctx.register_table("documents", Arc::new(provider))?;

    println!("📋 注册表 'documents' 到 DataFusion\n");
    println!("═══════════════ SQL 查询测试 ═══════════════\n");

    // 6. 执行各种 SQL 查询

    // 查询1: 全表扫描
    println!("🔵 查询1: SELECT * FROM documents");
    let df = ctx.sql("SELECT * FROM documents").await?;
    let results = df.collect().await?;
    print_batches(&results)?;
    println!();

    // 查询2: 选择特定列
    println!("🟢 查询2: SELECT name, age, score FROM documents");
    let df = ctx.sql("SELECT name, age, score FROM documents").await?;
    let results = df.collect().await?;
    print_batches(&results)?;
    println!();

    // 查询3: 条件过滤
    println!("🟡 查询3: WHERE age >= 35");
    let df = ctx
        .sql("SELECT name, age, score FROM documents WHERE age >= 35")
        .await?;
    let results = df.collect().await?;
    print_batches(&results)?;
    println!();

    // 查询4: 排序
    println!("🟠 查询4: ORDER BY score DESC");
    let df = ctx
        .sql("SELECT name, score FROM documents ORDER BY score DESC LIMIT 3")
        .await?;
    let results = df.collect().await?;
    print_batches(&results)?;
    println!();

    // 查询5: 聚合统计
    println!("🔴 查询5: 聚合统计 (AVG, MAX, MIN, COUNT)");
    let df = ctx
        .sql(
            "SELECT COUNT(*) as total, \
             AVG(score) as avg_score, \
             MAX(score) as max_score, \
             MIN(score) as min_score \
             FROM documents",
        )
        .await?;
    let results = df.collect().await?;
    print_batches(&results)?;
    println!();

    // 查询6: 分组统计
    println!("🟣 查询6: GROUP BY 年龄段");
    let df = ctx
        .sql(
            "SELECT age / 10 * 10 as age_group, \
             COUNT(*) as count, \
             AVG(score) as avg_score \
             FROM documents \
             GROUP BY age_group \
             ORDER BY age_group",
        )
        .await?;
    let results = df.collect().await?;
    print_batches(&results)?;
    println!();

    // 查询7: 复杂条件
    println!("⚪ 查询7: 复杂条件 (BETWEEN + ORDER BY)");
    let df = ctx
        .sql(
            "SELECT name, age, score \
             FROM documents \
             WHERE age BETWEEN 30 AND 45 AND score > 80 \
             ORDER BY score DESC",
        )
        .await?;
    let results = df.collect().await?;
    print_batches(&results)?;
    println!();

    println!("✨ 测试完成!\n");
    println!("💡 关键实现:");
    println!("   ✅ 使用 Segment::write() 写入数据到 BTree");
    println!("   ✅ 使用 Segment::scan_documents() 读取所有 RecordBatch");
    println!("   ✅ RecordBatch 包含 _internal_id 列 (CalmCore 自动添加)");
    println!("   ✅ 直接传递给 MemTable，零拷贝集成");
    println!("   ✅ 支持完整 SQL 功能");
    println!("\n🎯 生产环境优化:");
    println!("   - 使用 Segment::get_documents(doc_ids) 仅获取需要的文档");
    println!("   - 结合倒排索引,先过滤再查询 RowData");
    println!("   - 实现自定义 TableProvider 支持过滤下推");
    println!("   - 跨多个 Segment 的分布式查询");

    Ok(())
}
