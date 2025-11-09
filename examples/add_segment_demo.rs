use calm::{
    partition::Partition,
    schema::{field::FieldOption, Schema},
};
use datafusion::arrow::{
    array::{Int64Array, RecordBatch, StringArray},
    datatypes::{DataType, Field, Schema as ArrowSchema},
};
use datafusion::parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::mpsc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🎯 演示 add_segment_from_parquet 功能\n");

    // === 步骤 1: 准备外部 Parquet 文件 ===
    println!("📝 步骤 1: 创建外部 Parquet 文件");

    let external_parquet_dir = PathBuf::from("/tmp/external_parquet_demo");
    if external_parquet_dir.exists() {
        std::fs::remove_dir_all(&external_parquet_dir)?;
    }
    std::fs::create_dir_all(&external_parquet_dir)?;

    let external_parquet_path = external_parquet_dir.join("external_data.parquet");

    // 创建 Arrow Schema
    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("score", DataType::Int64, false),
    ]));

    // 准备数据
    let ids = Int64Array::from((1..=500).collect::<Vec<i64>>());
    let names = StringArray::from(
        (1..=500)
            .map(|i| format!("外部数据_{}", i))
            .collect::<Vec<String>>(),
    );
    let scores = Int64Array::from((1..=500).map(|i| (i % 100) as i64).collect::<Vec<i64>>());

    let batch = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![Arc::new(ids), Arc::new(names), Arc::new(scores)],
    )?;

    // 写入 Parquet 文件
    let file = File::create(&external_parquet_path)?;
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(file, arrow_schema.clone(), Some(props))?;
    writer.write(&batch)?;
    writer.close()?;

    println!("  ✓ 创建外部 Parquet 文件: {:?}", external_parquet_path);
    println!("  ✓ 包含 {} 行数据\n", batch.num_rows());

    // === 步骤 2: 创建 Partition ===
    println!("📝 步骤 2: 创建 Partition");

    let schema = Schema {
        name: "demo_partition".to_string(),
        primary_key: None,
        store_source: false,
        fields: vec![
            FieldOption::I64 {
                name: "id".to_string(),
                index: true,
            },
            FieldOption::Keyword {
                name: "name".to_string(),
                index: true,
                is_array: false,
                persist_option: None,
                case_sensitive: true,
            },
            FieldOption::I64 {
                name: "score".to_string(),
                index: true,
            },
        ],
        persist_policy: Default::default(),
    };

    let data_dir = PathBuf::from("/tmp/add_segment_demo");
    if data_dir.exists() {
        std::fs::remove_dir_all(&data_dir)?;
    }
    std::fs::create_dir_all(&data_dir)?;

    let (tx, _rx) = mpsc::unbounded_channel();
    let partition = Partition::new(0, data_dir.clone(), schema, tx);

    println!("  ✓ Partition 创建完成\n");

    // === 步骤 3: 先插入一些正常数据 ===
    println!("📝 步骤 3: 插入正常数据 (通过 upsert)");

    let normal_ids = Int64Array::from(vec![1001, 1002, 1003, 1004, 1005]);
    let normal_names = StringArray::from(vec![
        "正常数据_1",
        "正常数据_2",
        "正常数据_3",
        "正常数据_4",
        "正常数据_5",
    ]);
    let normal_scores = Int64Array::from(vec![90, 85, 88, 92, 87]);

    let normal_batch = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![
            Arc::new(normal_ids),
            Arc::new(normal_names),
            Arc::new(normal_scores),
        ],
    )?;

    partition.upsert(normal_batch)?;
    println!("  ✓ 插入 5 条正常数据\n");

    // === 步骤 4: 使用 add_segment_from_parquet 添加外部数据 ===
    println!("📝 步骤 4: 添加外部 Parquet 文件作为 Segment");

    let seg_id = partition.add_segment_from_parquet(external_parquet_path.to_str().unwrap())?;

    println!("  ✓ Segment {} 已添加\n", seg_id);

    // === 步骤 5: 再插入一些数据 ===
    println!("📝 步骤 5: 再插入更多数据");

    let more_ids = Int64Array::from(vec![2001, 2002, 2003]);
    let more_names = StringArray::from(vec!["后续数据_1", "后续数据_2", "后续数据_3"]);
    let more_scores = Int64Array::from(vec![95, 93, 91]);

    let more_batch = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![
            Arc::new(more_ids),
            Arc::new(more_names),
            Arc::new(more_scores),
        ],
    )?;

    partition.upsert(more_batch)?;
    println!("  ✓ 插入 3 条后续数据\n");

    // === 步骤 6: 验证 ===
    println!("📝 步骤 6: 验证 Partition 状态");

    // 检查 segment 数量
    let segment_count = partition.segment_count();
    let total_count = partition.total_count();

    println!("  ✓ Partition 统计:");
    println!("    - Segment 数量: {}", segment_count);
    println!("    - 总文档数: {}", total_count);
    println!();

    // 验证 frozen segments
    let frozen = partition.get_frozen_segments();
    println!("  ✓ Frozen Segments:");
    for (seg_id, segment) in frozen.iter() {
        println!("    - Segment {}: {} 文档", seg_id, segment.doc_count());
        if let Some(base_path) = segment.base_path() {
            println!("      路径: {}", base_path);
        }
    }
    println!();

    // 验证 current segment
    let current = partition.get_current_segment();
    println!("  ✓ Current Segment:");
    println!("    - 文档数: {}", current.doc_count());
    println!("    - Start ID: {}", current.start);

    println!("\n✅ 演示完成!");
    println!("\n💡 关键点:");
    println!("   1. 外部 Parquet 文件保持原样,不被复制");
    println!("   2. 索引在内存中构建,加速查询");
    println!("   3. 可以与正常的 upsert 数据混合使用");
    println!("   4. 查询时自动合并所有 segment 的数据");

    Ok(())
}
