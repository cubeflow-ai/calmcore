use calm::engine::{Engine, EngineConfig};
use calm::schema::{DataType, Field, PersistPolicy, Schema};
use calm::utils::error::CoreResult;
use std::sync::Arc;
use std::time::Duration;

#[tokio::main]
async fn main() -> CoreResult<()> {
    println!("=== Engine Demo ===\n");

    // 1. 创建 Engine 实例
    println!("📦 Step 1: 创建 Engine");
    let engine = Engine::new(EngineConfig {
        data_dir: "./demo_data".into(),
        persist_check_interval_secs: 10, // 10秒检查一次
        max_concurrent_persists: 2,
        check_after_flush: true,
    });
    println!("✅ Engine 创建成功\n");

    // 2. 创建 Schema
    println!("📝 Step 2: 创建 Schema");
    let schema = Schema {
        fields: vec![
            Field::new("id", DataType::U32, true),        // 主键
            Field::new("name", DataType::Keyword, false), // 关键词字段
            Field::new("age", DataType::I32, false),      // 数值字段
            Field::new("score", DataType::F32, false),    // 浮点数字段
        ],
        persist_policy: PersistPolicy {
            max_docs_per_segment: 1000,               // 每个 segment 最多 1000 条
            max_segment_age: Duration::from_secs(60), // 或者 60 秒
            check_on_flush: true,
        },
    };
    println!("✅ Schema 配置: {} 个字段\n", schema.fields.len());

    // 3. 创建 Partition
    println!("🗂️  Step 3: 创建 Partition");
    let partition = engine.create_partition(1, schema.clone()).await;
    println!("✅ Partition {} 创建成功\n", partition.id());

    // 4. 写入数据
    println!("✍️  Step 4: 写入数据");
    write_demo_data(&partition).await?;
    println!();

    // 5. 读取数据
    println!("📖 Step 5: 读取数据");
    read_demo_data(&partition).await?;
    println!();

    // 6. 触发持久化
    println!("💾 Step 6: 触发持久化");
    engine.trigger_persist(partition.id());
    println!("✅ 已发送持久化请求");

    // 等待持久化完成
    tokio::time::sleep(Duration::from_secs(3)).await;
    println!();

    // 7. 查看统计信息
    println!("📊 Step 7: 查看统计信息");
    engine.print_stats().await;

    // 8. 测试加载功能
    println!("🔄 Step 8: 测试从磁盘加载");
    test_load_partition(&engine, schema.clone()).await?;
    println!();

    // 9. 关闭 Engine
    println!("🛑 Step 9: 关闭 Engine");
    engine.shutdown().await;
    println!("✅ Engine 已关闭");

    println!("\n=== Demo 完成 ===");
    Ok(())
}

/// 写入示例数据
async fn write_demo_data(partition: &Arc<calm::partition::Partition>) -> CoreResult<()> {
    use arrow::array::{ArrayRef, Float32Array, Int32Array, StringArray, UInt32Array};
    use std::sync::Arc as StdArc;

    // 准备数据
    let batch_size = 100;
    let num_batches = 5;

    for batch_idx in 0..num_batches {
        let start_id = batch_idx * batch_size;
        let end_id = start_id + batch_size;

        // 构造 Arrow Arrays
        let ids: Vec<u32> = (start_id..end_id).collect();
        let names: Vec<String> = (start_id..end_id).map(|i| format!("user_{}", i)).collect();
        let ages: Vec<i32> = (start_id..end_id).map(|i| (i % 80 + 18) as i32).collect();
        let scores: Vec<f32> = (start_id..end_id).map(|i| (i % 100) as f32 + 0.5).collect();

        let id_array: ArrayRef = StdArc::new(UInt32Array::from(ids));
        let name_array: ArrayRef = StdArc::new(StringArray::from(names));
        let age_array: ArrayRef = StdArc::new(Int32Array::from(ages));
        let score_array: ArrayRef = StdArc::new(Float32Array::from(scores));

        let columns = vec![id_array, name_array, age_array, score_array];

        // 写入
        let row_ids = partition.write(columns)?;

        println!(
            "  ✓ Batch {}: 写入 {} 条记录 (ID: {}-{})",
            batch_idx + 1,
            row_ids.len(),
            start_id,
            end_id - 1
        );
    }

    println!("✅ 总共写入 {} 条记录", batch_size * num_batches);

    Ok(())
}

/// 读取示例数据
async fn read_demo_data(partition: &Arc<calm::partition::Partition>) -> CoreResult<()> {
    use arrow::array::{AsArray, UInt32Array};
    use roaring::RoaringBitmap;

    // 1. 查询所有数据
    println!("  📋 查询所有数据:");
    let all_filter = RoaringBitmap::from_iter(0..500); // 假设有 500 条
    let result = partition.search(&all_filter)?;

    println!("    - 总记录数: {}", result.num_rows());

    if result.num_rows() > 0 {
        // 显示前 5 条
        println!("    - 前 5 条记录:");
        let id_col = result
            .column(0)
            .as_primitive::<arrow::datatypes::UInt32Type>();
        let name_col = result.column(1).as_string::<i32>();
        let age_col = result
            .column(2)
            .as_primitive::<arrow::datatypes::Int32Type>();
        let score_col = result
            .column(3)
            .as_primitive::<arrow::datatypes::Float32Type>();

        for i in 0..5.min(result.num_rows()) {
            println!(
                "      [{}] ID: {}, Name: {}, Age: {}, Score: {:.1}",
                i,
                id_col.value(i),
                name_col.value(i),
                age_col.value(i),
                score_col.value(i)
            );
        }
    }

    // 2. 按 ID 查询
    println!("\n  🔍 按 ID 查询 (ID: 42):");
    let mut id_filter = RoaringBitmap::new();
    id_filter.insert(42);
    let result = partition.search(&id_filter)?;

    if result.num_rows() > 0 {
        let id_col = result
            .column(0)
            .as_primitive::<arrow::datatypes::UInt32Type>();
        let name_col = result.column(1).as_string::<i32>();
        let age_col = result
            .column(2)
            .as_primitive::<arrow::datatypes::Int32Type>();
        let score_col = result
            .column(3)
            .as_primitive::<arrow::datatypes::Float32Type>();

        println!(
            "    ✓ 找到: ID: {}, Name: {}, Age: {}, Score: {:.1}",
            id_col.value(0),
            name_col.value(0),
            age_col.value(0),
            score_col.value(0)
        );
    } else {
        println!("    ✗ 未找到记录");
    }

    // 3. 统计信息
    println!("\n  📈 统计信息:");
    println!("    - 总文档数: {}", partition.total_count());
    println!("    - Frozen Segments: {}", partition.frozen_count());
    println!(
        "    - Unpersisted Segments: {}",
        partition.get_unpersisted_segments().len()
    );

    Ok(())
}

/// 测试加载 Partition
async fn test_load_partition(engine: &Arc<Engine>, schema: Schema) -> CoreResult<()> {
    println!("  🔄 从磁盘加载 Partition 1...");

    // 先等一下确保之前的持久化完成
    tokio::time::sleep(Duration::from_secs(2)).await;

    // 加载已存在的 Partition
    match engine.load_partition(1, schema).await {
        Ok(loaded_partition) => {
            println!("  ✅ 加载成功!");
            println!("    - Partition ID: {}", loaded_partition.id());
            println!("    - 总文档数: {}", loaded_partition.total_count());
            println!("    - Frozen Segments: {}", loaded_partition.frozen_count());

            // 验证数据
            use roaring::RoaringBitmap;
            let filter = RoaringBitmap::from_iter(0..10);
            let result = loaded_partition.search(&filter)?;
            println!("    - 验证查询: {} 条记录", result.num_rows());
        }
        Err(e) => {
            println!("  ⚠️  加载失败: {:?}", e);
            println!("     (这可能是因为数据还未持久化到磁盘)");
        }
    }

    Ok(())
}
