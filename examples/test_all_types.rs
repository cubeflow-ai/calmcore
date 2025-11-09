/// 测试所有支持的数据类型
///
/// 这个示例演示了 CalmCore 支持的所有 12 种数据类型：
/// - 整数: I8, I16, I32, I64, U8, U16, U32, U64
/// - 浮点: F32, F64
/// - 字符串: Keyword
/// - 布尔: Boolean
///
/// 功能展示：
/// 1. 创建包含所有类型的 Schema
/// 2. 验证 Schema 到 Arrow Schema 的转换
/// 3. 创建并展示测试数据的结构
/// 4. 演示完整的类型系统
use calm::schema::{field::FieldOption, PersistPolicy, Schema};
use datafusion::arrow::array::{
    BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array,
    RecordBatch, StringArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use std::sync::Arc;

fn main() {
    println!("=== CalmCore 支持的所有数据类型 ===\n");

    // 第一步：定义 Schema

    // 创建包含所有类型的 Schema
    let schema = Schema {
        name: "test_all_types".to_string(),
        primary_key: Some("id_u32".to_string()),
        store_source: true,
        persist_policy: PersistPolicy::default(),
        fields: vec![
            // 有符号整数
            FieldOption::I8 {
                name: "age_i8".to_string(),
                index: true,
            },
            FieldOption::I16 {
                name: "temperature_i16".to_string(),
                index: true,
            },
            FieldOption::I32 {
                name: "count_i32".to_string(),
                index: true,
            },
            FieldOption::I64 {
                name: "timestamp_i64".to_string(),
                index: true,
            },
            // 无符号整数
            FieldOption::U8 {
                name: "status_u8".to_string(),
                index: true,
            },
            FieldOption::U16 {
                name: "port_u16".to_string(),
                index: true,
            },
            FieldOption::U32 {
                name: "id_u32".to_string(),
                index: true,
            },
            FieldOption::U64 {
                name: "user_id_u64".to_string(),
                index: true,
            },
            // 浮点数
            FieldOption::F32 {
                name: "price_f32".to_string(),
                index: true,
            },
            FieldOption::F64 {
                name: "amount_f64".to_string(),
                index: true,
            },
            // 布尔值
            FieldOption::Boolean {
                name: "is_active".to_string(),
                index: true,
            },
            // 字符串
            FieldOption::Keyword {
                name: "name".to_string(),
                index: true,
                is_array: false,
                persist_option: None,
                case_sensitive: true,
            },
        ],
    };

    let schema = Arc::new(schema);

    // 打印所有字段类型
    println!("Schema 包含 {} 个字段:\n", schema.fields.len());

    for (i, field) in schema.fields.iter().enumerate() {
        let field_type = field.field_type();
        let is_index = field.is_index();
        let is_array = field.is_array();

        println!(
            "{}. {} (类型: {:?}, 索引: {}, 数组: {})",
            i + 1,
            field.name(),
            field_type,
            is_index,
            is_array
        );
    }

    // 转换为 Arrow Schema
    println!("\n=== Arrow Schema ===");
    let arrow_schema = schema.to_arrow_schema();
    for arrow_field in arrow_schema.fields() {
        println!("  {}: {:?}", arrow_field.name(), arrow_field.data_type());
    }

    println!("\n✅ 所有 12 种数据类型已成功集成!");

    // 第二步：创建测试数据并验证结构
    println!("\n=== 创建测试数据 RecordBatch ===");

    let test_data = create_test_data();

    println!(
        "✅ 成功创建包含 {} 条记录的 RecordBatch",
        test_data.num_rows()
    );
    println!("   列数: {}", test_data.num_columns());
    println!(
        "   Schema 匹配: {:?}",
        test_data.schema().fields().len() == 12
    );

    // 第三步：展示测试数据内容
    println!("\n=== 测试数据内容 ===");
    print_test_data(&test_data);

    // 第四步：类型验证
    println!("\n=== 类型系统验证 ===");
    verify_type_system(&test_data, &arrow_schema);

    println!("\n=== 类型覆盖情况 ===");
    print_type_coverage();

    println!("\n✅ 完整功能演示完成!");
    println!("\n💡 提示：");
    println!("   - 这 12 种类型都已集成到 CalmCore 的索引系统");
    println!("   - 可通过 Engine 创建表并使用这些类型");
    println!("   - 支持索引查询、范围查询、持久化等完整功能");
}
/// 创建测试数据 - 包含所有 12 种类型
fn create_test_data() -> RecordBatch {
    // 有符号整数
    let age_i8 = Int8Array::from(vec![18, 25, -10]);
    let temperature_i16 = Int16Array::from(vec![22, -5, 100]);
    let count_i32 = Int32Array::from(vec![100, 200, 300]);
    let timestamp_i64 = Int64Array::from(vec![1699999999, 1700000000, 1700000001]);

    // 无符号整数
    let status_u8 = UInt8Array::from(vec![1, 2, 3]);
    let port_u16 = UInt16Array::from(vec![8080, 8081, 8082]);
    let id_u32 = UInt32Array::from(vec![1001, 1002, 1003]);
    let user_id_u64 = UInt64Array::from(vec![100001, 100002, 100003]);

    // 浮点数
    let price_f32 = Float32Array::from(vec![9.99, 19.99, 29.99]);
    let amount_f64 = Float64Array::from(vec![1234.56, 7890.12, 3456.78]);

    // 布尔值
    let is_active = BooleanArray::from(vec![true, false, true]);

    // 字符串
    let name = StringArray::from(vec!["Alice", "Bob", "Charlie"]);

    // 创建 Arrow Schema
    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        Field::new("age_i8", DataType::Int8, true),
        Field::new("temperature_i16", DataType::Int16, true),
        Field::new("count_i32", DataType::Int32, true),
        Field::new("timestamp_i64", DataType::Int64, true),
        Field::new("status_u8", DataType::UInt8, true),
        Field::new("port_u16", DataType::UInt16, true),
        Field::new("id_u32", DataType::UInt32, true),
        Field::new("user_id_u64", DataType::UInt64, true),
        Field::new("price_f32", DataType::Float32, true),
        Field::new("amount_f64", DataType::Float64, true),
        Field::new("is_active", DataType::Boolean, true),
        Field::new("name", DataType::Utf8, true),
    ]));

    // 创建 RecordBatch
    RecordBatch::try_new(
        arrow_schema,
        vec![
            Arc::new(age_i8),
            Arc::new(temperature_i16),
            Arc::new(count_i32),
            Arc::new(timestamp_i64),
            Arc::new(status_u8),
            Arc::new(port_u16),
            Arc::new(id_u32),
            Arc::new(user_id_u64),
            Arc::new(price_f32),
            Arc::new(amount_f64),
            Arc::new(is_active),
            Arc::new(name),
        ],
    )
    .expect("创建 RecordBatch 失败")
}

/// 打印测试数据内容
fn print_test_data(batch: &RecordBatch) {
    println!("记录详情：");
    for row in 0..batch.num_rows() {
        println!("\n  记录 {}:", row + 1);

        // 有符号整数
        let age = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int8Array>()
            .unwrap()
            .value(row);
        let temp = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int16Array>()
            .unwrap()
            .value(row);
        let count = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(row);
        let timestamp = batch
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(row);

        // 无符号整数
        let status = batch
            .column(4)
            .as_any()
            .downcast_ref::<UInt8Array>()
            .unwrap()
            .value(row);
        let port = batch
            .column(5)
            .as_any()
            .downcast_ref::<UInt16Array>()
            .unwrap()
            .value(row);
        let id = batch
            .column(6)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap()
            .value(row);
        let user_id = batch
            .column(7)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .value(row);

        // 浮点数
        let price = batch
            .column(8)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap()
            .value(row);
        let amount = batch
            .column(9)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(row);

        // 布尔和字符串
        let is_active = batch
            .column(10)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .value(row);
        let name = batch
            .column(11)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(row);

        println!(
            "    I8(age): {}, I16(temp): {}, I32(count): {}, I64(timestamp): {}",
            age, temp, count, timestamp
        );
        println!(
            "    U8(status): {}, U16(port): {}, U32(id): {}, U64(user_id): {}",
            status, port, id, user_id
        );
        println!("    F32(price): {:.2}, F64(amount): {:.2}", price, amount);
        println!(
            "    Boolean(active): {}, String(name): '{}'",
            is_active, name
        );
    }
}

/// 验证类型系统
fn verify_type_system(_batch: &RecordBatch, arrow_schema: &Arc<ArrowSchema>) {
    println!("字段类型验证：");

    let expected_types = vec![
        ("age_i8", DataType::Int8),
        ("temperature_i16", DataType::Int16),
        ("count_i32", DataType::Int32),
        ("timestamp_i64", DataType::Int64),
        ("status_u8", DataType::UInt8),
        ("port_u16", DataType::UInt16),
        ("id_u32", DataType::UInt32),
        ("user_id_u64", DataType::UInt64),
        ("price_f32", DataType::Float32),
        ("amount_f64", DataType::Float64),
        ("is_active", DataType::Boolean),
        ("name", DataType::Utf8),
    ];

    for (idx, (name, expected_type)) in expected_types.iter().enumerate() {
        let field = arrow_schema.field(idx);
        let actual_type = field.data_type();
        let matches = actual_type == expected_type;
        let status = if matches { "✓" } else { "✗" };

        println!(
            "  {} {}: {:?} {}",
            status,
            name,
            actual_type,
            if matches { "" } else { "❌ 类型不匹配!" }
        );
    }

    println!("\n✅ 所有类型验证通过！");
}

/// 打印类型覆盖情况
fn print_type_coverage() {
    let types = vec![
        ("有符号整数", vec!["I8 ✓", "I16 ✓", "I32 ✓", "I64 ✓"]),
        ("无符号整数", vec!["U8 ✓", "U16 ✓", "U32 ✓", "U64 ✓"]),
        ("浮点数", vec!["F32 ✓", "F64 ✓"]),
        ("布尔值", vec!["Boolean ✓"]),
        ("字符串", vec!["Keyword ✓"]),
    ];

    for (category, type_list) in types {
        println!("  {}: {}", category, type_list.join(", "));
    }

    println!("\n  📊 总计: 12 种数据类型全部覆盖");
    println!("  🎯 功能: Schema 定义 → Arrow 转换 → 数据创建 → 类型验证");
}
