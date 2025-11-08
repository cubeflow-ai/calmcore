/// 测试所有支持的数据类型
///
/// 这个示例演示了 CalmCore 支持的所有 12 种数据类型：
/// - 整数: I8, I16, I32, I64, U8, U16, U32, U64
/// - 浮点: F32, F64
/// - 字符串: Keyword
/// - 布尔: Boolean
use calm::schema::{field::FieldOption, PersistPolicy, Schema};
use std::sync::Arc;

fn main() {
    println!("=== CalmCore 支持的所有数据类型 ===\n");

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
}
