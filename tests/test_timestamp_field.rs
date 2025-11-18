use calm::schema::{field::FieldOption, PersistPolicy, Schema};
use datafusion::arrow::array::{Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use std::sync::Arc;

#[test]
fn test_timestamp_field_schema() {
    // 测试包含Timestamp字段的Schema可以正确创建
    let schema = Schema {
        name: "test_timestamp".to_string(),
        primary_key: Some("id".to_string()),
        store_source: true,
        persist_policy: PersistPolicy::default(),
        fields: vec![
            FieldOption::Keyword {
                name: "id".to_string(),
                index: true,
                is_array: false,
                persist_option: None,
                case_sensitive: true,
            },
            FieldOption::Timestamp {
                name: "timestamp".to_string(),
                index: true,
                format: None,
            },
        ],
    };

    // 转换为 Arrow Schema
    let arrow_schema = schema.to_arrow_schema();
    let timestamp_field = arrow_schema.field(1);

    // 验证类型正确
    assert_eq!(timestamp_field.name(), "timestamp");
    assert!(matches!(
        timestamp_field.data_type(),
        DataType::Timestamp(TimeUnit::Millisecond, None)
    ));

    println!("✅ Timestamp field schema test passed!");
}

#[test]
fn test_timestamp_normalization() {
    use calm::utils::timestamp::normalize_timestamp;

    // Unix seconds (10位)
    let seconds = 1704067200; // 2024-01-01 00:00:00
    let ms = normalize_timestamp(seconds);
    assert_eq!(ms, 1704067200000);

    // Already milliseconds (13位)
    let milliseconds = 1704067200000;
    let ms = normalize_timestamp(milliseconds);
    assert_eq!(ms, 1704067200000);

    println!("✅ Timestamp normalization test passed!");
}

#[test]
fn test_timestamp_key_parsing() {
    use calm::segment::field_store::generic_index::IndexKey;
    use calm::segment::field_store::timestamp_key::TimestampKey;
    use datafusion::common::ScalarValue;

    // 测试从不同类型的ScalarValue创建TimestampKey

    // 1. 从Int64创建 (毫秒)
    let ts1 = TimestampKey::from_scalar(&ScalarValue::Int64(Some(1704067200000)));
    assert_eq!(ts1.unwrap().0, 1704067200000);

    // 2. 从字符串创建
    let ts2 =
        TimestampKey::from_scalar(&ScalarValue::Utf8(Some("2024-01-01 00:00:00".to_string())));
    assert!(ts2.is_some());
    println!("Parsed timestamp from string: {:?}", ts2.unwrap().0);

    // 3. 从Unix秒创建 (自动转换为毫秒)
    let ts3 = TimestampKey::from_scalar(&ScalarValue::Int64(Some(1704067200)));
    assert_eq!(ts3.unwrap().0, 1704067200000);

    println!("✅ Timestamp key parsing test passed!");
}

#[test]
fn test_timestamp_array_extraction() {
    use calm::segment::field_store::generic_index::IndexKey;
    use calm::segment::field_store::timestamp_key::TimestampKey;
    use datafusion::arrow::array::ArrayRef;

    // 测试从Int64Array提取
    let int_array: ArrayRef = Arc::new(Int64Array::from(vec![
        1704067200000,
        1704153600000,
        1704240000000,
    ]));

    let iter = TimestampKey::extract_from_array(&int_array);
    let keys: Vec<_> = iter.collect();

    assert_eq!(keys.len(), 3);
    assert_eq!(keys[0].1 .0, 1704067200000);
    assert_eq!(keys[1].1 .0, 1704153600000);
    assert_eq!(keys[2].1 .0, 1704240000000);

    // 测试从StringArray提取
    let str_array: ArrayRef = Arc::new(StringArray::from(vec![
        "2024-01-01 00:00:00",
        "2024-01-02 00:00:00",
        "2024-01-03 00:00:00",
    ]));

    let iter = TimestampKey::extract_from_array(&str_array);
    let keys: Vec<_> = iter.collect();

    assert_eq!(keys.len(), 3);
    println!("Extracted {} timestamp keys from string array", keys.len());

    println!("✅ Timestamp array extraction test passed!");
}
