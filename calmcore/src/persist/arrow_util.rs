use std::sync::Arc;

use arrow::{
    array::{
        Array, ArrayBuilder, ArrayRef, BooleanArray, BooleanBuilder, Float32Array, Float32Builder,
        Float64Array, Float64Builder, Int64Array, Int64Builder, LargeStringBuilder, StringArray,
        StringBuilder, StructArray, StructBuilder, UInt32Array, UInt64Array,
    },
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use itertools::Itertools;
use proto::core::{value::Kind, ObjectValue, Value};

use crate::{
    index_store::segment_mem::MemSegmentReader,
    util::{CoreError, CoreResult},
    Scope,
};

pub fn make_arrow_schema(reader: &MemSegmentReader) -> Schema {
    let fields = reader
        .index_term
        .iter()
        .map(|(_, t)| t.field())
        .chain(reader.index_fulltext.iter().map(|(_, t)| &t.inner))
        .map(|f| match f.r#type() {
            proto::core::field::Type::Bool => Field::new(f.name.clone(), DataType::Boolean, true),
            proto::core::field::Type::Int => Field::new(f.name.clone(), DataType::Int64, true),
            proto::core::field::Type::Float => Field::new(f.name.clone(), DataType::Float64, true),
            proto::core::field::Type::String => Field::new(f.name.clone(), DataType::Utf8, true),
            proto::core::field::Type::Text => Field::new(f.name.clone(), DataType::LargeUtf8, true),
            proto::core::field::Type::Vector => Field::new(
                f.name.clone(),
                DataType::FixedSizeList(
                    Arc::new(Field::new(f.name.clone(), DataType::Float32, true)),
                    if let Some(proto::core::field::Option::Embedding(o)) = f.option.as_ref() {
                        o.dimension as i32
                    } else {
                        unreachable!()
                    },
                ),
                true,
            ),
            proto::core::field::Type::Geo => todo!(),
        })
        .collect_vec();

    Schema::new(fields)
}

pub fn write_none(schema: &Schema, struct_builder: &mut StructBuilder) {
    for (i, f) in schema.fields().iter().enumerate() {
        match f.data_type() {
            DataType::Boolean => {
                struct_builder
                    .field_builder::<BooleanBuilder>(i)
                    .unwrap()
                    .append_null();
            }
            DataType::Int64 => {
                struct_builder
                    .field_builder::<Int64Builder>(i)
                    .unwrap()
                    .append_null();
            }
            DataType::Float32 => {
                struct_builder
                    .field_builder::<Float32Builder>(i)
                    .unwrap()
                    .append_null();
            }
            DataType::Utf8 => {
                struct_builder
                    .field_builder::<StringBuilder>(i)
                    .unwrap()
                    .append_null();
            }
            DataType::LargeUtf8 => {
                struct_builder
                    .field_builder::<LargeStringBuilder>(i)
                    .unwrap()
                    .append_null();
            }
            DataType::Float64 => {
                struct_builder
                    .field_builder::<Float64Builder>(i)
                    .unwrap()
                    .append_null();
            }
            _ => {
                panic!(
                    "Type mismatch or unsupported data type: {:?}",
                    f.data_type()
                );
            }
        }
    }
}

/// Convert an ObjectValue to an Arrow RecordBatch
pub fn write_object_to_arrow(
    schema: &Schema,
    struct_builder: &mut StructBuilder,
    obj: &ObjectValue,
) -> CoreResult<()> {
    // Fill the builder with data from ObjectValue
    for (name, field) in schema.fields().iter().enumerate() {
        let value = obj.fields.get(field.name());
        add_field_value(struct_builder, name, field.data_type(), value);
    }
    Ok(())
}

fn add_field_value(
    builder: &mut StructBuilder,
    index: usize,
    data_type: &DataType,
    value: Option<&Value>,
) {
    match (data_type, value) {
        (DataType::Boolean, v) => {
            let column = builder.field_builder::<BooleanBuilder>(index).unwrap();

            if let Some(Kind::BoolValue(b)) = v.and_then(|v| v.kind.as_ref()) {
                column.append_value(*b);
            } else {
                column.append_null();
            }
        }
        (DataType::Int64, v) => {
            let column = builder.field_builder::<Int64Builder>(index).unwrap();

            if let Some(Kind::IntValue(i)) = v.and_then(|v| v.kind.as_ref()) {
                column.append_value(*i);
            } else {
                column.append_null();
            }
        }
        (DataType::Float32, v) => {
            let column = builder.field_builder::<Float32Builder>(index).unwrap();

            if let Some(Kind::FloatValue(f)) = v.and_then(|v| v.kind.as_ref()) {
                column.append_value(*f);
            } else {
                column.append_null();
            }
        }
        (DataType::Utf8, v) => {
            let column = builder.field_builder::<StringBuilder>(index).unwrap();

            if let Some(Kind::StringValue(s)) = v.and_then(|v| v.kind.as_ref()) {
                column.append_value(s);
            } else {
                column.append_null();
            }
        }
        (DataType::LargeUtf8, v) => {
            let column = builder.field_builder::<LargeStringBuilder>(index).unwrap();

            if let Some(Kind::StringValue(s)) = v.and_then(|v| v.kind.as_ref()) {
                column.append_value(s);
            } else {
                column.append_null();
            }
        }
        (DataType::Float64, v) => {
            let column = builder.field_builder::<Float64Builder>(index).unwrap();

            if let Some(Kind::FloatValue(f)) = v.and_then(|v| v.kind.as_ref()) {
                column.append_value(*f as f64);
            } else {
                column.append_null();
            }
        }
        _ => {
            panic!("Type mismatch or unsupported data type: {:?}", data_type);
        }
    }
}

pub fn batch_to_record(batch: RecordBatch) -> Vec<ObjectValue> {
    let schema = batch.schema();
    let num_rows = batch.num_rows();

    let mut result = Vec::with_capacity(num_rows);

    for row_idx in 0..num_rows {
        let mut obj = ObjectValue {
            fields: std::collections::HashMap::new(),
        };

        // 处理每一列
        for (col_idx, field) in schema.fields().iter().enumerate() {
            let column = batch.column(col_idx);
            let field_name = field.name();

            // 根据数据类型提取值
            let value = match column.data_type() {
                DataType::Boolean => {
                    let array = column.as_any().downcast_ref::<BooleanArray>().unwrap();
                    if array.is_null(row_idx) {
                        None
                    } else {
                        Some(Value {
                            kind: Some(Kind::BoolValue(array.value(row_idx))),
                        })
                    }
                }
                DataType::Int64 => {
                    let array = column.as_any().downcast_ref::<Int64Array>().unwrap();
                    if array.is_null(row_idx) {
                        None
                    } else {
                        Some(Value {
                            kind: Some(Kind::IntValue(array.value(row_idx))),
                        })
                    }
                }
                DataType::Float64 => {
                    let array = column.as_any().downcast_ref::<Float64Array>().unwrap();
                    if array.is_null(row_idx) {
                        None
                    } else {
                        Some(Value {
                            kind: Some(Kind::FloatValue(array.value(row_idx) as f32)),
                        })
                    }
                }
                DataType::Utf8 => {
                    let array = column.as_any().downcast_ref::<StringArray>().unwrap();
                    if array.is_null(row_idx) {
                        None
                    } else {
                        Some(Value {
                            kind: Some(Kind::StringValue(array.value(row_idx).to_string())),
                        })
                    }
                }
                DataType::LargeUtf8 => {
                    // 对于大字符串类型
                    let array = column
                        .as_any()
                        .downcast_ref::<arrow::array::LargeStringArray>()
                        .unwrap();
                    if array.is_null(row_idx) {
                        None
                    } else {
                        Some(Value {
                            kind: Some(Kind::StringValue(array.value(row_idx).to_string())),
                        })
                    }
                }
                DataType::Float32 => {
                    let array = column.as_any().downcast_ref::<Float32Array>().unwrap();
                    if array.is_null(row_idx) {
                        None
                    } else {
                        Some(Value {
                            kind: Some(Kind::FloatValue(array.value(row_idx))),
                        })
                    }
                }
                _ => {
                    // 对于其他未处理的数据类型，返回None
                    None
                }
            };

            if let Some(v) = value {
                obj.fields.insert(field_name.clone(), v);
            }
        }

        result.push(obj);
    }

    result
}
