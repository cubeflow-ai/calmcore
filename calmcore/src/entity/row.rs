// use half::f16;
// use parquet::data_type::Decimal;

// pub struct Row {
//     pub name: String,
//     pub fields: Vec<Option<Value>>,
// }

// #[derive(Clone, Debug, PartialEq)]
// pub enum Value {
//     Null,
//     Bool(bool),
//     Byte(i8),
//     Short(i16),
//     Int(i32),
//     Long(i64),
//     UByte(u8),
//     UShort(u16),
//     UInt(u32),
//     ULong(u64),
//     Float16(f16),
//     Float(f32),
//     Double(f64),
//     Decimal(Decimal),
//     Str(String),
//     Bytes(Vec<u8>),
//     Vector32(Vec<f32>),
//     Vector16(Vec<f16>),
//     Date(i32),
//     TimestampMillis(i64),
//     TimestampMicros(i64),
//     List(Vec<Value>),
//     Map(Vec<(Value, Value)>),
// }
