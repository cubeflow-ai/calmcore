use ordered_float::OrderedFloat;

pub trait RoaringSerializer<T> {
    fn serialize_element(&self, value: &T) -> Vec<u32>;
    fn deserialize_element(&self, value: u32) -> T;
}

pub struct I32RoaringSerializer;
pub struct I64RoaringSerializer;
pub struct U32RoaringSerializer;
pub struct F32RoaringSerializer;
pub struct F64RoaringSerializer;

impl Default for I32RoaringSerializer {
    fn default() -> Self {
        Self {}
    }
}

impl Default for I64RoaringSerializer {
    fn default() -> Self {
        Self {}
    }
}

impl Default for U32RoaringSerializer {
    fn default() -> Self {
        Self {}
    }
}

impl Default for F32RoaringSerializer {
    fn default() -> Self {
        Self {}
    }
}

impl Default for F64RoaringSerializer {
    fn default() -> Self {
        Self {}
    }
}

impl RoaringSerializer<i32> for I32RoaringSerializer {
    fn serialize_element(&self, value: &i32) -> Vec<u32> {
        vec![*value as u32]
    }

    fn deserialize_element(&self, value: u32) -> i32 {
        value as i32
    }
}

impl RoaringSerializer<i64> for I64RoaringSerializer {
    fn serialize_element(&self, value: &i64) -> Vec<u32> {
        let bytes = value.to_be_bytes();
        vec![
            u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]),
            u32::from_be_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]),
        ]
    }

    fn deserialize_element(&self, value: u32) -> i64 {
        value as i64
    }
}

impl RoaringSerializer<u32> for U32RoaringSerializer {
    fn serialize_element(&self, value: &u32) -> Vec<u32> {
        vec![*value]
    }

    fn deserialize_element(&self, value: u32) -> u32 {
        value
    }
}

impl RoaringSerializer<OrderedFloat<f32>> for F32RoaringSerializer {
    fn serialize_element(&self, value: &OrderedFloat<f32>) -> Vec<u32> {
        vec![value.0.to_bits()]
    }

    fn deserialize_element(&self, value: u32) -> OrderedFloat<f32> {
        OrderedFloat(f32::from_bits(value))
    }
}

impl RoaringSerializer<OrderedFloat<f64>> for F64RoaringSerializer {
    fn serialize_element(&self, value: &OrderedFloat<f64>) -> Vec<u32> {
        let bits = value.0.to_bits();
        let bytes = bits.to_be_bytes();
        vec![
            u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]),
            u32::from_be_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]),
        ]
    }

    fn deserialize_element(&self, value: u32) -> OrderedFloat<f64> {
        OrderedFloat(f64::from_bits(value as u64))
    }
}

// ============== 通用的 WriteSerializer 和 ReadSerializer 实现 ==============
// 所有数字类型 -> RoaringBitmap 的序列化都可以复用这个实现

use mem_btree::persist::{ReadSerializer, WriteSerializer};
use roaring::RoaringBitmap;
use std::borrow::Cow;

// 为 i32 -> RoaringBitmap 实现
impl WriteSerializer<i32, RoaringBitmap> for I32RoaringSerializer {
    fn serialize_keys<'a>(&self, keys: &'a Vec<i32>) -> Cow<'a, [u8]> {
        use mem_btree::persist::num_ser::i64_coder;
        let i64_keys: Vec<i64> = keys.iter().map(|&k| k as i64).collect();
        let mut buf = Vec::new();
        i64_coder::write_delta(&mut buf, &i64_keys).expect("write delta failed");
        Cow::Owned(buf)
    }

    fn serialize_value<'a>(&self, value: &'a RoaringBitmap) -> Cow<'a, [u8]> {
        let bytes = mem_btree::persist::value_codec::encode_roaring_from_bitmap(value);
        Cow::Owned(bytes)
    }
}

impl ReadSerializer<i32, RoaringBitmap> for I32RoaringSerializer {
    fn deserialize_keys<'a>(&self, data: &'a [u8]) -> Vec<i32> {
        use mem_btree::persist::num_ser::i64_coder;
        i64_coder::read_delta(&data)
            .iter()
            .map(|&k| k as i32)
            .collect()
    }

    fn deserialize_value<'a>(
        &self,
        data: &'a [u8],
    ) -> Result<RoaringBitmap, Box<dyn std::error::Error>> {
        mem_btree::persist::value_codec::decode_roaring_from_bytes(data)
    }
}

// 为 i64 -> RoaringBitmap 实现
impl WriteSerializer<i64, RoaringBitmap> for I64RoaringSerializer {
    fn serialize_keys<'a>(&self, keys: &'a Vec<i64>) -> Cow<'a, [u8]> {
        use mem_btree::persist::num_ser::i64_coder;
        let mut buf = Vec::new();
        i64_coder::write_delta(&mut buf, keys).expect("write delta failed");
        Cow::Owned(buf)
    }

    fn serialize_value<'a>(&self, value: &'a RoaringBitmap) -> Cow<'a, [u8]> {
        let bytes = mem_btree::persist::value_codec::encode_roaring_from_bitmap(value);
        Cow::Owned(bytes)
    }
}

impl ReadSerializer<i64, RoaringBitmap> for I64RoaringSerializer {
    fn deserialize_keys<'a>(&self, data: &'a [u8]) -> Vec<i64> {
        use mem_btree::persist::num_ser::i64_coder;
        i64_coder::read_delta(&data)
    }

    fn deserialize_value<'a>(
        &self,
        data: &'a [u8],
    ) -> Result<RoaringBitmap, Box<dyn std::error::Error>> {
        mem_btree::persist::value_codec::decode_roaring_from_bytes(data)
    }
}

// 为 u32 -> RoaringBitmap 实现
impl WriteSerializer<u32, RoaringBitmap> for U32RoaringSerializer {
    fn serialize_keys<'a>(&self, keys: &'a Vec<u32>) -> Cow<'a, [u8]> {
        use mem_btree::persist::num_ser::i64_coder;
        let i64_keys: Vec<i64> = keys.iter().map(|&k| k as i64).collect();
        let mut buf = Vec::new();
        i64_coder::write_delta(&mut buf, &i64_keys).expect("write delta failed");
        Cow::Owned(buf)
    }

    fn serialize_value<'a>(&self, value: &'a RoaringBitmap) -> Cow<'a, [u8]> {
        let bytes = mem_btree::persist::value_codec::encode_roaring_from_bitmap(value);
        Cow::Owned(bytes)
    }
}

impl ReadSerializer<u32, RoaringBitmap> for U32RoaringSerializer {
    fn deserialize_keys<'a>(&self, data: &'a [u8]) -> Vec<u32> {
        use mem_btree::persist::num_ser::i64_coder;
        i64_coder::read_delta(&data)
            .iter()
            .map(|&k| k as u32)
            .collect()
    }

    fn deserialize_value<'a>(
        &self,
        data: &'a [u8],
    ) -> Result<RoaringBitmap, Box<dyn std::error::Error>> {
        mem_btree::persist::value_codec::decode_roaring_from_bytes(data)
    }
}

// 为 OrderedFloat<f32> -> RoaringBitmap 实现
impl WriteSerializer<OrderedFloat<f32>, RoaringBitmap> for F32RoaringSerializer {
    fn serialize_keys<'a>(&self, keys: &'a Vec<OrderedFloat<f32>>) -> Cow<'a, [u8]> {
        use mem_btree::persist::num_ser::i64_coder;
        let i64_keys: Vec<i64> = keys.iter().map(|k| k.0.to_bits() as i64).collect();
        let mut buf = Vec::new();
        i64_coder::write_delta(&mut buf, &i64_keys).expect("write delta failed");
        Cow::Owned(buf)
    }

    fn serialize_value<'a>(&self, value: &'a RoaringBitmap) -> Cow<'a, [u8]> {
        let bytes = mem_btree::persist::value_codec::encode_roaring_from_bitmap(value);
        Cow::Owned(bytes)
    }
}

impl ReadSerializer<OrderedFloat<f32>, RoaringBitmap> for F32RoaringSerializer {
    fn deserialize_keys<'a>(&self, data: &'a [u8]) -> Vec<OrderedFloat<f32>> {
        use mem_btree::persist::num_ser::i64_coder;
        i64_coder::read_delta(&data)
            .iter()
            .map(|&k| OrderedFloat(f32::from_bits(k as u32)))
            .collect()
    }

    fn deserialize_value<'a>(
        &self,
        data: &'a [u8],
    ) -> Result<RoaringBitmap, Box<dyn std::error::Error>> {
        mem_btree::persist::value_codec::decode_roaring_from_bytes(data)
    }
}

// 为 OrderedFloat<f64> -> RoaringBitmap 实现
impl WriteSerializer<OrderedFloat<f64>, RoaringBitmap> for F64RoaringSerializer {
    fn serialize_keys<'a>(&self, keys: &'a Vec<OrderedFloat<f64>>) -> Cow<'a, [u8]> {
        use mem_btree::persist::num_ser::i64_coder;
        let i64_keys: Vec<i64> = keys.iter().map(|k| k.0.to_bits() as i64).collect();
        let mut buf = Vec::new();
        i64_coder::write_delta(&mut buf, &i64_keys).expect("write delta failed");
        Cow::Owned(buf)
    }

    fn serialize_value<'a>(&self, value: &'a RoaringBitmap) -> Cow<'a, [u8]> {
        let bytes = mem_btree::persist::value_codec::encode_roaring_from_bitmap(value);
        Cow::Owned(bytes)
    }
}

impl ReadSerializer<OrderedFloat<f64>, RoaringBitmap> for F64RoaringSerializer {
    fn deserialize_keys<'a>(&self, data: &'a [u8]) -> Vec<OrderedFloat<f64>> {
        use mem_btree::persist::num_ser::i64_coder;
        i64_coder::read_delta(&data)
            .iter()
            .map(|&k| OrderedFloat(f64::from_bits(k as u64)))
            .collect()
    }

    fn deserialize_value<'a>(
        &self,
        data: &'a [u8],
    ) -> Result<RoaringBitmap, Box<dyn std::error::Error>> {
        mem_btree::persist::value_codec::decode_roaring_from_bytes(data)
    }
}
