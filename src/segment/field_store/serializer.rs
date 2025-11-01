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
