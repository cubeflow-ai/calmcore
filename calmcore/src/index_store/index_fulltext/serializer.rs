use std::borrow::Cow;

use croaring::{Bitmap, Portable};
use mem_btree::persist;

use crate::util::CoreError;

pub const TERM_INDEX: &str = "term_index";
pub const DOC_INDEX: &str = "doc_index";
pub const INDEX_INFO: &str = "index_info";

pub struct TokenSerializer;

impl persist::KVSerializer<String, Bitmap> for TokenSerializer {
    fn serialize_key<'a>(&self, k: &'a String) -> std::borrow::Cow<'a, [u8]> {
        Cow::Borrowed(k.as_bytes())
    }

    fn serialize_value<'a>(&self, v: &'a Bitmap) -> std::borrow::Cow<'a, [u8]> {
        let mut optimized = v.clone();
        optimized.run_optimize();
        optimized.shrink_to_fit();
        Cow::Owned(optimized.serialize::<Portable>())
    }
}

pub struct TokenDeserializer;

impl persist::KVDeserializer<String, Bitmap> for TokenDeserializer {
    fn deserialize_value(
        &self,
        v: &[u8],
    ) -> std::result::Result<Bitmap, Box<dyn std::error::Error>> {
        Bitmap::try_deserialize::<Portable>(v).ok_or_else(|| {
            CoreError::DecodeError("decode bitmap err".to_string(), v.to_vec()).into()
        })
    }

    fn serialize_key<'a>(&self, k: &'a String) -> Cow<'a, [u8]> {
        Cow::Borrowed(k.as_bytes())
    }
}

pub struct DocSerializer;

impl persist::KVSerializer<(u32, String), Vec<u32>> for DocSerializer {
    fn serialize_key<'a>(&self, k: &'a (u32, String)) -> Cow<'a, [u8]> {
        let mut bytes = vec![0; 4 + k.1.len()];
        bytes[..4].copy_from_slice(&k.0.to_be_bytes());
        bytes[4..].copy_from_slice(k.1.as_bytes());
        Cow::Owned(bytes)
    }

    fn serialize_value<'a>(&self, v: &'a Vec<u32>) -> Cow<'a, [u8]> {
        let bytes = unsafe {
            std::slice::from_raw_parts(
                v.as_ptr() as *const u8,
                v.len() * std::mem::size_of::<u32>(),
            )
        };
        Cow::Borrowed(bytes)
    }
}

pub struct DocDeserializer;

impl persist::KVDeserializer<(u32, String), Vec<u32>> for DocDeserializer {
    fn deserialize_value(
        &self,
        v: &[u8],
    ) -> std::result::Result<Vec<u32>, Box<dyn std::error::Error>> {
        let len = v.len() / std::mem::size_of::<u32>();
        let mut vec = Vec::with_capacity(len);
        for chunk in v.chunks_exact(4) {
            if let Ok(bytes) = chunk.try_into() {
                // 根据系统字节序选择适当的转换方法
                #[cfg(target_endian = "big")]
                let value = u32::from_be_bytes(bytes);
                #[cfg(target_endian = "little")]
                let value = u32::from_le_bytes(bytes);

                vec.push(value);
            }
        }
        Ok(vec)
    }

    fn serialize_key<'a>(&self, k: &'a (u32, String)) -> Cow<'a, [u8]> {
        let mut bytes = vec![0; 4 + k.1.len()];
        bytes[..4].copy_from_slice(&k.0.to_be_bytes());
        bytes[4..].copy_from_slice(k.1.as_bytes());
        Cow::Owned(bytes)
    }
}

#[cfg(test)]
mod tests {
    use mem_btree::persist::{KVDeserializer, KVSerializer};

    use super::*;
    #[test]
    fn test_term_serializer() {
        let serializer = TokenSerializer;
        let key = String::from("test_key");
        let mut bitmap = Bitmap::new();
        bitmap.add(1);
        bitmap.add(100);
        bitmap.add(1000);

        let serialized_key = serializer.serialize_key(&key);
        assert_eq!(serialized_key.as_ref(), b"test_key");

        let serialized_value = serializer.serialize_value(&bitmap);
        assert!(serialized_value.len() > 0);
    }

    #[test]
    fn test_term_deserializer() {
        let deserializer = TokenDeserializer;
        let key = String::from("test_key");
        let mut bitmap = Bitmap::new();
        bitmap.add(1);
        bitmap.add(100);
        bitmap.add(1000);

        let serialized_key = deserializer.serialize_key(&key);
        assert_eq!(serialized_key.as_ref(), b"test_key");

        let serialized_bitmap = bitmap.serialize::<Portable>();
        let deserialized_bitmap = deserializer.deserialize_value(&serialized_bitmap).unwrap();
        assert_eq!(bitmap, deserialized_bitmap);
    }

    #[test]
    fn test_doc_serializer() {
        let serializer = DocSerializer;
        let key = (123u32, String::from("test_doc"));
        let value = vec![1u32, 2u32, 3u32];

        let serialized_key = serializer.serialize_key(&key);
        assert_eq!(serialized_key[0..4], 123u32.to_be_bytes());
        assert_eq!(&serialized_key[4..], b"test_doc");

        let serialized_value = serializer.serialize_value(&value);
        assert_eq!(serialized_value.len(), 12); // 3 * sizeof(u32)
    }

    #[test]
    fn test_doc_deserializer() {
        let deserializer = DocDeserializer;
        let key = (123u32, String::from("test_doc"));
        let original_value = vec![1u32, 2u32, 3u32];

        let serialized_key = deserializer.serialize_key(&key);
        assert_eq!(serialized_key[0..4], 123u32.to_be_bytes());
        assert_eq!(&serialized_key[4..], b"test_doc");

        // 修改测试中的序列化方式，使用正确的字节序
        let mut value_bytes = Vec::with_capacity(original_value.len() * 4);
        for &value in &original_value {
            value_bytes.extend_from_slice(&value.to_ne_bytes());
        }

        let deserialized_value = deserializer.deserialize_value(&value_bytes).unwrap();
        assert_eq!(deserialized_value, original_value);
    }

    #[test]
    fn test_value() {
        let deserializer = DocDeserializer;
        let serializer = DocSerializer;
        let value = vec![1u32, 2u32, 3u32];

        let bvalue = serializer.serialize_value(&value);
        let dvalue = deserializer.deserialize_value(&bvalue.as_ref()).unwrap();

        assert_eq!(dvalue, value);
    }

    #[test]
    fn test_endianness() {
        println!(
            "Current system endianness: {}",
            if cfg!(target_endian = "big") {
                "big-endian"
            } else {
                "little-endian"
            }
        );

        // 可以添加更多的字节序相关测试...
    }
}
