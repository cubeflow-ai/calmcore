use std::{
    borrow::Cow,
    sync::{Arc, Mutex, RwLock},
};

use croaring::{Bitmap, Portable};
use mem_btree::persist;
use rkyv::ser::allocator::Arena;

use crate::{entity::TermPosition, util::CoreError};

pub const TERM_INDEX: &str = "term_index";
pub const VECTOR_INDEX: &str = "vector_index";
pub const TERM_POSITION: &str = "term_position";
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

pub struct PositionSerializer {
    arena: Mutex<Arena>,
}

impl PositionSerializer {
    pub fn new() -> Self {
        Self {
            arena: Mutex::new(Arena::new()),
        }
    }
}

impl persist::KVSerializer<String, Arc<RwLock<TermPosition>>> for PositionSerializer {
    fn serialize_key<'a>(&self, k: &'a String) -> Cow<'a, [u8]> {
        Cow::Borrowed(k.as_bytes())
    }

    fn serialize_value<'a>(&self, v: &'a Arc<RwLock<TermPosition>>) -> Cow<'a, [u8]> {
        let arena = &mut *self.arena.lock().unwrap();
        let bytes = v.read().unwrap().serializer(arena).unwrap();
        Cow::Owned(bytes.to_vec()) //TODO use ref
    }
}

pub struct DocDeserializer;

impl persist::KVDeserializer<String, Arc<RwLock<TermPosition>>> for DocDeserializer {
    fn deserialize_value(
        &self,
        v: &[u8],
    ) -> std::result::Result<Arc<RwLock<TermPosition>>, Box<dyn std::error::Error>> {
        // let info = rkyv::access(v).map_err(|e| {
        //     CoreError::DecodeError("decode doc index err".to_string(), v.to_vec()).into()
        // })?;
        // Ok(vec)
        todo!()
    }

    fn serialize_key<'a>(&self, k: &'a String) -> Cow<'a, [u8]> {
        // let mut bytes = vec![0; 4 + k.1.len()];
        // bytes[..4].copy_from_slice(&k.0.to_be_bytes());
        // bytes[4..].copy_from_slice(k.1.as_bytes());
        // Cow::Owned(bytes)
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use mem_btree::persist::{KVDeserializer, KVSerializer};
    use rkyv::ser::allocator::Arena;

    use crate::entity::TermPosting;

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
        todo!()
        // let deserializer = DocDeserializer;
        // let key = (123u32, String::from("test_doc"));
        // let original_value = vec![1u32, 2u32, 3u32];

        // let serialized_key = deserializer.serialize_key(&key);
        // assert_eq!(serialized_key[0..4], 123u32.to_be_bytes());
        // assert_eq!(&serialized_key[4..], b"test_doc");

        // // 修改测试中的序列化方式，使用正确的字节序
        // let mut value_bytes = Vec::with_capacity(original_value.len() * 4);
        // for &value in &original_value {
        //     value_bytes.extend_from_slice(&value.to_ne_bytes());
        // }

        // let deserialized_value = deserializer.deserialize_value(&value_bytes).unwrap();
        // assert_eq!(deserialized_value, original_value);
    }

    #[test]
    fn test_value() {
        todo!()
        // let deserializer = DocDeserializer;
        // let serializer = DocSerializer;
        // let value = vec![1u32, 2u32, 3u32];

        // let bvalue = serializer.serialize_value(&value);
        // let dvalue = deserializer.deserialize_value(&bvalue.as_ref()).unwrap();

        // assert_eq!(dvalue, value);
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

    #[test]
    fn test_doc_index_info() {
        let value = TermPosting {
            name: "example_term".to_string(),
            ids: vec![1, 2, 3, 10, 20],
            offsets: vec![
                vec![0, 5],
                vec![10, 15, 20],
                vec![25],
                vec![100],
                vec![200, 201],
            ],
        };

        let mut arena = Arena::new();

        let data = value.serializer(&mut arena).unwrap();
        println!("Serialized data: {:?}", data);

        let data_ref = TermPosting::deserializer(&data).unwrap();
        println!("Deserialized data: {:?}", data_ref.name.as_str());
    }
}
