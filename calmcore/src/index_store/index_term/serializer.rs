use std::{borrow::Cow, error::Error};

use croaring::{Bitmap, Portable};
use mem_btree::persist;

use crate::util::CoreError;

pub struct TermDeserializer;

impl persist::KVDeserializer<Vec<u8>, Bitmap> for TermDeserializer {
    fn deserialize_value(&self, v: &[u8]) -> std::result::Result<Bitmap, Box<dyn Error>> {
        Bitmap::try_deserialize::<Portable>(v).ok_or_else(|| {
            CoreError::DecodeError("decode bitmap err".to_string(), v.to_vec()).into()
        })
    }

    fn serialize_key<'a>(&self, k: &'a Vec<u8>) -> Cow<'a, [u8]> {
        Cow::Borrowed(k.as_slice())
    }
}

#[derive(Default, Clone)]
pub struct TermSerializer;

impl persist::KVSerializer<Vec<u8>, Bitmap> for TermSerializer {
    fn serialize_key<'a>(&self, k: &'a Vec<u8>) -> std::borrow::Cow<'a, [u8]> {
        Cow::Borrowed(k.as_slice())
    }

    fn serialize_value<'a>(&self, v: &'a Bitmap) -> std::borrow::Cow<'a, [u8]> {
        let mut optimized = v.clone();
        optimized.run_optimize();
        optimized.shrink_to_fit();
        Cow::Owned(optimized.serialize::<Portable>())
    }
}

#[cfg(test)]
mod tests {
    use mem_btree::persist::{KVDeserializer, KVSerializer};

    use super::*;

    #[test]
    fn test_term_serializer() {
        let serializer = TermSerializer::default();
        let key = vec![1, 2, 3, 4];
        let mut bitmap = Bitmap::new();
        bitmap.add(1);
        bitmap.add(100);
        bitmap.add(1000);

        // Test key serialization
        let serialized_key = serializer.serialize_key(&key);
        assert_eq!(serialized_key.as_ref(), &[1, 2, 3, 4]);

        // Test value serialization
        let serialized_value = serializer.serialize_value(&bitmap);
        assert!(serialized_value.len() > 0);
    }

    #[test]
    fn test_term_deserializer() {
        let deserializer = TermDeserializer;
        let key = vec![1, 2, 3, 4];
        let mut bitmap = Bitmap::new();
        bitmap.add(1);
        bitmap.add(100);
        bitmap.add(1000);

        // Test key serialization
        let serialized_key = deserializer.serialize_key(&key);
        assert_eq!(serialized_key.as_ref(), &[1, 2, 3, 4]);

        // Test value deserialization
        let serialized_bitmap = bitmap.serialize::<Portable>();
        let deserialized_bitmap = deserializer.deserialize_value(&serialized_bitmap).unwrap();
        assert_eq!(bitmap, deserialized_bitmap);
    }
}
