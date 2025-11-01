// mod num_array;
pub mod num_ser;
mod reader;
mod writer;
pub mod zigzag;

use roaring::RoaringBitmap;
use std::{borrow::Cow, error::Error, path::Path};

const MAGIC_VERSION: &[u8] = &[95, 67];

const DATA_NAME: &str = "data";
const NODE_NAME: &str = "node";

type Result<T> = std::io::Result<T>;

pub type TreeReader<K, V, R> = reader::TreeReader<K, V, R>;
pub type TreeWriter = writer::TreeWriter;

pub trait KeySerializer<K, V, R>: Send + Sync {
    fn serialize_keys<'a>(&self, keys: &'a Vec<K>) -> Result<Vec<u8>>;
    fn deserialize_keys<'a>(&self, data: &'a [u8]) -> Result<Vec<K>>;
    fn serialize_value<'a>(&self, value: &'a V) -> Cow<'a, [u8]>;
    fn deserialize_value<'a>(&self, data: &'a [u8]) -> std::result::Result<R, Box<dyn Error>>;
}
pub trait KVDeserializer<K, V>: Send + Sync {
    fn deserialize_value(&self, v: &[u8]) -> std::result::Result<V, Box<dyn Error>>;
    fn serialize_key<'a>(&self, k: &'a K) -> Cow<'a, [u8]>;
}
