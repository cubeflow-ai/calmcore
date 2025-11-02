// mod num_array;
pub mod num_ser;
mod reader;
pub mod value_codec;
mod writer;
pub mod zigzag;

use std::{borrow::Cow, error::Error, path::Path};

const MAGIC_VERSION: &[u8] = &[95, 67];

const DATA_NAME: &str = "data";
const NODE_NAME: &str = "node";

type Result<T> = std::io::Result<T>;

pub type TreeReader<K, V, R> = reader::TreeReader<K, V, R>;
pub type TreeWriter = writer::TreeWriter;

pub trait WriteSerializer<K, V>: Send + Sync {
    fn serialize_keys<'a>(&self, keys: &'a Vec<K>) -> Cow<'a, [u8]>;
    fn serialize_value<'a>(&self, value: &'a V) -> Cow<'a, [u8]>;
}

pub trait ReadSerializer<K, R>: Send + Sync {
    fn deserialize_keys<'a>(&self, data: &'a [u8]) -> Vec<K>;
    fn deserialize_value<'a>(&self, data: &'a [u8]) -> std::result::Result<R, Box<dyn Error>>;
}
