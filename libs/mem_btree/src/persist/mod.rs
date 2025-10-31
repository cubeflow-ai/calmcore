mod num_array;
mod num_ser;
mod reader;
mod writer;
mod zigzag;

use std::{
    borrow::Cow,
    collections::LinkedList,
    error::Error,
    fs::{File, OpenOptions},
    io::{BufWriter, Read, Seek, Write},
    path::{Path, PathBuf},
};

use crate::{leaf::Leaf, node::Node, BTree, BTreeType};

const MAGIC_VERSION: &[u8] = &[95, 67];

const DATA_NAME: &str = "data";
const NODE_NAME: &str = "node";

type Result<T> = std::io::Result<T>;

pub type TreeReader<K, V> = reader::TreeReader<K, V>;
pub type TreeWriter = writer::TreeWriter;

pub trait KeySerializer<K, V>: Send + Sync {
    fn serialize_keys<'a>(&self, keys: &'a Vec<K>) -> Cow<'a, [u8]>;
    fn deserialize_keys<'a>(&self, data: &'a [u8]) -> Vec<K>;
    fn serialize_value<'a>(&self, value: &'a V) -> Cow<'a, [u8]>;
    fn deserialize_value<'a>(&self, data: &'a [u8]) -> std::result::Result<V, Box<dyn Error>>;
}
pub trait KVDeserializer<K, V>: Send + Sync {
    fn deserialize_value(&self, v: &[u8]) -> std::result::Result<V, Box<dyn Error>>;
    fn serialize_key<'a>(&self, k: &'a K) -> Cow<'a, [u8]>;
}

fn ___debug(file: PathBuf) {
    let mut vv = Vec::with_capacity(10000);
    File::open(file).unwrap().read_to_end(&mut vv).unwrap();
    println!("+++++++++++:{:?}", vv);
}

#[cfg(test)]
mod tests {}
