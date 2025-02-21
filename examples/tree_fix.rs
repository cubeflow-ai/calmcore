use std::{borrow::Cow, error::Error, path::PathBuf};

use mem_btree::{
    persist::{KVDeserializer, KVSerializer, TreeReader, TreeWriter},
    BTree,
};

struct DefaultSerializer;

impl KVSerializer<Vec<u8>, Vec<u8>> for DefaultSerializer {
    fn serialize_key<'a>(&self, k: &'a Vec<u8>) -> Cow<'a, [u8]> {
        Cow::Borrowed(k.as_slice())
    }

    fn serialize_value<'a>(&self, v: &'a Vec<u8>) -> Cow<'a, [u8]> {
        Cow::Borrowed(v.as_slice())
    }
}

impl KVDeserializer<Vec<u8>, Vec<u8>> for DefaultSerializer {
    fn deserialize_value(&self, v: &[u8]) -> std::result::Result<Vec<u8>, Box<dyn Error>> {
        Ok(v.to_vec())
    }

    fn serialize_key<'a>(&self, k: &'a Vec<u8>) -> Cow<'a, [u8]> {
        Cow::Borrowed(k.as_slice())
    }
}

pub fn main() {
    let dir = PathBuf::from("data");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let total = 50_000;

    let start = 10_000;

    // Create tree and insert data
    let mut tree = BTree::new(128);

    for i in start..total as i32 {
        tree.put(i.to_be_bytes().to_vec(), i.to_be_bytes().to_vec());
        if i % 10000 == 0 {
            println!("inserted: {}", i);
        }
    }

    if 2 == 1 {
        println!("xxxxx: {}", i32::from_be_bytes([0, 0, 194, 145]));
        panic!("stop");
    }

    // Persist tree to disk
    let writer = TreeWriter::new(tree, 0, Box::new(DefaultSerializer {}));
    writer.persist(&dir).unwrap();

    let tree: TreeReader<Vec<u8>, Vec<u8>> =
        TreeReader::new(&dir, Box::new(DefaultSerializer {})).unwrap();

    for i in 0..start as i32 {
        eprintln!("=====+++++++++++++++++++++++==========={}", i);
        assert!(tree.get(&i.to_be_bytes().to_vec()).is_none());
    }

    for i in start..total as i32 {
        eprintln!("=====+++++++++++++++++++++++==========={}", i);
        assert!(tree.get(&i.to_be_bytes().to_vec()).is_some());
    }

    for i in total..total + 1000 as i32 {
        eprintln!("=====+++++++++++++++++++++++==========={}", i);
        assert!(tree.get(&i.to_be_bytes().to_vec()).is_none());
    }

    let s = 0_i32;

    assert!(tree.get(&s.to_be_bytes().to_vec()).is_none());

    let s = total as i32 + 100;

    assert!(tree.get(&s.to_be_bytes().to_vec()).is_none());
}
