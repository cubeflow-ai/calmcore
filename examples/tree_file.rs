use mem_btree::persist::{KVDeserializer, TreeReader};
use std::{borrow::Cow, error::Error, path::PathBuf};

pub struct TermDeserializer;

impl KVDeserializer<Vec<u8>, String> for TermDeserializer {
    fn deserialize_value(&self, v: &[u8]) -> std::result::Result<String, Box<dyn Error>> {
        let v = String::from_utf8_lossy(v);
        Ok(v.to_string())
    }

    fn serialize_key<'a>(&self, k: &'a Vec<u8>) -> Cow<'a, [u8]> {
        Cow::Borrowed(k.as_slice())
    }
}

pub fn main() {
    let dir = PathBuf::from("validate_data/validate_test/segments/9571002-10000001/_source");
    let deserializer = Box::new(TermDeserializer {});
    let tree = TreeReader::new(&dir, deserializer).unwrap();

    let v = tree.get(&vec![0, 6, 136, 2]);

    println!("{:?}", v);

    // let mut iter = tree.iter();

    // iter.seek(&vec![0, 6, 136, 0]);

    // while let Some((k, v)) = iter.next() {
    //     println!("{:?} {:?}", k, v);
    //     let s = &k.to_vec();
    // }
}
