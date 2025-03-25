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

pub type TreeWriter<K, V> = writer::TreeWriter<K, V>;

pub trait KVSerializer<K, V>: Send + Sync {
    fn serialize_key<'a>(&self, k: &'a K) -> Cow<'a, [u8]>;
    fn serialize_value<'a>(&self, v: &'a V) -> Cow<'a, [u8]>;
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
mod tests {

    use flate2::read;

    use super::*;

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

    #[test]
    fn test_get_simple_example() {
        let dir = PathBuf::from("test/test_get_simple_example");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        // Create tree and insert data
        let mut tree = BTree::new(128);

        for i in 0..1_000 as i32 {
            let i = i * 2;
            tree.put(i.to_be_bytes().to_vec(), i.to_be_bytes().to_vec());
        }

        // Persist tree to disk
        let writer = TreeWriter::new(tree, 0, Box::new(DefaultSerializer {}));
        writer.persist(&dir).unwrap();

        // Load tree from disk
        let reader = TreeReader::new(&dir, Box::new(DefaultSerializer {})).unwrap();

        assert_eq!(
            10_i32.to_be_bytes().to_vec(),
            reader.get(&10_i32.to_be_bytes().to_vec()).unwrap()
        );
    }

    #[test]
    fn test_get_fix_simple_example() {
        let dir = PathBuf::from("test/test_get_simple_example");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        // Create tree and insert data
        let mut tree = BTree::new(128);

        for i in 0..1_000 as i32 {
            let i = i * 2;
            tree.put(i.to_be_bytes().to_vec(), i.to_be_bytes().to_vec());
        }

        // Persist tree to disk
        let writer = TreeWriter::new(tree, 4, Box::new(DefaultSerializer {}));
        writer.persist(&dir).unwrap();

        // Load tree from disk
        let reader = TreeReader::new(&dir, Box::new(DefaultSerializer {})).unwrap();

        assert_eq!(
            10_i32.to_be_bytes().to_vec(),
            reader.get(&10_i32.to_be_bytes().to_vec()).unwrap()
        );
    }

    #[test]
    fn test_tree_reader_and_iter() {
        let dir = PathBuf::from("test/test_tree_reader_and_iter");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        // Create tree and insert data
        let mut tree = BTree::new(128);

        for i in 0..10_000 as i32 {
            let i = i * 2;
            tree.put(i.to_be_bytes().to_vec(), i.to_be_bytes().to_vec());
        }

        // Persist tree to disk
        let writer = TreeWriter::new(tree, 0, Box::new(DefaultSerializer {}));
        writer.persist(&dir).unwrap();

        // Load tree from disk
        let reader = TreeReader::new(&dir, Box::new(DefaultSerializer {})).unwrap();

        // Verify contents
        let mut iter = reader.iter().unwrap();

        for i in 0..10_000 {
            let (k1, v1) = iter.next().unwrap().unwrap();
            assert_eq!(i32::from_be_bytes(k1.try_into().unwrap()), i * 2);
            assert_eq!(i32::from_be_bytes(v1.try_into().unwrap()), i * 2);
        }

        assert!(iter.next().unwrap().is_none());

        iter.seek_prev(&i32::MAX.to_be_bytes().to_vec()).unwrap();
        for i in 0..10000 {
            let (k1, v1) = iter.prev().unwrap().unwrap();
            assert_eq!(
                i32::from_be_bytes(k1.try_into().unwrap()),
                (10000 - i - 1) * 2
            );
            assert_eq!(
                i32::from_be_bytes(v1.try_into().unwrap()),
                (10000 - i - 1) * 2
            );
        }
        assert!(iter.prev().unwrap().is_none());

        iter.seek_prev(&5_i32.to_be_bytes().to_vec()).unwrap();

        let (k1, v1) = iter.prev().unwrap().unwrap();
        assert_eq!(i32::from_be_bytes(k1.try_into().unwrap()), 4);
        assert_eq!(i32::from_be_bytes(v1.try_into().unwrap()), 4);
        let (k1, v1) = iter.prev().unwrap().unwrap();
        assert_eq!(i32::from_be_bytes(k1.try_into().unwrap()), 2);
        assert_eq!(i32::from_be_bytes(v1.try_into().unwrap()), 2);
        let (k1, v1) = iter.prev().unwrap().unwrap();
        assert_eq!(i32::from_be_bytes(k1.try_into().unwrap()), 0);
        assert_eq!(i32::from_be_bytes(v1.try_into().unwrap()), 0);

        assert_eq!(iter.prev().unwrap(), None);

        // Test seek_prev with empty key (should go to last element)
        iter.seek_last().unwrap();
        let (k1, v1) = iter.prev().unwrap().unwrap();
        assert_eq!(i32::from_be_bytes(k1.try_into().unwrap()), 19998);
        assert_eq!(i32::from_be_bytes(v1.try_into().unwrap()), 19998);

        // Test seek_prev with key larger than any existing key
        iter.seek_prev(&(20000_i32).to_be_bytes().to_vec()).unwrap();
        let (k1, v1) = iter.prev().unwrap().unwrap();
        assert_eq!(i32::from_be_bytes(k1.try_into().unwrap()), 19998);
        assert_eq!(i32::from_be_bytes(v1.try_into().unwrap()), 19998);

        // Test seek_prev with non-existent key (between existing keys)
        iter.seek_prev(&3_i32.to_be_bytes().to_vec()).unwrap();
        let (k1, v1) = iter.prev().unwrap().unwrap();
        assert_eq!(i32::from_be_bytes(k1.try_into().unwrap()), 2);
        assert_eq!(i32::from_be_bytes(v1.try_into().unwrap()), 2);

        // Test consecutive prev calls after seek_prev
        iter.seek_prev(&10_i32.to_be_bytes().to_vec()).unwrap();
        let (k1, v1) = iter.prev().unwrap().unwrap();
        assert_eq!(i32::from_be_bytes(k1.try_into().unwrap()), 10);
        let (k1, v1) = iter.prev().unwrap().unwrap();
        assert_eq!(i32::from_be_bytes(k1.try_into().unwrap()), 8);
        let (k1, v1) = iter.prev().unwrap().unwrap();
        assert_eq!(i32::from_be_bytes(k1.try_into().unwrap()), 6);
        let (k1, v1) = iter.prev().unwrap().unwrap();
        assert_eq!(i32::from_be_bytes(k1.try_into().unwrap()), 4);

        // Test forward iteration from beginning
        iter.seek_first().unwrap();
        let (k1, v1) = iter.next().unwrap().unwrap();
        assert_eq!(i32::from_be_bytes(k1.try_into().unwrap()), 0);
        assert_eq!(i32::from_be_bytes(v1.try_into().unwrap()), 0);
        let (k1, v1) = iter.next().unwrap().unwrap();
        assert_eq!(i32::from_be_bytes(k1.try_into().unwrap()), 2);
        assert_eq!(i32::from_be_bytes(v1.try_into().unwrap()), 2);
        let (k1, v1) = iter.next().unwrap().unwrap();
        assert_eq!(i32::from_be_bytes(k1.try_into().unwrap()), 4);
        assert_eq!(i32::from_be_bytes(v1.try_into().unwrap()), 4);

        // Test seek with key larger than any existing key
        iter.seek(&(20000_i32).to_be_bytes().to_vec()).unwrap();
        assert!(iter.next().unwrap().is_none());

        // Test seek with non-existent key (between existing keys)
        iter.seek(&3_i32.to_be_bytes().to_vec()).unwrap();
        let (k1, v1) = iter.next().unwrap().unwrap();
        assert_eq!(i32::from_be_bytes(k1.try_into().unwrap()), 4);
        assert_eq!(i32::from_be_bytes(v1.try_into().unwrap()), 4);

        // Test consecutive next calls after seek
        iter.seek(&8_i32.to_be_bytes().to_vec()).unwrap();
        let (k1, v1) = iter.next().unwrap().unwrap();
        assert_eq!(i32::from_be_bytes(k1.try_into().unwrap()), 8);
        let (k1, v1) = iter.next().unwrap().unwrap();
        assert_eq!(i32::from_be_bytes(k1.try_into().unwrap()), 10);
        let (k1, v1) = iter.next().unwrap().unwrap();
        assert_eq!(i32::from_be_bytes(k1.try_into().unwrap()), 12);
        let (k1, v1) = iter.next().unwrap().unwrap();
        assert_eq!(i32::from_be_bytes(k1.try_into().unwrap()), 14);

        // Test seek with empty key (should go to first element)
        iter.seek(&0_i32.to_be_bytes().to_vec()).unwrap();
        let (k1, v1) = iter.next().unwrap().unwrap();
        assert_eq!(i32::from_be_bytes(k1.try_into().unwrap()), 0);
        assert_eq!(i32::from_be_bytes(v1.try_into().unwrap()), 0);
    }

    #[test]
    fn test_tree_fix_len() {
        let dir = PathBuf::from("test/test_tree_fix_len");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let total = 50_000;

        // Create tree and insert data
        let mut tree = BTree::new(32);

        for i in 0..total as i32 {
            tree.put(i.to_be_bytes().to_vec(), i.to_be_bytes().to_vec());
            if i % 10000 == 0 {
                println!("inserted: {}", i);
            }
        }

        for i in 0..total as i32 {
            if i % 10000 == 0 {
                println!("get: {}", i);
            }
            match tree.get(&i.to_be_bytes().to_vec()) {
                Some(v) => {
                    assert_eq!(&i.to_be_bytes().to_vec(), v);
                }
                None => {
                    panic!("key not found: {}", i);
                }
            }
        }

        // Persist tree to disk
        let writer = TreeWriter::new(tree, 4, Box::new(DefaultSerializer {}));
        writer.persist(&dir).unwrap();

        // Load tree from disk
        let reader = TreeReader::new(&dir, Box::new(DefaultSerializer {})).unwrap();

        for i in 0..total as i32 {
            if i % 10000 == 0 {
                println!("get: {}", i);
            }
            match reader.get(&i.to_be_bytes().to_vec()) {
                Some(v) => {
                    assert_eq!(&i.to_be_bytes().to_vec(), &v);
                }
                None => {
                    panic!("key not found: {}", i);
                }
            }
        }
    }

    #[test]
    fn test_tree_var_len() {
        let dir = PathBuf::from("test/test_tree_var_len");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let total = 50_000;

        // Create tree and insert data
        let mut tree = BTree::new(128);

        for i in 0..total as i32 {
            tree.put(i.to_be_bytes().to_vec(), i.to_be_bytes().to_vec());
            if i % 10000 == 0 {
                println!("inserted: {}", i);
            }
        }

        for i in 0..total as i32 {
            if i % 10000 == 0 {
                println!("get: {}", i);
            }
            match tree.get(&i.to_be_bytes().to_vec()) {
                Some(v) => {
                    assert_eq!(&i.to_be_bytes().to_vec(), v);
                }
                None => {
                    panic!("key not found: {}", i);
                }
            }
        }

        // Persist tree to disk
        let writer = TreeWriter::new(tree, 0, Box::new(DefaultSerializer {}));
        writer.persist(&dir).unwrap();

        // Load tree from disk
        let reader = TreeReader::new(&dir, Box::new(DefaultSerializer {})).unwrap();

        for i in 0..total as i32 {
            if i % 10000 == 0 {
                println!("get: {}", i);
            }
            match reader.get(&i.to_be_bytes().to_vec()) {
                Some(v) => {
                    assert_eq!(&i.to_be_bytes().to_vec(), &v);
                }
                None => {
                    panic!("key not found: {}", i);
                }
            }
        }
    }

    #[test]
    pub fn test_get() {
        let dir = PathBuf::from("test/test_get");
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

        // Persist tree to disk
        let writer = TreeWriter::new(tree, 0, Box::new(DefaultSerializer {}));
        writer.persist(&dir).unwrap();

        let tree: TreeReader<Vec<u8>, Vec<u8>> =
            TreeReader::new(&dir, Box::new(DefaultSerializer {})).unwrap();

        let v = tree.get(&(start + 1).to_be_bytes().to_vec());
        println!("get: {:?}", v);

        for i in start..total as i32 {
            assert!(tree.get(&i.to_be_bytes().to_vec()).is_some());
        }

        let s = 14033_i32;

        assert!(tree.get(&s.to_be_bytes().to_vec()).is_some());
    }

    #[test]
    fn test_mget_simple() {
        let dir = PathBuf::from("test/test_mget_simple");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        // Create tree and insert data
        let mut tree = BTree::new(128);

        for i in 0..1000 as i32 {
            tree.put(i.to_be_bytes().to_vec(), i.to_be_bytes().to_vec());
        }

        let writer = TreeWriter::new(tree, 0, Box::new(DefaultSerializer {}));
        writer.persist(&dir).unwrap();

        let reader = TreeReader::new(&dir, Box::new(DefaultSerializer {})).unwrap();

        // Test existing keys
        let keys: Vec<Vec<u8>> = vec![0, 1, 2, 3, 4]
            .into_iter()
            .map(|i: i32| i.to_be_bytes().to_vec())
            .collect();

        let results = reader.mget(&keys);

        for (i, result) in results.iter().enumerate() {
            let i = i as i32;
            match result {
                Some(v) => {
                    assert_eq!(&i.to_be_bytes().to_vec(), v);
                }
                None => panic!("Key {} should exist", i),
            }
        }

        // Test mix of existing and non-existing keys
        let keys: Vec<Vec<u8>> = vec![998, 999, 1000, 1001]
            .into_iter()
            .map(|i: i32| i.to_be_bytes().to_vec())
            .collect();

        let results = reader.mget(&keys);
        assert!(results[0].is_some()); // 998 exists
        assert!(results[1].is_some()); // 999 exists
        assert!(results[2].is_none()); // 1000 doesn't exist
        assert!(results[3].is_none()); // 1001 doesn't exist
    }

    #[test]
    fn test_mget_ordered() {
        let dir = PathBuf::from("test/test_mget_ordered");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let mut tree = BTree::new(128);

        // 插入0-999的数据
        for i in 0..1000 as i32 {
            tree.put(i.to_be_bytes().to_vec(), i.to_be_bytes().to_vec());
        }

        let writer = TreeWriter::new(tree, 0, Box::new(DefaultSerializer {}));
        writer.persist(&dir).unwrap();

        let reader = TreeReader::new(&dir, Box::new(DefaultSerializer {})).unwrap();

        // 测试连续的key
        let keys: Vec<Vec<u8>> = (10..20).map(|i: i32| i.to_be_bytes().to_vec()).collect();

        let results = reader.mget(&keys);

        // 验证结果
        for (i, result) in results.iter().enumerate() {
            let expected = (i as i32) + 10;
            match result {
                Some(v) => {
                    assert_eq!(expected.to_be_bytes().to_vec(), *v);
                }
                None => panic!("Key {} should exist", expected),
            }
        }

        // 测试部分存在部分不存在的连续key
        let keys: Vec<Vec<u8>> = (998..1002).map(|i: i32| i.to_be_bytes().to_vec()).collect();

        let results = reader.mget(&keys);
        assert!(results[0].is_some()); // 998 exists
        assert!(results[1].is_some()); // 999 exists
        assert!(results[2].is_none()); // 1000 不存在
        assert!(results[3].is_none()); // 1001 不存在
    }

    #[test]
    fn test_mget_large_scale() {
        let dir = PathBuf::from("test/test_mget_large_scale");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let mut tree = BTree::new(128);

        // 构造有间隔的数据: 0,2,4,...,1998 (跳过奇数)
        for i in (0..1000).map(|x: i32| x * 2) {
            tree.put(i.to_be_bytes().to_vec(), i.to_be_bytes().to_vec());
        }

        let writer = TreeWriter::new(tree, 0, Box::new(DefaultSerializer {}));
        writer.persist(&dir).unwrap();

        let reader = TreeReader::new(&dir, Box::new(DefaultSerializer {})).unwrap();

        // 测试1: 连续查询包含存在和不存在的键
        let keys: Vec<Vec<u8>> = (0..10).map(|i: i32| i.to_be_bytes().to_vec()).collect();
        let results = reader.mget(&keys);

        for (i, result) in results.iter().enumerate() {
            let i = i as i32;
            if i % 2 == 0 {
                assert!(result.is_some());
                assert_eq!(&i.to_be_bytes().to_vec(), result.as_ref().unwrap());
            } else {
                assert!(result.is_none(), "Key {} should not exist", i);
            }
        }

        // 测试2: 大规模间隔查询
        let keys: Vec<Vec<u8>> = (900..1100).map(|i: i32| i.to_be_bytes().to_vec()).collect();
        let results = reader.mget(&keys);

        for (i, result) in results.iter().enumerate() {
            let key_value = i as i32 + 900;
            if key_value % 2 == 0 {
                assert!(result.is_some());
                assert_eq!(&key_value.to_be_bytes().to_vec(), result.as_ref().unwrap());
            } else {
                assert!(
                    result.is_none(),
                    "Key {} should not exist but found:{:?}",
                    key_value,
                    i32::from_be_bytes(result.as_ref().unwrap().as_slice().try_into().unwrap())
                );
            }
        }

        // 测试3: 边界值测试
        let boundary_keys = vec![
            0, 998, 999, 1000, // 测试最大值附近
            1998, 1999,
            2000, // 测试最大值以上
                  // 测试不存在的值
        ];
        let mut keys: Vec<Vec<u8>> = boundary_keys
            .into_iter()
            .map(|i: i32| i.to_be_bytes().to_vec())
            .collect();

        keys.sort();

        let results = reader.mget(&keys);
        assert!(results[0].is_some()); // 0 存在
        assert!(results[1].is_some()); // 998 存在
        assert!(results[2].is_none()); // 999 不存在
        assert!(results[3].is_some()); // 1000 不存在
        assert!(results[4].is_some()); // 1998 不存在
        assert!(results[5].is_none()); // 1999 不存在
        assert!(results[6].is_none()); // 2000 不存在

        // 测试4: 大间隔查询
        let sparse_keys = vec![0, 100, 200, 300, 400, 500, 600, 700, 800, 900];
        let keys: Vec<Vec<u8>> = sparse_keys
            .clone()
            .into_iter()
            .map(|i: i32| i.to_be_bytes().to_vec())
            .collect();

        let results = reader.mget(&keys);
        for (i, result) in results.iter().enumerate() {
            let key_value: i32 = sparse_keys[i];
            if key_value % 2 == 0 && key_value < 1000 {
                assert!(result.is_some());
                assert_eq!(&key_value.to_be_bytes().to_vec(), result.as_ref().unwrap());
            } else {
                assert!(result.is_none(), "Key {} should not exist", key_value);
            }
        }

        // 测试5: 中间有大段空隙的查询
        let gap_keys = vec![
            0, 2, 4, // 开头连续存在
            497, 498, 499, // 中间一段不存在
            500, 502, 504, // 中间一段存在
            995, 996, 997, // 结尾一段不存在
        ];
        let keys: Vec<Vec<u8>> = gap_keys
            .into_iter()
            .map(|i: i32| i.to_be_bytes().to_vec())
            .collect();

        let results = reader.mget(&keys);
        assert!(results[0].is_some()); // 0 存在
        assert!(results[1].is_some()); // 2 存在
        assert!(results[2].is_some()); // 4 存在
        assert!(results[3].is_none()); // 497 不存在
        assert!(results[4].is_some()); // 498 存在
        assert!(results[5].is_none()); // 499 不存在
        assert!(results[6].is_some()); // 500 存在
        assert!(results[7].is_some()); // 502 存在
        assert!(results[8].is_some()); // 504 存在
        assert!(results[9].is_none()); // 995 不存在
        assert!(results[10].is_some()); // 996 存在
        assert!(results[11].is_none()); // 997 不存在
    }

    #[test]
    fn test_mget_u32_even() {
        use std::path::PathBuf;
        // 为避免与其它测试冲突，使用新的目录
        let dir = PathBuf::from("test/test_mget_u32_even");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let mut tree = BTree::new(4);

        // 插入 1000 ~ 1_000_000 范围内的偶数
        for i in 1000_u32..=1_000_000 {
            if i % 2 == 0 {
                tree.put(i.to_be_bytes().to_vec(), i.to_be_bytes().to_vec());
            }
        }

        // 持久化到磁盘
        let writer = TreeWriter::new(tree, 4, Box::new(DefaultSerializer {}));
        writer.persist(&dir).unwrap();

        // 从磁盘加载树
        let reader = TreeReader::new(&dir, Box::new(DefaultSerializer {})).unwrap();

        // 测试用例 1: 查询低边界附近的连续偶数 key
        let keys: Vec<Vec<u8>> = (1000_u32..1010)
            .filter(|i| i % 2 == 0)
            .map(|i| i.to_be_bytes().to_vec())
            .collect();
        let results = reader.mget(&keys);
        for (i, result) in results.iter().enumerate() {
            let expected = 1000 + (i as u32) * 2;
            assert!(
                result.is_some(),
                "Key {} should exist, but not found",
                expected
            );
            assert_eq!(&expected.to_be_bytes().to_vec(), result.as_ref().unwrap());
        }

        // 测试用例 2: 混合存在与不存在的 key（偶数存在，奇数不存在）
        let keys: Vec<Vec<u8>> = vec![
            1000_u32,  // 存在
            1001,      // 不存在
            500000,    // 存在
            500001,    // 不存在
            999_999,   // 不存在
            1_000_000, // 存在
        ]
        .into_iter()
        .map(|i| i.to_be_bytes().to_vec())
        .collect();
        let results = reader.mget(&keys);
        assert!(results[0].is_some());
        assert!(results[1].is_none());
        assert!(results[2].is_some());
        assert!(results[3].is_none());
        assert!(results[4].is_none());
        assert!(results[5].is_some());

        // 测试用例 3: 中间有较大空隙的查询
        let keys: Vec<Vec<u8>> = vec![
            1000_u32, // 存在
            1002,     // 存在
            1004,     // 存在
            500000,   // 存在
            500002,   // 存在
            500003,   // 不存在 (奇数)
        ]
        .into_iter()
        .map(|i| i.to_be_bytes().to_vec())
        .collect();
        let results = reader.mget(&keys);

        assert!(results[0].is_some());
        assert!(results[1].is_some());
        assert!(results[2].is_some());
        assert!(results[3].is_some());
        assert!(results[4].is_some());
        assert!(results[5].is_none());

        let result = reader.mget(&vec![1004_u32.to_be_bytes().to_vec()]);
        assert!(result[0].is_some() && result.len() == 1);
    }

    #[test]
    fn test_mget_batch_even() {
        let dir: PathBuf = PathBuf::from("test/test_mget_batch_even");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let mut tree = BTree::new(128);

        // 只插入1000-1000000中的偶数
        for i in (1000_u32..=1_000_000).step_by(2) {
            tree.put(i.to_be_bytes().to_vec(), i.to_be_bytes().to_vec());
        }

        let writer = TreeWriter::new(tree, 0, Box::new(DefaultSerializer {}));
        writer.persist(&dir).unwrap();

        let reader = TreeReader::new(&dir, Box::new(DefaultSerializer {})).unwrap();

        let start_time = std::time::Instant::now();

        // 分批查询0-2000000的数据,每批1000个
        let batch_size = 1000;
        for start in (0..2_000_000).step_by(batch_size) {
            let end = (start + batch_size).min(2_000_000);

            let keys: Vec<Vec<u8>> = (start..end)
                .map(|i| (i as u32).to_be_bytes().to_vec())
                .collect();

            let results = reader.mget(&keys);

            // 验证结果
            for (idx, result) in results.iter().enumerate() {
                let key_value = start + idx;

                if key_value >= 1000 && key_value <= 1_000_000 && key_value % 2 == 0 {
                    // 在范围内的偶数应该存在
                    assert!(result.is_some(), "Key {} should exist", key_value);
                    assert_eq!(
                        &(key_value as i32).to_be_bytes().to_vec(),
                        result.as_ref().unwrap(),
                        "Value mismatch for key {}",
                        key_value
                    );
                } else {
                    // 其他情况应该不存在
                    assert!(
                        result.is_none(),
                        "Key {} should not exist but found {:?}",
                        key_value,
                        result
                            .as_ref()
                            .map(|v| i32::from_be_bytes(v.as_slice().try_into().unwrap()))
                    );
                }
            }
        }
        println!("Batch done {:?}", start_time.elapsed());

        let start_time = std::time::Instant::now();
        // 单条查询0-2000000的数据
        for i in 0..=2_000_000 {
            if i >= 1000 && i <= 1_000_000 && i % 2 == 0 {
                assert!(reader.get(&(i as u32).to_be_bytes().to_vec()).is_some());
            } else {
                assert!(reader.get(&(i as u32).to_be_bytes().to_vec()).is_none());
            }
        }
        println!("Get done {:?}", start_time.elapsed());
    }
}
