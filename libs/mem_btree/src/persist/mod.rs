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

pub trait KVSerializer<K, V>: Send + Sync {
    fn serialize_key<'a>(&self, k: &'a K) -> Cow<'a, [u8]>;
    fn serialize_value<'a>(&self, v: &'a V) -> Cow<'a, [u8]>;
}

pub struct TreeWriter<K, V> {
    tree: BTree<K, V>,
    key_len: u16,
    var_len: bool,
    serializer: Box<dyn KVSerializer<K, V>>,
}

impl<K, V> TreeWriter<K, V>
where
    K: Ord,
{
    pub fn new(tree: BTree<K, V>, key_len: u16, serializer: Box<dyn KVSerializer<K, V>>) -> Self {
        Self {
            tree,
            key_len,
            var_len: key_len == 0,
            serializer,
        }
    }
}

/// next_items_offset < 0 point to data file
///  MAGIC_VERSION:[u8;2] + key_len:u16 + node_count: u32
/// fixedkey_node
///         [item_count:u16 + key:[u8; key_len] + next_items_offset:[var(i64)]]
/// varkey_node
///         [item_count:u16 + key_len:var(u32) + key:[u8; key_len] + next_items_offset:[var(i64)]]
/// data
///    [
///         value_len:u32 + value:[u8; value_len]
///    ]
/// if key_len == 0 means not fixed key
impl<K, V> TreeWriter<K, V>
where
    K: Ord,
{
    pub fn persist(&self, dir: &PathBuf) -> std::io::Result<()> {
        if !dir.exists() {
            std::fs::create_dir_all(dir)?;
        }

        let mut node_file = BufWriter::new(
            OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(dir.join(NODE_NAME))?,
        );

        let mut data_file = BufWriter::new(
            OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(dir.join(DATA_NAME))?,
        );

        data_file.write_all(MAGIC_VERSION)?;

        node_file.write_all(MAGIC_VERSION)?;

        node_file.write_all(&self.key_len.to_be_bytes())?;
        node_file.write_all(&(self.tree.len() as u32).to_be_bytes())?;

        let root_offset = self.inner_persist(&self.tree.root, &mut node_file, &mut data_file)?;

        //write root offset
        node_file.write_all(root_offset.to_be_bytes().as_slice())?;

        node_file.flush()?;
        data_file.flush()?;

        Ok(())
    }

    fn inner_persist(
        &self,
        bt: &BTreeType<K, V>,
        node_file: &mut BufWriter<File>,
        data_file: &mut BufWriter<File>,
    ) -> std::io::Result<i64> {
        match bt {
            crate::BTreeType::Leaf(leaf) => self.persist_leaf(node_file, data_file, leaf),
            crate::BTreeType::Node(node) => self.persist_node(node_file, data_file, node),
        }
    }

    fn persist_leaf(
        &self,
        node_file: &mut BufWriter<File>,
        data_file: &mut BufWriter<File>,
        leaf: &Leaf<K, V>,
    ) -> std::io::Result<i64> {
        let items = &leaf.items;

        let start = node_file.stream_position()? as i64;

        zigzag::write_u16(items.len() as u16, node_file)?;

        for k in items {
            let key_bytes = self.serializer.serialize_key(&k.0);
            if self.var_len {
                zigzag::write_u16(key_bytes.len() as u16, node_file)?;
            };

            node_file.write_all(&key_bytes)?;
            zigzag::write_i64(-(data_file.stream_position()? as i64), node_file)?;

            let value_bytes = self.serializer.serialize_value(&k.1);
            zigzag::write_u32(value_bytes.len() as u32, data_file)?;
            data_file.write_all(&value_bytes)?;
        }

        Ok(start)
    }

    fn persist_node(
        &self,
        node_file: &mut BufWriter<File>,
        data_file: &mut BufWriter<File>,
        node: &Node<K, V>,
    ) -> std::io::Result<i64> {
        let chidren = &node.children;

        let mut lazy = Lazy {
            count: chidren.len() as u16,
            items: Vec::with_capacity(chidren.len()),
            offset_values: Vec::with_capacity(chidren.len()),
        };

        for t in chidren {
            if let Some(k) = t.key() {
                let key_bytes = self.serializer.serialize_key(&k.0);
                lazy.items.push(key_bytes);
            }
        }

        for c in chidren {
            lazy.offset_values
                .push(self.inner_persist(c, node_file, data_file)?);
        }

        let start = node_file.stream_position()? as i64;

        // Write item count
        zigzag::write_u16(lazy.count, node_file)?;

        // write keys and offset values
        for (key, offset) in lazy.items.into_iter().zip(lazy.offset_values) {
            if self.var_len {
                zigzag::write_u32(key.len() as u32, node_file)?;
            }
            node_file.write_all(&key)?;
            zigzag::write_i64(offset, node_file)?;
        }

        Ok(start)
    }
}

fn ___debug(file: PathBuf) {
    let mut vv = Vec::with_capacity(10000);
    File::open(file).unwrap().read_to_end(&mut vv).unwrap();
    println!("+++++++++++:{:?}", vv);
}

struct Lazy<'a> {
    count: u16,
    items: Vec<Cow<'a, [u8]>>,
    offset_values: Vec<i64>,
}

pub trait KVDeserializer<K, V>: Send + Sync {
    fn deserialize_value(&self, v: &[u8]) -> std::result::Result<V, Box<dyn Error>>;
    fn serialize_key<'a>(&self, k: &'a K) -> Cow<'a, [u8]>;
}

pub struct TreeReader<K, V> {
    key_len: u16,
    var_len: bool,
    tree_len: u32,
    root_offset: usize,
    node: memmap2::Mmap,
    data: memmap2::Mmap,
    deserializer: Box<dyn KVDeserializer<K, V>>,
    dir: PathBuf,
}

impl<K, V> TreeReader<K, V> {
    /// Creates a new TreeReader instance.
    ///
    /// # Arguments
    ///
    /// * `dir` - Directory path containing the node and data files
    /// * `deserializer` - Implementation of KVDeserializer for key-value deserialization
    ///
    /// # Returns
    ///
    /// Returns `Result<TreeReader<K,V>>` which is:
    /// - `Ok(TreeReader)` if files are valid and successfully loaded
    /// - `Err` if files are invalid or cannot be opened
    pub fn new(dir: &Path, deserializer: Box<dyn KVDeserializer<K, V>>) -> std::io::Result<Self> {
        // Memory map the node file for node file
        let node = unsafe { memmap2::Mmap::map(&File::open(dir.join(NODE_NAME))?)? };
        Self::validate_magic(&node)?;
        let (key_len, tree_len) = Self::read_meta(&node)?;

        // Open and validate data file
        let data = unsafe { memmap2::Mmap::map(&File::open(dir.join(DATA_NAME))?)? };
        Self::validate_magic(&node)?;

        let root_offset =
            i64::from_be_bytes(node[node.len() - 8..node.len()].try_into().unwrap()) as usize;

        Ok(Self {
            key_len,
            var_len: key_len == 0,
            tree_len,
            root_offset,
            node,
            data,
            deserializer,
            dir: dir.to_path_buf(),
        })
    }

    /// Returns the total number of key-value pairs in the tree
    pub fn len(&self) -> u32 {
        self.tree_len
    }

    /// Returns true if the tree is empty
    pub fn is_empty(&self) -> bool {
        self.tree_len == 0
    }

    /// Returns the value associated with the key
    pub fn get(&self, k: &K) -> Option<V> {
        // Find key position in node file
        let offset = self.find_key_offset(k)?;
        Some(self.read_value(offset))
    }

    pub fn iter(&self) -> Iter<K, V> {
        Iter::new(self)
    }

    fn read_value(&self, offset: i64) -> V {
        let mut offset = offset as usize;
        let value_len = read_u32(&self.data, &mut offset);

        self.deserializer
            .deserialize_value(&self.data[offset..offset + value_len as usize])
            .expect("deserialize value failed")
    }

    /// Find offset of a key in the node file using binary search
    fn find_key_offset(&self, key: &K) -> Option<i64> {
        let mut current_offset = self.root_offset;

        let key = &*self.deserializer.serialize_key(key);

        while current_offset < self.node.len() {
            // if current_offset > 0 {
            //     println!(
            //         "---------------------start_offset:{}---------------:{:?}---------------{:?}",
            //         current_offset,
            //         Iter::read_keys(self, current_offset as i64),
            //         key
            //     );
            // } else {
            //     println!(
            //         "---------------------start_offset:{}-----------------------------{:?}",
            //         current_offset, key
            //     );
            // }

            // Read item count
            let count = read_u16(&self.node, &mut current_offset) as usize;

            // Binary search in current node
            let mut pre_offset = None;

            let mut leaf_node = false;

            'out: for _ in 0..count {
                let (k, offset) = self.read_key(&mut current_offset);

                // means leaf node
                if offset < 0 {
                    leaf_node = true;
                    match key.cmp(k) {
                        std::cmp::Ordering::Greater => {
                            continue;
                        }
                        std::cmp::Ordering::Less => {
                            return None;
                        }
                        std::cmp::Ordering::Equal => {
                            // Found key, read value offset
                            // Negative offset points to data file
                            return Some(-offset);
                        }
                    }
                } else {
                    // means node
                    match key.cmp(k) {
                        std::cmp::Ordering::Less => {
                            // Key is less than current k, move to pre child offset
                            current_offset = pre_offset?;
                            pre_offset = None;
                            break 'out;
                        }
                        std::cmp::Ordering::Greater => {
                            pre_offset = Some(offset as usize);
                        }
                        std::cmp::Ordering::Equal => {
                            pre_offset = None;
                            current_offset = offset as usize;
                            break 'out;
                        }
                    }
                }
            }

            // if leaf node and not found
            if leaf_node {
                return None;
            }

            if pre_offset.is_some() {
                current_offset = pre_offset.unwrap();
            }
        }
        None
    }

    // Validate data file magic number
    fn validate_magic(data: &memmap2::Mmap) -> std::io::Result<()> {
        if data.len() < 2 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Invalid data file format",
            ));
        }

        if data[0..MAGIC_VERSION.len()] != MAGIC_VERSION[..] {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Invalid data file format",
            ));
        }

        Ok(())
    }

    // Extract key_len and tree_len
    fn read_meta(node: &memmap2::Mmap) -> std::io::Result<(u16, u32)> {
        if node.len() < 8 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Invalid node file format",
            ));
        }
        let key_len = u16::from_be_bytes([node[2], node[3]]);
        let tree_len = u32::from_be_bytes([node[4], node[5], node[6], node[7]]);
        Ok((key_len, tree_len))
    }

    fn read_key(&self, offset: &mut usize) -> (&[u8], i64) {
        let key_len = if self.var_len {
            read_u16(&self.node, offset) as usize
        } else {
            self.key_len as usize
        };

        if *offset + key_len > self.node.len() {
            let dir = self.dir.join(NODE_NAME);
            let file = File::open(&dir).unwrap();
            let len = file.metadata().unwrap().len();
            println!(
                "path:{:?} offset: {:?}, key_len: {:?}, node_len: {:?}===========len:{}",
                dir,
                offset,
                key_len,
                self.node.len(),
                len
            );
        }

        let data = read_data(&self.node, offset, key_len);
        let data_offset = read_i64(&self.node, offset);
        (data, data_offset)
    }
}

fn read_u16(data: &memmap2::Mmap, offset: &mut usize) -> u16 {
    zigzag::read_u16(data, offset)
}

fn read_u32(data: &memmap2::Mmap, offset: &mut usize) -> u32 {
    zigzag::read_u32(data, offset)
}

fn read_i64(data: &memmap2::Mmap, offset: &mut usize) -> i64 {
    zigzag::read_i64(data, offset)
}

fn read_data<'a>(node: &'a memmap2::Mmap, offset: &mut usize, len: usize) -> &'a [u8] {
    *offset += len;
    &node[*offset - len..*offset]
}

struct NextLevel<'a> {
    keys: Vec<(&'a [u8], i64)>,
    index: usize,
}

/// Iterator implementation for TreeReader
pub struct Iter<'a, K, V> {
    reader: &'a TreeReader<K, V>,
    stack: LinkedList<NextLevel<'a>>,
    seek_key: Vec<u8>,
}

impl<'a, K, V> Iter<'a, K, V> {
    fn new(reader: &'a TreeReader<K, V>) -> Self {
        let mut stack = LinkedList::new();
        let keys = Self::read_keys(reader, reader.root_offset as i64);
        stack.push_back(NextLevel { keys, index: 0 });
        Self {
            reader,
            stack,
            seek_key: vec![],
        }
    }

    fn read_keys(reader: &'a TreeReader<K, V>, offset: i64) -> Vec<(&'a [u8], i64)> {
        let mut offset = offset as usize;
        let count = read_u16(&reader.node, &mut offset);
        (0..count).map(|_| reader.read_key(&mut offset)).collect()
    }

    pub fn reset(&mut self) {
        self.seek_key.clear();
        if self.stack.len() == 1 && self.stack.front().unwrap().index == 0 {
            return;
        }

        let keys = match self.stack.pop_front() {
            Some(root) => root.keys,
            None => Self::read_keys(self.reader, self.reader.root_offset as i64),
        };
        self.stack.clear();
        self.stack.push_back(NextLevel { keys, index: 0 });
    }

    pub fn seek_first(&mut self) {
        self.reset();
    }

    pub fn seek(&mut self, key: &K) {
        self.reset();

        self.seek_key = self.reader.deserializer.serialize_key(key).to_vec();

        let back = self.stack.back_mut().unwrap();
        back.index = Self::binary_index(&back.keys, &self.seek_key);

        loop {
            if self.stack.is_empty() {
                return;
            }

            let back = self.stack.back_mut().unwrap();

            if back.index >= back.keys.len() {
                match self.stack.pop_back() {
                    Some(_) => continue,
                    None => return,
                }
            }

            let offset = back.keys[back.index].1;
            if offset < 0 {
                return;
            }
            back.index += 1;
            let keys = Self::read_keys(self.reader, offset);
            let index = Self::binary_index(&keys, &self.seek_key);

            self.stack.push_back(NextLevel { keys, index });
        }
    }

    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Option<(&[u8], V)> {
        loop {
            if self.stack.is_empty() {
                return None;
            }

            let back = self.stack.back_mut().unwrap();

            if back.index >= back.keys.len() {
                match self.stack.pop_back() {
                    Some(_) => continue,
                    None => return None,
                }
            }

            let (key, offset) = &back.keys[back.index];
            back.index += 1;

            let offset = *offset;
            if offset < 0 {
                return Some((key, self.reader.read_value(-offset)));
            }

            let keys = Self::read_keys(self.reader, offset);

            let index = Self::binary_index(&keys, &self.seek_key);

            self.stack.push_back(NextLevel { keys, index });
        }
    }

    pub fn seek_last(&mut self) {
        self.reset();
        self.inner_seek_prev();
    }

    pub fn seek_prev(&mut self, key: &K) {
        self.reset();
        self.seek_key = self.reader.deserializer.serialize_key(key).to_vec();
        self.inner_seek_prev();
    }

    fn inner_seek_prev(&mut self) {
        let back = self.stack.back_mut().unwrap();
        back.index = Self::pre_binary_index(&back.keys, &self.seek_key);

        loop {
            let back = self.stack.back_mut().unwrap();
            let offset = back.keys[back.index - 1].1;
            if offset < 0 {
                return;
            }
            back.index -= 1;
            let keys = Self::read_keys(self.reader, offset);
            let index = Self::pre_binary_index(&keys, &self.seek_key);

            self.stack.push_back(NextLevel { keys, index });
        }
    }

    pub fn prev(&mut self) -> Option<(&[u8], V)> {
        loop {
            if self.stack.is_empty() {
                return None;
            }

            let back = self.stack.back_mut().unwrap();

            if back.index == 0 {
                match self.stack.pop_back() {
                    Some(_) => continue,
                    None => return None,
                }
            }

            let (key, offset) = &back.keys[back.index - 1];
            back.index -= 1;

            let offset = *offset;
            if offset < 0 {
                return Some((key, self.reader.read_value(-offset)));
            }

            let keys = Self::read_keys(self.reader, offset);

            let index = Self::pre_binary_index(&keys, &self.seek_key);

            self.stack.push_back(NextLevel { keys, index });
        }
    }

    fn pre_binary_index(keys: &[(&[u8], i64)], key: &[u8]) -> usize {
        //if keys is empty, return the last index
        if key.is_empty() {
            return keys.len();
        }

        match keys.binary_search_by(|v| v.0.cmp(key)) {
            Ok(i) => i + 1,
            Err(i) => i,
        }
    }

    fn binary_index(keys: &[(&[u8], i64)], key: &[u8]) -> usize {
        //if keys is empty, return the first index
        if key.is_empty() {
            return 0;
        }

        let is_leaf = keys[0].1 < 0;

        match keys.binary_search_by(|v| v.0.cmp(key)) {
            Ok(i) => i,
            Err(i) => {
                if is_leaf {
                    i
                } else {
                    i - 1
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {

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
    fn test_tree_reader_and_iter() {
        let dir = PathBuf::from("data");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        // Create tree and insert data
        let mut tree = BTree::new(128);

        for i in 0..1_000_000 as i32 {
            let i = i * 2;
            tree.put(i.to_be_bytes().to_vec(), i.to_be_bytes().to_vec());
        }

        // Persist tree to disk
        let writer = TreeWriter::new(tree, 0, Box::new(DefaultSerializer {}));
        writer.persist(&dir).unwrap();

        // Load tree from disk
        let reader = TreeReader::new(&dir, Box::new(DefaultSerializer {})).unwrap();

        let mut iter = reader.iter();
        while let Some(v) = iter.next() {
            println!("============k2: {:?}, v1: {:?}", v.0, v.1);
        }

        // Verify contents
        let mut iter = reader.iter();

        for i in 0..10000 {
            let (k1, v1) = iter.next().unwrap();
            assert_eq!(i32::from_be_bytes(k1.try_into().unwrap()), i * 2);
            assert_eq!(i32::from_be_bytes(v1.try_into().unwrap()), i * 2);
        }

        assert!(iter.next().is_none());

        iter.seek_prev(&i32::MAX.to_be_bytes().to_vec());
        for i in 0..10000 {
            let (k1, v1) = iter.prev().unwrap();
            assert_eq!(
                i32::from_be_bytes(k1.try_into().unwrap()),
                (10000 - i - 1) * 2
            );
            assert_eq!(
                i32::from_be_bytes(v1.try_into().unwrap()),
                (10000 - i - 1) * 2
            );
        }
        assert!(iter.prev().is_none());

        iter.seek_prev(&5_i32.to_be_bytes().to_vec());

        let (k1, v1) = iter.prev().unwrap();
        assert_eq!(i32::from_be_bytes(k1.try_into().unwrap()), 4);
        assert_eq!(i32::from_be_bytes(v1.try_into().unwrap()), 4);
        let (k1, v1) = iter.prev().unwrap();
        assert_eq!(i32::from_be_bytes(k1.try_into().unwrap()), 2);
        assert_eq!(i32::from_be_bytes(v1.try_into().unwrap()), 2);
        let (k1, v1) = iter.prev().unwrap();
        assert_eq!(i32::from_be_bytes(k1.try_into().unwrap()), 0);
        assert_eq!(i32::from_be_bytes(v1.try_into().unwrap()), 0);

        assert_eq!(iter.prev(), None);

        // Test seek_prev with empty key (should go to last element)
        iter.seek_last();
        let (k1, v1) = iter.prev().unwrap();
        assert_eq!(i32::from_be_bytes(k1.try_into().unwrap()), 19998);
        assert_eq!(i32::from_be_bytes(v1.try_into().unwrap()), 19998);

        // Test seek_prev with key larger than any existing key
        iter.seek_prev(&(20000_i32).to_be_bytes().to_vec());
        let (k1, v1) = iter.prev().unwrap();
        assert_eq!(i32::from_be_bytes(k1.try_into().unwrap()), 19998);
        assert_eq!(i32::from_be_bytes(v1.try_into().unwrap()), 19998);

        // Test seek_prev with non-existent key (between existing keys)
        iter.seek_prev(&3_i32.to_be_bytes().to_vec());
        let (k1, v1) = iter.prev().unwrap();
        assert_eq!(i32::from_be_bytes(k1.try_into().unwrap()), 2);
        assert_eq!(i32::from_be_bytes(v1.try_into().unwrap()), 2);

        // Test consecutive prev calls after seek_prev
        iter.seek_prev(&10_i32.to_be_bytes().to_vec());
        let (k1, v1) = iter.prev().unwrap();
        assert_eq!(i32::from_be_bytes(k1.try_into().unwrap()), 10);
        let (k1, v1) = iter.prev().unwrap();
        assert_eq!(i32::from_be_bytes(k1.try_into().unwrap()), 8);
        let (k1, v1) = iter.prev().unwrap();
        assert_eq!(i32::from_be_bytes(k1.try_into().unwrap()), 6);
        let (k1, v1) = iter.prev().unwrap();
        assert_eq!(i32::from_be_bytes(k1.try_into().unwrap()), 4);

        // Test forward iteration from beginning
        iter.seek_first();
        let (k1, v1) = iter.next().unwrap();
        assert_eq!(i32::from_be_bytes(k1.try_into().unwrap()), 0);
        assert_eq!(i32::from_be_bytes(v1.try_into().unwrap()), 0);
        let (k1, v1) = iter.next().unwrap();
        assert_eq!(i32::from_be_bytes(k1.try_into().unwrap()), 2);
        assert_eq!(i32::from_be_bytes(v1.try_into().unwrap()), 2);
        let (k1, v1) = iter.next().unwrap();
        assert_eq!(i32::from_be_bytes(k1.try_into().unwrap()), 4);
        assert_eq!(i32::from_be_bytes(v1.try_into().unwrap()), 4);

        // Test seek with key larger than any existing key
        iter.seek(&(20000_i32).to_be_bytes().to_vec());
        assert!(iter.next().is_none());

        // Test seek with non-existent key (between existing keys)
        iter.seek(&3_i32.to_be_bytes().to_vec());
        let (k1, v1) = iter.next().unwrap();
        assert_eq!(i32::from_be_bytes(k1.try_into().unwrap()), 4);
        assert_eq!(i32::from_be_bytes(v1.try_into().unwrap()), 4);

        // Test consecutive next calls after seek
        iter.seek(&8_i32.to_be_bytes().to_vec());
        let (k1, v1) = iter.next().unwrap();
        assert_eq!(i32::from_be_bytes(k1.try_into().unwrap()), 8);
        let (k1, v1) = iter.next().unwrap();
        assert_eq!(i32::from_be_bytes(k1.try_into().unwrap()), 10);
        let (k1, v1) = iter.next().unwrap();
        assert_eq!(i32::from_be_bytes(k1.try_into().unwrap()), 12);
        let (k1, v1) = iter.next().unwrap();
        assert_eq!(i32::from_be_bytes(k1.try_into().unwrap()), 14);

        // Test seek with empty key (should go to first element)
        iter.seek(&0_i32.to_be_bytes().to_vec());
        let (k1, v1) = iter.next().unwrap();
        assert_eq!(i32::from_be_bytes(k1.try_into().unwrap()), 0);
        assert_eq!(i32::from_be_bytes(v1.try_into().unwrap()), 0);
    }

    #[test]
    fn test_tree_fix_len() {
        let dir = PathBuf::from("data");
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
        let dir = PathBuf::from("data");
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

        // Persist tree to disk
        let writer = TreeWriter::new(tree, 0, Box::new(DefaultSerializer {}));
        writer.persist(&dir).unwrap();

        let tree: TreeReader<Vec<u8>, Vec<u8>> =
            TreeReader::new(&dir, Box::new(DefaultSerializer {})).unwrap();

        let v = tree.get(&(start + 1).to_be_bytes().to_vec());
        println!("get: {:?}", v);

        for i in start..total as i32 {
            eprintln!("=====+++++++++++++++++++++++==========={}", i);
            assert!(tree.get(&i.to_be_bytes().to_vec()).is_some());
        }

        let s = 14033_i32;

        assert!(tree.get(&s.to_be_bytes().to_vec()).is_some());
    }
}

mod main {}
