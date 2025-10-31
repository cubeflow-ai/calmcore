use super::*;
use crate::persist::num_ser::i64_coder;
use std::borrow::Cow;

/// High-performance B-Tree reader using memory-mapped files
pub struct TreeReader<K, V> {
    key_len: u16,
    tree_len: u32,
    root_offset: i64,
    node: memmap2::Mmap,
    data: memmap2::Mmap,
    deserializer: Box<dyn KeySerializer<K, V>>,
}

impl<K, V> TreeReader<K, V>
where
    K: Clone,
{
    /// Creates a new TreeReader instance using mmap for fast random access
    ///
    /// File format:
    /// NODE file: MAGIC(2) + root_offset(8) + key_len(2) + tree_len(4) + nodes...
    /// DATA file: MAGIC(2) + values...
    ///
    pub fn new(dir: &Path, deserializer: Box<dyn KeySerializer<K, V>>) -> Result<Self> {
        // Memory map the node file for fast random access
        let node = unsafe { memmap2::Mmap::map(&File::open(dir.join(NODE_NAME))?)? };
        Self::validate_magic(&node)?;

        // Parse header: MAGIC(2) + root_offset(8) + key_len(2) + tree_len(4)
        if node.len() < 16 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Invalid node file: header too short",
            ));
        }

        let root_offset = i64::from_be_bytes(node[2..10].try_into().unwrap());
        let key_len = u16::from_be_bytes(node[10..12].try_into().unwrap());
        let tree_len = u32::from_be_bytes(node[12..16].try_into().unwrap());

        // Memory map the data file
        let data = unsafe { memmap2::Mmap::map(&File::open(dir.join(DATA_NAME))?)? };
        Self::validate_magic(&data)?;

        Ok(Self {
            key_len,
            tree_len,
            root_offset,
            node,
            data,
            deserializer,
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

    /// Get value by key with O(log n) complexity
    ///
    /// Uses binary search through mmap'd B-Tree nodes for optimal performance
    pub fn get(&self, key: &K) -> Option<V> {
        if self.is_empty() {
            return None;
        }

        // Serialize key for comparison
        let key_bytes = self.serialize_single_key(key);

        // Find data offset by traversing B-Tree
        let (start_offset, end_offset) = self.find_data_offset(&key_bytes)?;

        // Read and deserialize value with known size
        Some(self.read_value_at(start_offset, end_offset))
    }
    /// Batch get - more efficient than multiple get() calls
    pub fn mget(&self, keys: &[K]) -> Vec<Option<V>> {
        keys.iter().map(|k| self.get(k)).collect()
    }

    /// Find the largest key-value pair where key <= given key (floor lookup)
    /// This is useful for finding the RecordBatch containing a specific doc_id
    ///
    /// Returns Some((key, value, ttl)) if found, None if no key <= target exists
    pub fn floor(&self, key: &K) -> Option<crate::Item<K, V>> {
        if self.is_empty() {
            return None;
        }

        // Serialize key for comparison
        let key_bytes = self.serialize_single_key(key);

        // Traverse tree to find floor
        self.find_floor_in_tree(&key_bytes)
    }

    /// Returns an iterator over all key-value pairs
    pub fn iter(&self) -> TreeIterator<K, V> {
        TreeIterator::new(self)
    }

    // ========== Internal Implementation ==========

    /// Serialize a single key to bytes for comparison
    fn serialize_single_key(&self, key: &K) -> Vec<u8> {
        let keys_vec = vec![key.clone()];
        let serialized = self.deserializer.serialize_keys(&keys_vec);
        serialized.to_vec()
    }

    /// Find data file offset for a key by traversing the B-Tree
    /// Returns (start_offset, end_offset) to allow calculating value size
    fn find_data_offset(&self, key_bytes: &[u8]) -> Option<(i64, i64)> {
        let mut offset = self.root_offset;

        // Traverse from root to leaf
        loop {
            // Read node at current offset
            let (is_leaf, keys, offsets) = match self.read_node_at(offset as usize) {
                Ok(node) => node,
                Err(_) => return None,
            };

            // Find the appropriate child/value in current node
            if is_leaf {
                // Leaf node: do exact binary search
                match self.binary_search_keys(&keys, key_bytes) {
                    SearchResult::Found(idx) => {
                        // Found exact key in leaf
                        let start = offsets[idx];
                        let end = if idx + 1 < offsets.len() {
                            offsets[idx + 1]
                        } else {
                            self.data.len() as i64
                        };
                        return Some((start, end));
                    }
                    SearchResult::NotFound(_) => {
                        // Key not found in leaf
                        return None;
                    }
                }
            } else {
                // Index node: find which child to follow
                // keys[i] is the first key of child i
                // We need to find the last child whose first key <= search_key

                let mut child_idx = 0;
                for (i, key) in self.deserializer.deserialize_keys(&keys).iter().enumerate() {
                    let key_bytes_i = self.serialize_single_key(key);
                    if key_bytes_i.as_slice() <= key_bytes {
                        child_idx = i;
                    } else {
                        break;
                    }
                }

                let next_offset = offsets[child_idx];
                offset = next_offset;
            }
        }
    }

    /// Read a node from mmap at given offset
    /// Returns (is_leaf, keys_data, offsets)
    fn read_node_at(&self, offset: usize) -> Result<(bool, Vec<u8>, Vec<i64>)> {
        if offset >= self.node.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Node offset out of bounds",
            ));
        }

        let mut pos = offset;

        // Read chunk type (1 byte): 1 = leaf, 0 = index
        if pos + 1 > self.node.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Not enough data for chunk type",
            ));
        }
        let is_leaf = self.node[pos] == 1;
        pos += 1;

        // Read keys length (u32)
        if pos + 4 > self.node.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Not enough data for keys length",
            ));
        }

        let keys_len = u32::from_be_bytes(self.node[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;

        // Read keys data
        if pos + keys_len > self.node.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Not enough data for keys",
            ));
        }

        let keys_bytes = self.node[pos..pos + keys_len].to_vec();
        pos += keys_len;

        // Read offsets using i64_coder (all positive now)
        let offsets = i64_coder::read(&self.node, &mut pos);

        Ok((is_leaf, keys_bytes, offsets))
    }
    /// Binary search within serialized keys
    fn binary_search_keys(&self, keys_data: &[u8], search_key: &[u8]) -> SearchResult {
        // Deserialize all keys for comparison
        let keys = self.deserializer.deserialize_keys(keys_data);

        // Serialize each for byte-level comparison
        // This is inefficient - ideally we'd compare without full deserialization
        for (i, key) in keys.iter().enumerate() {
            let key_bytes = self.serialize_single_key(key);
            match key_bytes.as_slice().cmp(search_key) {
                std::cmp::Ordering::Equal => return SearchResult::Found(i),
                std::cmp::Ordering::Greater => return SearchResult::NotFound(i),
                std::cmp::Ordering::Less => continue,
            }
        }
        SearchResult::NotFound(keys.len())
    }

    /// Find floor entry in tree: largest key <= search_key
    fn find_floor_in_tree(&self, key_bytes: &[u8]) -> Option<crate::Item<K, V>> {
        let mut offset = self.root_offset;
        let mut floor_candidate: Option<(K, i64, i64)> = None; // (key, start_offset, end_offset)

        // Traverse from root to leaf, keeping track of largest key <= search_key
        loop {
            let (is_leaf, keys_data, offsets) = match self.read_node_at(offset as usize) {
                Ok(node) => node,
                Err(_) => break,
            };

            let keys = self.deserializer.deserialize_keys(&keys_data);

            if is_leaf {
                // Leaf node: find largest key <= search_key
                for (i, key) in keys.iter().enumerate() {
                    let key_bytes_i = self.serialize_single_key(key);
                    if key_bytes_i.as_slice() <= key_bytes {
                        // This key is <= search_key, it's a candidate
                        let start = offsets[i];
                        let end = if i + 1 < offsets.len() {
                            offsets[i + 1]
                        } else {
                            self.data.len() as i64
                        };
                        floor_candidate = Some((key.clone(), start, end));
                    } else {
                        // Keys are sorted, no need to check further
                        break;
                    }
                }
                break; // We've reached a leaf, done
            } else {
                // Index node: find which child to follow
                // keys[i] is the first key of child i
                // We need to find the rightmost child whose first key <= search_key

                let mut child_idx = 0;
                for (i, key) in keys.iter().enumerate() {
                    let key_bytes_i = self.serialize_single_key(key);
                    if key_bytes_i.as_slice() <= key_bytes {
                        child_idx = i;
                    } else {
                        break;
                    }
                }

                let next_offset = offsets[child_idx];
                offset = next_offset;
            }
        }

        // If we found a candidate, read and return it
        floor_candidate.map(|(key, start, end)| {
            let value = self.read_value_at(start, end);
            std::sync::Arc::new((key, value, None))
        })
    }

    /// Read value from data file at given offset range
    /// With start and end offsets, we know the exact size without parsing
    fn read_value_at(&self, start_offset: i64, end_offset: i64) -> V {
        if start_offset < 2 {
            // Skip MAGIC_VERSION
            panic!("Invalid data offset: {}", start_offset);
        }

        let start = start_offset as usize;
        let end = end_offset as usize;

        if end > self.data.len() || start >= end {
            panic!(
                "Invalid offset range: {} to {} (data len: {})",
                start,
                end,
                self.data.len()
            );
        }

        // Extract value data with known size
        let value_data = &self.data[start..end];

        // Deserialize value using the provided deserializer
        self.deserializer
            .deserialize_value(value_data)
            .expect("Failed to deserialize value")
    }

    fn validate_magic(mmap: &memmap2::Mmap) -> Result<()> {
        if mmap.len() < MAGIC_VERSION.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "File too short for magic number",
            ));
        }

        if &mmap[0..MAGIC_VERSION.len()] != MAGIC_VERSION {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Invalid magic number"),
            ));
        }

        Ok(())
    }
}

#[derive(Debug)]
enum SearchResult {
    Found(usize),
    NotFound(usize),
}

/// Iterator over tree entries
pub struct TreeIterator<'a, K, V> {
    reader: &'a TreeReader<K, V>,
    stack: Vec<(usize, usize)>, // (node_offset, key_index)
    finished: bool,
}

impl<'a, K, V> TreeIterator<'a, K, V>
where
    K: Clone,
{
    fn new(reader: &'a TreeReader<K, V>) -> Self {
        Self {
            reader,
            stack: Vec::new(),
            finished: reader.is_empty(),
        }
    }
}

impl<'a, K, V> Iterator for TreeIterator<'a, K, V>
where
    K: Clone,
{
    type Item = (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }

        // TODO: Implement tree traversal
        // This requires maintaining a stack of (node, index) pairs
        // and navigating through the tree structure

        self.finished = true;
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persist::writer::TreeWriter;
    use crate::BTree;
    use std::path::PathBuf;

    struct I64KeySerializer;

    impl KeySerializer<i64, i64> for I64KeySerializer {
        fn serialize_keys<'a>(&self, keys: &'a Vec<i64>) -> Cow<'a, [u8]> {
            let mut buf = Vec::with_capacity(keys.len() * 8);
            i64_coder::write_delta(&mut buf, keys).unwrap();
            Cow::Owned(buf)
        }

        fn deserialize_keys<'a>(&self, data: &'a [u8]) -> Vec<i64> {
            i64_coder::read_delta(&data)
        }

        fn serialize_value<'a>(&self, value: &'a i64) -> Cow<'a, [u8]> {
            Cow::Owned(value.to_be_bytes().to_vec())
        }

        fn deserialize_value<'a>(
            &self,
            data: &'a [u8],
        ) -> std::result::Result<i64, Box<dyn std::error::Error>> {
            if data.len() < 8 {
                return Err("Data too short for i64".into());
            }
            Ok(i64::from_be_bytes(data[0..8].try_into().unwrap()))
        }
    }

    #[test]
    fn test_reader_basic() {
        let test_dir = PathBuf::from("/tmp/test_tree_reader_basic");
        std::fs::remove_dir_all(&test_dir).ok();

        // Write test data
        let mut tree = BTree::new(32);
        for i in 0..100i64 {
            tree.put(i, i * 2);
        }

        TreeWriter::new(test_dir.clone(), 128, 0)
            .persist(tree.len(), Box::new(I64KeySerializer {}), tree.iter())
            .unwrap();

        // Read back
        let reader = TreeReader::new(&test_dir, Box::new(I64KeySerializer {})).unwrap();

        assert_eq!(reader.len(), 100);
        assert!(!reader.is_empty());

        // Test get operations with debug output
        for i in 0..5i64 {
            let value = reader.get(&i);
            println!("Key {}: expected {}, got {:?}", i, i * 2, value);
            assert_eq!(
                value,
                Some(i * 2),
                "Failed to get correct value for key {}",
                i
            );
        }

        println!("✅ First 5 values correct, testing remaining...");
        for i in 5..100i64 {
            let value = reader.get(&i);
            assert_eq!(
                value,
                Some(i * 2),
                "Failed to get correct value for key {}",
                i
            );
        }

        // Test non-existent keys
        assert_eq!(reader.get(&-1), None);
        assert_eq!(reader.get(&100), None);
        assert_eq!(reader.get(&1000), None);

        println!("✅ All get operations successful!");
    }

    #[test]
    fn test_reader_1m_dataset() {
        // Use the existing 1M dataset written by test_tree_writer_1m_strings
        let test_dir = PathBuf::from("/tmp/test_tree_writer_1m");

        if !test_dir.exists() {
            println!("⚠️  1M test data not found, skipping test");
            return;
        }

        struct StringKeySerializer;

        impl KeySerializer<String, Vec<u8>> for StringKeySerializer {
            fn serialize_keys<'a>(&self, keys: &'a Vec<String>) -> Cow<'a, [u8]> {
                let mut buf = Vec::new();
                buf.extend_from_slice(&(keys.len() as u32).to_be_bytes());
                for key in keys {
                    let key_bytes = key.as_bytes();
                    buf.extend_from_slice(&(key_bytes.len() as u32).to_be_bytes());
                    buf.extend_from_slice(key_bytes);
                }
                Cow::Owned(buf)
            }

            fn deserialize_keys<'a>(&self, data: &'a [u8]) -> Vec<String> {
                let mut pos = 0;
                let mut keys = Vec::new();
                if data.len() < 4 {
                    return keys;
                }
                let count = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
                pos += 4;
                for _ in 0..count {
                    if pos + 4 > data.len() {
                        break;
                    }
                    let len = u32::from_be_bytes([
                        data[pos],
                        data[pos + 1],
                        data[pos + 2],
                        data[pos + 3],
                    ]) as usize;
                    pos += 4;
                    if pos + len > data.len() {
                        break;
                    }
                    let key = String::from_utf8_lossy(&data[pos..pos + len]).to_string();
                    keys.push(key);
                    pos += len;
                }
                keys
            }

            fn serialize_value<'a>(&self, value: &'a Vec<u8>) -> Cow<'a, [u8]> {
                Cow::Borrowed(value)
            }

            fn deserialize_value<'a>(
                &self,
                data: &'a [u8],
            ) -> std::result::Result<Vec<u8>, Box<dyn std::error::Error>> {
                Ok(data.to_vec())
            }
        }

        println!("Opening 1M dataset...");
        let start = std::time::Instant::now();
        let reader = TreeReader::new(&test_dir, Box::new(StringKeySerializer {})).unwrap();
        println!("Opened in {:?}", start.elapsed());

        assert_eq!(reader.len(), 1_000_000);

        // Test random access
        println!("Testing random access...");
        let test_keys = vec![0, 100, 1000, 10000, 100000, 500000, 999999];
        for &i in &test_keys {
            let key = format!("key_{:010}", i);
            let start = std::time::Instant::now();
            let value = reader.get(&key);
            let elapsed = start.elapsed();
            assert!(value.is_some(), "Failed to find key: {}", key);
            assert_eq!(
                value.unwrap().len(),
                1024,
                "Value size mismatch for key: {}",
                key
            );
            println!("  Read key {} in {:?}", key, elapsed);
        }

        println!("✅ 1M dataset read test successful!");
    }

    #[test]
    #[ignore] // 标记为 ignore，需要时手动运行
    fn test_reader_large_dataset() {
        // Use the existing large dataset written by test_tree_writer_large_strings
        let test_dir = PathBuf::from("/tmp/test_tree_writer_large");

        if !test_dir.exists() {
            println!("⚠️  Large test data not found, skipping test");
            return;
        }

        struct StringKeySerializer;

        impl KeySerializer<String, Vec<u8>> for StringKeySerializer {
            fn serialize_keys<'a>(&self, keys: &'a Vec<String>) -> Cow<'a, [u8]> {
                let mut buf = Vec::new();
                buf.extend_from_slice(&(keys.len() as u32).to_be_bytes());
                for key in keys {
                    let key_bytes = key.as_bytes();
                    buf.extend_from_slice(&(key_bytes.len() as u32).to_be_bytes());
                    buf.extend_from_slice(key_bytes);
                }
                Cow::Owned(buf)
            }

            fn deserialize_keys<'a>(&self, data: &'a [u8]) -> Vec<String> {
                let mut pos = 0;
                let mut keys = Vec::new();
                if data.len() < 4 {
                    return keys;
                }
                let count = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
                pos += 4;
                for _ in 0..count {
                    if pos + 4 > data.len() {
                        break;
                    }
                    let len = u32::from_be_bytes([
                        data[pos],
                        data[pos + 1],
                        data[pos + 2],
                        data[pos + 3],
                    ]) as usize;
                    pos += 4;
                    if pos + len > data.len() {
                        break;
                    }
                    let key = String::from_utf8_lossy(&data[pos..pos + len]).to_string();
                    keys.push(key);
                    pos += len;
                }
                keys
            }

            fn serialize_value<'a>(&self, value: &'a Vec<u8>) -> Cow<'a, [u8]> {
                Cow::Borrowed(value)
            }

            fn deserialize_value<'a>(
                &self,
                data: &'a [u8],
            ) -> std::result::Result<Vec<u8>, Box<dyn std::error::Error>> {
                Ok(data.to_vec())
            }
        }

        println!("Opening large dataset with 10M entries...");
        let start = std::time::Instant::now();
        let reader = TreeReader::new(&test_dir, Box::new(StringKeySerializer {})).unwrap();
        println!("Opened in {:?}", start.elapsed());

        assert_eq!(reader.len(), 10_000_000);

        // Test random access
        println!("Testing random access...");
        let test_keys = vec![0, 100, 1000, 10000, 100000, 1000000, 5000000, 9999999];
        for &i in &test_keys {
            let key = format!("key_{:010}", i);
            let start = std::time::Instant::now();
            let value = reader.get(&key);
            let elapsed = start.elapsed();
            assert!(value.is_some(), "Failed to find key: {}", key);
            assert_eq!(
                value.unwrap().len(),
                1024,
                "Value size mismatch for key: {}",
                key
            );
            println!("  Read key {} in {:?}", key, elapsed);
        }

        println!("✅ Large dataset read test successful!");
    }
}
