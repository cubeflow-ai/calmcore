use crate::persist::num_ser::{i64_coder, u16_coder};

use super::*;

pub struct TreeReader<K, V> {
    key_len: u16,
    var_len: bool,
    tree_len: u32,
    root_offset: usize,
    node: memmap2::Mmap,
    data: memmap2::Mmap,
    deserializer: Box<dyn KVDeserializer<K, V>>,
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
    pub fn new(dir: &Path, deserializer: Box<dyn KVDeserializer<K, V>>) -> Result<Self> {
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

    /// Returns the values associated with the keys
    /// If a key is not found, None is returned for that position
    pub fn mget(&self, keys: &[K]) -> Vec<Option<V>> {
        let keys = keys
            .iter()
            .map(|k| self.deserializer.serialize_key(k))
            .collect::<Vec<_>>();

        let offsets = self
            .mget_key_node_offsets(self.root_offset, &keys[..])
            .unwrap();

        offsets
            .into_iter()
            .map(|offset| offset.map(|o| self.read_value(-o)))
            .collect()
    }

    pub fn iter(&self) -> Result<Iter<K, V>> {
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
        self.inner_find_key_offset(&self.deserializer.serialize_key(key))
    }

    fn inner_find_key_offset(&self, key: &[u8]) -> Option<i64> {
        let mut current_offset = self.root_offset;

        while current_offset < self.node.len() {
            let keys = read_keys(self, current_offset as i64).unwrap();

            match keys.binary_search(key) {
                Ok(i) => {
                    if keys.is_leaf() {
                        return Some(-keys.get_data_offsets(i));
                    } else {
                        current_offset = keys.data_offsets[i] as usize;
                    }
                }
                Err(i) => {
                    if keys.is_leaf() {
                        return None;
                    } else {
                        if i == 0 {
                            return None;
                        }
                        current_offset = keys.data_offsets[i - 1] as usize;
                    }
                }
            }
        }
        None
    }

    fn mget_key_node_offsets(
        &self,
        offset: usize,
        keys: &[Cow<'_, [u8]>],
    ) -> Result<Vec<Option<i64>>> {
        let group = read_keys(self, offset as i64)?;

        if group.is_leaf() {
            return Ok(group.mget(keys));
        }

        let range = group.group_range(keys);

        let mut result = Vec::with_capacity(keys.len());
        for (offset, keys) in range {
            if let Some(offset) = offset {
                result.extend(self.mget_key_node_offsets(offset as usize, keys)?);
            } else {
                for _ in 0..keys.len() {
                    result.push(None);
                }
            }
        }

        Ok(result)
    }

    // Validate data file magic number
    fn validate_magic(data: &memmap2::Mmap) -> Result<()> {
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
    fn read_meta(node: &memmap2::Mmap) -> Result<(u16, u32)> {
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

struct NextLevel {
    keys: ItemGroup,
    index: usize,
}

struct ItemGroup {
    buffer: Vec<u8>,
    offsets: Vec<(usize, usize)>,
    data_offsets: Vec<i64>,
}

impl ItemGroup {
    fn is_leaf(&self) -> bool {
        self.data_offsets[0] < 0
    }

    fn len(&self) -> usize {
        self.data_offsets.len()
    }

    fn get_uncheck(&self, index: usize) -> (&[u8], i64) {
        let (start, end) = self.offsets[index];
        (self.buffer[start..end].as_ref(), self.data_offsets[index])
    }

    pub fn get(&self, index: usize) -> Option<(&[u8], i64)> {
        if index < self.len() {
            Some(self.get_uncheck(index))
        } else {
            None
        }
    }

    fn get_data_offsets(&self, index: usize) -> i64 {
        self.data_offsets[index]
    }

    fn binary_index(&self, key: &[u8]) -> usize {
        //if keys is empty, return the first index
        if key.is_empty() {
            return 0;
        }

        let is_leaf = self.is_leaf();

        match self.binary_search(key) {
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

    fn pre_binary_index(&self, key: &[u8]) -> usize {
        //if keys is empty, return the last index
        if key.is_empty() {
            return self.data_offsets.len();
        }

        match self.binary_search(key) {
            Ok(i) => i + 1,
            Err(i) => i,
        }
    }

    fn binary_search(&self, key: &[u8]) -> std::result::Result<usize, usize> {
        self.offsets
            .binary_search_by(|v| self.buffer[v.0..v.1].cmp(key))
    }

    /// Group the range of the array by the keys
    fn group_range<'a>(&self, keys: &'a [Cow<'a, [u8]>]) -> Vec<(Option<i64>, &[Cow<'a, [u8]>])> {
        let mut result = Vec::new();
        if self.data_offsets.is_empty() {
            return result;
        }

        let start = match self.get(0) {
            Some(s) => s,
            None => {
                result.push((None, keys));
                return result;
            }
        };

        let mut k_start = 0;
        for k in keys {
            if k.as_ref() < start.0 {
                k_start += 1;
            }
        }

        if k_start > 0 {
            result.push((None, &keys[..k_start]));
        }

        let mut v_start = 0;

        let mut start = None;

        while k_start < keys.len() && v_start < self.len() {
            let (pre, offset) = self.get_uncheck(v_start);

            match self.get(v_start + 1) {
                Some((next, _)) => match (
                    keys[k_start].as_ref().cmp(pre),
                    keys[k_start].as_ref().cmp(next),
                ) {
                    (std::cmp::Ordering::Greater, std::cmp::Ordering::Less)
                    | (std::cmp::Ordering::Equal, std::cmp::Ordering::Less) => {
                        if start.is_none() {
                            start = Some(k_start);
                        }
                        k_start += 1;
                    }
                    (std::cmp::Ordering::Greater, std::cmp::Ordering::Equal) => {
                        if let Some(s) = start {
                            if k_start > s {
                                result.push((Some(offset), &keys[s..k_start]));
                            }
                            start = Some(k_start);
                            k_start += 1;
                            v_start += 1;
                        } else {
                            v_start += 1;
                        }
                    }
                    (std::cmp::Ordering::Greater, std::cmp::Ordering::Greater) => {
                        if let Some(s) = start {
                            if k_start > s {
                                result.push((Some(offset), &keys[s..k_start]));
                            }
                            start = None;
                        }
                        v_start += 1;
                    }
                    _ => {
                        unreachable!()
                    }
                },
                None => {
                    if let Some(s) = start {
                        if k_start > s {
                            result.push((Some(offset), &keys[s..k_start]));
                        }
                        start = Some(k_start);
                        k_start += 1;
                    }
                    break;
                }
            }
        }

        if let Some(start) = start {
            if start < keys.len() {
                result.push((
                    Some(self.get_uncheck(usize::min(v_start, self.len() - 1)).1),
                    &keys[start..],
                ));
            }
        } else if k_start < keys.len() {
            result.push((
                Some(self.get_uncheck(usize::min(v_start, self.len() - 1)).1),
                &keys[k_start..],
            ));
        }

        result
    }

    fn mget(&self, keys: &[Cow<'_, [u8]>]) -> Vec<Option<i64>> {
        let mut result = Vec::with_capacity(keys.len());

        let mut v_start = 0;
        let mut k_start = 0;

        while k_start < keys.len() && v_start < self.len() {
            match self.get(v_start) {
                Some((v, o)) => match keys[k_start].as_ref().cmp(v) {
                    std::cmp::Ordering::Less => {
                        result.push(None);
                        k_start += 1;
                    }
                    std::cmp::Ordering::Equal => {
                        result.push(Some(o));
                        k_start += 1;
                        v_start += 1;
                    }
                    std::cmp::Ordering::Greater => {
                        v_start += 1;
                    }
                },
                None => {
                    break;
                }
            };
        }

        for _ in result.len()..keys.len() {
            result.push(None);
        }
        result
    }
}

/// Iterator implementation for TreeReader
pub struct Iter<'a, K, V> {
    reader: &'a TreeReader<K, V>,
    stack: LinkedList<NextLevel>,
    seek_key: Vec<u8>,
}

impl<'a, K, V> Iter<'a, K, V> {
    fn new(reader: &'a TreeReader<K, V>) -> Result<Self> {
        let mut stack = LinkedList::new();
        let keys = read_keys(reader, reader.root_offset as i64)?;
        stack.push_back(NextLevel { keys, index: 0 });
        Ok(Self {
            reader,
            stack,
            seek_key: vec![],
        })
    }

    pub fn reset(&mut self) -> Result<()> {
        self.seek_key.clear();
        if self.stack.len() == 1 && self.stack.front().unwrap().index == 0 {
            return Ok(());
        }

        let keys = match self.stack.pop_front() {
            Some(root) => root.keys,
            None => read_keys(self.reader, self.reader.root_offset as i64)?,
        };
        self.stack.clear();
        self.stack.push_back(NextLevel { keys, index: 0 });
        Ok(())
    }

    pub fn seek_first(&mut self) -> Result<()> {
        self.reset()
    }

    pub fn seek(&mut self, key: &K) -> Result<()> {
        self.reset()?;

        self.seek_key = self.reader.deserializer.serialize_key(key).to_vec();

        let back = self.stack.back_mut().unwrap();
        back.index = back.keys.binary_index(&self.seek_key);

        loop {
            if self.stack.is_empty() {
                return Ok(());
            }

            let back = self.stack.back_mut().unwrap();

            if back.index >= back.keys.len() {
                match self.stack.pop_back() {
                    Some(_) => continue,
                    None => return Ok(()),
                }
            }

            let offset = back.keys.data_offsets[back.index];
            if offset < 0 {
                return Ok(());
            }
            back.index += 1;
            let keys = read_keys(self.reader, offset)?;
            let index = keys.binary_index(&self.seek_key);

            self.stack.push_back(NextLevel { keys, index });
        }
    }

    pub fn next(&mut self) -> Result<Option<(Vec<u8>, V)>> {
        loop {
            if self.stack.is_empty() {
                return Ok(None);
            }

            let back = self.stack.back_mut().unwrap();

            if back.index >= back.keys.len() {
                match self.stack.pop_back() {
                    Some(_) => continue,
                    None => return Ok(None),
                }
            }

            let back = self.stack.back_mut().unwrap();

            let (key, offset) = back.keys.get_uncheck(back.index);
            back.index += 1;

            if offset < 0 {
                return Ok(Some((key.to_vec(), self.reader.read_value(-offset))));
            }

            let keys = read_keys(self.reader, offset)?;

            let index = keys.binary_index(&self.seek_key);

            self.stack.push_back(NextLevel { keys, index });
        }
    }

    pub fn seek_last(&mut self) -> Result<()> {
        self.reset()?;
        self.inner_seek_prev()
    }

    pub fn seek_prev(&mut self, key: &K) -> Result<()> {
        self.reset()?;
        self.seek_key = self.reader.deserializer.serialize_key(key).to_vec();
        self.inner_seek_prev()
    }

    fn inner_seek_prev(&mut self) -> Result<()> {
        let back = self.stack.back_mut().unwrap();
        back.index = back.keys.pre_binary_index(&self.seek_key);

        loop {
            let back = self.stack.back_mut().unwrap();
            let offset = back.keys.data_offsets[back.index - 1];
            if offset < 0 {
                return Ok(());
            }
            back.index -= 1;
            let keys = read_keys(self.reader, offset)?;
            let index = keys.pre_binary_index(&self.seek_key);
            self.stack.push_back(NextLevel { keys, index });
        }
    }

    pub fn prev(&mut self) -> Result<Option<(Vec<u8>, V)>> {
        loop {
            if self.stack.is_empty() {
                return Ok(None);
            }

            let back = self.stack.back_mut().unwrap();

            if back.index == 0 {
                match self.stack.pop_back() {
                    Some(_) => continue,
                    None => return Ok(None),
                }
            }

            let (key, offset) = back.keys.get_uncheck(back.index - 1);
            back.index -= 1;

            let offset = offset;
            if offset < 0 {
                return Ok(Some((key.to_vec(), self.reader.read_value(-offset))));
            }

            let keys = read_keys(self.reader, offset)?;

            let index = keys.pre_binary_index(&self.seek_key);

            self.stack.push_back(NextLevel { keys, index });
        }
    }
}

fn read_keys<'a, K, V>(reader: &'a TreeReader<K, V>, offset: i64) -> Result<ItemGroup> {
    let mut offset = offset as usize;
    let data_offsets = i64_coder::read(&reader.node, &mut offset);

    let node_data_len = zigzag::read_u32(&reader.node, &mut offset) as usize;

    let mut buffer = Vec::new();
    {
        let mut decoder =
            flate2::read::ZlibDecoder::new(&reader.node[offset..offset + node_data_len]);
        decoder.read_to_end(&mut buffer).unwrap_or_else(|_| 0);
    }

    offset += node_data_len;

    let node_length = if reader.var_len {
        u16_coder::read(&reader.node, &mut offset)
    } else {
        vec![reader.key_len; data_offsets.len()]
    };

    let mut offsets = Vec::with_capacity(node_length.len());
    let mut start = 0;
    for i in node_length {
        offsets.push((start, start + i as usize));
        start += i as usize;
    }

    Ok(ItemGroup {
        buffer,
        offsets,
        data_offsets,
    })
}

#[test]
fn test_item_group_range() {
    let keys: Vec<Cow<'_, [u8]>> = vec![
        Cow::Owned(vec![1]),
        Cow::Owned(vec![2]),
        Cow::Owned(vec![3]),
        Cow::Owned(vec![4]),
        Cow::Owned(vec![5]),
        Cow::Owned(vec![6]),
        Cow::Owned(vec![7]),
        Cow::Owned(vec![8]),
        Cow::Owned(vec![9]),
        Cow::Owned(vec![10]),
        Cow::Owned(vec![100]),
    ];

    let group = ItemGroup {
        buffer: vec![1, 3, 5, 7, 9, 11, 13, 15, 17],
        offsets: vec![
            (0, 1),
            (1, 2),
            (2, 3),
            (3, 4),
            (4, 5),
            (5, 6),
            (6, 7),
            (7, 8),
            (8, 9),
        ],
        data_offsets: vec![1, 3, 5, 7, 9, 11, 13, 15, 17],
    };

    let result = group.group_range(&keys);

    assert_eq!(result.len(), 6);
    assert_eq!(result[0].0, Some(1));
    assert_eq!(result[0].1.len(), 2);

    let keys = vec![Cow::Owned(vec![1]), Cow::Owned(vec![100])];
    let result = group.group_range(&keys);
    assert_eq!(result.len(), 2);
    assert_eq!(result[0].0, Some(1));
    assert_eq!(result[0].1.len(), 1);

    let keys = vec![Cow::Owned(vec![0]), Cow::Owned(vec![100])];
    let result = group.group_range(&keys);
    assert_eq!(result.len(), 2);
    assert_eq!(result[0].0, None);
    assert_eq!(result[0].1.len(), 1);
}

#[test]
fn test_mget() {
    let keys: Vec<Cow<'_, [u8]>> = vec![
        Cow::Owned(vec![0]),
        Cow::Owned(vec![1]),
        Cow::Owned(vec![2]),
        Cow::Owned(vec![3]),
        Cow::Owned(vec![4]),
        Cow::Owned(vec![5]),
        Cow::Owned(vec![6]),
        Cow::Owned(vec![7]),
        Cow::Owned(vec![8]),
        Cow::Owned(vec![9]),
        Cow::Owned(vec![10]),
        Cow::Owned(vec![100]),
    ];

    let group = ItemGroup {
        buffer: vec![1, 3, 5, 7, 9, 11, 13, 15, 17],
        offsets: vec![
            (0, 1),
            (1, 2),
            (2, 3),
            (3, 4),
            (4, 5),
            (5, 6),
            (6, 7),
            (7, 8),
            (8, 9),
        ],
        data_offsets: vec![1, 3, 5, 7, 9, 11, 13, 15, 17],
    };

    let result = group.mget(&keys);

    assert_eq!(result.len(), 12);

    println!("{:?}", result);

    assert_eq!(
        result,
        vec![
            None,
            Some(1),
            None,
            Some(3),
            None,
            Some(5),
            None,
            Some(7),
            None,
            Some(9),
            None,
            None
        ]
    );
}
