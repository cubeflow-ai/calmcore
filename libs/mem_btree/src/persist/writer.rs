use std::{io::SeekFrom, sync::RwLock, vec};

use byteorder::{BigEndian, WriteBytesExt};
use parquet::file::serialized_reader;

use crate::{
    node,
    persist::num_ser::{i64_coder, u16_coder},
};

use super::*;

pub struct TreeWriter {
    dir: PathBuf,
    chunk_size: usize,
    key_len: u16,
}

impl TreeWriter {
    pub fn new(dir: PathBuf, chunk_size: usize, key_len: u16) -> Self {
        Self {
            dir,
            chunk_size,
            key_len,
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
impl TreeWriter {
    pub fn persist<'a, K: 'a + Clone, V>(
        &self,
        len: usize,
        serializer: Box<dyn KeySerializer<K, V>>,
        iter: impl Iterator<Item = crate::Item<K, V>>,
    ) -> Result<()> {
        println!("persist tree len:{}", len);
        if !self.dir.exists() {
            std::fs::create_dir_all(&self.dir)?;
        }

        println!("===============persist tree len:{}", self.dir.exists());

        let node_file = BufWriter::new(
            OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(self.dir.join(NODE_NAME))?,
        );

        let mut data_file = BufWriter::new(
            OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(self.dir.join(DATA_NAME))?,
        );

        let mut cw = ChunkWriter::new(self.chunk_size, &serializer, node_file, self.key_len, len)?;
        data_file.write_all(MAGIC_VERSION)?;

        let write_start = std::time::Instant::now();
        let mut offset_tracker = MAGIC_VERSION.len() as i64; // Track offset manually instead of calling stream_position()

        for (i, item) in iter.enumerate() {
            cw.add_key_offset(&item.0.clone(), offset_tracker)?;

            let value_bytes = serializer.serialize_value(&item.1);
            data_file.write_all(&value_bytes)?;
            offset_tracker += value_bytes.len() as i64;

            if i % 1_000_000 == 0 && i > 0 {
                println!(
                    "  Written {} million records in {:?}",
                    i / 1_000_000,
                    write_start.elapsed()
                );
            }
        }
        println!("Writing data completed in {:?}", write_start.elapsed());

        data_file.flush()?;

        // Add final offset to mark the end of the last chunk
        cw.add_final_offset(offset_tracker)?;

        cw.release_chunk()?;

        let level_index =
            std::mem::replace(&mut cw.second_level, vec![Chunk::new(self.chunk_size)]);

        // Second level chunks are index chunks (is_leaf=false) because they point to node file
        cw.write_second_level(level_index, false)?;

        Ok(())
    }
}

#[derive(Clone)]
struct Chunk<K> {
    chunk_size: usize,
    keys: Vec<K>,
    offsets: Vec<i64>,
}

impl<K: Clone> Chunk<K> {
    fn new(chunk_size: usize) -> Self {
        Self {
            chunk_size,
            keys: Vec::with_capacity(chunk_size),
            offsets: Vec::with_capacity(chunk_size),
        }
    }

    fn is_finish(&self) -> bool {
        self.keys.len() >= self.chunk_size
    }

    fn clear(&mut self) {
        self.keys.clear();
        self.offsets.clear();
    }
}

struct ChunkWriter<'a, K, V> {
    second_level: Vec<Chunk<K>>,
    node_file: BufWriter<File>,
    serializer: &'a Box<dyn KeySerializer<K, V>>,
    current: Chunk<K>,
    chunk_size: usize,
    node_file_offset: u64, // Track node file offset manually
}

impl<'a, K, V> ChunkWriter<'a, K, V>
where
    K: Clone,
{
    fn new(
        chunk_size: usize,
        serializer: &'a Box<dyn KeySerializer<K, V>>,
        mut node_file: BufWriter<File>,
        key_len: u16,
        len: usize,
    ) -> Result<ChunkWriter<'a, K, V>> {
        node_file.write_all(MAGIC_VERSION)?;
        node_file.write_all(&[0; 8])?; // root node offset placeholder
        node_file.write_all(&key_len.to_be_bytes())?;
        node_file.write_all(&(len as u32).to_be_bytes())?;

        let initial_offset = MAGIC_VERSION.len() + 8 + 2 + 4; // MAGIC + root_offset + key_len + tree_len

        Ok(Self {
            second_level: vec![Chunk::new(chunk_size)],
            node_file,
            serializer,
            current: Chunk::new(chunk_size),
            chunk_size,
            node_file_offset: initial_offset as u64,
        })
    }

    fn reset_current(&mut self) {
        self.current.clear();
    }

    fn add_key_offset(&mut self, k: &K, offset: i64) -> Result<()> {
        if self.current.keys.len() >= self.chunk_size {
            // move current to second level
            self.release_chunk()?;
        }

        self.current.keys.push(k.clone());
        self.current.offsets.push(offset);

        Ok(())
    }

    /// Add the final offset to mark the end position of the last value
    /// This allows calculating value sizes as: offsets[i+1] - offsets[i]
    fn add_final_offset(&mut self, offset: i64) -> Result<()> {
        // Add the final offset to current chunk
        self.current.offsets.push(offset);
        Ok(())
    }

    fn release_chunk(&mut self) -> Result<()> {
        if self.current.keys.len() == 0 {
            return Ok(());
        }

        // move current to second level
        let old_chunk = std::mem::replace(&mut self.current, Chunk::new(self.chunk_size));

        // Use manually tracked offset instead of system call
        let chunk_node_offset = self.node_file_offset as i64;

        // Write the chunk first (is_leaf=true, because this is from current level)
        self.write_chunk(old_chunk.clone(), true)?;

        // add to second level index
        // Store as positive offset (chunk type flag will distinguish leaf vs index)
        let last: &mut Chunk<K> = self.second_level.last_mut().unwrap();
        last.keys.push(old_chunk.keys[0].clone());
        last.offsets.push(chunk_node_offset);

        if last.is_finish() {
            self.second_level.push(Chunk::new(self.chunk_size));
        }

        Ok(())
    }

    fn write_second_level(&mut self, level_index: Vec<Chunk<K>>, is_leaf: bool) -> Result<()> {
        if level_index.len() == 1 {
            let root_offset = self.node_file_offset;

            self.write_chunk(level_index.into_iter().next().unwrap(), is_leaf)?;

            // write root node offset
            self.node_file.seek(SeekFrom::Start(2))?;
            self.node_file.write_i64::<BigEndian>(root_offset as i64)?;

            self.node_file.flush()?;

            return Ok(());
        }

        for chunk in level_index {
            let root_offset = self.node_file_offset as i64;

            // add to second level
            let last: &mut Chunk<K> = self.second_level.last_mut().unwrap();
            last.keys.push(chunk.keys[0].clone());
            last.offsets.push(root_offset);

            self.write_chunk(chunk, is_leaf)?;
        }

        let level_index =
            std::mem::replace(&mut self.second_level, vec![Chunk::new(self.chunk_size)]);

        // Recursive call: next level up is also index chunks (is_leaf=false)
        return self.write_second_level(level_index, false);
    }

    fn write_chunk(&mut self, chunk: Chunk<K>, is_leaf: bool) -> Result<()> {
        // Write chunk type: 1 = leaf (points to data file), 0 = index (points to node file)
        self.node_file
            .write_all(&[if is_leaf { 1u8 } else { 0u8 }])?;
        self.node_file_offset += 1;

        // Serialize keys
        let keys_data = self.serializer.serialize_keys(&chunk.keys);

        // Write keys length as u32, then keys data
        self.node_file
            .write_all(&(keys_data.len() as u32).to_be_bytes())?;
        self.node_file_offset += 4;

        self.node_file.write_all(&keys_data)?;
        self.node_file_offset += keys_data.len() as u64;

        // Write offsets using i64_coder (all positive now)
        // We need to track how many bytes i64_coder writes
        let mut temp_buf = Vec::new();
        num_ser::i64_coder::write(&mut temp_buf, &chunk.offsets)?;
        self.node_file.write_all(&temp_buf)?;
        self.node_file_offset += temp_buf.len() as u64;

        Ok(())
    }
}

mod test {
    use std::{borrow::Cow, path::PathBuf};

    use crate::{
        persist::{self, num_ser::i64_coder, KeySerializer},
        BTree,
    };

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
            value.to_be_bytes().to_vec().into()
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
    fn test_tree_writer() {
        std::fs::remove_dir_all("/tmp/test_tree_writer").ok();

        let mut tree = BTree::new(32);

        let start = std::time::Instant::now();

        for i in 0..1000000 as i64 {
            tree.put(i, i);
        }

        println!("build tree cost:{:?}", start.elapsed());

        persist::writer::TreeWriter::new(PathBuf::from("/tmp/test_tree_writer"), 128, 0)
            .persist(tree.len(), Box::new(I64KeySerializer {}), tree.iter())
            .unwrap();

        println!("persist tree cost:{:?}", start.elapsed());
    }

    struct StringKeySerializer;

    impl KeySerializer<String, Vec<u8>> for StringKeySerializer {
        fn serialize_keys<'a>(&self, keys: &'a Vec<String>) -> Cow<'a, [u8]> {
            let mut buf = Vec::new();

            // 写入键的数量
            buf.extend_from_slice(&(keys.len() as u32).to_be_bytes());

            // 写入每个键
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

                let len =
                    u32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]])
                        as usize;
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

    #[test]
    fn test_tree_writer_1m_strings() {
        std::fs::remove_dir_all("/tmp/test_tree_writer_1m").ok();

        let mut tree = BTree::new(32);

        let start = std::time::Instant::now();

        // 生成10字节的value数据（小value避免磁盘缓存失效）
        let value_10b: Vec<u8> = (0..10).map(|i| (i % 256) as u8).collect();

        println!("Building tree with 1M strings (10 bytes each)...");

        for i in 0..1_000_000 {
            // 生成唯一的字符串key，使用格式化确保排序正确
            let key = format!("key_{:010}", i);
            tree.put(key, value_10b.clone());

            if i % 100_000 == 0 && i > 0 {
                println!("Inserted {} records, elapsed: {:?}", i, start.elapsed());
            }
        }

        println!("Build tree completed! Total time: {:?}", start.elapsed());
        println!("Tree size: {} entries", tree.len());

        let persist_start = std::time::Instant::now();

        persist::writer::TreeWriter::new(
            PathBuf::from("/tmp/test_tree_writer_1m"),
            1024, // chunk_size (increased from 128 to reduce tree height)
            0,    // key_len (variable length)
        )
        .persist(tree.len(), Box::new(StringKeySerializer {}), tree.iter())
        .unwrap();

        println!(
            "Persist tree completed! Time: {:?}",
            persist_start.elapsed()
        );
        println!("Total time: {:?}", start.elapsed());

        // 检查文件大小
        if let Ok(metadata) = std::fs::metadata("/tmp/test_tree_writer_1m/NODE") {
            println!("NODE file size: {} MB", metadata.len() / 1024 / 1024);
        }
        if let Ok(metadata) = std::fs::metadata("/tmp/test_tree_writer_1m/DATA") {
            println!("DATA file size: {} MB", metadata.len() / 1024 / 1024);
        }
    }

    #[test]
    fn test_iterator_performance_1m() {
        let mut tree = BTree::new(32);
        let value_1kb: Vec<u8> = (0..1024).map(|i| (i % 256) as u8).collect();

        println!("Building tree with 1M strings...");
        let start = std::time::Instant::now();
        for i in 0..1_000_000 {
            let key = format!("key_{:010}", i);
            tree.put(key, value_1kb.clone());
        }
        println!("Build completed in {:?}", start.elapsed());

        // Test 1: Pure iteration without any work
        println!("\nTest 1: Pure iteration (just counting)");
        let iter_start = std::time::Instant::now();
        let count = tree.iter().count();
        println!("  Counted {} items in {:?}", count, iter_start.elapsed());

        // Test 2: Iteration with key cloning (模拟 persist 对 key 的处理)
        println!("\nTest 2: Key cloning only");
        let iter_start = std::time::Instant::now();
        let mut total_key_len = 0usize;
        for item in tree.iter() {
            let _key = item.0.clone(); // persist 中 add_key_offset 会克隆
            total_key_len += _key.len();
        }
        println!(
            "  Processed {} bytes of keys in {:?}",
            total_key_len,
            iter_start.elapsed()
        );

        // Test 3: 模拟完整的 persist 操作（key clone + value 引用访问）
        println!("\nTest 3: Simulate persist (key clone + value access)");
        let iter_start = std::time::Instant::now();
        let mut total_len = 0usize;
        for item in tree.iter() {
            let _key = item.0.clone(); // persist 中会克隆
            let value_ref = &item.1[..]; // persist 中只访问引用
            total_len += _key.len() + value_ref.len();
        }
        println!(
            "  Processed {} bytes total in {:?}",
            total_len,
            iter_start.elapsed()
        );
    }

    #[test]
    fn test_iterator_performance_10m() {
        let mut tree = BTree::new(32);
        let value_1kb: Vec<u8> = (0..1024).map(|i| (i % 256) as u8).collect();

        println!("Building tree with 10M strings...");
        let start = std::time::Instant::now();
        for i in 0..10_000_000 {
            let key = format!("key_{:010}", i);
            tree.put(key, value_1kb.clone());
            if i % 1_000_000 == 0 && i > 0 {
                println!("  Inserted {} million", i / 1_000_000);
            }
        }
        println!("Build completed in {:?}", start.elapsed());

        // Test 1: Pure iteration without any work
        println!("\nTest 1: Pure iteration (just counting)");
        let iter_start = std::time::Instant::now();
        let mut count = 0;
        for (i, _) in tree.iter().enumerate() {
            count += 1;
            if i % 1_000_000 == 0 && i > 0 {
                println!(
                    "  Counted {} million in {:?}",
                    i / 1_000_000,
                    iter_start.elapsed()
                );
            }
        }
        println!(
            "  Total counted {} items in {:?}",
            count,
            iter_start.elapsed()
        );

        // Test 2: Key cloning only (模拟 persist 对 key 的处理)
        println!("\nTest 2: Key cloning only");
        let iter_start = std::time::Instant::now();
        let mut total_key_len = 0usize;
        for (i, item) in tree.iter().enumerate() {
            let _key = item.0.clone(); // persist 中 add_key_offset 会克隆
            total_key_len += _key.len();
            if i % 1_000_000 == 0 && i > 0 {
                println!(
                    "  Processed {} million keys in {:?}",
                    i / 1_000_000,
                    iter_start.elapsed()
                );
            }
        }
        println!(
            "  Processed {} bytes of keys in {:?}",
            total_key_len,
            iter_start.elapsed()
        );

        // Test 3: 模拟完整的 persist 操作（key clone + value 引用访问）
        println!("\nTest 3: Simulate persist (key clone + value access)");
        let iter_start = std::time::Instant::now();
        let mut total_len = 0usize;
        for (i, item) in tree.iter().enumerate() {
            let _key = item.0.clone(); // persist 中会克隆
            let value_ref = &item.1[..]; // persist 中只访问引用
            total_len += _key.len() + value_ref.len();
            if i % 1_000_000 == 0 && i > 0 {
                println!(
                    "  Processed {} million items in {:?}",
                    i / 1_000_000,
                    iter_start.elapsed()
                );
            }
        }
        println!(
            "  Processed {} bytes total in {:?}",
            total_len,
            iter_start.elapsed()
        );
    }

    #[test]
    #[ignore] // 标记为 ignore，需要时手动运行
    fn test_tree_writer_large_strings() {
        std::fs::remove_dir_all("/tmp/test_tree_writer_large").ok();

        let mut tree = BTree::new(32);

        let start = std::time::Instant::now();

        // 生成10字节的value数据（小value避免磁盘缓存失效）
        let value_10b: Vec<u8> = (0..10).map(|i| (i % 256) as u8).collect();

        println!("Building tree with 10M strings (10 bytes each)...");

        for i in 0..10_000_000 {
            // 生成唯一的字符串key，使用格式化确保排序正确
            let key = format!("key_{:010}", i);
            tree.put(key, value_10b.clone());

            if i % 1_000_000 == 0 && i > 0 {
                println!(
                    "Inserted {} million records, elapsed: {:?}",
                    i / 1_000_000,
                    start.elapsed()
                );
            }
        }

        println!("Build tree completed! Total time: {:?}", start.elapsed());
        println!("Tree size: {} entries", tree.len());

        let persist_start = std::time::Instant::now();

        persist::writer::TreeWriter::new(
            PathBuf::from("/tmp/test_tree_writer_large"),
            128, // chunk_size
            0,   // key_len (variable length)
        )
        .persist(tree.len(), Box::new(StringKeySerializer {}), tree.iter())
        .unwrap();

        println!(
            "Persist tree completed! Time: {:?}",
            persist_start.elapsed()
        );
        println!("Total time: {:?}", start.elapsed());

        // 检查文件大小
        if let Ok(metadata) = std::fs::metadata("/tmp/test_tree_writer_large/NODE") {
            println!("NODE file size: {} MB", metadata.len() / 1024 / 1024);
        }
        if let Ok(metadata) = std::fs::metadata("/tmp/test_tree_writer_large/DATA") {
            println!("DATA file size: {} MB", metadata.len() / 1024 / 1024);
        }
    }
}
