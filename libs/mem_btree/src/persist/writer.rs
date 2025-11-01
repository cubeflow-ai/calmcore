use std::{
    fs::{File, OpenOptions},
    io::{BufWriter, Seek, SeekFrom, Write},
    path::PathBuf,
};

use byteorder::{BigEndian, WriteBytesExt};

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
    pub fn persist<'a, K: 'a + Clone, V, R>(
        &self,
        len: usize,
        serializer: Box<dyn KeySerializer<K, V, R>>,
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

    #[allow(dead_code)]
    fn clear(&mut self) {
        self.keys.clear();
        self.offsets.clear();
    }
}

struct ChunkWriter<'a, K, V, R> {
    second_level: Vec<Chunk<K>>,
    node_file: BufWriter<File>,
    serializer: &'a Box<dyn KeySerializer<K, V, R>>,
    current: Chunk<K>,
    chunk_size: usize,
    node_file_offset: u64, // Track node file offset manually
}

impl<'a, K, V, R> ChunkWriter<'a, K, V, R>
where
    K: Clone,
{
    fn new(
        chunk_size: usize,
        serializer: &'a Box<dyn KeySerializer<K, V, R>>,
        mut node_file: BufWriter<File>,
        key_len: u16,
        len: usize,
    ) -> Result<ChunkWriter<'a, K, V, R>> {
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

    #[allow(dead_code)]
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

        // Write offsets using delta-only encoding (all non-decreasing)
        // We need to track how many bytes i64_coder writes
        let mut temp_buf = Vec::new();
        num_ser::i64_coder::write_delta(&mut temp_buf, &chunk.offsets)?;
        self.node_file.write_all(&temp_buf)?;
        self.node_file_offset += temp_buf.len() as u64;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    // Tests removed - will be added back when needed
}
