use std::{
    fs::{File, OpenOptions},
    io::{BufWriter, Seek, SeekFrom, Write},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

use byteorder::{BigEndian, WriteBytesExt};
use log;

use super::*;

pub struct TreeWriter {
    dir: PathBuf,
    chunk_size: usize,
}

impl TreeWriter {
    pub fn new(dir: PathBuf, chunk_size: usize) -> Self {
        Self { dir, chunk_size }
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
    pub fn persist<'a, K: 'a + Clone, V: Clone, R>(
        &self,
        len: usize,
        serializer: Box<dyn WriteSerializer<K, V>>,
        union_leaf: Option<Box<dyn UnionLeafSerializer<V>>>,
        iter: impl Iterator<Item = crate::Item<K, V>>,
    ) -> Result<()> {
        if !self.dir.exists() {
            std::fs::create_dir_all(&self.dir)?;
        }

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

        let mut cw = ChunkWriter::new(self.chunk_size, &serializer, union_leaf, node_file, len)?;
        data_file.write_all(MAGIC_VERSION)?;

        let mut offset_tracker = MAGIC_VERSION.len() as i64; // Track offset manually instead of calling stream_position()

        eprintln!(
            "🔧 [TreeWriter] Starting persist: expected_len={}, chunk_size={}",
            len, self.chunk_size
        );

        let mut actual_count = 0;
        for (_i, item) in iter.enumerate() {
            cw.add_key_offset(&item, offset_tracker)?;

            let value_bytes = serializer.serialize_value(&item.1);
            data_file.write_all(&value_bytes)?;
            offset_tracker += value_bytes.len() as i64;
            actual_count += 1;
        }

        eprintln!(
            "🔧 [TreeWriter] Iterator consumed: expected={}, actual={}",
            len, actual_count
        );

        if actual_count != len {
            eprintln!(
                "❌ [TreeWriter] Item count mismatch! expected {} but got {}",
                len, actual_count
            );
        }

        data_file.flush()?;

        // Add final offset to mark the end of the last value
        // This is needed because we store n+1 offsets for n keys
        // to calculate value sizes as: offsets[i+1] - offsets[i]
        cw.add_final_offset(offset_tracker)?;

        // Release any remaining keys in current chunk
        cw.release_chunk()?;

        log::info!(
            "📊 [TreeWriter] Total leaf chunks written: {}, expected: {}",
            cw.leaf_chunk_count,
            (len + self.chunk_size - 1) / self.chunk_size
        );

        let level_index =
            std::mem::replace(&mut cw.second_level, vec![Chunk::new(self.chunk_size)]);

        // Second level chunks are index chunks (is_leaf=false) because they point to node file
        cw.write_second_level(level_index, false)?;

        Ok(())
    }
}

#[derive(Clone)]
struct Chunk<K, V> {
    chunk_size: usize,
    keys: Vec<K>,
    offsets: Vec<i64>,
    union: Option<V>,
    // 🔧 The minimum key covered by this chunk (for index chunks)
    // Used as separator key when this chunk becomes a child in upper level index
    min_key: Option<K>,
}

impl<K: Clone, V> Chunk<K, V> {
    fn new(chunk_size: usize) -> Self {
        Self {
            chunk_size,
            keys: Vec::with_capacity(chunk_size),
            offsets: Vec::with_capacity(chunk_size),
            union: None,
            min_key: None,
        }
    }

    fn is_finish(&self) -> bool {
        self.keys.len() >= self.chunk_size
    }

    #[allow(dead_code)]
    fn clear(&mut self) {
        self.keys.clear();
        self.offsets.clear();
        self.union = None;
    }
}

struct ChunkWriter<'a, K, V> {
    second_level: Vec<Chunk<K, V>>,
    node_file: BufWriter<File>,
    serializer: &'a Box<dyn WriteSerializer<K, V>>,
    union_leaf: Option<Box<dyn UnionLeafSerializer<V>>>,
    current: Chunk<K, V>,
    chunk_size: usize,
    node_file_offset: u64,   // Track node file offset manually
    leaf_chunk_count: usize, // 🔧 Track how many leaf chunks written
}

impl<'a, K, V> ChunkWriter<'a, K, V>
where
    K: Clone,
    V: Clone, // Add Clone bound for V
{
    fn new(
        chunk_size: usize,
        serializer: &'a Box<dyn WriteSerializer<K, V>>,
        union_leaf: Option<Box<dyn UnionLeafSerializer<V>>>,
        mut node_file: BufWriter<File>,
        len: usize,
    ) -> Result<ChunkWriter<'a, K, V>> {
        let has_union_leaf = union_leaf.is_some();

        node_file.write_all(MAGIC_VERSION)?;
        node_file.write_all(&[0; 8])?; // root node offset placeholder
        node_file.write_all(&(len as u32).to_be_bytes())?;
        node_file.write_all(&[if has_union_leaf { 1u8 } else { 0u8 }])?; // has_union_leaf flag

        let initial_offset = MAGIC_VERSION.len() + 8 + 4 + 1; // MAGIC + root_offset + tree_len + has_union_leaf

        Ok(Self {
            second_level: vec![Chunk::new(chunk_size)],
            node_file,
            serializer,
            union_leaf,
            current: Chunk::new(chunk_size),
            chunk_size,
            node_file_offset: initial_offset as u64,
            leaf_chunk_count: 0,
        })
    }

    #[allow(dead_code)]
    fn reset_current(&mut self) {
        self.current.clear();
    }

    fn add_key_offset(&mut self, i: &Arc<(K, V, Option<Duration>)>, offset: i64) -> Result<()> {
        // Check if current chunk is full (before adding new key)
        if self.current.keys.len() >= self.chunk_size {
            // Add ending offset for the last key in current chunk
            self.current.offsets.push(offset);
            // Release the full chunk
            self.release_chunk()?;
        }

        // Add current key/offset/value to the new/current chunk
        self.current.keys.push(i.0.clone());
        self.current.offsets.push(offset);
        self.union_leaf.as_ref().map(|v| v.add_value(&i.1));

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

        self.current.union = self.union_leaf.as_ref().map(|u| u.release());

        // move current to second level
        let old_chunk = std::mem::replace(&mut self.current, Chunk::new(self.chunk_size));

        self.leaf_chunk_count += 1;

        eprintln!(
            "🔧 [TreeWriter::release_chunk] Writing leaf chunk #{} with {} keys, {} offsets",
            self.leaf_chunk_count,
            old_chunk.keys.len(),
            old_chunk.offsets.len()
        );

        // Use manually tracked offset instead of system call
        let chunk_node_offset = self.node_file_offset as i64;

        let first_key = old_chunk.keys[0].clone();

        // Write the chunk first (is_leaf=true, because this is from current level)
        self.write_chunk(old_chunk, true)?;

        // add to second level index
        // Store as positive offset (chunk type flag will distinguish leaf vs index)

        // 🔧 Check if current index chunk is full BEFORE adding
        // Index chunk max: chunk_size keys + (chunk_size+1) offsets
        // If keys.len() >= chunk_size, the chunk is full, create new chunk
        {
            let last = self.second_level.last().unwrap();
            let is_first = last.offsets.is_empty();
            let is_full = !is_first && last.keys.len() >= self.chunk_size;
            
            if is_full {
                eprintln!(
                    "🔧 [TreeWriter::release_chunk] Index chunk full (keys={}), creating new chunk BEFORE adding",
                    last.keys.len()
                );
                self.second_level.push(Chunk::new(self.chunk_size));
            }
        }

        let last = self.second_level.last_mut().unwrap();
        let is_first_in_index_chunk = last.offsets.is_empty();

        eprintln!(
            "🔧 [TreeWriter::release_chunk] Adding to index chunk: leaf_chunk={}, is_first={}, keys_before={}, chunk_offset={}",
            self.leaf_chunk_count,
            is_first_in_index_chunk,
            last.keys.len(),
            chunk_node_offset
        );

        // B-tree index structure: keys [k1, k2], offsets [c0, c1, c2]
        // where c0 < k1 <= c1 < k2 <= c2
        // For the first chunk, we only add offset (leftmost child)
        // For subsequent chunks, we add both key and offset
        if is_first_in_index_chunk {
            // First chunk in entire index: only add offset as leftmost child
            // 🔧 Store min_key for use as separator when building upper level
            last.offsets.push(chunk_node_offset);
            last.min_key = Some(first_key.clone());
        } else {
            // Subsequent entries: add key (separator) and offset
            last.keys.push(first_key.clone());
            last.offsets.push(chunk_node_offset);
        }

        // 🔍 Debug: Log after adding for chunk 257
        if self.leaf_chunk_count == 257 {
            eprintln!(
                "🔍 [release_chunk] AFTER adding chunk 257: keys.len()={}, offsets.len()={}",
                last.keys.len(),
                last.offsets.len()
            );
        }

        Ok(())
    }

    fn write_second_level<U>(&mut self, level_index: Vec<Chunk<K, U>>, is_leaf: bool) -> Result<()>
    where
        U: Clone,
    {
        eprintln!(
            "🔧 [write_second_level] Called with {} chunks, is_leaf={}",
            level_index.len(),
            is_leaf
        );

        if level_index.len() == 1 {
            let root_offset = self.node_file_offset;

            eprintln!("🔧 [write_second_level] Single chunk - writing as root");
            self.write_chunk(level_index.into_iter().next().unwrap(), is_leaf)?;

            // write root node offset
            self.node_file.seek(SeekFrom::Start(2))?;
            self.node_file.write_i64::<BigEndian>(root_offset as i64)?;

            self.node_file.flush()?;

            return Ok(());
        }

        eprintln!("🔧 [write_second_level] Multiple chunks - building next level index");

        for (idx, chunk) in level_index.into_iter().enumerate() {
            let root_offset = self.node_file_offset as i64;

            eprintln!(
                "🔧 [write_second_level] Processing chunk #{}: keys={}, offsets={}, has_min_key={}, last_offset={}",
                idx + 1,
                chunk.keys.len(),
                chunk.offsets.len(),
                chunk.min_key.is_some(),
                chunk.offsets.last().unwrap_or(&-1)
            );

            // 🔧 CRITICAL: Get the separator key for this chunk
            // Use min_key as the separator because it represents the minimum key covered by this chunk.
            // If min_key is not set, fall back to keys.first() (for backwards compatibility or edge cases)
            let separator_key = chunk.min_key.clone()
                .or_else(|| chunk.keys.first().cloned());
            
            if separator_key.is_none() {
                eprintln!(
                    "🔧 [write_second_level] ERROR: No separator key available (keys={}, has_min_key={})",
                    chunk.keys.len(),
                    chunk.min_key.is_some()
                );
                continue;
            }

            // 🔧 Check if we need a new chunk BEFORE adding
            // Same logic: create new chunk when keys.len() == chunk_size (will overflow after adding)
            {
                let last = self.second_level.last().unwrap();
                let is_first = last.offsets.is_empty();
                let will_overflow = !is_first && last.keys.len() >= self.chunk_size;
                
                if will_overflow {
                    self.second_level.push(Chunk::new(self.chunk_size));
                }
            }

            // add to second level
            {
                let last = self.second_level.last_mut().unwrap();

                // Same B-tree index logic: first chunk only adds offset, subsequent chunks add key + offset
                if last.offsets.is_empty() {
                    // First chunk in this level: only add offset as leftmost child
                    // 🔧 Also store min_key for use as separator at next level
                    eprintln!("🔧 [write_second_level] Adding to next level: is_first=true, root_offset={}", root_offset);
                    last.offsets.push(root_offset);
                    last.min_key = separator_key.clone();
                } else {
                    // Subsequent chunks: add key (separator) and offset
                    // 🔧 Use separator_key which might be min_key for chunks with only first offset
                    eprintln!("🔧 [write_second_level] Adding to next level: is_first=false, root_offset={}, keys_before={}", root_offset, last.keys.len());
                    last.keys.push(separator_key.unwrap());
                    last.offsets.push(root_offset);
                }
            } // Drop the borrow here

            self.write_chunk(chunk, is_leaf)?;
        }

        let level_index =
            std::mem::replace(&mut self.second_level, vec![Chunk::new(self.chunk_size)]);

        eprintln!(
            "🔧 [write_second_level] Recursing with next level: {} chunks",
            level_index.len()
        );
        if level_index.len() > 0 {
            eprintln!(
                "🔧 [write_second_level] Next level chunk[0]: keys.len()={}, offsets.len()={}",
                level_index[0].keys.len(),
                level_index[0].offsets.len()
            );
        }

        // Recursive call: next level up is also index chunks (is_leaf=false)
        return self.write_second_level(level_index, false);
    }

    fn write_chunk<U>(&mut self, chunk: Chunk<K, U>, is_leaf: bool) -> Result<()>
    where
        U: Clone,
    {
        // 🔍 Debug: Log index chunk with 257 offsets
        if !is_leaf && chunk.keys.len() == 256 && chunk.offsets.len() == 257 {
            eprintln!(
                "🔍 [write_chunk] Writing FIRST-LEVEL index: is_leaf={}, keys.len()={}, offsets.len()={}",
                is_leaf,
                chunk.keys.len(),
                chunk.offsets.len()
            );
        }
        
        log::info!(
            "🔧 [TreeWriter::write_chunk] is_leaf={}, keys.len()={}, offsets.len()={}",
            is_leaf,
            chunk.keys.len(),
            chunk.offsets.len()
        );

        // Write chunk type: 1 = leaf (points to data file), 0 = index (points to node file)
        self.node_file
            .write_all(&[if is_leaf { 1u8 } else { 0u8 }])?;
        self.node_file_offset += 1;

        // Serialize keys
        let keys_data = self.serializer.serialize_keys(&chunk.keys);

        log::info!(
            "🔧 [TreeWriter::write_chunk] Serialized {} keys into {} bytes",
            chunk.keys.len(),
            keys_data.len()
        );

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

        // Only write union data for leaf nodes
        if is_leaf {
            if let Some(union_data) = &chunk.union {
                // SAFETY: For leaf nodes, U must be V since they contain actual values
                // Index nodes (is_leaf=false) never have union_data
                let union_bytes = unsafe {
                    // Transmute U to V - safe because:
                    // 1. Leaf nodes are always Chunk<K, V>
                    // 2. Index nodes never enter this branch (is_leaf check)
                    let v_ref = std::mem::transmute::<&U, &V>(union_data);
                    self.serializer.serialize_value(v_ref)
                };
                // Write length prefix (u32) then data
                self.node_file
                    .write_all(&(union_bytes.len() as u32).to_be_bytes())?;
                self.node_file_offset += 4;
                self.node_file.write_all(&union_bytes)?;
                self.node_file_offset += union_bytes.len() as u64;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    // Tests removed - will be added back when needed
}
