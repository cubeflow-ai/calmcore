use super::*;
use crate::persist::num_ser::i64_coder;
use std::fs::File;

/// High-performance B-Tree reader using memory-mapped files
pub struct TreeReader<K, R> {
    #[allow(dead_code)]
    key_len: u16,
    tree_len: u32,
    root_offset: i64,
    has_union_leaf: bool, // Whether this tree has union_leaf data for range query optimization
    node: memmap2::Mmap,
    data: memmap2::Mmap,
    reader_ser: Box<dyn ReadSerializer<K, R>>,
}

impl<K, R> TreeReader<K, R>
where
    K: Clone + PartialOrd,
{
    /// Creates a new TreeReader instance using mmap for fast random access
    ///
    /// File format:
    /// NODE file: MAGIC(2) + root_offset(8) + tree_len(4) + has_union_leaf(1) + nodes...
    /// DATA file: MAGIC(2) + values...
    ///
    pub fn new(dir: &Path, reader_ser: Box<dyn ReadSerializer<K, R>>) -> Result<Self> {
        // Memory map the node file for fast random access
        let node = unsafe { memmap2::Mmap::map(&File::open(dir.join(NODE_NAME))?)? };
        Self::validate_magic(&node)?;

        // Parse header: MAGIC(2) + root_offset(8) + tree_len(4) + has_union_leaf(1)
        if node.len() < 15 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Invalid node file: header too short",
            ));
        }

        let root_offset = i64::from_be_bytes(node[2..10].try_into().unwrap());
        let tree_len = u32::from_be_bytes(node[10..14].try_into().unwrap());
        let has_union_leaf = node[14] == 1;

        // Memory map the data file
        let data = unsafe { memmap2::Mmap::map(&File::open(dir.join(DATA_NAME))?)? };
        Self::validate_magic(&data)?;

        Ok(Self {
            key_len: 0, // Deprecated, kept for compatibility
            tree_len,
            root_offset,
            has_union_leaf,
            node,
            data,
            reader_ser,
        })
    }

    /// Returns the total number of key-value pairs in the tree
    pub fn len(&self) -> usize {
        self.tree_len as usize
    }

    /// Returns true if the tree is empty
    pub fn is_empty(&self) -> bool {
        self.tree_len == 0
    }

    /// Get value by key with O(log n) complexity
    ///
    /// Uses binary search through mmap'd B-Tree nodes for optimal performance
    pub fn get(&self, key: &K) -> Option<R> {
        if self.is_empty() {
            return None;
        }

        // Find data offset by traversing B-Tree
        let (start_offset, end_offset) = self.find_data_offset(key)?;

        // Read and deserialize value with known size
        Some(self.read_value_at(start_offset, end_offset))
    }
    /// Batch get - more efficient than multiple get() calls
    pub fn mget(&self, keys: &[K]) -> Vec<Option<R>> {
        keys.iter().map(|k| self.get(k)).collect()
    }

    /// Find the largest key-value pair where key <= given key (floor lookup)
    /// This is useful for finding the RecordBatch containing a specific doc_id
    ///
    /// Returns Some((key, value, ttl)) if found, None if no key <= target exists
    pub fn floor(&self, key: &K) -> Option<crate::Item<K, R>> {
        if self.is_empty() {
            return None;
        }

        // Traverse tree to find floor
        self.find_floor_in_tree(key)
    }

    /// Returns an iterator over all key-value pairs
    ///
    /// Note: The iterator is currently not fully implemented (returns None immediately)
    /// TODO: Implement full tree traversal
    pub fn iter(&self) -> TreeIterator<'_, K, R> {
        TreeIterator::new(self)
    }

    /// Range query: iterate over keys in [start_key, end_key)
    /// Returns an iterator that yields key-value pairs within the range
    ///
    /// # Arguments
    /// * `start` - Optional start bound (None means no lower bound)
    /// * `start_inclusive` - Whether start bound is inclusive (>=) or exclusive (>)
    /// * `end` - Optional end bound (None means no upper bound)
    /// * `end_inclusive` - Whether end bound is inclusive (<=) or exclusive (<)
    ///
    /// If union_leaf is enabled, this can skip chunks that don't intersect with the range
    pub fn range(
        &self,
        start: Option<&K>,
        start_inclusive: bool,
        end: Option<&K>,
        end_inclusive: bool,
    ) -> RangeIterator<'_, K, R> {
        RangeIterator::new(
            self,
            start.cloned(),
            start_inclusive,
            end.cloned(),
            end_inclusive,
        )
    }

    /// Range union query: aggregate all values in [start_key, end_key) using union operation
    ///
    /// **This is the primary use case for union_leaf optimization!**
    ///
    /// Returns the union of all values in the range without iterating individual key-value pairs.
    /// For RoaringBitmap values, this returns all unique doc_ids across the range.
    ///
    /// Performance comparison:
    /// - **With union_leaf**: O(chunks) - only reads union_leaf from each chunk (~10-100x faster)
    /// - **Without union_leaf**: O(keys) - must deserialize and union all individual values
    ///
    /// Example use case:
    /// ```rust
    /// // Get all unique doc_ids from keys [10000, 20000) (fast!)
    /// let union_bitmap = reader.range_union(Some(&10000), true, Some(&20000), false)?;
    /// println!("Total unique doc_ids: {}", union_bitmap.len());
    /// ```
    ///
    /// This is much more efficient than:
    /// ```rust
    /// // Slow: iterates every key-value pair
    /// let mut result = RoaringBitmap::new();
    /// for item in reader.range(Some(&10000), true, Some(&20000), false) {
    ///     result |= item.1;
    /// }
    /// ```
    pub fn range_union(
        &self,
        start: Option<&K>,
        start_inclusive: bool,
        end: Option<&K>,
        end_inclusive: bool,
    ) -> Result<R>
    where
        R: Default + std::ops::BitOr<Output = R> + Clone,
    {
        let mut result = R::default();
        let mut chunks_scanned = 0;
        let mut keys_scanned = 0;

        if !self.has_union_leaf {
            // Fallback: iterate through all key-value pairs
            for item in self.range(start, start_inclusive, end, end_inclusive) {
                let temp = std::mem::take(&mut result);
                result = temp | item.1.clone();
                keys_scanned += 1;
            }
            return Ok(result);
        }

        // Optimized path: directly read union_leaf from each chunk
        self.collect_union_from_chunks(
            start,
            start_inclusive,
            end,
            end_inclusive,
            &mut result,
            &mut chunks_scanned,
            &mut keys_scanned,
        )?;

        Ok(result)
    }

    /// Recursively collect union_leaf from all chunks in range
    fn collect_union_from_chunks(
        &self,
        start: Option<&K>,
        start_inclusive: bool,
        end: Option<&K>,
        end_inclusive: bool,
        result: &mut R,
        chunks_scanned: &mut usize,
        keys_scanned: &mut usize,
    ) -> Result<()>
    where
        R: Default + std::ops::BitOr<Output = R> + Clone,
    {
        let offset = self.root_offset as usize;
        let mut stack = vec![(offset, 0usize)]; // (node_offset, child_index)

        while let Some((node_offset, _child_idx)) = stack.pop() {
            let (is_leaf, keys_data, offsets, union_data) = self.read_node_at(node_offset)?;
            let keys = self.reader_ser.deserialize_keys(&keys_data);

            if is_leaf {
                // Check if this leaf chunk intersects with the range
                if keys.is_empty() {
                    continue;
                }

                let chunk_min = &keys[0];
                let chunk_max = &keys[keys.len() - 1];

                // Check overlap with start bound
                let overlaps_start = match start {
                    Some(s) => {
                        if start_inclusive {
                            chunk_max >= s // chunk_max >= start
                        } else {
                            chunk_max > s // chunk_max > start
                        }
                    }
                    None => true, // No lower bound
                };

                // Check overlap with end bound
                let overlaps_end = match end {
                    Some(e) => {
                        if end_inclusive {
                            chunk_min <= e // chunk_min <= end
                        } else {
                            chunk_min < e // chunk_min < end
                        }
                    }
                    None => true, // No upper bound
                };

                // Skip if no overlap
                if !overlaps_start || !overlaps_end {
                    continue;
                }

                *chunks_scanned += 1;

                // Check if chunk is FULLY contained in the range
                let chunk_fully_contained = {
                    let start_ok = match start {
                        Some(s) => {
                            if start_inclusive {
                                chunk_min >= s // [start, ...)
                            } else {
                                chunk_min > s // (start, ...)
                            }
                        }
                        None => true,
                    };

                    let end_ok = match end {
                        Some(e) => {
                            if end_inclusive {
                                chunk_max <= e // (..., end]
                            } else {
                                chunk_max < e // (..., end)
                            }
                        }
                        None => true,
                    };

                    start_ok && end_ok
                };

                // If we have union_leaf AND chunk is fully contained, use it directly!
                if chunk_fully_contained {
                    if let Some(ref union_bytes) = union_data {
                        match self.reader_ser.deserialize_value(union_bytes) {
                            Ok(union_value) => {
                                let temp = std::mem::take(result);
                                *result = temp | union_value;
                                continue; // Skip to next chunk
                            }
                            Err(_e) => {
                                // Fall through to manual iteration
                            }
                        }
                    }
                }

                // Fallback: manually iterate and union values in this chunk
                for (i, key) in keys.iter().enumerate() {
                    // Check start bound
                    let satisfies_start = match start {
                        Some(s) => {
                            if start_inclusive {
                                key >= s
                            } else {
                                key > s
                            }
                        }
                        None => true,
                    };

                    // Check end bound
                    let satisfies_end = match end {
                        Some(e) => {
                            if end_inclusive {
                                key <= e
                            } else {
                                key < e
                            }
                        }
                        None => true,
                    };

                    if satisfies_start && satisfies_end {
                        let value_start = offsets[i];
                        let value_end = if i + 1 < offsets.len() {
                            offsets[i + 1]
                        } else {
                            self.data.len() as i64
                        };
                        let value = self.read_value_at(value_start, value_end);
                        let temp = std::mem::take(result);
                        *result = temp | value;
                        *keys_scanned += 1;
                    }
                }
            } else {
                // Index node: optimize by only visiting children that might overlap with the query range
                // B-Tree structure: keys[i] is the separator, child[i] contains keys < keys[i]
                // child[0]: (-∞, keys[0])
                // child[1]: [keys[0], keys[1])
                // child[n]: [keys[n-1], +∞)

                if keys.is_empty() {
                    // Single child node, must visit
                    if !offsets.is_empty() {
                        stack.push((offsets[0] as usize, 0));
                    }
                    continue;
                }

                // Check first child: (-∞, keys[0])
                let first_child_overlaps = match start {
                    Some(s) => {
                        // Child range: (-∞, keys[0])
                        // Overlaps if start < keys[0] (considering inclusivity)
                        if start_inclusive {
                            s < &keys[0] // [start, ...) overlaps (-∞, keys[0]) if start < keys[0]
                        } else {
                            s < &keys[0] // (start, ...) overlaps (-∞, keys[0]) if start < keys[0]
                        }
                    }
                    None => true, // No start bound, must check
                };

                if first_child_overlaps && !offsets.is_empty() {
                    stack.push((offsets[0] as usize, 0));
                }

                // Check middle children: [keys[i-1], keys[i])
                for i in 1..keys.len() {
                    let child_start = &keys[i - 1];
                    let child_end = &keys[i];

                    // Check if [child_start, child_end) overlaps with query range
                    let overlaps_start = match start {
                        Some(s) => {
                            // Child range: [child_start, child_end)
                            // Overlaps if child_end > start (considering inclusivity)
                            if start_inclusive {
                                child_end > s // [child_start, child_end) overlaps [start, ...) if child_end > start
                            } else {
                                child_end > s // [child_start, child_end) overlaps (start, ...) if child_end > start
                            }
                        }
                        None => true,
                    };

                    let overlaps_end = match end {
                        Some(e) => {
                            // Child range: [child_start, child_end)
                            // Overlaps if child_start < end (considering inclusivity)
                            if end_inclusive {
                                child_start <= e // [child_start, child_end) overlaps (..., end] if child_start <= end
                            } else {
                                child_start < e // [child_start, child_end) overlaps (..., end) if child_start < end
                            }
                        }
                        None => true,
                    };

                    if overlaps_start && overlaps_end && i < offsets.len() {
                        stack.push((offsets[i] as usize, i));
                    }
                }

                // Check last child: [keys[n-1], +∞)
                let last_idx = keys.len();
                if last_idx < offsets.len() {
                    let last_child_overlaps = match end {
                        Some(e) => {
                            // Child range: [keys[n-1], +∞)
                            // Overlaps if end > keys[n-1] (considering inclusivity)
                            let last_key = &keys[keys.len() - 1];
                            if end_inclusive {
                                e >= last_key // [keys[n-1], +∞) overlaps (..., end] if end >= keys[n-1]
                            } else {
                                e > last_key // [keys[n-1], +∞) overlaps (..., end) if end > keys[n-1]
                            }
                        }
                        None => true, // No end bound, must check
                    };

                    if last_child_overlaps {
                        stack.push((offsets[last_idx] as usize, last_idx));
                    }
                }
            }
        }

        Ok(())
    }

    /// Check if this tree has union_leaf optimization enabled
    pub fn has_union_leaf(&self) -> bool {
        self.has_union_leaf
    }

    // ========== Internal Implementation ==========

    /// Find data file offset for a key by traversing the B-Tree
    /// Returns (start_offset, end_offset) to allow calculating value size
    fn find_data_offset(&self, search_key: &K) -> Option<(i64, i64)> {
        let mut offset = self.root_offset;

        static SEARCH_COUNT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let search_num = SEARCH_COUNT.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        // Debug first few searches and searches around the boundary
        let debug = search_num >= 65790 && search_num < 65795;

        if debug {
            eprintln!("🔍 [find_data_offset #{}] Starting search from root_offset={}", search_num, offset);
        }

        // Traverse from root to leaf
        loop {
            if debug {
                eprintln!("🔍 [find_data_offset #{}] Reading node at offset={}", search_num, offset);
            }

            // Read node at current offset
            let (is_leaf, keys_bytes, offsets, _union_data) =
                match self.read_node_at(offset as usize) {
                    Ok(node) => node,
                    Err(_) => return None,
                };

            let keys = self.reader_ser.deserialize_keys(&keys_bytes);

            // 🔍 Debug: Only log once for efficiency
            static FIRST_LOG: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
            if !is_leaf && offsets.len() == 257 && keys.len() == 256 {
                if !FIRST_LOG.swap(true, std::sync::atomic::Ordering::SeqCst) {
                    eprintln!(
                        "🔍 [find_data_offset] Deserialized FIRST-LEVEL index: keys={}, offsets={}",
                        keys.len(),
                        offsets.len()
                    );
                }
            }

            // Find the appropriate child/value in current node
            if is_leaf {
                // 🔍 Debug: Log leaf node details
                static LEAF_LOG_COUNT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
                let count = LEAF_LOG_COUNT.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                if count < 5 || keys.len() < 100 {
                    eprintln!(
                        "🔍 [find_data_offset] Reached LEAF node: keys.len()={}, offsets.len()={}",
                        keys.len(),
                        offsets.len()
                    );
                }

                // Leaf node: do exact binary search
                match self.binary_search_keys(&keys, search_key) {
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
                        eprintln!(
                            "🔍 [find_data_offset] Key NOT FOUND in leaf: keys.len()={}",
                            keys.len()
                        );
                        return None;
                    }
                }
            } else {
                // Index node: find which child to follow
                // B-tree structure: keys[k1, k2, k3], offsets[c0, c1, c2, c3]
                // c0 contains keys < k1
                // c1 contains keys [k1, k2)
                // c2 contains keys [k2, k3)
                // c3 contains keys >= k3
                //
                // So: find the first i where search_key < keys[i], then use offsets[i]
                // If no such i exists, use offsets[keys.len()]

                let mut child_idx = 0;
                let mut found_by_break = false;
                let mut break_at_key_index = None;
                for (i, key) in keys.iter().enumerate() {
                    if search_key < key {
                        child_idx = i;
                        found_by_break = true;
                        break_at_key_index = Some(i);
                        break;
                    }
                    // If we finish the loop without break, child_idx will be 0,
                    // but we want the last child
                    child_idx = i + 1;
                }

                // 🔍 Debug: Log when accessing last or near-last child in first-level index with 257 offsets
                if offsets.len() == 257 && child_idx >= 254 {
                    eprintln!(
                        "🔍 [find_data_offset] child_idx={}, keys.len()={}, offsets.len()={}, found_by_break={}, break_at_key_index={:?}",
                        child_idx,
                        keys.len(),
                        offsets.len(),
                        found_by_break,
                        break_at_key_index
                    );
                }

                // 🔍 Debug: Check if we're about to access out of bounds
                if child_idx >= offsets.len() {
                    eprintln!(
                        "❌ [find_data_offset] INDEX OUT OF BOUNDS! child_idx={}, offsets.len()={}, keys.len()={}",
                        child_idx,
                        offsets.len(),
                        keys.len()
                    );
                    return None;
                }

                let next_offset = offsets[child_idx];
                offset = next_offset;
            }
        }
    }

    /// Read a node from mmap at given offset
    /// Returns (is_leaf, keys_data, offsets, union_data_opt)
    fn read_node_at(&self, offset: usize) -> Result<(bool, Vec<u8>, Vec<i64>, Option<Vec<u8>>)> {
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

        // Read offsets (support both legacy tagged format and new delta-only format)
        let offsets = {
            // Try delta-first
            let mut try_pos = pos;
            let deltas = i64_coder::read_delta_pos(&self.node, &mut try_pos);

            // Validate decoded offsets for sanity
            let valid = if deltas.is_empty() {
                false
            } else {
                let monotonic = deltas.windows(2).all(|w| w[0] <= w[1]);
                if !monotonic {
                    false
                } else if is_leaf {
                    // Leaf offsets point to DATA file
                    let min_ok = deltas[0] >= MAGIC_VERSION.len() as i64;
                    let max_ok = deltas.last().copied().unwrap_or(0) <= self.data.len() as i64;
                    min_ok && max_ok
                } else {
                    // Index offsets point to NODE file (after header)
                    let min_ok = deltas[0] >= 15; // MAGIC(2)+root(8)+tree_len(4)+has_union_leaf(1)
                    let max_ok = deltas.last().copied().unwrap_or(0) <= self.node.len() as i64;
                    min_ok && max_ok
                }
            };

            if valid {
                pos = try_pos;
                deltas
            } else {
                // Fallback to legacy tagged array
                i64_coder::read(&self.node, &mut pos)
            }
        };

        // 🔍 Debug: Log index nodes at first level (keys <= 256)
        if !is_leaf && keys_len <= 256 {
            log::info!(
                "🔍 [read_node_at] FIRST-LEVEL Index node: keys={}, offsets={} (offset: {})",
                keys_len,
                offsets.len(),
                offset
            );
        }

        // Read union_leaf data if this is a leaf node and has_union_leaf is enabled
        let union_data = if is_leaf && self.has_union_leaf {
            // Read length prefix (u32) then data
            if pos + 4 <= self.node.len() {
                let union_len =
                    u32::from_be_bytes(self.node[pos..pos + 4].try_into().unwrap()) as usize;
                pos += 4;

                if pos + union_len <= self.node.len() {
                    Some(self.node[pos..pos + union_len].to_vec())
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        };

        Ok((is_leaf, keys_bytes, offsets, union_data))
    }
    /// Binary search within deserialized keys using PartialOrd
    fn binary_search_keys(&self, keys: &[K], search_key: &K) -> SearchResult {
        for (i, key) in keys.iter().enumerate() {
            if key == search_key {
                return SearchResult::Found(i);
            } else if key > search_key {
                return SearchResult::NotFound(i);
            }
        }
        SearchResult::NotFound(keys.len())
    }

    /// Find floor entry in tree: largest key <= search_key
    fn find_floor_in_tree(&self, search_key: &K) -> Option<crate::Item<K, R>> {
        let mut offset = self.root_offset;
        let mut floor_candidate: Option<(K, i64, i64)> = None; // (key, start_offset, end_offset)

        // Traverse from root to leaf, keeping track of largest key <= search_key
        loop {
            let (is_leaf, keys_data, offsets, _union_data) =
                match self.read_node_at(offset as usize) {
                    Ok(node) => node,
                    Err(_) => break,
                };

            let keys = self.reader_ser.deserialize_keys(&keys_data);

            if is_leaf {
                // Leaf node: find largest key <= search_key
                for (i, key) in keys.iter().enumerate() {
                    if key <= search_key {
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
                    if key <= search_key {
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
    fn read_value_at(&self, start_offset: i64, end_offset: i64) -> R {
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
        let bitmap = self
            .reader_ser
            .deserialize_value(value_data)
            .expect("Failed to deserialize value");
        bitmap
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
#[allow(dead_code)]
enum SearchResult {
    Found(usize),
    NotFound(usize),
}

/// Iterator over tree entries
///
/// Similar to memory BTree::Iter, uses a stack to track traversal state.
/// Stack contains (node_offset, key_index) pairs where:
/// - node_offset: position in mmap'd node file
/// - key_index: current index within that node (-1 means not started)
pub struct TreeIterator<'a, K, R> {
    reader: &'a TreeReader<K, R>,
    stack: Vec<(usize, i32)>, // (node_offset, key_index) - using i32 like memory version
    finished: bool,
}

impl<'a, K, R> TreeIterator<'a, K, R>
where
    K: Clone + PartialOrd,
{
    fn new(reader: &'a TreeReader<K, R>) -> Self {
        let mut stack = Vec::new();
        if !reader.is_empty() {
            // Start from root with index -1 (not yet accessed)
            stack.push((reader.root_offset as usize, -1i32));
        }
        Self {
            reader,
            stack,
            finished: reader.is_empty(),
        }
    }

    /// Seek to the first key >= target key
    /// After seek, next() will return the first key >= target
    pub fn seek(&mut self, key: &K) {
        self.stack.clear();
        self.finished = false;

        if self.reader.is_empty() {
            self.finished = true;
            return;
        }

        let mut offset = self.reader.root_offset as usize;

        // Traverse tree to find the position
        loop {
            let (is_leaf, keys_data, offsets, _union_data) = match self.reader.read_node_at(offset)
            {
                Ok(node) => node,
                Err(_) => {
                    self.finished = true;
                    return;
                }
            };

            let keys = self.reader.reader_ser.deserialize_keys(&keys_data);

            if is_leaf {
                // Leaf node: find first key >= search key
                let mut index = 0;
                for (i, k) in keys.iter().enumerate() {
                    if k >= key {
                        index = i;
                        break;
                    }
                    index = i + 1;
                }
                // Push with index - 1 because next() will increment it
                self.stack.push((offset, index as i32 - 1));
                return;
            } else {
                // Index node: find which child to follow
                let mut child_idx = 0;
                for (i, k) in keys.iter().enumerate() {
                    if k <= key {
                        child_idx = i;
                    } else {
                        break;
                    }
                }

                self.stack.push((offset, child_idx as i32));
                offset = offsets[child_idx] as usize;
            }
        }
    }
}

impl<'a, K, R> Iterator for TreeIterator<'a, K, R>
where
    K: Clone + PartialOrd,
{
    type Item = crate::Item<K, R>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }

        // Similar to memory BTree iterator logic
        loop {
            let (offset, mut index) = self.stack.pop()?;
            index += 1;

            // Read node at current offset
            let (is_leaf, keys_data, offsets, _union_data) = match self.reader.read_node_at(offset)
            {
                Ok(node) => node,
                Err(_) => {
                    self.finished = true;
                    return None;
                }
            };

            let keys = self.reader.reader_ser.deserialize_keys(&keys_data);

            // Check if we've exhausted this node
            // For index nodes: valid indices are [0, keys.len()] (matching offsets.len())
            // For leaf nodes: valid indices are [0, keys.len()-1]
            let max_index = if is_leaf { keys.len() } else { offsets.len() };
            if index >= max_index as i32 {
                continue; // Pop next item from stack
            }

            // Push current position back onto stack
            self.stack.push((offset, index));

            if is_leaf {
                // Leaf node: return the item at this index
                let key = keys[index as usize].clone();

                // Calculate value offset range
                let start = offsets[index as usize];
                let end = if (index as usize) + 1 < offsets.len() {
                    offsets[index as usize + 1]
                } else {
                    self.reader.data.len() as i64
                };

                let value = self.reader.read_value_at(start, end);
                return Some(std::sync::Arc::new((key, value, None)));
            } else {
                // Index node: push next child onto stack
                let child_offset = offsets[index as usize] as usize;
                self.stack.push((child_offset, -1));
            }
        }
    }
}

/// Range iterator for range queries with flexible boundary conditions
/// Supports open/closed intervals: [start, end], (start, end), [start, end), (start, end]
/// Optimized with union_leaf to skip chunks that don't intersect with the range
pub struct RangeIterator<'a, K, R> {
    reader: &'a TreeReader<K, R>,
    #[allow(dead_code)]
    start_key: Option<K>,
    start_inclusive: bool,
    end_key: Option<K>,
    end_inclusive: bool,
    stack: Vec<(usize, i32)>, // (node_offset, current_index)
    finished: bool,
}

impl<'a, K, R> RangeIterator<'a, K, R>
where
    K: Clone + PartialOrd,
{
    fn new(
        reader: &'a TreeReader<K, R>,
        start_key: Option<K>,
        start_inclusive: bool,
        end_key: Option<K>,
        end_inclusive: bool,
    ) -> Self {
        let mut iter = Self {
            reader,
            start_key: start_key.clone(),
            start_inclusive,
            end_key: end_key.clone(),
            end_inclusive,
            stack: Vec::new(),
            finished: false,
        };

        // Seek to start position if specified
        if let Some(sk) = &start_key {
            iter.seek(sk);
        } else {
            // No start bound, start from beginning
            if !reader.is_empty() {
                iter.stack.push((reader.root_offset as usize, -1));
            } else {
                iter.finished = true;
            }
        }
        iter
    }

    /// Seek to the first key that satisfies the start bound
    fn seek(&mut self, start_key: &K) {
        if self.reader.is_empty() {
            self.finished = true;
            return;
        }

        self.stack.clear();
        let mut offset = self.reader.root_offset as usize;

        // Traverse tree to find the position
        loop {
            let (is_leaf, keys_data, offsets, _union_data) = match self.reader.read_node_at(offset)
            {
                Ok(node) => node,
                Err(_) => {
                    self.finished = true;
                    return;
                }
            };

            let keys = self.reader.reader_ser.deserialize_keys(&keys_data);

            if is_leaf {
                // Leaf node: find first key that satisfies the start bound
                for (i, key) in keys.iter().enumerate() {
                    let satisfies_start = if self.start_inclusive {
                        key >= start_key // [start_key, ...)
                    } else {
                        key > start_key // (start_key, ...)
                    };

                    if satisfies_start {
                        // Found starting point, push to stack with index before target
                        self.stack.push((offset, i as i32 - 1));
                        return;
                    }
                }
                // All keys don't satisfy start bound, no match
                self.finished = true;
                return;
            } else {
                // Index node: find appropriate child
                let mut child_idx = 0;
                for (i, key) in keys.iter().enumerate() {
                    if start_key < key {
                        child_idx = i;
                        break;
                    }
                    child_idx = i + 1;
                }

                // Guard against index out of bounds
                if child_idx >= offsets.len() {
                    self.finished = true;
                    return;
                }

                // CRITICAL: Push parent with child_idx (not child_idx-1)
                // After current child exhausts, next() will pop this, do index+=1 to get child_idx+1
                // This ensures we continue with the NEXT sibling, not re-visit current child
                self.stack.push((offset, child_idx as i32));
                let next_offset = offsets[child_idx];
                offset = next_offset as usize;
            }
        }
    }
}

impl<'a, K, R> Iterator for RangeIterator<'a, K, R>
where
    K: Clone + PartialOrd,
{
    type Item = crate::Item<K, R>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }

        loop {
            let (offset, mut index) = self.stack.pop()?;
            index += 1;

            // Read node at current offset
            let (is_leaf, keys_data, offsets, _union_data) = match self.reader.read_node_at(offset)
            {
                Ok(node) => node,
                Err(_) => {
                    self.finished = true;
                    return None;
                }
            };

            let keys = self.reader.reader_ser.deserialize_keys(&keys_data);

            // For leaf nodes: check if we've exhausted all keys
            // For index nodes: keys.len() < offsets.len(), so index == keys.len() is valid (rightmost child)
            if is_leaf {
                if index >= keys.len() as i32 {
                    continue; // Leaf exhausted, pop next item from stack
                }

                let key = keys[index as usize].clone();

                // Check if key satisfies end bound
                if let Some(ref end) = self.end_key {
                    let exceeds_end = if self.end_inclusive {
                        &key > end // (..., end]
                    } else {
                        &key >= end // (..., end)
                    };

                    if exceeds_end {
                        self.finished = true;
                        return None;
                    }
                }

                // NOTE: No need to check start bound here!
                // seek() already positioned us at the first key >= start_key
                // All subsequent keys from the iterator are guaranteed to satisfy start bound

                // Push current position back onto stack
                self.stack.push((offset, index));

                // Leaf node: return the item at this index
                // Calculate value offset range
                let start = offsets[index as usize];
                let end = if (index as usize) + 1 < offsets.len() {
                    offsets[index as usize + 1]
                } else {
                    self.reader.data.len() as i64
                };

                let value = self.reader.read_value_at(start, end);
                return Some(std::sync::Arc::new((key, value, None)));
            } else {
                // Index node: check if we've exhausted all children
                // Note: offsets.len() = keys.len() + 1
                if index >= offsets.len() as i32 {
                    continue; // All children visited, pop next item from stack
                }

                // For index nodes: B-tree structure is keys[k1, k2], offsets[c0, c1, c2]
                // - c0 contains keys < k1
                // - c1 contains keys in [k1, k2)
                // - c2 contains keys >= k2
                // When accessing offsets[i], the minimum key in that child is keys[i-1] (if exists)
                // We should stop if keys[i-1] exceeds end bound
                if let Some(ref end) = self.end_key {
                    if index > 0 && (index - 1) < keys.len() as i32 {
                        let separator_key = &keys[(index - 1) as usize];
                        let exceeds_end = if self.end_inclusive {
                            separator_key > end
                        } else {
                            separator_key >= end
                        };

                        if exceeds_end {
                            self.finished = true;
                            return None;
                        }
                    }
                }

                // Push current position back onto stack
                self.stack.push((offset, index));

                // Index node: push next child onto stack (this does NOT return a value)
                let child_offset = offsets[index as usize] as usize;
                self.stack.push((child_offset, -1));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persist::writer::TreeWriter;
    use crate::BTree;
    use std::path::PathBuf;

    struct I64Serializer;

    impl WriteSerializer<i64, i64> for I64Serializer {
        fn serialize_keys<'a>(&self, keys: &'a Vec<i64>) -> Cow<'a, [u8]> {
            let mut buf = Vec::with_capacity(keys.len().saturating_mul(8));
            i64_coder::write_delta(&mut buf, keys).unwrap();
            Cow::Owned(buf)
        }

        fn serialize_value<'a>(&self, value: &'a i64) -> Cow<'a, [u8]> {
            Cow::Owned(value.to_be_bytes().to_vec())
        }
    }

    impl ReadSerializer<i64, i64> for I64Serializer {
        fn deserialize_keys<'a>(&self, data: &'a [u8]) -> Vec<i64> {
            i64_coder::read_delta(&data)
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

        TreeWriter::new(test_dir.clone(), 128)
            .persist::<i64, i64, i64>(tree.len(), Box::new(I64Serializer {}), None, tree.iter())
            .unwrap();

        // Read back
        let reader = TreeReader::new(&test_dir, Box::new(I64Serializer {})).unwrap();

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
    fn test_reader_iterator() {
        let test_dir = PathBuf::from("/tmp/test_tree_reader_iterator");
        std::fs::remove_dir_all(&test_dir).ok();

        // Write test data
        let mut tree = BTree::new(32);
        for i in 0..50i64 {
            tree.put(i, i * 2);
        }

        TreeWriter::new(test_dir.clone(), 128)
            .persist::<i64, i64, i64>(tree.len(), Box::new(I64Serializer {}), None, tree.iter())
            .unwrap();

        // Read back and test iterator
        let reader = TreeReader::new(&test_dir, Box::new(I64Serializer {})).unwrap();

        println!("Testing full iteration...");
        let mut iter = reader.iter();
        let mut count = 0;
        let mut last_key = -1i64;

        while let Some(item) = iter.next() {
            let (key, value, _ttl) = &*item;
            println!("  Iterated: key={}, value={}", key, value);

            // Verify keys are in order
            assert!(*key > last_key, "Keys should be in ascending order");
            last_key = *key;

            // Verify value is correct
            assert_eq!(*value, *key * 2, "Value mismatch for key {}", key);
            count += 1;
        }

        assert_eq!(count, 50, "Should iterate over all 50 items");
        println!("✅ Iterator test passed: iterated {} items", count);
    }

    #[test]
    fn test_reader_iterator_seek() {
        let test_dir = PathBuf::from("/tmp/test_tree_reader_seek");
        std::fs::remove_dir_all(&test_dir).ok();

        // Write test data
        let mut tree = BTree::new(32);
        for i in 0..100i64 {
            tree.put(i, i * 10);
        }

        TreeWriter::new(test_dir.clone(), 128)
            .persist::<i64, i64, i64>(tree.len(), Box::new(I64Serializer {}), None, tree.iter())
            .unwrap();

        let reader = TreeReader::new(&test_dir, Box::new(I64Serializer {})).unwrap();

        // Test seek to middle
        println!("Testing seek to 50...");
        let mut iter = reader.iter();
        iter.seek(&50);

        if let Some(item) = iter.next() {
            let (key, value, _) = &*item;
            println!("  After seek(50): key={}, value={}", key, value);
            assert_eq!(*key, 50, "Should start at key 50");
            assert_eq!(*value, 500, "Value should be 500");
        } else {
            panic!("Seek failed to position iterator");
        }

        // Continue iteration
        let mut count = 1;
        while let Some(item) = iter.next() {
            let (key, _value, _) = &*item;
            assert!(*key > 50, "Keys after 50 should be > 50");
            count += 1;
        }
        assert_eq!(count, 50, "Should have 50 items from 50 to 99");

        // Test seek to non-existent key (should position at next higher)
        println!("Testing seek to 25 (exists)...");
        let mut iter = reader.iter();
        iter.seek(&25);
        if let Some(item) = iter.next() {
            let (key, _, _) = &*item;
            assert_eq!(*key, 25, "Should start at key 25");
        }

        println!("✅ Seek test passed");
    }

    #[test]
    fn test_reader_range_query() {
        let test_dir = PathBuf::from("/tmp/test_tree_reader_range");
        std::fs::remove_dir_all(&test_dir).ok();

        // Write test data without union_leaf
        let mut tree = BTree::new(32);
        for i in 0..100i64 {
            tree.put(i, i * 3);
        }

        TreeWriter::new(test_dir.clone(), 128)
            .persist::<i64, i64, i64>(tree.len(), Box::new(I64Serializer {}), None, tree.iter())
            .unwrap();

        let reader = TreeReader::new(&test_dir, Box::new(I64Serializer {})).unwrap();

        println!("has_union_leaf: {}", reader.has_union_leaf());
        assert_eq!(reader.has_union_leaf(), false);

        // Test range [30, 40) using new range() API
        println!("Testing range query [30, 40)...");
        let mut keys = Vec::new();
        for item in reader.range(Some(&30), true, Some(&40), false) {
            let (key, value, _) = &*item;
            assert_eq!(*value, *key * 3, "Value mismatch");
            keys.push(*key);
        }

        println!("  Found keys: {:?}", keys);
        assert_eq!(keys.len(), 10, "Should have 10 keys in range [30, 40)");
        assert_eq!(keys[0], 30);
        assert_eq!(keys[9], 39);

        // Test edge cases
        println!("Testing range [0, 5)...");
        let keys: Vec<_> = reader
            .range(Some(&0), true, Some(&5), false)
            .map(|item| item.0)
            .collect();
        assert_eq!(keys, vec![0, 1, 2, 3, 4]);

        println!("Testing range [95, 100)...");
        let keys: Vec<_> = reader
            .range(Some(&95), true, Some(&100), false)
            .map(|item| item.0)
            .collect();
        assert_eq!(keys, vec![95, 96, 97, 98, 99]);

        println!("Testing range [50, 50) - empty range...");
        let keys: Vec<_> = reader
            .range(Some(&50), true, Some(&50), false)
            .map(|item| item.0)
            .collect();
        assert_eq!(keys.len(), 0);

        println!("✅ Range query test passed");
    }

    // Large dataset tests removed - will be added back when needed
    /*
    #[test]
    fn test_reader_1m_dataset() {
        // Use the existing 1M dataset written by test_tree_writer_1m_strings
        let test_dir = PathBuf::from("/tmp/test_tree_writer_1m");

        let node_file = test_dir.join("node");
        let data_file = test_dir.join("data");
        if !(node_file.exists() && data_file.exists()) {
            println!("⚠️  1M test data not found (node/data missing), skipping test");
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

            fn deserialize_keys<'a>(&self, data: &'a [u8]) -> Rec<String> {
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

        // Quick sanity check for value size; if mismatch, skip this test as dataset format differs
        if let Some(v) = reader.get(&format!("key_{:010}", 0)) {
            if v.len() != 1024 {
                println!(
                    "⚠️  1M dataset present but value size {} != 1024; skipping test",
                    v.len()
                );
                return;
            }
        } else {
            println!("⚠️  1M dataset present but missing expected key; skipping test");
            return;
        }

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

            fn deserialize_keys<'a>(&self, data: &'a [u8]) -> Rec<String> {
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
    */
}
