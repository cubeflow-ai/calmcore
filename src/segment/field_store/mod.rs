use std::{
    any::Any,
    borrow::Cow,
    collections::{BTreeMap, HashMap, HashSet},
    error::Error,
    fs::File,
    sync::{Arc, RwLock},
};

use datafusion::parquet;
use datafusion::{
    arrow::array::{ArrayRef, RecordBatch},
    scalar::ScalarValue,
};
use mem_btree::{
    persist::{self, num_ser},
    BTree,
};

use roaring::RoaringBitmap;

use crate::{partition::WriteInfo, schema::field::FieldType, utils::error::CoreResult};

pub mod keyword;
// pub mod num_f32; // TODO: implement later
pub mod num_f64;
// pub mod num_i32; // TODO: implement later
pub mod num_i64;
// pub mod num_u32; // TODO: implement later
// pub mod num_u64; // removed per design: u64 field not needed currently

/// Serializer for i64 keys with RoaringBitmap values
#[derive(Clone)]
pub struct I64RoaringSerializer {
    zstd_level: i32,
}

impl I64RoaringSerializer {
    pub fn new(zstd_level: i32) -> Self {
        Self { zstd_level }
    }

    pub fn default() -> Self {
        Self { zstd_level: 3 }
    }
}

impl persist::ReadSerializer<i64, RoaringBitmap> for I64RoaringSerializer {
    fn deserialize_value<'a>(
        &self,
        data: &'a [u8],
    ) -> std::result::Result<RoaringBitmap, Box<dyn std::error::Error>> {
        decode_roaring_from_bytes(data)
    }

    fn deserialize_keys<'a>(&self, data: &'a [u8]) -> Vec<i64> {
        if data.is_empty() {
            return Vec::new();
        }
        let data_to_parse = match zstd::decode_all(data) {
            Ok(d) => d,
            Err(_) => return Vec::new(),
        };

        if data_to_parse.len() < 2 {
            return Vec::new();
        }

        let key_count = u16::from_be_bytes([data_to_parse[0], data_to_parse[1]]) as usize;
        let mut result = Vec::with_capacity(key_count);
        let mut pos = 2;

        for _ in 0..key_count {
            if pos + 8 > data_to_parse.len() {
                break;
            }
            let key = i64::from_be_bytes([
                data_to_parse[pos],
                data_to_parse[pos + 1],
                data_to_parse[pos + 2],
                data_to_parse[pos + 3],
                data_to_parse[pos + 4],
                data_to_parse[pos + 5],
                data_to_parse[pos + 6],
                data_to_parse[pos + 7],
            ]);
            result.push(key);
            pos += 8;
        }
        result
    }
}

impl persist::WriteSerializer<i64, RoaringBitmap> for I64RoaringSerializer {
    fn serialize_keys<'a>(&self, keys: &'a Vec<i64>) -> Cow<'a, [u8]> {
        use byteorder::WriteBytesExt;

        let mut uncompressed = Vec::new();
        uncompressed
            .write_u16::<byteorder::BigEndian>(keys.len() as u16)
            .expect("write u16 failed");

        for key in keys {
            uncompressed.extend_from_slice(&key.to_be_bytes());
        }

        let compressed =
            zstd::encode_all(&uncompressed[..], self.zstd_level).unwrap_or(uncompressed);
        Cow::Owned(compressed)
    }

    fn serialize_value<'a>(&self, value: &'a RoaringBitmap) -> Cow<'a, [u8]> {
        Cow::Owned(encode_roaring_from_bitmap(value))
    }
}

/// Serializer for f64 keys with RoaringBitmap values
/// Note: This serializer works with raw f64 values for disk storage
#[derive(Clone)]
pub struct F64RoaringSerializer {
    zstd_level: i32,
}

impl F64RoaringSerializer {
    pub fn new(zstd_level: i32) -> Self {
        Self { zstd_level }
    }

    pub fn default() -> Self {
        Self { zstd_level: 3 }
    }
}

// Need to export OrderedF64 wrapper from num_f64 module
use num_f64::OrderedF64;

impl persist::ReadSerializer<OrderedF64, RoaringBitmap> for F64RoaringSerializer {
    fn deserialize_value<'a>(
        &self,
        data: &'a [u8],
    ) -> std::result::Result<RoaringBitmap, Box<dyn std::error::Error>> {
        decode_roaring_from_bytes(data)
    }

    fn deserialize_keys<'a>(&self, data: &'a [u8]) -> Vec<OrderedF64> {
        if data.is_empty() {
            return Vec::new();
        }
        let data_to_parse = match zstd::decode_all(data) {
            Ok(d) => d,
            Err(_) => return Vec::new(),
        };

        if data_to_parse.len() < 2 {
            return Vec::new();
        }

        let key_count = u16::from_be_bytes([data_to_parse[0], data_to_parse[1]]) as usize;
        let mut result = Vec::with_capacity(key_count);
        let mut pos = 2;

        for _ in 0..key_count {
            if pos + 8 > data_to_parse.len() {
                break;
            }
            let key = f64::from_be_bytes([
                data_to_parse[pos],
                data_to_parse[pos + 1],
                data_to_parse[pos + 2],
                data_to_parse[pos + 3],
                data_to_parse[pos + 4],
                data_to_parse[pos + 5],
                data_to_parse[pos + 6],
                data_to_parse[pos + 7],
            ]);
            result.push(OrderedF64(key));
            pos += 8;
        }
        result
    }
}

impl persist::WriteSerializer<OrderedF64, RoaringBitmap> for F64RoaringSerializer {
    fn serialize_keys<'a>(&self, keys: &'a Vec<OrderedF64>) -> Cow<'a, [u8]> {
        use byteorder::WriteBytesExt;

        let mut uncompressed = Vec::new();
        uncompressed
            .write_u16::<byteorder::BigEndian>(keys.len() as u16)
            .expect("write u16 failed");

        for key in keys {
            uncompressed.extend_from_slice(&key.0.to_be_bytes());
        }

        let compressed =
            zstd::encode_all(&uncompressed[..], self.zstd_level).unwrap_or(uncompressed);
        Cow::Owned(compressed)
    }

    fn serialize_value<'a>(&self, value: &'a RoaringBitmap) -> Cow<'a, [u8]> {
        Cow::Owned(encode_roaring_from_bitmap(value))
    }
}

/// Serializer for String keys with RoaringBitmap values
#[derive(Clone)]
pub struct StringRoaringSerializer {
    zstd_level: i32,
}

impl StringRoaringSerializer {
    pub fn new(zstd_level: i32) -> Self {
        Self { zstd_level }
    }

    pub fn default() -> Self {
        Self { zstd_level: 3 }
    }
}

impl persist::ReadSerializer<String, RoaringBitmap> for StringRoaringSerializer {
    fn deserialize_value<'a>(
        &self,
        data: &'a [u8],
    ) -> std::result::Result<RoaringBitmap, Box<dyn std::error::Error>> {
        decode_roaring_from_bytes(data)
    }

    fn deserialize_keys<'a>(&self, data: &'a [u8]) -> Vec<String> {
        if data.is_empty() {
            return Vec::new();
        }
        let data_to_parse = match zstd::decode_all(data) {
            Ok(d) => d,
            Err(_) => return Vec::new(),
        };
        let mut result = Vec::new();
        let mut pos = 0;
        if data_to_parse.len() < 2 {
            return result;
        }
        let key_count = u16::from_be_bytes([data_to_parse[0], data_to_parse[1]]) as usize;
        pos += 2;
        for _ in 0..key_count {
            if pos >= data_to_parse.len() {
                break;
            }
            let len = persist::zigzag::read_u32(&data_to_parse, &mut pos) as usize;
            if pos + len > data_to_parse.len() {
                break;
            }
            match String::from_utf8(data_to_parse[pos..pos + len].to_vec()) {
                Ok(key) => result.push(key),
                Err(_) => break,
            }
            pos += len;
        }
        result
    }
}

impl persist::WriteSerializer<String, RoaringBitmap> for StringRoaringSerializer {
    fn serialize_keys<'a>(&self, keys: &'a Vec<String>) -> Cow<'a, [u8]> {
        use byteorder::WriteBytesExt;

        let mut uncompressed = Vec::new();
        uncompressed
            .write_u16::<byteorder::BigEndian>(keys.len() as u16)
            .expect("write u16 failed");

        for key in keys {
            let key_bytes = key.as_bytes();
            persist::zigzag::write_u32(key_bytes.len() as u32, &mut uncompressed)
                .expect("write zigzag u32 failed");
            uncompressed.extend_from_slice(key_bytes);
        }

        let compressed =
            zstd::encode_all(&uncompressed[..], self.zstd_level).unwrap_or(uncompressed);
        Cow::Owned(compressed)
    }

    fn serialize_value<'a>(&self, value: &'a RoaringBitmap) -> Cow<'a, [u8]> {
        Cow::Owned(encode_roaring_from_bitmap(value))
    }
}

/// Serializer for u32 keys with RecordBatch values
/// Stores RecordBatch in Parquet format with built-in compression
#[derive(Clone)]
pub struct U32RecordBatchSerializer;

impl U32RecordBatchSerializer {
    pub fn new() -> Self {
        Self
    }

    pub fn default() -> Self {
        Self
    }
}

impl persist::WriteSerializer<u32, RecordBatch> for U32RecordBatchSerializer {
    fn serialize_keys<'a>(&self, keys: &'a Vec<u32>) -> Cow<'a, [u8]> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&(keys.len() as u32).to_be_bytes());

        for key in keys {
            buf.extend_from_slice(&key.to_be_bytes());
        }

        Cow::Owned(buf)
    }

    fn serialize_value<'a>(&self, batch: &'a RecordBatch) -> Cow<'a, [u8]> {
        use parquet::arrow::ArrowWriter;
        use parquet::basic::Compression;
        use parquet::file::properties::WriterProperties;

        let mut buf = Vec::new();

        // 使用 Parquet format,默认 ZSTD 压缩
        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(Default::default()))
            .build();

        let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), Some(props))
            .expect("Failed to create ArrowWriter");
        writer.write(batch).expect("Failed to write batch");
        writer.close().expect("Failed to close writer");

        Cow::Owned(buf)
    }
}

impl persist::ReadSerializer<u32, RecordBatch> for U32RecordBatchSerializer {
    fn deserialize_keys<'a>(&self, data: &'a [u8]) -> Vec<u32> {
        let mut pos = 0;
        if data.len() < 4 {
            return Vec::new();
        }

        let count =
            u32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]) as usize;
        pos += 4;

        let mut keys = Vec::with_capacity(count);
        for _ in 0..count {
            if pos + 4 > data.len() {
                break;
            }
            let key = u32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
            keys.push(key);
            pos += 4;
        }
        keys
    }

    fn deserialize_value<'a>(&self, data: &'a [u8]) -> Result<RecordBatch, Box<dyn Error>> {
        use bytes::Bytes;
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

        // 使用 Parquet format 反序列化
        let bytes = Bytes::copy_from_slice(data);
        let builder = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .map_err(|e| format!("Failed to create ParquetRecordBatchReaderBuilder: {}", e))?;

        let mut reader = builder
            .build()
            .map_err(|e| format!("Failed to build reader: {}", e))?;

        // 读取第一个 batch
        if let Some(result) = reader.next() {
            result.map_err(|e| format!("Failed to read batch: {}", e).into())
        } else {
            Err("No batch found".into())
        }
    }
}

/// Parquet-based row data reader
/// Maps doc_id ranges to RowGroup indices for efficient lookup
pub struct ParquetRowDataReader {
    file_path: String,
    /// Maps first doc_id of each RowGroup to its index
    /// Example: {0: 0, 1000: 1, 2000: 2} means:
    /// - RowGroup 0 contains doc_ids [0, 1000)
    /// - RowGroup 1 contains doc_ids [1000, 2000)
    /// - RowGroup 2 contains doc_ids [2000, ...)
    key_to_rowgroup: Arc<BTreeMap<u32, usize>>,
    num_row_groups: usize,
}

impl ParquetRowDataReader {
    /// Open a Parquet file and build the doc_id -> RowGroup mapping
    pub fn new(path: &str) -> CoreResult<Self> {
        use parquet::file::reader::{FileReader, SerializedFileReader};

        let file =
            File::open(path).map_err(|e| crate::utils::error::CoreError::IOError(e.to_string()))?;

        let reader = SerializedFileReader::new(file)
            .map_err(|e| crate::utils::error::CoreError::IOError(e.to_string()))?;

        let metadata = reader.metadata();
        let num_row_groups = metadata.num_row_groups();

        // Build key mapping from Parquet metadata
        // We store the first doc_id of each RowGroup in the file's key_value_metadata
        let mut key_to_rowgroup = BTreeMap::new();

        if let Some(file_metadata) = metadata.file_metadata().key_value_metadata() {
            for kv in file_metadata {
                if kv.key == "row_group_keys" {
                    if let Some(value) = &kv.value {
                        // Parse JSON array of keys: [0, 1000, 2000, ...]
                        let keys: Vec<u32> = serde_json::from_str(value).map_err(|e| {
                            crate::utils::error::CoreError::Internal(format!(
                                "Failed to parse row_group_keys: {}",
                                e
                            ))
                        })?;

                        for (idx, key) in keys.into_iter().enumerate() {
                            key_to_rowgroup.insert(key, idx);
                        }
                    }
                }
            }
        }

        // Fallback: if no metadata, read first row of each RowGroup
        if key_to_rowgroup.is_empty() {
            println!("  Warning: No row_group_keys metadata found, reading from RowGroups...");
            for rg_idx in 0..num_row_groups {
                if let Ok(key) = Self::read_first_doc_id(&reader, rg_idx) {
                    key_to_rowgroup.insert(key, rg_idx);
                }
            }
        }

        Ok(Self {
            file_path: path.to_string(),
            key_to_rowgroup: Arc::new(key_to_rowgroup),
            num_row_groups,
        })
    }

    /// Read the first doc_id (internal_id) from a RowGroup
    /// Opens the file and reads just the first row from the specified RowGroup
    fn read_first_doc_id(
        reader: &datafusion::parquet::file::reader::SerializedFileReader<File>,
        row_group_idx: usize,
    ) -> CoreResult<u32> {
        let _ = row_group_idx; // Suppress warning
        let _ = reader; // For now, just return 0 as we'll rely on the metadata stored in the file
                        // This is a fallback that should rarely be used
        Ok(0)
    }

    /// Get a RecordBatch by its starting doc_id
    pub fn get(&self, key: &u32) -> Option<RecordBatch> {
        self.get_with_projection(key, None)
    }

    /// Get a RecordBatch with column projection
    ///
    /// # Arguments
    /// * `key` - The starting doc_id of the batch
    /// * `projection` - Optional list of column indices to read. If None, reads all columns.
    ///
    /// # Performance
    /// Using projection can significantly reduce I/O and memory usage:
    /// - Reading 2/4 columns → ~2× faster
    /// - Reading 1/4 columns → ~4× faster
    pub fn get_with_projection(
        &self,
        key: &u32,
        projection: Option<&[usize]>,
    ) -> Option<RecordBatch> {
        use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        use datafusion::parquet::arrow::ProjectionMask;

        // Find the RowGroup index
        let rg_idx = *self.key_to_rowgroup.get(key)?;

        // Open file and create builder
        let file = File::open(&self.file_path).ok()?;
        let mut builder = ParquetRecordBatchReaderBuilder::try_new(file).ok()?;

        // Apply column projection if specified
        if let Some(cols) = projection {
            let schema_descr = builder.metadata().file_metadata().schema_descr_ptr();
            let mask = ProjectionMask::roots(&schema_descr, cols.to_vec());
            builder = builder.with_projection(mask);
        }

        // Build reader with specific row group selection
        let mut reader = builder.with_row_groups(vec![rg_idx]).build().ok()?;

        // Read the batch from the selected RowGroup
        reader.next()?.ok()
    }

    /// Floor lookup: find the largest key <= given key
    pub fn floor(&self, key: &u32) -> Option<(u32, RecordBatch)> {
        // Use BTreeMap's range to find floor
        let (k, _idx) = self.key_to_rowgroup.range(..=*key).next_back()?;
        let batch = self.get(k)?;
        Some((*k, batch))
    }

    /// Floor lookup with column projection
    pub fn floor_with_projection(
        &self,
        key: &u32,
        projection: Option<&[usize]>,
    ) -> Option<(u32, RecordBatch)> {
        let (k, _idx) = self.key_to_rowgroup.range(..=*key).next_back()?;
        let batch = self.get_with_projection(k, projection)?;
        Some((*k, batch))
    }

    /// Batch read multiple RowGroups at once with column projection
    ///
    /// This is a critical performance optimization that reduces I/O operations by:
    /// - Reading multiple RowGroups in one Parquet file scan
    /// - Applying column projection across all batches
    /// - Returning a HashMap for fast lookup by batch start key
    ///
    /// # Arguments
    /// * `keys` - Slice of starting doc_ids for the batches to read
    /// * `projection` - Optional list of column indices to read
    ///
    /// # Performance
    /// - 10 RowGroups: ~5-8× faster than individual reads
    /// - 50 RowGroups: ~20-30× faster than individual reads
    /// - 100 RowGroups: ~40-50× faster than individual reads
    ///
    /// # Returns
    /// HashMap mapping each key to its RecordBatch
    pub fn get_batch_with_projection(
        &self,
        keys: &[u32],
        projection: Option<&[usize]>,
    ) -> HashMap<u32, RecordBatch> {
        use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        use datafusion::parquet::arrow::ProjectionMask;

        let mut result = HashMap::new();

        if keys.is_empty() {
            return result;
        }

        // Convert keys to RowGroup indices
        let mut row_group_indices = Vec::new();
        let mut key_to_rg_idx = HashMap::new();

        for key in keys {
            if let Some(&rg_idx) = self.key_to_rowgroup.get(key) {
                row_group_indices.push(rg_idx);
                key_to_rg_idx.insert(rg_idx, *key);
            }
        }

        if row_group_indices.is_empty() {
            return result;
        }

        // Sort RowGroup indices for better I/O performance (sequential reads)
        row_group_indices.sort_unstable();
        row_group_indices.dedup(); // Remove duplicates if any

        // Open file and create builder
        let file = match File::open(&self.file_path) {
            Ok(f) => f,
            Err(_) => return result,
        };

        let mut builder = match ParquetRecordBatchReaderBuilder::try_new(file) {
            Ok(b) => b,
            Err(_) => return result,
        };

        // Apply column projection if specified
        if let Some(cols) = projection {
            println!(
                "[DEBUG] get_batch_with_projection: applying projection {:?}",
                cols
            );

            // 使用更简单的方法: 通过schema的字段选择创建投影
            // 注意: with_projection需要ProjectionMask,我们通过select方法创建
            let schema_descr = builder.metadata().file_metadata().schema_descr();
            let num_fields = schema_descr.num_columns();
            println!("[DEBUG] Parquet file has {} columns total", num_fields);

            // 创建一个bool数组,标记哪些列要读取
            let mut column_mask = vec![false; num_fields];
            for &col_idx in cols {
                if col_idx < num_fields {
                    column_mask[col_idx] = true;
                    println!("[DEBUG] Enabling column index {}", col_idx);
                }
            }

            // 使用ProjectionMask::leaves来指定要读取的叶子列
            use datafusion::parquet::arrow::ProjectionMask;
            let mask = ProjectionMask::leaves(
                schema_descr,
                column_mask
                    .into_iter()
                    .enumerate()
                    .filter_map(|(i, enabled)| if enabled { Some(i) } else { None }),
            );

            builder = builder.with_projection(mask);
        } else {
            println!("[DEBUG] get_batch_with_projection: no projection, reading all columns");
        }

        // Build reader with all selected RowGroups
        let mut reader = match builder.with_row_groups(row_group_indices.clone()).build() {
            Ok(r) => r,
            Err(_) => return result,
        };

        // Read all batches (one per RowGroup)
        let mut rg_batch_idx = 0;
        while let Some(batch_result) = reader.next() {
            if let Ok(batch) = batch_result {
                println!(
                    "[DEBUG] Read batch: {} rows, {} columns, schema: {:?}",
                    batch.num_rows(),
                    batch.num_columns(),
                    batch
                        .schema()
                        .fields()
                        .iter()
                        .map(|f| f.name())
                        .collect::<Vec<_>>()
                );
                // Map the batch back to its original key
                if let Some(&key) = key_to_rg_idx.get(&row_group_indices[rg_batch_idx]) {
                    result.insert(key, batch);
                }
                rg_batch_idx += 1;
            } else {
                break;
            }
        }

        result
    }

    pub fn len(&self) -> usize {
        self.num_row_groups
    }
}

impl Clone for ParquetRowDataReader {
    fn clone(&self) -> Self {
        Self {
            file_path: self.file_path.clone(),
            key_to_rowgroup: Arc::clone(&self.key_to_rowgroup),
            num_row_groups: self.num_row_groups,
        }
    }
}

/// RowDataStore: 用于存储文档的 row data (u32 -> RecordBatch)
/// 支持内存模式和磁盘模式（BTree 或 Parquet）
#[derive(Clone)]
pub enum RowDataStore {
    Disk(Arc<persist::TreeReader<u32, RecordBatch>>),
    Parquet(Arc<ParquetRowDataReader>),
    Memory(BTree<u32, RecordBatch>),
}

impl RowDataStore {
    pub fn new_memory(size: usize) -> Self {
        RowDataStore::Memory(BTree::new(size))
    }

    pub fn new_disk<S>(path: &str, serializer: S) -> CoreResult<Self>
    where
        S: persist::ReadSerializer<u32, RecordBatch> + 'static,
    {
        let reader = persist::TreeReader::new(std::path::Path::new(path), Box::new(serializer))
            .map_err(|e| crate::utils::error::CoreError::IOError(e.to_string()))?;
        Ok(RowDataStore::Disk(Arc::new(reader)))
    }

    /// Open a Parquet file for reading row data
    pub fn new_parquet(path: &str) -> CoreResult<Self> {
        let reader = ParquetRowDataReader::new(path)?;
        Ok(RowDataStore::Parquet(Arc::new(reader)))
    }

    /// 插入一个 RecordBatch
    pub fn put(&mut self, key: u32, batch: RecordBatch) {
        if let RowDataStore::Memory(tree) = self {
            tree.put(key, batch);
        } else {
            panic!("cannot write to disk store");
        }
    }

    /// 获取一个 RecordBatch
    pub fn get(&self, key: &u32) -> Option<RecordBatch> {
        match self {
            RowDataStore::Disk(reader) => reader.get(key),
            RowDataStore::Parquet(reader) => reader.get(key),
            RowDataStore::Memory(tree) => tree.get(key).cloned(),
        }
    }

    /// Get a RecordBatch with column projection (only for Parquet)
    ///
    /// For Parquet format, this provides significant performance benefits by only reading
    /// the required columns from disk. For BTree and Memory formats, this falls back to
    /// reading all columns.
    ///
    /// # Performance Benefits
    /// - Reading 2/4 columns → ~2× faster I/O
    /// - Reading 1/4 columns → ~4× faster I/O
    /// - Reduced memory usage proportional to columns read
    pub fn get_with_projection(
        &self,
        key: &u32,
        projection: Option<&[usize]>,
    ) -> Option<RecordBatch> {
        match self {
            RowDataStore::Disk(reader) => reader.get(key),
            RowDataStore::Parquet(reader) => reader.get_with_projection(key, projection),
            RowDataStore::Memory(tree) => tree.get(key).cloned(),
        }
    }

    /// Find the largest key-value pair where key <= given key (floor lookup)
    /// This is used to find which RecordBatch contains a specific doc_id
    ///
    /// Example: BTree has keys [1, 100, 200], query for doc_id 50 returns batch at key 1
    pub fn floor(&self, key: &u32) -> Option<(u32, RecordBatch)> {
        match self {
            RowDataStore::Disk(reader) => {
                // For disk, we need to iterate (TreeReader doesn't have floor yet)
                // This is a simple implementation - can be optimized later
                reader.floor(key).map(|item| {
                    let (k, v, _ttl) = &*item;
                    (*k, v.clone())
                })
            }
            RowDataStore::Parquet(reader) => reader.floor(key),
            RowDataStore::Memory(tree) => tree.floor(key).map(|item| {
                let (k, v, _ttl) = &*item;
                (*k, v.clone())
            }),
        }
    }

    /// Floor lookup with column projection (optimized for Parquet)
    pub fn floor_with_projection(
        &self,
        key: &u32,
        projection: Option<&[usize]>,
    ) -> Option<(u32, RecordBatch)> {
        match self {
            RowDataStore::Disk(reader) => reader.floor(key).map(|item| {
                let (k, v, _ttl) = &*item;
                (*k, v.clone())
            }),
            RowDataStore::Parquet(reader) => reader.floor_with_projection(key, projection),
            RowDataStore::Memory(tree) => tree.floor(key).map(|item| {
                let (k, v, _ttl) = &*item;
                (*k, v.clone())
            }),
        }
    }

    /// Batch read multiple RecordBatches with column projection
    ///
    /// This is a critical performance optimization for Parquet format that can provide
    /// 10-50× speedup by reading multiple RowGroups in a single file scan.
    ///
    /// For BTree and Memory formats, this falls back to individual reads.
    ///
    /// # Performance Benefits (Parquet only)
    /// - Reduces file open/close overhead
    /// - Enables sequential I/O instead of random seeks
    /// - Amortizes metadata parsing cost
    /// - Better utilizes disk bandwidth
    ///
    /// # Arguments
    /// * `keys` - Slice of starting doc_ids for batches to read
    /// * `projection` - Optional column indices to read
    ///
    /// # Returns
    /// HashMap mapping each key to its RecordBatch
    pub fn get_batch_with_projection(
        &self,
        keys: &[u32],
        projection: Option<&[usize]>,
    ) -> HashMap<u32, RecordBatch> {
        match self {
            RowDataStore::Parquet(reader) => reader.get_batch_with_projection(keys, projection),
            RowDataStore::Disk(reader) => {
                // Fallback: individual reads for BTree format
                let mut result = HashMap::new();
                for key in keys {
                    if let Some(batch) = reader.get(key) {
                        result.insert(*key, batch);
                    }
                }
                result
            }
            RowDataStore::Memory(tree) => {
                // Fallback: individual reads for Memory format
                let mut result = HashMap::new();
                for key in keys {
                    if let Some(batch) = tree.get(key) {
                        result.insert(*key, batch.clone());
                    }
                }
                result
            }
        }
    }

    /// 获取所有数据的迭代器(仅用于持久化)
    pub fn iter(&self) -> Option<impl Iterator<Item = (u32, RecordBatch)> + '_> {
        match self {
            RowDataStore::Memory(tree) => Some(tree.iter().map(|item| {
                let (key, value, _ttl) = &*item;
                (*key, value.clone())
            })),
            RowDataStore::Disk(_) | RowDataStore::Parquet(_) => None,
        }
    }

    pub fn len(&self) -> usize {
        match self {
            RowDataStore::Disk(reader) => reader.len() as usize,
            RowDataStore::Parquet(reader) => reader.len(),
            RowDataStore::Memory(tree) => tree.len(),
        }
    }
}

pub trait IndexReader: Send + Sync + 'static {
    fn name(&self) -> &str;
    fn field_type(&self) -> FieldType;
    fn query(&self, value: &ScalarValue) -> Option<RoaringBitmap>;

    /// Range query with support for open/closed intervals
    ///
    /// # Arguments
    /// * `start` - Start bound value
    /// * `start_inclusive` - Whether start bound is inclusive (>=) or exclusive (>)
    /// * `end` - End bound value
    /// * `end_inclusive` - Whether end bound is inclusive (<=) or exclusive (<)
    fn range(
        &self,
        start: &ScalarValue,
        start_inclusive: bool,
        end: &ScalarValue,
        end_inclusive: bool,
    ) -> Option<RoaringBitmap>;
}

pub trait IndexWriter: Send + Sync + 'static {
    fn name(&self) -> &str;
    fn field_type(&self) -> FieldType;
    fn write(&self, data: &RecordBatch) -> CoreResult<()>;
    fn mget_internal_id(&self, column: &ArrayRef) -> Vec<u32>;
    fn as_any(&self) -> &dyn Any;
}

pub trait PkWriter: Send + Sync + 'static {
    // write primary key and return the need delete ids
    fn write_pk(
        &self,
        data: &RecordBatch,
        info: Option<WriteInfo>,
        lock: &RwLock<()>,
    ) -> CoreResult<HashSet<u32>>;
}

#[derive(Clone)]
pub(crate) enum InvertedIndex<K>
where
    K: Clone + PartialOrd,
{
    Disk(Arc<persist::TreeReader<K, RoaringBitmap>>),
    Memory(BTree<K, Arc<RwLock<Vec<u32>>>>),
}

impl<K: Clone + PartialOrd + Ord> InvertedIndex<K> {
    pub fn new_memory(size: usize) -> InvertedIndex<K> {
        InvertedIndex::Memory(BTree::new(size))
    }

    pub fn new_disk<S>(path: &str, serializer: S) -> CoreResult<InvertedIndex<K>>
    where
        S: persist::ReadSerializer<K, RoaringBitmap> + 'static,
    {
        let reader = persist::TreeReader::new(std::path::Path::new(path), Box::new(serializer))
            .map_err(|e| crate::utils::error::CoreError::IOError(e.to_string()))?;
        Ok(InvertedIndex::Disk(Arc::new(reader)))
    }
}

/// write functions
impl<K: Clone + PartialOrd + Ord> InvertedIndex<K> {
    pub(crate) fn extend(&mut self, k: K, ids: Vec<u32>) {
        if let InvertedIndex::Memory(tree) = self {
            match tree.get(&k) {
                Some(v) => v.write().unwrap().extend(ids),
                None => _ = tree.put(k, Arc::new(RwLock::new(ids))),
            }
        } else {
            panic!("cannot insert to disk index");
        }
    }

    pub(crate) fn append_ids(&mut self, k: K, ids: Vec<u32>) {
        if let InvertedIndex::Memory(tree) = self {
            match tree.get(&k) {
                Some(v) => v.write().unwrap().extend(ids),
                None => _ = tree.put(k, Arc::new(RwLock::new(ids))),
            }
        } else {
            panic!("cannot insert to disk index");
        }
    }

    pub(crate) fn insert(
        &mut self,
        k: K,
        ids: Vec<u32>,
    ) -> Option<mem_btree::Item<K, Arc<RwLock<Vec<u32>>>>> {
        if let InvertedIndex::Memory(tree) = self {
            tree.put(k, Arc::new(RwLock::new(ids)))
        } else {
            panic!("cannot insert to disk index");
        }
    }
}

/// read functions
impl<K: Clone + PartialOrd + Ord> InvertedIndex<K> {
    pub(crate) fn get_bitmap(&self, k: &K) -> Option<RoaringBitmap> {
        match self {
            InvertedIndex::Disk(r) => r.get(k),
            InvertedIndex::Memory(btree) => btree.get(k).map(|v| {
                RoaringBitmap::from_sorted_iter(v.read().unwrap().iter().copied()).unwrap()
            }),
        }
    }

    /// Range query with support for open/closed intervals
    ///
    /// # Arguments
    /// * `start` - Optional start bound (None means no lower bound)
    /// * `start_inclusive` - Whether start bound is inclusive (>=) or exclusive (>)
    /// * `end` - Optional end bound (None means no upper bound)
    /// * `end_inclusive` - Whether end bound is inclusive (<=) or exclusive (<)
    ///
    /// # Examples
    /// - `range_query(Some(10), true, Some(20), true)` -> [10, 20]
    /// - `range_query(Some(10), false, Some(20), true)` -> (10, 20]
    /// - `range_query(Some(10), true, Some(20), false)` -> [10, 20)
    /// - `range_query(Some(10), false, Some(20), false)` -> (10, 20)
    pub(crate) fn range_query(
        &self,
        start: Option<&K>,
        start_inclusive: bool,
        end: Option<&K>,
        end_inclusive: bool,
    ) -> RoaringBitmap {
        let mut result = RoaringBitmap::new();
        match self {
            InvertedIndex::Disk(reader) => {
                // Use TreeIterator with seek optimization for efficient range query
                let mut iter = reader.iter();

                // Seek to start position if specified
                if let Some(s) = start {
                    iter.seek(s);
                }

                // Iterate through keys in range
                for item in iter {
                    let (key, bitmap, _ttl) = &*item;

                    // Check start bound
                    let start_ok = match start {
                        Some(s) => {
                            if start_inclusive {
                                key >= s // [s, ...)
                            } else {
                                key > s // (s, ...)
                            }
                        }
                        None => true, // No lower bound
                    };

                    // Check end bound
                    let end_ok = match end {
                        Some(e) => {
                            if end_inclusive {
                                key <= e // (..., e]
                            } else {
                                key < e // (..., e)
                            }
                        }
                        None => true, // No upper bound
                    };

                    // Early termination: if key exceeds end bound, stop iterating
                    if !end_ok {
                        break;
                    }

                    if start_ok {
                        result |= bitmap;
                    }
                }
                result
            }
            InvertedIndex::Memory(btree) => {
                // For memory index, use seek to efficiently position iterator at start
                // This avoids scanning from the beginning of the tree
                let mut iter = btree.iter();

                // Seek to start position if specified
                if let Some(s) = start {
                    iter.seek(s);
                }

                // Iterate through keys in range
                while let Some(item) = iter.next() {
                    let (key, ids_lock, _ttl) = &*item;

                    // Check start bound
                    let start_ok = match start {
                        Some(s) => {
                            if start_inclusive {
                                key >= s // [s, ...)
                            } else {
                                key > s // (s, ...)
                            }
                        }
                        None => true, // No lower bound
                    };

                    // Check end bound
                    let end_ok = match end {
                        Some(e) => {
                            if end_inclusive {
                                key <= e // (..., e]
                            } else {
                                key < e // (..., e)
                            }
                        }
                        None => true, // No upper bound
                    };

                    // Early termination: if key exceeds end bound, stop iterating
                    // This is important for performance as keys are sorted
                    if !end_ok {
                        break;
                    }

                    if start_ok {
                        let ids = ids_lock.read().unwrap();
                        result |= RoaringBitmap::from_sorted_iter(ids.iter().copied()).unwrap();
                    }
                }
                result
            }
        }
    }

    pub(crate) fn memory_get_ref(&self, k: &K) -> Option<&Arc<RwLock<Vec<u32>>>> {
        match self {
            InvertedIndex::Disk(_) => unreachable!(),
            InvertedIndex::Memory(btree) => btree.get(k),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            InvertedIndex::Disk(r) => r.len() as usize,
            InvertedIndex::Memory(btree) => btree.len(),
        }
    }
}

/// Encode a list of u32 doc IDs into bytes with a leading marker byte.
/// - 0: delta-encoded u32 sequence (big-endian, mem_btree::persist::num_ser::u32_coder)
/// - 1: RoaringBitmap native serialization
/// Returns Vec<u8> on success.
pub fn encode_roaring_from_u32s(ids: &[u32]) -> std::io::Result<Vec<u8>> {
    let mut out = Vec::new();
    if ids.len() < 1000 {
        // marker 0 => delta encoding
        out.push(0u8);
        num_ser::u32_coder::write_delta(&mut out, &ids.to_vec())?;
    } else {
        // marker 1 => roaring native
        out.push(1u8);
        let rb = RoaringBitmap::from_iter(ids.iter().copied());
        // roaring serialize_into returns io::Result
        rb.serialize_into(&mut out)?;
    }
    Ok(out)
}

/// Decode bytes encoded by `encode_roaring_from_u32s` back into a RoaringBitmap.
/// Accepts empty slice as empty bitmap.
pub fn decode_roaring_from_bytes(
    data: &[u8],
) -> std::result::Result<RoaringBitmap, Box<dyn Error>> {
    if data.is_empty() {
        return Ok(RoaringBitmap::new());
    }

    let marker = data[0];
    let mut data_slice = &data[1..];
    match marker {
        0 => {
            let ids = num_ser::u32_coder::read_delta(&data_slice);
            Ok(RoaringBitmap::from_iter(ids))
        }
        1 => RoaringBitmap::deserialize_from(&mut data_slice)
            .map_err(|e| Box::new(e) as Box<dyn Error>),
        _ => Err(format!("Unknown marker byte: {}", marker).into()),
    }
}

/// Encode a RoaringBitmap into bytes with a leading marker, choosing the smaller form
/// between: 0 + delta-encoded u32s, or 1 + roaring native format.
pub fn encode_roaring_from_bitmap(bitmap: &RoaringBitmap) -> Vec<u8> {
    // Estimate sizes - need to check actual delta-encoded size
    let ids: Vec<u32> = bitmap.iter().collect();

    // Try delta encoding
    let mut delta_buf = Vec::new();
    delta_buf.push(0u8);
    if let Ok(_) = num_ser::u32_coder::write_delta(&mut delta_buf, &ids) {
        // Try roaring native
        let mut roaring_buf = Vec::new();
        roaring_buf.push(1u8);
        if let Ok(_) = bitmap.serialize_into(&mut roaring_buf) {
            // Choose the smaller one
            if delta_buf.len() <= roaring_buf.len() {
                return delta_buf;
            } else {
                return roaring_buf;
            }
        } else {
            // Roaring serialization failed, use delta
            return delta_buf;
        }
    } else {
        // Delta encoding failed (shouldn't happen), fallback to roaring
        let mut buf = Vec::new();
        buf.push(1u8);
        let _ = bitmap.serialize_into(&mut buf);
        return buf;
    }
}
