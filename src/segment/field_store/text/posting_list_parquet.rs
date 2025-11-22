//! Parquet-based posting list persistence
//!
//! Schema:
//! - term: String (sorted, dictionary encoded)
//! - doc_freq: u32
//! - total_freq: u64  
//! - doc_ids: List<u32> (delta encoded by Parquet)
//! - term_freqs: List<u32>
//! - positions: List<List<u32>> (nested list, delta encoded)

use std::path::Path;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, ListArray, StringArray, UInt32Array, UInt64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::basic::Compression;
use datafusion::parquet::file::properties::WriterProperties;

use crate::utils::error::{CoreError, CoreResult};

use super::fulltext_field::{PostingEntry, TermStats};

/// Parquet schema for posting lists
pub fn posting_list_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("term", DataType::Utf8, false),
        Field::new("doc_freq", DataType::UInt32, false),
        Field::new("total_freq", DataType::UInt64, false),
        Field::new(
            "doc_ids",
            DataType::List(Arc::new(Field::new("item", DataType::UInt32, false))),
            false,
        ),
        Field::new(
            "term_freqs",
            DataType::List(Arc::new(Field::new("item", DataType::UInt32, false))),
            false,
        ),
        Field::new(
            "positions",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::List(Arc::new(Field::new("item", DataType::UInt32, false))),
                false,
            ))),
            false,
        ),
    ]))
}

/// Row representation for Parquet
#[derive(Debug, Clone)]
pub struct PostingListRow {
    pub term: String,
    pub doc_freq: u32,
    pub total_freq: u64,
    pub doc_ids: Vec<u32>,
    pub term_freqs: Vec<u32>,
    pub positions: Vec<Vec<u32>>,
}

impl PostingListRow {
    /// Create from term statistics and posting entries
    pub fn new(term: String, stats: &TermStats, postings: &[PostingEntry]) -> Self {
        Self {
            term,
            doc_freq: stats.doc_freq,
            total_freq: stats.total_freq,
            doc_ids: postings.iter().map(|p| p.doc_id).collect(),
            term_freqs: postings.iter().map(|p| p.term_freq).collect(),
            positions: postings.iter().map(|p| p.positions.clone()).collect(),
        }
    }

    /// Convert back to stats and postings
    pub fn to_stats_and_postings(&self) -> (TermStats, Vec<PostingEntry>) {
        let stats = TermStats {
            doc_freq: self.doc_freq,
            total_freq: self.total_freq,
        };

        let postings = self
            .doc_ids
            .iter()
            .zip(self.term_freqs.iter())
            .zip(self.positions.iter())
            .map(|((&doc_id, &term_freq), positions)| PostingEntry {
                doc_id,
                term_freq,
                positions: positions.clone(),
            })
            .collect();

        (stats, postings)
    }
}

/// Write posting lists to Parquet file
pub fn write_posting_lists(rows: &[PostingListRow], path: &str) -> CoreResult<()> {
    if rows.is_empty() {
        return Err(CoreError::Internal(
            "Cannot write empty posting lists".to_string(),
        ));
    }

    let schema = posting_list_schema();

    // Build Arrow arrays
    let term_array = StringArray::from(rows.iter().map(|r| r.term.as_str()).collect::<Vec<_>>());

    let doc_freq_array = UInt32Array::from(rows.iter().map(|r| r.doc_freq).collect::<Vec<_>>());

    let total_freq_array = UInt64Array::from(rows.iter().map(|r| r.total_freq).collect::<Vec<_>>());

    // Build doc_ids list array
    let mut doc_ids_offsets = vec![0i32];
    let mut doc_ids_values = Vec::new();
    for row in rows {
        doc_ids_values.extend_from_slice(&row.doc_ids);
        doc_ids_offsets.push(doc_ids_values.len() as i32);
    }
    let doc_ids_array = ListArray::try_new(
        Arc::new(Field::new("item", DataType::UInt32, false)),
        datafusion::arrow::buffer::OffsetBuffer::new(doc_ids_offsets.into()),
        Arc::new(UInt32Array::from(doc_ids_values)),
        None,
    )
    .map_err(|e| CoreError::Internal(format!("Failed to create doc_ids array: {}", e)))?;

    // Build term_freqs list array
    let mut term_freqs_offsets = vec![0i32];
    let mut term_freqs_values = Vec::new();
    for row in rows {
        term_freqs_values.extend_from_slice(&row.term_freqs);
        term_freqs_offsets.push(term_freqs_values.len() as i32);
    }
    let term_freqs_array = ListArray::try_new(
        Arc::new(Field::new("item", DataType::UInt32, false)),
        datafusion::arrow::buffer::OffsetBuffer::new(term_freqs_offsets.into()),
        Arc::new(UInt32Array::from(term_freqs_values)),
        None,
    )
    .map_err(|e| CoreError::Internal(format!("Failed to create term_freqs array: {}", e)))?;

    // Build positions nested list array
    let mut positions_outer_offsets = vec![0i32];
    let mut positions_inner_offsets = vec![0i32];
    let mut positions_values = Vec::new();

    for row in rows {
        for pos_vec in &row.positions {
            positions_values.extend_from_slice(pos_vec);
            positions_inner_offsets.push(positions_values.len() as i32);
        }
        positions_outer_offsets.push(positions_inner_offsets.len() as i32 - 1);
    }

    let positions_inner_array = ListArray::try_new(
        Arc::new(Field::new("item", DataType::UInt32, false)),
        datafusion::arrow::buffer::OffsetBuffer::new(positions_inner_offsets.into()),
        Arc::new(UInt32Array::from(positions_values)),
        None,
    )
    .map_err(|e| CoreError::Internal(format!("Failed to create positions inner array: {}", e)))?;

    let positions_array = ListArray::try_new(
        Arc::new(Field::new(
            "item",
            DataType::List(Arc::new(Field::new("item", DataType::UInt32, false))),
            false,
        )),
        datafusion::arrow::buffer::OffsetBuffer::new(positions_outer_offsets.into()),
        Arc::new(positions_inner_array),
        None,
    )
    .map_err(|e| CoreError::Internal(format!("Failed to create positions array: {}", e)))?;

    // Create RecordBatch
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(term_array) as ArrayRef,
            Arc::new(doc_freq_array) as ArrayRef,
            Arc::new(total_freq_array) as ArrayRef,
            Arc::new(doc_ids_array) as ArrayRef,
            Arc::new(term_freqs_array) as ArrayRef,
            Arc::new(positions_array) as ArrayRef,
        ],
    )
    .map_err(|e| CoreError::Internal(format!("Failed to create RecordBatch: {}", e)))?;

    // Create parent directory if needed
    if let Some(parent) = Path::new(path).parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| CoreError::Internal(format!("Failed to create directory: {}", e)))?;
    }

    // Write to Parquet with compression
    let file = std::fs::File::create(path)
        .map_err(|e| CoreError::Internal(format!("Failed to create file '{}': {}", path, e)))?;

    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(
            datafusion::parquet::basic::ZstdLevel::try_new(3).unwrap(),
        ))
        .set_dictionary_enabled(true) // Enable dictionary encoding for term column
        .set_statistics_enabled(datafusion::parquet::file::properties::EnabledStatistics::Page) // Enable page-level statistics
        .build();

    let mut writer = ArrowWriter::try_new(file, schema, Some(props))
        .map_err(|e| CoreError::Internal(format!("Failed to create Parquet writer: {}", e)))?;

    writer
        .write(&batch)
        .map_err(|e| CoreError::Internal(format!("Failed to write batch: {}", e)))?;

    writer
        .close()
        .map_err(|e| CoreError::Internal(format!("Failed to close writer: {}", e)))?;

    Ok(())
}

/// Read all posting lists from Parquet file
pub fn read_posting_lists(path: &str) -> CoreResult<Vec<PostingListRow>> {
    if !Path::new(path).exists() {
        return Err(CoreError::Internal(format!("File not found: {}", path)));
    }

    let file = std::fs::File::open(path)
        .map_err(|e| CoreError::Internal(format!("Failed to open file '{}': {}", path, e)))?;

    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| CoreError::Internal(format!("Failed to create reader: {}", e)))?;

    let reader = builder
        .build()
        .map_err(|e| CoreError::Internal(format!("Failed to build reader: {}", e)))?;

    let mut rows = Vec::new();

    for batch_result in reader {
        let batch = batch_result
            .map_err(|e| CoreError::Internal(format!("Failed to read batch: {}", e)))?;

        let term_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| CoreError::Internal("Invalid term column".to_string()))?;

        let doc_freq_array = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| CoreError::Internal("Invalid doc_freq column".to_string()))?;

        let total_freq_array = batch
            .column(2)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| CoreError::Internal("Invalid total_freq column".to_string()))?;

        let doc_ids_list = batch
            .column(3)
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| CoreError::Internal("Invalid doc_ids column".to_string()))?;

        let term_freqs_list = batch
            .column(4)
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| CoreError::Internal("Invalid term_freqs column".to_string()))?;

        let positions_list = batch
            .column(5)
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| CoreError::Internal("Invalid positions column".to_string()))?;

        for row_idx in 0..batch.num_rows() {
            let term = term_array.value(row_idx).to_string();
            let doc_freq = doc_freq_array.value(row_idx);
            let total_freq = total_freq_array.value(row_idx);

            // Extract doc_ids
            let doc_ids_slice = doc_ids_list.value(row_idx);
            let doc_ids_values = doc_ids_slice
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| CoreError::Internal("Invalid doc_ids values".to_string()))?;
            let doc_ids: Vec<u32> = (0..doc_ids_values.len())
                .map(|i| doc_ids_values.value(i))
                .collect();

            // Extract term_freqs
            let term_freqs_slice = term_freqs_list.value(row_idx);
            let term_freqs_values = term_freqs_slice
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| CoreError::Internal("Invalid term_freqs values".to_string()))?;
            let term_freqs: Vec<u32> = (0..term_freqs_values.len())
                .map(|i| term_freqs_values.value(i))
                .collect();

            // Extract positions (nested list)
            let positions_outer = positions_list.value(row_idx);
            let positions_outer_list = positions_outer
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| CoreError::Internal("Invalid positions outer list".to_string()))?;

            let mut positions = Vec::new();
            for pos_idx in 0..positions_outer_list.len() {
                let pos_inner = positions_outer_list.value(pos_idx);
                let pos_values = pos_inner
                    .as_any()
                    .downcast_ref::<UInt32Array>()
                    .ok_or_else(|| CoreError::Internal("Invalid position values".to_string()))?;
                let pos_vec: Vec<u32> =
                    (0..pos_values.len()).map(|i| pos_values.value(i)).collect();
                positions.push(pos_vec);
            }

            rows.push(PostingListRow {
                term,
                doc_freq,
                total_freq,
                doc_ids,
                term_freqs,
                positions,
            });
        }
    }

    Ok(rows)
}

/// Binary search for a term in sorted Parquet file using Row Group statistics
/// Returns the row containing the term if found
pub fn search_term(path: &str, term: &str) -> CoreResult<Option<PostingListRow>> {
    if !Path::new(path).exists() {
        return Err(CoreError::Internal(format!("File not found: {}", path)));
    }

    let file = std::fs::File::open(path)
        .map_err(|e| CoreError::Internal(format!("Failed to open file '{}': {}", path, e)))?;

    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| CoreError::Internal(format!("Failed to create reader: {}", e)))?;

    let metadata = builder.metadata();
    let num_row_groups = metadata.num_row_groups();

    // Binary search to find the Row Group containing the term using min/max statistics
    let mut target_row_group = None;
    for rg_idx in 0..num_row_groups {
        let row_group = metadata.row_group(rg_idx);

        // Get statistics for the term column (column 0)
        if let Some(stats) = row_group.column(0).statistics() {
            // Try to get min/max values
            if let (Some(min_val), Some(max_val)) = (stats.min_bytes_opt(), stats.max_bytes_opt()) {
                if let (Ok(min_str), Ok(max_str)) =
                    (std::str::from_utf8(min_val), std::str::from_utf8(max_val))
                {
                    // Check if term is within this Row Group's range
                    if term >= min_str && term <= max_str {
                        target_row_group = Some(rg_idx);
                        break;
                    } else if term < min_str {
                        // Term is before this Row Group, so it doesn't exist
                        return Ok(None);
                    }
                }
            }
        }
    }

    // If no Row Group contains the term, it doesn't exist
    let rg_idx = match target_row_group {
        Some(idx) => idx,
        None => return Ok(None),
    };

    // Read only the target Row Group
    let reader = builder
        .with_row_groups(vec![rg_idx])
        .build()
        .map_err(|e| CoreError::Internal(format!("Failed to build reader: {}", e)))?;

    let mut rows = Vec::new();
    for batch_result in reader {
        let batch = batch_result
            .map_err(|e| CoreError::Internal(format!("Failed to read batch: {}", e)))?;

        // Extract rows from the batch (same logic as read_posting_lists)
        let term_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| CoreError::Internal("Invalid term column".to_string()))?;

        let doc_freq_array = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| CoreError::Internal("Invalid doc_freq column".to_string()))?;

        let total_freq_array = batch
            .column(2)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| CoreError::Internal("Invalid total_freq column".to_string()))?;

        let doc_ids_list = batch
            .column(3)
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| CoreError::Internal("Invalid doc_ids column".to_string()))?;

        let term_freqs_list = batch
            .column(4)
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| CoreError::Internal("Invalid term_freqs column".to_string()))?;

        let positions_list = batch
            .column(5)
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| CoreError::Internal("Invalid positions column".to_string()))?;

        for row_idx in 0..batch.num_rows() {
            let row_term = term_array.value(row_idx).to_string();
            let doc_freq = doc_freq_array.value(row_idx);
            let total_freq = total_freq_array.value(row_idx);

            // Extract doc_ids
            let doc_ids_slice = doc_ids_list.value(row_idx);
            let doc_ids_values = doc_ids_slice
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| CoreError::Internal("Invalid doc_ids values".to_string()))?;
            let doc_ids: Vec<u32> = (0..doc_ids_values.len())
                .map(|i| doc_ids_values.value(i))
                .collect();

            // Extract term_freqs
            let term_freqs_slice = term_freqs_list.value(row_idx);
            let term_freqs_values = term_freqs_slice
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| CoreError::Internal("Invalid term_freqs values".to_string()))?;
            let term_freqs: Vec<u32> = (0..term_freqs_values.len())
                .map(|i| term_freqs_values.value(i))
                .collect();

            // Extract positions (nested list)
            let positions_outer = positions_list.value(row_idx);
            let positions_outer_list = positions_outer
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| CoreError::Internal("Invalid positions outer list".to_string()))?;

            let mut positions = Vec::new();
            for pos_idx in 0..positions_outer_list.len() {
                let pos_inner = positions_outer_list.value(pos_idx);
                let pos_values = pos_inner
                    .as_any()
                    .downcast_ref::<UInt32Array>()
                    .ok_or_else(|| CoreError::Internal("Invalid position values".to_string()))?;
                let pos_vec: Vec<u32> =
                    (0..pos_values.len()).map(|i| pos_values.value(i)).collect();
                positions.push(pos_vec);
            }

            rows.push(PostingListRow {
                term: row_term,
                doc_freq,
                total_freq,
                doc_ids,
                term_freqs,
                positions,
            });
        }
    }

    // Binary search within the Row Group rows
    Ok(rows
        .binary_search_by(|row| row.term.as_str().cmp(term))
        .ok()
        .map(|idx| rows[idx].clone()))
}

/// Columnar read: only read doc_ids without positions (useful for term queries)
/// This skips reading the positions column which can be large
pub fn search_term_docids_only(path: &str, term: &str) -> CoreResult<Option<Vec<u32>>> {
    if !Path::new(path).exists() {
        return Err(CoreError::Internal(format!("File not found: {}", path)));
    }

    let file = std::fs::File::open(path)
        .map_err(|e| CoreError::Internal(format!("Failed to open file '{}': {}", path, e)))?;

    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| CoreError::Internal(format!("Failed to create reader: {}", e)))?;

    let metadata = builder.metadata();
    let num_row_groups = metadata.num_row_groups();

    // Find target Row Group using statistics
    let mut target_row_group = None;
    for rg_idx in 0..num_row_groups {
        let row_group = metadata.row_group(rg_idx);

        if let Some(stats) = row_group.column(0).statistics() {
            if let (Some(min_val), Some(max_val)) = (stats.min_bytes_opt(), stats.max_bytes_opt()) {
                if let (Ok(min_str), Ok(max_str)) =
                    (std::str::from_utf8(min_val), std::str::from_utf8(max_val))
                {
                    if term >= min_str && term <= max_str {
                        target_row_group = Some(rg_idx);
                        break;
                    } else if term < min_str {
                        return Ok(None);
                    }
                }
            }
        }
    }

    let rg_idx = match target_row_group {
        Some(idx) => idx,
        None => return Ok(None),
    };

    // Get schema descriptor pointer (Arc cloned internally)
    let schema_descr_ptr = builder.metadata().file_metadata().schema_descr_ptr();

    // Read only columns 0 (term) and 3 (doc_ids) - skip positions!
    let reader = builder
        .with_row_groups(vec![rg_idx])
        .with_projection(datafusion::parquet::arrow::ProjectionMask::leaves(
            &schema_descr_ptr,
            vec![0, 3], // Only read term and doc_ids columns
        ))
        .build()
        .map_err(|e| CoreError::Internal(format!("Failed to build reader: {}", e)))?;

    for batch_result in reader {
        let batch = batch_result
            .map_err(|e| CoreError::Internal(format!("Failed to read batch: {}", e)))?;

        let term_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| CoreError::Internal("Invalid term column".to_string()))?;

        let doc_ids_list = batch
            .column(1) // Note: index 1 because we only projected 2 columns
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| CoreError::Internal("Invalid doc_ids column".to_string()))?;

        for row_idx in 0..batch.num_rows() {
            let row_term = term_array.value(row_idx);
            if row_term == term {
                let doc_ids_slice = doc_ids_list.value(row_idx);
                let doc_ids_values = doc_ids_slice
                    .as_any()
                    .downcast_ref::<UInt32Array>()
                    .ok_or_else(|| CoreError::Internal("Invalid doc_ids values".to_string()))?;
                let doc_ids: Vec<u32> = (0..doc_ids_values.len())
                    .map(|i| doc_ids_values.value(i))
                    .collect();
                return Ok(Some(doc_ids));
            }
        }
    }

    Ok(None)
}

/// Get Parquet file metadata including Row Group statistics
pub fn get_parquet_metadata(path: &str) -> CoreResult<ParquetMetadata> {
    if !Path::new(path).exists() {
        return Err(CoreError::Internal(format!("File not found: {}", path)));
    }

    let file = std::fs::File::open(path)
        .map_err(|e| CoreError::Internal(format!("Failed to open file '{}': {}", path, e)))?;

    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| CoreError::Internal(format!("Failed to create reader: {}", e)))?;

    Ok(ParquetMetadata {
        num_row_groups: builder.metadata().num_row_groups(),
        num_rows: builder.metadata().file_metadata().num_rows() as usize,
        file_size: std::fs::metadata(path).map(|m| m.len()).unwrap_or(0),
    })
}

/// Parquet file metadata
#[derive(Debug, Clone)]
pub struct ParquetMetadata {
    pub num_row_groups: usize,
    pub num_rows: usize,
    pub file_size: u64,
}

/// Batch read multiple terms efficiently
/// This is more efficient than calling search_term multiple times
pub fn search_terms_batch(path: &str, terms: &[&str]) -> CoreResult<Vec<Option<PostingListRow>>> {
    if terms.is_empty() {
        return Ok(Vec::new());
    }

    // Sort terms for efficient lookup
    let mut sorted_terms: Vec<&str> = terms.to_vec();
    sorted_terms.sort_unstable();
    sorted_terms.dedup();

    let all_rows = read_posting_lists(path)?;
    let mut results = Vec::with_capacity(sorted_terms.len());

    for term in sorted_terms {
        let result = all_rows
            .binary_search_by(|row| row.term.as_str().cmp(term))
            .ok()
            .map(|idx| all_rows[idx].clone());
        results.push(result);
    }

    Ok(results)
}

/// Term index entry for O(1) lookup
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TermIndexEntry {
    pub term: String,
    pub row_group_id: usize,
    pub row_offset: usize, // Offset within the Row Group
}

/// Build a term index for faster O(1) lookups
/// This creates a mapping from term -> (row_group_id, row_offset)
/// Useful when you have many point queries
pub fn build_term_index(path: &str) -> CoreResult<Vec<TermIndexEntry>> {
    if !Path::new(path).exists() {
        return Err(CoreError::Internal(format!("File not found: {}", path)));
    }

    let file = std::fs::File::open(path)
        .map_err(|e| CoreError::Internal(format!("Failed to open file '{}': {}", path, e)))?;

    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| CoreError::Internal(format!("Failed to create reader: {}", e)))?;

    let metadata = builder.metadata();
    let num_row_groups = metadata.num_row_groups();

    let mut index = Vec::new();

    // Read each Row Group and record term positions
    for rg_idx in 0..num_row_groups {
        let reader = ParquetRecordBatchReaderBuilder::try_new(
            std::fs::File::open(path)
                .map_err(|e| CoreError::Internal(format!("Failed to open file: {}", e)))?,
        )
        .map_err(|e| CoreError::Internal(format!("Failed to create reader: {}", e)))?
        .with_row_groups(vec![rg_idx])
        .build()
        .map_err(|e| CoreError::Internal(format!("Failed to build reader: {}", e)))?;

        let mut row_offset = 0;
        for batch_result in reader {
            let batch = batch_result
                .map_err(|e| CoreError::Internal(format!("Failed to read batch: {}", e)))?;

            let term_array = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| CoreError::Internal("Invalid term column".to_string()))?;

            for idx in 0..batch.num_rows() {
                let term = term_array.value(idx).to_string();
                index.push(TermIndexEntry {
                    term,
                    row_group_id: rg_idx,
                    row_offset,
                });
                row_offset += 1;
            }
        }
    }

    Ok(index)
}

/// Save term index to JSON file
pub fn save_term_index(index: &[TermIndexEntry], path: &str) -> CoreResult<()> {
    let json = serde_json::to_string_pretty(index)
        .map_err(|e| CoreError::Internal(format!("Failed to serialize index: {}", e)))?;

    // Ensure directory exists
    if let Some(parent) = std::path::Path::new(path).parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| CoreError::Internal(format!("Failed to create directory: {}", e)))?;
    }

    std::fs::write(path, json)
        .map_err(|e| CoreError::Internal(format!("Failed to write file: {}", e)))?;

    Ok(())
}

/// Load term index from JSON file
pub fn load_term_index(path: &str) -> CoreResult<Vec<TermIndexEntry>> {
    if !Path::new(path).exists() {
        return Err(CoreError::Internal(format!("File not found: {}", path)));
    }

    let json = std::fs::read_to_string(path)
        .map_err(|e| CoreError::Internal(format!("Failed to read file: {}", e)))?;

    let index: Vec<TermIndexEntry> = serde_json::from_str(&json)
        .map_err(|e| CoreError::Internal(format!("Failed to deserialize index: {}", e)))?;

    Ok(index)
}

/// Search using term index for O(1) lookup
pub fn search_with_term_index(
    parquet_path: &str,
    term: &str,
    index: &[TermIndexEntry],
) -> CoreResult<Option<PostingListRow>> {
    // Binary search in the index
    let entry = match index.binary_search_by(|e| e.term.as_str().cmp(term)) {
        Ok(idx) => &index[idx],
        Err(_) => return Ok(None),
    };

    // Read only the specific Row Group
    let file = std::fs::File::open(parquet_path)
        .map_err(|e| CoreError::Internal(format!("Failed to open file: {}", e)))?;

    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| CoreError::Internal(format!("Failed to create reader: {}", e)))?;

    let reader = builder
        .with_row_groups(vec![entry.row_group_id])
        .build()
        .map_err(|e| CoreError::Internal(format!("Failed to build reader: {}", e)))?;

    // Find the specific row
    let mut current_offset = 0;
    for batch_result in reader {
        let batch = batch_result
            .map_err(|e| CoreError::Internal(format!("Failed to read batch: {}", e)))?;

        if current_offset + batch.num_rows() > entry.row_offset {
            let row_idx = entry.row_offset - current_offset;

            let term_array = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| CoreError::Internal("Invalid term column".to_string()))?;

            let doc_freq_array = batch
                .column(1)
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| CoreError::Internal("Invalid doc_freq column".to_string()))?;

            let total_freq_array = batch
                .column(2)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| CoreError::Internal("Invalid total_freq column".to_string()))?;

            let doc_ids_list = batch
                .column(3)
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| CoreError::Internal("Invalid doc_ids column".to_string()))?;

            let term_freqs_list = batch
                .column(4)
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| CoreError::Internal("Invalid term_freqs column".to_string()))?;

            let positions_list = batch
                .column(5)
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| CoreError::Internal("Invalid positions column".to_string()))?;

            let row_term = term_array.value(row_idx).to_string();
            let doc_freq = doc_freq_array.value(row_idx);
            let total_freq = total_freq_array.value(row_idx);

            // Extract doc_ids
            let doc_ids_slice = doc_ids_list.value(row_idx);
            let doc_ids_values = doc_ids_slice
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| CoreError::Internal("Invalid doc_ids values".to_string()))?;
            let doc_ids: Vec<u32> = (0..doc_ids_values.len())
                .map(|i| doc_ids_values.value(i))
                .collect();

            // Extract term_freqs
            let term_freqs_slice = term_freqs_list.value(row_idx);
            let term_freqs_values = term_freqs_slice
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| CoreError::Internal("Invalid term_freqs values".to_string()))?;
            let term_freqs: Vec<u32> = (0..term_freqs_values.len())
                .map(|i| term_freqs_values.value(i))
                .collect();

            // Extract positions
            let positions_outer = positions_list.value(row_idx);
            let positions_outer_list = positions_outer
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| CoreError::Internal("Invalid positions outer list".to_string()))?;

            let mut positions = Vec::new();
            for pos_idx in 0..positions_outer_list.len() {
                let pos_inner = positions_outer_list.value(pos_idx);
                let pos_values = pos_inner
                    .as_any()
                    .downcast_ref::<UInt32Array>()
                    .ok_or_else(|| CoreError::Internal("Invalid position values".to_string()))?;
                let pos_vec: Vec<u32> =
                    (0..pos_values.len()).map(|i| pos_values.value(i)).collect();
                positions.push(pos_vec);
            }

            return Ok(Some(PostingListRow {
                term: row_term,
                doc_freq,
                total_freq,
                doc_ids,
                term_freqs,
                positions,
            }));
        }

        current_offset += batch.num_rows();
    }

    Ok(None)
}
