//! Full-text index field implementation
//!
//! Architecture (following Tantivy/Lucene design):
//! 1. Unified PostingList: term -> Vec<PostingEntry>
//!    - Each PostingEntry contains: doc_id + term_freq + positions
//!    - Single data structure, no separation of doc_ids and positions
//! 2. Memory: BTree<String, Vec<PostingEntry>>
//! 3. Disk: TreeWriter with custom PostingListSerializer
//! 4. Sequential access pattern for efficient iteration

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, RwLock};


use datafusion::arrow::array::{Array, ArrayRef, RecordBatch, StringArray};
use roaring::RoaringBitmap;

use crate::schema::field::{FieldOption, FieldType};
use crate::segment::field_store::IndexWriter;
use crate::utils::error::{CoreError, CoreResult};

use super::mmap_index::{self, MmapIndex};

use super::simple_analyzer::SimpleAnalyzer;

/// PostingEntry - Single entry in a posting list
/// Contains all information for one document occurrence
///
/// This matches Tantivy/Lucene design:
/// - doc_id: Document identifier
/// - term_freq: Number of times term appears in this document
/// - positions: Positions where term appears (for phrase queries)
#[derive(Debug, Clone)]
pub struct PostingEntry {
    pub doc_id: u32,
    pub term_freq: u32,
    pub positions: Vec<u32>,
}

impl PostingEntry {
    pub fn new(doc_id: u32, positions: Vec<u32>) -> Self {
        let term_freq = positions.len() as u32;
        Self {
            doc_id,
            term_freq,
            positions,
        }
    }
}

/// Term statistics (similar to Lucene's TermStatistics)
#[derive(Debug, Clone, Default)]
pub struct TermStats {
    pub doc_freq: u32,   // Number of documents containing this term
    pub total_freq: u64, // Total occurrences across all documents
}

/// Field statistics (similar to Lucene/ES CollectionStatistics)
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct FieldStats {
    pub num_docs: u32,            // Total documents in the field
    pub sum_doc_freq: u64,        // Sum of all term frequencies
    pub sum_total_term_freq: u64, // Sum of all term occurrences
    pub avg_field_length: f32,    // Average field length (for BM25)
}

impl Default for FieldStats {
    fn default() -> Self {
        Self {
            num_docs: 0,
            sum_doc_freq: 0,
            sum_total_term_freq: 0,
            avg_field_length: 0.0,
        }
    }
}

impl FieldStats {
    pub fn add_document(&mut self, field_length: u32) {
        self.num_docs += 1;
        self.sum_total_term_freq += field_length as u64;
        self.avg_field_length = self.sum_total_term_freq as f32 / self.num_docs as f32;
    }
}

/// Full-text indexed field
///
/// Architecture (Tantivy/Lucene style):
/// - Unified posting list: term -> Vec<PostingEntry>
/// - Each entry contains: doc_id + term_freq + positions
/// - Single BTree structure, no separation
/// - Efficient sequential iteration for queries and scoring
pub struct FullTextField {
    field: FieldOption,

    /// Memory-mapped index for persisted data
    index: Option<MmapIndex>,

    /// In-memory posting lists for newly added data (not yet persisted)
    temp_posting_lists: Arc<RwLock<HashMap<String, (TermStats, Vec<PostingEntry>)>>>,

    /// Field-level statistics
    /// For BM25 scoring (avgdl, etc.)
    field_stats: Arc<RwLock<FieldStats>>,

    /// Text analyzer
    analyzer: Arc<SimpleAnalyzer>,
}

impl FullTextField {
    /// Create new memory-based full-text index
    pub fn new(field: &FieldOption) -> Self {
        Self {
            field: field.clone(),
            index: None,
            temp_posting_lists: Arc::new(RwLock::new(HashMap::new())),
            field_stats: Arc::new(RwLock::new(FieldStats::default())),
            analyzer: Arc::new(SimpleAnalyzer::new()),
        }
    }

    /// Load from disk (mmap format)
    pub fn from_disk(field: &FieldOption, field_path: &str) -> CoreResult<Self> {
        use super::mmap_index::FIELD_STATS_FILE;
        use std::fs;

        let index = if Path::new(field_path).join(mmap_index::TERMS_FILE).exists() {
            log::debug!("📂 [FullTextField] Loading mmap index from {}", field_path);
            Some(MmapIndex::open(field_path)?)
        } else {
            log::debug!(
                "⚠️  [FullTextField] No mmap index found at {}, creating empty field",
                field_path
            );
            None
        };

        // Load field statistics
        let stats_path = Path::new(field_path).join(FIELD_STATS_FILE);
        let field_stats = if stats_path.exists() {
            log::debug!(
                "📂 [FullTextField] Loading field stats from {:?}",
                stats_path
            );
            let stats_json = fs::read_to_string(&stats_path)
                .map_err(|e| CoreError::Internal(format!("Failed to read field stats: {}", e)))?;
            let stats: FieldStats = serde_json::from_str(&stats_json).map_err(|e| {
                CoreError::Internal(format!("Failed to deserialize field stats: {}", e))
            })?;
            Arc::new(RwLock::new(stats))
        } else {
            log::warn!(
                "⚠️  [FullTextField] No field stats found at {:?}, using defaults",
                stats_path
            );
            Arc::new(RwLock::new(FieldStats::default()))
        };

        Ok(Self {
            field: field.clone(),
            index,
            temp_posting_lists: Arc::new(RwLock::new(HashMap::new())),
            field_stats,
            analyzer: Arc::new(SimpleAnalyzer::new()),
        })
    }

    /// Persist to disk using mmap format
    pub fn persist(&self, path: &str) -> CoreResult<Self> {
        use super::mmap_index::{MmapIndexWriter, FIELD_STATS_FILE};
        use std::fs;

        // Create directory if not exists
        fs::create_dir_all(path)
            .map_err(|e| CoreError::Internal(format!("Failed to create directory: {}", e)))?;

        let temp_postings = self.temp_posting_lists.read().unwrap();
        if !temp_postings.is_empty() {
            let mut writer = MmapIndexWriter::new(path)?;

            // Sort terms for deterministic order
            let mut sorted_terms: Vec<_> = temp_postings.keys().collect();
            sorted_terms.sort();

            for term in sorted_terms {
                let (_stats, postings) = &temp_postings[term];

                // Serialize postings to binary format
                let mut postings_data = Vec::new();
                for entry in postings {
                    postings_data.extend_from_slice(&entry.doc_id.to_be_bytes());
                    postings_data.extend_from_slice(&entry.term_freq.to_be_bytes());
                    postings_data.extend_from_slice(&(entry.positions.len() as u32).to_be_bytes());
                    for pos in &entry.positions {
                        postings_data.extend_from_slice(&pos.to_be_bytes());
                    }
                }
                // TODO: Add compression for postings_data

                writer.write_term(term, &postings_data)?;
            }

            writer.finish()?;
            log::debug!(
                "✅ [FullTextField] Persisted {} terms to mmap index at {}",
                temp_postings.len(),
                path
            );
        }

        // Write field statistics
        let field_stats = self.field_stats.read().unwrap();
        let stats_path = Path::new(path).join(FIELD_STATS_FILE);
        let stats_json = serde_json::to_string(&*field_stats)
            .map_err(|e| CoreError::Internal(format!("Failed to serialize field stats: {}", e)))?;
        fs::write(&stats_path, stats_json)
            .map_err(|e| CoreError::Internal(format!("Failed to write field stats: {}", e)))?;
        log::debug!(
            "✅ [FullTextField] Persisted field stats to {:?}",
            stats_path
        );

        // Reload from the persisted path to use the mmap index
        Self::from_disk(&self.field, path)
    }

    // ============ Query Interface (Full-text specific, not from IndexReader) ============

    /// Term query - finds documents containing the term
    /// Returns RoaringBitmap of matching document IDs
    pub fn term_query(&self, term: &str) -> Option<RoaringBitmap> {
        // Analyze query term
        let tokens = self.analyzer.analyzer_query(term);
        if tokens.is_empty() {
            return None;
        }
        let analyzed_term = &tokens[0].name;

        let mut final_bitmap = RoaringBitmap::new();

        // 1. Query the mmap index (persisted data)
        if let Some(index) = &self.index {
            // Binary search for the term in the term dictionary
            let mut low = 0;
            let mut high = index.num_terms();
            let mut mmap_bitmap = RoaringBitmap::new();

            while low < high {
                let mid = low + (high - low) / 2;
                if let Some(entry) = index.get_term_entry(mid) {
                    let current_term = index.get_term(entry);
                    match current_term.cmp(analyzed_term) {
                        std::cmp::Ordering::Less => low = mid + 1,
                        std::cmp::Ordering::Greater => high = mid,
                        std::cmp::Ordering::Equal => {
                            let postings_data = index.get_postings_data(entry);
                            // Deserialize postings
                            let mut cursor = std::io::Cursor::new(postings_data);
                            while let Ok(doc_id) = byteorder::ReadBytesExt::read_u32::<
                                byteorder::BigEndian,
                            >(&mut cursor)
                            {
                                mmap_bitmap.insert(doc_id);
                                // Skip term_freq and positions for now
                                let _ = byteorder::ReadBytesExt::read_u32::<byteorder::BigEndian>(
                                    &mut cursor,
                                ); // term_freq
                                if let Ok(pos_len) = byteorder::ReadBytesExt::read_u32::<
                                    byteorder::BigEndian,
                                >(&mut cursor)
                                {
                                    cursor.set_position(cursor.position() + (pos_len * 4) as u64);
                                } else {
                                    break;
                                }
                            }
                            break;
                        }
                    }
                } else {
                    break;
                }
            }
            final_bitmap |= &mmap_bitmap;
        }

        // 2. Query the in-memory temp posting lists (new data)
        let temp_postings = self.temp_posting_lists.read().unwrap();
        if let Some((_stats, postings)) = temp_postings.get(analyzed_term) {
            let temp_bitmap =
                RoaringBitmap::from_sorted_iter(postings.iter().map(|p| p.doc_id)).ok()?;
            final_bitmap |= &temp_bitmap;
        }

        if final_bitmap.is_empty() {
            None
        } else {
            Some(final_bitmap)
        }
    }

    /// Phrase query with slop (proximity search)
    ///
    /// # Arguments
    /// * `terms` - Phrase terms in order
    /// * `slop` - Maximum distance between terms (0 = exact phrase)
    ///
    /// # Example
    /// ```ignore
    /// // Exact phrase: "rust programming"
    /// index.phrase_query(&["rust", "programming"], 0);
    ///
    /// // Proximity: "rust" and "programming" within 2 words
    /// index.phrase_query(&["rust", "programming"], 2);
    /// ```
    pub fn phrase_query(&self, terms: &[&str], slop: u32) -> Option<RoaringBitmap> {
        // TODO: Add support for phrase queries on the mmap index.
        // This will require deserializing positions from the mmap data.

        if terms.is_empty() {
            return None;
        }

        // For now, only query in-memory temp data
        let temp_postings = self.temp_posting_lists.read().unwrap();
        if temp_postings.is_empty() {
            return None;
        }

        // Analyze terms
        let mut analyzed_terms = Vec::new();
        for &term in terms {
            let tokens = self.analyzer.analyzer_query(term);
            if tokens.is_empty() {
                return None;
            }
            analyzed_terms.push(tokens[0].name.clone());
        }

        // Get posting lists for all terms from in-memory data
        let mut term_postings = Vec::new();
        for term in &analyzed_terms {
            if let Some((_stats, postings)) = temp_postings.get(term) {
                term_postings.push(postings);
            } else {
                return None; // Term not found in-memory
            }
        }

        // Build position index from posting lists for efficient lookup
        let mut position_map: HashMap<(String, u32), &Vec<u32>> = HashMap::new();
        for (idx, term) in analyzed_terms.iter().enumerate() {
            for entry in term_postings[idx] {
                position_map.insert((term.clone(), entry.doc_id), &entry.positions);
            }
        }

        // Get candidate documents (docs containing all terms)
        let mut candidates =
            RoaringBitmap::from_sorted_iter(term_postings[0].iter().map(|p| p.doc_id)).ok()?;

        for postings in &term_postings[1..] {
            let bitmap = RoaringBitmap::from_sorted_iter(postings.iter().map(|p| p.doc_id)).ok()?;
            candidates &= bitmap;
        }

        // Check position constraints
        let mut result = RoaringBitmap::new();
        for doc_id in candidates.iter() {
            if self.check_phrase_positions(doc_id, &analyzed_terms, slop, &position_map) {
                result.insert(doc_id);
            }
        }

        if result.is_empty() {
            None
        } else {
            Some(result)
        }
    }

    fn check_phrase_positions(
        &self,
        doc_id: u32,
        terms: &[String],
        slop: u32,
        position_map: &HashMap<(String, u32), &Vec<u32>>,
    ) -> bool {
        // Get positions for all terms in this document
        let mut all_positions = Vec::new();
        for term in terms {
            if let Some(positions) = position_map.get(&(term.clone(), doc_id)) {
                all_positions.push(*positions);
            } else {
                return false;
            }
        }

        // Check if positions satisfy phrase constraint
        for &start_pos in all_positions[0] {
            let mut current_pos = start_pos;
            let mut matched = true;

            for positions in &all_positions[1..] {
                let min_pos = current_pos + 1;
                let max_pos = current_pos + 1 + slop;

                match positions
                    .iter()
                    .filter(|&&pos| pos >= min_pos && pos <= max_pos)
                    .min()
                {
                    Some(&pos) => current_pos = pos,
                    None => {
                        matched = false;
                        break;
                    }
                }
            }

            if matched {
                return true;
            }
        }

        false
    }

    /// Boolean query - combine multiple queries with AND/OR/NOT
    ///
    /// # Example
    /// ```ignore
    /// let rust_docs = index.term_query("rust").unwrap();
    /// let python_docs = index.term_query("python").unwrap();
    ///
    /// // AND: documents with both terms
    /// let and_result = &rust_docs & &python_docs;
    ///
    /// // OR: documents with either term
    /// let or_result = &rust_docs | &python_docs;
    ///
    /// // NOT: rust but not python
    /// let not_result = &rust_docs - &python_docs;
    /// ```
    pub fn boolean_query(
        &self,
        must: &[&str],     // All terms must match (AND)
        should: &[&str],   // At least one should match (OR)
        must_not: &[&str], // None of these should match (NOT)
    ) -> Option<RoaringBitmap> {
        let mut result: Option<RoaringBitmap> = None;

        // Process MUST clauses (AND)
        for &term in must {
            if let Some(bitmap) = self.term_query(term) {
                result = Some(match result {
                    Some(existing) => existing & bitmap,
                    None => bitmap,
                });
            } else {
                return None; // If any MUST term is missing, no results
            }
        }

        // Process SHOULD clauses (OR)
        if !should.is_empty() {
            let mut should_bitmap = RoaringBitmap::new();
            for &term in should {
                if let Some(bitmap) = self.term_query(term) {
                    should_bitmap |= bitmap;
                }
            }

            if !should_bitmap.is_empty() {
                result = Some(match result {
                    Some(existing) => existing & should_bitmap,
                    None => should_bitmap,
                });
            } else if result.is_none() {
                return None; // No matches in either MUST or SHOULD
            }
        }

        // Process MUST_NOT clauses (NOT)
        for &term in must_not {
            if let Some(bitmap) = self.term_query(term) {
                result = Some(match result {
                    Some(existing) => existing - bitmap,
                    None => return None, // Nothing to exclude from
                });
            }
        }

        result
    }

    /// Get term statistics (for BM25 scoring)
    pub fn get_term_stats(&self, term: &str) -> Option<TermStats> {
        // TODO: Get term stats from mmap index as well.
        let tokens = self.analyzer.analyzer_query(term);
        if tokens.is_empty() {
            return None;
        }
        let analyzed_term = &tokens[0].name;
        let temp_postings = self.temp_posting_lists.read().unwrap();
        temp_postings
            .get(analyzed_term)
            .map(|(stats, _postings)| stats.clone())
    }

    /// Get field statistics (for BM25 scoring)
    pub fn get_field_stats(&self) -> FieldStats {
        self.field_stats.read().unwrap().clone()
    }

    /// Get posting list for a term (doc_ids + positions)
    /// Returns unified posting entries from the posting list
    pub fn get_postings(&self, term: &str) -> Option<Vec<PostingEntry>> {
        // TODO: Get postings from mmap index as well.
        let tokens = self.analyzer.analyzer_query(term);
        if tokens.is_empty() {
            return None;
        }
        let analyzed_term = &tokens[0].name;

        let temp_postings = self.temp_posting_lists.read().unwrap();
        temp_postings
            .get(analyzed_term)
            .map(|(_stats, postings)| postings.clone())
    }
}

// ============ IndexWriter Implementation (for batch data ingestion) ============

impl IndexWriter for FullTextField {
    fn write(&self, data: &RecordBatch, start_id: u32) -> CoreResult<()> {
        let Some(arr) = data.column_by_name(self.field.name()) else {
            return Ok(());
        };

        let text_array = arr
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| CoreError::Internal("Expected StringArray".to_string()))?;

        // Build unified posting lists for the new data
        let mut term_postings: HashMap<String, HashMap<u32, Vec<u32>>> = HashMap::new();
        let mut term_frequencies: HashMap<String, u64> = HashMap::new();

        for row_idx in 0..text_array.len() {
            if text_array.is_null(row_idx) {
                continue;
            }

            let text = text_array.value(row_idx);
            let doc_id = start_id + row_idx as u32;

            // Tokenize
            let tokens = self.analyzer.analyzer_index(text);
            let field_length = tokens.len() as u32;

            // Update field statistics
            self.field_stats.write().unwrap().add_document(field_length);

            for token in tokens {
                let term = token.name.clone();
                let position = token.index as u32;

                term_postings
                    .entry(term.clone())
                    .or_default()
                    .entry(doc_id)
                    .or_default()
                    .push(position);

                *term_frequencies.entry(term).or_default() += 1;
            }
        }

        // Merge into the main in-memory temp_posting_lists
        let mut temp_posting_lists = self.temp_posting_lists.write().unwrap();

        for (term, doc_positions_map) in term_postings {
            let (stats, postings) = temp_posting_lists.entry(term.clone()).or_insert_with(|| {
                let total_freq = *term_frequencies.get(&term).unwrap_or(&0);
                (
                    TermStats {
                        doc_freq: 0,
                        total_freq,
                    },
                    Vec::new(),
                )
            });

            let new_postings: Vec<PostingEntry> = doc_positions_map
                .into_iter()
                .map(|(doc_id, positions)| PostingEntry::new(doc_id, positions))
                .collect();

            stats.doc_freq += new_postings.len() as u32;
            postings.extend(new_postings);
            postings.sort_by_key(|e| e.doc_id);
            postings.dedup_by_key(|e| e.doc_id);
        }

        Ok(())
    }

    fn name(&self) -> &str {
        self.field.name()
    }

    fn field_type(&self) -> FieldType {
        self.field.field_type()
    }

    fn mget_internal_id(&self, _column: &ArrayRef) -> Vec<u32> {
        // Full-text fields don't support direct lookup
        Vec::new()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

// Note: FullTextField does NOT implement IndexReader
// Full-text search has its own query interface (term_query, phrase_query, boolean_query)
// This is by design - text search is fundamentally different from scalar equality/range queries
