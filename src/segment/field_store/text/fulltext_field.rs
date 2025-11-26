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
use std::sync::{Arc, RwLock};

use datafusion::arrow::array::{Array, ArrayRef, RecordBatch, StringArray};
use mem_btree::BTree;
use roaring::RoaringBitmap;

use crate::schema::field::{FieldOption, FieldType};
use crate::segment::field_store::IndexWriter;
use crate::utils::error::{CoreError, CoreResult};

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

    /// Unified posting list index: term -> (stats, postings)
    /// This is the ONLY index structure, containing all data:
    /// - Term statistics (doc_freq, total_freq) for BM25
    /// - Posting entries: doc_ids (sorted) + term_freq + positions
    ///
    /// Memory: BTree<String, (TermStats, Vec<PostingEntry>)>
    /// Disk: TreeReader with PostingListSerializer (future)
    posting_lists: Arc<RwLock<BTree<String, (TermStats, Vec<PostingEntry>)>>>,

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
            posting_lists: Arc::new(RwLock::new(BTree::new(128))),
            field_stats: Arc::new(RwLock::new(FieldStats::default())),
            analyzer: Arc::new(SimpleAnalyzer::new()),
        }
    }

    /// Load from disk (Parquet format)
    pub fn from_disk(field: &FieldOption, field_path: &str) -> CoreResult<Self> {
        use super::posting_list_parquet::read_posting_lists;
        use std::fs;

        let posting_path = format!("{}/posting_lists.parquet", field_path);
        let stats_path = format!("{}/field_stats.json", field_path);

        // Load posting lists from Parquet
        let posting_lists = if std::path::Path::new(&posting_path).exists() {
            log::debug!(
                "📂 [FullTextField] Loading posting lists from {}",
                posting_path
            );

            let rows = read_posting_lists(&posting_path)?;
            log::debug!("✅ [FullTextField] Loaded {} terms", rows.len());

            // Convert rows to BTree
            let mut btree = BTree::new(128);
            for row in rows {
                let (stats, postings) = row.to_stats_and_postings();
                btree.put(row.term, (stats, postings));
            }

            Arc::new(RwLock::new(btree))
        } else {
            log::debug!(
                "⚠️  [FullTextField] No posting lists found at {}, creating empty index",
                posting_path
            );
            Arc::new(RwLock::new(BTree::new(128)))
        };

        // Load field statistics
        let field_stats = if std::path::Path::new(&stats_path).exists() {
            log::debug!("📂 [FullTextField] Loading field stats from {}", stats_path);
            let stats_json = fs::read_to_string(&stats_path)
                .map_err(|e| CoreError::Internal(format!("Failed to read field stats: {}", e)))?;
            let stats: FieldStats = serde_json::from_str(&stats_json).map_err(|e| {
                CoreError::Internal(format!("Failed to deserialize field stats: {}", e))
            })?;
            Arc::new(RwLock::new(stats))
        } else {
            log::warn!(
                "⚠️  [FullTextField] No field stats found at {}, using defaults",
                stats_path
            );
            Arc::new(RwLock::new(FieldStats::default()))
        };

        Ok(Self {
            field: field.clone(),
            posting_lists,
            field_stats,
            analyzer: Arc::new(SimpleAnalyzer::new()),
        })
    }

    /// Persist to disk using Parquet format
    pub fn persist(&self, path: &str) -> CoreResult<Self> {
        use super::posting_list_parquet::{write_posting_lists, PostingListRow};
        use std::fs;

        // Create directory if not exists
        if let Some(parent) = std::path::Path::new(path).parent() {
            fs::create_dir_all(parent)
                .map_err(|e| CoreError::Internal(format!("Failed to create directory: {}", e)))?;
        }

        // Collect all posting lists into rows
        let posting_lists = self.posting_lists.read().unwrap();
        let mut rows: Vec<PostingListRow> = Vec::new();

        for item in posting_lists.iter() {
            let (term, (stats, postings), _ttl) = item.as_ref();
            rows.push(PostingListRow::new(term.clone(), stats, postings));
        }

        // Sort by term (required for binary search)
        rows.sort_by(|a, b| a.term.cmp(&b.term));

        if !rows.is_empty() {
            // Write posting lists to Parquet
            let posting_path = format!("{}/posting_lists.parquet", path);
            write_posting_lists(&rows, &posting_path)?;
            log::debug!(
                "✅ [FullTextField] Persisted {} terms to {}",
                rows.len(),
                posting_path
            );
        }

        // Write field statistics (use serde_json for simplicity)
        let field_stats = self.field_stats.read().unwrap();
        let stats_path = format!("{}/field_stats.json", path);
        let stats_json = serde_json::to_string(&*field_stats)
            .map_err(|e| CoreError::Internal(format!("Failed to serialize field stats: {}", e)))?;
        fs::write(&stats_path, stats_json)
            .map_err(|e| CoreError::Internal(format!("Failed to write field stats: {}", e)))?;

        log::debug!("✅ [FullTextField] Persisted field stats to {}", stats_path);

        Ok(Self {
            field: self.field.clone(),
            posting_lists: self.posting_lists.clone(),
            field_stats: self.field_stats.clone(),
            analyzer: self.analyzer.clone(),
        })
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
        let posting_lists = self.posting_lists.read().unwrap();

        // Extract doc_ids from posting list (stats, postings)
        if let Some((_stats, postings)) = posting_lists.get(analyzed_term) {
            let bitmap = RoaringBitmap::from_sorted_iter(postings.iter().map(|p| p.doc_id)).ok()?;
            Some(bitmap)
        } else {
            None
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
        if terms.is_empty() {
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

        let posting_lists = self.posting_lists.read().unwrap();

        // Get posting lists for all terms
        let mut term_postings = Vec::new();
        for term in &analyzed_terms {
            if let Some((_stats, postings)) = posting_lists.get(term) {
                term_postings.push(postings);
            } else {
                return None; // Term not found
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
        let tokens = self.analyzer.analyzer_query(term);
        if tokens.is_empty() {
            return None;
        }
        let analyzed_term = &tokens[0].name;
        let posting_lists = self.posting_lists.read().unwrap();
        posting_lists
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
        let tokens = self.analyzer.analyzer_query(term);
        if tokens.is_empty() {
            return None;
        }
        let analyzed_term = &tokens[0].name;

        let posting_lists = self.posting_lists.read().unwrap();
        posting_lists
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

        // Build unified posting lists
        // term -> [(doc_id, [positions])]
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

            // Build unified posting list structure
            for token in tokens {
                let term = token.name.clone();
                let position = token.index as u32;

                // Add to posting list
                let doc_positions = term_postings
                    .entry(term.clone())
                    .or_insert_with(HashMap::new)
                    .entry(doc_id)
                    .or_insert_with(Vec::new);

                doc_positions.push(position);

                // Update term frequencies
                *term_frequencies.entry(term).or_insert(0) += 1;
            }
        }

        // Convert to (TermStats, Vec<PostingEntry>) and update posting_lists
        let mut posting_lists = self.posting_lists.write().unwrap();

        for (term, doc_positions_map) in term_postings {
            // Create PostingEntry for each document
            let mut new_entries: Vec<PostingEntry> = doc_positions_map
                .into_iter()
                .map(|(doc_id, positions)| PostingEntry::new(doc_id, positions))
                .collect();

            // Sort by doc_id (required for efficient iteration)
            new_entries.sort_by_key(|e| e.doc_id);

            // Merge with existing posting list if present
            let merged_postings = if let Some((old_stats, existing)) = posting_lists.get(&term) {
                let mut merged = existing.clone();
                merged.extend(new_entries);
                merged.sort_by_key(|e| e.doc_id);
                // Deduplicate by doc_id (merge positions if same doc)
                merged.dedup_by(|a, b| {
                    if a.doc_id == b.doc_id {
                        b.positions.extend(&a.positions);
                        b.positions.sort_unstable();
                        b.term_freq = b.positions.len() as u32;
                        true
                    } else {
                        false
                    }
                });

                // Update term statistics
                let new_stats = TermStats {
                    doc_freq: merged.len() as u32,
                    total_freq: old_stats.total_freq + *term_frequencies.get(&term).unwrap_or(&0),
                };
                (new_stats, merged)
            } else {
                // New term, create fresh statistics
                let stats = TermStats {
                    doc_freq: new_entries.len() as u32,
                    total_freq: *term_frequencies.get(&term).unwrap_or(&0),
                };
                (stats, new_entries)
            };

            posting_lists.put(term.clone(), merged_postings);
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
