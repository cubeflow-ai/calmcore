//! Full-text search index implementation
//!
//! Architecture (following Tantivy/Lucene design):
//! - **Unified PostingList**: term -> (TermStats, Vec<PostingEntry>)
//! - **Single BTree structure**: Contains all data (doc_ids, term_freq, positions, stats)
//! - **Memory**: BTree<String, (TermStats, Vec<PostingEntry>)>
//! - **Disk**: Parquet with columnar compression (ZSTD + Dictionary encoding)
//! - **Custom query interface**: Does not implement IndexReader (by design)

mod fulltext_field;
mod posting_list_parquet;
mod simple_analyzer;

// Re-export public API
pub use fulltext_field::{FieldStats, FullTextField, PostingEntry, TermStats};
pub use posting_list_parquet::{
    build_term_index, get_parquet_metadata, load_term_index, read_posting_lists, save_term_index,
    search_term, search_term_docids_only, search_terms_batch, search_terms_batch_parallel,
    search_with_term_index, write_posting_lists, ParquetMetadata, PostingListRow, TermIndexEntry,
};
pub use simple_analyzer::{SimpleAnalyzer, Token};
