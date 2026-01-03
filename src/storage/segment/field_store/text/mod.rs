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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::schema::field::FieldOption;
    use crate::storage::segment::field_store::IndexWriter;
    use crate::utils::error::{CoreError, CoreResult};
    use datafusion::arrow::array::StringArray;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use std::sync::Arc;
    use tempfile::tempdir;

    const FIELD_NAME: &str = "content";

    fn text_field() -> FieldOption {
        FieldOption::Keyword {
            name: FIELD_NAME.to_string(),
            index: true,
            is_array: false,
            persist_option: None,
            case_sensitive: false,
            description: None,
            default_value: None,
            nullable: true,
        }
    }

    fn build_batch(values: &[&str]) -> CoreResult<RecordBatch> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            FIELD_NAME,
            DataType::Utf8,
            true,
        )]));
        let column = Arc::new(StringArray::from(values.to_vec()));
        RecordBatch::try_new(schema, vec![column]).map_err(|err| {
            CoreError::Internal(format!("Failed to build test record batch: {}", err))
        })
    }

    #[test]
    fn term_and_phrase_queries_work() -> CoreResult<()> {
        let field = text_field();
        let index = FullTextField::new(&field);
        let batch = build_batch(&[
            "rust programming language",
            "distributed storage routing",
            "rust query engine internals",
        ])?;

        index.write(&batch, 0)?;

        let rust_docs = index.term_query("rust").expect("rust term exists");
        assert_eq!(rust_docs.len(), 2);
        assert!(rust_docs.contains(0));
        assert!(rust_docs.contains(2));

        let phrase_docs = index
            .phrase_query(&["rust", "programming"], 0)
            .expect("phrase should match");
        assert_eq!(phrase_docs.len(), 1);
        assert!(phrase_docs.contains(0));

        Ok(())
    }

    #[test]
    fn boolean_queries_respect_clauses() -> CoreResult<()> {
        let field = text_field();
        let index = FullTextField::new(&field);
        let batch = build_batch(&[
            "rust storage engine",
            "rust planner module",
            "python storage engine",
        ])?;

        index.write(&batch, 0)?;

        let must_match = index
            .boolean_query(&["rust"], &["engine"], &[])
            .expect("must + should should match at least one doc");
        assert_eq!(must_match.len(), 1);
        assert!(must_match.contains(0));

        let filtered = index
            .boolean_query(&[], &["engine"], &["rust"])
            .expect("should clause with must_not should match doc2 only");
        assert_eq!(filtered.len(), 1);
        assert!(filtered.contains(2));

        Ok(())
    }

    #[test]
    fn persist_round_trip_keeps_terms() -> CoreResult<()> {
        let field = text_field();
        let index = FullTextField::new(&field);
        let batch = build_batch(&["calm database uses roaring", "fulltext persistence path"])?;

        index.write(&batch, 0)?;

        let dir = tempdir().expect("create tempdir");
        let field_path = dir.path().join("fulltext");
        let path_str = field_path.to_string_lossy().to_string();
        index.persist(&path_str)?;

        let reloaded = FullTextField::from_disk(&field, &path_str)?;
        let bitmap = reloaded
            .term_query("calm")
            .expect("term must exist after reload");
        assert_eq!(bitmap.len(), 1);
        assert!(bitmap.contains(0));
        assert_eq!(reloaded.get_field_stats().num_docs, 2);

        Ok(())
    }
}
