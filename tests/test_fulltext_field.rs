//! FullTextField integration test
//!
//! Tests the new architecture following KeywordField pattern

use calm::schema::field::{FieldOption, FieldType};
use calm::segment::field_store::text::{FullTextField, SimpleAnalyzer};
use calm::segment::field_store::{IndexReader, IndexWriter};
use datafusion::arrow::array::{RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::scalar::ScalarValue;
use std::sync::Arc;

#[test]
fn test_fulltext_field_basic() {
    // Create field
    let field_opt = FieldOption::new("content", FieldType::Text, true, true);
    let index = FullTextField::new(&field_opt);

    // Create test data
    let schema = Schema::new(vec![Field::new("content", DataType::Utf8, true)]);

    let content = StringArray::from(vec![
        "the quick brown fox",
        "the lazy dog",
        "quick brown dog",
    ]);

    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(content)]).unwrap();

    // Write data
    index.write(&batch, 0).unwrap();

    // Test term query
    let result = index.term_query("quick");
    assert!(result.is_some());
    let bitmap = result.unwrap();
    assert_eq!(bitmap.len(), 2); // docs 0, 2
    assert!(bitmap.contains(0));
    assert!(bitmap.contains(2));

    // Test another term
    let result = index.term_query("dog");
    assert!(result.is_some());
    let bitmap = result.unwrap();
    assert_eq!(bitmap.len(), 2); // docs 1, 2
    assert!(bitmap.contains(1));
    assert!(bitmap.contains(2));
}

#[test]
fn test_fulltext_phrase_query() {
    let field_opt = FieldOption::new("content", FieldType::Text, true, true);
    let index = FullTextField::new(&field_opt);

    let schema = Schema::new(vec![Field::new("content", DataType::Utf8, true)]);

    let content = StringArray::from(vec![
        "the quick brown fox jumps",
        "quick brown dog runs",
        "the brown fox sleeps",
    ]);

    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(content)]).unwrap();

    index.write(&batch, 0).unwrap();

    // Phrase query: "quick brown"
    let result = index.phrase_query(
        &["quick".to_string(), "brown".to_string()],
        0, // exact match, no slop
    );

    assert!(result.is_some());
    let bitmap = result.unwrap();
    assert_eq!(bitmap.len(), 2); // docs 0, 1
    assert!(bitmap.contains(0));
    assert!(bitmap.contains(1));

    // Phrase query: "brown fox"
    let result = index.phrase_query(&["brown".to_string(), "fox".to_string()], 0);

    assert!(result.is_some());
    let bitmap = result.unwrap();
    assert_eq!(bitmap.len(), 2); // docs 0, 2
    assert!(bitmap.contains(0));
    assert!(bitmap.contains(2));
}

#[test]
fn test_fulltext_phrase_with_slop() {
    let field_opt = FieldOption::new("content", FieldType::Text, true, true);
    let index = FullTextField::new(&field_opt);

    let schema = Schema::new(vec![Field::new("content", DataType::Utf8, true)]);

    let content = StringArray::from(vec![
        "the quick brown fox",
        "the very quick brown fox",
        "brown lazy fox",
    ]);

    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(content)]).unwrap();

    index.write(&batch, 0).unwrap();

    // Phrase query: "quick fox" with slop=1 (允许中间有1个词)
    let result = index.phrase_query(&["quick".to_string(), "fox".to_string()], 1);

    assert!(result.is_some());
    let bitmap = result.unwrap();
    assert_eq!(bitmap.len(), 1); // only doc 0 (quick brown fox)
    assert!(bitmap.contains(0));

    // With slop=2, should match doc 1 as well (quick very brown fox)
    let result = index.phrase_query(&["quick".to_string(), "fox".to_string()], 2);

    assert!(result.is_some());
    let bitmap = result.unwrap();
    assert_eq!(bitmap.len(), 2); // docs 0, 1
}

#[test]
fn test_fulltext_indexreader_trait() {
    let field_opt = FieldOption::new("content", FieldType::Text, true, true);
    let index = FullTextField::new(&field_opt);

    let schema = Schema::new(vec![Field::new("content", DataType::Utf8, true)]);

    let content = StringArray::from(vec![
        "rust programming",
        "python coding",
        "rust development",
    ]);

    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(content)]).unwrap();

    index.write(&batch, 0).unwrap();

    // Test query_eq (term query via trait)
    let result = index
        .query_eq(&ScalarValue::Utf8(Some("rust".to_string())))
        .unwrap();
    assert_eq!(result.len(), 2); // docs 0, 2

    // Test query_in (multiple terms)
    let result = index
        .query_in(&[
            ScalarValue::Utf8(Some("rust".to_string())),
            ScalarValue::Utf8(Some("python".to_string())),
        ])
        .unwrap();
    assert_eq!(result.len(), 3); // all docs match
}

#[test]
fn test_fulltext_persist() {
    use tempfile::TempDir;

    let temp_dir = TempDir::new().unwrap();
    let index_path = temp_dir.path().join("fulltext_index");

    // Create and populate index
    let field_opt = FieldOption::new("content", FieldType::Text, true, true);
    let index = FullTextField::new(&field_opt);

    let schema = Schema::new(vec![Field::new("content", DataType::Utf8, true)]);

    let content = StringArray::from(vec![
        "rust programming language",
        "system programming with rust",
        "learn rust today",
    ]);

    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(content)]).unwrap();

    index.write(&batch, 0).unwrap();

    // Persist to disk
    let persisted = index.persist(index_path.to_str().unwrap()).unwrap();

    // Query from disk index
    let result = persisted.term_query("rust");
    assert!(result.is_some());
    let bitmap = result.unwrap();
    assert_eq!(bitmap.len(), 3); // all docs contain "rust"
}
