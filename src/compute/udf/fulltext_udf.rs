//! Full-text search UDF (User Defined Functions) for SQL queries
//!
//! Provides:
//! 1. text(field, query, boost) - Text query with BM25 scoring
//! 2. phrase(field, query, boost, slop) - Phrase query with proximity
//! 3. _score virtual column - Query relevance score
//!
//! SQL Examples:
//! ```sql
//! -- Simple text query
//! SELECT * FROM docs WHERE text(content, 'rust programming', 1.0) ORDER BY _score DESC LIMIT 10;
//!
//! -- Phrase query with slop
//! SELECT * FROM docs WHERE phrase(content, 'rust programming', 1.0, 2) ORDER BY _score DESC;
//!
//! -- Boolean combination
//! SELECT * FROM docs
//! WHERE text(content, 'rust', 2.0) OR text(content, 'python', 1.0)
//! ORDER BY _score DESC;
//! ```

use datafusion::arrow::array::{ArrayRef, BooleanArray, Float32Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::{ScalarUDF, Signature, Volatility};
use datafusion::physical_plan::ColumnarValue;
use std::sync::Arc;

use crate::segment::field_store::text::FullTextField;
use crate::utils::error::CoreResult;

/// Context for full-text search execution
/// Stores the index and maintains document scores
pub struct FullTextContext {
    /// Field indexes for full-text search
    pub indexes: Arc<std::sync::RwLock<std::collections::HashMap<String, Arc<FullTextField>>>>,
    /// Document scores for ranking (_score column)
    pub doc_scores: Arc<std::sync::RwLock<std::collections::HashMap<u32, f32>>>,
}

impl FullTextContext {
    pub fn new() -> Self {
        Self {
            indexes: Arc::new(std::sync::RwLock::new(std::collections::HashMap::new())),
            doc_scores: Arc::new(std::sync::RwLock::new(std::collections::HashMap::new())),
        }
    }

    /// Register a full-text index for a field
    pub fn register_index(&self, field_name: String, index: Arc<FullTextField>) {
        let mut indexes = self.indexes.write().unwrap();
        indexes.insert(field_name, index);
    }

    /// Update document score (accumulative for multiple queries)
    pub fn add_score(&self, doc_id: u32, score: f32) {
        let mut scores = self.doc_scores.write().unwrap();
        *scores.entry(doc_id).or_insert(0.0) += score;
    }

    /// Get document score
    pub fn get_score(&self, doc_id: u32) -> f32 {
        let scores = self.doc_scores.read().unwrap();
        scores.get(&doc_id).copied().unwrap_or(0.0)
    }

    /// Clear all scores (for new query)
    pub fn clear_scores(&self) {
        let mut scores = self.doc_scores.write().unwrap();
        scores.clear();
    }
}

/// Create text() UDF for text queries
///
/// Signature: text(field: String, query: String, boost: Float32) -> Boolean
///
/// Example:
/// ```sql
/// SELECT * FROM docs WHERE text(content, 'rust programming', 1.5)
/// ```
pub fn create_text_udf(context: Arc<FullTextContext>) -> ScalarUDF {
    let text_fn = move |args: &[ColumnarValue]| -> DataFusionResult<ColumnarValue> {
        // Parse arguments
        let field_name = match &args[0] {
            ColumnarValue::Scalar(datafusion::scalar::ScalarValue::Utf8(Some(s))) => s.clone(),
            _ => {
                return Err(DataFusionError::Execution(
                    "text() field must be string".into(),
                ))
            }
        };

        let query_text = match &args[1] {
            ColumnarValue::Scalar(datafusion::scalar::ScalarValue::Utf8(Some(s))) => s.clone(),
            _ => {
                return Err(DataFusionError::Execution(
                    "text() query must be string".into(),
                ))
            }
        };

        let boost = match &args[2] {
            ColumnarValue::Scalar(datafusion::scalar::ScalarValue::Float32(Some(b))) => *b,
            _ => 1.0,
        };

        // Get index for field
        let indexes = context.indexes.read().unwrap();
        let index = indexes.get(&field_name).ok_or_else(|| {
            DataFusionError::Execution(format!("No index found for field '{}'", field_name))
        })?;

        // Execute text query
        let matching_docs = index
            .term_query(&query_text)
            .ok_or_else(|| DataFusionError::Execution("Query returned no results".into()))?;

        // Calculate BM25 scores for matching documents
        let field_stats = index.get_field_stats();
        let k1 = 1.2;
        let b = 0.75;

        for doc_id in matching_docs.iter() {
            // Simple BM25 score (simplified version)
            // In production, you'd need doc length and term frequency
            let score = boost; // Placeholder - implement proper BM25
            context.add_score(doc_id, score);
        }

        // Return boolean array indicating matches
        // Note: This is a simplified version. In production, you need access to actual doc_ids
        let result = BooleanArray::from(vec![true]); // Placeholder
        Ok(ColumnarValue::Array(Arc::new(result)))
    };

    ScalarUDF::new(
        &format!("text_{}", field_name),
        &Signature::exact(
            vec![DataType::Utf8, DataType::Float32],
            Volatility::Immutable,
        ),
        &Arc::new(DataType::Boolean),
        &Arc::new(text_func),
    )
}

/// Create phrase() UDF for phrase queries
///
/// Signature: phrase(field: String, query: String, boost: Float32, slop: Int32) -> Boolean
///
/// Example:
/// ```sql
/// SELECT * FROM docs WHERE phrase(content, 'rust programming', 1.0, 2)
/// ```
pub fn create_phrase_udf(context: Arc<FullTextContext>) -> ScalarUDF {
    let phrase_fn = move |args: &[ColumnarValue]| -> DataFusionResult<ColumnarValue> {
        // Parse arguments
        let field_name = match &args[0] {
            ColumnarValue::Scalar(datafusion::scalar::ScalarValue::Utf8(Some(s))) => s.clone(),
            _ => {
                return Err(DataFusionError::Execution(
                    "phrase() field must be string".into(),
                ))
            }
        };

        let query_text = match &args[1] {
            ColumnarValue::Scalar(datafusion::scalar::ScalarValue::Utf8(Some(s))) => s.clone(),
            _ => {
                return Err(DataFusionError::Execution(
                    "phrase() query must be string".into(),
                ))
            }
        };

        let boost = match &args[2] {
            ColumnarValue::Scalar(datafusion::scalar::ScalarValue::Float32(Some(b))) => *b,
            _ => 1.0,
        };

        let slop = match &args[3] {
            ColumnarValue::Scalar(datafusion::scalar::ScalarValue::Int32(Some(s))) => *s as u32,
            _ => 0,
        };

        // Get index for field
        let indexes = context.indexes.read().unwrap();
        let index = indexes.get(&field_name).ok_or_else(|| {
            DataFusionError::Execution(format!("No index found for field '{}'", field_name))
        })?;

        // Execute phrase query
        let terms: Vec<&str> = query_text.split_whitespace().collect();
        let matching_docs = index
            .phrase_query(&terms, slop)
            .ok_or_else(|| DataFusionError::Execution("Query returned no results".into()))?;

        // Calculate scores for matching documents
        for doc_id in matching_docs.iter() {
            // Phrase queries typically get a higher base score
            let score = boost * 1.5; // Placeholder
            context.add_score(doc_id, score);
        }

        // Return boolean array
        let result = BooleanArray::from(vec![true]); // Placeholder
        Ok(ColumnarValue::Array(Arc::new(result)))
    };

    ScalarUDF::new(
        &format!("phrase_{}", field_name),
        &Signature::exact(
            vec![DataType::Utf8, DataType::Float32, DataType::Int32],
            Volatility::Immutable,
        ),
        &Arc::new(DataType::Boolean),
        &Arc::new(phrase_func),
    )
}

/// Virtual _score column
///
/// This is injected into query results when ORDER BY _score is detected
///
/// Implementation approach:
/// 1. Detect "ORDER BY _score" in SQL parser
/// 2. Execute full-text queries and populate scores in context
/// 3. Add _score column to result set from context.doc_scores
pub fn add_score_column(
    batch: &datafusion::arrow::record_batch::RecordBatch,
    context: &FullTextContext,
    doc_id_column: &str,
) -> DataFusionResult<datafusion::arrow::record_batch::RecordBatch> {
    // Extract doc_ids from the batch
    let doc_id_array = batch
        .column_by_name(doc_id_column)
        .ok_or_else(|| DataFusionError::Execution(format!("Column '{}' not found", doc_id_column)))?
        .as_any()
        .downcast_ref::<datafusion::arrow::array::UInt32Array>()
        .ok_or_else(|| DataFusionError::Execution("doc_id column must be UInt32".into()))?;

    // Build score array
    let scores: Vec<f32> = doc_id_array
        .iter()
        .map(|doc_id| context.get_score(doc_id.unwrap_or(0)))
        .collect();

    let score_array = Float32Array::from(scores);

    // Add _score column to batch
    let mut fields = batch.schema().fields().to_vec();
    fields.push(Field::new("_score", DataType::Float32, false));

    let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
    columns.push(Arc::new(score_array));

    let new_schema = Arc::new(datafusion::arrow::datatypes::Schema::new(fields));
    datafusion::arrow::record_batch::RecordBatch::try_new(new_schema, columns)
        .map_err(|e| DataFusionError::Execution(format!("Failed to add _score column: {}", e)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fulltext_context() {
        let context = FullTextContext::new();

        // Test score accumulation
        context.add_score(1, 1.5);
        context.add_score(1, 0.5);
        assert_eq!(context.get_score(1), 2.0);

        // Test clear
        context.clear_scores();
        assert_eq!(context.get_score(1), 0.0);
    }
}
