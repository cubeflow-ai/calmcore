//! Full-text search UDF (User Defined Functions) for SQL queries
//!
//! Provides:
//! 1. `text(field, query, boost)` - Text query returning boolean mask
//! 2. `phrase(field, query, boost, slop)` - Phrase query with optional slop
//! 3. `_score` helper utilities (future work)
//!
//! The current implementation focuses on wiring the UDFs into DataFusion so
//! that SQL queries using the `text()` / `phrase()` helpers can be planned and
//! executed without hitting "Invalid function" errors. Scoring is currently a
//! simple heuristic (boost value per match). More advanced BM25 scoring can be
//! layered on top once the storage layer exposes doc_id aligned metadata.

use datafusion::arrow::array::{
    Array, ArrayRef, BooleanBuilder, Float32Array, LargeStringArray, StringArray,
};
use datafusion::arrow::compute::{
    concat_batches, lexsort_to_indices, take, SortColumn, SortOptions,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::{create_udf, ScalarUDF, Volatility};
use datafusion::physical_plan::{ColumnarValue, RecordBatchStream, SendableRecordBatchStream};
use datafusion::prelude::SessionContext;
use datafusion::scalar::ScalarValue;
use futures::Stream;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::{
    compute::sql_normalizer::{ScoreColumnPlacement, ScoreConfig, ScoreLimit, ScoreOrder},
    segment::field_store::text::FullTextField,
};

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
pub fn create_text_udf(context: Arc<FullTextContext>) -> ScalarUDF {
    let fun = Arc::new(
        move |args: &[ColumnarValue]| -> DataFusionResult<ColumnarValue> {
            if args.len() != 3 {
                return Err(DataFusionError::Execution(
                    "text(field, query, boost) expects exactly 3 arguments".into(),
                ));
            }

            let field_values = string_column_from_arg(&args[0], "field")?;
            let query_text = scalar_string_from_arg(&args[1], "query")?;
            let boost = scalar_f32_from_arg(&args[2], 1.0, "boost")?;
            let query_terms = tokenize(&query_text);

            let mut builder = BooleanBuilder::with_capacity(field_values.len());
            for (row_idx, maybe_value) in field_values.iter().enumerate() {
                if let Some(value) = maybe_value {
                    let matches = match_text(value, &query_terms);
                    builder.append_value(matches);
                    if matches {
                        context.add_score(row_idx as u32, boost);
                    }
                } else {
                    builder.append_value(false);
                }
            }

            Ok(ColumnarValue::Array(Arc::new(builder.finish())))
        },
    );

    create_udf(
        "text",
        vec![DataType::Utf8, DataType::Utf8, DataType::Float32],
        DataType::Boolean,
        Volatility::Immutable,
        fun,
    )
}

/// Create phrase() UDF for phrase queries
pub fn create_phrase_udf(context: Arc<FullTextContext>) -> ScalarUDF {
    let fun = Arc::new(
        move |args: &[ColumnarValue]| -> DataFusionResult<ColumnarValue> {
            if args.len() != 4 {
                return Err(DataFusionError::Execution(
                    "phrase(field, query, boost, slop) expects exactly 4 arguments".into(),
                ));
            }

            let field_values = string_column_from_arg(&args[0], "field")?;
            let phrase_text = scalar_string_from_arg(&args[1], "query")?;
            let boost = scalar_f32_from_arg(&args[2], 1.0, "boost")?;
            let _slop = scalar_i32_from_arg(&args[3], 0, "slop")?;

            let mut builder = BooleanBuilder::with_capacity(field_values.len());
            let needle = phrase_text.to_lowercase();
            for (row_idx, maybe_value) in field_values.iter().enumerate() {
                if let Some(value) = maybe_value {
                    let haystack = value.to_lowercase();
                    let matches = haystack.contains(&needle);
                    builder.append_value(matches);
                    if matches {
                        context.add_score(row_idx as u32, boost * 1.5);
                    }
                } else {
                    builder.append_value(false);
                }
            }

            Ok(ColumnarValue::Array(Arc::new(builder.finish())))
        },
    );

    create_udf(
        "phrase",
        vec![
            DataType::Utf8,
            DataType::Utf8,
            DataType::Float32,
            DataType::Int32,
        ],
        DataType::Boolean,
        Volatility::Immutable,
        fun,
    )
}

/// Register both text() and phrase() UDFs on the provided context and return the shared FT context
pub fn register_fulltext_udfs(ctx: &SessionContext) -> Arc<FullTextContext> {
    let ft_context = Arc::new(FullTextContext::new());
    ctx.register_udf(create_text_udf(ft_context.clone()));
    ctx.register_udf(create_phrase_udf(ft_context.clone()));
    ft_context
}

fn string_column_from_arg(arg: &ColumnarValue, label: &str) -> DataFusionResult<StringArray> {
    match arg {
        ColumnarValue::Array(array) => match array.data_type() {
            DataType::Utf8 => array
                .as_any()
                .downcast_ref::<StringArray>()
                .cloned()
                .ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "Failed to downcast {} argument to StringArray",
                        label
                    ))
                }),
            DataType::LargeUtf8 => array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .map(|arr| {
                    let owned: Vec<Option<String>> =
                        arr.iter().map(|v| v.map(|s| s.to_string())).collect();
                    StringArray::from(owned)
                })
                .ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "Failed to downcast {} argument to LargeStringArray",
                        label
                    ))
                }),
            other => Err(DataFusionError::Execution(format!(
                "{} column must be Utf8, got {:?}",
                label, other
            ))),
        },
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(value)))
        | ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(value))) => {
            Ok(StringArray::from(vec![Some(value.to_string())]))
        }
        ColumnarValue::Scalar(ScalarValue::Utf8(None))
        | ColumnarValue::Scalar(ScalarValue::LargeUtf8(None))
        | ColumnarValue::Scalar(ScalarValue::Null) => {
            Ok(StringArray::from(vec![Option::<String>::None]))
        }
        other => Err(DataFusionError::Execution(format!(
            "{} column must be Utf8, got {:?}",
            label, other
        ))),
    }
}

fn scalar_string_from_arg(arg: &ColumnarValue, label: &str) -> DataFusionResult<String> {
    match arg {
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(value))) => Ok(value.clone()),
        ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(value))) => Ok(value.clone()),
        ColumnarValue::Scalar(ScalarValue::Utf8(None))
        | ColumnarValue::Scalar(ScalarValue::LargeUtf8(None))
        | ColumnarValue::Scalar(ScalarValue::Null) => Err(DataFusionError::Execution(format!(
            "{} cannot be NULL",
            label
        ))),
        ColumnarValue::Array(array) => match array.data_type() {
            DataType::Utf8 => {
                let string_array = array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| DataFusionError::Execution(format!("{} must be Utf8", label)))?;
                if string_array.is_empty() {
                    return Err(DataFusionError::Execution(format!(
                        "{} column must have at least one value",
                        label
                    )));
                }
                Ok(string_array.value(0).to_string())
            }
            DataType::LargeUtf8 => {
                let string_array = array
                    .as_any()
                    .downcast_ref::<LargeStringArray>()
                    .ok_or_else(|| {
                        DataFusionError::Execution(format!("{} must be LargeUtf8", label))
                    })?;
                if string_array.is_empty() {
                    return Err(DataFusionError::Execution(format!(
                        "{} column must have at least one value",
                        label
                    )));
                }
                Ok(string_array.value(0).to_string())
            }
            other => Err(DataFusionError::Execution(format!(
                "{} must be Utf8, got {:?}",
                label, other
            ))),
        },
        other => Err(DataFusionError::Execution(format!(
            "{} must be Utf8, got {:?}",
            label, other
        ))),
    }
}

fn scalar_f32_from_arg(arg: &ColumnarValue, default: f32, label: &str) -> DataFusionResult<f32> {
    match arg {
        ColumnarValue::Scalar(ScalarValue::Float32(Some(v))) => Ok(*v),
        ColumnarValue::Scalar(ScalarValue::Float64(Some(v))) => Ok(*v as f32),
        ColumnarValue::Scalar(ScalarValue::Int64(Some(v))) => Ok(*v as f32),
        ColumnarValue::Scalar(ScalarValue::Int32(Some(v))) => Ok(*v as f32),
        ColumnarValue::Scalar(ScalarValue::Float32(None))
        | ColumnarValue::Scalar(ScalarValue::Float64(None))
        | ColumnarValue::Scalar(ScalarValue::Int64(None))
        | ColumnarValue::Scalar(ScalarValue::Int32(None))
        | ColumnarValue::Scalar(ScalarValue::Null) => Ok(default),
        ColumnarValue::Array(_) => Err(DataFusionError::Execution(format!(
            "{} must be a scalar value",
            label
        ))),
        other => Err(DataFusionError::Execution(format!(
            "Unsupported {} scalar {:?}",
            label, other
        ))),
    }
}

fn scalar_i32_from_arg(arg: &ColumnarValue, default: i32, label: &str) -> DataFusionResult<i32> {
    match arg {
        ColumnarValue::Scalar(ScalarValue::Int32(Some(v))) => Ok(*v),
        ColumnarValue::Scalar(ScalarValue::Int64(Some(v))) => Ok(*v as i32),
        ColumnarValue::Scalar(ScalarValue::UInt32(Some(v))) => Ok(*v as i32),
        ColumnarValue::Scalar(ScalarValue::UInt64(Some(v))) => Ok(*v as i32),
        ColumnarValue::Scalar(ScalarValue::Int32(None))
        | ColumnarValue::Scalar(ScalarValue::Int64(None))
        | ColumnarValue::Scalar(ScalarValue::UInt32(None))
        | ColumnarValue::Scalar(ScalarValue::UInt64(None))
        | ColumnarValue::Scalar(ScalarValue::Null) => Ok(default),
        ColumnarValue::Array(_) => Err(DataFusionError::Execution(format!(
            "{} must be a scalar",
            label
        ))),
        other => Err(DataFusionError::Execution(format!(
            "Unsupported {} scalar {:?}",
            label, other
        ))),
    }
}

fn tokenize(input: &str) -> Vec<String> {
    input
        .split_whitespace()
        .map(|token| {
            token
                .trim_matches(|c: char| !c.is_alphanumeric())
                .to_lowercase()
        })
        .filter(|token| !token.is_empty())
        .collect()
}

fn match_text(value: &str, tokens: &[String]) -> bool {
    if tokens.is_empty() {
        return false;
    }
    let haystack = value.to_lowercase();
    tokens.iter().all(|token| haystack.contains(token))
}

/// Virtual _score column injector
fn inject_score_column(
    batch: &RecordBatch,
    context: &FullTextContext,
    placement: &ScoreColumnPlacement,
) -> DataFusionResult<RecordBatch> {
    let scores: Vec<f32> = (0..batch.num_rows())
        .map(|row_idx| context.get_score(row_idx as u32))
        .collect();
    let score_array: ArrayRef = Arc::new(Float32Array::from(scores));

    match placement {
        ScoreColumnPlacement::Append { field_name } => {
            let mut fields = batch.schema().fields().to_vec();
            fields.push(Arc::new(Field::new(field_name, DataType::Float32, false)));

            let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
            columns.push(score_array);

            let new_schema = Arc::new(Schema::new(fields));
            RecordBatch::try_new(new_schema, columns).map_err(|e| {
                DataFusionError::Execution(format!("Failed to append _score column: {}", e))
            })
        }
        ScoreColumnPlacement::Replace { index, .. } => {
            if *index >= batch.num_columns() {
                return Err(DataFusionError::Execution(format!(
                    "_score alias index {} out of bounds ({} columns)",
                    index,
                    batch.num_columns()
                )));
            }

            let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
            columns[*index] = score_array;
            RecordBatch::try_new(batch.schema(), columns).map_err(|e| {
                DataFusionError::Execution(format!("Failed to replace _score column: {}", e))
            })
        }
    }
}

pub fn build_score_stream(
    inner: SendableRecordBatchStream,
    context: Arc<FullTextContext>,
    score: &ScoreConfig,
) -> SendableRecordBatchStream {
    let placement = score
        .placement
        .clone()
        .unwrap_or(ScoreColumnPlacement::Append {
            field_name: "_score".to_string(),
        });
    let column_name = placement.field_name().to_string();
    let stream = wrap_stream_with_scores(inner, context, placement.clone());

    if let Some(order) = score.order {
        Box::pin(ScoreSortStream::new(
            stream,
            column_name,
            order,
            score.limit,
        ))
    } else {
        stream
    }
}

fn wrap_stream_with_scores(
    inner: SendableRecordBatchStream,
    context: Arc<FullTextContext>,
    placement: ScoreColumnPlacement,
) -> SendableRecordBatchStream {
    let schema = match &placement {
        ScoreColumnPlacement::Append { field_name } => {
            append_score_schema(&inner.schema(), field_name)
        }
        ScoreColumnPlacement::Replace { .. } => inner.schema(),
    };
    Box::pin(ScoreAugmentedStream::new(inner, context, schema, placement))
}

fn append_score_schema(base: &SchemaRef, field_name: &str) -> SchemaRef {
    let mut fields: Vec<datafusion::arrow::datatypes::Field> =
        base.fields().iter().map(|f| f.as_ref().clone()).collect();
    fields.push(Field::new(field_name, DataType::Float32, false));
    Arc::new(Schema::new_with_metadata(fields, base.metadata().clone()))
}

struct ScoreAugmentedStream {
    inner: SendableRecordBatchStream,
    context: Arc<FullTextContext>,
    schema: SchemaRef,
    placement: ScoreColumnPlacement,
}

impl Unpin for ScoreAugmentedStream {}

impl ScoreAugmentedStream {
    fn new(
        inner: SendableRecordBatchStream,
        context: Arc<FullTextContext>,
        schema: SchemaRef,
        placement: ScoreColumnPlacement,
    ) -> Self {
        Self {
            inner,
            context,
            schema,
            placement,
        }
    }
}

impl Stream for ScoreAugmentedStream {
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.as_mut().get_mut();
        match this.inner.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                let result = inject_score_column(&batch, &this.context, &this.placement);
                this.context.clear_scores();
                Poll::Ready(Some(result))
            }
            Poll::Ready(Some(Err(err))) => {
                this.context.clear_scores();
                Poll::Ready(Some(Err(err.into())))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl RecordBatchStream for ScoreAugmentedStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

struct ScoreSortStream {
    inner: SendableRecordBatchStream,
    schema: SchemaRef,
    column_name: String,
    order: ScoreOrder,
    limit: Option<ScoreLimit>,
    buffer: Vec<RecordBatch>,
    finished: bool,
    pending_batch: Option<RecordBatch>,
}

impl ScoreSortStream {
    fn new(
        inner: SendableRecordBatchStream,
        column_name: String,
        order: ScoreOrder,
        limit: Option<ScoreLimit>,
    ) -> Self {
        Self {
            schema: inner.schema(),
            inner,
            column_name,
            order,
            limit,
            buffer: Vec::new(),
            finished: false,
            pending_batch: None,
        }
    }

    fn finalize(&mut self) -> DataFusionResult<()> {
        if self.finished {
            return Ok(());
        }

        let batch = self.build_sorted_batch()?;
        self.pending_batch = Some(batch);
        self.finished = true;
        self.buffer.clear();
        Ok(())
    }

    fn build_sorted_batch(&mut self) -> DataFusionResult<RecordBatch> {
        if self.buffer.is_empty() {
            return Ok(RecordBatch::new_empty(self.schema.clone()));
        }

        let batch_refs: Vec<&RecordBatch> = self.buffer.iter().collect();
        let combined = concat_batches(&self.schema, batch_refs).map_err(|e| {
            DataFusionError::Execution(format!("Failed to concatenate batches: {}", e))
        })?;

        let score_index = self.schema.index_of(&self.column_name).map_err(|_| {
            DataFusionError::Execution(format!(
                "_score column '{}' not found in schema",
                self.column_name
            ))
        })?;

        let sort_columns = vec![SortColumn {
            values: combined.column(score_index).clone(),
            options: Some(SortOptions {
                descending: matches!(self.order, ScoreOrder::Desc),
                nulls_first: false,
            }),
        }];

        let indices = lexsort_to_indices(&sort_columns, None)
            .map_err(|e| DataFusionError::Execution(format!("Failed to sort by _score: {}", e)))?;

        let mut sorted_columns = Vec::with_capacity(combined.num_columns());
        for column in combined.columns() {
            let taken = take(column.as_ref(), &indices, None).map_err(|e| {
                DataFusionError::Execution(format!("Failed to reorder column: {}", e))
            })?;
            sorted_columns.push(taken);
        }

        let sorted_batch =
            RecordBatch::try_new(self.schema.clone(), sorted_columns).map_err(|e| {
                DataFusionError::Execution(format!("Failed to build sorted batch: {}", e))
            })?;

        let total = sorted_batch.num_rows();
        let offset = self.limit.and_then(|l| l.offset).unwrap_or(0);

        if offset >= total {
            return Ok(RecordBatch::new_empty(self.schema.clone()));
        }

        let remaining = total - offset;
        let wanted = self.limit.and_then(|l| l.limit).unwrap_or(remaining);
        let len = wanted.min(remaining);

        Ok(sorted_batch.slice(offset, len))
    }
}

impl Stream for ScoreSortStream {
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            if let Some(batch) = self.pending_batch.take() {
                return Poll::Ready(Some(Ok(batch)));
            }

            if self.finished {
                return Poll::Ready(None);
            }

            match self.inner.as_mut().poll_next(cx) {
                Poll::Ready(Some(Ok(batch))) => {
                    self.buffer.push(batch);
                }
                Poll::Ready(Some(Err(err))) => {
                    self.finished = true;
                    return Poll::Ready(Some(Err(err)));
                }
                Poll::Ready(None) => match self.finalize() {
                    Ok(()) => {}
                    Err(err) => {
                        self.finished = true;
                        return Poll::Ready(Some(Err(err)));
                    }
                },
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl RecordBatchStream for ScoreSortStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{ArrayRef, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::MemTable;
    use std::sync::Arc;

    #[tokio::test]
    async fn text_udf_filters_rows() {
        let ctx = SessionContext::new();
        register_fulltext_udfs(&ctx);

        let schema = Arc::new(Schema::new(vec![Field::new(
            "message",
            DataType::Utf8,
            true,
        )]));
        let columns: Vec<ArrayRef> = vec![Arc::new(StringArray::from(vec![
            Some("system ok"),
            Some("error detected"),
            Some("panic unreachable"),
        ])) as ArrayRef];
        let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
        let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("logs", Arc::new(table)).unwrap();

        let df = ctx
            .sql("SELECT message FROM logs WHERE text(message, 'error', 1.0)")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
        let values = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(values.value(0), "error detected");
    }
}
