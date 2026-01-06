use std::{
    any::Any,
    collections::HashMap,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use crate::compute::udf::fulltext_udf::register_fulltext_udfs;
use crate::segment::{field_store::row_data::RowDataStoreReader, IndexReader, RowDataStore};
use datafusion::scalar::ScalarValue;
use datafusion::{
    arrow::{datatypes::SchemaRef, record_batch::RecordBatch},
    common::ToDFSchema,
    error::Result as DFResult,
    execution::SendableRecordBatchStream,
    logical_expr::Expr,
    physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, RecordBatchStream},
};
use roaring::RoaringBitmap;

/// SegmentScanner - 内部工具类,用于扫描单个 Segment
///
/// 不实现 TableProvider - Segment 不直接对外暴露,最小查询单位是 Partition
pub(crate) struct SegmentScanner {
    schema: SchemaRef,
    raw_data: RowDataStore,
    index_readers: HashMap<String, Box<dyn IndexReader>>,
    doc_count: u32,
    /// 有效文档的 bitmap (doc_count - del)，预先计算避免每次都重新生成
    valid_docs: RoaringBitmap,
    emit_internal_id: bool,
    internal_id_index: Option<usize>,
    data_field_count: usize,
    segment_start: u64,
}

impl std::fmt::Debug for SegmentScanner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SegmentScanner")
            .field("doc_count", &self.doc_count)
            .finish()
    }
}

impl SegmentScanner {
    pub fn new(
        schema: SchemaRef,
        raw_data: RowDataStore,
        index_readers: HashMap<String, Box<dyn IndexReader>>,
        doc_count: u32,
        del: RoaringBitmap,
        emit_internal_id: bool,
        segment_start: u64,
    ) -> Self {
        // Commented for cleaner logs
        // println!("[DEBUG] SegmentScanner::new() called");
        // println!("[DEBUG] Received {} index_readers: {:?}", index_readers.len(), index_readers.keys().collect::<Vec<_>>());

        // 预先计算有效文档的 bitmap (只计算一次)
        let mut valid_docs = RoaringBitmap::new();
        valid_docs.insert_range(0..doc_count);
        valid_docs -= &del;

        let total_fields = schema.fields().len();
        let internal_id_index = if emit_internal_id {
            if total_fields == 0 {
                panic!("Schema must contain fields before appending _internal_id");
            }
            Some(total_fields - 1)
        } else {
            None
        };
        let data_field_count = if emit_internal_id {
            total_fields.saturating_sub(1)
        } else {
            total_fields
        };

        Self {
            schema: schema.clone(),
            raw_data,
            index_readers,
            doc_count,
            valid_docs,
            emit_internal_id,
            internal_id_index,
            data_field_count,
            segment_start,
        }
    }

    /// 创建执行计划 - 主入口方法
    ///
    /// 应用 filters，计算命中的文档，然后构建执行计划
    ///
    /// # 优化策略
    /// 1. 如果有 ORDER BY + LIMIT，尝试使用索引有序扫描
    /// 2. 否则使用普通的 bitmap 扫描
    pub fn create_plan(
        &self,
        filters: &[Expr],
        projection: Option<&Vec<usize>>,
        limit: Option<usize>,
        sort: Option<(String, bool)>,
    ) -> Option<Arc<dyn ExecutionPlan>> {
        log::debug!(
            "🔍 [SegmentScanner::create_plan] filters={:?}, projection={:?}, limit={:?}, sort={:?}",
            filters,
            projection,
            limit,
            sort
        );

        // 应用 filters，计算命中的文档
        let (result_bitmap, unsupported_filters) = self.apply_filters(filters);

        // 🔧 修复: 正确处理 bitmap 为 None 的情况
        // 1. 如果 bitmap 为 Some 且有数据 -> 使用 bitmap
        // 2. 如果 bitmap 为 None 但只有 unsupported_filters(没有支持的过滤器) -> 使用所有文档,让 DataFusion 过滤
        // 3. 如果 bitmap 为 None 且有支持的过滤器(已返回空) -> 直接返回 None,因为支持的过滤器已经确定没数据
        let result_bitmap = match result_bitmap {
            Some(bitmap) => bitmap,
            None => {
                // bitmap 为 None 有两种情况:
                // A) 支持的过滤器返回空结果 -> 应该返回 None
                // B) 只有不支持的过滤器,没有支持的过滤器 -> 使用所有文档
                // 我们无法从这里区分,但 apply_filters 已经做了处理
                // 如果有不支持的过滤器,apply_filters 应该返回 Some(valid_docs)
                // 所以 None 意味着真的没数据
                log::debug!("📋 [SegmentScanner] Bitmap filter returned None, no documents match");
                return None;
            }
        };

        log::debug!(
            "🎯 [SegmentScanner] hit={}/{} ({:.1}%), limit={:?}, sort={:?}",
            result_bitmap.len(),
            self.valid_docs.len(),
            result_bitmap.len() as f64 / self.valid_docs.len() as f64 * 100.0,
            limit,
            sort
        );

        // 尝试使用索引有序扫描优化
        if let Some((field_name, ascending)) = &sort {
            if let Some(limit_val) = limit {
                // 判断是否应该使用有序扫描
                if self.should_use_ordered_scan(&result_bitmap, limit_val) {
                    // 尝试使用索引有序扫描
                    if let Some(ordered_plan) = self.try_ordered_scan(
                        field_name,
                        *ascending,
                        &result_bitmap,
                        projection,
                        limit_val,
                    ) {
                        log::info!(
                            "🚀 [SegmentScanner] Using index ordered scan for ORDER BY {} {}",
                            field_name,
                            if *ascending { "ASC" } else { "DESC" }
                        );
                        // 如果有未支持的过滤器,需要在有序扫描之上再加一层 FilterExec
                        if !unsupported_filters.is_empty() {
                            return self.wrap_with_filter(ordered_plan, unsupported_filters);
                        }
                        return Some(ordered_plan);
                    }
                }
            }
        }

        // 否则使用普通的 bitmap 扫描
        self.build_exec_plan(result_bitmap, projection, sort, limit, unsupported_filters)
    }

    /// 应用过滤条件，返回命中的文档bitmap
    fn apply_filters(&self, filters: &[Expr]) -> (Option<RoaringBitmap>, Vec<Expr>) {
        let mut result_bitmap = self.valid_docs.clone();
        let mut unsupported_filters = Vec::new();
        let mut has_supported_filter = false;

        log::debug!(
            "🔍 [apply_filters] Processing {} filters, valid_docs={}, thread={:?}",
            filters.len(),
            self.valid_docs.len(),
            std::thread::current().id()
        );

        for filter in filters {
            match self.expr_to_bitmap(filter) {
                Some(bitmap) => {
                    log::info!(
                        "✓ [apply_filters] Filter handled via index: {:?}, bitmap_size={}, thread={:?}",
                        filter,
                        bitmap.len(),
                        std::thread::current().id()
                    );
                    result_bitmap &= bitmap;
                    has_supported_filter = true;
                }
                None => {
                    // 遇到无法通过索引处理的条件(如 LIKE)
                    // 收集这些过滤器,稍后让 DataFusion 处理
                    unsupported_filters.push(filter.clone());
                    log::debug!(
                        "⚠️  [apply_filters] Cannot handle filter via index: {:?}, will use DataFusion filter",
                        filter
                    );
                }
            }
        }

        // 如果有无法处理的过滤器
        if !unsupported_filters.is_empty() {
            log::debug!(
                "🔍 [apply_filters] Has {} unsupported filters, current result_bitmap={} docs",
                unsupported_filters.len(),
                result_bitmap.len()
            );
        }

        // 决定是否返回 bitmap:
        // 1. 如果有支持的过滤器且结果为空 -> 返回 None (真的没数据)
        // 2. 如果只有不支持的过滤器 -> 返回所有有效文档 (让 DataFusion 过滤)
        // 3. 如果有支持的过滤器且结果非空 -> 返回 bitmap
        let bitmap = if has_supported_filter {
            if result_bitmap.is_empty() {
                log::debug!(
                    "📋 [apply_filters] Has supported filter but result empty, returning None"
                );
                None
            } else {
                log::debug!(
                    "📋 [apply_filters] Has supported filter with {} results, returning bitmap",
                    result_bitmap.len()
                );
                Some(result_bitmap)
            }
        } else {
            // 没有支持的过滤器,只有 LIKE 等不支持的过滤器
            // 返回所有有效文档
            log::debug!(
                "📋 [apply_filters] No supported filter, only {} unsupported filters, returning all {} valid docs",
                unsupported_filters.len(),
                self.valid_docs.len()
            );
            Some(self.valid_docs.clone())
        };

        log::debug!(
            "✅ [apply_filters] Final result: bitmap={}, unsupported_filters={}",
            if bitmap.is_some() { "Some" } else { "None" },
            unsupported_filters.len()
        );

        (bitmap, unsupported_filters)
    }

    /// 尝试对范围查询使用预分组优化
    ///

    /// 🚀 COUNT(*) 优化专用方法: 只计算匹配数,不读取数据
    ///
    /// 用于 `SELECT COUNT(*) FROM table WHERE ...` 查询的极速优化
    ///
    /// 带 skip 和 limit 的扫描（用于自然顺序查询）
    ///
    /// # 参数
    /// - `filters`: WHERE 条件表达式列表
    /// - `skip`: 跳过的行数（在 bitmap 中跳过）
    /// - `limit`: 最多返回的行数
    ///
    /// # 核心优化
    /// 通过 bitmap 直接跳过 skip 行，只读取 limit 行数据
    ///
    /// # 示例
    /// ```
    /// // segment 有 10000 行，skip=5000, limit=1000
    /// // 1. 应用 filters 得到 bitmap
    /// // 2. 在 bitmap 中跳过前 5000 个 doc_id
    /// // 3. 读取接下来的 1000 个 doc_id
    /// ```
    /// 扫描 segment 数据，支持跳过和限制行数
    ///
    /// # 参数
    /// - `filters`: WHERE 过滤条件
    /// - `skip`: 跳过的物理行数（包括已删除的行）
    /// - `limit`: 读取的物理行数上限
    /// - `projection`: 投影列索引（用于列裁剪）
    ///
    /// # 返回
    /// 返回过滤后的实际数据，行数可能少于 limit（因为删除或过滤）
    pub(crate) fn scan_with_skip_and_limit(
        &self,
        filters: &[Expr],
        skip: usize,
        limit: usize,
        projection: Option<&Vec<usize>>,
    ) -> crate::utils::error::CoreResult<RecordBatch> {
        let scan_start = std::time::Instant::now();

        // 计算要读取的物理 doc_ids 范围
        let start_doc_id = skip as u32;
        let end_doc_id = (skip + limit).min(self.doc_count as usize) as u32;

        if start_doc_id >= self.doc_count {
            // 起始位置超出范围，返回空
            return self.create_empty_batch();
        }

        // 生成物理 doc_ids
        let gen_ids_start = std::time::Instant::now();
        let physical_doc_ids: Vec<u32> = (start_doc_id..end_doc_id).collect();
        log::debug!(
            "    ⏱️  [scan] generate doc_ids: {:?}",
            gen_ids_start.elapsed()
        );

        log::debug!(
            "🔍 [SegmentScanner::scan_with_skip_and_limit] doc_count={}, skip={}, limit={}, reading docs [{}, {}), projection={:?}",
            self.doc_count,
            skip,
            limit,
            start_doc_id,
            end_doc_id,
            projection
        );

        // 读取物理数据（应用投影下推）
        let read_start = std::time::Instant::now();
        let batch = if let Some(proj) = projection {
            self.read_docs_by_ids_with_projection(&physical_doc_ids, proj)?
        } else {
            self.read_docs_by_ids(&physical_doc_ids)?
        };
        log::info!(
            "    ⏱️  [scan] read_docs_by_ids ({}): {:?}",
            physical_doc_ids.len(),
            read_start.elapsed()
        );

        // 如果没有过滤条件，直接返回
        if filters.is_empty() {
            // 应用删除标记过滤
            let filter_start = std::time::Instant::now();
            let valid_bitmap = self.get_valid_docs_in_range(start_doc_id, end_doc_id);
            if valid_bitmap.is_empty() {
                return self.create_empty_batch();
            }

            // 过滤掉已删除的行
            let result = self.filter_batch_by_bitmap(&batch, &valid_bitmap, start_doc_id);
            log::debug!("    ⏱️  [scan] filter bitmap: {:?}", filter_start.elapsed());
            log::info!(
                "    ⏱️  [scan] TOTAL scan_with_skip_and_limit: {:?}",
                scan_start.elapsed()
            );
            return result;
        }

        // 应用 WHERE 过滤条件
        let (result_bitmap, unsupported_filters) = self.apply_filters(filters);
        let result_bitmap = match result_bitmap {
            Some(bitmap) => bitmap,
            None => {
                log::debug!(
                    "📊 [SegmentScanner] Filter returned no matching docs, returning empty batch"
                );
                return self.create_empty_batch();
            }
        };

        // 只保留在当前范围内且满足条件的行
        let mut filtered_bitmap = RoaringBitmap::new();
        for doc_id in start_doc_id..end_doc_id {
            if result_bitmap.contains(doc_id) {
                filtered_bitmap.insert(doc_id);
            }
        }

        log::debug!(
            "📊 [SegmentScanner] Physical range: [{}, {}), after filter: {} rows",
            start_doc_id,
            end_doc_id,
            filtered_bitmap.len()
        );

        if filtered_bitmap.is_empty() {
            return self.create_empty_batch();
        }

        // 过滤 batch
        let filtered_batch = self.filter_batch_by_bitmap(&batch, &filtered_bitmap, start_doc_id)?;

        // 如果有不支持的过滤器，使用 DataFusion 进行二次过滤
        if !unsupported_filters.is_empty() {
            log::debug!(
                "🔍 [scan_with_skip_and_limit] Applying {} DataFusion filters",
                unsupported_filters.len()
            );
            return self.apply_datafusion_filter(filtered_batch, &unsupported_filters);
        }

        Ok(filtered_batch)
    }

    /// 使用 DataFusion 对 RecordBatch 进行二次过滤
    fn apply_datafusion_filter(
        &self,
        batch: RecordBatch,
        filters: &[Expr],
    ) -> crate::utils::error::CoreResult<RecordBatch> {
        use crate::utils::error::CoreError;
        use datafusion::arrow::compute::filter_record_batch;
        use datafusion::common::ToDFSchema;
        use datafusion::execution::context::SessionContext;
        use datafusion::physical_expr::create_physical_expr;

        if filters.is_empty() || batch.num_rows() == 0 {
            return Ok(batch);
        }

        // 组合所有 filters
        let combined_filter = if filters.len() == 1 {
            filters[0].clone()
        } else {
            let mut combined = filters[0].clone();
            for i in 1..filters.len() {
                combined = Expr::BinaryExpr(datafusion::logical_expr::BinaryExpr {
                    left: Box::new(combined),
                    op: datafusion::logical_expr::Operator::And,
                    right: Box::new(filters[i].clone()),
                });
            }
            combined
        };

        let ctx = SessionContext::new();
        let _fulltext_context = register_fulltext_udfs(&ctx);
        let df_schema = batch.schema().to_dfschema_ref().map_err(|e| {
            CoreError::Internal(format!("Failed to convert schema to DFSchema: {}", e))
        })?;

        let physical_expr =
            create_physical_expr(&combined_filter, &df_schema, ctx.state().execution_props())
                .map_err(|e| {
                    CoreError::Internal(format!("Failed to create physical expr: {}", e))
                })?;

        let result = physical_expr
            .evaluate(&batch)
            .map_err(|e| CoreError::Internal(format!("Failed to evaluate filter: {}", e)))?;

        let predicate = match result {
            datafusion::physical_plan::ColumnarValue::Array(array) => {
                use datafusion::arrow::array::AsArray;
                let boolean_array = array.as_boolean();
                if boolean_array.len() != batch.num_rows() {
                    return Err(CoreError::Internal(format!(
                        "Filter returned array of length {}, expected {}",
                        boolean_array.len(),
                        batch.num_rows()
                    )));
                }
                boolean_array.clone()
            }
            datafusion::physical_plan::ColumnarValue::Scalar(scalar) => match scalar {
                ScalarValue::Boolean(Some(v)) => {
                    if v {
                        return Ok(batch);
                    } else {
                        return Ok(RecordBatch::new_empty(batch.schema()));
                    }
                }
                ScalarValue::Boolean(None) => {
                    return Ok(RecordBatch::new_empty(batch.schema()));
                }
                _ => {
                    return Err(CoreError::Internal(
                        "Filter returned non-boolean scalar".to_string(),
                    ))
                }
            },
        };

        filter_record_batch(&batch, &predicate)
            .map_err(|e| CoreError::Internal(format!("Failed to filter batch: {}", e)))
    }

    /// 获取指定范围内的有效文档（未删除）
    fn get_valid_docs_in_range(&self, start: u32, end: u32) -> RoaringBitmap {
        let mut valid = RoaringBitmap::new();
        for doc_id in start..end {
            if self.valid_docs.contains(doc_id) {
                valid.insert(doc_id);
            }
        }
        valid
    }

    /// 根据 bitmap 过滤 RecordBatch
    fn filter_batch_by_bitmap(
        &self,
        batch: &RecordBatch,
        bitmap: &RoaringBitmap,
        base_doc_id: u32,
    ) -> crate::utils::error::CoreResult<RecordBatch> {
        use crate::utils::error::CoreError;
        use datafusion::arrow::array::UInt32Array;
        use datafusion::arrow::compute::take;

        // 将 bitmap 中的 doc_id 转换为 batch 内的相对索引
        let indices: Vec<u32> = bitmap.iter().map(|doc_id| doc_id - base_doc_id).collect();

        if indices.is_empty() {
            // 返回与输入 batch 相同 schema 的空 batch
            let empty_columns: Vec<Arc<dyn datafusion::arrow::array::Array>> = batch
                .schema()
                .fields()
                .iter()
                .map(|field| datafusion::arrow::array::new_empty_array(field.data_type()))
                .collect();
            return RecordBatch::try_new(batch.schema(), empty_columns)
                .map_err(|e| CoreError::Internal(format!("Failed to create empty batch: {}", e)));
        }

        let indices_array = UInt32Array::from(indices);
        let mut columns = Vec::new();

        // 使用 batch 实际的列数，而不是 self.schema（投影后列数会不同）
        for i in 0..batch.num_columns() {
            let column = batch.column(i);
            let taken = take(column, &indices_array, None)
                .map_err(|e| CoreError::Internal(format!("Failed to take rows: {}", e)))?;
            columns.push(taken);
        }

        // 使用 batch 的 schema，而不是 self.schema
        RecordBatch::try_new(batch.schema(), columns)
            .map_err(|e| CoreError::Internal(format!("Failed to create batch: {}", e)))
    }

    /// 创建空的 RecordBatch
    fn create_empty_batch(&self) -> crate::utils::error::CoreResult<RecordBatch> {
        use crate::utils::error::CoreError;
        use datafusion::arrow::array::new_empty_array;

        let empty_columns: Vec<Arc<dyn datafusion::arrow::array::Array>> = self
            .schema
            .fields()
            .iter()
            .map(|field| new_empty_array(field.data_type()))
            .collect();

        RecordBatch::try_new(self.schema.clone(), empty_columns)
            .map_err(|e| CoreError::Internal(format!("Failed to create empty batch: {}", e)))
    }

    /// 创建空的 RecordBatch（带投影）
    fn create_empty_batch_with_projection(
        &self,
        projection: &[usize],
    ) -> crate::utils::error::CoreResult<RecordBatch> {
        use crate::utils::error::CoreError;
        use datafusion::arrow::array::new_empty_array;

        let projected_schema = self.build_projected_schema(Some(&projection.to_vec()));
        let empty_columns: Vec<Arc<dyn datafusion::arrow::array::Array>> = projected_schema
            .fields()
            .iter()
            .map(|field| new_empty_array(field.data_type()))
            .collect();

        RecordBatch::try_new(projected_schema, empty_columns)
            .map_err(|e| CoreError::Internal(format!("Failed to create empty batch: {}", e)))
    }

    /// 根据 doc_ids 读取数据
    fn read_docs_by_ids(&self, doc_ids: &[u32]) -> crate::utils::error::CoreResult<RecordBatch> {
        use crate::utils::error::CoreError;

        let read_start = std::time::Instant::now();

        if doc_ids.is_empty() {
            return self.create_empty_batch();
        }

        let include_internal_id = should_emit_internal_id_for_projection(
            self.emit_internal_id,
            self.internal_id_index,
            None,
        );

        log::debug!(
            "📖 [read_docs_by_ids] Requested {} doc_ids: first={}, last={}, doc_count={}",
            doc_ids.len(),
            doc_ids.first().unwrap(),
            doc_ids.last().unwrap(),
            self.doc_count
        );

        // 查找 doc_ids 对应的 batch_key
        let lookup_start = std::time::Instant::now();
        let batch_doc_map = self.raw_data.batch_lookup_doc_ids(doc_ids);
        log::debug!(
            "      ⏱️  [read_docs] batch_lookup: {:?}",
            lookup_start.elapsed()
        );

        if batch_doc_map.is_empty() {
            log::warn!("⚠️  batch_lookup_doc_ids returned empty");
            return self.create_empty_batch();
        }

        log::debug!(
            "📋 [read_docs_by_ids] Found {} batches: keys={:?}",
            batch_doc_map.len(),
            batch_doc_map.keys().collect::<Vec<_>>()
        );

        // 🚀 性能优化:
        // - 少量 batches (≤5): 批量读取,一次打开文件 → 快
        // - 大量 batches (>5): 逐个读取,避免内存占用过大
        let batch_read_start = std::time::Instant::now();
        let source_batches = if batch_doc_map.len() <= 5 {
            let batch_keys: Vec<u32> = batch_doc_map.keys().copied().collect();
            let result = self.raw_data.get_batch_with_projection(&batch_keys, None);
            log::info!(
                "      ⏱️  [read_docs] batch_read ({} RowGroups): {:?}",
                batch_keys.len(),
                batch_read_start.elapsed()
            );
            result
        } else {
            HashMap::new() // 空,后面逐个读取
        };

        // 读取各个 batch 并合并
        let merge_start = std::time::Instant::now();
        let mut batches = Vec::new();
        for (batch_key, doc_ids_in_batch) in batch_doc_map {
            log::debug!(
                "📦 Processing batch_key={}, contains {} doc_ids (first={}, last={})",
                batch_key,
                doc_ids_in_batch.len(),
                doc_ids_in_batch.first().unwrap(),
                doc_ids_in_batch.last().unwrap()
            );

            // 尝试从批量读取的结果中获取,否则单独读取
            let batch = if let Some(b) = source_batches.get(&batch_key) {
                b.clone()
            } else {
                match self.raw_data.get(&batch_key) {
                    Some(b) => b,
                    None => {
                        log::warn!("⚠️  Batch not found for key={}", batch_key);
                        continue;
                    }
                }
            };

            {
                // 从 batch 中提取需要的行
                // batch_key 是 batch 的起始 doc_id，需要转换为相对索引
                let batch_size = batch.num_rows() as u32;

                log::debug!(
                    "📦 [read_docs_by_ids] batch_key={}, batch_size={}, doc_ids range=[{}, {}]",
                    batch_key,
                    batch_size,
                    doc_ids_in_batch.first().unwrap(),
                    doc_ids_in_batch.last().unwrap()
                );

                let mut indices: Vec<u32> = Vec::new();
                let mut selected_doc_ids: Vec<u32> = Vec::new();
                for doc_id in doc_ids_in_batch.iter() {
                    let relative_idx = doc_id - batch_key;
                    if relative_idx < batch_size {
                        indices.push(relative_idx);
                        selected_doc_ids.push(*doc_id);
                    } else {
                        log::error!(
                            "❌ CRITICAL: doc_id={} mapped to batch_key={} but relative_idx={} >= batch_size={}",
                            doc_id, batch_key, relative_idx, batch_size
                        );
                    }
                }

                if indices.is_empty() {
                    log::warn!("⚠️  No valid indices for batch_key={}, skipping", batch_key);
                    continue;
                }

                log::trace!(
                    "📖 [read_docs_by_ids] batch_key={}, batch_rows={}, reading {} indices (first few: {:?})",
                    batch_key,
                    batch_size,
                    indices.len(),
                    &indices[..indices.len().min(5)]
                );

                // 使用 arrow 的 take 操作提取指定行
                use datafusion::arrow::array::UInt32Array;
                use datafusion::arrow::compute::take;

                let indices_array = UInt32Array::from(indices);
                let mut columns = Vec::new();

                for i in 0..self.data_field_count {
                    let column = batch.column(i);
                    let taken = take(column, &indices_array, None).map_err(|e| {
                        CoreError::Internal(format!(
                            "Failed to take rows from batch {} (size={}): {}",
                            batch_key, batch_size, e
                        ))
                    })?;
                    columns.push(taken);
                }

                if include_internal_id {
                    columns.push(build_internal_id_array(
                        self.segment_start,
                        &selected_doc_ids,
                    ));
                }

                let selected_batch = RecordBatch::try_new(self.schema.clone(), columns)
                    .map_err(|e| CoreError::Internal(format!("Failed to create batch: {}", e)))?;
                batches.push(selected_batch);
            }
        }

        log::debug!(
            "      ⏱️  [read_docs] merge & extract: {:?}",
            merge_start.elapsed()
        );

        log::debug!(
            "      ⏱️  [read_docs] TOTAL read_docs_by_ids: {:?}",
            read_start.elapsed()
        );

        if batches.is_empty() {
            return self.create_empty_batch();
        }

        if batches.len() == 1 {
            return Ok(batches.into_iter().next().unwrap());
        }

        // 合并所有 batches
        use datafusion::arrow::compute::concat_batches;
        concat_batches(&self.schema, &batches)
            .map_err(|e| CoreError::Internal(format!("Failed to concat batches: {}", e)))
    }

    /// 根据 doc_ids 读取数据（带投影下推）
    fn read_docs_by_ids_with_projection(
        &self,
        doc_ids: &[u32],
        projection: &[usize],
    ) -> crate::utils::error::CoreResult<RecordBatch> {
        use crate::utils::error::CoreError;

        let read_start = std::time::Instant::now();

        if doc_ids.is_empty() {
            return self.create_empty_batch_with_projection(projection);
        }

        log::debug!(
            "📖 [read_docs_by_ids_with_projection] Requested {} doc_ids with projection {:?}",
            doc_ids.len(),
            projection
        );

        let include_internal_id = should_emit_internal_id_for_projection(
            self.emit_internal_id,
            self.internal_id_index,
            Some(projection),
        );
        let storage_projection =
            storage_projection_indices(Some(projection), self.internal_id_index);

        // 查找 doc_ids 对应的 batch_key
        let lookup_start = std::time::Instant::now();
        let batch_doc_map = self.raw_data.batch_lookup_doc_ids(doc_ids);
        log::debug!(
            "      ⏱️  [read_docs] batch_lookup: {:?}",
            lookup_start.elapsed()
        );

        if batch_doc_map.is_empty() {
            log::warn!("⚠️  batch_lookup_doc_ids returned empty");
            return self.create_empty_batch_with_projection(projection);
        }

        // 🚀 投影下推：只读取需要的列
        let batch_read_start = std::time::Instant::now();
        let source_batches = if batch_doc_map.len() <= 5 {
            let batch_keys: Vec<u32> = batch_doc_map.keys().copied().collect();
            let result = self.raw_data.get_batch_with_projection(
                &batch_keys,
                storage_projection.as_ref().map(|v| v.as_slice()),
            );
            log::debug!(
                "      ⏱️  [read_docs] batch_read ({} RowGroups, {} cols): {:?}",
                batch_keys.len(),
                projection.len(),
                batch_read_start.elapsed()
            );
            result
        } else {
            HashMap::new()
        };

        // 构建投影后的 schema
        let projected_schema = self.build_projected_schema(Some(&projection.to_vec()));

        // 读取各个 batch 并合并
        let merge_start = std::time::Instant::now();
        let mut batches = Vec::new();
        for (batch_key, doc_ids_in_batch) in batch_doc_map {
            // 尝试从批量读取的结果中获取，否则单独读取
            let batch = if let Some(b) = source_batches.get(&batch_key) {
                b.clone()
            } else {
                // 单独读取时也应用投影
                match self.raw_data.get_with_projection(
                    &batch_key,
                    storage_projection.as_ref().map(|v| v.as_slice()),
                ) {
                    Some(b) => b,
                    None => {
                        log::warn!("⚠️  Batch not found for key={}", batch_key);
                        continue;
                    }
                }
            };

            {
                let batch_size = batch.num_rows() as u32;
                let mut rows_with_ids: Vec<(u32, u32)> = Vec::new();
                for doc_id in doc_ids_in_batch.iter() {
                    let relative_idx = doc_id - batch_key;
                    if relative_idx < batch_size {
                        rows_with_ids.push((relative_idx, *doc_id));
                    } else {
                        log::error!(
                            "❌ CRITICAL: doc_id={} mapped to batch_key={} but relative_idx={} >= batch_size={}",
                            doc_id, batch_key, relative_idx, batch_size
                        );
                    }
                }

                if rows_with_ids.is_empty() {
                    log::warn!("⚠️  No valid indices for batch_key={}, skipping", batch_key);
                    continue;
                }

                rows_with_ids.sort_unstable_by_key(|(row_idx, _)| *row_idx);
                let indices: Vec<u32> = rows_with_ids.iter().map(|(row_idx, _)| *row_idx).collect();
                let doc_ids_for_rows: Vec<u32> =
                    rows_with_ids.iter().map(|(_, doc_id)| *doc_id).collect();

                if indices.is_empty() {
                    log::warn!("⚠️  No valid indices for batch_key={}, skipping", batch_key);
                    continue;
                }

                // 使用 arrow 的 take 操作提取指定行
                use datafusion::arrow::array::UInt32Array;
                use datafusion::arrow::compute::take;

                let indices_array = UInt32Array::from(indices);
                let mut data_columns = Vec::new();
                for column in batch.columns() {
                    let taken = take(column, &indices_array, None).map_err(|e| {
                        CoreError::Internal(format!(
                            "Failed to take rows from batch {}: {}",
                            batch_key, e
                        ))
                    })?;
                    data_columns.push(taken);
                }

                let mut data_iter = data_columns.into_iter();
                let internal_id_column = if include_internal_id {
                    Some(build_internal_id_array(
                        self.segment_start,
                        &doc_ids_for_rows,
                    ))
                } else {
                    None
                };
                let mut columns = Vec::new();
                for idx in projection {
                    if self
                        .internal_id_index
                        .is_some_and(|internal_idx| internal_idx == *idx)
                    {
                        if let Some(array) = internal_id_column.as_ref() {
                            columns.push(array.clone());
                        }
                    } else if let Some(col) = data_iter.next() {
                        columns.push(col);
                    }
                }

                let selected_batch = RecordBatch::try_new(projected_schema.clone(), columns)
                    .map_err(|e| CoreError::Internal(format!("Failed to create batch: {}", e)))?;
                batches.push(selected_batch);
            }
        }

        log::debug!(
            "      ⏱️  [read_docs] merge & extract: {:?}",
            merge_start.elapsed()
        );

        log::debug!(
            "      ⏱️  [read_docs] TOTAL read_docs_by_ids_with_projection: {:?}",
            read_start.elapsed()
        );

        if batches.is_empty() {
            return self.create_empty_batch_with_projection(projection);
        }

        if batches.len() == 1 {
            return Ok(batches.into_iter().next().unwrap());
        }

        // 合并所有 batches
        use datafusion::arrow::compute::concat_batches;
        concat_batches(&projected_schema, &batches)
            .map_err(|e| CoreError::Internal(format!("Failed to concat batches: {}", e)))
    }

    /// 判断是否应该使用有序扫描
    ///
    /// # 核心思想
    /// 有序扫描的优势：只需要读取 LIMIT 条数据，而不是所有命中的数据
    /// 但前提是：命中率不能太低，否则需要遍历太多索引 keys
    ///
    /// # 规则（3 条）
    /// 1. 命中率 >= 0.1%：避免遍历太多索引 keys
    /// 2. 命中数 >= 1000：确保排序开销足够大
    /// 3. LIMIT < 命中数 × 10%：确保有序扫描只读少量数据
    ///
    /// # Arguments
    /// * `result_bitmap` - 过滤后的文档 bitmap
    /// * `limit` - LIMIT 值
    fn should_use_ordered_scan(&self, result_bitmap: &RoaringBitmap, limit: usize) -> bool {
        let hit_count = result_bitmap.len() as usize;
        let total_count = self.valid_docs.len() as usize;
        let hit_ratio = hit_count as f64 / total_count as f64;

        // 规则 1：命中率必须 >= 0.1% (千分之一)
        // 原因：命中率太低时，有序扫描需要遍历太多索引 keys
        //
        // 例子：
        // ❌ 总数 1000万，命中 1万 (0.1%)，LIMIT 100
        //    如果索引有 100万 个不同值，每个值平均 10 个文档
        //    需要遍历 ~10000 个 keys 才能找到 100 条（太慢）
        //
        // ✅ 总数 1000万，命中 10万 (1%)，LIMIT 100
        //    需要遍历 ~1000 个 keys 就能找到 100 条（可接受）
        if hit_ratio < 0.001 {
            return false;
        }

        // 规则 2：命中数必须 >= 1000
        // 原因：命中数太少时，排序开销很小
        //       例如：1000 条数据排序只需要 ~10000 次比较，非常快
        if hit_count < 1000 {
            return false;
        }

        // 规则 3：LIMIT 必须 < 命中数的 10%
        // 原因：有序扫描的优势在于只读取 LIMIT 条数据
        //       如果 LIMIT 太大（接近命中数），优势不明显
        //
        // 例子：
        // ✅ 命中 10000 条，LIMIT 100 (1%)：读取 ~100 条 vs 10000 条（100x 提升）
        // ❌ 命中 10000 条，LIMIT 2000 (20%)：读取 ~2000 条 vs 10000 条（5x 提升，不值得）
        let limit_ratio = limit as f64 / hit_count as f64;
        if limit_ratio >= 0.1 {
            return false;
        }

        true
    }

    /// 尝试使用索引有序扫描（ORDER BY 优化）
    ///
    /// # Arguments
    /// * `field_name` - 排序字段名
    /// * `ascending` - 是否升序
    /// * `filter_bitmap` - 过滤后的文档 bitmap
    /// * `projection` - 投影列
    /// * `limit` - LIMIT 值
    ///
    /// # Returns
    /// 如果字段有索引且支持有序扫描，返回优化的执行计划；否则返回 None
    fn try_ordered_scan(
        &self,
        field_name: &str,
        ascending: bool,
        filter_bitmap: &RoaringBitmap,
        projection: Option<&Vec<usize>>,
        limit: usize,
    ) -> Option<Arc<dyn ExecutionPlan>> {
        // 检查字段是否有索引
        let index_reader = self.index_readers.get(field_name)?;

        // 尝试使用索引的有序扫描
        let ordered_doc_ids =
            index_reader.scan_ordered(ascending, Some(filter_bitmap), Some(limit))?;

        if ordered_doc_ids.is_empty() {
            return None;
        }

        log::debug!(
            "🎯 [OrderedScan] Field '{}' returned {} docs (limit={})",
            field_name,
            ordered_doc_ids.len(),
            limit
        );

        // 将有序的 doc_ids 转换为 bitmap
        let ordered_bitmap = RoaringBitmap::from_sorted_iter(ordered_doc_ids)
            .unwrap_or_else(|_| RoaringBitmap::new());

        // 构建执行计划
        let projected_schema = self.build_projected_schema(projection);
        let exec = SegmentExec::new(
            self.raw_data.clone(),
            self.schema.clone(),
            projected_schema,
            ordered_bitmap,
            projection.map(|p| p.to_vec()),
            Some((field_name.to_string(), ascending)),
            Some(limit),
        );

        Some(Arc::new(exec))
    }

    /// 构建执行计划（通用方法）
    fn build_exec_plan(
        &self,
        result_bitmap: RoaringBitmap,
        projection: Option<&Vec<usize>>,
        sort: Option<(String, bool)>,
        limit: Option<usize>,
        unsupported_filters: Vec<Expr>,
    ) -> Option<Arc<dyn ExecutionPlan>> {
        // 🔧 策略调整: 如果有不支持的过滤器(如 LIKE),需要特殊处理投影
        // 1. 先用完整 schema 创建 SegmentExec
        // 2. 然后添加 FilterExec 进行过滤
        // 3. 最后添加 ProjectionExec 应用投影
        // 这样 FilterExec 可以访问所有字段来进行过滤

        let (exec_projection, need_projection_after_filter) = if !unsupported_filters.is_empty() {
            // 有不支持的过滤器: 先不应用投影,在 FilterExec 之后再投影
            log::debug!(
                "🔧 [build_exec_plan] Deferring projection due to {} unsupported filters",
                unsupported_filters.len()
            );
            (None, projection.map(|p| p.to_vec()))
        } else {
            // 没有不支持的过滤器: 直接应用投影
            (projection.map(|p| p.to_vec()), None)
        };

        let projected_schema = self.build_projected_schema(exec_projection.as_ref());

        let exec = SegmentExec::new(
            self.raw_data.clone(),
            self.schema.clone(),
            projected_schema,
            result_bitmap,
            exec_projection,
            sort,
            limit,
        );

        let mut plan: Arc<dyn ExecutionPlan> = Arc::new(exec);

        // 如果有无法通过索引处理的过滤器(如 LIKE),使用 FilterExec 包装
        if !unsupported_filters.is_empty() {
            plan = self.wrap_with_filter(plan, unsupported_filters)?;

            // 🔧 如果之前延迟了投影,现在在 FilterExec 之后应用
            if let Some(proj_indices) = need_projection_after_filter {
                if !proj_indices.is_empty() {
                    use datafusion::physical_plan::projection::ProjectionExec;

                    log::debug!(
                        "🔧 [build_exec_plan] Adding ProjectionExec after FilterExec with {} columns",
                        proj_indices.len()
                    );

                    let schema = plan.schema();

                    log::debug!(
                        "🔧 [build_exec_plan] Applying projection after filter. Schema fields: {}, Projection indices: {:?}",
                        schema.fields().len(),
                        proj_indices
                    );

                    // 构建投影表达式: (物理表达式, 列名)
                    let projection_exprs: Vec<_> = proj_indices
                        .iter()
                        .map(|&i| {
                            if i >= schema.fields().len() {
                                log::error!(
                                    "❌ [build_exec_plan] Projection index {} out of bounds for schema with {} fields!",
                                    i,
                                    schema.fields().len()
                                );
                            }
                            use datafusion::physical_expr::expressions::Column;
                            let field = schema.field(i);
                            (
                                Arc::new(Column::new(field.name(), i)) as _,
                                field.name().to_string(),
                            )
                        })
                        .collect();

                    plan = Arc::new(ProjectionExec::try_new(projection_exprs, plan).ok()?);
                }
            }
        }
        Some(plan)
    }

    /// 构建投影后的schema
    fn build_projected_schema(&self, projection: Option<&Vec<usize>>) -> SchemaRef {
        match projection {
            Some(indices) if !indices.is_empty() => {
                let fields: Vec<_> = indices
                    .iter()
                    .map(|i| self.schema.field(*i).clone())
                    .collect();
                Arc::new(datafusion::arrow::datatypes::Schema::new(fields))
            }
            Some(_) => Arc::new(datafusion::arrow::datatypes::Schema::empty()),
            None => self.schema.clone(),
        }
    }

    /// 使用 FilterExec 包装执行计划以应用无法通过索引处理的过滤器
    fn wrap_with_filter(
        &self,
        input: Arc<dyn ExecutionPlan>,
        filters: Vec<Expr>,
    ) -> Option<Arc<dyn ExecutionPlan>> {
        use datafusion::execution::context::SessionContext;
        use datafusion::physical_expr::create_physical_expr;
        use datafusion::physical_plan::filter::FilterExec;

        log::debug!(
            "🔧 [wrap_with_filter] Creating FilterExec for {} unsupported filters",
            filters.len()
        );
        log::debug!("🔧 [wrap_with_filter] Input schema: {:?}", input.schema());
        log::debug!("🔧 [wrap_with_filter] Filters: {:?}", filters);

        // 创建临时 session context 用于表达式转换
        let session_ctx = SessionContext::new();
        let _fulltext_context = register_fulltext_udfs(&session_ctx);
        let df_schema = input.schema().clone().to_dfschema().ok()?;

        log::debug!("🔧 [wrap_with_filter] DFSchema: {:?}", df_schema);

        // 将所有过滤器用 AND 连接
        let combined_filter = if filters.len() == 1 {
            filters.into_iter().next()?
        } else {
            let mut iter = filters.into_iter();
            let mut combined = iter.next()?;
            for filter in iter {
                combined = Expr::BinaryExpr(datafusion::logical_expr::BinaryExpr {
                    left: Box::new(combined),
                    op: datafusion::logical_expr::Operator::And,
                    right: Box::new(filter),
                });
            }
            combined
        };

        log::debug!(
            "🔧 [wrap_with_filter] Combined filter: {:?}",
            combined_filter
        );

        // 转换为物理表达式
        let physical_expr = create_physical_expr(
            &combined_filter,
            &df_schema,
            session_ctx.state().execution_props(),
        )
        .ok()?;

        log::debug!("🔧 [wrap_with_filter] Physical expression created successfully");

        // 创建 FilterExec
        let filter_exec = FilterExec::try_new(physical_expr, input).ok()?;

        log::debug!("🔧 [wrap_with_filter] FilterExec created successfully");

        Some(Arc::new(filter_exec))
    }

    fn expr_to_bitmap(&self, expr: &Expr) -> Option<RoaringBitmap> {
        match expr {
            // BETWEEN 表达式: col BETWEEN start AND end
            Expr::Between(between) => {
                if let Expr::Column(column) = &*between.expr {
                    let field_name = &column.name;

                    if let (Expr::Literal(low, _), Expr::Literal(high, _)) =
                        (&*between.low, &*between.high)
                    {
                        // BETWEEN 是闭区间 [low, high]
                        // NOT BETWEEN 的话需要取反
                        let bitmap = self.query_range(field_name, low, true, high, true)?;
                        return Some(if between.negated {
                            // NOT BETWEEN: 返回补集
                            // 这需要知道全集，暂时返回 None
                            return None;
                        } else {
                            bitmap
                        });
                    }
                }
                None
            }

            // IN 表达式: col IN (value1, value2, ...)
            Expr::InList(in_list) => {
                if let Expr::Column(column) = &*in_list.expr {
                    let field_name = &column.name;

                    // 将 IN 转换为多个等值查询的 OR
                    let mut combined_bitmap = RoaringBitmap::new();

                    for value_expr in &in_list.list {
                        if let Expr::Literal(scalar_value, _) = value_expr {
                            if let Some(bitmap) = self.query_equal(field_name, scalar_value) {
                                combined_bitmap |= bitmap;
                            }
                        }
                    }

                    // 如果是 NOT IN，返回补集
                    return Some(if in_list.negated {
                        &self.valid_docs - &combined_bitmap
                    } else {
                        combined_bitmap
                    });
                }
                None
            }

            // 二元表达式: col > value, col = value 等
            Expr::BinaryExpr(binary) => {
                use datafusion::logical_expr::Operator;

                // 处理常量表达式: Literal op Literal (如 1=1, 2=4)
                if let (Expr::Literal(left_val, _), Expr::Literal(right_val, _)) =
                    (&*binary.left, &*binary.right)
                {
                    let result = match binary.op {
                        Operator::Eq => left_val == right_val,
                        Operator::NotEq => left_val != right_val,
                        Operator::Lt => left_val < right_val,
                        Operator::LtEq => left_val <= right_val,
                        Operator::Gt => left_val > right_val,
                        Operator::GtEq => left_val >= right_val,
                        _ => return None, // 其他操作符不支持
                    };

                    // true 返回全量, false 返回空集
                    return Some(if result {
                        self.valid_docs.clone()
                    } else {
                        RoaringBitmap::new()
                    });
                }

                // 先处理 AND/OR 逻辑运算
                match binary.op {
                    Operator::And => {
                        // AND: 两边都需要 bitmap
                        let left_opt = self.expr_to_bitmap(&binary.left);
                        let right_opt = self.expr_to_bitmap(&binary.right);

                        // 使用引用进行位运算，避免不必要的克隆
                        let result = match (left_opt.as_ref(), right_opt.as_ref()) {
                            (Some(left), Some(right)) => left & right,
                            (Some(left), None) => left & &self.valid_docs,
                            (None, Some(right)) => &self.valid_docs & right,
                            (None, None) => self.valid_docs.clone(),
                        };
                        return Some(result);
                    }
                    Operator::Or => {
                        // OR: 任何一边无法处理，返回全量
                        let left_opt = self.expr_to_bitmap(&binary.left);
                        let right_opt = self.expr_to_bitmap(&binary.right);

                        // 使用引用进行位运算，避免不必要的克隆
                        let result = match (left_opt.as_ref(), right_opt.as_ref()) {
                            (Some(left), Some(right)) => left | right,
                            (Some(left), None) => left | &self.valid_docs,
                            (None, Some(right)) => &self.valid_docs | right,
                            (None, None) => self.valid_docs.clone(),
                        };
                        return Some(result);
                    }
                    _ => {}
                }

                if let Expr::Column(column) = &*binary.left {
                    let field_name = &column.name;

                    if let Expr::Literal(scalar_value, _) = &*binary.right {
                        match binary.op {
                            Operator::Eq => {
                                return self.query_equal(field_name, scalar_value);
                            }
                            // col != value -> NOT (col = value) -> valid_docs - equal_bitmap
                            Operator::NotEq => {
                                if let Some(eq_bitmap) = self.query_equal(field_name, scalar_value)
                                {
                                    return Some(&self.valid_docs - &eq_bitmap);
                                }
                                // 如果字段没有索引，返回 None 让 DataFusion 处理
                                return None;
                            }
                            // col > value -> range(value, false, +∞, true)
                            Operator::Gt => {
                                return self.query_range(
                                    field_name,
                                    scalar_value,
                                    false,
                                    &ScalarValue::Null,
                                    true,
                                );
                            }
                            // col >= value -> range(value, true, +∞, true)
                            Operator::GtEq => {
                                return self.query_range(
                                    field_name,
                                    scalar_value,
                                    true,
                                    &ScalarValue::Null,
                                    true,
                                );
                            }
                            // col < value -> range(-∞, true, value, false)
                            Operator::Lt => {
                                return self.query_range(
                                    field_name,
                                    &ScalarValue::Null,
                                    true,
                                    scalar_value,
                                    false,
                                );
                            }
                            // col <= value -> range(-∞, true, value, true)
                            Operator::LtEq => {
                                return self.query_range(
                                    field_name,
                                    &ScalarValue::Null,
                                    true,
                                    scalar_value,
                                    true,
                                );
                            }
                            _ => {}
                        }
                    }
                }
                None
            }
            _ => None,
        }
    }
    /// 查询等值条件
    /// 返回 None 的情况：
    /// 1. 字段没有索引（index_readers 中不存在）- 返回 None 让 DataFusion 全表扫描
    /// 2. IndexReader 无法处理这个查询（如 Keyword 类型查询数字）- 返回空 bitmap
    ///
    /// 注意: 返回 None 会让 AND/OR 逻辑将该条件视为"全表扫描"
    fn query_equal(
        &self,
        field_name: &str,
        value: &datafusion::scalar::ScalarValue,
    ) -> Option<RoaringBitmap> {
        match self.index_readers.get(field_name) {
            Some(reader) => {
                // 有索引,调用 reader.query()
                // 如果查询失败(类型不匹配等),返回空 bitmap
                let bitmap = reader.query(value).unwrap_or_else(RoaringBitmap::new);
                log::debug!(
                    "🔎 [query_equal] field='{}', value={:?}, result_count={}, thread={:?}",
                    field_name,
                    value,
                    bitmap.len(),
                    std::thread::current().id()
                );
                Some(bitmap)
            }
            None => {
                log::info!(
                    "⚠️  [SegmentScanner::query_equal] field '{}' has no index, returning None for full scan",
                    field_name
                );
                None
            }
        }
    }

    /// 查询范围条件
    /// 返回 None 的情况：
    /// 1. 字段没有索引（index_readers 中不存在）- 返回 None 让 DataFusion 全表扫描
    /// 2. 字段类型不支持范围查询（如 Keyword）- 返回空 bitmap
    ///
    /// 注意: 返回 None 会让 AND/OR 逻辑将该条件视为"全表扫描"
    fn query_range(
        &self,
        field_name: &str,
        start: &ScalarValue,
        start_inclusive: bool,
        end: &ScalarValue,
        end_inclusive: bool,
    ) -> Option<RoaringBitmap> {
        match self.index_readers.get(field_name) {
            Some(reader) => {
                log::debug!(
                    "🔍 [SegmentScanner::query_range] field={}, trying range_union first",
                    field_name
                );

                // 优先尝试 range_union() - 100-200x faster for large ranges
                if let Some(bitmap) = reader.range_union(start, start_inclusive, end, end_inclusive)
                {
                    log::debug!(
                        "🔍 [SegmentScanner::query_range] range_union returned {} docs",
                        bitmap.len()
                    );
                    return Some(bitmap);
                }

                log::debug!("🔍 [SegmentScanner::query_range] range_union returned None, falling back to range()");

                // 回退到普通 range() - 兼容不支持 range_union 的索引类型
                let range_result = reader.range(start, start_inclusive, end, end_inclusive);
                let is_some = range_result.is_some();
                let bitmap = range_result.unwrap_or_else(RoaringBitmap::new);
                log::info!(
                    "🔍 [SegmentScanner::query_range] range() returned {}, bitmap.len()={}",
                    if is_some { "Some" } else { "None" },
                    bitmap.len()
                );
                Some(bitmap)
            }
            None => {
                log::debug!(
                    "⚠️  [SegmentScanner::query_range] field '{}' has no index, returning None for full scan",
                    field_name
                );
                None
            }
        }
    }
}

/// SegmentExec - 自定义的 ExecutionPlan
///
/// 只扫描 matched_docs bitmap 中的 doc_ids
struct SegmentExec {
    raw_data: RowDataStore,
    #[allow(dead_code)]
    full_schema: SchemaRef, // 完整 schema (用于读取数据)
    projected_schema: SchemaRef, // 投影后的 schema (返回给 DataFusion)
    matched_docs: RoaringBitmap,
    projection: Option<Vec<usize>>, // 投影列索引
    /// 执行计划属性，在构造时创建并存储，避免每次调用 properties() 都创建
    properties: datafusion::physical_plan::PlanProperties,
    sort: Option<(String, bool)>,
    limit: Option<usize>,
}

impl SegmentExec {
    fn new(
        raw_data: RowDataStore,
        full_schema: SchemaRef,
        projected_schema: SchemaRef,
        matched_docs: RoaringBitmap,
        projection: Option<Vec<usize>>,
        sort: Option<(String, bool)>,
        limit: Option<usize>,
    ) -> Self {
        use datafusion::physical_expr::EquivalenceProperties;
        use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
        use datafusion::physical_plan::Partitioning;

        // 在构造时就创建 PlanProperties（只创建一次）
        // 注意: 使用 projected_schema 作为输出 schema
        let eq_properties = EquivalenceProperties::new(projected_schema.clone());
        let partitioning = Partitioning::UnknownPartitioning(1);
        // EmissionType::Final 表示 batch 之间没有重复的行，也没有更新关系
        // 每个 batch 包含不同的 doc_ids，是最终的、完整的数据
        let emission_type = EmissionType::Final;
        let boundedness = Boundedness::Bounded; // 有界数据（不是无限流）

        let properties = datafusion::physical_plan::PlanProperties::new(
            eq_properties,
            partitioning,
            emission_type,
            boundedness,
        );

        Self {
            raw_data,
            full_schema,
            projected_schema,
            matched_docs,
            projection,
            properties,
            sort,
            limit,
        }
    }
}
impl std::fmt::Debug for SegmentExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SegmentExec")
            .field("projected_schema", &self.projected_schema)
            .field("matched_docs_count", &self.matched_docs.len())
            .field("projection", &self.projection)
            .finish()
    }
}

impl std::fmt::Display for SegmentExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SegmentExec: matched_docs={}", self.matched_docs.len())
    }
}

impl DisplayAs for SegmentExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "SegmentExec: matched_docs={}", self.matched_docs.len())
    }
}

impl ExecutionPlan for SegmentExec {
    fn name(&self) -> &str {
        "SegmentExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
    }

    fn properties(&self) -> &datafusion::physical_plan::PlanProperties {
        // 直接返回存储在结构体中的 properties 引用
        // 没有内存泄漏，没有每次都创建新对象
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        // 这是叶子节点,没有子节点
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<datafusion::execution::TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        // 🚀 流式处理优化: 使用真正的异步流,按需分批生成数据
        // 避免一次性加载所有数据到内存,防止 OOM

        let projected_schema = self.projected_schema.clone();
        let raw_data = self.raw_data.clone();
        let matched_docs = self.matched_docs.clone();
        let projection = self.projection.clone();

        // LIMIT 下推：如果没有 SORT，在 Segment 层面应用 LIMIT
        let pushdown_limit = if self.sort.is_none() {
            self.limit
        } else {
            None
        };

        const CHUNK_SIZE: usize = 1000;

        log::debug!(
            "🔍 [SegmentExec::execute] Starting streaming execution, total matched_docs={}, pushdown_limit={:?}",
            matched_docs.len(),
            pushdown_limit
        );

        Ok(Box::pin(SegmentStream::new(
            projected_schema,
            raw_data,
            matched_docs,
            projection.unwrap_or_default(),
            CHUNK_SIZE,
            pushdown_limit,
        )))
    }
}

/// 真正的流式 RecordBatchStream 实现
///
/// 按需分批读取数据,避免一次性加载全部到内存
struct SegmentStream {
    schema: SchemaRef,
    reader: RowDataStoreReader,
    doc_count: usize,
    doc_ids_iter: std::iter::Peekable<roaring::bitmap::IntoIter>,
    chunk_size: usize,
    limit: Option<usize>,
    rows_returned: i32,
    pending_batches: std::vec::IntoIter<RecordBatch>,
}

impl SegmentStream {
    fn new(
        schema: SchemaRef,
        store: RowDataStore,
        matched_docs: RoaringBitmap,
        projection: Vec<usize>,
        chunk_size: usize,
        limit: Option<usize>,
    ) -> Self {
        let reader = RowDataStoreReader::new(store, projection);

        Self {
            schema,
            reader,
            doc_count: matched_docs.len() as usize,
            doc_ids_iter: matched_docs.into_iter().peekable(),
            chunk_size,
            limit,
            rows_returned: 0,
            pending_batches: Vec::new().into_iter(),
        }
    }

    /// 批量读取数据并返回 RecordBatch
    ///
    /// 策略：收集一批 doc_ids → 批量查找分组 → 批量读取 → 返回合并后的单个 batch
    fn generate_next_chunk(&mut self) -> DFResult<Option<RecordBatch>> {
        use datafusion::arrow::compute::concat_batches;

        // LIMIT 下推检查
        if self.limit.is_some_and(|l| self.rows_returned >= l as i32)
            || self.doc_count <= self.rows_returned as usize
            || self.doc_count == 0
        {
            return Ok(None);
        }

        //先判断是否是空投影（COUNT 优化）
        if self.reader.is_empty_projection() {
            // 空投影，直接返回行数
            let batch = RecordBatch::try_new_with_options(
                self.schema.clone(),
                vec![],
                &datafusion::arrow::record_batch::RecordBatchOptions::new()
                    .with_row_count(Some(self.doc_count)),
            )?;
            self.rows_returned += self.doc_count as i32;
            return Ok(Some(batch));
        }

        // 计算本次最多取多少行
        let max_rows = match self.limit {
            Some(limit) => {
                let remaining = (limit as i32 - self.rows_returned) as usize;
                if remaining == 0 {
                    return Ok(None);
                }
                // 如果有 limit，严格控制不超过 remaining
                remaining
            }
            None => {
                // 如果没有 limit，用 chunk_size 控制（约 chunk_size * 1000 行）
                self.chunk_size * 1000
            }
        };

        let mut batches = Vec::new();
        let mut total_rows = 0;

        // 循环读取 rowgroup，直到达到 max_rows 或 iter 空了
        let start = std::time::Instant::now();
        while total_rows < max_rows {
            if max_rows <= total_rows {
                break;
            }

            let remaining = max_rows - total_rows;

            // 调用 next_group 读取一个完整的 rowgroup（或其中的 remaining 行）
            match self
                .reader
                .next_group(&mut self.doc_ids_iter, remaining)
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?
            {
                Some(batch) => {
                    let batch_rows = batch.num_rows();
                    total_rows += batch_rows;
                    batches.push(batch);

                    log::debug!(
                        "🔍 [generate_next_chunk] Read rowgroup batch: {} rows, total so far: {}",
                        batch_rows,
                        total_rows
                    );
                }
                None => {
                    log::debug!("🔍 [generate_next_chunk] No more data from next_group");
                    break;
                }
            }
        }

        log::info!(
            "🔍 [generate_next_chunk] type:{} Completed reading chunk: max_rows={}, total_rows={}, batches={}, elapsed={:?}",
            self.reader.segment_type(),
            max_rows,
            total_rows,
            batches.len(),
            start.elapsed()
        );

        if batches.is_empty() {
            return Ok(None);
        }

        self.rows_returned += total_rows as i32;

        // 合并 batches
        if batches.len() == 1 {
            return Ok(Some(batches.into_iter().next().unwrap()));
        }

        let merged = concat_batches(&self.schema, &batches)
            .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))?;

        Ok(Some(merged))
    }
}

impl RecordBatchStream for SegmentStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl futures::Stream for SegmentStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // 1. 如果有待处理的 batch,先返回
        if let Some(batch) = self.pending_batches.next() {
            return Poll::Ready(Some(Ok(batch)));
        }

        // 2. 生成下一批数据
        match self.generate_next_chunk() {
            Ok(Some(batch)) => Poll::Ready(Some(Ok(batch))),
            Ok(None) => Poll::Ready(None), // 没有更多数据
            Err(e) => Poll::Ready(Some(Err(e))),
        }
    }
}

fn should_emit_internal_id_for_projection(
    emit_internal_id: bool,
    internal_id_index: Option<usize>,
    projection: Option<&[usize]>,
) -> bool {
    if !emit_internal_id {
        return false;
    }

    match projection {
        Some(indices) => internal_id_index
            .map(|internal_idx| indices.iter().any(|idx| *idx == internal_idx))
            .unwrap_or(false),
        None => true,
    }
}

fn storage_projection_indices(
    projection: Option<&[usize]>,
    internal_id_index: Option<usize>,
) -> Option<Vec<usize>> {
    projection.map(|indices| {
        if let Some(internal_idx) = internal_id_index {
            indices
                .iter()
                .filter(|&&idx| idx != internal_idx)
                .cloned()
                .collect()
        } else {
            indices.to_vec()
        }
    })
}

fn build_internal_id_array(
    segment_start: u64,
    doc_ids: &[u32],
) -> datafusion::arrow::array::ArrayRef {
    use datafusion::arrow::array::{ArrayRef, UInt32Builder};

    let mut builder = UInt32Builder::with_capacity(doc_ids.len());
    for doc_id in doc_ids {
        if (*doc_id as u64) < segment_start {
            log::warn!(
                "⚠️  doc_id={} is less than segment_start={} when building _internal_id column",
                doc_id,
                segment_start
            );
        }

        builder.append_value(*doc_id);
    }

    Arc::new(builder.finish()) as ArrayRef
}
