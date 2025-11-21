use super::query_converter::{EsQuery, QueryConverter, QueryExecutor};
use crate::engine::Engine;
use crate::utils::error::CoreError;
use serde_json::{json, Value};
use std::sync::Arc;

/// 搜索引擎
pub struct SearchEngine {
    engine: Arc<Engine>,
}

impl SearchEngine {
    pub fn new(engine: Arc<Engine>) -> Self {
        Self { engine }
    }

    /// 执行搜索请求
    pub async fn search(&self, index: &str, search_request: &Value) -> Result<Value, CoreError> {
        // 获取表的元数据
        let meta = self
            .engine
            .get_table_meta(index)
            .map_err(|_| CoreError::NotExisted(format!("Index '{}' not found", index)))?;

        // 解析搜索请求
        let query = self.parse_query(search_request)?;
        let from = search_request
            .get("from")
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as usize;
        let size = search_request
            .get("size")
            .and_then(|v| v.as_u64())
            .unwrap_or(10) as usize;

        // 从所有分区收集数据
        let mut all_docs = Vec::new();
        for partition_index in 0..meta.parallel_workers {
            let partition_id = meta.partition_strategy.generate_partition_id(index, partition_index, None);
            if let Some(partition) = self.engine.get_partition(index, &partition_id).await {
                // 从分区读取数据
                let partition_docs = self.read_partition_data(&partition, index).await?;
                all_docs.extend(partition_docs);
            }
        }

        // 应用查询过滤
        let filtered_docs = if let Some(ref query) = query {
            QueryExecutor::execute_query(&all_docs, query)
        } else {
            all_docs
        };

        // 应用排序
        let sorted_docs = self.apply_sorting(filtered_docs, search_request);

        // 应用分页
        let total = sorted_docs.len();
        let hits: Vec<Value> = sorted_docs
            .into_iter()
            .skip(from)
            .take(size)
            .enumerate()
            .map(|(idx, doc)| {
                let id = doc
                    .get("_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or(&format!("doc_{}", from + idx))
                    .to_string();

                json!({
                    "_index": index,
                    "_id": id,
                    "_score": self.calculate_score(&doc, &query),
                    "_source": doc
                })
            })
            .collect();

        // 构建响应
        Ok(json!({
            "took": 5,
            "timed_out": false,
            "_shards": {
                "total": meta.parallel_workers,
                "successful": meta.parallel_workers,
                "skipped": 0,
                "failed": 0
            },
            "hits": {
                "total": {
                    "value": total,
                    "relation": "eq"
                },
                "max_score": 1.0,
                "hits": hits
            }
        }))
    }

    /// 解析查询条件
    fn parse_query(&self, search_request: &Value) -> Result<Option<EsQuery>, CoreError> {
        if let Some(query) = search_request.get("query") {
            let query_filter = QueryConverter::convert_query(query);
            Ok(Some(EsQuery::Filter(query_filter)))
        } else {
            Ok(Some(EsQuery::MatchAll))
        }
    }

    /// 从分区读取数据
    async fn read_partition_data(
        &self,
        _partition: &crate::partition::Partition,
        _index: &str,
    ) -> Result<Vec<Value>, CoreError> {
        let docs = Vec::new();

        // TODO: Implement actual data retrieval from partition
        // Currently, the Partition struct has current_segment and frozen_segments,
        // but they are not directly accessible via public API
        // This is a placeholder implementation

        Ok(docs)
    }

    /// 从段读取数据
    async fn read_segment_data(
        &self,
        _segment: &crate::segment::Segment,
        _index: &str,
    ) -> Result<Vec<Value>, CoreError> {
        let docs = Vec::new();

        // TODO: Implement actual data retrieval from segment
        // Currently, the Segment struct does not expose row_data_store
        // This is a placeholder implementation

        Ok(docs)
    }

    /// 应用排序
    fn apply_sorting(&self, mut docs: Vec<Value>, search_request: &Value) -> Vec<Value> {
        if let Some(sort) = search_request.get("sort") {
            if let Some(sort_array) = sort.as_array() {
                for sort_spec in sort_array {
                    if let Some(sort_obj) = sort_spec.as_object() {
                        for (field, order_spec) in sort_obj {
                            let ascending = match order_spec {
                                Value::String(s) => s != "desc",
                                Value::Object(obj) => obj
                                    .get("order")
                                    .and_then(|v| v.as_str())
                                    .map(|s| s != "desc")
                                    .unwrap_or(true),
                                _ => true,
                            };

                            docs.sort_by(|a, b| {
                                let a_val = self.get_sort_value(a, field);
                                let b_val = self.get_sort_value(b, field);

                                let cmp = a_val
                                    .partial_cmp(&b_val)
                                    .unwrap_or(std::cmp::Ordering::Equal);
                                if ascending {
                                    cmp
                                } else {
                                    cmp.reverse()
                                }
                            });
                        }
                    }
                }
            }
        }

        docs
    }

    /// 获取排序值
    fn get_sort_value(&self, doc: &Value, field: &str) -> SortValue {
        if let Some(value) = self.get_field_value(doc, field) {
            match value {
                Value::Number(n) => {
                    if let Some(f) = n.as_f64() {
                        SortValue::Number(f)
                    } else {
                        SortValue::String(value.to_string())
                    }
                }
                Value::String(s) => SortValue::String(s.clone()),
                _ => SortValue::String(value.to_string()),
            }
        } else {
            SortValue::Null
        }
    }

    /// 获取字段值
    fn get_field_value<'a>(&self, doc: &'a Value, field: &str) -> Option<&'a Value> {
        let parts: Vec<&str> = field.split('.').collect();
        let mut current = doc;

        for part in parts {
            if let Some(obj) = current.as_object() {
                current = obj.get(part)?;
            } else {
                return None;
            }
        }

        Some(current)
    }

    /// 计算文档得分
    fn calculate_score(&self, _doc: &Value, query: &Option<EsQuery>) -> f64 {
        match query {
            Some(EsQuery::MatchAll) => 1.0,
            Some(_) => 1.0, // 简单实现，所有匹配文档得分都是 1.0
            None => 1.0,
        }
    }
}

/// 排序值类型
#[derive(Debug, Clone, PartialEq, PartialOrd)]
enum SortValue {
    Null,
    Number(f64),
    String(String),
}
