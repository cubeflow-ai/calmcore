use crate::schema::Schema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

/// Elasticsearch 聚合请求
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Aggregations {
    #[serde(flatten)]
    pub aggs: HashMap<String, AggregationContainer>,
}

/// 聚合容器
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(untagged)]
pub enum AggregationContainer {
    Terms(TermsAggregation),
    DateHistogram(DateHistogramAggregation),
    Histogram(HistogramAggregation),
    Stats(StatsAggregation),
    Sum(MetricAggregation),
    Avg(MetricAggregation),
    Min(MetricAggregation),
    Max(MetricAggregation),
    Count(MetricAggregation),
}

/// Terms 聚合（类似 GROUP BY）
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TermsAggregation {
    pub terms: TermsConfig,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TermsConfig {
    /// 字段名或脚本
    #[serde(skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,

    /// 脚本（用于计算字段）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub script: Option<Script>,

    /// 返回的桶数量
    #[serde(default = "default_size")]
    pub size: usize,

    /// 最小文档数
    #[serde(default = "default_min_doc_count")]
    pub min_doc_count: usize,

    /// 排序
    #[serde(skip_serializing_if = "Option::is_none")]
    pub order: Option<Vec<HashMap<String, String>>>,

    /// 其他字段（兼容性）
    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

/// 脚本配置
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Script {
    pub source: String,
    #[serde(default = "default_lang")]
    pub lang: String,
}

/// Date Histogram 聚合
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DateHistogramAggregation {
    pub date_histogram: DateHistogramConfig,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DateHistogramConfig {
    pub field: String,
    pub calendar_interval: Option<String>,
    pub fixed_interval: Option<String>,
    pub format: Option<String>,
}

/// Histogram 聚合
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct HistogramAggregation {
    pub histogram: HistogramConfig,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct HistogramConfig {
    pub field: String,
    pub interval: f64,
}

/// 统计聚合
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct StatsAggregation {
    pub stats: MetricConfig,
}

/// 指标聚合（sum, avg, min, max, count）
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MetricAggregation {
    #[serde(flatten)]
    pub config: HashMap<String, MetricConfig>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MetricConfig {
    pub field: String,
}

// 默认值函数
fn default_size() -> usize {
    10
}

fn default_min_doc_count() -> usize {
    1
}

fn default_lang() -> String {
    "painless".to_string()
}

/// 将 ES 聚合转换为 SQL
pub fn convert_aggregations_to_sql(
    aggs: &Aggregations,
    schema: &Schema,
) -> Result<AggregationSql, String> {
    let mut agg_sql = AggregationSql::default();

    for (agg_name, agg_container) in &aggs.aggs {
        match agg_container {
            AggregationContainer::Terms(terms_agg) => {
                convert_terms_agg(agg_name, terms_agg, schema, &mut agg_sql)?;
            }
            AggregationContainer::DateHistogram(date_hist) => {
                convert_date_histogram_agg(agg_name, date_hist, schema, &mut agg_sql)?;
            }
            AggregationContainer::Histogram(hist) => {
                convert_histogram_agg(agg_name, hist, schema, &mut agg_sql)?;
            }
            AggregationContainer::Stats(stats) => {
                convert_stats_agg(agg_name, stats, schema, &mut agg_sql)?;
            }
            AggregationContainer::Sum(metric) => {
                convert_metric_agg(agg_name, "SUM", metric, schema, &mut agg_sql)?;
            }
            AggregationContainer::Avg(metric) => {
                convert_metric_agg(agg_name, "AVG", metric, schema, &mut agg_sql)?;
            }
            AggregationContainer::Min(metric) => {
                convert_metric_agg(agg_name, "MIN", metric, schema, &mut agg_sql)?;
            }
            AggregationContainer::Max(metric) => {
                convert_metric_agg(agg_name, "MAX", metric, schema, &mut agg_sql)?;
            }
            AggregationContainer::Count(metric) => {
                convert_metric_agg(agg_name, "COUNT", metric, schema, &mut agg_sql)?;
            }
        }
    }

    Ok(agg_sql)
}

/// SQL 聚合表示
#[derive(Debug, Default)]
pub struct AggregationSql {
    pub select_fields: Vec<String>,
    pub group_by_fields: Vec<String>,
    pub having_clause: Option<String>,
    pub order_by: Vec<String>,
    pub limit: Option<usize>,
    /// 聚合名称和对应的 SQL 表达式
    pub agg_expressions: Vec<(String, String)>,
}

/// 转换 Terms 聚合
fn convert_terms_agg(
    agg_name: &str,
    terms_agg: &TermsAggregation,
    _schema: &Schema,
    agg_sql: &mut AggregationSql,
) -> Result<(), String> {
    let config = &terms_agg.terms;

    // 处理字段或脚本
    let (select_expr, group_by_fields) = if let Some(script) = &config.script {
        // 脚本: 解析 painless 脚本转为 SQL
        let sql_expr = parse_painless_script(&script.source)?;
        (
            format!("{} AS {}", sql_expr, agg_name),
            vec![agg_name.to_string()],
        )
    } else if let Some(field) = &config.field {
        // 单个字段
        (field.clone(), vec![field.clone()])
    } else {
        return Err("Terms aggregation must have either 'field' or 'script'".to_string());
    };

    agg_sql.select_fields.push(select_expr);
    agg_sql.group_by_fields.extend(group_by_fields);

    // COUNT(*) 用于文档计数
    agg_sql
        .agg_expressions
        .push(("doc_count".to_string(), "COUNT(*)".to_string()));

    // 处理 min_doc_count
    if config.min_doc_count > 1 {
        agg_sql.having_clause = Some(format!("COUNT(*) >= {}", config.min_doc_count));
    }

    // 处理排序
    if let Some(order) = &config.order {
        for order_item in order {
            for (key, direction) in order_item {
                let order_expr = if key == "_count" {
                    format!("COUNT(*) {}", direction.to_uppercase())
                } else if key == "_key" {
                    format!("{} {}", agg_name, direction.to_uppercase())
                } else {
                    format!("{} {}", key, direction.to_uppercase())
                };
                agg_sql.order_by.push(order_expr);
            }
        }
    }

    // 限制返回数量
    agg_sql.limit = Some(config.size);

    Ok(())
}

/// 转换 Date Histogram 聚合
fn convert_date_histogram_agg(
    agg_name: &str,
    _date_hist: &DateHistogramAggregation,
    _schema: &Schema,
    _agg_sql: &mut AggregationSql,
) -> Result<(), String> {
    // TODO: 实现日期直方图转换
    log::warn!(
        "⚠️  [ES Agg] Date histogram not yet implemented for {}",
        agg_name
    );
    Err("Date histogram aggregation not yet implemented".to_string())
}

/// 转换 Histogram 聚合
fn convert_histogram_agg(
    agg_name: &str,
    _hist: &HistogramAggregation,
    _schema: &Schema,
    _agg_sql: &mut AggregationSql,
) -> Result<(), String> {
    // TODO: 实现直方图转换
    log::warn!(
        "⚠️  [ES Agg] Histogram not yet implemented for {}",
        agg_name
    );
    Err("Histogram aggregation not yet implemented".to_string())
}

/// 转换 Stats 聚合
fn convert_stats_agg(
    agg_name: &str,
    _stats: &StatsAggregation,
    _schema: &Schema,
    _agg_sql: &mut AggregationSql,
) -> Result<(), String> {
    // TODO: 实现统计聚合
    log::warn!("⚠️  [ES Agg] Stats not yet implemented for {}", agg_name);
    Err("Stats aggregation not yet implemented".to_string())
}

/// 转换指标聚合
fn convert_metric_agg(
    agg_name: &str,
    _metric_type: &str,
    _metric: &MetricAggregation,
    _schema: &Schema,
    _agg_sql: &mut AggregationSql,
) -> Result<(), String> {
    // TODO: 实现指标聚合
    log::warn!("⚠️  [ES Agg] Metric not yet implemented for {}", agg_name);
    Err("Metric aggregation not yet implemented".to_string())
}

/// 解析 Painless 脚本转为 SQL 表达式
/// 例如: "doc['field1'].value + '|' + doc['field2'].value"
/// 转为: "CONCAT(field1, '|', field2)"
fn parse_painless_script(script: &str) -> Result<String, String> {
    log::debug!("🔍 [Painless] Parsing script: {}", script);

    // 简单的正则解析: doc['field'].value 模式
    let re = regex::Regex::new(r"doc\['([^']+)'\]\.value").map_err(|e| e.to_string())?;

    // 提取所有字段
    let mut fields = Vec::new();
    for cap in re.captures_iter(script) {
        if let Some(field) = cap.get(1) {
            fields.push(field.as_str().to_string());
        }
    }

    if fields.is_empty() {
        return Err("No fields found in painless script".to_string());
    }

    // 检查是否是字符串拼接（包含 + '|' +）
    if script.contains(" + '") || script.contains(" + \"") {
        // 构建 CONCAT 表达式
        let concat_parts: Vec<String> = fields.iter().cloned().collect();

        // 从脚本中提取分隔符
        let separators = extract_separators(script);

        if !separators.is_empty() {
            // 交错插入字段和分隔符
            let mut result_parts = Vec::new();
            for (i, field) in fields.iter().enumerate() {
                result_parts.push(field.clone());
                if i < separators.len() {
                    result_parts.push(format!("'{}'", separators[i]));
                }
            }
            Ok(format!("CONCAT({})", result_parts.join(", ")))
        } else {
            // 简单拼接
            Ok(format!("CONCAT({})", concat_parts.join(", ")))
        }
    } else {
        // 单个字段或其他表达式
        Ok(fields[0].clone())
    }
}

/// 从脚本中提取字符串分隔符
fn extract_separators(script: &str) -> Vec<String> {
    let mut separators = Vec::new();

    // 匹配 + 'xxx' + 或 + "xxx" +
    let re = regex::Regex::new(r#"\+\s*['"]([^'"]+)['"]\s*\+"#).unwrap();

    for cap in re.captures_iter(script) {
        if let Some(sep) = cap.get(1) {
            separators.push(sep.as_str().to_string());
        }
    }

    separators
}

/// 构建完整的聚合 SQL
pub fn build_aggregation_sql(
    base_query: &str,
    where_clause: &str,
    agg_sql: &AggregationSql,
) -> String {
    let mut sql = String::new();

    // SELECT 子句
    sql.push_str("SELECT ");

    let mut select_parts = Vec::new();

    // 添加分组字段
    for field in &agg_sql.select_fields {
        select_parts.push(field.clone());
    }

    // 添加聚合表达式
    for (alias, expr) in &agg_sql.agg_expressions {
        select_parts.push(format!("{} AS {}", expr, alias));
    }

    sql.push_str(&select_parts.join(", "));

    // FROM 子句
    sql.push_str(&format!(" FROM {}", base_query));

    // WHERE 子句
    if !where_clause.is_empty() {
        sql.push_str(where_clause);
    }

    // GROUP BY 子句
    if !agg_sql.group_by_fields.is_empty() {
        sql.push_str(" GROUP BY ");
        sql.push_str(&agg_sql.group_by_fields.join(", "));
    }

    // HAVING 子句
    if let Some(having) = &agg_sql.having_clause {
        sql.push_str(&format!(" HAVING {}", having));
    }

    // ORDER BY 子句
    if !agg_sql.order_by.is_empty() {
        sql.push_str(" ORDER BY ");
        sql.push_str(&agg_sql.order_by.join(", "));
    }

    // LIMIT 子句
    if let Some(limit) = agg_sql.limit {
        sql.push_str(&format!(" LIMIT {}", limit));
    }

    sql
}

/// 格式化聚合结果为 ES 响应格式
pub fn format_aggregation_response(
    agg_name: &str,
    records: Vec<serde_json::Map<String, Value>>,
    use_typed_keys: bool,
) -> serde_json::Map<String, Value> {
    // 构建桶
    let buckets: Vec<Value> = records
        .iter()
        .map(|record| {
            let mut bucket = serde_json::Map::new();

            // key: 分组键
            if let Some(key_value) = record.get(agg_name) {
                bucket.insert("key".to_string(), key_value.clone());
            }

            // doc_count: 文档数
            if let Some(count) = record.get("doc_count") {
                bucket.insert("doc_count".to_string(), count.clone());
            }

            Value::Object(bucket)
        })
        .collect();

    let mut agg_result = serde_json::Map::new();
    let mut agg_details = serde_json::Map::new();
    agg_details.insert("doc_count_error_upper_bound".to_string(), Value::from(0));
    agg_details.insert("sum_other_doc_count".to_string(), Value::from(0));
    agg_details.insert("buckets".to_string(), Value::Array(buckets));

    // 当使用 typed_keys 时，需要在聚合名称前加上类型前缀
    // sterms = string terms, lterms = long terms
    let final_agg_name = if use_typed_keys {
        format!("sterms#{}", agg_name)
    } else {
        agg_name.to_string()
    };

    agg_result.insert(final_agg_name, Value::Object(agg_details));
    agg_result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_painless_concat() {
        let script = "doc['field1'].value + '|' + doc['field2'].value + '|' + doc['field3'].value";
        let result = parse_painless_script(script).unwrap();
        assert_eq!(result, "CONCAT(field1, '|', field2, '|', field3)");
    }

    #[test]
    fn test_parse_painless_single_field() {
        let script = "doc['status'].value";
        let result = parse_painless_script(script).unwrap();
        assert_eq!(result, "status");
    }
}
