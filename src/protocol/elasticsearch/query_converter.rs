use serde_json::Value;
use std::collections::HashMap;

/// Elasticsearch 查询枚举
#[derive(Debug, Clone)]
pub enum EsQuery {
    MatchAll,
    Filter(QueryFilter),
}

/// Elasticsearch 查询转换器
/// 负责将 ES 查询 DSL 转换为内部查询条件
pub struct QueryConverter;

impl QueryConverter {
    /// 将 Elasticsearch 查询转换为过滤条件
    pub fn convert_query(query: &Value) -> QueryFilter {
        if let Some(query_obj) = query.as_object() {
            // match_all 查询
            if query_obj.contains_key("match_all") {
                return QueryFilter::MatchAll;
            }

            // term 查询
            if let Some(term) = query_obj.get("term") {
                if let Some(term_obj) = term.as_object() {
                    let mut conditions = HashMap::new();
                    for (field, value) in term_obj {
                        conditions.insert(field.clone(), TermCondition::Exact(value.clone()));
                    }
                    return QueryFilter::Term(conditions);
                }
            }

            // match 查询
            if let Some(match_query) = query_obj.get("match") {
                if let Some(match_obj) = match_query.as_object() {
                    let mut conditions = HashMap::new();
                    for (field, value) in match_obj {
                        conditions.insert(field.clone(), TermCondition::Match(value.clone()));
                    }
                    return QueryFilter::Match(conditions);
                }
            }

            // range 查询
            if let Some(range) = query_obj.get("range") {
                if let Some(range_obj) = range.as_object() {
                    let mut conditions = HashMap::new();
                    for (field, range_spec) in range_obj {
                        if let Some(range_spec_obj) = range_spec.as_object() {
                            let mut range_condition = RangeCondition::default();

                            if let Some(gte) = range_spec_obj.get("gte") {
                                range_condition.gte = Some(gte.clone());
                            }
                            if let Some(gt) = range_spec_obj.get("gt") {
                                range_condition.gt = Some(gt.clone());
                            }
                            if let Some(lte) = range_spec_obj.get("lte") {
                                range_condition.lte = Some(lte.clone());
                            }
                            if let Some(lt) = range_spec_obj.get("lt") {
                                range_condition.lt = Some(lt.clone());
                            }

                            conditions.insert(field.clone(), range_condition);
                        }
                    }
                    return QueryFilter::Range(conditions);
                }
            }

            // bool 查询
            if let Some(bool_query) = query_obj.get("bool") {
                return Self::convert_bool_query(bool_query);
            }

            // exists 查询
            if let Some(exists) = query_obj.get("exists") {
                if let Some(field) = exists.get("field").and_then(|f| f.as_str()) {
                    return QueryFilter::Exists(field.to_string());
                }
            }

            // wildcard 查询
            if let Some(wildcard) = query_obj.get("wildcard") {
                if let Some(wildcard_obj) = wildcard.as_object() {
                    let mut conditions = HashMap::new();
                    for (field, pattern) in wildcard_obj {
                        if let Some(pattern_str) = pattern.as_str() {
                            conditions.insert(field.clone(), pattern_str.to_string());
                        }
                    }
                    return QueryFilter::Wildcard(conditions);
                }
            }
        }

        QueryFilter::MatchAll
    }

    /// 转换 bool 查询
    fn convert_bool_query(bool_query: &Value) -> QueryFilter {
        if let Some(bool_obj) = bool_query.as_object() {
            let mut bool_filter = BoolFilter::default();

            if let Some(must) = bool_obj.get("must") {
                if let Some(must_array) = must.as_array() {
                    for clause in must_array {
                        bool_filter.must.push(Self::convert_query(clause));
                    }
                }
            }

            if let Some(should) = bool_obj.get("should") {
                if let Some(should_array) = should.as_array() {
                    for clause in should_array {
                        bool_filter.should.push(Self::convert_query(clause));
                    }
                }
            }

            if let Some(must_not) = bool_obj.get("must_not") {
                if let Some(must_not_array) = must_not.as_array() {
                    for clause in must_not_array {
                        bool_filter.must_not.push(Self::convert_query(clause));
                    }
                }
            }

            if let Some(minimum_should_match) = bool_obj.get("minimum_should_match") {
                if let Some(min_match) = minimum_should_match.as_u64() {
                    bool_filter.minimum_should_match = min_match as usize;
                }
            }

            return QueryFilter::Bool(bool_filter);
        }

        QueryFilter::MatchAll
    }
}

/// 查询过滤器枚举
#[derive(Debug, Clone)]
pub enum QueryFilter {
    MatchAll,
    Term(HashMap<String, TermCondition>),
    Match(HashMap<String, TermCondition>),
    Range(HashMap<String, RangeCondition>),
    Bool(BoolFilter),
    Exists(String),
    Wildcard(HashMap<String, String>),
}

/// 词条条件
#[derive(Debug, Clone)]
pub enum TermCondition {
    Exact(Value),
    Match(Value),
}

/// 范围条件
#[derive(Debug, Clone, Default)]
pub struct RangeCondition {
    pub gte: Option<Value>,
    pub gt: Option<Value>,
    pub lte: Option<Value>,
    pub lt: Option<Value>,
}

/// Bool 查询过滤器
#[derive(Debug, Clone, Default)]
pub struct BoolFilter {
    pub must: Vec<QueryFilter>,
    pub should: Vec<QueryFilter>,
    pub must_not: Vec<QueryFilter>,
    pub minimum_should_match: usize,
}

impl QueryFilter {
    /// 检查文档是否匹配查询条件
    pub fn matches(&self, doc: &Value) -> bool {
        match self {
            QueryFilter::MatchAll => true,
            QueryFilter::Term(conditions) => conditions
                .iter()
                .all(|(field, condition)| Self::check_term_condition(doc, field, condition)),
            QueryFilter::Match(conditions) => conditions
                .iter()
                .all(|(field, condition)| Self::check_match_condition(doc, field, condition)),
            QueryFilter::Range(conditions) => conditions
                .iter()
                .all(|(field, range)| Self::check_range_condition(doc, field, range)),
            QueryFilter::Bool(bool_filter) => Self::check_bool_condition(doc, bool_filter),
            QueryFilter::Exists(field) => Self::check_field_exists(doc, field),
            QueryFilter::Wildcard(conditions) => conditions
                .iter()
                .all(|(field, pattern)| Self::check_wildcard_condition(doc, field, pattern)),
        }
    }

    /// 检查词条条件
    fn check_term_condition(doc: &Value, field: &str, condition: &TermCondition) -> bool {
        if let Some(doc_obj) = doc.as_object() {
            if let Some(field_value) = doc_obj.get(field) {
                match condition {
                    TermCondition::Exact(expected) => field_value == expected,
                    TermCondition::Match(expected) => {
                        // 简单的文本匹配，可以扩展为更复杂的匹配逻辑
                        Self::text_match(field_value, expected)
                    }
                }
            } else {
                false
            }
        } else {
            false
        }
    }

    /// 检查匹配条件
    fn check_match_condition(doc: &Value, field: &str, condition: &TermCondition) -> bool {
        Self::check_term_condition(doc, field, condition)
    }

    /// 检查范围条件
    fn check_range_condition(doc: &Value, field: &str, range: &RangeCondition) -> bool {
        if let Some(doc_obj) = doc.as_object() {
            if let Some(field_value) = doc_obj.get(field) {
                // 检查 gte
                if let Some(gte) = &range.gte {
                    if !Self::compare_values(field_value, gte, ">=") {
                        return false;
                    }
                }

                // 检查 gt
                if let Some(gt) = &range.gt {
                    if !Self::compare_values(field_value, gt, ">") {
                        return false;
                    }
                }

                // 检查 lte
                if let Some(lte) = &range.lte {
                    if !Self::compare_values(field_value, lte, "<=") {
                        return false;
                    }
                }

                // 检查 lt
                if let Some(lt) = &range.lt {
                    if !Self::compare_values(field_value, lt, "<") {
                        return false;
                    }
                }

                true
            } else {
                false
            }
        } else {
            false
        }
    }

    /// 检查 bool 条件
    fn check_bool_condition(doc: &Value, bool_filter: &BoolFilter) -> bool {
        // 检查 must 条件（所有都必须匹配）
        for condition in &bool_filter.must {
            if !condition.matches(doc) {
                return false;
            }
        }

        // 检查 must_not 条件（所有都不能匹配）
        for condition in &bool_filter.must_not {
            if condition.matches(doc) {
                return false;
            }
        }

        // 检查 should 条件（至少 minimum_should_match 个匹配）
        if !bool_filter.should.is_empty() {
            let should_matches = bool_filter
                .should
                .iter()
                .filter(|condition| condition.matches(doc))
                .count();

            if should_matches < bool_filter.minimum_should_match {
                return false;
            }
        }

        true
    }

    /// 检查字段是否存在
    fn check_field_exists(doc: &Value, field: &str) -> bool {
        if let Some(doc_obj) = doc.as_object() {
            doc_obj.contains_key(field)
        } else {
            false
        }
    }

    /// 检查通配符条件
    fn check_wildcard_condition(doc: &Value, field: &str, pattern: &str) -> bool {
        if let Some(doc_obj) = doc.as_object() {
            if let Some(field_value) = doc_obj.get(field) {
                if let Some(field_str) = field_value.as_str() {
                    return Self::wildcard_match(field_str, pattern);
                }
            }
        }
        false
    }

    /// 文本匹配逻辑
    fn text_match(field_value: &Value, expected: &Value) -> bool {
        match (field_value, expected) {
            (Value::String(field_str), Value::String(expected_str)) => field_str
                .to_lowercase()
                .contains(&expected_str.to_lowercase()),
            _ => field_value == expected,
        }
    }

    /// 比较两个值
    fn compare_values(left: &Value, right: &Value, op: &str) -> bool {
        match (left, right) {
            (Value::Number(l), Value::Number(r)) => {
                if let (Some(l_f64), Some(r_f64)) = (l.as_f64(), r.as_f64()) {
                    match op {
                        ">=" => l_f64 >= r_f64,
                        ">" => l_f64 > r_f64,
                        "<=" => l_f64 <= r_f64,
                        "<" => l_f64 < r_f64,
                        _ => false,
                    }
                } else {
                    false
                }
            }
            (Value::String(l), Value::String(r)) => match op {
                ">=" => l >= r,
                ">" => l > r,
                "<=" => l <= r,
                "<" => l < r,
                _ => false,
            },
            _ => false,
        }
    }

    /// 通配符匹配
    fn wildcard_match(text: &str, pattern: &str) -> bool {
        // 简单的通配符实现，支持 * 和 ?
        let regex_pattern = pattern.replace("*", ".*").replace("?", ".");

        if let Ok(regex) = regex::Regex::new(&format!("^{}$", regex_pattern)) {
            regex.is_match(text)
        } else {
            false
        }
    }
}

/// 查询执行器
pub struct QueryExecutor;

impl QueryExecutor {
    /// 执行查询过滤
    pub fn execute_query(docs: &[Value], query: &EsQuery) -> Vec<Value> {
        match query {
            EsQuery::MatchAll => docs.to_vec(),
            EsQuery::Filter(filter) => Self::apply_filter(docs, filter),
        }
    }

    /// 应用过滤器
    fn apply_filter(docs: &[Value], filter: &QueryFilter) -> Vec<Value> {
        docs.iter()
            .filter(|doc| Self::matches_filter(doc, filter))
            .cloned()
            .collect()
    }

    /// 检查文档是否匹配过滤条件
    fn matches_filter(doc: &Value, filter: &QueryFilter) -> bool {
        match filter {
            QueryFilter::MatchAll => true,
            QueryFilter::Term(conditions) => {
                conditions.iter().all(|(field, condition)| {
                    if let Some(value) = Self::get_field_value(doc, field) {
                        match condition {
                            TermCondition::Exact(expected) => value == expected,
                            TermCondition::Match(expected) => {
                                // 简单的字符串匹配
                                if let (Some(v), Some(e)) = (value.as_str(), expected.as_str()) {
                                    v.contains(e)
                                } else {
                                    value == expected
                                }
                            }
                        }
                    } else {
                        false
                    }
                })
            }
            QueryFilter::Match(conditions) => conditions
                .iter()
                .all(|(field, _)| Self::get_field_value(doc, field).is_some()),
            QueryFilter::Range(conditions) => conditions.iter().all(|(field, range_cond)| {
                if let Some(value) = Self::get_field_value(doc, field) {
                    Self::check_range(value, range_cond)
                } else {
                    false
                }
            }),
            QueryFilter::Bool(_) => true, // 简化实现
            QueryFilter::Exists(field) => Self::get_field_value(doc, field).is_some(),
            QueryFilter::Wildcard(patterns) => patterns.iter().all(|(field, pattern)| {
                if let Some(value) = Self::get_field_value(doc, field) {
                    if let Some(v) = value.as_str() {
                        Self::wildcard_match(v, pattern)
                    } else {
                        false
                    }
                } else {
                    false
                }
            }),
        }
    }

    /// 获取字段值
    fn get_field_value<'a>(doc: &'a Value, field: &str) -> Option<&'a Value> {
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

    /// 检查值是否在范围内
    fn check_range(value: &Value, range: &RangeCondition) -> bool {
        if let Some(gte) = &range.gte {
            if !Self::compare_values(value, gte, ">=") {
                return false;
            }
        }
        if let Some(gt) = &range.gt {
            if !Self::compare_values(value, gt, ">") {
                return false;
            }
        }
        if let Some(lte) = &range.lte {
            if !Self::compare_values(value, lte, "<=") {
                return false;
            }
        }
        if let Some(lt) = &range.lt {
            if !Self::compare_values(value, lt, "<") {
                return false;
            }
        }
        true
    }

    /// 比较值
    fn compare_values(left: &Value, right: &Value, op: &str) -> bool {
        match (left, right) {
            (Value::Number(l), Value::Number(r)) => {
                let l_f64 = l.as_f64().unwrap_or(0.0);
                let r_f64 = r.as_f64().unwrap_or(0.0);
                match op {
                    ">=" => l_f64 >= r_f64,
                    ">" => l_f64 > r_f64,
                    "<=" => l_f64 <= r_f64,
                    "<" => l_f64 < r_f64,
                    _ => false,
                }
            }
            (Value::String(l), Value::String(r)) => match op {
                ">=" => l >= r,
                ">" => l > r,
                "<=" => l <= r,
                "<" => l < r,
                _ => false,
            },
            _ => false,
        }
    }

    /// 通配符匹配
    fn wildcard_match(text: &str, pattern: &str) -> bool {
        let regex_pattern = pattern.replace("*", ".*").replace("?", ".");

        if let Ok(regex) = regex::Regex::new(&format!("^{}$", regex_pattern)) {
            regex.is_match(text)
        } else {
            false
        }
    }
}
