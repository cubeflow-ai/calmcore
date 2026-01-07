/// SQL 标准化器
///
/// 将MySQL特有的SQL语法转换为DataFusion支持的标准SQL语法
/// 同时验证SQL的合法性
use crate::utils::error::{CoreError, CoreResult};
use datafusion::sql::parser::{DFParser, Statement};
use datafusion::sql::sqlparser::ast::{
    BinaryOperator, CastKind, DataType, ExactNumberInfo, Expr, FunctionArg, FunctionArgExpr,
    FunctionArgumentClause, FunctionArguments, Ident, LimitClause, OrderBy, OrderByExpr,
    OrderByKind, Query, Select, SelectItem, SetExpr, Value, ValueWithSpan,
};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use regex::Regex;
use std::collections::HashSet;

/// SQL 标准化结果
#[derive(Debug, Clone)]
pub struct NormalizedSql {
    /// 原始 AST Statement
    pub statement: Statement,

    /// 改写后的 SQL（移除了 _partition 条件）
    pub rewritten_sql: String,

    /// 提取出的分区过滤条件
    pub partition_filters: PartitionFilters,

    /// _score 相关配置
    pub score: ScoreConfig,

    /// 是否需要注入 `_score` 虚拟列
    pub needs_score_column: bool,

    /// 是否是纯 COUNT(*) 查询（用于优化）
    /// 条件：SELECT COUNT(*) / COUNT(1)，无 GROUP BY，可以有 WHERE
    pub is_count_only: bool,

    /// 查询的 LIMIT（用于分布式查询优化）
    /// 从 SQL 的 LIMIT 子句中提取
    pub limit: Option<usize>,
}

/// 分区过滤条件
#[derive(Debug, Clone, Default)]
pub struct PartitionFilters {
    /// 精确匹配的分区名列表
    /// 例如: _partition = '20240101' 或 _partition IN ('20240101', '20240102')
    pub exact_matches: Vec<String>,

    /// LIKE 模式列表
    /// 例如: _partition LIKE '2024%' 或 _partition LIKE '202401__'
    pub like_patterns: Vec<String>,

    /// 是否有 _partition 条件
    pub has_filter: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScoreOrder {
    Asc,
    Desc,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ScoreLimit {
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

impl ScoreLimit {
    pub fn new(limit: Option<usize>, offset: Option<usize>) -> Self {
        Self { limit, offset }
    }
}

#[derive(Debug, Clone)]
pub enum ScoreColumnPlacement {
    Append { field_name: String },
    Replace { index: usize, field_name: String },
}

impl ScoreColumnPlacement {
    pub fn field_name(&self) -> &str {
        match self {
            ScoreColumnPlacement::Append { field_name }
            | ScoreColumnPlacement::Replace { field_name, .. } => field_name,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct ScoreConfig {
    pub placement: Option<ScoreColumnPlacement>,
    pub order: Option<ScoreOrder>,
    pub limit: Option<ScoreLimit>,
}

impl ScoreConfig {
    pub fn needs_score(&self) -> bool {
        self.placement.is_some()
    }
}

impl PartitionFilters {
    /// 判断是否需要扫描所有分区
    pub fn scan_all(&self) -> bool {
        !self.has_filter
    }

    /// 解析分区列表（根据过滤条件）
    pub fn resolve_partitions(&self, all_partitions: &[String]) -> Vec<String> {
        log::debug!(
            "🔍 [resolve_partitions] has_filter={}, exact_matches={:?}, like_patterns={:?}, all_partitions={:?}",
            self.has_filter,
            self.exact_matches,
            self.like_patterns,
            all_partitions
        );

        if !self.has_filter {
            // 没有过滤条件，返回所有分区
            log::debug!(
                "🔍 [resolve_partitions] No filter, returning all {} partitions",
                all_partitions.len()
            );
            return all_partitions.to_vec();
        }

        let mut matched = HashSet::new();

        // 1. 添加精确匹配的分区
        for partition in &self.exact_matches {
            log::debug!(
                "🔍 [resolve_partitions] Checking exact match: '{}' in {:?}",
                partition,
                all_partitions
            );
            if all_partitions.contains(partition) {
                log::debug!("✅ [resolve_partitions] Matched: '{}'", partition);
                matched.insert(partition.clone());
            } else {
                log::debug!("❌ [resolve_partitions] Not found: '{}'", partition);
            }
        }

        // 2. 处理 LIKE 模式
        for pattern in &self.like_patterns {
            if let Ok(regex_pattern) = Self::convert_sql_like_to_regex(pattern) {
                for partition in all_partitions {
                    if regex_pattern.is_match(partition) {
                        matched.insert(partition.clone());
                    }
                }
            }
        }

        let result: Vec<String> = matched.into_iter().collect();
        log::debug!(
            "🔍 [resolve_partitions] Final matched partitions: {:?}",
            result
        );
        result
    }

    /// 将 SQL LIKE 模式转换为正则表达式
    ///
    /// SQL LIKE:
    /// - `%` 匹配任意字符序列
    /// - `_` 匹配单个字符
    ///
    /// 例如:
    /// - `2024%` -> `^2024.*$`
    /// - `2024__01` -> `^2024..01$`
    fn convert_sql_like_to_regex(pattern: &str) -> Result<Regex, regex::Error> {
        // 转义特殊字符，除了 % 和 _
        let mut regex_pattern = String::new();
        regex_pattern.push('^');

        for ch in pattern.chars() {
            match ch {
                '%' => regex_pattern.push_str(".*"),
                '_' => regex_pattern.push('.'),
                // 转义正则表达式特殊字符
                '.' | '+' | '*' | '?' | '^' | '$' | '(' | ')' | '[' | ']' | '{' | '}' | '|'
                | '\\' => {
                    regex_pattern.push('\\');
                    regex_pattern.push(ch);
                }
                _ => regex_pattern.push(ch),
            }
        }

        regex_pattern.push('$');
        Regex::new(&regex_pattern)
    }
}

pub struct SqlNormalizer;

impl SqlNormalizer {
    /// 标准化SQL语句（新版本，返回 NormalizedSql）
    ///
    /// 1. 验证SQL语法
    /// 2. 提取 _partition 过滤条件
    /// 3. 改写 SQL，移除 _partition 条件
    /// 4. 将MySQL特有语法转换为标准SQL
    /// 5. 将 Timestamp 字段的 Int64 比较值转换为 CAST 表达式
    pub fn normalize(sql: &str) -> CoreResult<NormalizedSql> {
        let dialect = GenericDialect {};

        let mut statements = DFParser::parse_sql_with_dialect(sql, &dialect)?;

        let statement = if statements.len() != 1 {
            return Err(CoreError::InvalidParam(format!(
                "Expected 1 SQL statement, got {}",
                statements.len()
            )));
        } else {
            statements.pop_front().ok_or_else(|| {
                CoreError::InvalidParam("Failed to extract SQL statement".to_string())
            })?
        };

        let (statement, score) = Self::process_score_clauses(statement)?;

        // 🎯 提取 _partition 过滤条件
        let partition_filters = Self::extract_partition_filters(&statement)?;

        // 🔧 改写 SQL，移除 _partition 条件
        let rewritten_statement = Self::remove_partition_conditions(statement.clone())?;
        let mut rewritten_sql = rewritten_statement.to_string();

        // 🔧 修复 Timestamp 字段类型不匹配问题
        rewritten_sql = Self::fix_timestamp_comparisons(&rewritten_sql);

        let needs_score_column = score.needs_score();

        // 🎯 判断是否是 COUNT(*) 优化场景
        let is_count_only = Self::is_count_only_query(&rewritten_statement);

        // 🔧 提取 LIMIT
        let limit = Self::extract_limit(&rewritten_statement);

        Ok(NormalizedSql {
            statement,
            rewritten_sql,
            partition_filters,
            score,
            needs_score_column,
            is_count_only,
            limit,
        })
    }

    /// 从 Statement 中提取 LIMIT
    fn extract_limit(statement: &Statement) -> Option<usize> {
        // DataFusion Statement 包装了 sqlparser Statement
        match statement {
            Statement::Statement(boxed_stmt) => {
                if let sqlparser::ast::Statement::Query(query) = boxed_stmt.as_ref() {
                    // DataFusion 使用 limit_clause 字段
                    if let Some(limit_clause) = &query.limit_clause {
                        match limit_clause {
                            LimitClause::LimitOffset { limit, .. } => {
                                if let Some(expr) = limit {
                                    return Self::parse_limit_value(expr).ok().flatten();
                                }
                            }
                            LimitClause::OffsetCommaLimit { limit, .. } => {
                                return Self::parse_limit_value(limit).ok().flatten();
                            }
                        }
                    }
                }
            }
            // 其他 Statement 类型（CreateExternalTable, CopyTo, Explain 等）没有 limit
            _ => {}
        }
        None
    }

    /// 判断是否是纯 COUNT(*) 查询（可以优化为只返回行数）
    ///
    /// 条件：
    /// 1. SELECT 中只有一个表达式
    /// 2. 该表达式是 COUNT(*) 或 COUNT(1)
    /// 3. 没有 GROUP BY
    /// 4. 可以有 WHERE 条件（因为只需要统计行数）
    fn is_count_only_query(statement: &Statement) -> bool {
        match statement {
            Statement::Statement(boxed) => {
                if let datafusion::sql::sqlparser::ast::Statement::Query(query) = boxed.as_ref() {
                    return Self::is_count_only_query_inner(query);
                }
            }
            _ => {}
        }
        false
    }

    /// 内部方法：检查 Query 是否是 COUNT(*)
    fn is_count_only_query_inner(query: &Query) -> bool {
        if let SetExpr::Select(select) = query.body.as_ref() {
            // 不能有 GROUP BY
            if !matches!(select.group_by, datafusion::sql::sqlparser::ast::GroupByExpr::Expressions(ref v, _) if v.is_empty())
            {
                return false;
            }

            // 检查 SELECT 列表
            if select.projection.len() != 1 {
                return false;
            }

            // 检查第一个表达式是否是 COUNT(*) 或 COUNT(1)
            if let Some(SelectItem::UnnamedExpr(expr)) = select.projection.first() {
                return Self::is_count_expr(expr);
            } else if let Some(SelectItem::ExprWithAlias { expr, .. }) = select.projection.first() {
                return Self::is_count_expr(expr);
            }
        }
        false
    }

    /// 判断表达式是否是 COUNT(*) 或 COUNT(1)
    fn is_count_expr(expr: &Expr) -> bool {
        match expr {
            Expr::Function(func) => {
                // 函数名必须是 COUNT
                if func.name.to_string().to_uppercase() != "COUNT" {
                    return false;
                }

                // 检查参数
                match &func.args {
                    FunctionArguments::List(list) => {
                        if list.args.len() != 1 {
                            return false;
                        }

                        // 检查第一个参数是 * 或 1
                        match &list.args[0] {
                            FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => true,
                            FunctionArg::Unnamed(FunctionArgExpr::Expr(arg_expr)) => {
                                matches!(
                                    arg_expr,
                                    Expr::Value(ValueWithSpan { value: Value::Number(num, _), .. }) if num == "1"
                                )
                            }
                            _ => false,
                        }
                    }
                    _ => false,
                }
            }
            _ => false,
        }
    }

    fn process_score_clauses(statement: Statement) -> CoreResult<(Statement, ScoreConfig)> {
        let mut score = ScoreConfig::default();

        let processed_statement = match statement {
            Statement::Statement(mut boxed) => {
                if let datafusion::sql::sqlparser::ast::Statement::Query(query) = boxed.as_mut() {
                    Self::handle_score_in_query(query, &mut score)?;
                }

                Self::ensure_no_remaining_score_refs(boxed.as_ref())?;
                Statement::Statement(boxed)
            }
            other => other,
        };

        Ok((processed_statement, score))
    }

    fn handle_score_in_query(query: &mut Query, score: &mut ScoreConfig) -> CoreResult<()> {
        if let Some(order) = Self::strip_score_ordering(&mut query.order_by)? {
            score.order = Some(order);

            if let Some(limit_clause) = query.limit_clause.take() {
                let (limit_value, offset_value) = Self::extract_score_limit(limit_clause)?;
                if limit_value.is_some() || offset_value.is_some() {
                    score.limit = Some(ScoreLimit::new(limit_value, offset_value));
                }
            }
        }

        if let SetExpr::Select(select) = query.body.as_mut() {
            Self::rewrite_projection_for_score(select, score)?;
        }

        if score.placement.is_none() && score.order.is_some() {
            score.placement = Some(ScoreColumnPlacement::Append {
                field_name: "_score".to_string(),
            });
        }

        Ok(())
    }

    fn strip_score_ordering(order_by: &mut Option<OrderBy>) -> CoreResult<Option<ScoreOrder>> {
        let Some(order_clause) = order_by.as_mut() else {
            return Ok(None);
        };

        let OrderByKind::Expressions(exprs) = &mut order_clause.kind else {
            return Ok(None);
        };

        let mut detected = None;
        let mut retained = Vec::with_capacity(exprs.len());

        for expr in exprs.drain(..) {
            if Self::expr_refs_score(&expr.expr) {
                if !Self::is_score_identifier(&expr.expr) {
                    return Err(CoreError::InvalidParam(
                        "ORDER BY _score currently only supports plain identifiers".into(),
                    ));
                }

                let descending = expr.options.asc.map(|flag| !flag).unwrap_or(true);
                if detected.is_none() {
                    detected = Some(if descending {
                        ScoreOrder::Desc
                    } else {
                        ScoreOrder::Asc
                    });
                }
            } else {
                retained.push(expr);
            }
        }

        *exprs = retained;

        if exprs.is_empty() {
            *order_by = None;
        }

        Ok(detected)
    }

    fn rewrite_projection_for_score(
        select: &mut Select,
        score: &mut ScoreConfig,
    ) -> CoreResult<()> {
        for (idx, item) in select.projection.iter_mut().enumerate() {
            match item {
                SelectItem::UnnamedExpr(expr) if Self::is_score_identifier(expr) => {
                    *item = SelectItem::ExprWithAlias {
                        expr: Self::score_placeholder_expr(),
                        alias: Ident::new("_score"),
                    };
                    Self::set_score_placement(score, idx, "_score")?;
                }
                SelectItem::ExprWithAlias { expr, alias } if Self::is_score_identifier(expr) => {
                    *expr = Self::score_placeholder_expr();
                    Self::set_score_placement(score, idx, &alias.value)?;
                }
                _ if Self::expr_refs_score_in_item(item) => {
                    return Err(CoreError::InvalidParam(
                        "_score can only appear as a standalone select item".into(),
                    ));
                }
                _ => {}
            }
        }

        Ok(())
    }

    fn set_score_placement(
        score: &mut ScoreConfig,
        index: usize,
        field_name: &str,
    ) -> CoreResult<()> {
        if score.placement.is_some() {
            return Err(CoreError::InvalidParam(
                "_score can only appear once in the projection list".into(),
            ));
        }

        score.placement = Some(ScoreColumnPlacement::Replace {
            index,
            field_name: field_name.to_string(),
        });
        Ok(())
    }

    fn score_placeholder_expr() -> Expr {
        Expr::Cast {
            kind: CastKind::Cast,
            expr: Box::new(Expr::Value(ValueWithSpan::from(Value::Number(
                "0".to_string(),
                false,
            )))),
            data_type: DataType::Float(ExactNumberInfo::None),
            format: None,
        }
    }

    fn parse_limit_value(expr: &Expr) -> CoreResult<Option<usize>> {
        match expr {
            Expr::Value(value_with_span) => match &value_with_span.value {
                Value::Number(value, _) => value
                    .parse::<usize>()
                    .map(Some)
                    .map_err(|_| CoreError::InvalidParam("LIMIT must be numeric".into())),
                Value::SingleQuotedString(s) if s.eq_ignore_ascii_case("all") => Ok(None),
                Value::DoubleQuotedString(s) if s.eq_ignore_ascii_case("all") => Ok(None),
                _ => Err(CoreError::InvalidParam(
                    "LIMIT must be a positive integer or ALL".into(),
                )),
            },
            _ => Err(CoreError::InvalidParam(
                "LIMIT expressions referencing columns are not supported with _score".into(),
            )),
        }
    }

    fn parse_offset_value(expr: &Expr) -> CoreResult<usize> {
        match expr {
            Expr::Value(value_with_span) => match &value_with_span.value {
                Value::Number(value, _) => value.parse::<usize>().map_err(|_| {
                    CoreError::InvalidParam("OFFSET must be a positive integer".into())
                }),
                _ => Err(CoreError::InvalidParam(
                    "OFFSET must be a positive integer".into(),
                )),
            },
            _ => Err(CoreError::InvalidParam(
                "OFFSET expressions referencing columns are not supported".into(),
            )),
        }
    }

    fn extract_score_limit(clause: LimitClause) -> CoreResult<(Option<usize>, Option<usize>)> {
        match clause {
            LimitClause::LimitOffset {
                limit,
                offset,
                limit_by,
            } => {
                if !limit_by.is_empty() {
                    return Err(CoreError::InvalidParam(
                        "LIMIT BY is not supported when ordering by _score".into(),
                    ));
                }

                let limit_value = match limit {
                    Some(expr) => Some(Self::parse_limit_value(&expr)?),
                    None => None,
                }
                .flatten();

                let offset_value = match offset {
                    Some(offset) => Some(Self::parse_offset_value(&offset.value)?),
                    None => None,
                };

                Ok((limit_value, offset_value))
            }
            LimitClause::OffsetCommaLimit { offset, limit } => {
                let offset_value = Some(Self::parse_offset_value(&offset)?);
                let limit_value = Self::parse_limit_value(&limit)?;
                Ok((limit_value, offset_value))
            }
        }
    }

    fn ensure_no_remaining_score_refs(
        statement: &datafusion::sql::sqlparser::ast::Statement,
    ) -> CoreResult<()> {
        if Self::statement_contains_score(statement) {
            return Err(CoreError::InvalidParam(
                "_score is only supported in SELECT clauses and ORDER BY".into(),
            ));
        }
        Ok(())
    }

    fn statement_contains_score(statement: &datafusion::sql::sqlparser::ast::Statement) -> bool {
        match statement {
            datafusion::sql::sqlparser::ast::Statement::Query(query) => {
                Self::query_contains_score(query)
            }
            _ => false,
        }
    }

    fn query_contains_score(query: &Query) -> bool {
        if query
            .order_by
            .as_ref()
            .map(|order_by| Self::order_by_contains_score(order_by))
            .unwrap_or(false)
        {
            return true;
        }

        Self::query_body_contains_score(query.body.as_ref())
    }

    fn query_body_contains_score(body: &SetExpr) -> bool {
        match body {
            SetExpr::Select(select) => {
                if let Some(selection) = &select.selection {
                    if Self::expr_refs_score(selection) {
                        return true;
                    }
                }

                select
                    .projection
                    .iter()
                    .any(|item| Self::expr_refs_score_in_item(item))
            }
            SetExpr::Query(query) => Self::query_contains_score(query),
            SetExpr::SetOperation { left, right, .. } => {
                Self::query_body_contains_score(left) || Self::query_body_contains_score(right)
            }
            _ => false,
        }
    }

    fn expr_refs_score_in_item(item: &SelectItem) -> bool {
        match item {
            SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                Self::expr_refs_score(expr)
            }
            _ => false,
        }
    }

    fn expr_refs_score(expr: &Expr) -> bool {
        match expr {
            Expr::Identifier(ident) => ident.value.eq_ignore_ascii_case("_score"),
            Expr::CompoundIdentifier(idents) => idents
                .last()
                .map(|ident| ident.value.eq_ignore_ascii_case("_score"))
                .unwrap_or(false),
            Expr::BinaryOp { left, right, .. } => {
                Self::expr_refs_score(left) || Self::expr_refs_score(right)
            }
            Expr::Nested(inner) => Self::expr_refs_score(inner),
            Expr::UnaryOp { expr, .. } => Self::expr_refs_score(expr),
            Expr::Function(function) => {
                Self::function_arguments_ref_score(&function.parameters)
                    || Self::function_arguments_ref_score(&function.args)
                    || function
                        .filter
                        .as_ref()
                        .map(|expr| Self::expr_refs_score(expr))
                        .unwrap_or(false)
                    || Self::order_by_exprs_refs_score(&function.within_group)
            }
            Expr::Between {
                expr, low, high, ..
            } => {
                Self::expr_refs_score(expr)
                    || Self::expr_refs_score(low)
                    || Self::expr_refs_score(high)
            }
            Expr::Case {
                operand,
                conditions,
                else_result,
                ..
            } => {
                operand
                    .as_ref()
                    .map(|expr| Self::expr_refs_score(expr))
                    .unwrap_or(false)
                    || conditions.iter().any(|case_when| {
                        Self::expr_refs_score(&case_when.condition)
                            || Self::expr_refs_score(&case_when.result)
                    })
                    || else_result
                        .as_ref()
                        .map(|expr| Self::expr_refs_score(expr))
                        .unwrap_or(false)
            }
            Expr::InList { expr, list, .. } => {
                Self::expr_refs_score(expr) || list.iter().any(Self::expr_refs_score)
            }
            Expr::Exists { subquery, .. } => Self::query_contains_score(subquery),
            Expr::Subquery(query) => Self::query_contains_score(query),
            _ => false,
        }
    }

    fn order_by_contains_score(order_by: &OrderBy) -> bool {
        match &order_by.kind {
            OrderByKind::Expressions(exprs) => Self::order_by_exprs_refs_score(exprs),
            OrderByKind::All(_) => false,
        }
    }

    fn order_by_exprs_refs_score(exprs: &[OrderByExpr]) -> bool {
        exprs
            .iter()
            .any(|order_expr| Self::expr_refs_score(&order_expr.expr))
    }

    fn function_arguments_ref_score(args: &FunctionArguments) -> bool {
        match args {
            FunctionArguments::None => false,
            FunctionArguments::Subquery(query) => Self::query_contains_score(query),
            FunctionArguments::List(list) => {
                for arg in &list.args {
                    let matches = match arg {
                        FunctionArg::Named { arg, .. } | FunctionArg::Unnamed(arg) => {
                            Self::function_arg_expr_refs_score(arg)
                        }
                        FunctionArg::ExprNamed { name, arg, .. } => {
                            Self::expr_refs_score(name) || Self::function_arg_expr_refs_score(arg)
                        }
                    };

                    if matches {
                        return true;
                    }
                }

                for clause in &list.clauses {
                    match clause {
                        FunctionArgumentClause::OrderBy(exprs) => {
                            if Self::order_by_exprs_refs_score(exprs) {
                                return true;
                            }
                        }
                        FunctionArgumentClause::Limit(expr) => {
                            if Self::expr_refs_score(expr) {
                                return true;
                            }
                        }
                        FunctionArgumentClause::Having(bound) => {
                            if Self::expr_refs_score(&bound.1) {
                                return true;
                            }
                        }
                        _ => {}
                    }
                }

                false
            }
        }
    }

    fn function_arg_expr_refs_score(arg: &FunctionArgExpr) -> bool {
        match arg {
            FunctionArgExpr::Expr(expr) => Self::expr_refs_score(expr),
            FunctionArgExpr::QualifiedWildcard(_) | FunctionArgExpr::Wildcard => false,
        }
    }

    fn is_score_identifier(expr: &Expr) -> bool {
        matches!(expr, Expr::Identifier(ident) if ident.value.eq_ignore_ascii_case("_score"))
            || matches!(expr, Expr::CompoundIdentifier(idents) if idents
                .last()
                .map(|ident| ident.value.eq_ignore_ascii_case("_score"))
                .unwrap_or(false))
    }
    /// 提取 _partition 过滤条件
    fn extract_partition_filters(statement: &Statement) -> CoreResult<PartitionFilters> {
        let mut filters = PartitionFilters::default();

        if let Statement::Statement(boxed) = statement {
            if let datafusion::sql::sqlparser::ast::Statement::Query(query) = boxed.as_ref() {
                if let SetExpr::Select(select) = query.body.as_ref() {
                    if let Some(selection) = &select.selection {
                        Self::extract_from_expr(selection, &mut filters);
                    }
                }
            }
        }

        filters.has_filter = !filters.exact_matches.is_empty() || !filters.like_patterns.is_empty();

        Ok(filters)
    }

    /// 从表达式中递归提取 _partition 条件
    fn extract_from_expr(expr: &Expr, filters: &mut PartitionFilters) {
        match expr {
            // _partition = 'value'
            Expr::BinaryOp { left, op, right } => {
                if *op == BinaryOperator::Eq && Self::is_partition_field(left) {
                    if let Some(value) = Self::extract_string_literal(right) {
                        log::debug!("🔍 [extract_from_expr] Found _partition = '{}'", value);
                        filters.exact_matches.push(value);
                    }
                } else if matches!(op, BinaryOperator::And | BinaryOperator::Or) {
                    // 递归处理 AND/OR
                    Self::extract_from_expr(left, filters);
                    Self::extract_from_expr(right, filters);
                }
            }
            // _partition IN ('val1', 'val2')
            Expr::InList { expr, list, .. } => {
                if Self::is_partition_field(expr) {
                    for item in list {
                        if let Some(value) = Self::extract_string_literal(item) {
                            filters.exact_matches.push(value);
                        }
                    }
                }
            }
            // _partition LIKE 'pattern'
            Expr::Like { expr, pattern, .. } | Expr::ILike { expr, pattern, .. } => {
                if Self::is_partition_field(expr) {
                    if let Some(pattern_str) = Self::extract_string_literal(pattern) {
                        filters.like_patterns.push(pattern_str);
                    }
                }
            }
            Expr::Nested(inner) => {
                Self::extract_from_expr(inner, filters);
            }
            _ => {}
        }
    }

    /// 判断是否是 _partition 字段
    fn is_partition_field(expr: &Expr) -> bool {
        match expr {
            Expr::Identifier(ident) => ident.value == "_partition",
            Expr::CompoundIdentifier(idents) => {
                idents.last().map(|i| i.value.as_str()) == Some("_partition")
            }
            _ => false,
        }
    }

    /// 提取字符串字面量
    fn extract_string_literal(expr: &Expr) -> Option<String> {
        match expr {
            Expr::Value(value_with_span) => match &value_with_span.value {
                Value::SingleQuotedString(s) => Some(s.clone()),
                Value::DoubleQuotedString(s) => Some(s.clone()),
                _ => None,
            },
            _ => None,
        }
    }

    /// 移除 _partition 条件
    fn remove_partition_conditions(statement: Statement) -> CoreResult<Statement> {
        match statement {
            Statement::Statement(boxed) => {
                match *boxed {
                    datafusion::sql::sqlparser::ast::Statement::Query(mut query) => {
                        // 修改 query 的 body
                        if let SetExpr::Select(mut select) = *query.body {
                            if let Some(selection) = select.selection {
                                select.selection = Some(Self::remove_partition_expr(selection));
                            }
                            query.body = Box::new(SetExpr::Select(select));
                        }

                        Ok(Statement::Statement(Box::new(
                            datafusion::sql::sqlparser::ast::Statement::Query(query),
                        )))
                    }
                    other => {
                        // 非 Query 语句，直接返回
                        Ok(Statement::Statement(Box::new(other)))
                    }
                }
            }
            // 处理其他 DataFusion 特有的 Statement 类型
            other => Ok(other),
        }
    }

    /// 递归移除表达式中的 _partition 条件
    fn remove_partition_expr(expr: Expr) -> Expr {
        match expr {
            Expr::BinaryOp { left, op, right } => {
                // 先递归处理子表达式
                if matches!(op, BinaryOperator::And) {
                    let left_cleaned = Self::remove_partition_expr(*left);
                    let right_cleaned = Self::remove_partition_expr(*right);

                    // 如果左边是 _partition 条件（被替换为 TRUE），返回右边
                    if Self::is_true_value(&left_cleaned) {
                        return right_cleaned;
                    }

                    // 如果右边是 _partition 条件（被替换为 TRUE），返回左边
                    if Self::is_true_value(&right_cleaned) {
                        return left_cleaned;
                    }

                    return Expr::BinaryOp {
                        left: Box::new(left_cleaned),
                        op,
                        right: Box::new(right_cleaned),
                    };
                }

                // 检查是否是 _partition 等值条件
                if matches!(op, BinaryOperator::Eq) && Self::is_partition_field(&left) {
                    return Expr::Value(ValueWithSpan::from(Value::Boolean(true)));
                }

                Expr::BinaryOp { left, op, right }
            }
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                if Self::is_partition_field(&expr) {
                    return Expr::Value(ValueWithSpan::from(Value::Boolean(true)));
                }
                Expr::InList {
                    expr,
                    list,
                    negated,
                }
            }
            Expr::Like {
                negated,
                expr,
                pattern,
                escape_char,
                any,
            } => {
                if Self::is_partition_field(&expr) {
                    return Expr::Value(ValueWithSpan::from(Value::Boolean(true)));
                }
                Expr::Like {
                    negated,
                    expr,
                    pattern,
                    escape_char,
                    any,
                }
            }
            Expr::Nested(inner) => {
                let cleaned = Self::remove_partition_expr(*inner);
                if Self::is_true_value(&cleaned) {
                    Expr::Value(ValueWithSpan::from(Value::Boolean(true)))
                } else {
                    Expr::Nested(Box::new(cleaned))
                }
            }
            _ => expr,
        }
    }

    /// 判断表达式是否是 TRUE 值
    fn is_true_value(expr: &Expr) -> bool {
        match expr {
            Expr::Value(value_with_span) => matches!(value_with_span.value, Value::Boolean(true)),
            _ => false,
        }
    }

    /// 修复 Timestamp 字段的类型不匹配
    ///
    /// DataFusion 要求 Timestamp 字段与 Timestamp 类型比较，不能直接与 Int64 比较
    /// 这个函数将常见的时间戳字段（如 pickup_datetime, dropoff_datetime, created_at, updated_at 等）
    /// 的 Int64 字面量自动包装为 to_timestamp_millis(value) 函数调用
    /// 使用 to_timestamp_millis 而不是 CAST 可以避免溢出问题
    fn fix_timestamp_comparisons(sql: &str) -> String {
        // 常见的时间戳字段名模式
        let timestamp_fields = vec![
            "pickup_datetime",
            "dropoff_datetime",
            "created_at",
            "updated_at",
            "timestamp",
            "time",
            "datetime",
        ];

        let mut result = sql.to_string();

        for field in timestamp_fields {
            // 匹配模式: field_name [比较符] [数字]
            // 例如: pickup_datetime >= 1704067200000
            let pattern = format!(
                r"(?i)\b{}\s*(>=|<=|>|<|=|!=)\s*(\d+)\b",
                regex::escape(field)
            );

            if let Ok(re) = Regex::new(&pattern) {
                result = re
                    .replace_all(&result, |caps: &regex::Captures| {
                        let operator = &caps[1];
                        let value = &caps[2];

                        // 替换为: field_name [比较符] to_timestamp_millis(数字)
                        // to_timestamp_millis 是 DataFusion 内置函数，将毫秒时间戳转为 Timestamp
                        format!("{} {} to_timestamp_millis({})", field, operator, value)
                    })
                    .to_string();
            }
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_standard_sql() {
        let sql = "SELECT * FROM users ORDER BY age DESC LIMIT 10";
        let result = SqlNormalizer::normalize(sql).unwrap();
        // AST 转换后可能格式略有不同，只检查包含关键部分
        assert!(result.rewritten_sql.contains("SELECT"));
        assert!(result.rewritten_sql.contains("FROM users"));
        assert!(result.rewritten_sql.contains("ORDER BY age DESC"));
        assert!(result.rewritten_sql.contains("LIMIT 10"));
        assert!(!result.partition_filters.has_filter);
        assert!(!result.needs_score_column);
    }

    #[test]
    fn test_convert_mysql_limit() {
        let sql = "SELECT * FROM users ORDER BY age DESC LIMIT 0,10";
        let result = SqlNormalizer::normalize(sql).unwrap();

        // 验证 Statement 能够被正确解析
        assert!(matches!(result.statement, Statement::Statement(_)));
    }

    #[test]
    fn test_convert_mysql_limit_with_space() {
        let sql = "SELECT * FROM users ORDER BY age DESC LIMIT 5, 20";
        let result = SqlNormalizer::normalize(sql).unwrap();

        // 验证 Statement 能够被正确解析
        assert!(matches!(result.statement, Statement::Statement(_)));
    }

    #[test]
    fn test_convert_mysql_limit_zero_offset() {
        let sql = "SELECT * FROM users ORDER BY age DESC LIMIT 0, 10";
        let result = SqlNormalizer::normalize(sql).unwrap();

        // 验证 Statement 能够被正确解析
        assert!(matches!(result.statement, Statement::Statement(_)));
    }

    #[test]
    fn test_validate_invalid_sql() {
        let sql = "SELCT * FROM users"; // 故意拼错
        let result = SqlNormalizer::normalize(sql);
        assert!(result.is_err());
    }

    #[test]
    fn test_standard_limit_offset() {
        let sql = "SELECT * FROM users LIMIT 10 OFFSET 5";
        let result = SqlNormalizer::normalize(sql).unwrap();
        assert!(result.rewritten_sql.contains("LIMIT 10"));
        assert!(result.rewritten_sql.contains("OFFSET 5"));
    }

    #[test]
    fn test_partition_filter_exact_match() {
        let sql = "SELECT * FROM logs WHERE _partition = '20240101' AND level = 'ERROR'";
        let result = SqlNormalizer::normalize(sql).unwrap();

        // 验证提取到了分区过滤条件
        assert!(result.partition_filters.has_filter);
        assert_eq!(result.partition_filters.exact_matches.len(), 1);
        assert_eq!(result.partition_filters.exact_matches[0], "20240101");

        // 验证 SQL 被改写，移除了 _partition 条件
        assert!(!result.rewritten_sql.contains("_partition"));
        assert!(result.rewritten_sql.contains("level = 'ERROR'"));
    }

    #[test]
    fn test_partition_filter_in_list() {
        let sql =
            "SELECT * FROM logs WHERE _partition IN ('20240101', '20240102') AND status = 200";
        let result = SqlNormalizer::normalize(sql).unwrap();

        assert!(result.partition_filters.has_filter);
        assert_eq!(result.partition_filters.exact_matches.len(), 2);
        assert!(result
            .partition_filters
            .exact_matches
            .contains(&"20240101".to_string()));
        assert!(result
            .partition_filters
            .exact_matches
            .contains(&"20240102".to_string()));

        // 验证改写后的 SQL
        assert!(!result.rewritten_sql.contains("_partition"));
        assert!(result.rewritten_sql.contains("status = 200"));
    }

    #[test]
    fn test_partition_filter_like() {
        let sql = "SELECT * FROM logs WHERE _partition LIKE '2024%' AND user_id > 100";
        let result = SqlNormalizer::normalize(sql).unwrap();

        assert!(result.partition_filters.has_filter);
        assert_eq!(result.partition_filters.like_patterns.len(), 1);
        assert_eq!(result.partition_filters.like_patterns[0], "2024%");

        assert!(!result.rewritten_sql.contains("_partition"));
        assert!(result.rewritten_sql.contains("user_id > 100"));
    }

    #[test]
    fn test_partition_filter_resolve() {
        let mut filters = PartitionFilters::default();
        filters.exact_matches.push("20240101".to_string());
        filters.like_patterns.push("202402%".to_string());
        filters.has_filter = true;

        let all_partitions = vec![
            "20240101".to_string(),
            "20240102".to_string(),
            "20240201".to_string(),
            "20240202".to_string(),
            "20240301".to_string(),
        ];

        let matched = filters.resolve_partitions(&all_partitions);

        // 应该匹配: 20240101 (精确) + 20240201, 20240202 (LIKE '202402%')
        assert_eq!(matched.len(), 3);
        assert!(matched.contains(&"20240101".to_string()));
        assert!(matched.contains(&"20240201".to_string()));
        assert!(matched.contains(&"20240202".to_string()));
    }

    #[test]
    fn test_fulltext_functions_survive_normalization() {
        let sql =
            "SELECT id, message FROM logs WHERE text(message, 'error', 1.0) ORDER BY _score DESC";
        let result = SqlNormalizer::normalize(sql).unwrap();

        assert!(result.rewritten_sql.contains("text"));
        assert!(!result.rewritten_sql.contains("_score"));
        assert!(!result.partition_filters.has_filter);
        assert!(result.needs_score_column);
        assert!(matches!(result.score.order, Some(ScoreOrder::Desc)));
        assert!(result.score.limit.is_none());
        assert!(matches!(
            result.score.placement,
            Some(ScoreColumnPlacement::Append { .. })
        ));
    }

    #[test]
    fn test_score_order_with_limit_and_offset() {
        let sql =
            "SELECT id FROM logs WHERE text(message, 'error', 1.0) ORDER BY _score DESC LIMIT 5 OFFSET 2";
        let result = SqlNormalizer::normalize(sql).unwrap();

        assert!(!result.rewritten_sql.contains("_score"));
        assert!(!result.rewritten_sql.to_uppercase().contains("ORDER BY"));
        assert!(!result.rewritten_sql.to_uppercase().contains("LIMIT"));
        assert!(matches!(result.score.order, Some(ScoreOrder::Desc)));
        let limit = result.score.limit.expect("limit expected");
        assert_eq!(limit.limit, Some(5));
        assert_eq!(limit.offset, Some(2));
    }

    #[test]
    fn test_score_projection_with_alias() {
        let sql = "SELECT _score AS ranking FROM logs";
        let result = SqlNormalizer::normalize(sql).unwrap();

        assert!(result.rewritten_sql.contains("ranking"));
        assert!(result.needs_score_column);
        assert!(matches!(
            result.score.placement,
            Some(ScoreColumnPlacement::Replace { index: 0, field_name }) if field_name == "ranking"
        ));
    }
}
