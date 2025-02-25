pub mod statement;

use sqlparser::ast::{BinaryOperator, Expr, FunctionArg, FunctionArgExpr, SelectItem};
use statement::parse_where;

use crate::{
    index_store::seacher::plan::{ComparisonOperator, LogicOperator, Query},
    util::{str_to_vec_fix_type, string_to_vec_fix_type, CoreError, CoreResult},
    Scope,
};

use proto::core::{field, Field, Query as PBQuery};

pub fn pbquery_to_query(scope: &Scope, req: PBQuery) -> CoreResult<Query> {
    let projection = req.fields;

    let query = match parse_where(&req.query)? {
        Some(expr) => Some(Box::new(parse_filter_expr(scope, &expr)?)),
        None => None,
    };

    let order_by = parse_order_by(req.order_by)?;
    let limit = (req.offset as usize, req.limit as usize);

    Ok(Query::Search {
        projection,
        query,
        order_by,
        limit,
    })
}

pub fn sql_to_query(scope: &Scope, sql: &str) -> CoreResult<Query> {
    let statement = statement::sql_to_statement(sql)?;

    // process projection
    let projection = statement
        .projection
        .into_iter()
        .filter_map(|item| match item {
            SelectItem::UnnamedExpr(Expr::Identifier(ident)) => Some(ident.value),
            SelectItem::ExprWithAlias {
                expr: Expr::Identifier(ident),
                ..
            } => Some(ident.value),
            _ => None,
        })
        .collect();

    // process filter and query
    let query = if let Some(query) = statement.query {
        Some(Box::new(parse_filter_expr(scope, &query)?))
    } else {
        None
    };

    // process order by
    let order_by = statement
        .order_by
        .into_iter()
        .map(|o| match o.expr {
            Expr::Identifier(ident) => Ok((ident.value, matches!(o.asc, Some(true) | None))),
            _ => Err(CoreError::InvalidParam(
                "order by only support identifier".to_string(),
            )),
        })
        .collect::<CoreResult<Vec<_>>>()?;

    // process limit and offset
    let limit = match (statement.limit, statement.offset) {
        (Some(l), Some(o)) => {
            let offset = match o.value {
                Expr::Value(sqlparser::ast::Value::Number(n, _)) => n.parse::<usize>()?,
                _ => return Err(CoreError::InvalidParam("offset must be number".to_string())),
            };
            let limit = match l {
                Expr::Value(sqlparser::ast::Value::Number(n, _)) => n.parse::<usize>()?,
                _ => return Err(CoreError::InvalidParam("limit must be number".to_string())),
            };
            (offset, limit)
        }
        (Some(l), None) => {
            let limit = match l {
                Expr::Value(sqlparser::ast::Value::Number(n, _)) => n.parse::<usize>()?,
                _ => return Err(CoreError::InvalidParam("limit must be number".to_string())),
            };
            (0, limit)
        }
        _ => (0, 10), // 默认值
    };

    Ok(Query::Search {
        projection,
        query,
        order_by,
        limit,
    })
}

fn parse_order_by(input: Vec<String>) -> CoreResult<Vec<(String, bool)>> {
    if input.is_empty() {
        return Ok(vec![]);
    }

    let mut result = Vec::new();

    for item in input {
        let parts: Vec<&str> = if item.contains(' ') {
            item.split_whitespace().collect()
        } else {
            item.split(',')
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .collect()
        };

        match parts.len() {
            2 => {
                let field = parts[0].trim().to_string();
                let direction = parts[1].trim_end_matches('.').trim().to_lowercase();

                match direction.as_str() {
                    "asc" => result.push((field, true)),
                    "desc" => result.push((field, false)),
                    _ => return Err(CoreError::InvalidParam(parts[1].to_string())),
                }
            }
            _ => return Err(CoreError::InvalidParam(item)),
        }
    }

    Ok(result)
}

fn parse_filter_expr(scope: &Scope, expr: &Expr) -> CoreResult<Query> {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            match (left.as_ref(), right.as_ref()) {
                (Expr::Identifier(c), Expr::Function(f)) => {
                    let Function {
                        name,
                        value,
                        boost,
                        slop,
                        operator,
                    } = parse_function(f)?;

                    let query = match name.as_ref() {
                        "phrase" => {
                            let field = scope.get_field(&c.value).ok_or_else(|| {
                                CoreError::InvalidParam(format!("field not found: {}", c.value))
                            })?;
                            check_text(&field)?;
                            let value = expr_liternal(&value)?;

                            Query::Phrase {
                                value,
                                boost,
                                slop,
                                field,
                            }
                        }
                        "text" => {
                            let field = scope.get_field(&c.value).ok_or_else(|| {
                                CoreError::InvalidParam(format!("field not found: {}", c.value))
                            })?;

                            check_text(&field)?;

                            let value = expr_liternal(&value)?;
                            Query::Text {
                                value,
                                boost,
                                operator,
                                field,
                            }
                        }
                        "score" => {
                            let expr = Expr::BinaryOp {
                                left: Box::new(Expr::Identifier(c.clone())),
                                op: op.clone(),
                                right: Box::new(value.clone()),
                            };
                            let mut query = parse_filter_expr(scope, &expr)?;
                            query.set_boost(boost);
                            query
                        }
                        _ => {
                            return Err(CoreError::InvalidParam(format!(
                                "function name:{:?} is not support only support phrase,text",
                                name
                            )));
                        }
                    };
                    return Ok(query);
                }
                (Expr::Identifier(c), Expr::Value(v)) => {
                    let field = scope.get_field(&c.value).ok_or_else(|| {
                        CoreError::InvalidParam(format!("field not found: {}", c.value))
                    })?;

                    let value = value_to_str(v)?;

                    // if text so return text query
                    if field.r#type() == field::Type::Text {
                        if BinaryOperator::Eq != *op {
                            return Err(CoreError::InvalidParam(format!(
                                "text search only support eq:{:?}",
                                expr
                            )));
                        }
                        return Ok(Query::Text {
                            value: value.to_string(),
                            boost: 1.0,
                            operator: "or".to_string(),
                            field,
                        });
                    }

                    let value = str_to_vec_fix_type(value, &field.r#type())?;

                    let query = match op {
                        BinaryOperator::Eq => Query::Term {
                            value,
                            boost: 1.0,
                            operator: ComparisonOperator::Eq,
                            field,
                        },
                        BinaryOperator::NotEq => Query::Term {
                            value,
                            boost: 1.0,
                            operator: ComparisonOperator::NotEq,
                            field,
                        },
                        BinaryOperator::Lt => Query::Between {
                            low: None,
                            low_eq: false,
                            high: Some(value),
                            high_eq: false,
                            boost: 1.0,
                            field,
                        },
                        BinaryOperator::LtEq => Query::Between {
                            low: None,
                            low_eq: false,
                            high: Some(value),
                            high_eq: true,
                            boost: 1.0,
                            field,
                        },
                        BinaryOperator::Gt => Query::Between {
                            low: Some(value),
                            low_eq: false,
                            high: None,
                            high_eq: false,
                            boost: 1.0,
                            field,
                        },
                        BinaryOperator::GtEq => Query::Between {
                            low: Some(value),
                            low_eq: true,
                            high: None,
                            high_eq: false,
                            boost: 1.0,
                            field,
                        },
                        _ => {
                            return Err(CoreError::InvalidParam(format!(
                                "{:?} for binary_op",
                                expr
                            )));
                        }
                    };

                    return Ok(query);
                }
                (Expr::Identifier(c), Expr::InList { list, .. }) => {
                    let field = scope.get_field(&c.value).ok_or_else(|| {
                        CoreError::InvalidParam(format!("field not found: {}", c.value))
                    })?;
                    let mut values = Vec::with_capacity(list.len());
                    for v in list.iter() {
                        let value = string_to_vec_fix_type(expr_liternal(v)?, &field.r#type())?;
                        values.push(value);
                    }

                    return Ok(Query::InList {
                        list: values,
                        boost: 1.0,
                        field,
                    });
                }
                (Expr::Identifier(c), Expr::Between { low, high, .. }) => {
                    let field = scope.get_field(&c.value).ok_or_else(|| {
                        CoreError::InvalidParam(format!("field not found: {}", c.value))
                    })?;
                    let low = Some(string_to_vec_fix_type(
                        expr_liternal(low)?,
                        &field.r#type(),
                    )?);
                    let high = Some(string_to_vec_fix_type(
                        expr_liternal(high)?,
                        &field.r#type(),
                    )?);
                    return Ok(Query::Between {
                        low,
                        low_eq: true,
                        high,
                        high_eq: false,
                        boost: 1.0,
                        field,
                    });
                }
                _ => {}
            };

            let l = parse_filter_expr(scope, left)?;
            let r = parse_filter_expr(scope, right)?;

            Ok(Query::Logical {
                left: Box::new(l),
                right: Box::new(r),
                operator: match op {
                    BinaryOperator::And => LogicOperator::And,
                    BinaryOperator::Or => LogicOperator::Or,
                    _ => {
                        return Err(CoreError::InvalidParam(format!("{:?} for binary_op", expr)));
                    }
                },
            })
        }
        Expr::Nested(nested) => parse_filter_expr(scope, nested.as_ref()),
        _ => Err(CoreError::InvalidParam(format!("unsupport: {:?}", expr))),
    }
}

fn check_text(f: &Field) -> CoreResult<()> {
    if field::Type::Text == f.r#type() {
        Ok(())
    } else {
        Err(CoreError::InvalidParam(format!(
            "field type is not text:{:?}",
            f.name
        )))
    }
}

struct Function {
    name: String,
    boost: f32,
    value: Expr,
    slop: i32,
    operator: String, // default is "or"
}

fn parse_function(f: &sqlparser::ast::Function) -> CoreResult<Function> {
    let sqlparser::ast::Function { name, args, .. } = f;

    let name = name
        .0
        .first()
        .ok_or_else(|| CoreError::InvalidParam(format!("function name is empty:{:?}", f)))?
        .value
        .to_lowercase();

    match name.as_str() {
        "phrase" | "text" | "score" => {}
        _ => {
            return Err(CoreError::InvalidParam(format!(
                "function name:{:?} is not support only support phrase,text,fn",
                name
            )));
        }
    }

    if let sqlparser::ast::FunctionArguments::List(args) = args {
        let mut iter = args.args.iter();
        let value = match iter
            .next()
            .ok_or_else(|| CoreError::InvalidParam(format!("function value is empty:{:?}", f)))?
        {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => expr,
            _ => {
                return Err(CoreError::InvalidParam(format!(
                    "function value is not unnamed:{:?}",
                    f
                )));
            }
        }
        .clone();

        let mut function = Function {
            name,
            value,
            boost: 1.0,
            slop: 0,
            operator: "or".to_string(),
        };

        for value in iter {
            match value {
                FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::BinaryOp {
                    left,
                    op: _,
                    right,
                })) => match expr_liternal(left)?.as_ref() {
                    "boost" | "score" => {
                        function.boost = expr_liternal(right)?.parse::<f32>()?;
                    }
                    "slop" => {
                        function.slop = expr_liternal(right)?.parse::<i32>()?;
                    }
                    "operator" => {
                        function.operator = expr_liternal(right)?;
                    }
                    _ => {
                        return Err(CoreError::InvalidParam(format!(
                            "function value is not supprot:{:?}",
                            f
                        )));
                    }
                },
                _ => {
                    return Err(CoreError::InvalidParam(format!(
                        "function value is not unnamed:{:?}",
                        f
                    )));
                }
            }
        }
        Ok(function)
    } else {
        Err(CoreError::InvalidParam(format!(
            "function value is not support:{:?}",
            f
        )))
    }
}

fn expr_liternal(expr: &Expr) -> CoreResult<String> {
    match expr {
        Expr::Identifier(v) => Ok(v.value.to_string()),
        Expr::Value(v) => Ok(value_to_str(v)?.to_string()),
        _ => Err(CoreError::InvalidParam(format!("{:?}", expr))),
    }
}

fn value_to_str(value: &sqlparser::ast::Value) -> CoreResult<&str> {
    match value {
        sqlparser::ast::Value::Number(v, _) => Ok(v),
        sqlparser::ast::Value::SingleQuotedString(v) => Ok(v),
        _ => Err(CoreError::InvalidParam(format!("{:?}", value))),
    }
}

#[cfg(test)]
mod tests {

    use std::sync::{Arc, RwLock};

    use proto::core::field;

    use super::*;

    fn create_test_scope() -> Scope {
        let schema = crate::easy_schema(
            "test",
            vec![
                ("id".to_string(), field::Type::Int, None),
                ("name".to_string(), field::Type::String, None),
                ("age".to_string(), field::Type::Int, None),
                ("content".to_string(), field::Type::Text, None),
            ],
        );

        let user_field = schema
            .fields
            .clone()
            .into_iter()
            .map(|(k, v)| (k, Arc::new(v)))
            .collect();

        let scope = Scope {
            schema,
            user_fields: RwLock::new(user_field),
        };

        scope
    }

    #[test]
    fn test_sql_to_query() {
        let scope = create_test_scope();

        // 测试基本选择
        let sql = "SELECT id, name FROM test";
        let query = sql_to_query(&scope, sql).unwrap();
        match query {
            Query::Search {
                projection,
                query,
                order_by,
                limit,
            } => {
                assert_eq!(projection, vec!["id", "name"]);
                assert!(query.is_none());
                assert!(order_by.is_empty());
                assert_eq!(limit, (0, 10));
            }
            _ => panic!("Unexpected query type"),
        }

        // 测试带条件的查询
        let sql = "SELECT id, name FROM test WHERE age > 20";
        let query = sql_to_query(&scope, sql).unwrap();
        match query {
            Query::Search {
                projection,
                query: Some(query),
                ..
            } => {
                assert_eq!(projection, vec!["id", "name"]);
                match *query {
                    Query::Between {
                        low,
                        low_eq,
                        high,
                        high_eq,
                        field,
                        ..
                    } => {
                        assert_eq!(field.name, "age");
                        assert_eq!(low, Some(vec![20]));
                        assert!(!low_eq);
                        assert!(high.is_none());
                        assert!(!high_eq);
                    }
                    _ => panic!("Expected Between query"),
                }
            }
            _ => panic!("Unexpected query type"),
        }

        // 测试带排序和限制的查询
        let sql = "SELECT id, name FROM test ORDER BY age DESC LIMIT 5 OFFSET 10";
        let query = sql_to_query(&scope, sql).unwrap();
        match query {
            Query::Search {
                projection,
                order_by,
                limit,
                ..
            } => {
                assert_eq!(projection, vec!["id", "name"]);
                assert_eq!(order_by, vec![("age".to_string(), false)]);
                assert_eq!(limit, (10, 5));
            }
            _ => panic!("Unexpected query type"),
        }

        // 测试错误情况
        let sql = "SELECT invalid_field FROM test";
        assert!(sql_to_query(&scope, sql).is_err());
    }

    #[test]
    fn test_sql_to_query2() {
        let scope = create_test_scope();
        // 测试基本选择
        let sql = "SELECT * FROM test where content = phrase('hello world', boost=2.0, slop=1)";
        let query = sql_to_query(&scope, sql).unwrap();
        assert!(format!("{:#?}", query).contains(" Phrase {"));

        let sql =
            "SELECT * FROM test where content = text('hello world', boost=2.0, operator='and')";
        let query = sql_to_query(&scope, sql).unwrap();
        let query = format!("{:#?}", query);
        assert!(query.contains(" Text {"));
        assert!(query.contains("boost: 2.0"));
        assert!(query.contains("operator: \"and\""));

        let sql = "SELECT * FROM test where name = score('hello world', boost=2.0)";
        let query = sql_to_query(&scope, sql).unwrap();
        let query = format!("{:#?}", query);
        assert!(query.contains("boost: 2.0"));
        assert!(query.contains("Term {"));

        let sql = "SELECT * FROM test where content = score('hello world', boost=2.0)";
        let query = sql_to_query(&scope, sql).unwrap();
        let query = format!("{:#?}", query);
        assert!(query.contains("boost: 2.0"));
        assert!(query.contains(" Text {"));
        assert!(query.contains("operator: \"or\""));

        let sql = "SELECT * FROM test where content = 'hello world'";
        let query = sql_to_query(&scope, sql).unwrap();
        let query = format!("{:#?}", query);
        assert!(query.contains(" Text {"));
        assert!(query.contains("operator: \"or\""));
        assert!(query.contains("boost: 1.0"));
    }
}
