use sqlparser::{ast::*, keywords::Keyword, parser::Parser, tokenizer::Token};

use crate::util::CoreResult;

lazy_static::lazy_static! {
    static ref DIALECT: sqlparser::dialect::MySqlDialect = sqlparser::dialect::MySqlDialect {};
}

#[derive(Debug)]
pub struct Statement {
    /// projection expressions
    pub projection: Vec<SelectItem>,
    /// FROM
    #[allow(unused)]
    pub from: Vec<TableWithJoins>,
    /// WHERE
    pub query: Option<Expr>,
    /// GROUP BY
    pub order_by: Vec<OrderByExpr>,
    /// `LIMIT { <N> } BY { <expr>,<expr>,... } }`
    pub limit: Option<Expr>,
    /// `OFFSET <N> [ { ROW | ROWS } ]`
    pub offset: Option<Offset>,
}

pub fn parse_where(sql: &str) -> CoreResult<Option<Expr>> {
    if sql.is_empty() {
        return Ok(None);
    }
    let mut parser = Parser::new(&*DIALECT).try_with_sql(sql)?;
    Ok(Some(parser.parse_expr()?))
}

pub fn sql_to_statement(sql: &str) -> CoreResult<Statement> {
    let mut parser = Parser::new(&*DIALECT).try_with_sql(sql)?;

    if !parser.parse_keyword(Keyword::SELECT) {
        return Err(crate::util::CoreError::InvalidParam(format!(
            "only support start with SELECT sql:{:?}",
            sql
        )));
    }

    let projection = parser.parse_projection()?;

    let from = if parser.parse_keyword(Keyword::FROM) {
        parser.parse_comma_separated(Parser::parse_table_and_joins)?
    } else {
        vec![]
    };

    let query = if parser.parse_keyword(Keyword::WHERE) {
        Some(parser.parse_expr()?)
    } else {
        None
    };

    let order_by = if parser.parse_keywords(&[Keyword::ORDER, Keyword::BY]) {
        parser
            .parse_comma_separated(Parser::parse_order_by_expr)
            .unwrap()
    } else {
        vec![]
    };

    let mut limit = None;
    let mut offset = None;

    for _x in 0..2 {
        if limit.is_none() && parser.parse_keyword(Keyword::LIMIT) {
            limit = parser.parse_limit()?
        }

        if limit.is_some() && offset.is_none() && parser.consume_token(&Token::Comma) {
            offset = Some(Offset {
                value: limit.unwrap(),
                rows: OffsetRows::None,
            });
            limit = Some(parser.parse_expr()?);
        }
    }

    Ok(Statement {
        projection,
        from,
        query,
        order_by,
        limit,
        offset,
    })
}

#[test]
fn test_statement() {
    let sql = "select id,name from a where a > 1";
    let statement = sql_to_statement(sql).unwrap();

    let sql = "select abs(id),name, t1/t2,123 from a where name='hello' and age> 20 and age<30 order by age desc limit 10, 100;";
    let statement = sql_to_statement(sql).unwrap();

    let sql = "select abs(id),name, t1/t2,123 from a where name='hello' and age> 20 and age<30 query content='aaaa' and title='ccc' order by age desc limit 10, 100;";
    let statement = sql_to_statement(sql).unwrap();

    let sql = "select id,name from a1 where age> 20 QUERY content = '[1,2,3,4,5]' limit 100";
    let statement = sql_to_statement(sql).unwrap();

    let sql = "select id,name from a1 where age> 20 QUERY content = '[1,2,3,4,5]' OR data = phrase('java php') OR data = score(phrase('java php'), 2) OR data =score('java', 10) limit 100";
    let statement = sql_to_statement(sql).unwrap();
    println!("{:#?}", statement);
}
