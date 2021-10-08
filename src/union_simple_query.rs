use sqlparser::ast::{Query, Expr, Select, BinaryOperator, SetOperator, SetExpr};
use crate::errors::QueryPlannerError;
use crate::parser::QueryPlaner;

pub struct UnionSimpleQuery {
    subquery_ast: Box<Query>,
    parent_cond: Option<Expr>,
}

impl UnionSimpleQuery {
    pub fn new(subquery_ast: Box<Query>, parent_cond: Option<Expr>) -> Self {
        UnionSimpleQuery {
            subquery_ast,
            parent_cond,
        }
    }
}

impl QueryPlaner for UnionSimpleQuery {
    fn parse(&self) -> Result<Vec<Box<Select>>, QueryPlannerError> {
        let mut result = Vec::new();

        let subqueries = match self.extract_subquery() {
            Ok(q) => q,
            Err(e) => return Err(e)
        };

        for s in subqueries.iter() {
            let mut subquery = s.to_owned();
            subquery.selection = Some(Expr::BinaryOp {
                left: Box::new(Expr::Nested(Box::new(self.parent_cond.clone().unwrap()))),
                op: BinaryOperator::And,
                right: Box::new(Expr::Nested(Box::new(subquery.selection.unwrap()))),
            });
            result.push(subquery);
        }
        Ok(result)
    }
}

impl UnionSimpleQuery {
    fn extract_subquery(&self) -> Result<Vec<Box<Select>>, QueryPlannerError> {
        let mut result = Vec::new();

        match &self.subquery_ast.body {
            SetExpr::SetOperation { op, all: _, left, right } => {
                if op == &SetOperator::Union {
                    if let SetExpr::Select(current_query) = *left.to_owned() {
                        result.push(current_query);
                    }

                    if let SetExpr::Select(current_query) = *right.to_owned() {
                        result.push(current_query);
                    }
                }
            }
            _ => return Err(QueryPlannerError::SimpleUnionQueryError)
        }

        Ok(result)
    }
}

#[test]
fn test_simple_select_query() {
    use crate::query::query_to_ast;
    use sqlparser::ast::{Ident, Value};

    let test_query_ast = query_to_ast("SELECT * FROM \"test_space\" WHERE \"sysFrom\" > 0
    UNION ALL
    SELECT * FROM \"test_space\" WHERE \"sysFrom\" < 0").unwrap();
    let parent_cond = Some(
        Expr::BinaryOp {
            left: Box::new(Expr::Identifier(Ident { value: "id".to_string(), quote_style: Some('"') })),
            op: BinaryOperator::Eq,
            right: Box::new(Expr::Value(Value::Number("1".to_string(), false))),
        }
    );


    let q = UnionSimpleQuery::new(test_query_ast, parent_cond);

    let result = q.parse().unwrap();

    assert_eq!(result[0].to_string(), "SELECT * FROM \"test_space\" WHERE (\"id\" = 1) AND (\"sysFrom\" > 0)");
    assert_eq!(result[1].to_string(), "SELECT * FROM \"test_space\" WHERE (\"id\" = 1) AND (\"sysFrom\" < 0)");
}