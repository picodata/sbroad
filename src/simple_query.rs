use crate::parser::QueryPlaner;
use sqlparser::ast::Select;
use crate::errors::QueryPlannerError;

pub struct SimpleQuery {
    ast: Box<Select>
}

impl SimpleQuery {
    pub fn new(ast: Box<Select>) -> Self {
        SimpleQuery {
            ast
        }
    }
}

impl QueryPlaner for SimpleQuery {
    fn parse(&self) -> Result<Vec<Box<Select>>, QueryPlannerError> {
        let mut result = Vec::new();
        result.push(self.ast.to_owned());
        Ok(result)
    }
}

#[test]
fn test_simple_select_query() {
    use crate::query::query_to_ast;
    use sqlparser::ast::SetExpr;

    let test_query_ast = match query_to_ast("SELECT * FROM \"test_space\" WHERE \"id\" = 1") {
        Ok(q) => {
            match q.body {
                SetExpr::Select(s) => { s }
                _ => panic!("test error")
            }
        }
        _ => panic!("test error")
    };

    let q = SimpleQuery::new(test_query_ast);

    let result = q.parse().unwrap();

    assert_eq!(result[0].to_string(), "SELECT * FROM \"test_space\" WHERE \"id\" = 1");
}