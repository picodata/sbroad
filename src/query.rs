use crate::bucket::get_bucket_id;
use crate::errors::QueryPlannerError;
use crate::parser::{QueryPlaner, QueryResult};
use crate::schema::Cluster;
use crate::simple_query::SimpleQuery;
use crate::union_simple_query::UnionSimpleQuery;
use sqlparser::ast::{BinaryOperator, Expr, Query, Select, SetExpr, Statement, TableFactor};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::collections::HashMap;

pub fn get_ast(query: &str) -> Result<Box<Query>, QueryPlannerError> {
    let dialect = GenericDialect {};

    let mut stm = Parser::parse_sql(&dialect, query).unwrap();

    let ast = match stm.pop().unwrap() {
        Statement::Query(s) => s,
        _ => return Err(QueryPlannerError::QueryNotImplemented),
    };

    Ok(ast)
}

pub struct ParsedTree {
    ast: Box<Query>,
    schema: Cluster,
    bucket_count: u64,
}

impl ParsedTree {
    pub fn new(query: &str, schema: Cluster, bucket_count: u64) -> Result<Self, QueryPlannerError> {
        let ast = match get_ast(query) {
            Ok(s) => s,
            _ => return Err(QueryPlannerError::QueryNotImplemented),
        };

        Ok(ParsedTree {
            ast,
            schema,
            bucket_count,
        })
    }

    fn detect_query_type(&self) -> Result<Box<dyn QueryPlaner>, QueryPlannerError> {
        match &self.ast.body {
            SetExpr::Select(current_query) => {
                if let Some(t) = current_query.from.get(0) {
                    if !t.joins.is_empty() {
                        return Err(QueryPlannerError::QueryNotImplemented);
                    }

                    return match &t.relation {
                        TableFactor::Table {
                            name: _,
                            alias: _,
                            args: _,
                            with_hints: _,
                        } => Ok(Box::new(SimpleQuery::new(current_query.clone()))),
                        TableFactor::Derived {
                            lateral: _,
                            subquery,
                            alias: _,
                        } => Ok(Box::new(UnionSimpleQuery::new(
                            subquery.clone(),
                            current_query.selection.clone(),
                        ))),
                        _ => Err(QueryPlannerError::QueryNotImplemented),
                    };
                }

                Err(QueryPlannerError::QueryNotImplemented)
            }
            _ => Err(QueryPlannerError::QueryNotImplemented),
        }
    }

    pub fn transform(&self) -> Result<Vec<QueryResult>, QueryPlannerError> {
        let mut result = Vec::new();

        let query_set = self.detect_query_type()?;

        for sq in &query_set.parse().unwrap() {
            let shard_info = self.extract_shard_info(sq)?;

            for k in shard_info.keys {
                let mut sub_result = QueryResult::new();

                sub_result.node_query.push_str(&sq.to_string());

                sub_result.bucket_id =
                    match get_bucket_id(&k, &shard_info.sharding_keys, self.bucket_count) {
                        Ok(r) => r,
                        _ => 0,
                    };

                result.push(sub_result);
            }
        }

        Ok(result)
    }

    fn extract_shard_info(&self, select_query: &Select) -> Result<ShardInfo, QueryPlannerError> {
        let t = select_query.from.clone().pop().unwrap();
        if !t.joins.is_empty() {
            return Err(QueryPlannerError::SimpleQueryError);
        }

        let sharding_key = match &t.relation {
            TableFactor::Table {
                name,
                alias: _,
                args: _,
                with_hints: _,
            } => {
                let table = name.to_string().replace("\"", "");
                self.schema.clone().get_sharding_key_by_space(&table)
            }
            _ => return Err(QueryPlannerError::SimpleQueryError),
        };
        let mut result = ShardInfo::from(sharding_key);

        let filters = select_query.clone().selection.unwrap();
        let sharding_key_values = extract_sharding_key_values(&filters, &result.sharding_keys)?;

        result.keys = sharding_key_values;

        Ok(result)
    }
}

#[derive(Debug, Clone)]
struct ShardInfo {
    pub sharding_keys: Vec<String>,
    pub keys: Vec<HashMap<String, String>>,
}

impl From<Vec<String>> for ShardInfo {
    fn from(sharding_keys: Vec<String>) -> Self {
        ShardInfo {
            sharding_keys,
            keys: Vec::new(),
        }
    }
}

/// Function extract sharding key values from query filter section.
///
/// For example there is clause (sharding key is <col1, col2>):
///
///     (col1 = 1 and col2 = 2) OR (col1 = 3 and col2 = 4)
///
/// This condition will parse to ast (`e: Expr`):
///                                         `BinaryOperator::Or`
///                                  /                             \
///                           `Expr::Nested`                     `Expr::Nested`
///                             /                                      \
///                     `BinaryOperator::And`                        `BinaryOperator::And`
///                  /                       \                       /                   \
///          `BinaryOperator::Eq`       `BinaryOperator::Eq`    `BinaryOperator::Eq`    `BinaryOperator::Eq`
///             /       \                  /   \                  /   \                 /   \
///           col1      1                col2   2               col1  3               col2   4
///
///
/// for above ast query function return list sharding key values:
///     [
///      {"col1":"1", "col2":"2"},
///      {"col1":"4", "col2":"4"}
///     ]
///
/// Another example is clause:
///  ("col1" = 1 OR ("col1" = 2 OR "col1" = 3)) AND ("col2" = 4 OR "col2" = 5)
///
/// Ast in this case is:
///
///                                              `BinaryOperator::AND`
///                                  /                                       \
///                           `Expr::Nested`                                `Expr::Nested`
///                             /                                             \
///                     `BinaryOperator::Or`                                     `BinaryOperator::Or`
///                  /                      \                                    /                   \
///           `BinaryOperator::Eq`       `Expr::Nested`                  `BinaryOperator::Eq`    `BinaryOperator::Eq`
///             /       \                   \                                 /   \                 /   \
///           col1      1             `BinaryOperator::Or`                  col2   4             col2    5
///                                     /           \
///                      `BinaryOperator::Eq`      `BinaryOperator::Eq`
///                         /          \             /        \
///                      col1          2           col1       3
///
/// This ast function transforms to list:
///     [
///      {"col1":"1", "col2":"4"},
///      {"col1":"1", "col2":"5"},
///      {"col1":"2", "col2":"4"},
///      {"col1":"2", "col2":"5"},
///      {"col1":"3", "col2":"4"},
///      {"col1":"3", "col2":"5"}
///     ]

fn extract_sharding_key_values(
    e: &Expr,
    sharding_key: &[String],
) -> Result<Vec<HashMap<String, String>>, QueryPlannerError> {
    let mut result = Vec::new();

    match e {
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::Eq => {
                let mut shard_key_value = HashMap::new();

                let field_name = left.to_string().to_lowercase().replace("\"", "");

                for shard_key_part in sharding_key {
                    if shard_key_part == &field_name {
                        shard_key_value.insert(
                            field_name.to_string(),
                            right.to_string().as_str().trim_matches('\'').to_string(),
                        );
                    }
                }

                result.push(shard_key_value);
            }

            BinaryOperator::And => {
                // if operation operator `AND` needs cross join children leaves results,
                // because they contains sharding key parts (see example AST in function docs)

                let l_leaf = extract_sharding_key_values(left, sharding_key)?;

                let r_leaf = extract_sharding_key_values(right, sharding_key)?;

                if l_leaf.is_empty() {
                    result.extend(r_leaf);
                    return Ok(result);
                }

                if r_leaf.is_empty() {
                    result.extend(l_leaf);
                    return Ok(result);
                }

                // cross join hashmap vector for getting all combination of complex sharding key values.
                for i in &l_leaf {
                    for j in &r_leaf {
                        let mut v = i.clone();
                        v.extend(j.clone());
                        result.push(v);
                    }
                }
            }

            BinaryOperator::Or => {
                // if operation operator `OR` needs union results from children leaves,
                // as they contains full sharding key values (see example AST in function docs)

                let res_l = extract_sharding_key_values(left, sharding_key)?;
                for k in res_l {
                    result.push(k);
                }

                let res_r = extract_sharding_key_values(right, sharding_key)?;
                for k in res_r {
                    result.push(k);
                }
            }

            _ => (),
        },
        Expr::Nested(e) => {
            return extract_sharding_key_values(e, sharding_key);
        }
        Expr::InSubquery {
            expr: _,
            subquery: _,
            negated: _,
        } => {
            return Err(QueryPlannerError::QueryNotImplemented);
        }
        // Another operation doesn't support now, because first of all needs make query with equijoin
        _ => (),
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_SCHEMA: &str = "spaces:
  test_space:
    engine: \"memtx\"
    is_local: false
    temporary: false
    format:
      - name: \"ID\"
        is_nullable: false
        type: \"number\"
      - name: \"sysFrom\"
        is_nullable: false
        type: \"number\"
      - name: \"FIRST_NAME\"
        is_nullable: false
        type: \"string\"
      - name: \"sysOp\"
        is_nullable: false
        type: \"number\"
      - name: \"bucket_id\"
        is_nullable: true
        type: \"unsigned\"
    indexes:
      - type: \"TREE\"
        name: \"ID\"
        unique: true
        parts:
          - path: \"ID\"
            type: \"number\"
            is_nullable: false
      - type: \"TREE\"
        name: \"bucket_id\"
        unique: false
        parts:
          - path: \"bucket_id\"
            type: \"unsigned\"
            is_nullable: true
    sharding_key:
      - ID
  complex_idx_test:
    is_local: false
    temporary: false
    engine: \"memtx\"
    format:
      - name: \"identification_number\"
        type: \"integer\"
        is_nullable: false
      - name: \"product_code\"
        type: \"string\"
        is_nullable: false
      - name: \"product_units\"
        type: \"integer\"
        is_nullable: false
      - name: \"sys_op\"
        type: \"number\"
        is_nullable: false
      - name: \"bucket_id\"
        type: \"unsigned\"
        is_nullable: true
    indexes:
      - name: \"id\"
        unique: true
        type: \"TREE\"
        parts:
          - path: \"identification_number\"
            is_nullable: false
            type: \"integer\"
      - name: bucket_id
        unique: false
        parts:
          - path: \"bucket_id\"
            is_nullable: true
            type: \"unsigned\"
        type: \"TREE\"
    sharding_key:
      - identification_number
      - product_code";

    #[test]
    fn test_simple_query() {
        let test_query = "SELECT * FROM \"test_space\" WHERE \"id\" = 1";

        let s = Cluster::from(TEST_SCHEMA.to_string());

        let mut expected_result = Vec::new();
        expected_result.push(QueryResult {
            bucket_id: 3939,
            node_query: "SELECT * FROM \"test_space\" WHERE \"id\" = 1".to_string(),
        });

        let q = ParsedTree::new(test_query, s.clone(), 30000).unwrap();
        assert_eq!(q.transform().unwrap(), expected_result)
    }

    #[test]
    fn test_simple_union_query() {
        let s = Cluster::from(TEST_SCHEMA.to_string());

        let test_query = "SELECT * FROM (
    SELECT * FROM \"test_space\" WHERE \"sysFrom\" > 0
    UNION ALL
    SELECT * FROM \"test_space\" WHERE \"sysFrom\" < 0
    ) as \"t3\"
    WHERE \"id\" = 1
    ";

        let mut expected_result = Vec::new();
        expected_result.push(QueryResult {
            bucket_id: 3939,
            node_query: "SELECT * FROM \"test_space\" WHERE (\"id\" = 1) AND (\"sysFrom\" > 0)"
                .to_string(),
        });
        expected_result.push(QueryResult {
            bucket_id: 3939,
            node_query: "SELECT * FROM \"test_space\" WHERE (\"id\" = 1) AND (\"sysFrom\" < 0)"
                .to_string(),
        });

        let q = ParsedTree::new(test_query, s.clone(), 30000).unwrap();
        assert_eq!(q.transform().unwrap(), expected_result)
    }

    #[test]
    fn test_simple_union_complex_shard_query() {
        let s = Cluster::from(TEST_SCHEMA.to_string());

        let test_query = "SELECT * FROM (
    SELECT * FROM \"complex_idx_test\" WHERE \"sysFrom\" > 0
    UNION ALL
    SELECT * FROM \"complex_idx_test\" WHERE \"sysFrom\" < 0
    ) as \"t3\"
    WHERE \"identification_number\" = 1 AND \"product_code\" = '222'
    ";

        let mut expected_result = Vec::new();
        expected_result.push(QueryResult {
            bucket_id: 2926,
            node_query: "SELECT * FROM \"complex_idx_test\" WHERE (\"identification_number\" = 1 AND \"product_code\" = '222') AND (\"sysFrom\" > 0)".to_string(),
        });
        expected_result.push(QueryResult {
            bucket_id: 2926,
            node_query: "SELECT * FROM \"complex_idx_test\" WHERE (\"identification_number\" = 1 AND \"product_code\" = '222') AND (\"sysFrom\" < 0)".to_string(),
        });

        let q = ParsedTree::new(test_query, s.clone(), 30000).unwrap();
        assert_eq!(q.transform().unwrap(), expected_result)
    }

    #[test]
    fn test_simple_disjunction_in_union_query() {
        let s = Cluster::from(TEST_SCHEMA.to_string());

        let test_query = "SELECT * FROM (
    SELECT * FROM \"test_space\" WHERE \"sysFrom\" > 0
    UNION ALL
    SELECT * FROM \"test_space\" WHERE \"sysFrom\" < 0
    ) as \"t3\"
    WHERE \"id\" = 1 OR \"id\" = 100
    ";

        let first_sub_query =
            "SELECT * FROM \"test_space\" WHERE (\"id\" = 1 OR \"id\" = 100) AND (\"sysFrom\" > 0)"
                .to_string();
        let second_sub_query =
            "SELECT * FROM \"test_space\" WHERE (\"id\" = 1 OR \"id\" = 100) AND (\"sysFrom\" < 0)"
                .to_string();
        let mut expected_result = Vec::new();
        expected_result.push(QueryResult {
            bucket_id: 3939,
            node_query: first_sub_query.clone(),
        });
        expected_result.push(QueryResult {
            bucket_id: 18511,
            node_query: first_sub_query.clone(),
        });
        expected_result.push(QueryResult {
            bucket_id: 3939,
            node_query: second_sub_query.clone(),
        });
        expected_result.push(QueryResult {
            bucket_id: 18511,
            node_query: second_sub_query.clone(),
        });

        let q = ParsedTree::new(test_query, s.clone(), 30000).unwrap();
        assert_eq!(q.transform().unwrap(), expected_result)
    }

    #[test]
    fn test_complex_disjunction_union_query() {
        let s = Cluster::from(TEST_SCHEMA.to_string());

        let test_query = "SELECT * FROM (
    SELECT * FROM \"complex_idx_test\" WHERE \"sys_op\" > 0
    UNION ALL
    SELECT * FROM \"complex_idx_test\" WHERE \"sys_op\" < 0
    ) as \"t3\"
    WHERE (\"identification_number\" = 1 AND \"product_code\" = '222')
        OR (\"identification_number\" = 100 AND \"product_code\" = '111')
    ";

        let first_sub_query = "SELECT * FROM \"complex_idx_test\" WHERE ((\"identification_number\" = 1 AND \"product_code\" = '222') OR (\"identification_number\" = 100 AND \"product_code\" = '111')) AND (\"sys_op\" > 0)".to_string();
        let second_sub_query = "SELECT * FROM \"complex_idx_test\" WHERE ((\"identification_number\" = 1 AND \"product_code\" = '222') OR (\"identification_number\" = 100 AND \"product_code\" = '111')) AND (\"sys_op\" < 0)".to_string();
        let mut expected_result = Vec::new();
        expected_result.push(QueryResult {
            bucket_id: 2926,
            node_query: first_sub_query.clone(),
        });
        expected_result.push(QueryResult {
            bucket_id: 4202,
            node_query: first_sub_query.clone(),
        });
        expected_result.push(QueryResult {
            bucket_id: 2926,
            node_query: second_sub_query.clone(),
        });
        expected_result.push(QueryResult {
            bucket_id: 4202,
            node_query: second_sub_query.clone(),
        });

        let q = ParsedTree::new(test_query, s.clone(), 30000).unwrap();
        assert_eq!(q.transform().unwrap(), expected_result)
    }

    #[test]
    fn test_transform_in_cond() {
        let test_query = "SELECT * FROM (
        SELECT * FROM \"test_space\" WHERE \"sys_from\" <= 0 AND \"sys_to\" >= 0
        UNION ALL
        SELECT * FROM \"test_space\" WHERE \"sys_from\" <= 0
        ) AS \"t3\"
        WHERE \"id\" = 1 OR (\"id\" = 2 OR \"id\" = 3)";

        let first_sub_query = "SELECT * FROM \"test_space\" WHERE (\"id\" = 1 OR (\"id\" = 2 OR \"id\" = 3)) AND (\"sys_from\" <= 0 AND \"sys_to\" >= 0)".to_string();
        let second_sub_query = "SELECT * FROM \"test_space\" WHERE (\"id\" = 1 OR (\"id\" = 2 OR \"id\" = 3)) AND (\"sys_from\" <= 0)".to_string();
        let mut expected_result = Vec::new();
        expected_result.push(QueryResult {
            bucket_id: 3939,
            node_query: first_sub_query.clone(),
        });
        expected_result.push(QueryResult {
            bucket_id: 22071,
            node_query: first_sub_query.clone(),
        });
        expected_result.push(QueryResult {
            bucket_id: 21300,
            node_query: first_sub_query.clone(),
        });
        expected_result.push(QueryResult {
            bucket_id: 3939,
            node_query: second_sub_query.clone(),
        });
        expected_result.push(QueryResult {
            bucket_id: 22071,
            node_query: second_sub_query.clone(),
        });
        expected_result.push(QueryResult {
            bucket_id: 21300,
            node_query: second_sub_query.clone(),
        });

        let s = Cluster::from(TEST_SCHEMA.to_string());
        let q = ParsedTree::new(test_query, s, 30000).unwrap();
        assert_eq!(q.transform().unwrap(), expected_result)
    }

    #[test]
    fn test_transform_complex_in_cond() {
        let test_query = "SELECT * FROM (
        SELECT * FROM \"complex_idx_test\" WHERE \"sys_from\" <= 0 AND \"sys_to\" >= 0
        UNION ALL
        SELECT * FROM \"complex_idx_test\" WHERE \"sys_from\" <= 0
        ) AS \"t3\"
        WHERE (\"identification_number\" = 1 OR (\"identification_number\" = 100 OR \"identification_number\" = 1000))
            AND (\"product_code\" = '222' OR \"product_code\" = '111')
        ";

        let first_sub_query = "SELECT * FROM \"complex_idx_test\" WHERE ((\"identification_number\" = 1 OR (\"identification_number\" = 100 OR \"identification_number\" = 1000)) AND (\"product_code\" = '222' OR \"product_code\" = '111')) AND (\"sys_from\" <= 0 AND \"sys_to\" >= 0)".to_string();
        let second_sub_query = "SELECT * FROM \"complex_idx_test\" WHERE ((\"identification_number\" = 1 OR (\"identification_number\" = 100 OR \"identification_number\" = 1000)) AND (\"product_code\" = '222' OR \"product_code\" = '111')) AND (\"sys_from\" <= 0)".to_string();
        let mut expected_result = Vec::new();
        expected_result.push(QueryResult {
            bucket_id: 2926,
            node_query: first_sub_query.clone(),
        });
        expected_result.push(QueryResult {
            bucket_id: 22115,
            node_query: first_sub_query.clone(),
        });
        expected_result.push(QueryResult {
            bucket_id: 6672,
            node_query: first_sub_query.clone(),
        });
        expected_result.push(QueryResult {
            bucket_id: 4202,
            node_query: first_sub_query.clone(),
        });
        expected_result.push(QueryResult {
            bucket_id: 23259,
            node_query: first_sub_query.clone(),
        });
        expected_result.push(QueryResult {
            bucket_id: 6557,
            node_query: first_sub_query.clone(),
        });
        expected_result.push(QueryResult {
            bucket_id: 2926,
            node_query: second_sub_query.clone(),
        });
        expected_result.push(QueryResult {
            bucket_id: 22115,
            node_query: second_sub_query.clone(),
        });
        expected_result.push(QueryResult {
            bucket_id: 6672,
            node_query: second_sub_query.clone(),
        });
        expected_result.push(QueryResult {
            bucket_id: 4202,
            node_query: second_sub_query.clone(),
        });
        expected_result.push(QueryResult {
            bucket_id: 23259,
            node_query: second_sub_query.clone(),
        });
        expected_result.push(QueryResult {
            bucket_id: 6557,
            node_query: second_sub_query.clone(),
        });

        let s = Cluster::from(TEST_SCHEMA.to_string());
        let q = ParsedTree::new(test_query, s, 30000).unwrap();
        assert_eq!(q.transform().unwrap(), expected_result)
    }

    #[test]
    fn test_transform_complex_in_ext_cond() {
        let test_query = "SELECT * FROM (
        SELECT * FROM \"complex_idx_test\" WHERE \"sys_from\" <= 0 AND \"sys_to\" >= 0
        UNION ALL
        SELECT * FROM \"complex_idx_test\" WHERE \"sys_from\" <= 0
        ) AS \"t3\"
        WHERE (\"identification_number\" = 1 OR (\"identification_number\" = 100 OR \"identification_number\" = 1000))
            AND ((\"product_code\" = '222' OR \"product_code\" = '111') AND \"amount\" > 0)
        ";

        let first_sub_query = "SELECT * FROM \"complex_idx_test\" WHERE ((\"identification_number\" = 1 OR (\"identification_number\" = 100 OR \"identification_number\" = 1000)) AND ((\"product_code\" = '222' OR \"product_code\" = '111') AND \"amount\" > 0)) AND (\"sys_from\" <= 0 AND \"sys_to\" >= 0)".to_string();
        let second_sub_query = "SELECT * FROM \"complex_idx_test\" WHERE ((\"identification_number\" = 1 OR (\"identification_number\" = 100 OR \"identification_number\" = 1000)) AND ((\"product_code\" = '222' OR \"product_code\" = '111') AND \"amount\" > 0)) AND (\"sys_from\" <= 0)".to_string();
        let mut expected_result = Vec::new();
        expected_result.push(QueryResult {
            bucket_id: 2926,
            node_query: first_sub_query.clone(),
        });
        expected_result.push(QueryResult {
            bucket_id: 22115,
            node_query: first_sub_query.clone(),
        });
        expected_result.push(QueryResult {
            bucket_id: 6672,
            node_query: first_sub_query.clone(),
        });
        expected_result.push(QueryResult {
            bucket_id: 4202,
            node_query: first_sub_query.clone(),
        });
        expected_result.push(QueryResult {
            bucket_id: 23259,
            node_query: first_sub_query.clone(),
        });
        expected_result.push(QueryResult {
            bucket_id: 6557,
            node_query: first_sub_query.clone(),
        });
        expected_result.push(QueryResult {
            bucket_id: 2926,
            node_query: second_sub_query.clone(),
        });
        expected_result.push(QueryResult {
            bucket_id: 22115,
            node_query: second_sub_query.clone(),
        });
        expected_result.push(QueryResult {
            bucket_id: 6672,
            node_query: second_sub_query.clone(),
        });
        expected_result.push(QueryResult {
            bucket_id: 4202,
            node_query: second_sub_query.clone(),
        });
        expected_result.push(QueryResult {
            bucket_id: 23259,
            node_query: second_sub_query.clone(),
        });
        expected_result.push(QueryResult {
            bucket_id: 6557,
            node_query: second_sub_query.clone(),
        });

        let s = Cluster::from(TEST_SCHEMA.to_string());
        let q = ParsedTree::new(test_query, s, 30000).unwrap();
        assert_eq!(q.transform().unwrap(), expected_result)
    }

    #[test]
    fn test_unsupported_query() {
        let s = Cluster::from(TEST_SCHEMA.to_string());

        let mut test_query = "SELECT * FROM \"test_space\" as s1
    inner join \"join_space\" as s2 on s1.\"f1\" = s2.\"s1_id\"
    WHERE \"field_1\" = 86";
        let mut q = ParsedTree::new(test_query, s.clone(), 30000).unwrap();
        assert_eq!(
            q.transform().unwrap_err(),
            QueryPlannerError::QueryNotImplemented
        );

        test_query = "SELECT * FROM \"test_space\" WHERE \"field_1\" in (SELECT * FROM \"test_space_2\" WHERE \"field_2\" = 5)";
        q = ParsedTree::new(test_query, s.clone(), 30000).unwrap();
        assert_eq!(
            q.transform().unwrap_err(),
            QueryPlannerError::QueryNotImplemented
        );
    }
}
