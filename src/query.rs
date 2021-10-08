use sqlparser::ast::{Statement, Query, SetExpr, TableFactor, Expr, BinaryOperator, Select};
use crate::schema::ClusterSchema;
use sqlparser::parser::Parser;
use sqlparser::dialect::GenericDialect;
use crate::simple_query::SimpleQuery;
use crate::union_simple_query::UnionSimpleQuery;
use crate::parser::{QueryPlaner, QueryResult};
use crate::bucket::get_bucket_id;
use std::collections::HashMap;
use crate::errors::QueryPlannerError;


pub fn query_to_ast(query: &str) -> Result<Box<Query>, QueryPlannerError> {
    let dialect = GenericDialect {};

    let stm = Parser::parse_sql(&dialect, query).unwrap();

    let ast = match stm.to_owned().pop().unwrap() {
        Statement::Query(s) => s,
        _ => return Err(QueryPlannerError::QueryNotImplemented)
    };

    Ok(ast)
}

pub struct UserQuery {
    ast: Box<Query>,
    schema: ClusterSchema,
    bucket_count: u64,
}

impl UserQuery {
    pub fn new(query: &str, schema: ClusterSchema, bucket_count: u64) -> Result<Self, QueryPlannerError> {
        let ast = match query_to_ast(query) {
            Ok(s) => s,
            _ => return Err(QueryPlannerError::QueryNotImplemented)
        };

        Ok(UserQuery {
            ast,
            schema,
            bucket_count,
        })
    }

    fn detect_query_type(&self) -> Result<Box<dyn QueryPlaner>, QueryPlannerError> {
        return match &self.ast.body {
            SetExpr::Select(current_query) => {
                for t in current_query.from.iter() {
                    if t.joins.len() != 0 {
                        return Err(QueryPlannerError::QueryNotImplemented);
                    }

                    return match &t.relation {
                        TableFactor::Table { name: _, alias: _, args: _, with_hints: _ } => {
                            Ok(Box::new(SimpleQuery::new(current_query.clone())))
                        }
                        TableFactor::Derived { lateral: _, subquery, alias: _ } => {
                            Ok(Box::new(UnionSimpleQuery::new(subquery.to_owned(), current_query.selection.to_owned())))
                        }
                        _ => Err(QueryPlannerError::QueryNotImplemented)
                    };
                }

                Err(QueryPlannerError::QueryNotImplemented)
            }
            _ => Err(QueryPlannerError::QueryNotImplemented)
        };
    }

    pub fn transform(&self) -> Result<Vec<QueryResult>, QueryPlannerError> {
        let mut result = Vec::new();

        let query_set = match self.detect_query_type() {
            Ok(q) => q.parse().unwrap(),
            Err(e) => return Err(e)
        };

        for sq in query_set.iter() {
            let shard_info = match self.extract_shard_info(&sq) {
                Ok(i) => i,
                Err(e) => return Err(e)
            };

            for k in shard_info.keys {
                let mut sub_result = QueryResult::new();

                sub_result.node_query.push_str(&sq.to_string());

                sub_result.bucket_id = match get_bucket_id(&k, &shard_info.sharding_keys, self.bucket_count) {
                    Ok(r) => r,
                    _ => 0
                };

                result.push(sub_result)
            }
        }


        Ok(result)
    }

    fn extract_shard_info(&self, select_query: &Box<Select>) -> Result<ShardInfo, QueryPlannerError> {
        let t = select_query.from.clone().pop().unwrap();
        if t.joins.len() > 0 {
            return Err(QueryPlannerError::SimpleQueryError);
        }

        let sharding_key = match &t.relation {
            TableFactor::Table { name, alias: _, args: _, with_hints: _ } => {
                let table = name.to_string().replace("\"", "");
                self.schema.to_owned().get_sharding_key_by_space(&table)
            },
            _ => return Err(QueryPlannerError::SimpleQueryError)
        };
        let mut result = ShardInfo::from(sharding_key);

        let filters = select_query.to_owned().selection.unwrap();
        let sharding_key_parts = match extract_sharding_key_values(&filters, &result.sharding_keys) {
            Ok(r) => r,
            Err(e) => return Err(e),
        };

        match result.set_sharding_key_values(sharding_key_parts) {
            Err(e) => return Err(e),
            _ => ()
        };

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

impl ShardInfo {
    /// Function transform parts sharding key `query_values` to result sharding key value.
    /// For example there is sharding key parts `query_values`:
    ///    {"col1": [1, 3], "col2": [2, 4]}
    ///
    /// Function result for example above will be:
    ///    [{ "col1": 1, "col2": 2}, { "col1": 4, "col2": 4}]
    ///
    /// If parts key in `query_values` have different count of values, then function works
    /// with shortest from it's, because it count equals count of full sharding key value.
    fn set_sharding_key_values(&mut self, query_values: HashMap<String, Vec<String>>) -> Result<(), QueryPlannerError> {
        let mut count_sharding_key_vals = usize::MAX;
        for (_, v) in &query_values {
            if count_sharding_key_vals > v.len() {
                count_sharding_key_vals = v.len();
            }
        }

        if count_sharding_key_vals == usize::MAX {
            return Err(QueryPlannerError::ShardingKeyFilterError);
        }

        // loop needs for transforming  parts of sharding key to sharding key values, for example:
        for i in 0..count_sharding_key_vals {
            let mut tmp = HashMap::new();
            for k in self.sharding_keys.iter() {
                tmp.insert(k.to_string(), query_values[k][i].clone());
            }
            self.keys.push(tmp);
        }

        Ok(())
    }
}

/// Function extract part of sharding key from `e` condition, and append it to map of sharding key parts (`out_map`).
///
/// For example there is condition (sharding key is <col1, col2>):
/// 
///     (col1 = 1 and col2 = 2) OR (col1 = 3 and col2 = 4)
///
/// This condition will parse to recursive structure (`e: Expr`):
///                                         BinaryOperator::Or
///                                  /                           \
///                           Expr::Nested                     Expr::Nested
///                             /                                    \
///                     BinaryOperator::And                        BinaryOperator::And
///                  /                      \                    /                   \
///           BinaryOperator::Eq       BinaryOperator::Eq    BinaryOperator::Eq    BinaryOperator::Eq
///             /       \                  /   \                  /   \                 /   \
///           col1      1                col2   2               col1  3               col2   4
///
///
/// for above condition example `out_map` will be:
///
///  {
///     "col1": [1, 3],
///     "col2": [2, 4],
///  }
///
/// Variable `out_rec_count` contains count of sharding key values from query condition.
fn extract_sharding_key_values(e: &Expr, sharding_key: &Vec<String>) -> Result<HashMap<String, Vec<String>>, QueryPlannerError> {
    let mut result: HashMap<String, Vec<String>> = HashMap::new();
    for r in sharding_key {
        result.insert(r.to_string(), Vec::new());
    }

    match e {
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::Eq => {
                let field_name = left.to_string().to_lowercase().replace("\"", "");
                if let Some(key) = result.get_mut(&field_name) {
                    key.push(right.to_string().replace("\"", ""));
                }
            }

            BinaryOperator::And | BinaryOperator::Or => {
                match extract_sharding_key_values(&left, sharding_key) {
                    Ok(res) => {
                        for (k, val) in res {
                            if let Some(key) = result.get_mut(&k) {
                                key.extend(val);
                            }
                        }
                    }
                    Err(e) => return Err(e)
                };

                match extract_sharding_key_values(&right, sharding_key) {
                    Ok(res) => {
                        for (k, val) in res {
                            if let Some(key) = result.get_mut(&k) {
                                key.extend(val);
                            }
                        }
                    }
                    Err(e) => return Err(e)
                };
            }

            _ => ()
        },
        Expr::Nested(e) => {
            return extract_sharding_key_values(&e, sharding_key);
        }
        Expr::InSubquery { expr: _, subquery: _, negated: _ } => {
            return Err(QueryPlannerError::QueryNotImplemented);
        }
        // Another operation doesn't support now, because first of all needs make query with equijoin
        _ => ()
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

        let s = ClusterSchema::from(TEST_SCHEMA.to_string());


        let mut expected_result = Vec::new();
        expected_result.push(QueryResult {
            bucket_id: 3939,
            node_query: "SELECT * FROM \"test_space\" WHERE \"id\" = 1".to_string(),
        });

        let q = UserQuery::new(test_query, s.clone(), 30000).unwrap();
        assert_eq!(q.transform().unwrap(), expected_result)
    }

    #[test]
    fn test_simple_union_query() {
        let s = ClusterSchema::from(TEST_SCHEMA.to_string());

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
            node_query: "SELECT * FROM \"test_space\" WHERE (\"id\" = 1) AND (\"sysFrom\" > 0)".to_string(),
        });
        expected_result.push(QueryResult {
            bucket_id: 3939,
            node_query: "SELECT * FROM \"test_space\" WHERE (\"id\" = 1) AND (\"sysFrom\" < 0)".to_string(),
        });

        let q = UserQuery::new(test_query, s.clone(), 30000).unwrap();
        assert_eq!(q.transform().unwrap(), expected_result)
    }

    #[test]
    fn test_simple_union_complex_shard_query() {
        let s = ClusterSchema::from(TEST_SCHEMA.to_string());

        let test_query = "SELECT * FROM (
    SELECT * FROM \"complex_idx_test\" WHERE \"sysFrom\" > 0
    UNION ALL
    SELECT * FROM \"complex_idx_test\" WHERE \"sysFrom\" < 0
    ) as \"t3\"
    WHERE \"identification_number\" = 1 AND \"product_code\" = \"222\"
    ";

        let mut expected_result = Vec::new();
        expected_result.push(QueryResult {
            bucket_id: 2926,
            node_query: "SELECT * FROM \"complex_idx_test\" WHERE (\"identification_number\" = 1 AND \"product_code\" = \"222\") AND (\"sysFrom\" > 0)".to_string(),
        });
        expected_result.push(QueryResult {
            bucket_id: 2926,
            node_query: "SELECT * FROM \"complex_idx_test\" WHERE (\"identification_number\" = 1 AND \"product_code\" = \"222\") AND (\"sysFrom\" < 0)".to_string(),
        });

        let q = UserQuery::new(test_query, s.clone(), 30000).unwrap();
        assert_eq!(q.transform().unwrap(), expected_result)
    }

    #[test]
    fn test_simple_disjunction_in_union_query() {
        let s = ClusterSchema::from(TEST_SCHEMA.to_string());

        let test_query = "SELECT * FROM (
    SELECT * FROM \"test_space\" WHERE \"sysFrom\" > 0
    UNION ALL
    SELECT * FROM \"test_space\" WHERE \"sysFrom\" < 0
    ) as \"t3\"
    WHERE \"id\" = 1 OR \"id\" = 100
    ";

        let mut expected_result = Vec::new();
        expected_result.push(QueryResult {
            bucket_id: 3939,
            node_query: "SELECT * FROM \"test_space\" WHERE (\"id\" = 1 OR \"id\" = 100) AND (\"sysFrom\" > 0)".to_string(),
        });
        expected_result.push(QueryResult {
            bucket_id: 18511,
            node_query: "SELECT * FROM \"test_space\" WHERE (\"id\" = 1 OR \"id\" = 100) AND (\"sysFrom\" > 0)".to_string(),
        });
        expected_result.push(QueryResult {
            bucket_id: 3939,
            node_query: "SELECT * FROM \"test_space\" WHERE (\"id\" = 1 OR \"id\" = 100) AND (\"sysFrom\" < 0)".to_string(),
        });
        expected_result.push(QueryResult {
            bucket_id: 18511,
            node_query: "SELECT * FROM \"test_space\" WHERE (\"id\" = 1 OR \"id\" = 100) AND (\"sysFrom\" < 0)".to_string(),
        });

        let q = UserQuery::new(test_query, s.clone(), 30000).unwrap();
        assert_eq!(q.transform().unwrap(), expected_result)
    }

    #[test]
    fn test_complex_disjunction_union_query() {
        let s = ClusterSchema::from(TEST_SCHEMA.to_string());

        let test_query = "SELECT * FROM (
    SELECT * FROM \"complex_idx_test\" WHERE \"sys_op\" > 0
    UNION ALL
    SELECT * FROM \"complex_idx_test\" WHERE \"sys_op\" < 0
    ) as \"t3\"
    WHERE (\"identification_number\" = 1 AND \"product_code\" = \"222\")
        AND (\"identification_number\" = 100 AND \"product_code\" = \"111\")
    ";

        let mut expected_result = Vec::new();
        expected_result.push(QueryResult {
            bucket_id: 2926,
            node_query: "SELECT * FROM \"complex_idx_test\" WHERE ((\"identification_number\" = 1 AND \"product_code\" = \"222\") AND (\"identification_number\" = 100 AND \"product_code\" = \"111\")) AND (\"sys_op\" > 0)".to_string(),
        });
        expected_result.push(QueryResult {
            bucket_id: 4202,
            node_query: "SELECT * FROM \"complex_idx_test\" WHERE ((\"identification_number\" = 1 AND \"product_code\" = \"222\") AND (\"identification_number\" = 100 AND \"product_code\" = \"111\")) AND (\"sys_op\" > 0)".to_string(),
        });
        expected_result.push(QueryResult {
            bucket_id: 2926,
            node_query: "SELECT * FROM \"complex_idx_test\" WHERE ((\"identification_number\" = 1 AND \"product_code\" = \"222\") AND (\"identification_number\" = 100 AND \"product_code\" = \"111\")) AND (\"sys_op\" < 0)".to_string(),
        });
        expected_result.push(QueryResult {
            bucket_id: 4202,
            node_query: "SELECT * FROM \"complex_idx_test\" WHERE ((\"identification_number\" = 1 AND \"product_code\" = \"222\") AND (\"identification_number\" = 100 AND \"product_code\" = \"111\")) AND (\"sys_op\" < 0)".to_string(),
        });

        let q = UserQuery::new(test_query, s.clone(), 30000).unwrap();
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

        let mut expected_result = Vec::new();
        expected_result.push(QueryResult {
            bucket_id: 3939,
            node_query: "SELECT * FROM \"test_space\" WHERE (\"id\" = 1 OR (\"id\" = 2 OR \"id\" = 3)) AND (\"sys_from\" <= 0 AND \"sys_to\" >= 0)".to_string(),
        });
        expected_result.push(QueryResult {
            bucket_id: 22071,
            node_query: "SELECT * FROM \"test_space\" WHERE (\"id\" = 1 OR (\"id\" = 2 OR \"id\" = 3)) AND (\"sys_from\" <= 0 AND \"sys_to\" >= 0)".to_string(),
        });
        expected_result.push(QueryResult {
            bucket_id: 21300,
            node_query: "SELECT * FROM \"test_space\" WHERE (\"id\" = 1 OR (\"id\" = 2 OR \"id\" = 3)) AND (\"sys_from\" <= 0 AND \"sys_to\" >= 0)".to_string(),
        });
        expected_result.push(QueryResult {
            bucket_id: 3939,
            node_query: "SELECT * FROM \"test_space\" WHERE (\"id\" = 1 OR (\"id\" = 2 OR \"id\" = 3)) AND (\"sys_from\" <= 0)".to_string(),
        });
        expected_result.push(QueryResult {
            bucket_id: 22071,
            node_query: "SELECT * FROM \"test_space\" WHERE (\"id\" = 1 OR (\"id\" = 2 OR \"id\" = 3)) AND (\"sys_from\" <= 0)".to_string(),
        });
        expected_result.push(QueryResult {
            bucket_id: 21300,
            node_query: "SELECT * FROM \"test_space\" WHERE (\"id\" = 1 OR (\"id\" = 2 OR \"id\" = 3)) AND (\"sys_from\" <= 0)".to_string(),
        });

        let s = ClusterSchema::from(TEST_SCHEMA.to_string());
        let q = UserQuery::new(test_query, s, 30000).unwrap();
        assert_eq!(q.transform().unwrap(), expected_result)
    }

    #[test]
    fn test_unsupported_query() {
        let s = ClusterSchema::from(TEST_SCHEMA.to_string());

        let mut test_query = "SELECT * FROM \"test_space\" as s1
    inner join \"join_space\" as s2 on s1.\"f1\" = s2.\"s1_id\"
    WHERE \"field_1\" = 86";
        let mut q = UserQuery::new(test_query, s.clone(), 30000).unwrap();
        assert_eq!(q.transform().unwrap_err(), QueryPlannerError::QueryNotImplemented);

        test_query = "SELECT * FROM \"test_space\" WHERE \"field_1\" in (SELECT * FROM \"test_space_2\" WHERE \"field_2\" = 5)";
        q = UserQuery::new(test_query, s.clone(), 30000).unwrap();
        assert_eq!(q.transform().unwrap_err(), QueryPlannerError::QueryNotImplemented);
    }
}

