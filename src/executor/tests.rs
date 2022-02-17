use pretty_assertions::assert_eq;

use crate::executor::engine::mock::EngineMock;

use super::*;

#[test]
fn shard_query() {
    let sql = r#"SELECT "FIRST_NAME" FROM "test_space" where "id" = 1"#;
    let engine = EngineMock::new();

    let mut query = Query::new(engine.clone(), sql).unwrap();
    query.optimize().unwrap();

    assert_eq!(engine.shard_query, query.exec().unwrap())
}

#[test]
fn map_reduce_query() {
    let sql = r#"SELECT "product_code" FROM "hash_testing" where "identification_number" = 1 and "product_code" = '457'"#;
    let engine = EngineMock::new();

    let mut query = Query::new(engine.clone(), sql).unwrap();
    query.optimize().unwrap();

    assert_eq!(engine.mp_query, query.exec().unwrap())
}
