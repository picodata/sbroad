use pretty_assertions::assert_eq;

use crate::executor::bucket::Buckets;
use crate::executor::engine::mock::EngineMock;
use crate::executor::engine::Engine;
use crate::executor::Query;
use crate::ir::value::Value;

#[test]
fn simple_union_query() {
    let query = r#"SELECT * FROM (
    SELECT * FROM "test_space" WHERE "sysFrom" > 0
    UNION ALL
    SELECT * FROM "test_space_hist" WHERE "sysFrom" < 0
    ) as "t3"
    WHERE "id" = 1"#;

    let engine = EngineMock::new();
    let mut query = Query::new(&engine, query, &[]).unwrap();
    let plan = query.exec_plan.get_ir_plan();
    let top = plan.get_top().unwrap();
    let buckets = query.bucket_discovery(top).unwrap();

    let param1 = Value::number_from_str("1").unwrap();

    let bucket1 = query.engine.determine_bucket_id(&[&param1]);
    let expected = Buckets::new_filtered([bucket1].into());

    assert_eq!(expected, buckets);
}

#[test]
fn simple_disjunction_in_union_query() {
    let query = r#"SELECT * FROM (
    SELECT * FROM "test_space" WHERE "sysFrom" > 0
    UNION ALL
    SELECT * FROM "test_space_hist" WHERE "sysFrom" < 0
    ) as "t3"
    WHERE ("id" = 1) OR ("id" = 100)"#;

    let engine = EngineMock::new();
    let mut query = Query::new(&engine, query, &[]).unwrap();
    let plan = query.exec_plan.get_ir_plan();
    let top = plan.get_top().unwrap();
    let buckets = query.bucket_discovery(top).unwrap();

    let param1 = Value::number_from_str("1").unwrap();
    let bucket1 = query.engine.determine_bucket_id(&[&param1]);

    let param100 = Value::number_from_str("100").unwrap();
    let bucket100 = query.engine.determine_bucket_id(&[&param100]);

    let expected = Buckets::new_filtered([bucket1, bucket100].into());

    assert_eq!(expected, buckets);
}

#[test]
fn complex_shard_key_union_query() {
    let query = r#"SELECT *
    FROM
        (SELECT "identification_number", "product_code"
        FROM "hash_testing"
        WHERE "sys_op" = 1
        UNION ALL
        SELECT "identification_number", "product_code"
        FROM "hash_testing_hist"
        WHERE "sys_op" > 1) AS "t3"
    WHERE "identification_number" = 1 AND "product_code" = '222'"#;

    let engine = EngineMock::new();
    let mut query = Query::new(&engine, query, &[]).unwrap();
    let plan = query.exec_plan.get_ir_plan();
    let top = plan.get_top().unwrap();
    let buckets = query.bucket_discovery(top).unwrap();

    let param1 = Value::number_from_str("1").unwrap();
    let param222 = Value::string_from_str("222");

    let bucket = query.engine.determine_bucket_id(&[&param1, &param222]);
    let expected = Buckets::new_filtered([bucket].into());

    assert_eq!(expected, buckets);
}

#[test]
fn union_complex_cond_query() {
    let query = r#"SELECT *
    FROM
        (SELECT "identification_number", "product_code"
        FROM "hash_testing"
        WHERE "sys_op" = 1
        UNION ALL
        SELECT "identification_number", "product_code"
        FROM "hash_testing_hist"
        WHERE "sys_op" > 1) AS "t3"
    WHERE ("identification_number" = 1
        OR ("identification_number" = 100
        OR "identification_number" = 1000))
        AND ("product_code" = '222'
        OR "product_code" = '111')"#;

    let engine = EngineMock::new();
    let mut query = Query::new(&engine, query, &[]).unwrap();
    let plan = query.exec_plan.get_ir_plan();
    let top = plan.get_top().unwrap();
    let buckets = query.bucket_discovery(top).unwrap();

    let param1 = Value::number_from_str("1").unwrap();
    let param100 = Value::number_from_str("100").unwrap();
    let param1000 = Value::number_from_str("1000").unwrap();
    let param222 = Value::string_from_str("222");
    let param111 = Value::string_from_str("111");

    let bucket1222 = query.engine.determine_bucket_id(&[&param1, &param222]);
    let bucket100222 = query.engine.determine_bucket_id(&[&param100, &param222]);
    let bucket1000222 = query.engine.determine_bucket_id(&[&param1000, &param222]);

    let bucket1111 = query.engine.determine_bucket_id(&[&param1, &param111]);
    let bucket100111 = query.engine.determine_bucket_id(&[&param100, &param111]);
    let bucket1000111 = query.engine.determine_bucket_id(&[&param1000, &param111]);

    let expected = Buckets::new_filtered(
        [
            bucket1222,
            bucket100222,
            bucket1000222,
            bucket1111,
            bucket100111,
            bucket1000111,
        ]
        .into(),
    );

    assert_eq!(expected, buckets);
}

#[test]
fn union_query_conjunction() {
    let query = r#"SELECT * FROM "test_space" WHERE "id" = 1
    UNION ALL
    SELECT * FROM "test_space_hist" WHERE "id" = 2"#;

    let engine = EngineMock::new();
    let mut query = Query::new(&engine, query, &[]).unwrap();
    let plan = query.exec_plan.get_ir_plan();
    let top = plan.get_top().unwrap();
    let buckets = query.bucket_discovery(top).unwrap();

    let param1 = Value::number_from_str("1").unwrap();
    let bucket1 = query.engine.determine_bucket_id(&[&param1]);

    let param2 = Value::number_from_str("2").unwrap();
    let bucket2 = query.engine.determine_bucket_id(&[&param2]);
    let expected = Buckets::new_filtered([bucket1, bucket2].into());

    assert_eq!(expected, buckets);
}
