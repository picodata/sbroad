use criterion::{black_box, criterion_group, criterion_main, Criterion};
use sbroad::backend::sql::tree::{OrderedSyntaxNodes, SyntaxPlan};
use sbroad::executor::engine::mock::RouterRuntimeMock;
use sbroad::executor::Query;
use sbroad::frontend::sql::ast::AbstractSyntaxTree;
use sbroad::frontend::Ast;
use sbroad::ir::tree::Snapshot;
use sbroad::ir::value::Value;

use pest::Parser;
use pest_derive::Parser;

#[derive(Parser)]
#[grammar = "../sbroad-core/src/frontend/sql/query.pest"]
struct ParseTree;

#[allow(clippy::too_many_lines)]
fn query_sql() -> String {
    let sql = r#"SELECT
    *
    FROM
        (
            SELECT
                "vehicleguid",
                "reestrid",
                "reestrstatus",
                "vehicleregno",
                "vehiclevin",
                "vehiclevin2",
                "vehiclechassisnum",
                "vehiclereleaseyear",
                "operationregdoctypename",
                "operationregdoc",
                "operationregdocissuedate",
                "operationregdoccomments",
                "vehicleptstypename",
                "vehicleptsnum",
                "vehicleptsissuedate",
                "vehicleptsissuer",
                "vehicleptscomments",
                "vehiclebodycolor",
                "vehiclebrand",
                "vehiclemodel",
                "vehiclebrandmodel",
                "vehiclebodynum",
                "vehiclecost",
                "vehiclegasequip",
                "vehicleproducername",
                "vehiclegrossmass",
                "vehiclemass",
                "vehiclesteeringwheeltypeid",
                "vehiclekpptype",
                "vehicletransmissiontype",
                "vehicletypename",
                "vehiclecategory",
                "vehicletypeunit",
                "vehicleecoclass",
                "vehiclespecfuncname",
                "vehicleenclosedvolume",
                "vehicleenginemodel",
                "vehicleenginenum",
                "vehicleenginepower",
                "vehicleenginepowerkw",
                "vehicleenginetype",
                "holdrestrictiondate",
                "approvalnum",
                "approvaldate",
                "approvaltype",
                "utilizationfeename",
                "customsdoc",
                "customsdocdate",
                "customsdocissue",
                "customsdocrestriction",
                "customscountryremovalid",
                "customscountryremovalname",
                "ownerorgname",
                "ownerinn",
                "ownerogrn",
                "ownerkpp",
                "ownerpersonlastname",
                "ownerpersonfirstname",
                "ownerpersonmiddlename",
                "ownerpersonbirthdate",
                "ownerbirthplace",
                "ownerpersonogrnip",
                "owneraddressindex",
                "owneraddressmundistrict",
                "owneraddresssettlement",
                "owneraddressstreet",
                "ownerpersoninn",
                "ownerpersondoccode",
                "ownerpersondocnum",
                "ownerpersondocdate",
                "operationname",
                "operationdate",
                "operationdepartmentname",
                "operationattorney",
                "operationlising",
                "holdertypeid",
                "holderpersondoccode",
                "holderpersondocnum",
                "holderpersondocdate",
                "holderpersondocissuer",
                "holderpersonlastname",
                "holderpersonfirstname",
                "holderpersonmiddlename",
                "holderpersonbirthdate",
                "holderpersonbirthregionid",
                "holderpersonsex",
                "holderpersonbirthplace",
                "holderpersoninn",
                "holderpersonsnils",
                "holderpersonogrnip",
                "holderaddressguid",
                "holderaddressregionid",
                "holderaddressregionname",
                "holderaddressdistrict",
                "holderaddressmundistrict",
                "holderaddresssettlement",
                "holderaddressstreet",
                "holderaddressbuilding",
                "holderaddressstructureid",
                "holderaddressstructurename",
                "holderaddressstructure"
            FROM
                "test__gibdd_db__vehicle_reg_and_res100_history"
            WHERE
                "sys_from" <= 332
                AND "sys_to" >= 332
            UNION
            ALL
            SELECT
                "vehicleguid",
                "reestrid",
                "reestrstatus",
                "vehicleregno",
                "vehiclevin",
                "vehiclevin2",
                "vehiclechassisnum",
                "vehiclereleaseyear",
                "operationregdoctypename",
                "operationregdoc",
                "operationregdocissuedate",
                "operationregdoccomments",
                "vehicleptstypename",
                "vehicleptsnum",
                "vehicleptsissuedate",
                "vehicleptsissuer",
                "vehicleptscomments",
                "vehiclebodycolor",
                "vehiclebrand",
                "vehiclemodel",
                "vehiclebrandmodel",
                "vehiclebodynum",
                "vehiclecost",
                "vehiclegasequip",
                "vehicleproducername",
                "vehiclegrossmass",
                "vehiclemass",
                "vehiclesteeringwheeltypeid",
                "vehiclekpptype",
                "vehicletransmissiontype",
                "vehicletypename",
                "vehiclecategory",
                "vehicletypeunit",
                "vehicleecoclass",
                "vehiclespecfuncname",
                "vehicleenclosedvolume",
                "vehicleenginemodel",
                "vehicleenginenum",
                "vehicleenginepower",
                "vehicleenginepowerkw",
                "vehicleenginetype",
                "holdrestrictiondate",
                "approvalnum",
                "approvaldate",
                "approvaltype",
                "utilizationfeename",
                "customsdoc",
                "customsdocdate",
                "customsdocissue",
                "customsdocrestriction",
                "customscountryremovalid",
                "customscountryremovalname",
                "ownerorgname",
                "ownerinn",
                "ownerogrn",
                "ownerkpp",
                "ownerpersonlastname",
                "ownerpersonfirstname",
                "ownerpersonmiddlename",
                "ownerpersonbirthdate",
                "ownerbirthplace",
                "ownerpersonogrnip",
                "owneraddressindex",
                "owneraddressmundistrict",
                "owneraddresssettlement",
                "owneraddressstreet",
                "ownerpersoninn",
                "ownerpersondoccode",
                "ownerpersondocnum",
                "ownerpersondocdate",
                "operationname",
                "operationdate",
                "operationdepartmentname",
                "operationattorney",
                "operationlising",
                "holdertypeid",
                "holderpersondoccode",
                "holderpersondocnum",
                "holderpersondocdate",
                "holderpersondocissuer",
                "holderpersonlastname",
                "holderpersonfirstname",
                "holderpersonmiddlename",
                "holderpersonbirthdate",
                "holderpersonbirthregionid",
                "holderpersonsex",
                "holderpersonbirthplace",
                "holderpersoninn",
                "holderpersonsnils",
                "holderpersonogrnip",
                "holderaddressguid",
                "holderaddressregionid",
                "holderaddressregionname",
                "holderaddressdistrict",
                "holderaddressmundistrict",
                "holderaddresssettlement",
                "holderaddressstreet",
                "holderaddressbuilding",
                "holderaddressstructureid",
                "holderaddressstructurename",
                "holderaddressstructure"
            FROM
                "test__gibdd_db__vehicle_reg_and_res100_actual"
            WHERE
                "sys_from" <= ?
        ) AS "t3"
    WHERE
        "reestrid" = ?"#;

    sql.into()
}

fn get_target_queries() -> Vec<&'static str> {
    vec![
        r#"
        SELECT *
FROM
    (SELECT "col1", "col2", "account_id", "amount"
    FROM "col1_col2_transactions_history"
    WHERE "sys_from" <= ?
            AND "sys_to" >= ?
    UNION ALL
    SELECT "col1", "col2", "account_id", "amount"
    FROM "col1_col2_transactions_actual"
    WHERE "sys_from" <= ?) AS "t3"
WHERE "col1" = ?
        AND ("col2" = ?
        AND "amount" > ?)
        "#,
        r#"
        SELECT *
FROM
    (SELECT "col1", "col2", "account_id", "amount"
    FROM "col1_col2_transactions_history"
    WHERE "sys_from" <= ?
            AND "sys_to" >= ?
    UNION ALL
    SELECT "col1", "col2", "account_id", "amount"
    FROM "col1_col2_transactions_actual"
    WHERE "sys_from" <= ?) AS "t3"
WHERE ("col1" = ?
        OR ("col1" = ?
        OR "col1" = ?))
        AND (("col2" = ?
        OR "col2" = ?)
        AND "amount" > ?)
        "#,
        r#"
        SELECT *
FROM
    (SELECT "col1", "col2", "account_id", "amount"
    FROM "col1_col2_transactions_history"
    WHERE "sys_from" <= 0 AND "sys_to" >= 0
    UNION ALL
    SELECT "col1", "col2", "account_id", "amount"
    FROM "col1_col2_transactions_actual"
    WHERE "sys_from" <= 0) AS "t3"
INNER JOIN
    (SELECT "id", "cola", "colb"
    FROM "cola_colb_accounts_history"
    WHERE "sys_from" <= 0 AND "sys_to" >= 0
    UNION ALL
    SELECT "id", "cola", "colb"
    FROM "cola_colb_accounts_actual"
    WHERE "sys_from" <= 0) AS "t8"
    ON "t3"."account_id" = "t8"."id"
WHERE "t3"."col1" = 1 AND "t3"."col2" = 2
AND ("t8"."cola" = 1 AND ("t8"."colb" = 2 AND "t3"."amount" > 0))
        "#,
        r#"
        SELECT *
FROM
    (SELECT "col1", "col2", "account_id", "amount"
    FROM "col1_col2_transactions_history"
    WHERE "sys_from" <= 0 AND "sys_to" >= 0
    UNION ALL
    SELECT "col1", "col2", "account_id", "amount"
    FROM "col1_col2_transactions_actual"
    WHERE "sys_from" <= 0) AS "t3"
INNER JOIN
    (SELECT "id", "cola", "colb"
    FROM "cola_accounts_history"
    WHERE "sys_from" <= 0 AND "sys_to" >= 0
    UNION ALL
    SELECT "id", "cola", "colb"
    FROM "cola_accounts_actual"
    WHERE "sys_from" <= 0) AS "t8"
    ON "t3"."account_id" = "t8"."id"
WHERE "t3"."col1" = 1 AND ("t3"."col2" = 1 AND "t8"."colb" = 2)
        "#,
        r#"
        SELECT *
FROM
    (SELECT "col1", "col2", "account_id", "amount"
    FROM "col1_col2_transactions_history"
    WHERE "sys_from" <= 0 AND "sys_to" >= 0
    UNION ALL
    SELECT "col1", "col2", "account_id", "amount"
    FROM "col1_col2_transactions_actual"
    WHERE "sys_from" <= 0) AS "t3"
WHERE "account_id" IN
    (SELECT "id"
    FROM
        (SELECT "id", "cola", "colb"
        FROM "cola_colb_accounts_history"
        WHERE "sys_from" <= 0 AND "sys_to" >= 0
        UNION ALL
        SELECT "id", "cola", "colb"
        FROM "cola_colb_accounts_actual"
        WHERE "sys_from" <= 0) AS "t8"
        WHERE "cola" = 1 AND "colb" = 2)
    AND ("col1" = ? AND "col2" = 2)
        "#,
    ]
}

fn parse_ast(pattern: &str) {
    AbstractSyntaxTree::new(pattern).unwrap();
}

fn bench_ast_build(c: &mut Criterion) {
    c.bench_function("parsing_ast", |b| {
        let sql_str = query_sql();
        b.iter(|| parse_ast(&sql_str));
    });
}

fn build_ir(pattern: &str, params: Vec<Value>, engine: &mut RouterRuntimeMock) {
    let mut query = Query::new(engine, pattern, params).unwrap();
    let top_id = query.get_exec_plan().get_ir_plan().get_top().unwrap();
    let buckets = query.bucket_discovery(top_id).unwrap();
    let plan = query.get_exec_plan();
    let sp = SyntaxPlan::new(plan, top_id, Snapshot::Oldest).unwrap();
    let ordered = OrderedSyntaxNodes::try_from(sp).unwrap();
    let nodes = ordered.to_syntax_data().unwrap();
    plan.to_sql(&nodes, &buckets, "").unwrap();
}

fn bench_ir_build(c: &mut Criterion) {
    let mut engine = RouterRuntimeMock::new();
    let sql = query_sql();
    let mut sys_from: u64 = 42;
    let mut reestrid: u64 = 666;
    c.bench_function("building_ir", |b| {
        b.iter(|| {
            let params = vec![Value::from(sys_from), Value::from(reestrid)];
            sys_from += 1;
            reestrid += 1;
            build_ir(&sql, params, &mut engine);
        });
    });
}

fn bench_target_queries(c: &mut Criterion) {
    let queries = get_target_queries();

    for (index, query) in queries.iter().enumerate() {
        let bench_name = format!("parsing_target_query{index}");
        c.bench_function(bench_name.as_str(), |b| {
            b.iter(|| parse_ast(query));
        });
    }
}

fn bench_pure_pest_parsing(c: &mut Criterion) {
    let queries = get_target_queries();

    for (index, query) in queries.iter().enumerate() {
        let bench_name = format!("pure_pest_parsing_target_query{index}");
        c.bench_function(bench_name.as_str(), |b| {
            b.iter(|| black_box(ParseTree::parse(Rule::Command, query)));
        });
    }
}

criterion_group!(
    benches,
    bench_ir_build,
    bench_ast_build,
    bench_target_queries,
    bench_pure_pest_parsing
);
criterion_main!(benches);
