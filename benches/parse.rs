extern crate sbroad;

use criterion::{criterion_group, criterion_main, Criterion};
use engine::EngineMock;
use sbroad::executor::Query;
use sbroad::ir::value::Value;

fn query1_sql() -> String {
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

fn query1(pattern: &str, params: &[Value], engine: &mut EngineMock) {
    let mut query = Query::new(engine, pattern, params).unwrap();
    let top_id = query.get_exec_plan().get_ir_plan().get_top().unwrap();
    let buckets = query.bucket_discovery(top_id).unwrap();
    let plan = query.get_exec_plan();
    let nodes = plan.get_sql_order(top_id).unwrap();
    plan.subtree_as_sql(&nodes, &buckets).unwrap();
}

fn bench_query1(c: &mut Criterion) {
    let mut engine = EngineMock::new();
    let sql = query1_sql();
    let mut sys_from: u64 = 42;
    let mut reestrid: u64 = 666;
    c.bench_function("query1", |b| {
        b.iter(|| {
            let params = vec![
                Value::number_from_str(&sys_from.to_string()).unwrap(),
                Value::number_from_str(&reestrid.to_string()).unwrap(),
            ];
            sys_from += 1;
            reestrid += 1;
            query1(&sql, &params, &mut engine)
        })
    });
}

criterion_group!(benches, bench_query1);
criterion_main!(benches);

mod engine;
