use pretty_assertions::assert_eq;

use crate::backend::sql::tree::{OrderedSyntaxNodes, SyntaxPlan};
use crate::executor::engine::mock::RouterRuntimeMock;
use crate::ir::relation::Type;
use crate::ir::tests::{column_integer_user_non_null, column_user_non_null};
use crate::ir::transformation::redistribution::MotionPolicy;
use crate::ir::tree::Snapshot;

use super::*;

/// Helper function to generate sql from `exec_plan` from given `top_id` node.
/// Used for testing.
fn get_sql_from_execution_plan(
    exec_plan: &mut ExecutionPlan,
    top_id: usize,
    snapshot: Snapshot,
    buckets: &Buckets,
    name_base: &str,
) -> PatternWithParams {
    let subplan = exec_plan.take_subtree(top_id).unwrap();
    let subplan_top_id = subplan.get_ir_plan().get_top().unwrap();
    let sp = SyntaxPlan::new(&subplan, subplan_top_id, snapshot).unwrap();
    let ordered = OrderedSyntaxNodes::try_from(sp).unwrap();
    let nodes = ordered.to_syntax_data().unwrap();
    let (sql, _) = subplan.to_sql(&nodes, buckets, name_base).unwrap();
    sql
}

#[test]
fn exec_plan_subtree_test() {
    let sql = r#"SELECT "FIRST_NAME" FROM "test_space" where "id" in
    (SELECT "identification_number" FROM "hash_testing" where "identification_number" > 1)"#;
    let coordinator = RouterRuntimeMock::new();

    let mut query = Query::new(&coordinator, sql, vec![]).unwrap();
    let motion_id = *query
        .exec_plan
        .get_ir_plan()
        .clone_slices()
        .slice(0)
        .unwrap()
        .position(0)
        .unwrap();
    let mut virtual_table = virtual_table_23(None);
    if let MotionPolicy::Segment(key) = get_motion_policy(query.exec_plan.get_ir_plan(), motion_id)
    {
        virtual_table.reshard(key, &query.coordinator).unwrap();
    }
    let mut vtables: HashMap<usize, Rc<VirtualTable>> = HashMap::new();
    vtables.insert(motion_id, Rc::new(virtual_table));

    let exec_plan = query.get_mut_exec_plan();
    exec_plan.set_vtables(vtables);
    let top_id = exec_plan.get_ir_plan().get_top().unwrap();
    let motion_child_id = exec_plan.get_motion_subtree_root(motion_id).unwrap();

    // Check sub-query
    let sql = get_sql_from_execution_plan(
        exec_plan,
        motion_child_id,
        Snapshot::Oldest,
        &Buckets::All,
        "test",
    );
    assert_eq!(
        sql,
        PatternWithParams::new(
            r#"SELECT "hash_testing"."identification_number" FROM "hash_testing" WHERE ("hash_testing"."identification_number") > (?)"#.to_string(),
            vec![Value::from(1_u64)]
        ));

    // Check main query
    let sql =
        get_sql_from_execution_plan(exec_plan, top_id, Snapshot::Oldest, &Buckets::All, "test");
    assert_eq!(
        sql,
        PatternWithParams::new(
            r#"SELECT "test_space"."FIRST_NAME" FROM "test_space" WHERE ("test_space"."id") in (SELECT "identification_number" FROM "TMP_test_20")"#.to_string(),
            vec![]
        ));
}

#[test]
fn exec_plan_subtree_two_stage_groupby_test() {
    let sql = r#"SELECT t1."FIRST_NAME" FROM "test_space" as t1 group by t1."FIRST_NAME""#;
    let coordinator = RouterRuntimeMock::new();

    let mut query = Query::new(&coordinator, sql, vec![]).unwrap();
    let motion_id = *query
        .exec_plan
        .get_ir_plan()
        .clone_slices()
        .slice(0)
        .unwrap()
        .position(0)
        .unwrap();

    let mut virtual_table = VirtualTable::new();
    virtual_table.add_column(column_user_non_null(
        String::from("FIRST_NAME"),
        Type::String,
    ));

    if let MotionPolicy::Segment(key) = get_motion_policy(query.exec_plan.get_ir_plan(), motion_id)
    {
        virtual_table.reshard(key, &query.coordinator).unwrap();
    }

    let mut vtables: HashMap<usize, Rc<VirtualTable>> = HashMap::new();
    vtables.insert(motion_id, Rc::new(virtual_table));

    let exec_plan = query.get_mut_exec_plan();
    exec_plan.set_vtables(vtables);
    let top_id = exec_plan.get_ir_plan().get_top().unwrap();
    let motion_child_id = exec_plan.get_motion_subtree_root(motion_id).unwrap();
    if let MotionPolicy::Segment(_) = exec_plan.get_motion_policy(motion_id).unwrap() {
    } else {
        panic!("Expected MotionPolicy::Segment for local aggregation stage");
    };

    // Check groupby local stage
    let sql = get_sql_from_execution_plan(
        exec_plan,
        motion_child_id,
        Snapshot::Oldest,
        &Buckets::All,
        "test",
    );
    assert_eq!(
        sql,
        PatternWithParams::new(
            r#"SELECT "T1"."FIRST_NAME" as "column_12" FROM "test_space" as "T1" GROUP BY "T1"."FIRST_NAME""#
                .to_string(),
            vec![]
        )
    );

    // Check main query
    let sql =
        get_sql_from_execution_plan(exec_plan, top_id, Snapshot::Oldest, &Buckets::All, "test");
    assert_eq!(
        sql,
        PatternWithParams::new(
            r#"SELECT "column_12" as "FIRST_NAME" FROM (SELECT "FIRST_NAME" FROM "TMP_test_6") GROUP BY "column_12""#.to_string(),
            vec![]
        ));
}

#[test]
fn exec_plan_subtree_two_stage_groupby_test_2() {
    let sql = r#"SELECT t1."FIRST_NAME", t1."sys_op", t1."sysFrom" FROM "test_space" as t1 GROUP BY t1."FIRST_NAME", t1."sys_op", t1."sysFrom""#;
    let coordinator = RouterRuntimeMock::new();

    let mut query = Query::new(&coordinator, sql, vec![]).unwrap();
    let motion_id = *query
        .exec_plan
        .get_ir_plan()
        .clone_slices()
        .slice(0)
        .unwrap()
        .position(0)
        .unwrap();
    let mut virtual_table = VirtualTable::new();
    virtual_table.add_column(column_user_non_null(
        String::from("column_12"),
        Type::String,
    ));
    virtual_table.add_column(column_user_non_null(
        String::from("column_13"),
        Type::Integer,
    ));
    virtual_table.add_column(column_user_non_null(
        String::from("column_14"),
        Type::Integer,
    ));
    if let MotionPolicy::Segment(key) = get_motion_policy(query.exec_plan.get_ir_plan(), motion_id)
    {
        virtual_table.reshard(key, &query.coordinator).unwrap();
    }

    let mut vtables: HashMap<usize, Rc<VirtualTable>> = HashMap::new();
    vtables.insert(motion_id, Rc::new(virtual_table));

    let exec_plan = query.get_mut_exec_plan();
    exec_plan.set_vtables(vtables);
    let top_id = exec_plan.get_ir_plan().get_top().unwrap();
    let motion_child_id = exec_plan.get_motion_subtree_root(motion_id).unwrap();

    // Check groupby local stage
    let sql = get_sql_from_execution_plan(
        exec_plan,
        motion_child_id,
        Snapshot::Oldest,
        &Buckets::All,
        "test",
    );
    if let MotionPolicy::Segment(_) = exec_plan.get_motion_policy(motion_id).unwrap() {
    } else {
        panic!("Expected MotionPolicy::Segment for local aggregation stage");
    };
    assert_eq!(
        sql,
        PatternWithParams::new(
            format!(
                "{} {} {}",
                r#"SELECT "T1"."FIRST_NAME" as "column_12", "T1"."sysFrom" as "column_14","#,
                r#""T1"."sys_op" as "column_13" FROM "test_space" as "T1""#,
                r#"GROUP BY "T1"."sys_op", "T1"."FIRST_NAME", "T1"."sysFrom""#,
            ),
            vec![]
        )
    );

    // Check main query
    let sql =
        get_sql_from_execution_plan(exec_plan, top_id, Snapshot::Oldest, &Buckets::All, "test");
    assert_eq!(
        sql,
        PatternWithParams::new(
            format!(
                "{} {} {} {}",
                r#"SELECT "column_12" as "FIRST_NAME", "column_13" as "sys_op","#,
                r#""column_14" as "sysFrom" FROM"#,
                r#"(SELECT "column_12","column_13","column_14" FROM "TMP_test_14")"#,
                r#"GROUP BY "column_13", "column_12", "column_14""#,
            ),
            vec![]
        )
    );
}

#[test]
fn exec_plan_subtree_aggregates() {
    let sql = format!(
        "{} {} {} {}",
        r#"SELECT t1."sys_op" || t1."sys_op", t1."sys_op"*2 + count(t1."sysFrom"),"#,
        r#"sum(t1."id"), sum(distinct t1."id"*t1."sys_op") / count(distinct "id"),"#,
        r#"group_concat(t1."id", 'o'), avg(t1."id"), total(t1."id"), min(t1."id"), max(t1."id")"#,
        r#"FROM "test_space" as t1 group by t1."sys_op""#
    );
    let coordinator = RouterRuntimeMock::new();

    let mut query = Query::new(&coordinator, sql.as_str(), vec![]).unwrap();
    let motion_id = *query
        .exec_plan
        .get_ir_plan()
        .clone_slices()
        .slice(0)
        .unwrap()
        .position(0)
        .unwrap();
    let mut virtual_table = VirtualTable::new();
    virtual_table.add_column(column_integer_user_non_null(String::from("sys_op")));
    virtual_table.add_column(column_integer_user_non_null(String::from("sum_42")));
    virtual_table.add_column(column_integer_user_non_null(String::from("count_37")));
    virtual_table.add_column(column_integer_user_non_null(String::from("sum_49")));
    virtual_table.add_column(column_integer_user_non_null(String::from("count_51")));
    virtual_table.add_column(column_user_non_null(
        String::from("group_concat_58"),
        Type::String,
    ));
    virtual_table.add_column(column_integer_user_non_null(String::from("count_61")));
    virtual_table.add_column(column_integer_user_non_null(String::from("total_64")));
    virtual_table.add_column(column_integer_user_non_null(String::from("min_67")));
    virtual_table.add_column(column_integer_user_non_null(String::from("max_70")));
    if let MotionPolicy::Segment(key) = get_motion_policy(query.exec_plan.get_ir_plan(), motion_id)
    {
        virtual_table.reshard(key, &query.coordinator).unwrap();
    }

    let mut vtables: HashMap<usize, Rc<VirtualTable>> = HashMap::new();
    vtables.insert(motion_id, Rc::new(virtual_table));

    let exec_plan = query.get_mut_exec_plan();
    exec_plan.set_vtables(vtables);
    let top_id = exec_plan.get_ir_plan().get_top().unwrap();
    let motion_child_id = exec_plan.get_motion_subtree_root(motion_id).unwrap();

    // Check groupby local stage
    let sql = get_sql_from_execution_plan(
        exec_plan,
        motion_child_id,
        Snapshot::Oldest,
        &Buckets::All,
        "test",
    );
    if let MotionPolicy::Segment(_) = exec_plan.get_motion_policy(motion_id).unwrap() {
    } else {
        panic!("Expected MotionPolicy::Segment for local aggregation stage");
    };
    assert_eq!(
        sql,
        PatternWithParams::new(
            format!(
                "{} {} {} {} {} {}",
                r#"SELECT "T1"."sys_op" as "column_12", ("T1"."id") * ("T1"."sys_op") as "column_48","#,
                r#""T1"."id" as "column_50", group_concat ("T1"."id", ?) as "group_concat_58","#,
                r#"sum ("T1"."id") as "sum_42", count ("T1"."id") as "count_61", total ("T1"."id") as "total_64","#,
                r#"min ("T1"."id") as "min_67", max ("T1"."id") as "max_70", count ("T1"."sysFrom") as "count_37""#,
                r#"FROM "test_space" as "T1""#,
                r#"GROUP BY "T1"."sys_op", ("T1"."id") * ("T1"."sys_op"), "T1"."id""#,
            ),
            vec![Value::from("o")]
        )
    );

    // Check main query
    let sql =
        get_sql_from_execution_plan(exec_plan, top_id, Snapshot::Oldest, &Buckets::All, "test");
    assert_eq!(
        sql,
        PatternWithParams::new(
            format!(
                "{} {} {} {} {} {} {} {}",
                r#"SELECT ("column_12") || ("column_12") as "COL_1","#,
                r#"("column_12") * (?) + (sum ("count_37")) as "COL_2", sum ("sum_42") as "COL_3","#,
                r#"(sum (DISTINCT "column_48")) / (count (DISTINCT "column_50")) as "COL_4","#,
                r#"group_concat ("group_concat_58", ?) as "COL_5","#,
                r#"(sum (CAST ("sum_42" as double)) / sum (CAST ("count_61" as double))) as "COL_6","#,
                r#"total ("total_64") as "COL_7", min ("min_67") as "COL_8", max ("max_70") as "COL_9""#,
                r#"FROM (SELECT "sys_op","sum_42","count_37","sum_49","count_51","group_concat_58","count_61","total_64","min_67","max_70" FROM "TMP_test_70")"#,
                r#"GROUP BY "column_12""#
            ),
            vec![Value::Unsigned(2), Value::from("o")]
        )
    );
}

#[test]
fn exec_plan_subtree_aggregates_no_groupby() {
    let sql = r#"SELECT count(t1."sysFrom"), sum(distinct t1."id" + t1."sysFrom") FROM "test_space" as t1"#;
    let coordinator = RouterRuntimeMock::new();

    let mut query = Query::new(&coordinator, sql, vec![]).unwrap();
    let motion_id = *query
        .exec_plan
        .get_ir_plan()
        .clone_slices()
        .slice(0)
        .unwrap()
        .position(0)
        .unwrap();
    let mut virtual_table = VirtualTable::new();
    virtual_table.add_column(column_integer_user_non_null(String::from("column_19")));
    virtual_table.add_column(column_integer_user_non_null(String::from("count_13")));
    if let MotionPolicy::Segment(key) = get_motion_policy(query.exec_plan.get_ir_plan(), motion_id)
    {
        virtual_table.reshard(key, &query.coordinator).unwrap();
    }

    let mut vtables: HashMap<usize, Rc<VirtualTable>> = HashMap::new();
    vtables.insert(motion_id, Rc::new(virtual_table));

    let exec_plan = query.get_mut_exec_plan();
    exec_plan.set_vtables(vtables);
    let top_id = exec_plan.get_ir_plan().get_top().unwrap();
    let motion_child_id = exec_plan.get_motion_subtree_root(motion_id).unwrap();

    // Check groupby local stage
    let sql = get_sql_from_execution_plan(
        exec_plan,
        motion_child_id,
        Snapshot::Oldest,
        &Buckets::All,
        "test",
    );
    if let MotionPolicy::Full = exec_plan.get_motion_policy(motion_id).unwrap() {
    } else {
        panic!("Expected MotionPolicy::Full for local aggregation stage");
    };
    assert_eq!(
        sql,
        PatternWithParams::new(
            r#"SELECT ("T1"."id") + ("T1"."sysFrom") as "column_19", count ("T1"."sysFrom") as "count_13" FROM "test_space" as "T1" GROUP BY ("T1"."id") + ("T1"."sysFrom")"#.to_string(),
            vec![]
        ));

    // Check main query
    let sql =
        get_sql_from_execution_plan(exec_plan, top_id, Snapshot::Oldest, &Buckets::All, "test");
    assert_eq!(
        sql,
        PatternWithParams::new(
            r#"SELECT sum ("count_13") as "COL_1", sum (DISTINCT "column_19") as "COL_2" FROM (SELECT "column_19","count_13" FROM "TMP_test_12")"#.to_string(),
            vec![]
        ));
}

#[test]
fn exec_plan_subquery_under_motion_without_alias() {
    let sql = r#"
    SELECT * FROM
            (SELECT "id" as "tid" FROM "test_space")
    INNER JOIN
            (SELECT "identification_number" as "sid" FROM "hash_testing")
    ON true
    "#;
    let coordinator = RouterRuntimeMock::new();

    let mut query = Query::new(&coordinator, sql, vec![]).unwrap();
    let motion_id = *query
        .exec_plan
        .get_ir_plan()
        .clone_slices()
        .slice(0)
        .unwrap()
        .position(0)
        .unwrap();
    let mut virtual_table = virtual_table_23(None);
    if let MotionPolicy::Segment(key) = get_motion_policy(query.exec_plan.get_ir_plan(), motion_id)
    {
        virtual_table.reshard(key, &query.coordinator).unwrap();
    }
    let mut vtables: HashMap<usize, Rc<VirtualTable>> = HashMap::new();
    vtables.insert(motion_id, Rc::new(virtual_table));

    let exec_plan = query.get_mut_exec_plan();
    exec_plan.set_vtables(vtables);
    let top_id = exec_plan.get_ir_plan().get_top().unwrap();

    let sql =
        get_sql_from_execution_plan(exec_plan, top_id, Snapshot::Oldest, &Buckets::All, "test");
    assert_eq!(
        sql,
        PatternWithParams::new(
            r#"SELECT "tid", "sid" FROM (SELECT "test_space"."id" as "tid" FROM "test_space") INNER JOIN (SELECT "identification_number" FROM "TMP_test_28") ON ?"#.to_string(),
            vec![Value::Boolean(true)]
        ));
}

#[test]
fn exec_plan_subquery_under_motion_with_alias() {
    let sql = r#"
    SELECT * FROM
            (SELECT "id" as "tid" FROM "test_space")
    INNER JOIN
            (SELECT "identification_number" as "sid" FROM "hash_testing") AS "hti"
    ON true
    "#;
    let coordinator = RouterRuntimeMock::new();

    let mut query = Query::new(&coordinator, sql, vec![]).unwrap();
    let motion_id = *query
        .exec_plan
        .get_ir_plan()
        .clone_slices()
        .slice(0)
        .unwrap()
        .position(0)
        .unwrap();
    let mut virtual_table = virtual_table_23(Some("\"hti\""));
    if let MotionPolicy::Segment(key) = get_motion_policy(query.exec_plan.get_ir_plan(), motion_id)
    {
        virtual_table.reshard(key, &query.coordinator).unwrap();
    }
    let mut vtables: HashMap<usize, Rc<VirtualTable>> = HashMap::new();
    vtables.insert(motion_id, Rc::new(virtual_table));

    let exec_plan = query.get_mut_exec_plan();
    exec_plan.set_vtables(vtables);
    let top_id = exec_plan.get_ir_plan().get_top().unwrap();

    let sql =
        get_sql_from_execution_plan(exec_plan, top_id, Snapshot::Oldest, &Buckets::All, "test");
    assert_eq!(
        sql,
        PatternWithParams::new(
            r#"SELECT "tid", "hti"."sid" FROM (SELECT "test_space"."id" as "tid" FROM "test_space") INNER JOIN (SELECT "identification_number" FROM "TMP_test_28") as "hti" ON ?"#.to_string(),
            vec![Value::Boolean(true)]
        ));
}

#[test]
fn exec_plan_motion_under_in_operator() {
    let sql = r#"SELECT "id" FROM "test_space" WHERE "id" in (SELECT "identification_number" FROM "hash_testing")"#;
    let coordinator = RouterRuntimeMock::new();

    let mut query = Query::new(&coordinator, sql, vec![]).unwrap();
    let motion_id = *query
        .exec_plan
        .get_ir_plan()
        .clone_slices()
        .slice(0)
        .unwrap()
        .position(0)
        .unwrap();
    let mut virtual_table = virtual_table_23(None);
    if let MotionPolicy::Segment(key) = get_motion_policy(query.exec_plan.get_ir_plan(), motion_id)
    {
        virtual_table.reshard(key, &query.coordinator).unwrap();
    }
    let mut vtables: HashMap<usize, Rc<VirtualTable>> = HashMap::new();
    vtables.insert(motion_id, Rc::new(virtual_table));

    let exec_plan = query.get_mut_exec_plan();
    exec_plan.set_vtables(vtables);
    let top_id = exec_plan.get_ir_plan().get_top().unwrap();

    let sql =
        get_sql_from_execution_plan(exec_plan, top_id, Snapshot::Oldest, &Buckets::All, "test");
    assert_eq!(
        sql,
        PatternWithParams::new(
            r#"SELECT "test_space"."id" FROM "test_space" WHERE ("test_space"."id") in (SELECT "identification_number" FROM "TMP_test_20")"#.to_string(),
            vec![]
        ));
}

#[test]
fn exec_plan_motion_under_except() {
    let sql = r#"
    SELECT "id" FROM "test_space"
    EXCEPT
    SELECT "identification_number" FROM "hash_testing"
    "#;
    let coordinator = RouterRuntimeMock::new();

    let mut query = Query::new(&coordinator, sql, vec![]).unwrap();
    let motion_id = *query
        .exec_plan
        .get_ir_plan()
        .clone_slices()
        .slice(0)
        .unwrap()
        .position(0)
        .unwrap();
    let mut virtual_table = virtual_table_23(None);
    if let MotionPolicy::Segment(key) = get_motion_policy(query.exec_plan.get_ir_plan(), motion_id)
    {
        virtual_table.reshard(key, &query.coordinator).unwrap();
    }
    let mut vtables: HashMap<usize, Rc<VirtualTable>> = HashMap::new();
    vtables.insert(motion_id, Rc::new(virtual_table));

    let exec_plan = query.get_mut_exec_plan();
    exec_plan.set_vtables(vtables);
    let top_id = exec_plan.get_ir_plan().get_top().unwrap();

    let sql =
        get_sql_from_execution_plan(exec_plan, top_id, Snapshot::Oldest, &Buckets::All, "test");
    assert_eq!(
        sql,
        PatternWithParams::new(
            r#"SELECT "test_space"."id" FROM "test_space" EXCEPT SELECT "identification_number" FROM "TMP_test_19""#.to_string(),
            vec![]
        ));
}

#[test]
fn exec_plan_subtree_count_asterisk() {
    let sql = r#"SELECT count(*) FROM "test_space""#;
    let coordinator = RouterRuntimeMock::new();

    let mut query = Query::new(&coordinator, sql, vec![]).unwrap();
    let motion_id = *query
        .exec_plan
        .get_ir_plan()
        .clone_slices()
        .slice(0)
        .unwrap()
        .position(0)
        .unwrap();
    let mut virtual_table = VirtualTable::new();
    virtual_table.add_column(column_integer_user_non_null(String::from("count_13")));
    if let MotionPolicy::Segment(key) = get_motion_policy(query.exec_plan.get_ir_plan(), motion_id)
    {
        virtual_table.reshard(key, &query.coordinator).unwrap();
    }

    let mut vtables: HashMap<usize, Rc<VirtualTable>> = HashMap::new();
    vtables.insert(motion_id, Rc::new(virtual_table));

    let exec_plan = query.get_mut_exec_plan();
    exec_plan.set_vtables(vtables);
    let top_id = exec_plan.get_ir_plan().get_top().unwrap();
    let motion_child_id = exec_plan.get_motion_subtree_root(motion_id).unwrap();

    // Check groupby local stage
    let sql = get_sql_from_execution_plan(
        exec_plan,
        motion_child_id,
        Snapshot::Oldest,
        &Buckets::All,
        "test",
    );
    if let MotionPolicy::Full = exec_plan.get_motion_policy(motion_id).unwrap() {
    } else {
        panic!("Expected MotionPolicy::Full for local aggregation stage");
    };

    assert_eq!(
        sql,
        PatternWithParams::new(
            r#"SELECT count (*) as "count_13" FROM "test_space""#.to_string(),
            vec![]
        )
    );

    // Check main query
    let sql =
        get_sql_from_execution_plan(exec_plan, top_id, Snapshot::Oldest, &Buckets::All, "test");
    assert_eq!(
        sql,
        PatternWithParams::new(
            r#"SELECT sum ("count_13") as "COL_1" FROM (SELECT "count_13" FROM "TMP_test_7")"#
                .to_string(),
            vec![]
        )
    );
}

#[test]
fn exec_plan_subtree_having() {
    let sql = format!(
        "{} {} {}",
        r#"SELECT t1."sys_op" || t1."sys_op", count(t1."sys_op"*2) + count(distinct t1."sys_op"*2)"#,
        r#"FROM "test_space" as t1 group by t1."sys_op""#,
        r#"HAVING sum(distinct t1."sys_op"*2) > 1"#
    );
    let coordinator = RouterRuntimeMock::new();

    let mut query = Query::new(&coordinator, sql.as_str(), vec![]).unwrap();
    let motion_id = *query
        .exec_plan
        .get_ir_plan()
        .clone_slices()
        .slice(0)
        .unwrap()
        .position(0)
        .unwrap();
    let mut virtual_table = VirtualTable::new();
    virtual_table.add_column(column_integer_user_non_null(String::from("column_63")));
    virtual_table.add_column(column_integer_user_non_null(String::from("column_12")));
    virtual_table.add_column(column_integer_user_non_null(String::from("count_58")));
    if let MotionPolicy::Segment(key) = get_motion_policy(query.exec_plan.get_ir_plan(), motion_id)
    {
        virtual_table.reshard(key, &query.coordinator).unwrap();
    }

    let mut vtables: HashMap<usize, Rc<VirtualTable>> = HashMap::new();
    vtables.insert(motion_id, Rc::new(virtual_table));

    let exec_plan = query.get_mut_exec_plan();
    exec_plan.set_vtables(vtables);
    let top_id = exec_plan.get_ir_plan().get_top().unwrap();
    let motion_child_id = exec_plan.get_motion_subtree_root(motion_id).unwrap();

    // Check groupby local stage
    let subplan1 = exec_plan.take_subtree(motion_child_id).unwrap();
    let subplan1_top_id = subplan1.get_ir_plan().get_top().unwrap();
    let sp = SyntaxPlan::new(&subplan1, subplan1_top_id, Snapshot::Oldest).unwrap();
    let ordered = OrderedSyntaxNodes::try_from(sp).unwrap();
    let nodes = ordered.to_syntax_data().unwrap();
    let (sql, _) = subplan1.to_sql(&nodes, &Buckets::All, "test").unwrap();
    if let MotionPolicy::Segment(_) = exec_plan.get_motion_policy(motion_id).unwrap() {
    } else {
        panic!("Expected MotionPolicy::Segment for local aggregation stage");
    };
    assert_eq!(
        sql,
        PatternWithParams::new(
            format!(
                "{} {} {}",
                r#"SELECT "T1"."sys_op" as "column_12", ("T1"."sys_op") * (?) as "column_63","#,
                r#"count (("T1"."sys_op") * (?)) as "count_58" FROM "test_space" as "T1""#,
                r#"GROUP BY "T1"."sys_op", ("T1"."sys_op") * (?)"#,
            ),
            vec![Value::Unsigned(2), Value::Unsigned(2), Value::Unsigned(2)]
        )
    );

    // Check main query
    let subplan2 = exec_plan.take_subtree(top_id).unwrap();
    let subplan2_top_id = subplan2.get_ir_plan().get_top().unwrap();
    let sp = SyntaxPlan::new(&subplan2, subplan2_top_id, Snapshot::Oldest).unwrap();
    let ordered = OrderedSyntaxNodes::try_from(sp).unwrap();
    let nodes = ordered.to_syntax_data().unwrap();
    let (sql, _) = subplan2.to_sql(&nodes, &Buckets::All, "test").unwrap();
    assert_eq!(
        sql,
        PatternWithParams::new(
            format!(
                "{} {} {} {}",
                r#"SELECT ("column_12") || ("column_12") as "COL_1","#,
                r#"(sum ("count_58")) + (count (DISTINCT "column_63")) as "COL_2" FROM"#,
                r#"(SELECT "column_63","column_12","count_58" FROM "TMP_test_22")"#,
                r#"GROUP BY "column_12" HAVING (sum (DISTINCT "column_63")) > (?)"#
            ),
            vec![Value::Unsigned(1u64)]
        )
    );
}

#[test]
fn exec_plan_subtree_having_without_groupby() {
    let sql = format!(
        "{} {} {}",
        r#"SELECT count(t1."sys_op"*2) + count(distinct t1."sys_op"*2)"#,
        r#"FROM "test_space" as t1"#,
        r#"HAVING sum(distinct t1."sys_op"*2) > 1"#
    );
    let coordinator = RouterRuntimeMock::new();

    let mut query = Query::new(&coordinator, sql.as_str(), vec![]).unwrap();
    let motion_id = *query
        .exec_plan
        .get_ir_plan()
        .clone_slices()
        .slice(0)
        .unwrap()
        .position(0)
        .unwrap();
    let mut virtual_table = VirtualTable::new();
    virtual_table.add_column(column_integer_user_non_null(String::from("column_63")));
    virtual_table.add_column(column_integer_user_non_null(String::from("column_12")));
    virtual_table.add_column(column_integer_user_non_null(String::from("count_58")));
    if let MotionPolicy::Segment(key) = get_motion_policy(query.exec_plan.get_ir_plan(), motion_id)
    {
        virtual_table.reshard(key, &query.coordinator).unwrap();
    }

    let mut vtables: HashMap<usize, Rc<VirtualTable>> = HashMap::new();
    vtables.insert(motion_id, Rc::new(virtual_table));

    let exec_plan = query.get_mut_exec_plan();
    exec_plan.set_vtables(vtables);
    let top_id = exec_plan.get_ir_plan().get_top().unwrap();
    let motion_child_id = exec_plan.get_motion_subtree_root(motion_id).unwrap();

    // Check groupby local stage
    let subplan1 = exec_plan.take_subtree(motion_child_id).unwrap();
    let subplan1_top_id = subplan1.get_ir_plan().get_top().unwrap();
    let sp = SyntaxPlan::new(&subplan1, subplan1_top_id, Snapshot::Oldest).unwrap();
    let ordered = OrderedSyntaxNodes::try_from(sp).unwrap();
    let nodes = ordered.to_syntax_data().unwrap();
    let (sql, _) = subplan1.to_sql(&nodes, &Buckets::All, "test").unwrap();
    if let MotionPolicy::Full = exec_plan.get_motion_policy(motion_id).unwrap() {
    } else {
        panic!("Expected MotionPolicy::Full after local stage");
    };

    assert_eq!(
        sql,
        PatternWithParams::new(
            format!(
                "{} {} {}",
                r#"SELECT ("T1"."sys_op") * (?) as "column_44","#,
                r#"count (("T1"."sys_op") * (?)) as "count_39" FROM "test_space" as "T1""#,
                r#"GROUP BY ("T1"."sys_op") * (?)"#,
            ),
            vec![Value::Unsigned(2), Value::Unsigned(2), Value::Unsigned(2)]
        )
    );

    // Check main query
    let subplan2 = exec_plan.take_subtree(top_id).unwrap();
    let subplan2_top_id = subplan2.get_ir_plan().get_top().unwrap();
    let sp = SyntaxPlan::new(&subplan2, subplan2_top_id, Snapshot::Oldest).unwrap();
    let ordered = OrderedSyntaxNodes::try_from(sp).unwrap();
    let nodes = ordered.to_syntax_data().unwrap();
    let (sql, _) = subplan2.to_sql(&nodes, &Buckets::All, "test").unwrap();
    assert_eq!(
        sql,
        PatternWithParams::new(
            format!(
                "{} {} {}",
                r#"SELECT (sum ("count_39")) + (count (DISTINCT "column_44")) as "COL_1""#,
                r#"FROM (SELECT "column_63","column_12","count_58" FROM "TMP_test_14")"#,
                r#"HAVING (sum (DISTINCT "column_44")) > (?)"#,
            ),
            vec![Value::Unsigned(1u64)]
        )
    );
}
