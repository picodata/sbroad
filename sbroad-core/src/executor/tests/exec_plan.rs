use std::rc::Rc;

use engine::mock::TEMPLATE;
use itertools::Itertools;
use pretty_assertions::assert_eq;
use smol_str::SmolStr;

use crate::backend::sql::tree::{OrderedSyntaxNodes, SyntaxPlan};
use crate::collection;
use crate::executor::engine::mock::{ReplicasetDispatchInfo, RouterRuntimeMock, VshardMock};
use crate::ir::node::{ArenaType, Node136};
use crate::ir::relation::Type;
use crate::ir::tests::{column_integer_user_non_null, column_user_non_null};
use crate::ir::transformation::redistribution::MotionPolicy;
use crate::ir::tree::Snapshot;
use crate::ir::Slice;

use super::*;

// Helper function to format back sql.
// The local sql we produce doesn't contain line breaks,
// but in code it's hard to read such long string, so
// we insert line breaks and remove them back for
// string comparison with expected pattern.
fn f_sql(s: &str) -> String {
    s.replace("\n", " ")
}

/// Helper function to generate sql from `exec_plan` from given `top_id` node.
/// Used for testing.
fn get_sql_from_execution_plan(
    exec_plan: &mut ExecutionPlan,
    top_id: NodeId,
    snapshot: Snapshot,
    name_base: &str,
) -> PatternWithParams {
    let subplan = exec_plan.take_subtree(top_id).unwrap();
    let subplan_top_id = subplan.get_ir_plan().get_top().unwrap();
    let sp = SyntaxPlan::new(&subplan, subplan_top_id, snapshot).unwrap();
    let ordered = OrderedSyntaxNodes::try_from(sp).unwrap();
    let nodes = ordered.to_syntax_data().unwrap();
    let (sql, _) = subplan.to_sql(&nodes, name_base, None).unwrap();
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
    let mut vtables: HashMap<NodeId, Rc<VirtualTable>> = HashMap::new();
    vtables.insert(motion_id, Rc::new(virtual_table));

    let exec_plan = query.get_mut_exec_plan();
    exec_plan.set_vtables(vtables);
    let top_id = exec_plan.get_ir_plan().get_top().unwrap();
    let motion_child_id = exec_plan.get_motion_subtree_root(motion_id).unwrap();

    // Check sub-query
    let sql = get_sql_from_execution_plan(exec_plan, motion_child_id, Snapshot::Oldest, TEMPLATE);
    assert_eq!(
        sql,
        PatternWithParams::new(
            r#"SELECT "hash_testing"."identification_number" FROM "hash_testing" WHERE ("hash_testing"."identification_number") > (?)"#.to_string(),
            vec![Value::from(1_u64)]
        ));

    // Check main query
    let sql = get_sql_from_execution_plan(exec_plan, top_id, Snapshot::Oldest, TEMPLATE);
    assert_eq!(
        sql,
        PatternWithParams::new(
            r#"SELECT "test_space"."FIRST_NAME" FROM "test_space" WHERE ("test_space"."id") in (SELECT "identification_number" FROM "TMP_test_0136")"#.to_string(),
            vec![]
        ));
}

#[test]
fn exec_plan_subtree_two_stage_groupby_test() {
    let sql = r#"SELECT "T1"."FIRST_NAME" FROM "test_space" as "T1" group by "T1"."FIRST_NAME""#;
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
        SmolStr::from("FIRST_NAME"),
        Type::String,
    ));

    if let MotionPolicy::Segment(key) = get_motion_policy(query.exec_plan.get_ir_plan(), motion_id)
    {
        virtual_table.reshard(key, &query.coordinator).unwrap();
    }

    let mut vtables: HashMap<NodeId, Rc<VirtualTable>> = HashMap::new();
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
    let sql = get_sql_from_execution_plan(exec_plan, motion_child_id, Snapshot::Oldest, TEMPLATE);
    assert_eq!(
        sql,
        PatternWithParams::new(
            r#"SELECT "T1"."FIRST_NAME" as "column_764" FROM "test_space" as "T1" GROUP BY "T1"."FIRST_NAME""#
                .to_string(),
            vec![]
        )
    );

    // Check main query
    let sql = get_sql_from_execution_plan(exec_plan, top_id, Snapshot::Oldest, TEMPLATE);
    assert_eq!(
        sql,
        PatternWithParams::new(
            r#"SELECT "column_764" as "FIRST_NAME" FROM (SELECT "FIRST_NAME" FROM "TMP_test_0136") GROUP BY "column_764""#.to_string(),
            vec![]
        ));
}

#[test]
fn exec_plan_subtree_two_stage_groupby_test_2() {
    let sql = r#"SELECT "T1"."FIRST_NAME", "T1"."sys_op", "T1"."sysFrom" FROM "test_space" as "T1" GROUP BY "T1"."FIRST_NAME", "T1"."sys_op", "T1"."sysFrom""#;
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
        SmolStr::from("column_764"),
        Type::String,
    ));
    virtual_table.add_column(column_user_non_null(
        SmolStr::from("column_864"),
        Type::Integer,
    ));
    virtual_table.add_column(column_user_non_null(
        SmolStr::from("column_964"),
        Type::Integer,
    ));
    if let MotionPolicy::Segment(key) = get_motion_policy(query.exec_plan.get_ir_plan(), motion_id)
    {
        virtual_table.reshard(key, &query.coordinator).unwrap();
    }

    let mut vtables: HashMap<NodeId, Rc<VirtualTable>> = HashMap::new();
    vtables.insert(motion_id, Rc::new(virtual_table));

    let exec_plan = query.get_mut_exec_plan();
    exec_plan.set_vtables(vtables);
    let top_id = exec_plan.get_ir_plan().get_top().unwrap();
    let motion_child_id = exec_plan.get_motion_subtree_root(motion_id).unwrap();

    // Check groupby local stage
    let sql = get_sql_from_execution_plan(exec_plan, motion_child_id, Snapshot::Oldest, TEMPLATE);
    if let MotionPolicy::Segment(_) = exec_plan.get_motion_policy(motion_id).unwrap() {
    } else {
        panic!("Expected MotionPolicy::Segment for local aggregation stage");
    };
    assert_eq!(
        sql,
        PatternWithParams::new(
            format!(
                "{} {} {}",
                r#"SELECT "T1"."sysFrom" as "column_964", "T1"."FIRST_NAME" as "column_764","#,
                r#""T1"."sys_op" as "column_864" FROM "test_space" as "T1""#,
                r#"GROUP BY "T1"."FIRST_NAME", "T1"."sys_op", "T1"."sysFrom""#,
            ),
            vec![]
        )
    );

    // Check main query
    let sql = get_sql_from_execution_plan(exec_plan, top_id, Snapshot::Oldest, TEMPLATE);
    assert_eq!(
        sql,
        PatternWithParams::new(
            format!(
                "{} {} {} {}",
                r#"SELECT "column_764" as "FIRST_NAME", "column_864" as "sys_op","#,
                r#""column_964" as "sysFrom" FROM"#,
                r#"(SELECT "column_764","column_864","column_964" FROM "TMP_test_0136")"#,
                r#"GROUP BY "column_764", "column_864", "column_964""#,
            ),
            vec![]
        )
    );
}

#[test]
fn exec_plan_subtree_aggregates() {
    let sql = r#"SELECT "T1"."sys_op" || "T1"."sys_op", "T1"."sys_op"*2 + count("T1"."sysFrom"),
                      sum("T1"."id"), sum(distinct "T1"."id"*"T1"."sys_op") / count(distinct "id"),
                      group_concat("T1"."FIRST_NAME", 'o'), avg("T1"."id"), total("T1"."id"), min("T1"."id"), max("T1"."id")
                      FROM "test_space" as "T1" group by "T1"."sys_op""#;
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
    virtual_table.add_column(column_integer_user_non_null(SmolStr::from("sys_op")));
    virtual_table.add_column(column_integer_user_non_null(SmolStr::from("sum_42")));
    virtual_table.add_column(column_integer_user_non_null(SmolStr::from("count_37")));
    virtual_table.add_column(column_integer_user_non_null(SmolStr::from("sum_49")));
    virtual_table.add_column(column_integer_user_non_null(SmolStr::from("count_51")));
    virtual_table.add_column(column_user_non_null(
        SmolStr::from("group_concat_58"),
        Type::String,
    ));
    virtual_table.add_column(column_integer_user_non_null(SmolStr::from("count_61")));
    virtual_table.add_column(column_integer_user_non_null(SmolStr::from("total_64")));
    virtual_table.add_column(column_integer_user_non_null(SmolStr::from("min_67")));
    virtual_table.add_column(column_integer_user_non_null(SmolStr::from("max_70")));
    if let MotionPolicy::Segment(key) = get_motion_policy(query.exec_plan.get_ir_plan(), motion_id)
    {
        virtual_table.reshard(key, &query.coordinator).unwrap();
    }

    let mut vtables: HashMap<NodeId, Rc<VirtualTable>> = HashMap::new();
    vtables.insert(motion_id, Rc::new(virtual_table));

    let exec_plan = query.get_mut_exec_plan();
    exec_plan.set_vtables(vtables);
    let top_id = exec_plan.get_ir_plan().get_top().unwrap();
    let motion_child_id = exec_plan.get_motion_subtree_root(motion_id).unwrap();

    // Check groupby local stage
    let sql = get_sql_from_execution_plan(exec_plan, motion_child_id, Snapshot::Oldest, TEMPLATE);
    if let MotionPolicy::Segment(_) = exec_plan.get_motion_policy(motion_id).unwrap() {
    } else {
        panic!("Expected MotionPolicy::Segment for local aggregation stage");
    };
    assert_eq!(
        sql,
        PatternWithParams::new(
            format!(
                "{} {} {} {} {} {} {}",
                r#"SELECT "T1"."sys_op" as "column_764", "T1"."id" as "column_2864","#,
                r#"("T1"."id") * ("T1"."sys_op") as "column_1632", group_concat ("T1"."FIRST_NAME", ?) as "group_concat_496","#,
                r#"count ("T1"."sysFrom") as "count_096", total ("T1"."id") as "total_696","#,
                r#"min ("T1"."id") as "min_796", count ("T1"."id") as "count_596","#,
                r#"max ("T1"."id") as "max_896", sum ("T1"."id") as "sum_196""#,
                r#"FROM "test_space" as "T1""#,
                r#"GROUP BY "T1"."sys_op", ("T1"."id") * ("T1"."sys_op"), "T1"."id""#,
            ),
            vec![Value::from("o")]
        )
    );

    // Check main query
    let sql = get_sql_from_execution_plan(exec_plan, top_id, Snapshot::Oldest, TEMPLATE);
    assert_eq!(
        sql,
        PatternWithParams::new(
            format!(
                "{} {} {} {} {} {} {} {}",
                r#"SELECT ("column_764") || ("column_764") as "col_1","#,
                r#"("column_764") * (?) + (sum ("count_096")) as "col_2", sum ("sum_196") as "col_3","#,
                r#"(sum (DISTINCT "column_1632")) / (count (DISTINCT "column_2864")) as "col_4","#,
                r#"group_concat ("group_concat_496", ?) as "col_5","#,
                r#"sum (CAST ("sum_196" as double)) / sum (CAST ("count_596" as double)) as "col_6","#,
                r#"total ("total_696") as "col_7", min ("min_796") as "col_8", max ("max_896") as "col_9""#,
                r#"FROM (SELECT "sys_op","sum_42","count_37","sum_49","count_51","group_concat_58","count_61","total_64","min_67","max_70" FROM "TMP_test_0136")"#,
                r#"GROUP BY "column_764""#
            ),
            vec![Value::Unsigned(2), Value::from("o")]
        )
    );
}

#[test]
fn exec_plan_subtree_aggregates_no_groupby() {
    let sql = r#"SELECT count("T1"."sysFrom"), sum(distinct "T1"."id" + "T1"."sysFrom") FROM "test_space" as "T1""#;
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
    virtual_table.add_column(column_integer_user_non_null(SmolStr::from("column_932")));
    virtual_table.add_column(column_integer_user_non_null(SmolStr::from("count_096")));
    if let MotionPolicy::Segment(key) = get_motion_policy(query.exec_plan.get_ir_plan(), motion_id)
    {
        virtual_table.reshard(key, &query.coordinator).unwrap();
    }

    let mut vtables: HashMap<NodeId, Rc<VirtualTable>> = HashMap::new();
    vtables.insert(motion_id, Rc::new(virtual_table));

    let exec_plan = query.get_mut_exec_plan();
    exec_plan.set_vtables(vtables);
    let top_id = exec_plan.get_ir_plan().get_top().unwrap();
    let motion_child_id = exec_plan.get_motion_subtree_root(motion_id).unwrap();

    // Check groupby local stage
    let sql = get_sql_from_execution_plan(exec_plan, motion_child_id, Snapshot::Oldest, TEMPLATE);
    if let MotionPolicy::Full = exec_plan.get_motion_policy(motion_id).unwrap() {
    } else {
        panic!("Expected MotionPolicy::Full for local aggregation stage");
    };
    assert_eq!(
        sql,
        PatternWithParams::new(
            r#"SELECT ("T1"."id") + ("T1"."sysFrom") as "column_632", count ("T1"."sysFrom") as "count_096" FROM "test_space" as "T1" GROUP BY ("T1"."id") + ("T1"."sysFrom")"#.to_string(),
            vec![]
        ));

    // Check main query
    let sql = get_sql_from_execution_plan(exec_plan, top_id, Snapshot::Oldest, TEMPLATE);
    assert_eq!(
        sql,
        PatternWithParams::new(
            r#"SELECT sum ("count_096") as "col_1", sum (DISTINCT "column_632") as "col_2" FROM (SELECT "column_932","count_096" FROM "TMP_test_0136")"#.to_string(),
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
    let mut vtables: HashMap<NodeId, Rc<VirtualTable>> = HashMap::new();
    vtables.insert(motion_id, Rc::new(virtual_table));

    let exec_plan = query.get_mut_exec_plan();
    exec_plan.set_vtables(vtables);
    let top_id = exec_plan.get_ir_plan().get_top().unwrap();

    let sql = get_sql_from_execution_plan(exec_plan, top_id, Snapshot::Oldest, TEMPLATE);
    assert_eq!(
        sql,
        PatternWithParams::new(
            r#"SELECT "tid", "sid" FROM (SELECT "test_space"."id" as "tid" FROM "test_space") INNER JOIN (SELECT "identification_number" FROM "TMP_test_0136") ON ?"#.to_string(),
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
    let mut virtual_table = virtual_table_23(Some("hti"));
    if let MotionPolicy::Segment(key) = get_motion_policy(query.exec_plan.get_ir_plan(), motion_id)
    {
        virtual_table.reshard(key, &query.coordinator).unwrap();
    }
    let mut vtables: HashMap<NodeId, Rc<VirtualTable>> = HashMap::new();
    vtables.insert(motion_id, Rc::new(virtual_table));

    let exec_plan = query.get_mut_exec_plan();
    exec_plan.set_vtables(vtables);
    let top_id = exec_plan.get_ir_plan().get_top().unwrap();

    let sql = get_sql_from_execution_plan(exec_plan, top_id, Snapshot::Oldest, TEMPLATE);
    assert_eq!(
        sql,
        PatternWithParams::new(
            r#"SELECT "tid", "hti"."sid" FROM (SELECT "test_space"."id" as "tid" FROM "test_space") INNER JOIN (SELECT "identification_number" FROM "TMP_test_0136") as "hti" ON ?"#.to_string(),
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
    let mut vtables: HashMap<NodeId, Rc<VirtualTable>> = HashMap::new();
    vtables.insert(motion_id, Rc::new(virtual_table));

    let exec_plan = query.get_mut_exec_plan();
    exec_plan.set_vtables(vtables);
    let top_id = exec_plan.get_ir_plan().get_top().unwrap();

    let sql = get_sql_from_execution_plan(exec_plan, top_id, Snapshot::Oldest, TEMPLATE);
    assert_eq!(
        sql,
        PatternWithParams::new(
            r#"SELECT "test_space"."id" FROM "test_space" WHERE ("test_space"."id") in (SELECT "identification_number" FROM "TMP_test_0136")"#.to_string(),
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
    let mut vtables: HashMap<NodeId, Rc<VirtualTable>> = HashMap::new();
    vtables.insert(motion_id, Rc::new(virtual_table));

    let exec_plan = query.get_mut_exec_plan();
    exec_plan.set_vtables(vtables);
    let top_id = exec_plan.get_ir_plan().get_top().unwrap();

    let sql = get_sql_from_execution_plan(exec_plan, top_id, Snapshot::Oldest, TEMPLATE);
    assert_eq!(
        sql,
        PatternWithParams::new(
            r#"SELECT "test_space"."id" FROM "test_space" EXCEPT SELECT "identification_number" FROM "TMP_test_0136""#.to_string(),
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
    virtual_table.add_column(column_integer_user_non_null(SmolStr::from("count_096")));
    if let MotionPolicy::Segment(key) = get_motion_policy(query.exec_plan.get_ir_plan(), motion_id)
    {
        virtual_table.reshard(key, &query.coordinator).unwrap();
    }

    let mut vtables: HashMap<NodeId, Rc<VirtualTable>> = HashMap::new();
    vtables.insert(motion_id, Rc::new(virtual_table));

    let exec_plan = query.get_mut_exec_plan();
    exec_plan.set_vtables(vtables);
    let top_id = exec_plan.get_ir_plan().get_top().unwrap();
    let motion_child_id = exec_plan.get_motion_subtree_root(motion_id).unwrap();

    // Check groupby local stage
    let sql = get_sql_from_execution_plan(exec_plan, motion_child_id, Snapshot::Oldest, TEMPLATE);
    if let MotionPolicy::Full = exec_plan.get_motion_policy(motion_id).unwrap() {
    } else {
        panic!("Expected MotionPolicy::Full for local aggregation stage");
    };

    assert_eq!(
        sql,
        PatternWithParams::new(
            r#"SELECT count (*) as "count_096" FROM "test_space""#.to_string(),
            vec![]
        )
    );

    // Check main query
    let sql = get_sql_from_execution_plan(exec_plan, top_id, Snapshot::Oldest, TEMPLATE);
    assert_eq!(
        sql,
        PatternWithParams::new(
            r#"SELECT sum ("count_096") as "col_1" FROM (SELECT "count_096" FROM "TMP_test_0136")"#
                .to_string(),
            vec![]
        )
    );
}

#[test]
fn exec_plan_subtree_having() {
    let sql = format!(
        "{} {} {}",
        r#"SELECT "T1"."sys_op" || "T1"."sys_op", count("T1"."sys_op"*2) + count(distinct "T1"."sys_op"*2)"#,
        r#"FROM "test_space" as "T1" group by "T1"."sys_op""#,
        r#"HAVING sum(distinct "T1"."sys_op"*2) > 1"#
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
    virtual_table.add_column(column_integer_user_non_null(SmolStr::from("column_63")));
    virtual_table.add_column(column_integer_user_non_null(SmolStr::from("column_12")));
    virtual_table.add_column(column_integer_user_non_null(SmolStr::from("count_58")));
    if let MotionPolicy::Segment(key) = get_motion_policy(query.exec_plan.get_ir_plan(), motion_id)
    {
        virtual_table.reshard(key, &query.coordinator).unwrap();
    }

    let mut vtables: HashMap<NodeId, Rc<VirtualTable>> = HashMap::new();
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
    let (sql, _) = subplan1.to_sql(&nodes, TEMPLATE, None).unwrap();
    if let MotionPolicy::Segment(_) = exec_plan.get_motion_policy(motion_id).unwrap() {
    } else {
        panic!("Expected MotionPolicy::Segment for local aggregation stage");
    };
    assert_eq!(
        sql,
        PatternWithParams::new(
            format!(
                "{} {} {}",
                r#"SELECT "T1"."sys_op" as "column_764", ("T1"."sys_op") * (?) as "column_2032","#,
                r#"count (("T1"."sys_op") * (?)) as "count_196" FROM "test_space" as "T1""#,
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
    let (sql, _) = subplan2.to_sql(&nodes, TEMPLATE, None).unwrap();
    assert_eq!(
        sql,
        PatternWithParams::new(
            format!(
                "{} {} {} {}",
                r#"SELECT ("column_764") || ("column_764") as "col_1","#,
                r#"(sum ("count_196")) + (count (DISTINCT "column_2032")) as "col_2" FROM"#,
                r#"(SELECT "column_63","column_12","count_58" FROM "TMP_test_0136")"#,
                r#"GROUP BY "column_764" HAVING (sum (DISTINCT "column_2032")) > (?)"#
            ),
            vec![Value::Unsigned(1u64)]
        )
    );
}

#[test]
fn exec_plan_subtree_having_without_groupby() {
    let sql = format!(
        "{} {} {}",
        r#"SELECT count("T1"."sys_op"*2) + count(distinct "T1"."sys_op"*2)"#,
        r#"FROM "test_space" as "T1""#,
        r#"HAVING sum(distinct "T1"."sys_op"*2) > 1"#
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
    virtual_table.add_column(column_integer_user_non_null(SmolStr::from("column_63")));
    virtual_table.add_column(column_integer_user_non_null(SmolStr::from("column_12")));
    virtual_table.add_column(column_integer_user_non_null(SmolStr::from("count_58")));
    if let MotionPolicy::Segment(key) = get_motion_policy(query.exec_plan.get_ir_plan(), motion_id)
    {
        virtual_table.reshard(key, &query.coordinator).unwrap();
    }

    let mut vtables: HashMap<NodeId, Rc<VirtualTable>> = HashMap::new();
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
    let (sql, _) = subplan1.to_sql(&nodes, TEMPLATE, None).unwrap();
    if let MotionPolicy::Full = exec_plan.get_motion_policy(motion_id).unwrap() {
    } else {
        panic!("Expected MotionPolicy::Full after local stage");
    };

    assert_eq!(
        sql,
        PatternWithParams::new(
            format!(
                "{} {} {}",
                r#"SELECT ("T1"."sys_op") * (?) as "column_1332","#,
                r#"count (("T1"."sys_op") * (?)) as "count_196" FROM "test_space" as "T1""#,
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
    let (sql, _) = subplan2.to_sql(&nodes, TEMPLATE, None).unwrap();
    assert_eq!(
        sql,
        PatternWithParams::new(
            format!(
                "{} {} {}",
                r#"SELECT (sum ("count_196")) + (count (DISTINCT "column_1332")) as "col_1""#,
                r#"FROM (SELECT "column_63","column_12","count_58" FROM "TMP_test_0136")"#,
                r#"HAVING (sum (DISTINCT "column_1332")) > (?)"#,
            ),
            vec![Value::Unsigned(1u64)]
        )
    );
}

#[test]
fn global_table_scan() {
    let sql = r#"SELECT * from "global_t""#;
    let coordinator = RouterRuntimeMock::new();

    let mut query = Query::new(&coordinator, sql, vec![]).unwrap();
    let exec_plan = query.get_mut_exec_plan();
    assert_eq!(Vec::<Slice>::new(), exec_plan.get_ir_plan().slices.slices);

    let top_id = exec_plan.get_ir_plan().get_top().unwrap();
    let buckets = query.bucket_discovery(top_id).unwrap();
    assert_eq!(Buckets::Any, buckets);
    let exec_plan = query.get_mut_exec_plan();
    let subplan = exec_plan.take_subtree(top_id).unwrap();
    let subplan_top_id = subplan.get_ir_plan().get_top().unwrap();
    let sp = SyntaxPlan::new(&subplan, subplan_top_id, Snapshot::Oldest).unwrap();
    let ordered = OrderedSyntaxNodes::try_from(sp).unwrap();
    let nodes = ordered.to_syntax_data().unwrap();
    let (sql, _) = subplan.to_sql(&nodes, TEMPLATE, None).unwrap();

    assert_eq!(
        sql,
        PatternWithParams::new(
            format!(
                "{}",
                r#"SELECT "global_t"."a", "global_t"."b" FROM "global_t""#,
            ),
            vec![]
        )
    );
}

#[test]
fn global_union_all() {
    let sql = r#"SELECT "a", "b" from "global_t" union all select "e", "f" from "t2""#;
    let mut coordinator = RouterRuntimeMock::new();
    coordinator.set_vshard_mock(2);

    let mut query = Query::new(&coordinator, sql, vec![]).unwrap();
    let exec_plan = query.get_mut_exec_plan();

    let top_id = exec_plan.get_ir_plan().get_top().unwrap();
    let buckets = query.bucket_discovery(top_id).unwrap();
    assert_eq!(Buckets::All, buckets);
    let exec_plan = query.get_mut_exec_plan();
    let sub_plan = exec_plan.take_subtree(top_id).unwrap();
    let actual_dispatch = coordinator.detailed_dispatch(sub_plan, &buckets);
    let expected = vec![
        ReplicasetDispatchInfo {
            rs_id: 0,
            pattern: r#" select cast(null as integer),cast(null as integer) where false UNION ALL SELECT "t2"."e", "t2"."f" FROM "t2""#.to_string(),
            params: vec![],
            vtables_map: HashMap::new(),
        },
        ReplicasetDispatchInfo {
            rs_id: 1,
            pattern: r#"SELECT "global_t"."a", "global_t"."b" FROM "global_t" UNION ALL SELECT "t2"."e", "t2"."f" FROM "t2""#.to_string(),
            params: vec![],
            vtables_map: HashMap::new(),
        },
    ];

    assert_eq!(expected, actual_dispatch);
}

#[test]
fn global_union_all2() {
    // check that we don't send virtual table to replicasets, where
    // global child is not materialized.
    let sql = r#"SELECT "a", "b" from "global_t" where "b"
    in (select "e" from "t2")
    union all select "e", "f" from "t2""#;
    let mut coordinator = RouterRuntimeMock::new();
    coordinator.set_vshard_mock(3);

    let mut query = Query::new(&coordinator, sql, vec![]).unwrap();

    let motion_id = *query
        .exec_plan
        .get_ir_plan()
        .clone_slices()
        .slice(0)
        .unwrap()
        .position(0)
        .unwrap();
    let motion_child = query.exec_plan.get_motion_subtree_root(motion_id).unwrap();
    // imitate plan execution
    query.exec_plan.take_subtree(motion_child).unwrap();

    let mut virtual_table = VirtualTable::new();
    virtual_table.add_column(column_integer_user_non_null(SmolStr::from("e")));
    virtual_table.add_tuple(vec![Value::Integer(1)]);
    let exec_plan = query.get_mut_exec_plan();
    exec_plan
        .set_motion_vtable(&motion_id, virtual_table.clone(), &coordinator)
        .unwrap();

    let top_id = exec_plan.get_ir_plan().get_top().unwrap();
    let buckets = query.bucket_discovery(top_id).unwrap();
    assert_eq!(Buckets::All, buckets);
    let exec_plan = query.get_mut_exec_plan();

    let sub_plan = exec_plan.take_subtree(top_id).unwrap();
    // after take subtree motion id has changed
    let (motion_id, _) = sub_plan
        .get_ir_plan()
        .nodes
        .iter136()
        .find_position(|n| {
            matches!(
                n,
                Node136::Motion(Motion {
                    policy: MotionPolicy::Full,
                    ..
                })
            )
        })
        .unwrap();

    let actual_dispatch = coordinator.detailed_dispatch(sub_plan, &buckets);
    let motion_id = NodeId {
        offset: motion_id as u32,
        arena_type: ArenaType::Arena136,
    };

    let expected = vec![
        ReplicasetDispatchInfo {
            rs_id: 0,
            pattern: r#" select cast(null as integer),cast(null as integer) where false UNION ALL SELECT "t2"."e", "t2"."f" FROM "t2""#.to_string(),
            params: vec![],
            vtables_map: HashMap::new(),
        },
        ReplicasetDispatchInfo {
            rs_id: 1,
            pattern: r#" select cast(null as integer),cast(null as integer) where false UNION ALL SELECT "t2"."e", "t2"."f" FROM "t2""#.to_string(),
            params: vec![],
            vtables_map: HashMap::new(),
        },
        ReplicasetDispatchInfo {
            rs_id: 2,
            pattern: r#"SELECT "global_t"."a", "global_t"."b" FROM "global_t" WHERE ("global_t"."b") in (SELECT "e" FROM "TMP_test_0136") UNION ALL SELECT "t2"."e", "t2"."f" FROM "t2""#.to_string(),
            params: vec![],
            vtables_map: collection!(motion_id => Rc::new(virtual_table)),
        },
    ];

    assert_eq!(expected, actual_dispatch);
}

#[test]
fn global_union_all3() {
    // also check that we don't delete vtables,
    // from other subtree
    let sql = r#"
    select "a" from "global_t" where "b"
    in (select "e" from "t2")
    union all
    select "f" from "t2"
    group by "f""#;
    let mut coordinator = RouterRuntimeMock::new();
    coordinator.set_vshard_mock(2);

    let mut query = Query::new(&coordinator, sql, vec![]).unwrap();

    let slices = query.exec_plan.get_ir_plan().clone_slices();
    let sq_motion_id = *slices.slice(0).unwrap().position(0).unwrap();
    let sq_motion_child = query
        .exec_plan
        .get_motion_subtree_root(sq_motion_id)
        .unwrap();

    // imitate plan execution
    query.exec_plan.take_subtree(sq_motion_child).unwrap();

    let mut sq_vtable = VirtualTable::new();
    sq_vtable.add_column(column_integer_user_non_null(SmolStr::from("f")));
    sq_vtable.add_tuple(vec![Value::Integer(1)]);
    query
        .exec_plan
        .set_motion_vtable(&sq_motion_id, sq_vtable.clone(), &coordinator)
        .unwrap();

    let groupby_motion_id = *slices.slice(1).unwrap().position(0).unwrap();
    let groupby_motion_child = query
        .exec_plan
        .get_motion_subtree_root(groupby_motion_id)
        .unwrap();
    query.exec_plan.take_subtree(groupby_motion_child).unwrap();

    let mut groupby_vtable = VirtualTable::new();
    // these tuples must belong to different replicasets
    let tuple1 = vec![Value::Integer(3)];
    let tuple2 = vec![Value::Integer(2929)];
    groupby_vtable.add_column(column_integer_user_non_null(SmolStr::from("column_3364")));
    groupby_vtable.add_tuple(tuple1.clone());
    groupby_vtable.add_tuple(tuple2.clone());
    if let MotionPolicy::Segment(key) =
        get_motion_policy(query.exec_plan.get_ir_plan(), groupby_motion_id)
    {
        groupby_vtable.reshard(key, &query.coordinator).unwrap();
    }
    query
        .exec_plan
        .set_motion_vtable(&groupby_motion_id, groupby_vtable.clone(), &coordinator)
        .unwrap();

    let top_id = query.exec_plan.get_ir_plan().get_top().unwrap();
    let buckets = query.bucket_discovery(top_id).unwrap();
    let bucket1 = coordinator.determine_bucket_id(&[&tuple1[0]]).unwrap();
    let bucket2 = coordinator.determine_bucket_id(&[&tuple2[0]]).unwrap();
    let expected_buckets = Buckets::Filtered(collection!(bucket1, bucket2));
    assert_eq!(expected_buckets, buckets);

    let grouped = coordinator.vshard_mock.group(&buckets);
    let mut rs_buckets: Vec<(usize, Vec<u64>)> = grouped
        .into_iter()
        .map(|(k, v)| (VshardMock::get_id(&k), v))
        .collect();
    rs_buckets.sort_by_key(|(id, _)| *id);
    let groupby_vtable1 = groupby_vtable.new_with_buckets(&rs_buckets[0].1).unwrap();
    let groupby_vtable2 = groupby_vtable.new_with_buckets(&rs_buckets[1].1).unwrap();

    let sub_plan = query.exec_plan.take_subtree(top_id).unwrap();
    // after take subtree motion id has changed
    let (groupby_motion_id, _) = sub_plan
        .get_ir_plan()
        .nodes
        .iter136()
        .find_position(|n| {
            matches!(
                n,
                Node136::Motion(Motion {
                    policy: MotionPolicy::Segment(_),
                    ..
                })
            )
        })
        .unwrap();
    let (sq_motion_id, _) = sub_plan
        .get_ir_plan()
        .nodes
        .iter136()
        .find_position(|n| {
            matches!(
                n,
                Node136::Motion(Motion {
                    policy: MotionPolicy::Full,
                    ..
                })
            )
        })
        .unwrap();

    let actual_dispatch = coordinator.detailed_dispatch(sub_plan, &buckets);

    let groupby_motion_id = NodeId {
        offset: groupby_motion_id as u32,
        arena_type: ArenaType::Arena136,
    };

    let sq_motion_id = NodeId {
        offset: sq_motion_id as u32,
        arena_type: ArenaType::Arena136,
    };

    let expected = vec![
        ReplicasetDispatchInfo {
            rs_id: 0,
            pattern: r#" select cast(null as integer) where false UNION ALL SELECT "column_3364" as "f" FROM (SELECT "column_3364" FROM "TMP_test_2136") GROUP BY "column_3364""#.to_string(),
            params: vec![],
            vtables_map: collection!(groupby_motion_id => Rc::new(groupby_vtable1)),
        },
        ReplicasetDispatchInfo {
            rs_id: 1,
            pattern: r#"SELECT "global_t"."a" FROM "global_t" WHERE ("global_t"."b") in (SELECT "f" FROM "TMP_test_0136") UNION ALL SELECT "column_3364" as "f" FROM (SELECT "column_3364" FROM "TMP_test_2136") GROUP BY "column_3364""#.to_string(),
            params: vec![],
            vtables_map: collection!(sq_motion_id => Rc::new(sq_vtable), groupby_motion_id => Rc::new(groupby_vtable2)),
        },
    ];

    assert_eq!(expected, actual_dispatch);
}

#[test]
fn global_union_all4() {
    let sql = r#"
    select "b" from "global_t"
    union all
    select * from (
        select "a" from "global_t"
        union all
        select "f" from "t2"
    )
    "#;
    let mut coordinator = RouterRuntimeMock::new();
    coordinator.set_vshard_mock(2);

    let mut query = Query::new(&coordinator, sql, vec![]).unwrap();

    let top_id = query.exec_plan.get_ir_plan().get_top().unwrap();
    let buckets = query.bucket_discovery(top_id).unwrap();
    let sub_plan = query.exec_plan.take_subtree(top_id).unwrap();

    let actual_dispatch = coordinator.detailed_dispatch(sub_plan, &buckets);

    let expected = vec![
        ReplicasetDispatchInfo {
            rs_id: 0,
            pattern: r#" select cast(null as integer) where false UNION ALL SELECT "a" FROM (select cast(null as integer) where false UNION ALL SELECT "t2"."f" FROM "t2")"#.to_string(),
            params: vec![],
            vtables_map: HashMap::new(),
        },
        ReplicasetDispatchInfo {
            rs_id: 1,
            pattern: r#"SELECT "global_t"."b" FROM "global_t" UNION ALL SELECT "a" FROM (SELECT "global_t"."a" FROM "global_t" UNION ALL SELECT "t2"."f" FROM "t2")"#.to_string(),
            params: vec![],
            vtables_map: HashMap::new(),
        },
    ];

    assert_eq!(expected, actual_dispatch);
}

#[test]
fn global_except() {
    let sql = r#"select "a" from "global_t"
    except select "e" from "t2""#;
    let mut coordinator = RouterRuntimeMock::new();
    coordinator.set_vshard_mock(3);

    let mut query = Query::new(&coordinator, sql, vec![]).unwrap();

    let intersect_motion_id = *query
        .exec_plan
        .get_ir_plan()
        .clone_slices()
        .slice(0)
        .unwrap()
        .position(0)
        .unwrap();

    {
        // check map stage
        let motion_child = query
            .exec_plan
            .get_motion_subtree_root(intersect_motion_id)
            .unwrap();
        let sql = get_sql_from_execution_plan(
            &mut query.exec_plan,
            motion_child,
            Snapshot::Oldest,
            TEMPLATE,
        );
        assert_eq!(
            sql,
            PatternWithParams::new(
                r#"SELECT "t2"."e" FROM "t2" INTERSECT SELECT "global_t"."a" FROM "global_t""#
                    .to_string(),
                vec![]
            )
        );

        let mut virtual_table = VirtualTable::new();
        virtual_table.add_column(column_integer_user_non_null(SmolStr::from("e")));
        virtual_table.add_tuple(vec![Value::Integer(1)]);
        query
            .get_mut_exec_plan()
            .set_motion_vtable(&intersect_motion_id, virtual_table.clone(), &coordinator)
            .unwrap();
    }

    // check reduce stage
    let res = *query
        .dispatch()
        .unwrap()
        .downcast::<ProducerResult>()
        .unwrap();
    let mut expected = ProducerResult::new();
    expected.rows.extend(vec![vec![
        LuaValue::String(format!("Execute query locally")),
        LuaValue::String(String::from(PatternWithParams::new(
            r#"SELECT "global_t"."a" FROM "global_t" EXCEPT SELECT "e" FROM "TMP_test_0136""#
                .into(),
            vec![],
        ))),
    ]]);
    assert_eq!(expected, res)
}

#[test]
fn exec_plan_order_by() {
    let sql = r#"SELECT "identification_number"
                      FROM (select "identification_number" from "hash_testing")
                      ORDER BY "identification_number""#;
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
    let mut vtables: HashMap<NodeId, Rc<VirtualTable>> = HashMap::new();
    vtables.insert(motion_id, Rc::new(virtual_table));

    let exec_plan = query.get_mut_exec_plan();
    exec_plan.set_vtables(vtables);
    let top_id = exec_plan.get_ir_plan().get_top().unwrap();
    let motion_child_id = exec_plan.get_motion_subtree_root(motion_id).unwrap();

    // Check sub-query
    let sql = get_sql_from_execution_plan(exec_plan, motion_child_id, Snapshot::Oldest, TEMPLATE);
    assert_eq!(
        sql,
        PatternWithParams::new(
            r#"SELECT "identification_number" FROM (SELECT "hash_testing"."identification_number" FROM "hash_testing")"#.to_string(),
            vec![]
        )
    );

    // Check main query
    let sql = get_sql_from_execution_plan(exec_plan, top_id, Snapshot::Oldest, TEMPLATE);
    assert_eq!(
        sql,
        PatternWithParams::new(
            r#"SELECT "identification_number" FROM (SELECT "identification_number" FROM "TMP_test_0136") ORDER BY "identification_number""#.to_string(),
            vec![]
        ));
}

fn check_subtree_hashes_are_equal(
    sql1: &str,
    values1: Vec<Value>,
    sql2: &str,
    values2: Vec<Value>,
) {
    let coordinator = RouterRuntimeMock::new();
    let get_hash = |sql: &str, values: Vec<Value>| -> SmolStr {
        let mut query = Query::new(&coordinator, sql, values).unwrap();
        query
            .get_mut_exec_plan()
            .get_mut_ir_plan()
            .stash_constants()
            .unwrap();
        let ir = query.get_exec_plan().get_ir_plan();
        let top = ir.get_top().unwrap();
        ir.pattern_id(top).unwrap()
    };

    assert_eq!(get_hash(sql1, values1), get_hash(sql2, values2));
}

#[test]
fn subtree_hash1() {
    check_subtree_hashes_are_equal(
        r#"select ?, ? from "t""#,
        vec![Value::Unsigned(1), Value::Unsigned(1)],
        r#"select $1, $1 from "t""#,
        vec![Value::Unsigned(1)],
    );
}

#[test]
fn subtree_hash2() {
    check_subtree_hashes_are_equal(
        r#"select ?, ? from "t"
        option(sql_vdbe_max_steps = ?, vtable_max_rows = ?)"#,
        vec![
            Value::Unsigned(1),
            Value::Unsigned(11),
            Value::Unsigned(3),
            Value::Unsigned(10),
        ],
        r#"select $1, $1 from "t"
        option(sql_vdbe_max_steps = $1, vtable_max_rows = $1)"#,
        vec![Value::Unsigned(1)],
    );
}

/* FIXME: https://git.picodata.io/picodata/picodata/sbroad/-/issues/583
#[test]
fn subtree_hash3() {
    check_subtree_hashes_are_equal(
        r#"select ?, ? from "t"
        option(sql_vdbe_max_steps = ?)"#,
        vec![Value::Unsigned(1), Value::Unsigned(11), Value::Unsigned(10)],
        r#"select ?, ? from "t""#,
        vec![Value::Unsigned(1), Value::Unsigned(1)],
    );
}
*/
