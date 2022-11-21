use pretty_assertions::assert_eq;

use crate::backend::sql::tree::{OrderedSyntaxNodes, SyntaxPlan};
use crate::executor::engine::mock::RouterRuntimeMock;
use crate::ir::transformation::redistribution::MotionPolicy;
use crate::ir::tree::Snapshot;

use super::*;

#[test]
fn exec_plan_subtree_test() {
    let sql = r#"SELECT "FIRST_NAME" FROM "test_space" where "id" in
    (SELECT "identification_number" FROM "hash_testing" where "identification_number" > 1)"#;
    let coordinator = RouterRuntimeMock::new();

    let mut query = Query::new(&coordinator, sql, vec![]).unwrap();
    let motion_id = query.exec_plan.get_ir_plan().clone_slices().unwrap()[0][0];
    let mut virtual_table = virtual_table_23();
    if let MotionPolicy::Segment(key) = get_motion_policy(query.exec_plan.get_ir_plan(), motion_id)
    {
        query
            .reshard_vtable(&mut virtual_table, key, &DataGeneration::None)
            .unwrap();
    }
    let mut vtables: HashMap<usize, Rc<VirtualTable>> = HashMap::new();
    vtables.insert(motion_id, Rc::new(virtual_table));

    let exec_plan = query.get_mut_exec_plan();
    exec_plan.set_vtables(vtables);
    let top_id = exec_plan.get_ir_plan().get_top().unwrap();
    let motion_child_id = exec_plan.get_motion_subtree_root(motion_id).unwrap();

    // Check sub-query
    let subplan1 = exec_plan.new_from_subtree(motion_child_id).unwrap();
    let subplan1_top_id = subplan1.get_ir_plan().get_top().unwrap();
    let sp = SyntaxPlan::new(&subplan1, subplan1_top_id, Snapshot::Oldest).unwrap();
    let ordered = OrderedSyntaxNodes::try_from(sp).unwrap();
    let nodes = ordered.to_syntax_data().unwrap();
    let sql = subplan1.to_sql(&nodes, &Buckets::All).unwrap();
    assert_eq!(
        sql,
        PatternWithParams::new(
            r#"SELECT "hash_testing"."identification_number" FROM "hash_testing" WHERE ("hash_testing"."identification_number") > (?)"#.to_string(),
            vec![Value::from(1_u64)]
        ));

    // Check main query
    let subplan2 = exec_plan.new_from_subtree(top_id).unwrap();
    let subplan2_top_id = subplan2.get_ir_plan().get_top().unwrap();
    let sp = SyntaxPlan::new(&subplan2, subplan2_top_id, Snapshot::Oldest).unwrap();
    let ordered = OrderedSyntaxNodes::try_from(sp).unwrap();
    let nodes = ordered.to_syntax_data().unwrap();
    let sql = subplan2.to_sql(&nodes, &Buckets::All).unwrap();
    assert_eq!(
        sql,
        PatternWithParams::new(
            r#"SELECT "test_space"."FIRST_NAME" FROM "test_space" WHERE ("test_space"."id") in (SELECT COLUMN_2 as "identification_number" FROM (VALUES (?),(?)))"#.to_string(),
            vec![Value::from(2_u64), Value::from(3_u64)]
        ));
}