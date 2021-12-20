use super::*;
use crate::ir::value::*;
use crate::ir::*;
use pretty_assertions::assert_eq;

#[test]
fn row_duplicate_column_names() {
    let mut plan = Plan::new();

    let c1 = plan.nodes.add_const(Value::number_from_str("1").unwrap());
    let c1_alias_a = plan.nodes.add_alias("a", c1).unwrap();
    let c2 = plan.nodes.add_const(Value::number_from_str("2").unwrap());
    let c2_alias_a = plan.nodes.add_alias("a", c2).unwrap();
    assert_eq!(
        QueryPlannerError::DuplicateColumn,
        plan.nodes
            .add_row(vec![c1_alias_a, c2_alias_a], None)
            .unwrap_err()
    );
}
