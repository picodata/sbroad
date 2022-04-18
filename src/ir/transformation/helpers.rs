//! IR test helpers.

use crate::executor::engine::mock::MetadataMock;
use crate::executor::ir::ExecutionPlan;
use crate::frontend::sql::ast::AbstractSyntaxTree;
use crate::frontend::Ast;
use crate::ir::Plan;

/// Compiles an SQL query to IR plan.
#[allow(dead_code)]
pub fn sql_to_ir(input: &str) -> Plan {
    let metadata = &MetadataMock::new();
    let ast = AbstractSyntaxTree::new(input).unwrap();
    let plan = ast.to_ir(metadata).unwrap();
    plan
}

/// Compiles and transforms an SQL query to a new SQL.
#[allow(dead_code)]
pub fn sql_to_sql(input: &str, f_transform: &dyn Fn(&mut Plan)) -> String {
    let mut plan = sql_to_ir(input);
    f_transform(&mut plan);
    let ex_plan = ExecutionPlan::from(plan);
    let top_id = ex_plan.get_ir_plan().get_top().unwrap();
    ex_plan.subtree_as_sql(top_id).unwrap()
}
