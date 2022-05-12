//! IR test helpers.

use crate::executor::bucket::Buckets;
use crate::executor::engine::mock::MetadataMock;
use crate::executor::ir::ExecutionPlan;
use crate::frontend::sql::ast::AbstractSyntaxTree;
use crate::frontend::Ast;
use crate::ir::value::Value;
use crate::ir::Plan;

/// Compiles an SQL query to IR plan.
#[allow(dead_code)]
pub fn sql_to_ir(query: &str, params: &[Value]) -> Plan {
    let metadata = &MetadataMock::new();
    let ast = AbstractSyntaxTree::new(query).unwrap();
    let mut plan = ast.resolve_metadata(metadata).unwrap();
    plan.bind_params(params).unwrap();
    plan
}

/// Compiles and transforms an SQL query to a new SQL.
#[allow(dead_code)]
pub fn sql_to_sql(query: &str, params: &[Value], f_transform: &dyn Fn(&mut Plan)) -> String {
    let mut plan = sql_to_ir(query, params);
    f_transform(&mut plan);
    let ex_plan = ExecutionPlan::from(plan);
    let top_id = ex_plan.get_ir_plan().get_top().unwrap();
    let nodes = ex_plan.get_sql_order(top_id).unwrap();
    ex_plan.subtree_as_sql(&nodes, &Buckets::All).unwrap()
}
