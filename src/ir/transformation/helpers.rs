use crate::executor::engine::mock::MetadataMock;
use crate::executor::ir::ExecutionPlan;
use crate::frontend::sql::ast::AbstractSyntaxTree;

#[allow(dead_code)]
pub fn transform_sql(input: &str) -> String {
    let metadata = &MetadataMock::new();
    let ast = AbstractSyntaxTree::new(input).unwrap();

    let mut plan = ast.to_ir(metadata).unwrap();
    plan.derive_equalities().unwrap();

    let ex_plan = ExecutionPlan::from(plan);
    let top_id = ex_plan.get_ir_plan().get_top().unwrap();
    ex_plan.subtree_as_sql(top_id).unwrap()
}
