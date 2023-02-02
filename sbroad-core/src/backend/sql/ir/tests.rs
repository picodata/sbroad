use pretty_assertions::assert_eq;

use crate::backend::sql::tree::{OrderedSyntaxNodes, SyntaxPlan};
use crate::executor::bucket::Buckets;
use crate::executor::engine::mock::RouterConfigurationMock;
use crate::executor::ir::ExecutionPlan;
use crate::frontend::sql::ast::AbstractSyntaxTree;
use crate::frontend::Ast;
use crate::ir::tree::Snapshot;

use super::*;

#[allow(clippy::needless_pass_by_value)]
fn check_sql_with_snapshot(
    query: &str,
    params: Vec<Value>,
    expected: PatternWithParams,
    snapshot: Snapshot,
) {
    let metadata = &RouterConfigurationMock::new();
    let ast = AbstractSyntaxTree::new(query).unwrap();
    let mut plan = ast.resolve_metadata(metadata).unwrap();
    plan.bind_params(params).unwrap();
    plan.replace_in_operator().unwrap();
    plan.split_columns().unwrap();
    plan.set_dnf().unwrap();
    plan.derive_equalities().unwrap();
    plan.merge_tuples().unwrap();
    let ex_plan = ExecutionPlan::from(plan);

    let top_id = ex_plan.get_ir_plan().get_top().unwrap();
    let sp = SyntaxPlan::new(&ex_plan, top_id, snapshot).unwrap();
    let ordered = OrderedSyntaxNodes::try_from(sp).unwrap();
    let nodes = ordered.to_syntax_data().unwrap();
    let (sql, _) = ex_plan.to_sql(&nodes, &Buckets::All, "test").unwrap();

    assert_eq!(expected, sql,);
}

#[cfg(test)]
mod except;

#[cfg(test)]
mod projection;

#[cfg(test)]
mod inner_join;

#[cfg(test)]
mod selection;

#[cfg(test)]
mod sub_query;

#[cfg(test)]
mod union_all;
