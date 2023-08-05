//! Helper module with functions and structures for the IR.

use crate::backend::sql::tree::{SyntaxData, SyntaxPlan};
use crate::errors::{Action, Entity, SbroadError};
use crate::ir::expression::Expression;
use crate::ir::operator::Relational;
use crate::ir::tree::traversal::{PostOrder, EXPR_CAPACITY};
use crate::ir::Plan;
use std::collections::hash_map::DefaultHasher;
use std::fmt::Write;
use std::hash::BuildHasher;

/// Helper macros to build a hash map or set
/// from the list of arguments.
#[macro_export]
macro_rules! collection {
    // map-like
    ($($k:expr => $v:expr),* $(,)?) => {{
        use std::iter::{Iterator, IntoIterator};
        Iterator::collect(IntoIterator::into_iter([$(($k, $v),)*]))
    }};
    // set-like
    ($($v:expr),* $(,)?) => {{
        use std::iter::{Iterator, IntoIterator};
        Iterator::collect(IntoIterator::into_iter([$($v,)*]))
    }};
}

/// By default, `HashSet` uses `RandomState` to initialize the hasher.
/// Because of that, we can't get repeatable order in the set between
/// runs that can cause flaky test results in some cases.
#[derive(Clone)]
pub struct RepeatableState;

impl BuildHasher for RepeatableState {
    type Hasher = DefaultHasher;
    fn build_hasher(&self) -> DefaultHasher {
        DefaultHasher::new()
    }
}

impl Default for RepeatableState {
    fn default() -> Self {
        Self
    }
}

/// Helper formatting function for tabulation.
fn formatted_tabulate(buf: &mut String, n: i32) -> Result<(), std::fmt::Error> {
    for _ in 0..n {
        write!(buf, "\t")?;
    }
    Ok(())
}

/// Formatting helper debug functions
impl Plan {
    /// Helper function for printing Expression node.
    fn write_expr(
        &self,
        buf: &mut String,
        tabulation_number: i32,
        node_id: usize,
    ) -> Result<(), std::fmt::Error> {
        let expr_try = self.get_expression_node(node_id);
        if let Ok(expr) = expr_try {
            write!(buf, "expression: ")?;
            match expr {
                Expression::Alias { name, child } => {
                    let child_expr = self.get_expression_node(*child);
                    if let Ok(child_expr) = child_expr {
                        writeln!(buf, "Alias [name = {name}, child = {child_expr:?}]")?;
                    } else {
                        writeln!(buf, "Alias [name = {name}, child = NOT_FOUND]")?;
                    }
                }
                Expression::Bool { op, left, right } => {
                    writeln!(buf, "Bool [op: {op}]")?;
                    formatted_tabulate(buf, tabulation_number + 1)?;
                    writeln!(buf, "Left child")?;
                    self.formatted_arena_node(buf, tabulation_number + 1, *left)?;
                    formatted_tabulate(buf, tabulation_number + 1)?;
                    writeln!(buf, "Right child")?;
                    self.formatted_arena_node(buf, tabulation_number + 1, *right)?;
                }
                Expression::Constant { value } => {
                    writeln!(buf, "Constant [value = {value}]")?;
                }
                Expression::CountAsterisk => {
                    writeln!(buf, "CountAsterisk")?;
                }
                Expression::Reference {
                    targets,
                    position,
                    parent,
                    col_type,
                } => {
                    let alias_name = self.get_alias_from_reference_node(expr).unwrap();

                    writeln!(buf, "Reference")?;
                    formatted_tabulate(buf, tabulation_number + 1)?;
                    writeln!(buf, "Alias: {alias_name}")?;

                    // See explain logic for Reference node
                    let rel_id = self.get_relational_from_reference_node(node_id);
                    if let Ok(rel_id) = rel_id {
                        let rel_node = self.get_relation_node(*rel_id);
                        if let Ok(rel_node) = rel_node {
                            if let Ok(Some(name)) = rel_node.scan_name(self, *position) {
                                formatted_tabulate(buf, tabulation_number + 1)?;
                                writeln!(buf, "Referenced table name (or alias): {name}")?;
                            }
                        }
                    }

                    formatted_tabulate(buf, tabulation_number + 1)?;
                    writeln!(buf, "Parent: {parent:?}")?;

                    if let Some(targets) = targets {
                        for target_id in targets {
                            formatted_tabulate(buf, tabulation_number + 1)?;
                            writeln!(buf, "target_id: {target_id}")?;
                        }
                    } else {
                        writeln!(buf, "NO TARGETS")?;
                    }

                    formatted_tabulate(buf, tabulation_number + 1)?;
                    writeln!(buf, "Column type: {col_type}")?;
                }
                Expression::Row { list, distribution } => {
                    writeln!(buf, "Row [distribution = {distribution:?}]")?;
                    formatted_tabulate(buf, tabulation_number + 1)?;
                    writeln!(buf, "List:")?;
                    for value in list {
                        self.formatted_arena_node(buf, tabulation_number + 2, *value)?;
                    }
                }
                Expression::Cast { .. } => writeln!(buf, "Cast")?,
                Expression::Concat { .. } => writeln!(buf, "Concat")?,
                Expression::StableFunction { .. } => writeln!(buf, "StableFunction")?,
                Expression::Unary { op, child } => {
                    writeln!(buf, "Unary [op: {op}]")?;
                    formatted_tabulate(buf, tabulation_number + 1)?;
                    writeln!(buf, "Child")?;
                    self.formatted_arena_node(buf, tabulation_number + 1, *child)?;
                }
                Expression::Arithmetic { .. } => writeln!(buf, "Arithmetic")?,
            };
        }
        Ok(())
    }

    /// Debug function for formatting working arena node.
    #[allow(clippy::too_many_lines)]
    fn formatted_arena_node(
        &self,
        buf: &mut String,
        tabulation_number: i32,
        node_id: usize,
    ) -> Result<(), std::fmt::Error> {
        if tabulation_number == 0 {
            writeln!(buf, "---------------------------------------------")?;
        }
        formatted_tabulate(buf, tabulation_number)?;
        write!(buf, "[id: {node_id}] ")?;
        let relation = self.get_relation_node(node_id);
        if let Ok(relation) = relation {
            write!(buf, "relation: ")?;
            // Print relation name and specific info.
            match relation {
                Relational::ScanRelation {
                    alias, relation, ..
                } => {
                    writeln!(buf, "ScanRelation")?;
                    formatted_tabulate(buf, tabulation_number + 1)?;
                    writeln!(buf, "Relation: {relation}")?;
                    if let Some(alias) = alias {
                        formatted_tabulate(buf, tabulation_number + 1)?;
                        writeln!(buf, "Alias: {alias}")?;
                    }
                }
                Relational::Join { condition, .. } => {
                    writeln!(buf, "InnerJoin")?;
                    formatted_tabulate(buf, tabulation_number + 1)?;
                    writeln!(buf, "Condition:")?;
                    self.formatted_arena_node(buf, tabulation_number + 2, *condition)?;
                }
                Relational::Projection { .. } => {
                    writeln!(buf, "Projection")?;
                }
                Relational::ScanSubQuery { alias, .. } => {
                    writeln!(buf, "ScanSubQuery")?;
                    if let Some(alias) = alias {
                        if !alias.is_empty() {
                            formatted_tabulate(buf, tabulation_number + 1)?;
                            writeln!(buf, "Alias: {alias}")?;
                        }
                    }
                }
                Relational::Selection {
                    children: _,
                    filter,
                    output: _,
                } => {
                    writeln!(buf, "Selection")?;
                    formatted_tabulate(buf, tabulation_number + 1)?;
                    writeln!(buf, "Filter")?;
                    self.formatted_arena_node(buf, tabulation_number + 1, *filter)?;
                }
                Relational::Having { filter, .. } => {
                    writeln!(buf, "Having")?;
                    formatted_tabulate(buf, tabulation_number + 1)?;
                    writeln!(buf, "Filter")?;
                    self.formatted_arena_node(buf, tabulation_number + 1, *filter)?;
                }
                Relational::GroupBy {
                    gr_cols, is_final, ..
                } => {
                    writeln!(buf, "GroupBy [is_final = {is_final}]")?;
                    formatted_tabulate(buf, tabulation_number + 1)?;
                    writeln!(buf, "Gr_cols:")?;
                    for gr_col in gr_cols {
                        formatted_tabulate(buf, tabulation_number + 2)?;
                        let gl_col_expr = self.get_expression_node(*gr_col);
                        if let Ok(gl_col_expr) = gl_col_expr {
                            writeln!(buf, "Gr_col: {gl_col_expr:?}")?;
                        } else {
                            writeln!(buf, "Gr_col: {gr_col}")?;
                        }
                    }
                }
                Relational::Values { .. } => writeln!(buf, "Values")?,
                Relational::ValuesRow { .. } => writeln!(buf, "ValuesRow")?,
                Relational::Motion { policy, .. } => {
                    writeln!(buf, "Motion [policy = {policy:?}]")?;
                }
                Relational::UnionAll { .. } => writeln!(buf, "UnionAll")?,
                Relational::Update {
                    relation,
                    update_columns_map,
                    ..
                } => {
                    writeln!(buf, "Update")?;
                    formatted_tabulate(buf, tabulation_number + 1)?;
                    writeln!(buf, "Update columns map:")?;
                    for (rel_pos, proj_pos) in update_columns_map {
                        formatted_tabulate(buf, tabulation_number + 2)?;
                        writeln!(
                            buf,
                            "Update {relation} column on pos {rel_pos} to child projection column on pos {proj_pos}"
                        )?;
                    }
                }
                Relational::Delete { .. } => writeln!(buf, "Delete")?,
                Relational::Insert { .. } => writeln!(buf, "Insert")?,
                Relational::Except { .. } => writeln!(buf, "Except")?,
            }
            // Print children.
            match relation {
                Relational::Join { children, .. }
                | Relational::Projection { children, .. }
                | Relational::Except { children, .. }
                | Relational::Delete { children, .. }
                | Relational::Insert { children, .. }
                | Relational::ScanSubQuery { children, .. }
                | Relational::Selection { children, .. }
                | Relational::Values { children, .. }
                | Relational::Motion { children, .. }
                | Relational::UnionAll { children, .. }
                | Relational::Update { children, .. }
                | Relational::Having { children, .. }
                | Relational::GroupBy { children, .. }
                | Relational::ValuesRow { children, .. } => {
                    formatted_tabulate(buf, tabulation_number + 1)?;
                    writeln!(buf, "Children:")?;
                    for child in children {
                        formatted_tabulate(buf, tabulation_number + 2)?;
                        writeln!(buf, "Child_id = {child}")?;
                    }
                }
                Relational::ScanRelation { .. } => {
                    formatted_tabulate(buf, tabulation_number + 1)?;
                    writeln!(buf, "[No children]")?;
                }
            }
            // Print output.
            match relation {
                Relational::ScanRelation { output, .. }
                | Relational::Join { output, .. }
                | Relational::Except { output, .. }
                | Relational::Delete { output, .. }
                | Relational::Insert { output, .. }
                | Relational::Projection { output, .. }
                | Relational::ScanSubQuery { output, .. }
                | Relational::GroupBy { output, .. }
                | Relational::Selection { output, .. }
                | Relational::Having { output, .. }
                | Relational::Values { output, .. }
                | Relational::Motion { output, .. }
                | Relational::UnionAll { output, .. }
                | Relational::Update { output, .. }
                | Relational::ValuesRow { output, .. } => {
                    formatted_tabulate(buf, tabulation_number + 1)?;
                    writeln!(buf, "Output_id: {output}")?;
                    self.formatted_arena_node(buf, tabulation_number + 2, *output)?;
                }
            }
            writeln!(buf, "---------------------------------------------")?;
        } else {
            self.write_expr(buf, tabulation_number, node_id)?;
        }
        Ok(())
    }

    /// Pretty-printing plan `arena` subtree.
    ///
    /// # Errors
    /// Fail to format one of the relational node.
    pub fn formatted_arena_subtree(&mut self, node_id: usize) -> Result<String, SbroadError> {
        let mut ir_tree = PostOrder::with_capacity(|node| self.nodes.rel_iter(node), EXPR_CAPACITY);
        ir_tree.populate_nodes(node_id);
        let nodes = ir_tree.take_nodes();

        let mut buf = String::new();
        for (_, id) in &nodes {
            if self.formatted_arena_node(&mut buf, 0, *id).is_err() {
                return Err(SbroadError::FailedTo(
                    Action::Serialize,
                    Some(Entity::Plan),
                    "Unable to get formatted arena string".to_string(),
                ));
            }
        }
        Ok(buf)
    }

    /// Pretty-printing plan `arena`.
    ///
    /// # Errors
    /// Fail to format one of the relational node.
    pub fn formatted_arena(&mut self) -> Result<String, SbroadError> {
        let top_id = self.get_top()?;
        self.formatted_arena_subtree(top_id)
    }
}

/// Formatting helper debug functions
impl SyntaxPlan<'_> {
    /// Pretty-printing plan `nodes.arena`.
    ///
    /// # Errors
    /// - Failed to retrieve top node.
    /// - Failed to format one of the nodes.
    pub fn formatted(&self, plan: &Plan) -> Result<String, SbroadError> {
        let mut buf = String::new();
        let top = self.top.ok_or_else(|| {
            SbroadError::Invalid(
                Entity::SyntaxPlan,
                Some(String::from(
                    "Top id should have already be set for SyntaxPlan",
                )),
            )
        })?;
        if self.formatted_inner(plan, &mut buf, 0, top).is_err() {
            return Err(SbroadError::FailedTo(
                Action::Serialize,
                Some(Entity::SyntaxPlan),
                "Unable to get formatted arena string".to_string(),
            ));
        }
        Ok(buf)
    }

    fn formatted_inner(
        &self,
        plan: &Plan,
        buf: &mut String,
        tabulation_number: i32,
        node_id: usize,
    ) -> Result<(), std::fmt::Error> {
        let node = self.nodes.arena.get(node_id);
        if let Some(node) = node {
            formatted_tabulate(buf, tabulation_number)?;
            let data = &node.data;
            if let SyntaxData::PlanId(id) = data {
                let node = plan.get_node(*id);
                if let Ok(node) = node {
                    writeln!(buf, "{node:?} [id={id}]")?;
                }
            } else {
                writeln!(buf, "{data:?}")?;
            }

            if let Some(left_id) = node.left {
                formatted_tabulate(buf, tabulation_number + 1)?;
                writeln!(buf, "Left:")?;
                self.formatted_inner(plan, buf, tabulation_number + 2, left_id)?;
            }
            if !node.right.is_empty() {
                formatted_tabulate(buf, tabulation_number + 1)?;
                writeln!(buf, "Right:")?;
            }
            for right in &node.right {
                self.formatted_inner(plan, buf, tabulation_number + 2, *right)?;
            }
        } else {
            formatted_tabulate(buf, tabulation_number)?;
            writeln!(buf, "MISSING")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
