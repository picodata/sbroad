//! Equality propagation deduces new equality expressions in join conditions
//! and selection filters.
//! For example:
//! ```
//! (a = 1) and (b = 1) => (a = b)
//! ```
//!
//! It spawns new equalities that can be helpful for the final motion
//! transformation (compares left and right row distribution in the
//! joins and subqueries, in a case of a distribution conflict inserts
//! a motion node).
//!
//! That is why equality propagation deducts only new equalities with rows
//! containing references (at the moment only rows with a single column of
//! the reference type are supported). Constants are replicated and do not
//! produce distribution conflicts.
//! ```
//! select * from (select t1.a, t2.b from t1 join t2 on row(t1.c) = row(t2.d))
//! where row(a) = 1 and row(b) = 1
//! =>
//! select * from (select t1.a, t2.b from t1 join t2 on row(t1.c) = row(t2.d))
//! where row(a) = 1 and row(b) = 1 and row(a) = row(b)
//! =>
//! ..
//! =>
//! select * from (select t1.a, t2.b from t1 join t2 on row(t1.c) = row(t2.d)
//! and row(t1.a) = row(t2.b))
//! where row(a) = 1 and row(b) = 1
//! ```
//! We don't produce (1) = (1) equality as it don't give any new information,
//! but row(a) = row(b) can be pushed down to the join condition.
//!
//! Currently implementation produces new equalities only for constants, references
//! and rows that contain a single reference column.
//!
//! # Implementation
//! Let's look on an example:
//! ```
//! select * from t where
//! (a) = 1 and (c) = (e) and (b) = 1 and (d) = 1 and (e) = 4 and (f) = 1 and (a) = (f)
//! or (e) = 3
//! or (a) = 1 and (c) = (b) and (b) = 1
//! ```
//!
//! 1. We traverse the plan nodes list and collect subtree top nodes for
//!    selection filter and join condition. In a current example wa have
//!    only selection filter (second "OR" in the where clause). It is
//!    implemented in `gather_expr_for_eq_propagation()`.
//!
//!    Selection filter is a tree constructed from the "AND"-ed chains
//!    ((a) = 1 and (c) = (b) and (b) = 1) of boolean expressions. These
//!    chains are bind with "OR" expressions. We are interested in the chains
//!    as only their elements can form equivalence classes.
//!
//!    Each chain can produce multiple equivalence classes. For example,
//!    ```
//!    (a) = 1 and (c) = (e) and (b) = 1 and (d) = 1 and (e) = 4 and (f) = 1
//!    and (a) = (f)
//!    ```
//!    produces two eq classes: {a, 1, b, d, f} and {c, e, 4}.
//!    Also, every chain is a tree structure, so it can be represented by its
//!    top (`(f) = 1 AND (a) = (f)` in the example above).
//!
//! 2. Traverse the filter tree top-down level by level (BFT) and look for the
//!    chain tops. When a chain top is found, traverse the whole chain of the
//!    "AND"-ed boolean expressions in DFT preorder manner. All traversed nodes
//!    are added to visited set as we want to escape them next time in BFT loop.
//!
//! 3. After traversing any chain we build its equivalence classes. Some classes
//!    in the list can be splitted (because of the order of equalities in the chain)
//!    and they should be merged later.
//!    `(a) = 1 and (c) = (b) and (b) = 1` after DFT produces two eq classes: `{a, 1, b}`
//!    and `{c, b, 1}`. They can be merged into a single eq class `{a, 1, b, c}`
//!
//!    We also track already existing references pairs in equalities (there is no need
//!    to generate already existing ones). So, in the current example pairs set contains
//!    `{c, b}`. We subtract these pairs from every equality class in the list as well
//!    as constants. So, as a result we get a single reference eq class
//!    `{a, 1, b, c} -> {a, b, c} -> {a, b, c} - {c, b} -> {a}`. We don't have enough
//!    elements to produce a new equivalence (need two items at least), so nothing to
//!    add in this example.
//!
//!    But an example from the step 1 produces eq class chain [{a, 1, b, d, f}, {c, e, 4}]
//!    with pairs `{c, e, a, f}`. After removing constants and subtracting pairs we get a
//!    new pair to produce:
//!    `[{a, b, d, f} - {c, e, a, f}, {c, e} - {c, e, a, f}] -> [{b, d}, {}] -> [{b, d}]`
//!
//! 4. We collect all equivalence classes in the chains (a chain is determined by its top)
//!    with `eq_class_suggestions()` function. Then we produce new equality boolean nodes
//!    in the plan united with "AND" for each chain and append them under the chain top node
//!    (`add_new_equalities()`).

use crate::errors::QueryPlannerError;
use crate::ir::expression::Expression;
use crate::ir::helpers::RepeatableState;
use crate::ir::operator::{Bool, Relational};
use crate::ir::value::Value;
use crate::ir::{Node, Nodes};
use itertools::Itertools;
use std::collections::{HashMap, HashSet};
use traversal::{Bft, DftPre};

/// A copy of the `Expression::Reference` with traits for the `HashSet`.
#[derive(Clone, Hash, PartialEq, Eq, Debug)]
struct EqClassRef {
    targets: Option<Vec<usize>>,
    position: usize,
    parent: Option<usize>,
}

impl EqClassRef {
    fn from_ref(expr: &Expression) -> Result<Self, QueryPlannerError> {
        if let Expression::Reference {
            targets: expr_tgt,
            position: expr_pos,
            parent: expr_prt,
        } = expr
        {
            return Ok(EqClassRef {
                targets: expr_tgt.clone(),
                position: *expr_pos,
                parent: *expr_prt,
            });
        }
        Err(QueryPlannerError::InvalidReference)
    }

    fn from_single_col_row(expr: &Expression, nodes: &Nodes) -> Result<Self, QueryPlannerError> {
        if let Expression::Row { list, .. } = expr {
            if list.len() == 1 {
                let col = *list.get(0).ok_or(QueryPlannerError::ValueOutOfRange)?;
                if let Node::Expression(expr) =
                    nodes.arena.get(col).ok_or(QueryPlannerError::InvalidNode)?
                {
                    return EqClassRef::from_ref(expr);
                }
            }
            return Err(QueryPlannerError::DoSkip);
        }
        Err(QueryPlannerError::InvalidRow)
    }

    fn to_single_col_row(&self, nodes: &mut Nodes) -> usize {
        let r_node = nodes.add_ref(self.parent, self.targets.clone(), self.position);
        nodes.add_row(vec![r_node], None)
    }
}

#[derive(Clone, Hash, PartialEq, Debug)]
struct EqClassConst {
    value: Value,
}

/// Original `Expression::Constant` doesn't support Eq trait because
/// NaN doesn't satisfy equality reflexive property (NaN != NaN).
///
/// We always filter NaN in our equality class sets, so implement
/// Eq trait for `EqClassConst`.
impl Eq for EqClassConst {}

impl EqClassConst {
    fn from_const(expr: &Expression) -> Result<Self, QueryPlannerError> {
        if let Expression::Constant { value: expr_value } = expr {
            return Ok(EqClassConst {
                value: expr_value.clone(),
            });
        }
        Err(QueryPlannerError::InvalidConstant)
    }

    fn to_const(&self, nodes: &mut Nodes) -> usize {
        nodes.add_const(self.value.clone())
    }
}

/// A enum for supported elements in equality class.
#[derive(Clone, Hash, PartialEq, Eq, Debug)]
enum EqClassExpr {
    EqClassConst(EqClassConst),
    EqClassRef(EqClassRef),
}

impl EqClassExpr {
    fn to_node(&self, nodes: &mut Nodes) -> usize {
        match self {
            EqClassExpr::EqClassConst(ec_const) => ec_const.to_const(nodes),
            EqClassExpr::EqClassRef(ec_ref) => ec_ref.to_single_col_row(nodes),
        }
    }
}

/// A set of expressions that are equal to each other.
#[derive(Clone, PartialEq, Debug)]
struct EqClass {
    set: HashSet<EqClassExpr, RepeatableState>,
}

impl EqClass {
    fn new() -> Self {
        EqClass {
            set: HashSet::with_hasher(RepeatableState),
        }
    }

    /// Build a copy of the current equality class containing only references.
    fn ref_copy(&self) -> Self {
        let mut result = EqClass::new();

        for item in &self.set {
            if let EqClassExpr::EqClassRef(expr) = item {
                result.set.insert(EqClassExpr::EqClassRef(expr.clone()));
            }
        }

        result
    }
}

/// A list of equivalence classes from a single "AND"-ed chain of expressions.
#[derive(Clone, PartialEq, Debug)]
struct EqClassChain {
    list: Vec<EqClass>,
}

impl EqClassChain {
    fn new() -> Self {
        EqClassChain { list: Vec::new() }
    }

    /// Insert a new pair to the equality classes chain.
    fn insert(&mut self, left: &EqClassExpr, right: &EqClassExpr) {
        let mut ok = false;

        // Insert a pair to all equality classes that have equal element.
        for class in &mut self.list {
            if class.set.get(left).is_some() || class.set.get(right).is_some() {
                class.set.insert(left.clone());
                class.set.insert(right.clone());
                ok = true;
            }
        }

        // No matches, so add a new equality class.
        if !ok {
            let mut class = EqClass::new();
            class.set.insert(left.clone());
            class.set.insert(right.clone());
            self.list.push(class);
        }
    }

    /// Merge equality classes in the chain if they contain common elements
    fn merged_copy(&self) -> Self {
        let mut result = EqClassChain::new();
        let mut matched: HashSet<usize> = HashSet::new();

        for i in 0..self.list.len() {
            if matched.get(&i).is_some() {
                continue;
            }

            if let Some(class) = self.list.get(i) {
                let mut new_class = class.clone();
                matched.insert(i);

                for j in i..self.list.len() {
                    if matched.get(&j).is_some() {
                        continue;
                    }

                    if let Some(item) = self.list.get(j) {
                        let mut buf = EqClass::new();

                        for expr in item.set.intersection(&new_class.set) {
                            buf.set.insert(expr.clone());
                        }

                        if !buf.set.is_empty() {
                            matched.insert(j);
                        }

                        for expr in buf.set {
                            new_class.set.insert(expr);
                        }
                    }
                }

                result.list.push(new_class);
            }
        }

        result
    }
}

impl Nodes {
    /// Scan all the plan nodes and return a list of boolean expressions
    /// from selection and join nodes that can be used for equality
    /// propagation transformation.
    ///
    /// Other relational operators simply create references to the input tuple
    /// and don't contain any weird expression trees under hood. So, skip them.
    ///
    /// TODO: IR can support arbitrary expressions in projections as well, but
    /// at the moment we forbid this functionality at the parser level.
    ///
    /// # Errors
    /// - invalid nodes
    /// - no candidates for the current transformation (`RedundantTransformation` error)
    fn gather_expr_for_eq_propagation(&self) -> Result<Vec<usize>, QueryPlannerError> {
        let mut tops: Vec<usize> = Vec::new();
        for node in &self.arena {
            if let Node::Relational(
                Relational::Selection { filter: top, .. }
                | Relational::InnerJoin { condition: top, .. },
            ) = node
            {
                if let Node::Expression(Expression::Bool { .. }) =
                    self.arena.get(*top).ok_or(QueryPlannerError::InvalidNode)?
                {
                    tops.push(*top);
                }
            }
        }
        Ok(tops)
    }

    // Get expression node value and convert it to the suitable one for an equivalence class or error.
    // DoSkip is a special case of an error - nothing bad had happened, the target node doesn't contain
    // anything interesting for us, skip it without any serious error.
    fn node_to_eq_class_expr(&self, node: usize) -> Result<EqClassExpr, QueryPlannerError> {
        if let Node::Expression(expr) =
            self.arena.get(node).ok_or(QueryPlannerError::InvalidNode)?
        {
            match expr {
                Expression::Constant { .. } => {
                    Ok(EqClassExpr::EqClassConst(EqClassConst::from_const(expr)?))
                }
                Expression::Reference { .. } => {
                    Ok(EqClassExpr::EqClassRef(EqClassRef::from_ref(expr)?))
                }
                Expression::Row { .. } => match EqClassRef::from_single_col_row(expr, self) {
                    Ok(ecr) => Ok(EqClassExpr::EqClassRef(ecr)),
                    Err(e) => Err(e),
                },
                _ => Err(QueryPlannerError::DoSkip),
            }
        } else {
            Err(QueryPlannerError::InvalidNode)
        }
    }

    /// Returns suggestions with new equations that should be added under specific node.
    /// The result is serialized as a hash map:
    /// - key: node position in the plan nodes, that is the top of the "AND"-ed expression chain.
    ///        The new expressions should be added under it.
    /// - value: a chain of equivalence classes to produce new equalities.
    fn eq_class_suggestions(&self) -> Result<HashMap<usize, EqClassChain>, QueryPlannerError> {
        let tops = self.gather_expr_for_eq_propagation()?;
        let mut visited: HashSet<usize> = HashSet::new();
        let mut map: HashMap<usize, EqClassChain> = HashMap::new();

        // Build equivalence classes and pair set.
        for top in &tops {
            let bft = Bft::new(top, |node| self.expr_iter(node, true));
            for (_level, chain_top) in bft {
                // Skip all nodes rather then boolean "and", and those "and"
                // that were already visited.
                let is_and: bool = if let Node::Expression(Expression::Bool { op, .. }) = self
                    .arena
                    .get(*chain_top)
                    .ok_or(QueryPlannerError::InvalidNode)?
                {
                    *op == Bool::And
                } else {
                    false
                };
                if !is_and || visited.get(chain_top).is_some() {
                    continue;
                }

                // Iterate the chain of "and"-ed equivalents to build equivalence
                // classes. All traversed "and" nodes are placed to the visited set.
                let mut eq_classes = EqClassChain::new();
                let mut pairs: HashSet<EqClassExpr, RepeatableState> =
                    HashSet::with_hasher(RepeatableState);
                let mut res = EqClassChain::new();
                let chain = DftPre::new(chain_top, |node| self.eq_iter(node));
                for (_l, item) in chain {
                    visited.insert(*item);
                    if let Node::Expression(Expression::Bool {
                        left, op, right, ..
                    }) = self
                        .arena
                        .get(*item)
                        .ok_or(QueryPlannerError::InvalidNode)?
                    {
                        if (*op != Bool::Eq) && (*op != Bool::In) {
                            continue;
                        }

                        let eqe_left_res = self.node_to_eq_class_expr(*left);
                        let eqe_right_res = self.node_to_eq_class_expr(*right);
                        if let Ok(ref el) = eqe_left_res {
                            if let Ok(ref er) = eqe_right_res {
                                eq_classes.insert(el, er);
                                if let EqClassExpr::EqClassRef(_) = el {
                                    if let EqClassExpr::EqClassRef(_) = er {
                                        pairs.insert(el.clone());
                                        pairs.insert(er.clone());
                                    }
                                }
                            }
                        };
                    }
                }

                let merged_eq_classes = eq_classes.merged_copy();

                // Remove everything from each equality class except references
                for class in &merged_eq_classes.list {
                    let ec_ref = class.ref_copy();

                    // Remove already matched pairs from the equality class
                    let mut new_class = EqClass::new();
                    for elem in ec_ref.set.difference(&pairs) {
                        new_class.set.insert(elem.clone());
                    }

                    if !new_class.set.is_empty() {
                        res.list.push(new_class);
                    }
                }
                if !res.list.is_empty() {
                    map.insert(*chain_top, res);
                }
            }
        }
        Ok(map)
    }

    /// Collect equality classes in the plan, analyze them, derive new
    /// equality nodes and append them to the plan.
    ///
    /// # Errors
    /// - Invalid node or bool, can be caused only by internal logical errors.
    pub fn add_new_equalities(&mut self) -> Result<(), QueryPlannerError> {
        let map = self.eq_class_suggestions()?;
        for (top, chain) in &map {
            let mut left_child: usize =
                if let Node::Expression(Expression::Bool { left, op, .. }) =
                    self.arena.get(*top).ok_or(QueryPlannerError::InvalidBool)?
                {
                    // In a case of `Bool::Eq` current top never has "AND"-ed children, so skip it.
                    // We should never get anything except `Bool::And` under normal conditions.
                    if *op != Bool::And {
                        continue;
                    }
                    *left
                } else {
                    continue;
                };

            // Build "AND"-ed chain of new equalities linked on the right sight to the
            // left child of the top node. Left side of this new chain is still not
            // linked to the top.
            for ec in &chain.list {
                // Do not generate new equalities from a empty or single element lists.
                if ec.set.len() <= 1 {
                    continue;
                }

                for (a, b) in (&ec.set).iter().tuple_windows() {
                    let a_node = a.to_node(self);
                    let b_node = b.to_node(self);

                    let eq_node = self.add_bool(a_node, Bool::Eq, b_node)?;
                    let and_node = self.add_bool(left_child, Bool::And, eq_node)?;
                    left_child = and_node;
                }
            }

            // Link new chain to the top node (instead of its left element).
            if let Node::Expression(Expression::Bool { ref mut left, .. }) = self
                .arena
                .get_mut(*top)
                .ok_or(QueryPlannerError::InvalidBool)?
            {
                *left = left_child;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
