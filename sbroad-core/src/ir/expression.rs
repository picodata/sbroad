//! Expression module.
//!
//! Expressions are the building blocks of the tuple.
//! They provide information about:
//! - what input tuple's columns where used to build our tuple
//! - the order of the columns (and we can get their types as well)
//! - distribution of the data in the tuple

use ahash::RandomState;
use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};
use traversal::DftPost;

use crate::errors::QueryPlannerError;
use crate::ir::operator::{Bool, Relational};

use super::distribution::Distribution;
use super::value::Value;
use super::{operator, Node, Nodes, Plan};

pub mod cast;

/// Tuple tree build blocks.
///
/// A tuple describes a single portion of data moved among cluster nodes.
/// It consists of the ordered, strictly typed expressions with names
/// (columns) and additional information about data distribution policy.
///
/// Tuple is a tree with a `Row` top (level 0) and a list of the named
/// `Alias` columns (level 1). This convention is used across the code
/// and should not be changed. It ensures that we always know the
/// name of any column in the tuple and therefore simplifies AST
/// deserialization.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub enum Expression {
    /// Expression name.
    ///
    /// Example: `42 as a`.
    Alias {
        /// Alias name.
        name: String,
        /// Child expression node index in the plan node arena.
        child: usize,
    },
    /// Binary expression returning boolean result.
    ///
    /// Example: `a > 42`, `b in (select c from ...)`.
    Bool {
        /// Left branch expression node index in the plan node arena.
        left: usize,
        /// Boolean operator.
        op: operator::Bool,
        /// Right branch expression node index in the plan node arena.
        right: usize,
    },
    /// Type cast expression.
    ///
    /// Example: `cast(a as text)`.
    Cast {
        /// Target expression that must be casted to another type.
        child: usize,
        /// Cast type.
        to: cast::Type,
    },
    /// Constant expressions.
    ///
    // Example: `42`.
    Constant {
        /// Contained value (boolean, number, string or null)
        value: Value,
    },
    /// Reference to the position in the incoming tuple(s).
    /// Uses a relative pointer as a coordinate system:
    /// - relational node (containing this reference)
    /// - target(s) in the relational nodes list of children
    /// - column position in the child(ren) output tuple
    Reference {
        /// Relational node ID that contains current reference.
        parent: Option<usize>,
        /// Targets in the relational node children list.
        /// - Leaf nodes (relation scans): None.
        /// - Union nodes: two elements (left and right).
        /// - Other: single element.
        targets: Option<Vec<usize>>,
        /// Expression position in the input tuple (i.e. `Alias` column).
        position: usize,
    },
    /// Top of the tuple tree.
    ///
    /// If the current tuple is the output for some relational operator, it should
    /// consist of the list of aliases. Otherwise (rows in selection filter
    /// or in join condition) we don't require aliases in the list.
    ///
    ///
    ///  Example: (a, b, 1).
    Row {
        /// A list of the alias expression node indexes in the plan node arena.
        list: Vec<usize>,
        /// Resulting data distribution of the tuple. Should be filled as a part
        /// of the last "add Motion" transformation.
        distribution: Option<Distribution>,
    },
    /// Stable function cannot modify the database and
    /// is guaranteed to return the same results given
    /// the same arguments for all rows within a single
    /// statement.
    ///
    /// Example: `bucket_id("1")` (the number of buckets can be
    /// changed only after restarting the cluster).
    StableFunction {
        /// Function name.
        name: String,
        /// Function arguments.
        children: Vec<usize>,
    },
    /// Unary expression returning boolean result.
    Unary {
        /// Unary operator.
        op: operator::Unary,
        /// Child expression node index in the plan node arena.
        child: usize,
    },
}

#[allow(dead_code)]
impl Expression {
    /// Gets current row distribution.
    ///
    /// # Errors
    /// Returns `QueryPlannerError` when the function is called on expression
    /// other than `Row` or a node doesn't know its distribution yet.
    pub fn distribution(&self) -> Result<&Distribution, QueryPlannerError> {
        if let Expression::Row { distribution, .. } = self {
            let dist = match distribution {
                Some(d) => d,
                None => return Err(QueryPlannerError::UninitializedDistribution),
            };
            return Ok(dist);
        }
        Err(QueryPlannerError::InvalidRow)
    }

    /// Clone the row children list.
    ///
    /// # Errors
    /// - node isn't `Row`
    pub fn clone_row_list(&self) -> Result<Vec<usize>, QueryPlannerError> {
        match self {
            Expression::Row { list, .. } => Ok(list.clone()),
            _ => Err(QueryPlannerError::CustomError("Node isn't row type".into())),
        }
    }

    /// Get a reference to the row children list.
    ///
    /// # Errors
    /// - node isn't `Row`
    pub fn get_row_list(&self) -> Result<&[usize], QueryPlannerError> {
        match self {
            Expression::Row { ref list, .. } => Ok(list),
            _ => Err(QueryPlannerError::CustomError("Node isn't row type".into())),
        }
    }

    /// Gets alias node name.
    ///
    /// # Errors
    /// - node isn't `Alias`
    pub fn get_alias_name(&self) -> Result<&str, QueryPlannerError> {
        match self {
            Expression::Alias { name, .. } => Ok(name.as_str()),
            _ => Err(QueryPlannerError::CustomError(
                "Node isn't alias type".into(),
            )),
        }
    }

    /// Checks for distribution determination
    ///
    /// # Errors
    /// - distribution isn't set
    pub fn has_unknown_distribution(&self) -> Result<bool, QueryPlannerError> {
        let d = self.distribution()?;
        Ok(d.is_unknown())
    }

    /// Gets value from const node
    ///
    /// # Errors
    /// - node isn't constant type
    pub fn get_const_value(&self) -> Result<Value, QueryPlannerError> {
        if let Expression::Constant { value } = self.clone() {
            return Ok(value);
        }

        Err(QueryPlannerError::CustomError(
            "Node isn't const type".into(),
        ))
    }

    /// Gets reference value from const node
    ///
    /// # Errors
    /// - node isn't constant type
    pub fn get_const_value_ref(&self) -> Result<&Value, QueryPlannerError> {
        if let Expression::Constant { value } = self {
            return Ok(value);
        }

        Err(QueryPlannerError::CustomError(
            "Node isn't const type".into(),
        ))
    }

    /// Gets relational node id containing the reference.
    ///
    /// # Errors
    /// - node isn't reference type
    /// - reference doesn't have a parent
    pub fn get_parent(&self) -> Result<usize, QueryPlannerError> {
        if let Expression::Reference { parent, .. } = self {
            return parent
                .ok_or_else(|| QueryPlannerError::CustomError("Reference has no parent".into()));
        }
        Err(QueryPlannerError::CustomError(
            "Node isn't reference type".into(),
        ))
    }

    /// The node is a row expression.
    #[must_use]
    pub fn is_row(&self) -> bool {
        matches!(self, Expression::Row { .. })
    }

    /// The node is a constant expression.
    #[must_use]
    pub fn is_const(&self) -> bool {
        matches!(self, Expression::Constant { .. })
    }

    /// Replaces parent in the reference node with the new one.
    pub fn replace_parent_in_reference(&mut self, from_id: Option<usize>, to_id: Option<usize>) {
        if let Expression::Reference { parent, .. } = self {
            if *parent == from_id {
                *parent = to_id;
            }
        }
    }
}

impl Nodes {
    /// Adds alias node.
    ///
    /// # Errors
    /// - child node is invalid
    /// - name is empty
    pub fn add_alias(&mut self, name: &str, child: usize) -> Result<usize, QueryPlannerError> {
        self.arena
            .get(child)
            .ok_or(QueryPlannerError::InvalidNode)?;
        if name.is_empty() {
            return Err(QueryPlannerError::InvalidName);
        }
        let alias = Expression::Alias {
            name: String::from(name),
            child,
        };
        Ok(self.push(Node::Expression(alias)))
    }

    /// Adds boolean node.
    ///
    /// # Errors
    /// - when left or right nodes are invalid
    pub fn add_bool(
        &mut self,
        left: usize,
        op: operator::Bool,
        right: usize,
    ) -> Result<usize, QueryPlannerError> {
        self.arena.get(left).ok_or(QueryPlannerError::InvalidNode)?;
        self.arena
            .get(right)
            .ok_or(QueryPlannerError::InvalidNode)?;
        Ok(self.push(Node::Expression(Expression::Bool { left, op, right })))
    }

    /// Adds constant node.
    pub fn add_const(&mut self, value: Value) -> usize {
        self.push(Node::Expression(Expression::Constant { value }))
    }

    /// Adds reference node.
    pub fn add_ref(
        &mut self,
        parent: Option<usize>,
        targets: Option<Vec<usize>>,
        position: usize,
    ) -> usize {
        let r = Expression::Reference {
            parent,
            targets,
            position,
        };
        self.push(Node::Expression(r))
    }

    /// Adds row node.
    pub fn add_row(&mut self, list: Vec<usize>, distribution: Option<Distribution>) -> usize {
        self.push(Node::Expression(Expression::Row { list, distribution }))
    }

    /// Adds row node, where every column has an alias.
    /// Mostly used for relational node output.
    ///
    /// # Errors
    /// - nodes in a list are invalid
    /// - nodes in a list are not aliases
    /// - aliases in a list have duplicate names
    pub fn add_row_of_aliases(
        &mut self,
        list: Vec<usize>,
        distribution: Option<Distribution>,
    ) -> Result<usize, QueryPlannerError> {
        let mut names: HashSet<String> = HashSet::with_capacity(list.len());

        for alias_node in &list {
            if let Node::Expression(Expression::Alias { name, .. }) = self
                .arena
                .get(*alias_node)
                .ok_or(QueryPlannerError::InvalidNode)?
            {
                if !names.insert(String::from(name)) {
                    return Err(QueryPlannerError::CustomError(format!(
                        "Row can't be added because `{}` already has an alias",
                        name
                    )));
                }
            } else {
                return Err(QueryPlannerError::InvalidRow);
            }
        }
        Ok(self.add_row(list, distribution))
    }

    /// Adds unary boolean node.
    ///
    /// # Errors
    /// - child node is invalid
    pub fn add_unary_bool(
        &mut self,
        op: operator::Unary,
        child: usize,
    ) -> Result<usize, QueryPlannerError> {
        self.arena.get(child).ok_or_else(|| {
            QueryPlannerError::CustomError(format!("Invalid node in the plan arena:{}", child))
        })?;
        Ok(self.push(Node::Expression(Expression::Unary { op, child })))
    }
}

impl Plan {
    /// Returns a list of columns from the child node outputs.
    /// If the column list is empty then copies all the non-sharding columns
    /// from the child node to a new tuple.
    ///
    /// The `is_join` option "on" builds an output tuple for the left child and
    /// appends the right child's one to it. Otherwise we build an output tuple
    /// only from the first (left) child.
    /// # Errors
    /// Returns `QueryPlannerError`:
    /// - relation node contains invalid `Row` in the output
    /// - targets and children are inconsistent
    /// - column names don't exist
    #[allow(clippy::too_many_lines)]
    pub fn new_columns(
        &mut self,
        children: &[usize],
        is_join: bool,
        targets: &[usize],
        col_names: &[&str],
        need_aliases: bool,
        need_sharding_column: bool,
    ) -> Result<Vec<usize>, QueryPlannerError> {
        // We can pass two target children nodes only in case
        // of `UnionAll` and `InnerJoin`.
        // - For the `UnionAll` operator we need only the first
        // child to get correct column names for a new tuple
        // (the second child aliases would be shadowed). But each reference should point
        // to both children to give us additional information
        // during transformations.
        if (targets.len() > 2) || targets.is_empty() {
            return Err(QueryPlannerError::CustomError(format!(
                "Invalid target length: {}",
                targets.len()
            )));
        }

        if let Some(max) = targets.iter().max() {
            if *max >= children.len() {
                return Err(QueryPlannerError::CustomError(format!(
                    "Invalid children length: {}",
                    children.len()
                )));
            }
        }
        let mut result: Vec<usize> = Vec::new();

        if col_names.is_empty() {
            let required_targets = if is_join { targets } else { &targets[0..1] };
            for target_idx in required_targets {
                let target_child: usize = if let Some(target) = targets.get(*target_idx) {
                    *target
                } else {
                    return Err(QueryPlannerError::CustomError(
                        "Failed to find the child node pointed by target index".into(),
                    ));
                };
                let child_node: usize = if let Some(child) = children.get(target_child) {
                    *child
                } else {
                    return Err(QueryPlannerError::CustomError(
                        "Child node not found".into(),
                    ));
                };
                let relational_op = self.get_relation_node(child_node)?;
                let child_row_list: Vec<(usize, usize)> = if let Expression::Row { list, .. } =
                    self.get_expression_node(relational_op.output())?
                {
                    // We have to filter the sharding column in the projection nearest
                    // to the relational scan. We can have two possible combinations:
                    // 1. projection -> selection -> scan
                    // 2. projection -> scan
                    // As a result `relational_op` can be either a selection or a scan.
                    if need_sharding_column {
                        list.iter()
                            .enumerate()
                            .map(|(pos, id)| (pos, *id))
                            .collect()
                    } else {
                        let table_name: Option<&str> = match relational_op {
                            Relational::ScanRelation { relation, .. } => Some(relation.as_str()),
                            Relational::Selection {
                                children: sel_child_ids,
                                ..
                            } => {
                                let sel_child_id = if let (Some(sel_child_id), None) =
                                    (sel_child_ids.first(), sel_child_ids.get(1))
                                {
                                    *sel_child_id
                                } else {
                                    return Err(QueryPlannerError::CustomError(format!(
                                        "Selection node has invalid children: {:?}",
                                        sel_child_ids
                                    )));
                                };
                                let sel_child = self.get_relation_node(sel_child_id)?;
                                match sel_child {
                                    Relational::ScanRelation { relation, .. } => {
                                        Some(relation.as_str())
                                    }
                                    _ => None,
                                }
                            }
                            _ => None,
                        };
                        if let Some(relation) = table_name {
                            let table = self.get_relation(relation).ok_or_else(|| {
                                QueryPlannerError::CustomError(format!(
                                    "Failed to find table {} in the plan",
                                    relation
                                ))
                            })?;
                            let sharding_column_pos = table.get_bucket_id_position()?;
                            // Take an advantage of the fact that the output aliases
                            // in the relation scan are in the same order as its columns.
                            list.iter()
                                .enumerate()
                                .filter(|(pos, _)| sharding_column_pos.ne(pos))
                                .map(|(pos, id)| (pos, *id))
                                .collect()
                        } else {
                            list.iter()
                                .enumerate()
                                .map(|(pos, id)| (pos, *id))
                                .collect()
                        }
                    }
                } else {
                    return Err(QueryPlannerError::CustomError(
                        "Child node is not a row".into(),
                    ));
                };
                result.reserve(child_row_list.len());
                for (pos, alias_node) in child_row_list {
                    let name: String =
                        if let Node::Expression(Expression::Alias { ref name, .. }) =
                            self.get_node(alias_node)?
                        {
                            String::from(name)
                        } else {
                            return Err(QueryPlannerError::CustomError(
                                "Child node is not an alias".into(),
                            ));
                        };
                    let new_targets: Vec<usize> = if is_join {
                        // Reference in a join tuple first points to the left,
                        // then to the right child.
                        vec![*target_idx]
                    } else {
                        // Reference in union tuple points to **both** left and right children.
                        targets.to_vec()
                    };
                    // Adds new references and aliases to arena (if we need them).
                    let r_id = self.nodes.add_ref(None, Some(new_targets), pos);
                    if need_aliases {
                        let a_id = self.nodes.add_alias(&name, r_id)?;
                        result.push(a_id);
                    } else {
                        result.push(r_id);
                    }
                }
            }

            return Ok(result);
        }

        result.reserve(col_names.len());
        let target_child: usize = if let Some(target) = targets.first() {
            *target
        } else {
            return Err(QueryPlannerError::CustomError("Target is empty".into()));
        };
        let child_node: usize = if let Some(child) = children.get(target_child) {
            *child
        } else {
            return Err(QueryPlannerError::CustomError(
                "Failed to get a child pointed by the target".into(),
            ));
        };

        let mut col_names_set: HashSet<&str, RandomState> =
            HashSet::with_capacity_and_hasher(col_names.len(), RandomState::new());
        for col_name in col_names {
            col_names_set.insert(col_name);
        }

        let relational_op = self.get_relation_node(child_node)?;
        let output_id = relational_op.output();
        let output = self.get_expression_node(output_id)?;
        let map: HashMap<&str, usize, RandomState> = if let Expression::Row { list, .. } = output {
            let state = RandomState::new();
            let mut map: HashMap<&str, usize, RandomState> =
                HashMap::with_capacity_and_hasher(col_names.len(), state);
            for (pos, col_id) in list.iter().enumerate() {
                let alias = self.get_expression_node(*col_id)?;
                let name = alias.get_alias_name()?;
                if !col_names_set.contains(name) {
                    continue;
                }
                if map.insert(name, pos).is_some() {
                    return Err(QueryPlannerError::CustomError(format!(
                        "Duplicate column name {} at position {}",
                        name, pos
                    )));
                }
            }
            map
        } else {
            return Err(QueryPlannerError::CustomError(
                "Relational output tuple is not a row".into(),
            ));
        };

        let mut refs: Vec<(&str, Vec<usize>, usize)> = Vec::with_capacity(col_names.len());
        let all_found = col_names.iter().all(|col| {
            map.get(col).map_or(false, |pos| {
                refs.push((col, targets.to_vec(), *pos));
                true
            })
        });
        if !all_found {
            return Err(QueryPlannerError::CustomError(format!(
                "Some of the columns {:?} were not found in the table",
                col_names,
            )));
        }

        for (col, new_targets, pos) in refs {
            let r_id = self.nodes.add_ref(None, Some(new_targets), pos);
            if need_aliases {
                let a_id = self.nodes.add_alias(col, r_id)?;
                result.push(a_id);
            } else {
                result.push(r_id);
            }
        }

        Ok(result)
    }

    /// New output for a single child node (with aliases).
    ///
    /// If column names are empty, copy all the columns from the child.
    /// # Errors
    /// Returns `QueryPlannerError`:
    /// - child is an inconsistent relational node
    /// - column names don't exist
    pub fn add_row_for_output(
        &mut self,
        child: usize,
        col_names: &[&str],
        need_sharding_column: bool,
    ) -> Result<usize, QueryPlannerError> {
        let list =
            self.new_columns(&[child], false, &[0], col_names, true, need_sharding_column)?;
        self.nodes.add_row_of_aliases(list, None)
    }

    /// New output row for union node.
    ///
    /// # Errors
    /// Returns `QueryPlannerError`:
    /// - children are inconsistent relational nodes
    pub fn add_row_for_union_except(
        &mut self,
        left: usize,
        right: usize,
    ) -> Result<usize, QueryPlannerError> {
        let list = self.new_columns(&[left, right], false, &[0, 1], &[], true, true)?;
        self.nodes.add_row_of_aliases(list, None)
    }

    /// New output row for join node.
    ///
    /// Contains all the columns from left and right children.
    ///
    /// # Errors
    /// Returns `QueryPlannerError`:
    /// - children are inconsistent relational nodes
    pub fn add_row_for_join(
        &mut self,
        left: usize,
        right: usize,
    ) -> Result<usize, QueryPlannerError> {
        let list = self.new_columns(&[left, right], true, &[0, 1], &[], true, true)?;
        self.nodes.add_row_of_aliases(list, None)
    }

    /// Project columns from the child node.
    ///
    /// New columns don't have aliases. If column names are empty,
    /// copy all the columns from the child.
    /// # Errors
    /// Returns `QueryPlannerError`:
    /// - child is an inconsistent relational node
    /// - column names don't exist
    pub fn add_row_from_child(
        &mut self,
        child: usize,
        col_names: &[&str],
    ) -> Result<usize, QueryPlannerError> {
        let list = self.new_columns(&[child], false, &[0], col_names, false, true)?;
        Ok(self.nodes.add_row(list, None))
    }

    /// Project columns from the child subquery node.
    ///
    /// New columns don't have aliases. If column names are empty,
    /// copy all the columns from the child.
    /// # Errors
    /// Returns `QueryPlannerError`:
    /// - children nodes are not a relational
    /// - column names don't exist
    pub fn add_row_from_sub_query(
        &mut self,
        children: &[usize],
        children_pos: usize,
        col_names: &[&str],
    ) -> Result<usize, QueryPlannerError> {
        let list = self.new_columns(children, false, &[children_pos], col_names, false, true)?;
        Ok(self.nodes.add_row(list, None))
    }

    /// Project columns from the join's left branch.
    ///
    /// New columns don't have aliases. If column names are empty,
    /// copy all the columns from the left child.
    /// # Errors
    /// Returns `QueryPlannerError`:
    /// - children are inconsistent relational nodes
    /// - column names don't exist
    pub fn add_row_from_left_branch(
        &mut self,
        left: usize,
        right: usize,
        col_names: &[&str],
    ) -> Result<usize, QueryPlannerError> {
        let list = self.new_columns(&[left, right], true, &[0], col_names, false, true)?;
        Ok(self.nodes.add_row(list, None))
    }

    /// Project columns from the join's right branch.
    ///
    /// New columns don't have aliases. If column names are empty,
    /// copy all the columns from the right child.
    /// # Errors
    /// Returns `QueryPlannerError`:
    /// - children are inconsistent relational nodes
    /// - column names don't exist
    pub fn add_row_from_right_branch(
        &mut self,
        left: usize,
        right: usize,
        col_names: &[&str],
    ) -> Result<usize, QueryPlannerError> {
        let list = self.new_columns(&[left, right], true, &[1], col_names, false, true)?;
        Ok(self.nodes.add_row(list, None))
    }

    /// A relational node pointed by the reference.
    ///
    /// # Errors
    /// - reference is invalid
    pub fn get_relational_from_reference_node(
        &self,
        ref_id: usize,
    ) -> Result<&usize, QueryPlannerError> {
        if let Node::Expression(Expression::Reference {
            targets, parent, ..
        }) = self.get_node(ref_id)?
        {
            let referred_rel_id = parent.as_ref().ok_or_else(|| {
                QueryPlannerError::CustomError("Reference node has no parent".into())
            });
            let rel = self.get_relation_node(*referred_rel_id.clone()?)?;
            if let Relational::Insert { .. } = rel {
                return referred_rel_id;
            } else if let Some(children) = rel.children() {
                match targets {
                    None => {
                        return Err(QueryPlannerError::CustomError(
                            "Reference node has no targets".into(),
                        ))
                    }
                    Some(positions) => match (positions.first(), positions.get(1)) {
                        (Some(first), None) => {
                            let child_id = children.get(*first).ok_or_else(|| {
                                QueryPlannerError::CustomError(
                                    "Relational node has no child at first position".into(),
                                )
                            })?;
                            return Ok(child_id);
                        }
                        _ => {
                            return Err(QueryPlannerError::CustomError(
                                "Reference expected to point exactly a single relational node"
                                    .into(),
                            ))
                        }
                    },
                }
            }
        }
        Err(QueryPlannerError::InvalidReference)
    }

    /// Get relational nodes referenced in the row.
    ///
    /// # Errors
    /// - node is not a row
    /// - row is invalid
    /// - `relational_map` is not initialized
    pub fn get_relational_from_row_nodes(
        &self,
        row_id: usize,
    ) -> Result<HashSet<usize, RandomState>, QueryPlannerError> {
        let mut rel_nodes: HashSet<usize, RandomState> = HashSet::with_hasher(RandomState::new());

        let row = self.get_expression_node(row_id)?;
        if let Expression::Row { .. } = row {
        } else {
            return Err(QueryPlannerError::CustomError("Node is not a row".into()));
        }
        let post_tree = DftPost::new(&row_id, |node| self.nodes.expr_iter(node, false));
        for (_, id) in post_tree {
            let reference = self.get_expression_node(*id)?;
            if let Expression::Reference {
                targets, parent, ..
            } = reference
            {
                let referred_rel_id = parent.ok_or(QueryPlannerError::CustomError(
                    "Reference node has no parent".into(),
                ))?;
                let rel = self.get_relation_node(referred_rel_id)?;
                if let Some(children) = rel.children() {
                    if let Some(positions) = targets {
                        rel_nodes.reserve(positions.len());
                        for pos in positions {
                            if let Some(child) = children.get(*pos) {
                                rel_nodes.insert(*child);
                            }
                        }
                    }
                }
            }
        }
        Ok(rel_nodes)
    }

    /// Check that the node is a boolean equality and its children are both rows.
    #[must_use]
    pub fn is_bool_eq_with_rows(&self, node_id: usize) -> bool {
        let node = match self.get_expression_node(node_id) {
            Ok(e) => e,
            Err(_) => return false,
        };
        if let Expression::Bool { left, op, right } = node {
            if *op != Bool::Eq {
                return false;
            }

            let left_node = match self.get_expression_node(*left) {
                Ok(e) => e,
                Err(_) => return false,
            };

            let right_node = match self.get_expression_node(*right) {
                Ok(e) => e,
                Err(_) => return false,
            };

            if left_node.is_row() && right_node.is_row() {
                return true;
            }
        }

        false
    }

    /// The node is a trivalent (boolean or NULL).
    ///
    /// # Errors
    /// - If node is not an expression.
    pub fn is_trivalent(&self, expr_id: usize) -> Result<bool, QueryPlannerError> {
        let expr = self.get_expression_node(expr_id)?;
        match expr {
            Expression::Bool { .. }
            | Expression::Unary { .. }
            | Expression::Constant {
                value: Value::Boolean(_) | Value::Null,
                ..
            } => return Ok(true),
            Expression::Row { list, .. } => {
                if let (Some(inner_id), None) = (list.first(), list.get(1)) {
                    return self.is_trivalent(*inner_id);
                }
            }
            _ => {}
        }
        Ok(false)
    }

    /// The node is a reference (or a row of a single reference column).
    ///
    /// # Errors
    /// - If node is not an expression.
    pub fn is_ref(&self, expr_id: usize) -> Result<bool, QueryPlannerError> {
        let expr = self.get_expression_node(expr_id)?;
        match expr {
            Expression::Reference { .. } => return Ok(true),
            Expression::Row { list, .. } => {
                if let (Some(inner_id), None) = (list.first(), list.get(1)) {
                    return self.is_ref(*inner_id);
                }
            }
            _ => {}
        }
        Ok(false)
    }

    /// Extract `Const` value from `Row` by index
    ///
    /// # Errors
    /// - node is not a row
    /// - row doesn't have const
    /// - const value is invalid
    #[allow(dead_code)]
    pub fn get_child_const_from_row(
        &self,
        row_id: usize,
        child_num: usize,
    ) -> Result<Value, QueryPlannerError> {
        let node = self.get_expression_node(row_id)?;
        if let Expression::Row { list, .. } = node {
            let const_node_id = list.get(child_num).ok_or_else(|| {
                QueryPlannerError::CustomError(format!("Child {} wasn't found", child_num))
            })?;

            let v = self
                .get_expression_node(*const_node_id)?
                .get_const_value()?;

            return Ok(v);
        }
        Err(QueryPlannerError::InvalidRow)
    }

    /// Replace parent for all references in the expression subtree of the current node.
    ///
    /// # Errors
    /// - node is invalid
    /// - node is not an expression
    pub fn replace_parent_in_subtree(
        &mut self,
        node_id: usize,
        from_id: Option<usize>,
        to_id: Option<usize>,
    ) -> Result<(), QueryPlannerError> {
        let mut references: Vec<usize> = Vec::new();
        let subtree = DftPost::new(&node_id, |node| self.nodes.expr_iter(node, false));
        for (_, id) in subtree {
            if let Node::Expression(Expression::Reference { .. }) = self.get_node(*id)? {
                references.push(*id);
            }
        }
        for id in references {
            let node = self.get_mut_expression_node(id)?;
            node.replace_parent_in_reference(from_id, to_id);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
