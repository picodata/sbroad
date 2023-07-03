//! Expression module.
//!
//! Expressions are the building blocks of the tuple.
//! They provide information about:
//! - what input tuple's columns where used to build our tuple
//! - the order of the columns (and we can get their types as well)
//! - distribution of the data in the tuple

use ahash::RandomState;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

use crate::errors::{Entity, SbroadError};
use crate::ir::aggregates::AggregateKind;
use crate::ir::operator::{Bool, Relational};
use crate::ir::relation::Type;

use super::distribution::Distribution;
use super::tree::traversal::{PostOrder, EXPR_CAPACITY};
use super::value::Value;
use super::{operator, Node, Nodes, Plan};

pub mod cast;
pub mod concat;
pub mod types;

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
    /// Binary expression returning row result.
    ///
    /// Example: `a + b > 42`, `a + b < c + 1`, `1 + 2 != 2 * 2`.
    Arithmetic {
        /// Left branch expression node index in the plan node arena.
        left: usize,
        /// Arithmetic operator.
        op: operator::Arithmetic,
        /// Right branch expression node index in the plan node arena.
        right: usize,
        /// Has expr parentheses or not. Important to keep this information
        /// because we can not add parentheses for all exprs: we parse query
        /// from the depth and from left to the right and not all arithmetic
        /// operations are associative, example:
        /// `(6 - 2) - 1 != 6 - (2 - 1)`, `(8 / 4) / 2 != 8 / (4 / 2))`.
        with_parentheses: bool,
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
    /// String concatenation expression.
    ///
    /// Example: `a || 'hello'`.
    Concat {
        /// Left expression node id.
        left: usize,
        /// Right expression node id.
        right: usize,
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
        /// Referred column type in the input tuple.
        col_type: Type,
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
        /// If this function is an aggregate function: whether it is marked DISTINCT or not
        is_distinct: bool,
        /// Function return type.
        func_type: Type,
    },
    /// Unary expression returning boolean result.
    Unary {
        /// Unary operator.
        op: operator::Unary,
        /// Child expression node index in the plan node arena.
        child: usize,
    },
    /// Argument of `count` aggregate in `count(*)` expression
    CountAsterisk,
}

#[allow(dead_code)]
impl Expression {
    /// Gets current row distribution.
    ///
    /// # Errors
    /// Returns `SbroadError` when the function is called on expression
    /// other than `Row` or a node doesn't know its distribution yet.
    pub fn distribution(&self) -> Result<&Distribution, SbroadError> {
        if let Expression::Row { distribution, .. } = self {
            let Some(dist) = distribution else {
                return Err(SbroadError::Invalid(
                    Entity::Distribution,
                    Some("distribution is uninitialized".into()),
                ))
            };
            return Ok(dist);
        }
        Err(SbroadError::Invalid(Entity::Expression, None))
    }

    /// Clone the row children list.
    ///
    /// # Errors
    /// - node isn't `Row`
    pub fn clone_row_list(&self) -> Result<Vec<usize>, SbroadError> {
        match self {
            Expression::Row { list, .. } => Ok(list.clone()),
            _ => Err(SbroadError::Invalid(
                Entity::Expression,
                Some("node isn't Row type".into()),
            )),
        }
    }

    #[must_use]
    pub fn is_aggregate_name(name: &str) -> bool {
        // currently we support only simple aggregates
        AggregateKind::new(name).is_some()
    }

    #[must_use]
    pub fn is_aggregate_fun(&self) -> bool {
        match self {
            Expression::StableFunction { name, .. } => Expression::is_aggregate_name(name),
            _ => false,
        }
    }

    /// Get a reference to the row children list.
    ///
    /// # Errors
    /// - node isn't `Row`
    pub fn get_row_list(&self) -> Result<&[usize], SbroadError> {
        match self {
            Expression::Row { ref list, .. } => Ok(list),
            _ => Err(SbroadError::Invalid(
                Entity::Expression,
                Some("node isn't Row type".into()),
            )),
        }
    }

    /// Get a mutable reference to the row children list.
    ///
    /// # Errors
    /// - node isn't `Row`
    pub fn get_row_list_mut(&mut self) -> Result<&mut Vec<usize>, SbroadError> {
        match self {
            Expression::Row { ref mut list, .. } => Ok(list),
            _ => Err(SbroadError::Invalid(
                Entity::Expression,
                Some("node isn't Row type".into()),
            )),
        }
    }

    /// Gets alias node name.
    ///
    /// # Errors
    /// - node isn't `Alias`
    pub fn get_alias_name(&self) -> Result<&str, SbroadError> {
        match self {
            Expression::Alias { name, .. } => Ok(name.as_str()),
            _ => Err(SbroadError::Invalid(
                Entity::Node,
                Some("node is not Alias type".into()),
            )),
        }
    }

    /// Checks for distribution determination
    ///
    /// # Errors
    /// - distribution isn't set
    pub fn has_unknown_distribution(&self) -> Result<bool, SbroadError> {
        let d = self.distribution()?;
        Ok(d.is_unknown())
    }

    /// Gets relational node id containing the reference.
    ///
    /// # Errors
    /// - node isn't reference type
    /// - reference doesn't have a parent
    pub fn get_parent(&self) -> Result<usize, SbroadError> {
        if let Expression::Reference { parent, .. } = self {
            return parent.ok_or_else(|| {
                SbroadError::Invalid(Entity::Expression, Some("Reference has no parent".into()))
            });
        }
        Err(SbroadError::Invalid(
            Entity::Expression,
            Some("node is not Reference type".into()),
        ))
    }

    /// The node is a row expression.
    #[must_use]
    pub fn is_row(&self) -> bool {
        matches!(self, Expression::Row { .. })
    }
    #[must_use]
    pub fn is_arithmetic(&self) -> bool {
        matches!(self, Expression::Arithmetic { .. })
    }

    /// Replaces parent in the reference node with the new one.
    pub fn replace_parent_in_reference(&mut self, from_id: Option<usize>, to_id: Option<usize>) {
        if let Expression::Reference { parent, .. } = self {
            if *parent == from_id {
                *parent = to_id;
            }
        }
    }

    /// Flushes parent in the reference node.
    pub fn flush_parent_in_reference(&mut self) {
        if let Expression::Reference { parent, .. } = self {
            *parent = None;
        }
    }
}

impl Nodes {
    /// Adds alias node.
    ///
    /// # Errors
    /// - child node is invalid
    /// - name is empty
    pub fn add_alias(&mut self, name: &str, child: usize) -> Result<usize, SbroadError> {
        self.arena.get(child).ok_or_else(|| {
            SbroadError::NotFound(Entity::Node, format!("from arena with index {child}"))
        })?;
        if name.is_empty() {
            return Err(SbroadError::Invalid(
                Entity::Plan,
                Some(String::from("name is empty")),
            ));
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
    ) -> Result<usize, SbroadError> {
        self.arena.get(left).ok_or_else(|| {
            SbroadError::NotFound(
                Entity::Node,
                format!("(left child of boolean node) from arena with index {left}"),
            )
        })?;
        self.arena.get(right).ok_or_else(|| {
            SbroadError::NotFound(
                Entity::Node,
                format!("(right child of boolean node) from arena with index {right}"),
            )
        })?;
        Ok(self.push(Node::Expression(Expression::Bool { left, op, right })))
    }

    /// Adds arithmetic node.
    ///
    /// # Errors
    /// - when left or right nodes are invalid
    pub fn add_arithmetic_node(
        &mut self,
        left: usize,
        op: operator::Arithmetic,
        right: usize,
        with_parentheses: bool,
    ) -> Result<usize, SbroadError> {
        self.arena.get(left).ok_or_else(|| {
            SbroadError::NotFound(
                Entity::Node,
                format!("(left child of Arithmetic node) from arena with index {left}"),
            )
        })?;
        self.arena.get(right).ok_or_else(|| {
            SbroadError::NotFound(
                Entity::Node,
                format!("(right child of Arithmetic node) from arena with index {right}"),
            )
        })?;
        Ok(self.push(Node::Expression(Expression::Arithmetic {
            left,
            op,
            right,
            with_parentheses,
        })))
    }

    /// Set `with_parentheses` for arithmetic node.
    ///
    /// # Errors
    /// - when left or right nodes are invalid
    pub fn set_arithmetic_node_parentheses(
        &mut self,
        node_id: usize,
        parentheses_to_set: bool,
    ) -> Result<(), SbroadError> {
        let arith_node = self.arena.get_mut(node_id).ok_or_else(|| {
            SbroadError::NotFound(
                Entity::Node,
                format!("(Arithmetic node) from arena with index {node_id}"),
            )
        })?;

        if let Node::Expression(Expression::Arithmetic {
            with_parentheses, ..
        }) = arith_node
        {
            *with_parentheses = parentheses_to_set;
            Ok(())
        } else {
            Err(SbroadError::Invalid(
                Entity::Node,
                Some(format!("expected Arithmetic with index {node_id}")),
            ))
        }
    }

    /// Adds reference node.
    pub fn add_ref(
        &mut self,
        parent: Option<usize>,
        targets: Option<Vec<usize>>,
        position: usize,
        col_type: Type,
    ) -> usize {
        let r = Expression::Reference {
            parent,
            targets,
            position,
            col_type,
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
    ) -> Result<usize, SbroadError> {
        let mut names: HashSet<String> = HashSet::with_capacity(list.len());

        for node_id in &list {
            if let Some(Node::Expression(expr)) = self.arena.get(*node_id) {
                if let Expression::Alias { name, .. } = expr {
                    if !names.insert(String::from(name)) {
                        return Err(SbroadError::DuplicatedValue(format!(
                            "row can't be added because `{name}` already has an alias",
                        )));
                    }
                }
            } else {
                return Err(SbroadError::NotFound(
                    Entity::Node,
                    format!("with index {node_id}"),
                ));
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
    ) -> Result<usize, SbroadError> {
        self.arena.get(child).ok_or_else(|| {
            SbroadError::NotFound(Entity::Node, format!("from arena with index {child}"))
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
    /// Returns `SbroadError`:
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
    ) -> Result<Vec<usize>, SbroadError> {
        // We can pass two target children nodes only in case
        // of `UnionAll` and `InnerJoin`.
        // - For the `UnionAll` operator we need only the first
        // child to get correct column names for a new tuple
        // (the second child aliases would be shadowed). But each reference should point
        // to both children to give us additional information
        // during transformations.
        if (targets.len() > 2) || targets.is_empty() {
            return Err(SbroadError::UnexpectedNumberOfValues(format!(
                "invalid target length: {}",
                targets.len()
            )));
        }

        if let Some(max) = targets.iter().max() {
            if *max >= children.len() {
                return Err(SbroadError::UnexpectedNumberOfValues(format!(
                    "invalid children length: {}",
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
                    return Err(SbroadError::NotFound(
                        Entity::Node,
                        "(child) pointed by target index".into(),
                    ));
                };
                let child_node: usize = if let Some(child) = children.get(target_child) {
                    *child
                } else {
                    return Err(SbroadError::NotFound(
                        Entity::Node,
                        format!("pointed by target child {target_child}"),
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
                                    return Err(SbroadError::UnexpectedNumberOfValues(format!(
                                        "Selection node has invalid children: {sel_child_ids:?}"
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
                                SbroadError::NotFound(
                                    Entity::Table,
                                    format!("{relation} among the plan relations"),
                                )
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
                    return Err(SbroadError::Invalid(
                        Entity::Expression,
                        Some("child node is not a row".into()),
                    ));
                };
                result.reserve(child_row_list.len());
                for (pos, alias_node) in child_row_list {
                    let expr = self.get_expression_node(alias_node)?;
                    let name: String = if let Expression::Alias { ref name, .. } = expr {
                        String::from(name)
                    } else {
                        return Err(SbroadError::Invalid(
                            Entity::Expression,
                            Some(format!("expression {expr:?} is not an Alias")),
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
                    let col_type = expr.calculate_type(self)?;
                    // Adds new references and aliases to arena (if we need them).
                    let r_id = self.nodes.add_ref(None, Some(new_targets), pos, col_type);
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
            return Err(SbroadError::UnexpectedNumberOfValues(
                "Target is empty".into(),
            ));
        };
        let child_node: usize = if let Some(child) = children.get(target_child) {
            *child
        } else {
            return Err(SbroadError::NotFound(
                Entity::Node,
                "pointed by the target".into(),
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
        // Map of { column name (aliased) from child output -> its index in output }
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
                    return Err(SbroadError::DuplicatedValue(format!(
                        "Duplicate column name {name} at position {pos}"
                    )));
                }
            }
            map
        } else {
            return Err(SbroadError::Invalid(
                Entity::Node,
                Some("Relational output tuple is not a row".into()),
            ));
        };

        // Vec of { `map` key, targets, `map` value }
        let mut refs: Vec<(&str, Vec<usize>, usize)> = Vec::with_capacity(col_names.len());
        let all_found = col_names.iter().all(|col| {
            map.get(col).map_or(false, |pos| {
                refs.push((col, targets.to_vec(), *pos));
                true
            })
        });
        if !all_found {
            return Err(SbroadError::NotFound(
                Entity::Column,
                format!("with name {}", col_names.join(", ")),
            ));
        }

        let columns = output.clone_row_list()?;
        for (col, new_targets, pos) in refs {
            let col_id = *columns.get(pos).ok_or_else(|| {
                SbroadError::NotFound(
                    Entity::Column,
                    format!("at position {pos} in the child output"),
                )
            })?;
            let col_expr = self.get_expression_node(col_id)?;
            let col_type = col_expr.calculate_type(self)?;
            let r_id = self.nodes.add_ref(None, Some(new_targets), pos, col_type);
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
    /// Returns `SbroadError`:
    /// - child is an inconsistent relational node
    /// - column names don't exist
    pub fn add_row_for_output(
        &mut self,
        child: usize,
        col_names: &[&str],
        need_sharding_column: bool,
    ) -> Result<usize, SbroadError> {
        let list =
            self.new_columns(&[child], false, &[0], col_names, true, need_sharding_column)?;
        self.nodes.add_row_of_aliases(list, None)
    }

    /// New output row for union node.
    ///
    /// # Errors
    /// Returns `SbroadError`:
    /// - children are inconsistent relational nodes
    pub fn add_row_for_union_except(
        &mut self,
        left: usize,
        right: usize,
    ) -> Result<usize, SbroadError> {
        let list = self.new_columns(&[left, right], false, &[0, 1], &[], true, true)?;
        self.nodes.add_row_of_aliases(list, None)
    }

    /// New output row for join node.
    ///
    /// Contains all the columns from left and right children.
    ///
    /// # Errors
    /// Returns `SbroadError`:
    /// - children are inconsistent relational nodes
    pub fn add_row_for_join(&mut self, left: usize, right: usize) -> Result<usize, SbroadError> {
        let list = self.new_columns(&[left, right], true, &[0, 1], &[], true, true)?;
        self.nodes.add_row_of_aliases(list, None)
    }

    /// Project columns from the child node.
    ///
    /// New columns don't have aliases. If column names are empty,
    /// copy all the columns from the child.
    /// # Errors
    /// Returns `SbroadError`:
    /// - child is an inconsistent relational node
    /// - column names don't exist
    pub fn add_row_from_child(
        &mut self,
        child: usize,
        col_names: &[&str],
    ) -> Result<usize, SbroadError> {
        let list = self.new_columns(&[child], false, &[0], col_names, false, true)?;
        Ok(self.nodes.add_row(list, None))
    }

    /// Project columns from the child subquery node.
    ///
    /// New columns don't have aliases. If column names are empty,
    /// copy all the columns from the child.
    /// # Errors
    /// Returns `SbroadError`:
    /// - children nodes are not a relational
    /// - column names don't exist
    pub fn add_row_from_sub_query(
        &mut self,
        children: &[usize],
        children_pos: usize,
        col_names: &[&str],
    ) -> Result<usize, SbroadError> {
        let list = self.new_columns(children, false, &[children_pos], col_names, false, true)?;
        Ok(self.nodes.add_row(list, None))
    }

    /// Project columns from the join's left branch.
    ///
    /// New columns don't have aliases. If column names are empty,
    /// copy all the columns from the left child.
    /// # Errors
    /// Returns `SbroadError`:
    /// - children are inconsistent relational nodes
    /// - column names don't exist
    pub fn add_row_from_left_branch(
        &mut self,
        left: usize,
        right: usize,
        col_names: &[&str],
    ) -> Result<usize, SbroadError> {
        let list = self.new_columns(&[left, right], true, &[0], col_names, false, true)?;
        Ok(self.nodes.add_row(list, None))
    }

    /// Project columns from the join's right branch.
    ///
    /// New columns don't have aliases. If column names are empty,
    /// copy all the columns from the right child.
    /// # Errors
    /// Returns `SbroadError`:
    /// - children are inconsistent relational nodes
    /// - column names don't exist
    pub fn add_row_from_right_branch(
        &mut self,
        left: usize,
        right: usize,
        col_names: &[&str],
    ) -> Result<usize, SbroadError> {
        let list = self.new_columns(&[left, right], true, &[1], col_names, false, true)?;
        Ok(self.nodes.add_row(list, None))
    }

    /// A relational node pointed by the reference.
    /// In a case of a reference in the Motion node
    /// within a dispatched IR to the storage, returns
    /// the Motion node itself.
    ///
    /// # Errors
    /// - reference is invalid
    pub fn get_relational_from_reference_node(&self, ref_id: usize) -> Result<&usize, SbroadError> {
        if let Node::Expression(Expression::Reference {
            targets, parent, ..
        }) = self.get_node(ref_id)?
        {
            let Some(referred_rel_id) = parent else {
                return Err(SbroadError::NotFound(
                    Entity::Node,
                    format!("that is Reference ({ref_id}) parent"),
                ));
            };
            let rel = self.get_relation_node(*referred_rel_id)?;
            if let Relational::Insert { .. } = rel {
                return Ok(referred_rel_id);
            } else if let Some(children) = rel.children() {
                match targets {
                    None => {
                        return Err(SbroadError::UnexpectedNumberOfValues(
                            "Reference node has no targets".into(),
                        ))
                    }
                    Some(positions) => match (positions.first(), positions.get(1)) {
                        (Some(first), None) => {
                            if let Some(child_id) = children.get(*first) {
                                return Ok(child_id);
                            }
                            // When we dispatch IR to the storage, we truncate the
                            // subtree below the Motion node. So, the references in
                            // the Motion's output row are broken. We treat them in
                            // a special way: we return the Motion node itself. Be
                            // aware of the circular references in the tree!
                            if let Relational::Motion { .. } = rel {
                                return Ok(referred_rel_id);
                            }
                            return Err(SbroadError::UnexpectedNumberOfValues(format!(
                                "Relational node {rel:?} has no children"
                            )));
                        }
                        _ => {
                            return Err(SbroadError::UnexpectedNumberOfValues(
                                "Reference expected to point exactly a single relational node"
                                    .into(),
                            ))
                        }
                    },
                }
            }
        }
        Err(SbroadError::Invalid(Entity::Expression, None))
    }

    /// Get relational nodes referenced in the row.
    ///
    /// # Errors
    /// - node is not a row
    /// - row is invalid
    /// - `relational_map` is not initialized
    pub fn get_relational_nodes_from_row(
        &self,
        row_id: usize,
    ) -> Result<HashSet<usize, RandomState>, SbroadError> {
        let row = self.get_expression_node(row_id)?;
        let capacity = if let Expression::Row { list, .. } = row {
            list.len() * 2
        } else {
            return Err(SbroadError::Invalid(
                Entity::Node,
                Some("Node is not a row".into()),
            ));
        };
        let mut post_tree =
            PostOrder::with_capacity(|node| self.nodes.expr_iter(node, false), capacity);
        post_tree.populate_nodes(row_id);
        let nodes = post_tree.take_nodes();
        let mut rel_nodes: HashSet<usize, RandomState> =
            HashSet::with_capacity_and_hasher(nodes.len(), RandomState::new());
        for (_, id) in nodes {
            let reference = self.get_expression_node(id)?;
            if let Expression::Reference {
                targets, parent, ..
            } = reference
            {
                let referred_rel_id = parent.ok_or(SbroadError::NotFound(
                    Entity::Node,
                    format!("that is Reference ({id}) parent"),
                ))?;
                let rel = self.get_relation_node(referred_rel_id)?;
                if let Some(children) = rel.children() {
                    if let Some(positions) = targets {
                        for pos in positions {
                            if let Some(child) = children.get(*pos) {
                                rel_nodes.insert(*child);
                            }
                        }
                    }
                }
            }
        }
        rel_nodes.shrink_to_fit();
        Ok(rel_nodes)
    }

    /// Check that the node is a boolean equality and its children are both rows.
    #[must_use]
    pub fn is_bool_eq_with_rows(&self, node_id: usize) -> bool {
        let Ok(node) = self.get_expression_node(node_id) else {
            return false
        };
        if let Expression::Bool { left, op, right } = node {
            if *op != Bool::Eq {
                return false;
            }

            let Ok(left_node) = self.get_expression_node(*left) else {
                return false
            };

            let Ok(right_node) = self.get_expression_node(*right) else {
                return false
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
    pub fn is_trivalent(&self, expr_id: usize) -> Result<bool, SbroadError> {
        let expr = self.get_expression_node(expr_id)?;
        match expr {
            Expression::Bool { .. }
            | Expression::Arithmetic { .. }
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
    pub fn is_ref(&self, expr_id: usize) -> Result<bool, SbroadError> {
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
    ) -> Result<Value, SbroadError> {
        let node = self.get_expression_node(row_id)?;
        if let Expression::Row { list, .. } = node {
            let const_node_id = list
                .get(child_num)
                .ok_or_else(|| SbroadError::NotFound(Entity::Node, format!("{child_num}")))?;

            let v = self.get_expression_node(*const_node_id)?.as_const_value()?;

            return Ok(v);
        }
        Err(SbroadError::Invalid(
            Entity::Node,
            Some("node is not Row type".into()),
        ))
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
    ) -> Result<(), SbroadError> {
        let mut references: Vec<usize> = Vec::new();
        let mut subtree =
            PostOrder::with_capacity(|node| self.nodes.expr_iter(node, false), EXPR_CAPACITY);
        for (_, id) in subtree.iter(node_id) {
            if let Node::Expression(Expression::Reference { .. }) = self.get_node(id)? {
                references.push(id);
            }
        }
        for id in references {
            let node = self.get_mut_expression_node(id)?;
            node.replace_parent_in_reference(from_id, to_id);
        }
        Ok(())
    }

    /// Flush parent to `None` for all references in the expression subtree of the current node.
    ///
    /// # Errors
    /// - node is invalid
    /// - node is not an expression
    pub fn flush_parent_in_subtree(&mut self, node_id: usize) -> Result<(), SbroadError> {
        let mut references: Vec<usize> = Vec::new();
        let mut subtree =
            PostOrder::with_capacity(|node| self.nodes.expr_iter(node, false), EXPR_CAPACITY);
        for (_, id) in subtree.iter(node_id) {
            if let Node::Expression(Expression::Reference { .. }) = self.get_node(id)? {
                references.push(id);
            }
        }
        for id in references {
            let node = self.get_mut_expression_node(id)?;
            node.flush_parent_in_reference();
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
