use crate::errors::QueryPlannerError;
use crate::ir::expression::Expression;
use crate::ir::operator::Relational;
use crate::ir::{Node, Plan};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use traversal::DftPost;

/// Payload of the syntax tree node.
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum SyntaxData {
    /// "... as alias_name"
    Alias(String),
    /// ")"
    CloseParenthesis,
    /// ","
    Comma,
    /// "... on"
    Condition,
    /// "("
    OpenParenthesis,
    /// plan node id
    PlanId(usize),
}

/// A syntax tree node.
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct SyntaxNode {
    /// Payload
    pub(crate) data: SyntaxData,
    /// Pointer to the left node in the syntax tree. We keep it separate
    /// from "other" right nodes as we sometimes need it to be None, while
    /// other nodes have values (all children should be on the right of the
    /// current node in a case of in-order traversal - row or sub-query as
    /// an example).
    pub(crate) left: Option<usize>,
    /// Pointers to the right children.
    pub(crate) right: Vec<usize>,
}

impl SyntaxNode {
    fn new_alias(name: &str) -> Self {
        SyntaxNode {
            data: SyntaxData::Alias(name.into()),
            left: None,
            right: Vec::new(),
        }
    }

    fn new_close() -> Self {
        SyntaxNode {
            data: SyntaxData::CloseParenthesis,
            left: None,
            right: Vec::new(),
        }
    }

    fn new_comma() -> Self {
        SyntaxNode {
            data: SyntaxData::Comma,
            left: None,
            right: Vec::new(),
        }
    }

    fn new_condition() -> Self {
        SyntaxNode {
            data: SyntaxData::Condition,
            left: None,
            right: Vec::new(),
        }
    }

    fn new_open() -> Self {
        SyntaxNode {
            data: SyntaxData::OpenParenthesis,
            left: None,
            right: Vec::new(),
        }
    }

    fn new_pointer(id: usize, left: Option<usize>, right: &[usize]) -> Self {
        SyntaxNode {
            data: SyntaxData::PlanId(id),
            left,
            right: right.into(),
        }
    }
}

/// Storage for the syntax nodes.
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct SyntaxNodes {
    pub(crate) arena: Vec<SyntaxNode>,
    map: HashMap<usize, usize>,
}

impl SyntaxNodes {
    /// Add sub-query syntax node
    ///
    /// # Errors
    /// - sub-query in plan tree is invalid
    fn add_sq(&mut self, rel: &Relational, id: usize) -> Result<usize, QueryPlannerError> {
        if let Relational::ScanSubQuery {
            children, alias, ..
        } = rel
        {
            let right_id = *children.get(0).ok_or_else(|| {
                QueryPlannerError::CustomError("Sub-query has no children.".into())
            })?;
            let mut children: Vec<usize> = vec![
                self.push_syntax_node(SyntaxNode::new_open()),
                self.get_syntax_node_id(right_id)?,
                self.push_syntax_node(SyntaxNode::new_close()),
            ];
            if let Some(name) = alias {
                children.push(self.push_syntax_node(SyntaxNode::new_alias(name)));
            }
            let sn = SyntaxNode::new_pointer(id, None, &children);
            Ok(self.push_syntax_node(sn))
        } else {
            Err(QueryPlannerError::CustomError(
                "Current node is not a sub-query".into(),
            ))
        }
    }

    /// Construct syntax nodes from the YAML file.
    ///
    /// # Errors
    /// Returns `QueryPlannerError` when the YAML nodes arena is invalid.
    #[allow(dead_code)]
    pub fn from_yaml(s: &str) -> Result<Self, QueryPlannerError> {
        let nodes: SyntaxNodes = match serde_yaml::from_str(s) {
            Ok(p) => p,
            Err(_) => return Err(QueryPlannerError::Serialization),
        };
        Ok(nodes)
    }

    /// Get a syntax node from arena
    ///
    /// # Errors
    /// - current node is invalid (doesn't exist in arena)
    pub fn get_syntax_node(&self, id: usize) -> Result<&SyntaxNode, QueryPlannerError> {
        self.arena.get(id).ok_or(QueryPlannerError::InvalidNode)
    }

    /// Get a mutable syntax node from arena
    ///
    /// # Errors
    /// - current node is invalid (doesn't exist in arena)
    pub fn get_mut_syntax_node(&mut self, id: usize) -> Result<&mut SyntaxNode, QueryPlannerError> {
        self.arena.get_mut(id).ok_or(QueryPlannerError::InvalidNode)
    }

    /// Get syntax node id by the plan node's one
    ///
    /// # Errors
    /// - nothing was found
    fn get_syntax_node_id(&self, plan_id: usize) -> Result<usize, QueryPlannerError> {
        self.map.get(&plan_id).copied().ok_or_else(|| {
            QueryPlannerError::CustomError("Current plan node is absent in the map".into())
        })
    }

    /// Push a new syntax node to arena
    pub fn push_syntax_node(&mut self, node: SyntaxNode) -> usize {
        let id = self.next_id();
        if let SyntaxData::PlanId(plan_id) = node.data {
            self.map.insert(plan_id, id);
        }
        self.arena.push(node);
        id
    }

    /// Get next node id
    pub fn next_id(&self) -> usize {
        self.arena.len()
    }

    /// Constructor
    pub fn new() -> Self {
        SyntaxNodes {
            arena: Vec::new(),
            map: HashMap::new(),
        }
    }
}

/// Helper for `Selection` structure.
#[derive(Debug)]
enum Branch {
    Left,
    Right,
}

/// Keeps syntax node chain of the `SELECT` command:
/// projection, selection, scan and the upper node over
/// them all (parent).
#[derive(Debug)]
struct Select {
    /// The node over projection
    parent: Option<usize>,
    /// Parent's branch where projection was found
    branch: Option<Branch>,
    /// Projection syntax node
    proj: usize,
    /// Scan syntax node
    scan: usize,
    /// Selection syntax node
    selection: Option<usize>,
}

impl Select {
    fn new(
        sp: &SyntaxPlan,
        parent: Option<usize>,
        branch: Option<Branch>,
        id: usize,
    ) -> Result<Option<Select>, QueryPlannerError> {
        let sn = sp.nodes.get_syntax_node(id)?;
        // Check if the node is a projection
        if let Some(Node::Relational(Relational::Projection { .. })) = sp.get_plan_node(&sn.data)? {
        } else {
            return Ok(None);
        }
        // Get the left node
        let left_id = match sn.left {
            Some(id) => id,
            None => {
                return Err(QueryPlannerError::CustomError(
                    "Projection can't be a leaf node".into(),
                ))
            }
        };
        let syntax_node_left = sp.nodes.get_syntax_node(left_id)?;
        let plan_node_left = sp
            .get_plan_node(&syntax_node_left.data)?
            .ok_or(QueryPlannerError::InvalidNode)?;
        match plan_node_left {
            // Expecting projection over selection and scan
            Node::Relational(Relational::Selection { .. }) => {
                // Get the next left node
                let next_left_id = match syntax_node_left.left {
                    Some(id) => id,
                    None => {
                        return Err(QueryPlannerError::CustomError(
                            "Selection can't be a leaf node".into(),
                        ))
                    }
                };
                // We expect that the next left node is a scan
                let syntax_node_next_left = sp.nodes.get_syntax_node(next_left_id)?;
                let plan_node_next_left = sp
                    .get_plan_node(&syntax_node_next_left.data)?
                    .ok_or(QueryPlannerError::InvalidNode)?;
                if let Node::Relational(
                    Relational::ScanRelation { .. } | Relational::ScanSubQuery { .. },
                ) = plan_node_next_left
                {
                    Ok(Some(Select {
                        parent,
                        branch,
                        proj: id,
                        scan: next_left_id,
                        selection: Some(left_id),
                    }))
                } else {
                    Err(QueryPlannerError::InvalidPlan)
                }
            }
            // Expecting projection over scan
            Node::Relational(Relational::ScanRelation { .. } | Relational::ScanSubQuery { .. }) => {
                Ok(Some(Select {
                    parent,
                    branch,
                    proj: id,
                    scan: left_id,
                    selection: None,
                }))
            }
            _ => Err(QueryPlannerError::InvalidPlan),
        }
    }
}

/// A wrapper over original plan tree.
/// We can modify it as we wish without any influence
/// on the original plan tree.
pub struct SyntaxPlan<'p> {
    pub(crate) nodes: SyntaxNodes,
    top: Option<usize>,
    plan: &'p Plan,
}

#[allow(dead_code)]
impl<'p> SyntaxPlan<'p> {
    #[allow(clippy::too_many_lines)]
    pub fn add_plan_node(&mut self, id: usize) -> Result<usize, QueryPlannerError> {
        match self.plan.get_node(id)? {
            Node::Relational(rel) => match rel {
                Relational::InnerJoin {
                    children,
                    condition,
                    ..
                } => {
                    let left_id = *children.get(0).ok_or_else(|| {
                        QueryPlannerError::CustomError(
                            "Inner join doesn't have a left child.".into(),
                        )
                    })?;
                    let right_id = *children.get(1).ok_or_else(|| {
                        QueryPlannerError::CustomError(
                            "Inner join doesn't have a right child.".into(),
                        )
                    })?;

                    let sn = SyntaxNode::new_pointer(
                        id,
                        Some(self.nodes.get_syntax_node_id(left_id)?),
                        &[
                            self.nodes.get_syntax_node_id(right_id)?,
                            self.nodes.push_syntax_node(SyntaxNode::new_condition()),
                            self.nodes.get_syntax_node_id(*condition)?,
                        ],
                    );
                    Ok(self.nodes.push_syntax_node(sn))
                }
                Relational::Motion { .. } => Err(QueryPlannerError::CustomError(
                    "Motion nodes can't be serialized to SQL".into(),
                )),
                Relational::Projection {
                    children, output, ..
                } => {
                    let left_id = *children.get(0).ok_or_else(|| {
                        QueryPlannerError::CustomError("Projection has no children.".into())
                    })?;
                    // We don't need the row node itself, only its children.
                    // Otherwise we'll produce redundant parentheses between
                    // `SELECT` and `FROM`.
                    let expr = self.plan.get_expression_node(*output)?;
                    if let Expression::Row { list, .. } = expr {
                        let mut nodes: Vec<usize> = Vec::new();
                        if let Some((last, elements)) = list.split_last() {
                            for elem in elements {
                                nodes.push(self.nodes.get_syntax_node_id(*elem)?);
                                nodes.push(self.nodes.push_syntax_node(SyntaxNode::new_comma()));
                            }
                            nodes.push(self.nodes.get_syntax_node_id(*last)?);
                            let sn = SyntaxNode::new_pointer(
                                id,
                                Some(self.nodes.get_syntax_node_id(left_id)?),
                                &nodes,
                            );
                            return Ok(self.nodes.push_syntax_node(sn));
                        }
                    }
                    Err(QueryPlannerError::InvalidPlan)
                }
                Relational::ScanSubQuery { .. } => self.nodes.add_sq(rel, id),
                Relational::Selection {
                    children, filter, ..
                } => {
                    let left_id = *children.get(0).ok_or_else(|| {
                        QueryPlannerError::CustomError("Selection has no children.".into())
                    })?;
                    let sn = SyntaxNode::new_pointer(
                        id,
                        Some(self.nodes.get_syntax_node_id(left_id)?),
                        &[self.nodes.get_syntax_node_id(*filter)?],
                    );
                    Ok(self.nodes.push_syntax_node(sn))
                }
                Relational::UnionAll { children, .. } => {
                    let left_id = *children.get(0).ok_or_else(|| {
                        QueryPlannerError::CustomError(
                            "Union all doesn't have a left child.".into(),
                        )
                    })?;
                    let right_id = *children.get(1).ok_or_else(|| {
                        QueryPlannerError::CustomError(
                            "Union all doesn't have a right child.".into(),
                        )
                    })?;
                    let sn = SyntaxNode::new_pointer(
                        id,
                        Some(self.nodes.get_syntax_node_id(left_id)?),
                        &[self.nodes.get_syntax_node_id(right_id)?],
                    );
                    Ok(self.nodes.push_syntax_node(sn))
                }
                Relational::ScanRelation { .. } => {
                    let sn = SyntaxNode::new_pointer(id, None, &[]);
                    Ok(self.nodes.push_syntax_node(sn))
                }
            },
            Node::Expression(expr) => match expr {
                Expression::Constant { .. } | Expression::Reference { .. } => {
                    let sn = SyntaxNode::new_pointer(id, None, &[]);
                    Ok(self.nodes.push_syntax_node(sn))
                }
                Expression::Alias { child, name, .. } => {
                    let sn = SyntaxNode::new_pointer(
                        id,
                        Some(self.nodes.get_syntax_node_id(*child)?),
                        &[self.nodes.push_syntax_node(SyntaxNode::new_alias(name))],
                    );
                    Ok(self.nodes.push_syntax_node(sn))
                }
                Expression::Row { list, .. } => {
                    if let Some(sq_id) = self.plan.get_sub_query_from_row_node(id)? {
                        // Replace current row with the referred sub-query
                        // (except the case when sub-query is located in the FROM clause).
                        if !self.plan.is_first_child(sq_id)? {
                            let rel = self.plan.get_relation_node(sq_id)?;
                            return self.nodes.add_sq(rel, id);
                        }
                    }
                    let mut nodes: Vec<usize> =
                        vec![self.nodes.push_syntax_node(SyntaxNode::new_open())];
                    if let Some((last, elements)) = list.split_last() {
                        for elem in elements {
                            nodes.push(self.nodes.get_syntax_node_id(*elem)?);
                            nodes.push(self.nodes.push_syntax_node(SyntaxNode::new_comma()));
                        }
                        nodes.push(self.nodes.get_syntax_node_id(*last)?);
                        nodes.push(self.nodes.push_syntax_node(SyntaxNode::new_close()));
                        let sn = SyntaxNode::new_pointer(id, None, &nodes);
                        return Ok(self.nodes.push_syntax_node(sn));
                    }
                    Err(QueryPlannerError::InvalidRow)
                }
                Expression::Bool { left, right, .. } => {
                    let sn = SyntaxNode::new_pointer(
                        id,
                        Some(self.nodes.get_syntax_node_id(*left)?),
                        &[self.nodes.get_syntax_node_id(*right)?],
                    );
                    Ok(self.nodes.push_syntax_node(sn))
                }
            },
        }
    }

    /// Get the plan node from the syntax tree node.
    ///
    /// # Errors
    /// - plan node is invalid
    pub fn get_plan_node(&self, data: &SyntaxData) -> Result<Option<&Node>, QueryPlannerError> {
        if let SyntaxData::PlanId(id) = data {
            Ok(Some(self.plan.get_node(*id)?))
        } else {
            Ok(None)
        }
    }

    /// Set top of the tree.
    ///
    /// # Errors
    /// - top is invalid node
    pub fn set_top(&mut self, top: usize) -> Result<(), QueryPlannerError> {
        self.nodes.get_syntax_node(top)?;
        self.top = Some(top);
        Ok(())
    }

    /// Get the top of the syntax tree.
    ///
    /// # Errors
    /// - top is not set
    /// - top is not a valid node
    pub fn get_top(&self) -> Result<usize, QueryPlannerError> {
        if let Some(top) = self.top {
            self.nodes.get_syntax_node(top)?;
            Ok(top)
        } else {
            Err(QueryPlannerError::CustomError(
                "Syntax tree has an invalid top.".into(),
            ))
        }
    }

    /// Gather all projections with auxiliary nodes (scan, selection, parent)
    /// among the syntax tree.
    ///
    /// # Errors
    /// - got unexpected nodes under projection
    fn gather_selects(&self) -> Result<Option<Vec<Select>>, QueryPlannerError> {
        let mut selects: Vec<Select> = Vec::new();
        let top = self.get_top()?;
        for (pos, node) in self.nodes.arena.iter().enumerate() {
            if pos == top {
                let select = Select::new(self, None, None, pos)?;
                if let Some(s) = select {
                    selects.push(s);
                }
            }
            if let Some(left) = node.left {
                let select = Select::new(self, Some(pos), Some(Branch::Left), left)?;
                if let Some(s) = select {
                    selects.push(s);
                }
            }
            for right in &node.right {
                let select = Select::new(self, Some(pos), Some(Branch::Right), *right)?;
                if let Some(s) = select {
                    selects.push(s);
                }
            }
        }

        if selects.is_empty() {
            Ok(None)
        } else {
            Ok(Some(selects))
        }
    }

    /// Move projection nodes under their scans
    ///
    /// # Errors
    /// - got unexpected nodes under some projection
    fn move_proj_under_scan(&mut self) -> Result<(), QueryPlannerError> {
        let selects = self.gather_selects()?;
        if let Some(selects) = selects {
            for select in &selects {
                self.reorder(select)?;
            }
        }
        Ok(())
    }

    fn empty(plan: &'p Plan) -> Self {
        SyntaxPlan {
            nodes: SyntaxNodes::new(),
            top: None,
            plan,
        }
    }

    pub fn new(plan: &'p Plan, top: usize) -> Result<Self, QueryPlannerError> {
        let mut sp = SyntaxPlan::empty(plan);

        // Wrap plan's nodes and preserve their ids.
        let dft_post = DftPost::new(&top, |node| plan.nodes.subtree_iter(node));
        for (_, id) in dft_post {
            // it works only for post-order traversal
            let sn_id = sp.add_plan_node(*id)?;
            if *id == top {
                sp.set_top(sn_id)?;
            }
        }
        sp.move_proj_under_scan()?;

        Ok(sp)
    }

    /// Reorder `SELECT` chain to:
    ///
    /// parent (if some) -branch-> selection (if some) -left-> scan -left-> projection
    ///
    /// # Errors
    /// - select nodes (parent, scan, projection, selection) are invalid
    fn reorder(&mut self, select: &Select) -> Result<(), QueryPlannerError> {
        // Move projection under scan.
        let mut proj = self.nodes.get_mut_syntax_node(select.proj)?;
        proj.left = None;
        let mut scan = self.nodes.get_mut_syntax_node(select.scan)?;
        scan.left = Some(select.proj);
        let mut top = select.scan;

        // Try to move scan under selection.
        if let Some(id) = select.selection {
            let mut selection = self.nodes.get_mut_syntax_node(id)?;
            selection.left = Some(top);
            top = id;
        }

        // Try to move selection (or scan if selection doesn't exist) under parent.
        if let Some(id) = select.parent {
            let mut parent = self.nodes.get_mut_syntax_node(id)?;
            match select.branch {
                Some(Branch::Left) => {
                    parent.left = Some(top);
                }
                Some(Branch::Right) => {
                    let mut found: bool = false;
                    for child in &mut parent.right {
                        if child == &select.proj {
                            *child = top;
                            found = true;
                        }
                    }
                    if !found {
                        return Err(QueryPlannerError::CustomError(
                            "Parent node doesn't contain projection in its right children".into(),
                        ));
                    }
                }
                None => {
                    return Err(QueryPlannerError::CustomError(
                        "Selection structure is in inconsistent state.".into(),
                    ))
                }
            }
        }

        // Update the syntax plan top if it was current projection
        if self.get_top()? == select.proj {
            if let Some(select_id) = select.selection {
                self.set_top(select_id)?;
            } else {
                self.set_top(select.scan)?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests;
