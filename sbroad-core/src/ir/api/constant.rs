use crate::errors::QueryPlannerError;
use crate::ir::expression::Expression;
use crate::ir::value::Value;
use crate::ir::{Node, Nodes, Plan};

impl Expression {
    /// Gets value from const node
    ///
    /// # Errors
    /// - node isn't constant type
    pub fn as_const_value(&self) -> Result<Value, QueryPlannerError> {
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
    pub fn as_const_value_ref(&self) -> Result<&Value, QueryPlannerError> {
        if let Expression::Constant { value } = self {
            return Ok(value);
        }

        Err(QueryPlannerError::CustomError(
            "Node isn't const type".into(),
        ))
    }

    /// The node is a constant expression.
    #[must_use]
    pub fn is_const(&self) -> bool {
        matches!(self, Expression::Constant { .. })
    }
}

impl Nodes {
    /// Adds constant node.
    pub fn add_const(&mut self, value: Value) -> usize {
        self.push(Node::Expression(Expression::Constant { value }))
    }
}

impl Plan {
    /// Add constant value to the plan.
    pub fn add_const(&mut self, v: Value) -> usize {
        self.nodes.add_const(v)
    }

    #[must_use]
    pub fn get_const_list(&self) -> Vec<usize> {
        self.nodes
            .arena
            .iter()
            .enumerate()
            .filter_map(|(id, node)| {
                if let Node::Expression(Expression::Constant { .. }) = node {
                    Some(id)
                } else {
                    None
                }
            })
            .collect()
    }

    /// Replace parameters with constants from the parameters map.
    ///
    /// # Errors
    /// - The parameters map is corrupted (parameters map points to invalid nodes).
    pub fn restore_constants(&mut self) -> Result<(), QueryPlannerError> {
        for (id, const_node) in self.constants.drain() {
            if let Node::Expression(Expression::Constant { .. }) = const_node {
            } else {
                return Err(QueryPlannerError::CustomError(format!(
                    "Restoring parameters filed: node {:?} (id: {}) is not of a constant type",
                    const_node, id
                )));
            }
            self.nodes.replace(id, const_node)?;
        }
        Ok(())
    }

    /// Replace constant nodes with parameters (and hide them in the parameters map).
    ///
    /// # Errors
    /// - The plan is corrupted (collected constants point to invalid arena positions).
    pub fn stash_constants(&mut self) -> Result<(), QueryPlannerError> {
        let constants = self.get_const_list();
        for const_id in constants {
            let const_node = self.nodes.replace(const_id, Node::Parameter)?;
            self.constants.insert(const_id, const_node);
        }
        Ok(())
    }
}
