use crate::errors::{Entity, SbroadError};
use crate::ir::operator::Relational;
use crate::ir::relation::Table;
use crate::ir::Plan;

impl Plan {
    /// Return first child of `Delete` node
    ///
    /// # Errors
    /// - node is not `Delete`
    /// - `Delete` has 0 or more than 1 child
    pub fn delete_child_id(&self, delete_id: usize) -> Result<usize, SbroadError> {
        let delete = self.get_relation_node(delete_id)?;
        if let Relational::Delete { children, .. } = delete {
            if let (Some(child), None) = (children.first(), children.get(1)) {
                return Ok(*child);
            }
            return Err(SbroadError::Unsupported(
                Entity::Operator,
                Some("DELETE must have exactly a single child node".to_string()),
            ));
        }
        Err(SbroadError::Invalid(
            Entity::Node,
            Some(format!("DELETE with id {delete_id}")),
        ))
    }

    /// Return the table for given `Delete` node
    ///
    /// # Errors
    /// - Node is not an `Delete`
    pub fn delete_table(&self, delete_id: usize) -> Result<&Table, SbroadError> {
        let delete = self.get_relation_node(delete_id)?;
        if let Relational::Delete { relation, .. } = delete {
            return self
                .get_relation(relation)
                .ok_or_else(|| SbroadError::NotFound(Entity::Table, relation.to_string()));
        }
        Err(SbroadError::Invalid(
            Entity::Node,
            Some(format!("DELETE node: {delete:?}")),
        ))
    }
}
