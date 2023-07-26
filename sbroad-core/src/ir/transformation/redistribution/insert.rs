use crate::errors::{Entity, SbroadError};
use crate::ir::operator::{ConflictStrategy, Relational};
use crate::ir::relation::{Column, Table};
use crate::ir::Plan;
use ahash::AHashMap;

use super::{MotionKey, Target};

impl Plan {
    /// Return first child of `Insert` node
    ///
    /// # Errors
    /// - node is not `Insert`
    /// - `Insert` has 0 or more than 1 child
    pub fn insert_child_id(&self, insert_id: usize) -> Result<usize, SbroadError> {
        let insert = self.get_relation_node(insert_id)?;
        if let Relational::Insert { children, .. } = insert {
            if let (Some(child), None) = (children.first(), children.get(1)) {
                return Ok(*child);
            }
            return Err(SbroadError::Unsupported(
                Entity::Operator,
                Some("INSERT must have exactly a single child node".to_string()),
            ));
        }
        Err(SbroadError::Invalid(
            Entity::Node,
            Some(format!("INSERT with id {insert_id}")),
        ))
    }

    /// Return `ConflictStrategy` for given insert node
    ///
    /// # Errors
    /// - node is not an `Insert`
    pub fn insert_conflict_strategy(
        &self,
        insert_id: usize,
    ) -> Result<&ConflictStrategy, SbroadError> {
        let insert = self.get_relation_node(insert_id)?;
        if let Relational::Insert {
            conflict_strategy, ..
        } = insert
        {
            return Ok(conflict_strategy);
        }
        Err(SbroadError::Invalid(
            Entity::Node,
            Some(format!("INSERT with id {insert_id}")),
        ))
    }

    pub(crate) fn insert_child_motion_key(
        &self,
        insert_id: usize,
    ) -> Result<MotionKey, SbroadError> {
        let columns = self.insert_columns(insert_id)?;
        let columns_map: AHashMap<usize, usize> = columns
            .iter()
            .enumerate()
            .map(|(pos, id)| (*id, pos))
            .collect::<AHashMap<_, _>>();

        // Check that the INSERT columns have the same length as the child node's
        // output columns.
        let child_id = self.insert_child_id(insert_id)?;
        let child_output_id = self.get_relation_node(child_id)?.output();
        let child_row = self.get_expression_node(child_output_id)?.get_row_list()?;
        if columns.len() != child_row.len() {
            return Err(SbroadError::Invalid(
                Entity::Node,
                Some(format!(
                    "INSERT with id {} has {} columns, but the child node with id {} has {}",
                    insert_id,
                    columns.len(),
                    child_id,
                    child_row.len()
                )),
            ));
        }

        let mut motion_key: MotionKey = MotionKey::new();
        let table = self.insert_table(insert_id)?;
        for pos in &table.shard_key.positions {
            if let Some(child_pos) = columns_map.get(pos) {
                // We can use insert column's position instead of
                // the position in the child node as their lengths
                // are the same.
                motion_key.targets.push(Target::Reference(*child_pos));
            } else {
                // Check that the column exists on the requested position.
                table.columns.get(*pos).ok_or_else(|| {
                    SbroadError::NotFound(
                        Entity::Column,
                        format!("in the table {table:?} at position {pos}"),
                    )
                })?;
                // We need a default value for the key column.
                motion_key
                    .targets
                    .push(Target::Value(Column::default_value()));
            }
        }
        Ok(motion_key)
    }

    /// Return ids of `Insert` columns
    ///
    /// # Errors
    /// - node is not `Insert`
    pub(crate) fn insert_columns(&self, insert_id: usize) -> Result<&[usize], SbroadError> {
        let insert = self.get_relation_node(insert_id)?;
        if let Relational::Insert { columns, .. } = insert {
            return Ok(columns);
        }
        Err(SbroadError::Invalid(
            Entity::Node,
            Some(format!("INSERT with id {insert_id}")),
        ))
    }

    /// Return the table for given `Insert` node
    ///
    /// # Errors
    /// - Node is not an `Insert`
    pub fn insert_table(&self, insert_id: usize) -> Result<&Table, SbroadError> {
        let insert = self.get_relation_node(insert_id)?;
        if let Relational::Insert { relation, .. } = insert {
            return self
                .get_relation(relation)
                .ok_or_else(|| SbroadError::NotFound(Entity::Table, relation.to_string()));
        }
        Err(SbroadError::Invalid(
            Entity::Node,
            Some(format!("INSERT with id {insert_id}")),
        ))
    }
}
