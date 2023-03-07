use std::mem::take;

use crate::errors::{Entity, SbroadError};
use crate::executor::hash::ToHashString;
use crate::ir::expression::cast::Type as CastType;
use crate::ir::expression::Expression;
use crate::ir::function::{Behavior, Function};
use crate::ir::operator::Relational;
use crate::ir::relation::{Column, ColumnRole, Table};
use crate::ir::value::Value;
use crate::ir::Plan;
use ahash::AHashMap;

use super::{MotionKey, Target};

impl Plan {
    pub(crate) fn insert_child_id(&self, insert_id: usize) -> Result<usize, SbroadError> {
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

    pub(crate) fn set_insert_child_id(
        &mut self,
        insert_id: usize,
        new_child: usize,
    ) -> Result<(), SbroadError> {
        let insert = self.get_mut_relation_node(insert_id)?;
        if let Relational::Insert { children, .. } = insert {
            *children = vec![new_child];
            return Ok(());
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
        for pos in &table.key.positions {
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

    pub(crate) fn insert_columns_mut(
        &mut self,
        insert_id: usize,
    ) -> Result<&mut Vec<usize>, SbroadError> {
        let insert = self.get_mut_relation_node(insert_id)?;
        if let Relational::Insert { columns, .. } = insert {
            return Ok(columns);
        }
        Err(SbroadError::Invalid(
            Entity::Node,
            Some(format!("INSERT with id {insert_id}")),
        ))
    }

    pub(crate) fn insert_table(&self, insert_id: usize) -> Result<&Table, SbroadError> {
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

    /// Wraps the child node with nested sub-queries that cast expression types.
    /// ```text
    /// insert into t1 (a) select c from t2
    ///   =>
    /// insert into t1 (a)
    /// select
    ///   cast(c as integer) as COL_1
    /// from (select c from t2)
    /// ```
    pub(crate) fn replace_insert_child_with_cast_types_sq(
        &mut self,
        insert_id: usize,
    ) -> Result<(), SbroadError> {
        // Add sub-query.
        let child_id = self.insert_child_id(insert_id)?;
        let sq_id = self.add_sub_query(child_id, None)?;
        let sq_output_id = self.get_relation_node(sq_id)?.output();
        self.set_distribution(sq_output_id)?;

        // Create a new row for the type-cast projection.
        let proj_output_id = self.clone_expr_subtree(sq_output_id)?;
        let proj_row = self.get_expression_node(proj_output_id)?.clone_row_list()?;
        let mut new_row: Vec<usize> = Vec::with_capacity(proj_row.len());

        let columns = self.insert_columns(insert_id)?.to_vec();
        for (idx, col_idx) in columns.iter().enumerate() {
            let table = self.insert_table(insert_id)?;
            let col = table.columns.get(*col_idx).ok_or_else(|| {
                SbroadError::NotFound(
                    Entity::Column,
                    format!("in the table {table:?} at position {col_idx}"),
                )
            })?;
            let new_col_id: usize = match col.role {
                ColumnRole::User => {
                    let proj_alias_id = *proj_row.get(idx).ok_or_else(|| {
                        SbroadError::NotFound(
                            Entity::Column,
                            format!("in the INSERT child's output at position {idx}"),
                        )
                    })?;
                    let proj_alias = self.get_expression_node(proj_alias_id)?;
                    let proj_col_id = if let Expression::Alias { child, .. } = proj_alias {
                        *child
                    } else {
                        proj_alias_id
                    };
                    // Explicitly cast the value to the type of the table column.
                    let col_type = CastType::try_from(&col.r#type)?;
                    self.add_cast(proj_col_id, col_type)?
                }
                ColumnRole::Sharding => {
                    return Err(SbroadError::Unsupported(
                        Entity::Operator,
                        Some("INSERT bucket_id column".to_string()),
                    ));
                }
            };
            let alias_id = self.nodes.add_alias(&format!("COL_{idx}"), new_col_id)?;
            new_row.push(alias_id);
        }
        // Add projection with type cast on top of the sub-query.
        let proj_id = self.add_proj_internal(sq_id, &new_row)?;
        let proj_output_id = self.get_relation_node(proj_id)?.output();
        self.set_distribution(proj_output_id)?;
        self.set_insert_child_id(insert_id, proj_id)?;

        Ok(())
    }

    /// Wraps the child node with nested sub-queries that insert `bucket_id` column.
    ///
    /// ```text
    /// insert into t1 (a) select c from t2
    ///   =>
    /// insert into t1 (a, bucket_id)
    /// select
    ///   c,
    ///   bucket_id(coalesce(cast(NULL as string), 'NULL') || coalesce(cast(c as string), 'NULL'))
    /// from (select c from t2)
    /// ```
    pub(crate) fn replace_insert_child_with_bucket_id_sq(
        &mut self,
        insert_id: usize,
        child_motion_key: &MotionKey,
    ) -> Result<(), SbroadError> {
        // Add sub-query.
        let child_id = self.insert_child_id(insert_id)?;
        let sq_id = self.add_sub_query(child_id, None)?;
        let sq_output_id = self.get_relation_node(sq_id)?.output();
        self.set_distribution(sq_output_id)?;

        // Create a new row for the bucket_id projection.
        let proj_output_id = self.clone_expr_subtree(sq_output_id)?;
        self.flush_parent_in_subtree(proj_output_id)?;
        let mut new_row = take(
            self.get_mut_expression_node(proj_output_id)?
                .get_row_list_mut()?,
        );

        // Create the string concatenation node:
        // coalesce(cast(NULL as string), 'NULL') || coalesce(cast(c as string), 'NULL')
        let mut columns_to_concat: Vec<usize> = Vec::with_capacity(child_motion_key.targets.len());
        for target in &child_motion_key.targets {
            let alias_id = match target {
                Target::Value(value) => self.add_const(value.clone()),
                Target::Reference(row_pos) => *new_row.get(*row_pos).ok_or_else(|| {
                    SbroadError::NotFound(
                        Entity::Column,
                        format!("in the INSERT child's output at position {row_pos}"),
                    )
                })?,
            };
            let col_expr = self.get_expression_node(alias_id)?;
            let col_id = if let Expression::Alias { child, .. } = col_expr {
                *child
            } else {
                alias_id
            };
            let cast_id = self.add_cast(col_id, CastType::String)?;
            let null_string_id = self.add_const(Value::String(Value::Null.to_hash_string()));
            let fn_coalesce = Function::new("coalesce".to_string(), Behavior::Stable);
            let coalesce_id: usize =
                self.add_stable_function(&fn_coalesce, vec![cast_id, null_string_id])?;
            columns_to_concat.push(coalesce_id);
        }
        let concat_id = if let Some((first, others)) = columns_to_concat.split_first() {
            let mut left_id = *first;
            for right_id in others {
                let concat_id = self.add_concat(left_id, *right_id)?;
                left_id = concat_id;
            }
            left_id
        } else {
            return Err(SbroadError::NotFound(
                Entity::Column,
                "to concatenate distribution key string to calculate bucket_id".to_string(),
            ));
        };

        // Create the bucket_id function node.
        let fn_bucket_id = Function::new("bucket_id".to_string(), Behavior::Stable);
        let bucket_id_id: usize = self.add_stable_function(&fn_bucket_id, vec![concat_id])?;

        // Push the bucket_id column to the end of the INSERT columns' row.
        let table = self.insert_table(insert_id)?;
        let bucket_id_pos = table.get_bucket_id_position()?;
        let columns = self.insert_columns_mut(insert_id)?;
        columns.push(bucket_id_pos);

        // Push the bucket_id column to the end of projection row.
        new_row.push(bucket_id_id);

        // Add projection with bucket_id on top of the sub-query.
        let proj_id = self.add_proj_internal(sq_id, &new_row)?;
        let proj_output_id = self.get_relation_node(proj_id)?.output();
        self.set_distribution(proj_output_id)?;
        self.set_insert_child_id(insert_id, proj_id)?;

        Ok(())
    }
}
