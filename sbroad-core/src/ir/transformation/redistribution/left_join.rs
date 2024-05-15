//! Left Join trasformation logic when outer child has Global distribution
//! and inner child has Segment or Any distribution.

use smol_str::{format_smolstr, SmolStr};

use crate::{
    errors::{Entity, SbroadError},
    ir::{
        distribution::Distribution,
        expression::Expression,
        operator::{JoinKind, Relational},
        Plan,
    },
};

use super::{MotionOpcode, MotionPolicy, Program, Strategy};

impl Plan {
    pub(super) fn calculate_strategy_for_left_join_with_global_tbl(
        &mut self,
        join_id: usize,
        join_kind: &JoinKind,
    ) -> Result<Option<Strategy>, SbroadError> {
        let is_left_join = matches!(join_kind, JoinKind::LeftOuter);
        let is_outer_global = matches!(
            self.get_rel_distribution(self.get_relational_child(join_id, 0)?)?,
            Distribution::Global
        );
        let is_inner_non_local = matches!(
            self.get_rel_distribution(self.get_relational_child(join_id, 1)?)?,
            Distribution::Segment { .. } | Distribution::Any
        );
        if !(is_left_join && is_outer_global && is_inner_non_local) {
            return Ok(None);
        }

        if let Relational::Join { kind, .. } = self.get_mut_relation_node(join_id)? {
            *kind = JoinKind::Inner;
        }
        self.set_distribution(self.get_relational_output(join_id)?)?;

        let Some(parent_id) = self.find_parent_rel(join_id)? else {
            return Err(SbroadError::Invalid(
                Entity::Plan,
                Some(format_smolstr!("join ({join_id}) has no parent!")),
            ));
        };
        let projection_id = create_projection(self, join_id)?;
        let sq_id = self.add_sub_query(projection_id, None)?;
        self.set_distribution(self.get_relational_output(sq_id)?)?;
        self.change_child(parent_id, join_id, sq_id)?;

        let outer_id = self.get_relational_child(join_id, 0)?;

        // In case there are no motions under outer child,
        // we need to add one, because we need to materialize
        // the subtree from which missing rows will be added.
        let outer_child_motion_id = {
            let child = self.get_relation_node(outer_id)?;
            let mut motion_child_id = None;

            // Check if there is already motion under outer child
            if child.is_subquery_or_cte() {
                let sq_child = self.get_relational_child(outer_id, 0)?;
                if self.get_relation_node(sq_child)?.is_motion() {
                    motion_child_id = Some(sq_child);
                }
            } else if child.is_motion() {
                motion_child_id = Some(outer_id);
            }

            if motion_child_id.is_none() {
                let motion_id =
                    self.add_motion(outer_id, &MotionPolicy::Full, Program::default())?;
                self.change_child(join_id, outer_id, motion_id)?;
                motion_child_id = Some(motion_id);
            }
            motion_child_id.unwrap()
        };

        // Add motion which will do the reduce stage of joining:
        // adding missing rows.
        let motion_op = MotionOpcode::AddMissingRowsForLeftJoin {
            motion_id: outer_child_motion_id,
        };
        let mut strategy = Strategy::new(parent_id);
        strategy.add_child(sq_id, MotionPolicy::Full, Program(vec![motion_op]));

        Ok(Some(strategy))
    }
}

fn create_projection(plan: &mut Plan, join_id: usize) -> Result<usize, SbroadError> {
    let proj_columns_names = collect_projection_columns(plan, join_id)?;
    let proj_columns_refs: Vec<&str> = proj_columns_names.iter().map(SmolStr::as_str).collect();
    let proj_id = plan.add_proj(join_id, &proj_columns_refs, false, false)?;
    let output_id = plan.get_relational_output(proj_id)?;
    plan.replace_parent_in_subtree(output_id, Some(join_id), Some(proj_id))?;
    plan.set_distribution(output_id)?;
    Ok(proj_id)
}

// Returns a list of column aliases from join node output.
fn collect_projection_columns(
    plan: &mut Plan,
    join_id: usize,
) -> Result<Vec<SmolStr>, SbroadError> {
    // TODO: currently we use all columns from joined tables,
    // but it is possible that a lot of columns are not used
    // above in the plan, we can remove unused columns to
    // reduce amount of data sent through the network.
    // https://git.picodata.io/picodata/picodata/sbroad/-/issues/36
    let output_id = plan.get_relational_output(join_id)?;
    let columns_len = plan.get_row_list(output_id)?.len();
    let mut projection_columns: Vec<SmolStr> = Vec::with_capacity(columns_len);
    for idx in 0..columns_len {
        let expr_id = *plan.get_row_list(output_id)?.get(idx).ok_or_else(|| {
            SbroadError::UnexpectedNumberOfValues("output row size changed".into())
        })?;
        if let Expression::Alias { name, .. } = plan.get_expression_node(expr_id)? {
            projection_columns.push(name.clone());
        } else {
            return Err(SbroadError::Invalid(
                Entity::Node,
                Some(format_smolstr!(
                    "node ({join_id}) output columns is not alias"
                )),
            ));
        }
    }
    Ok(projection_columns)
}
