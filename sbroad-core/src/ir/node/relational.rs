use serde::Serialize;
use smol_str::SmolStr;

use crate::{
    errors::{Entity, SbroadError},
    ir::api::children::{Children, MutChildren},
};

use super::{
    Delete, Except, GroupBy, Having, Insert, Intersect, Join, Limit, Motion, NodeAligned, NodeId,
    OrderBy, Projection, ScanCte, ScanRelation, ScanSubQuery, Selection, Union, UnionAll, Update,
    Values, ValuesRow,
};

#[allow(clippy::module_name_repetitions)]
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub enum RelOwned {
    ScanCte(ScanCte),
    Except(Except),
    Delete(Delete),
    Insert(Insert),
    Intersect(Intersect),
    Update(Update),
    Join(Join),
    Limit(Limit),
    Motion(Motion),
    Projection(Projection),
    ScanRelation(ScanRelation),
    ScanSubQuery(ScanSubQuery),
    Selection(Selection),
    GroupBy(GroupBy),
    Having(Having),
    OrderBy(OrderBy),
    UnionAll(UnionAll),
    Union(Union),
    Values(Values),
    ValuesRow(ValuesRow),
}

impl From<RelOwned> for NodeAligned {
    fn from(value: RelOwned) -> Self {
        match value {
            RelOwned::ScanCte(scan_cte) => scan_cte.into(),
            RelOwned::Delete(delete) => delete.into(),
            RelOwned::Except(except) => except.into(),
            RelOwned::GroupBy(group_by) => group_by.into(),
            RelOwned::Having(having) => having.into(),
            RelOwned::Insert(insert) => insert.into(),
            RelOwned::Intersect(intersect) => intersect.into(),
            RelOwned::Join(join) => join.into(),
            RelOwned::Limit(limit) => limit.into(),
            RelOwned::Motion(motion) => motion.into(),
            RelOwned::OrderBy(order_by) => order_by.into(),
            RelOwned::Projection(proj) => proj.into(),
            RelOwned::ScanRelation(scan_rel) => scan_rel.into(),
            RelOwned::ScanSubQuery(scan_squery) => scan_squery.into(),
            RelOwned::Selection(selection) => selection.into(),
            RelOwned::Union(un) => un.into(),
            RelOwned::UnionAll(union_all) => union_all.into(),
            RelOwned::Update(update) => update.into(),
            RelOwned::Values(values) => values.into(),
            RelOwned::ValuesRow(values_row) => values_row.into(),
        }
    }
}

impl RelOwned {
    /// Sets new children to relational node.
    ///
    /// # Panics
    /// - wrong number of children for the given node
    pub fn set_children(&mut self, children: Vec<NodeId>) {
        match self {
            RelOwned::Join(Join {
                children: ref mut old,
                ..
            })
            | RelOwned::Delete(Delete {
                children: ref mut old,
                ..
            })
            | RelOwned::Update(Update {
                children: ref mut old,
                ..
            })
            | RelOwned::Insert(Insert {
                children: ref mut old,
                ..
            })
            | RelOwned::Motion(Motion {
                children: ref mut old,
                ..
            })
            | RelOwned::Projection(Projection {
                children: ref mut old,
                ..
            })
            | RelOwned::ScanSubQuery(ScanSubQuery {
                children: ref mut old,
                ..
            })
            | RelOwned::Selection(Selection {
                children: ref mut old,
                ..
            })
            | RelOwned::Values(Values {
                children: ref mut old,
                ..
            })
            | RelOwned::GroupBy(GroupBy {
                children: ref mut old,
                ..
            })
            | RelOwned::Having(Having {
                children: ref mut old,
                ..
            })
            | RelOwned::OrderBy(OrderBy {
                children: ref mut old,
                ..
            })
            | RelOwned::ValuesRow(ValuesRow {
                children: ref mut old,
                ..
            }) => {
                *old = children;
            }
            RelOwned::Except(Except { left, right, .. })
            | RelOwned::UnionAll(UnionAll { left, right, .. })
            | RelOwned::Intersect(Intersect { left, right, .. })
            | RelOwned::Union(Union { left, right, .. }) => {
                if children.len() != 2 {
                    unreachable!("Node has only two children!");
                }
                *left = children[0];
                *right = children[1];
            }
            RelOwned::ScanCte(ScanCte { ref mut child, .. }) => {
                if children.len() != 1 {
                    unreachable!("CTE may have only a single relational child");
                }
                // It is safe to unwrap here, because the length is already checked above.
                *child = children[0];
            }
            RelOwned::Limit(Limit { ref mut child, .. }) => {
                if children.len() != 1 {
                    unreachable!("LIMIT may have only a single relational child");
                }
                // It is safe to unwrap here, because the length is already checked above.
                *child = children[0];
            }
            RelOwned::ScanRelation(ScanRelation { .. }) => {
                assert!(children.is_empty(), "scan must have no children!");
            }
        }
    }

    // Gets an immutable reference to the children nodes.
    #[must_use]
    pub fn children(&self) -> Children<'_> {
        match self {
            RelOwned::Limit(Limit { child, .. }) | RelOwned::ScanCte(ScanCte { child, .. }) => {
                Children::Single(child)
            }
            RelOwned::Except(Except { left, right, .. })
            | RelOwned::Intersect(Intersect { left, right, .. })
            | RelOwned::UnionAll(UnionAll { left, right, .. })
            | RelOwned::Union(Union { left, right, .. }) => Children::Couple(left, right),
            RelOwned::GroupBy(GroupBy { children, .. })
            | RelOwned::Update(Update { children, .. })
            | RelOwned::Join(Join { children, .. })
            | RelOwned::Having(Having { children, .. })
            | RelOwned::OrderBy(OrderBy { children, .. })
            | RelOwned::Delete(Delete { children, .. })
            | RelOwned::Insert(Insert { children, .. })
            | RelOwned::Motion(Motion { children, .. })
            | RelOwned::Projection(Projection { children, .. })
            | RelOwned::ScanSubQuery(ScanSubQuery { children, .. })
            | RelOwned::Selection(Selection { children, .. })
            | RelOwned::ValuesRow(ValuesRow { children, .. })
            | RelOwned::Values(Values { children, .. }) => Children::Many(children),
            RelOwned::ScanRelation(_) => Children::None,
        }
    }

    #[must_use]
    pub fn mut_children(&mut self) -> MutChildren<'_> {
        match self {
            RelOwned::Limit(Limit { ref mut child, .. })
            | RelOwned::ScanCte(ScanCte { ref mut child, .. }) => MutChildren::Single(child),
            RelOwned::Except(Except {
                ref mut left,
                ref mut right,
                ..
            })
            | RelOwned::Intersect(Intersect {
                ref mut left,
                ref mut right,
                ..
            })
            | RelOwned::UnionAll(UnionAll {
                ref mut left,
                ref mut right,
                ..
            })
            | RelOwned::Union(Union {
                ref mut left,
                ref mut right,
                ..
            }) => MutChildren::Couple(left, right),
            RelOwned::GroupBy(GroupBy {
                ref mut children, ..
            })
            | RelOwned::Update(Update {
                ref mut children, ..
            })
            | RelOwned::Join(Join {
                ref mut children, ..
            })
            | RelOwned::OrderBy(OrderBy {
                ref mut children, ..
            })
            | RelOwned::Having(Having {
                ref mut children, ..
            })
            | RelOwned::Delete(Delete {
                ref mut children, ..
            })
            | RelOwned::Insert(Insert {
                ref mut children, ..
            })
            | RelOwned::Motion(Motion {
                ref mut children, ..
            })
            | RelOwned::Projection(Projection {
                ref mut children, ..
            })
            | RelOwned::ScanSubQuery(ScanSubQuery {
                ref mut children, ..
            })
            | RelOwned::Selection(Selection {
                ref mut children, ..
            })
            | RelOwned::ValuesRow(ValuesRow {
                ref mut children, ..
            })
            | RelOwned::Values(Values {
                ref mut children, ..
            }) => MutChildren::Many(children),
            RelOwned::ScanRelation(_) => MutChildren::None,
        }
    }

    /// Gets an mutable reference to the output tuple node id.
    #[must_use]
    pub fn mut_output(&mut self) -> &mut NodeId {
        match self {
            RelOwned::ScanCte(ScanCte { output, .. })
            | RelOwned::Except(Except { output, .. })
            | RelOwned::GroupBy(GroupBy { output, .. })
            | RelOwned::OrderBy(OrderBy { output, .. })
            | RelOwned::Update(Update { output, .. })
            | RelOwned::Having(Having { output, .. })
            | RelOwned::Join(Join { output, .. })
            | RelOwned::Limit(Limit { output, .. })
            | RelOwned::Delete(Delete { output, .. })
            | RelOwned::Insert(Insert { output, .. })
            | RelOwned::Intersect(Intersect { output, .. })
            | RelOwned::Motion(Motion { output, .. })
            | RelOwned::Projection(Projection { output, .. })
            | RelOwned::ScanRelation(ScanRelation { output, .. })
            | RelOwned::ScanSubQuery(ScanSubQuery { output, .. })
            | RelOwned::Selection(Selection { output, .. })
            | RelOwned::Union(Union { output, .. })
            | RelOwned::UnionAll(UnionAll { output, .. })
            | RelOwned::Values(Values { output, .. })
            | RelOwned::ValuesRow(ValuesRow { output, .. }) => output,
        }
    }
}

/// Relational algebra operator returning a new tuple.
///
/// Transforms input tuple(s) into the output one using the
/// relation algebra logic.
#[allow(clippy::module_name_repetitions)]
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub enum Relational<'a> {
    ScanCte(&'a ScanCte),
    Except(&'a Except),
    Delete(&'a Delete),
    Insert(&'a Insert),
    Intersect(&'a Intersect),
    Update(&'a Update),
    Join(&'a Join),
    Limit(&'a Limit),
    Motion(&'a Motion),
    Projection(&'a Projection),
    ScanRelation(&'a ScanRelation),
    ScanSubQuery(&'a ScanSubQuery),
    Selection(&'a Selection),
    GroupBy(&'a GroupBy),
    Having(&'a Having),
    OrderBy(&'a OrderBy),
    UnionAll(&'a UnionAll),
    Union(&'a Union),
    Values(&'a Values),
    ValuesRow(&'a ValuesRow),
}

#[allow(clippy::module_name_repetitions)]
#[derive(Debug, PartialEq, Eq, Serialize)]
pub enum MutRelational<'a> {
    ScanCte(&'a mut ScanCte),
    Except(&'a mut Except),
    Delete(&'a mut Delete),
    Insert(&'a mut Insert),
    Intersect(&'a mut Intersect),
    Update(&'a mut Update),
    Join(&'a mut Join),
    Limit(&'a mut Limit),
    Motion(&'a mut Motion),
    Projection(&'a mut Projection),
    ScanRelation(&'a mut ScanRelation),
    ScanSubQuery(&'a mut ScanSubQuery),
    Selection(&'a mut Selection),
    GroupBy(&'a mut GroupBy),
    Having(&'a mut Having),
    OrderBy(&'a mut OrderBy),
    UnionAll(&'a mut UnionAll),
    Union(&'a mut Union),
    Values(&'a mut Values),
    ValuesRow(&'a mut ValuesRow),
}

impl MutRelational<'_> {
    /// Gets an mutable reference to the output tuple node id.
    #[must_use]
    pub fn mut_output(&mut self) -> &mut NodeId {
        match self {
            MutRelational::ScanCte(ScanCte { output, .. })
            | MutRelational::Except(Except { output, .. })
            | MutRelational::GroupBy(GroupBy { output, .. })
            | MutRelational::OrderBy(OrderBy { output, .. })
            | MutRelational::Update(Update { output, .. })
            | MutRelational::Having(Having { output, .. })
            | MutRelational::Join(Join { output, .. })
            | MutRelational::Limit(Limit { output, .. })
            | MutRelational::Delete(Delete { output, .. })
            | MutRelational::Insert(Insert { output, .. })
            | MutRelational::Intersect(Intersect { output, .. })
            | MutRelational::Motion(Motion { output, .. })
            | MutRelational::Projection(Projection { output, .. })
            | MutRelational::ScanRelation(ScanRelation { output, .. })
            | MutRelational::ScanSubQuery(ScanSubQuery { output, .. })
            | MutRelational::Selection(Selection { output, .. })
            | MutRelational::Union(Union { output, .. })
            | MutRelational::UnionAll(UnionAll { output, .. })
            | MutRelational::Values(Values { output, .. })
            | MutRelational::ValuesRow(ValuesRow { output, .. }) => output,
        }
    }

    // Gets a mutable reference to the children nodes.
    #[must_use]
    pub fn mut_children(&mut self) -> MutChildren<'_> {
        // return MutChildren { node: self };
        match self {
            MutRelational::Limit(Limit { child, .. })
            | MutRelational::ScanCte(ScanCte { child, .. }) => MutChildren::Single(child),
            MutRelational::Except(Except { left, right, .. })
            | MutRelational::Intersect(Intersect { left, right, .. })
            | MutRelational::UnionAll(UnionAll { left, right, .. })
            | MutRelational::Union(Union { left, right, .. }) => MutChildren::Couple(left, right),
            MutRelational::GroupBy(GroupBy {
                ref mut children, ..
            })
            | MutRelational::Update(Update {
                ref mut children, ..
            })
            | MutRelational::OrderBy(OrderBy {
                ref mut children, ..
            })
            | MutRelational::Having(Having {
                ref mut children, ..
            })
            | MutRelational::Join(Join {
                ref mut children, ..
            })
            | MutRelational::Delete(Delete {
                ref mut children, ..
            })
            | MutRelational::Insert(Insert {
                ref mut children, ..
            })
            | MutRelational::Motion(Motion {
                ref mut children, ..
            })
            | MutRelational::Projection(Projection {
                ref mut children, ..
            })
            | MutRelational::ScanSubQuery(ScanSubQuery {
                ref mut children, ..
            })
            | MutRelational::Selection(Selection {
                ref mut children, ..
            })
            | MutRelational::ValuesRow(ValuesRow {
                ref mut children, ..
            })
            | MutRelational::Values(Values {
                ref mut children, ..
            }) => MutChildren::Many(children),
            MutRelational::ScanRelation(_) => MutChildren::None,
        }
    }

    /// Sets new children to relational node.
    ///
    /// # Panics
    /// - wrong number of children for the given node
    pub fn set_children(&mut self, children: Vec<NodeId>) {
        match self {
            MutRelational::Join(Join {
                children: ref mut old,
                ..
            })
            | MutRelational::Delete(Delete {
                children: ref mut old,
                ..
            })
            | MutRelational::Update(Update {
                children: ref mut old,
                ..
            })
            | MutRelational::Insert(Insert {
                children: ref mut old,
                ..
            })
            | MutRelational::Motion(Motion {
                children: ref mut old,
                ..
            })
            | MutRelational::Projection(Projection {
                children: ref mut old,
                ..
            })
            | MutRelational::ScanSubQuery(ScanSubQuery {
                children: ref mut old,
                ..
            })
            | MutRelational::Selection(Selection {
                children: ref mut old,
                ..
            })
            | MutRelational::Values(Values {
                children: ref mut old,
                ..
            })
            | MutRelational::GroupBy(GroupBy {
                children: ref mut old,
                ..
            })
            | MutRelational::Having(Having {
                children: ref mut old,
                ..
            })
            | MutRelational::OrderBy(OrderBy {
                children: ref mut old,
                ..
            })
            | MutRelational::ValuesRow(ValuesRow {
                children: ref mut old,
                ..
            }) => {
                *old = children;
            }
            MutRelational::Except(Except { left, right, .. })
            | MutRelational::UnionAll(UnionAll { left, right, .. })
            | MutRelational::Intersect(Intersect { left, right, .. })
            | MutRelational::Union(Union { left, right, .. }) => {
                if children.len() != 2 {
                    unreachable!("Node has only two children!");
                }
                *left = children[0];
                *right = children[1];
            }
            MutRelational::ScanCte(ScanCte { ref mut child, .. }) => {
                if children.len() != 1 {
                    unreachable!("CTE may have only a single relational child");
                }
                // It is safe to unwrap here, because the length is already checked above.
                *child = children[0];
            }
            MutRelational::Limit(Limit { ref mut child, .. }) => {
                if children.len() != 1 {
                    unreachable!("LIMIT may have only a single relational child");
                }
                // It is safe to unwrap here, because the length is already checked above.
                *child = children[0];
            }
            MutRelational::ScanRelation(ScanRelation { .. }) => {
                assert!(children.is_empty(), "scan must have no children!");
            }
        }
    }

    /// Add `SubQuery` to the list of relational children.
    ///
    /// # Panics
    /// - Trying to add subquery to inapplicable relational node.
    pub fn add_sq_child(&mut self, sq_id: NodeId) {
        match self {
            MutRelational::Join(Join { children, .. })
            | MutRelational::Projection(Projection { children, .. })
            | MutRelational::Selection(Selection { children, .. })
            | MutRelational::GroupBy(GroupBy { children, .. })
            | MutRelational::Having(Having { children, .. })
            | MutRelational::OrderBy(OrderBy { children, .. })
            | MutRelational::Update(Update { children, .. })
            | MutRelational::ValuesRow(ValuesRow { children, .. }) => children.push(sq_id),
            _ => panic!("Unable to add SubQuery child to {self:?}."),
        }
    }

    /// Sets new scan name to relational node.
    ///
    /// # Errors
    /// - relational node is not a scan.
    ///
    /// # Panics
    /// - CTE must have a name.
    pub fn set_scan_name(&mut self, name: Option<SmolStr>) -> Result<(), SbroadError> {
        match self {
            MutRelational::ScanRelation(ScanRelation { ref mut alias, .. })
            | MutRelational::ScanSubQuery(ScanSubQuery { ref mut alias, .. }) => {
                *alias = name;
                Ok(())
            }
            MutRelational::ScanCte(ScanCte { ref mut alias, .. }) => {
                let name = name.expect("CTE must have a name");
                *alias = name;
                Ok(())
            }
            _ => Err(SbroadError::Invalid(
                Entity::Relational,
                Some("Relational node is not a Scan.".into()),
            )),
        }
    }
}

#[allow(dead_code)]
impl Relational<'_> {
    /// Gets an immutable id of the output tuple node of the plan's arena.
    #[must_use]
    pub fn output(&self) -> NodeId {
        match self {
            Relational::ScanCte(ScanCte { output, .. })
            | Relational::Except(Except { output, .. })
            | Relational::GroupBy(GroupBy { output, .. })
            | Relational::OrderBy(OrderBy { output, .. })
            | Relational::Having(Having { output, .. })
            | Relational::Update(Update { output, .. })
            | Relational::Limit(Limit { output, .. })
            | Relational::Join(Join { output, .. })
            | Relational::Delete(Delete { output, .. })
            | Relational::Insert(Insert { output, .. })
            | Relational::Intersect(Intersect { output, .. })
            | Relational::Motion(Motion { output, .. })
            | Relational::Projection(Projection { output, .. })
            | Relational::ScanRelation(ScanRelation { output, .. })
            | Relational::ScanSubQuery(ScanSubQuery { output, .. })
            | Relational::Selection(Selection { output, .. })
            | Relational::Union(Union { output, .. })
            | Relational::UnionAll(UnionAll { output, .. })
            | Relational::Values(Values { output, .. })
            | Relational::ValuesRow(ValuesRow { output, .. }) => *output,
        }
    }

    // Gets an immutable reference to the children nodes.
    #[must_use]
    pub fn children(&self) -> Children<'_> {
        match self {
            Relational::Limit(Limit { child, .. }) | Relational::ScanCte(ScanCte { child, .. }) => {
                Children::Single(child)
            }
            Relational::Except(Except { left, right, .. })
            | Relational::Intersect(Intersect { left, right, .. })
            | Relational::UnionAll(UnionAll { left, right, .. })
            | Relational::Union(Union { left, right, .. }) => Children::Couple(left, right),
            Relational::GroupBy(GroupBy { children, .. })
            | Relational::Update(Update { children, .. })
            | Relational::Join(Join { children, .. })
            | Relational::OrderBy(OrderBy { children, .. })
            | Relational::Having(Having { children, .. })
            | Relational::Delete(Delete { children, .. })
            | Relational::Insert(Insert { children, .. })
            | Relational::Motion(Motion { children, .. })
            | Relational::Projection(Projection { children, .. })
            | Relational::ScanSubQuery(ScanSubQuery { children, .. })
            | Relational::Selection(Selection { children, .. })
            | Relational::ValuesRow(ValuesRow { children, .. })
            | Relational::Values(Values { children, .. }) => Children::Many(children),
            Relational::ScanRelation(_) => Children::None,
        }
    }

    /// Checks if the node is deletion.
    #[must_use]
    pub fn is_delete(&self) -> bool {
        matches!(self, Relational::Delete { .. })
    }
    /// Checks if the node is an insertion.
    #[must_use]
    pub fn is_insert(&self) -> bool {
        matches!(self, Relational::Insert { .. })
    }

    /// Checks if the node is dml node
    #[must_use]
    pub fn is_dml(&self) -> bool {
        matches!(
            self,
            Relational::Insert { .. } | Relational::Update { .. } | Relational::Delete { .. }
        )
    }

    /// Checks that the node is a motion.
    #[must_use]
    pub fn is_motion(&self) -> bool {
        matches!(self, &Relational::Motion { .. })
    }

    /// Checks that the node is a sub-query or CTE scan.
    #[must_use]
    pub fn is_subquery_or_cte(&self) -> bool {
        matches!(
            self,
            &Relational::ScanSubQuery { .. } | &Relational::ScanCte { .. }
        )
    }

    #[must_use]
    pub fn name(&self) -> &str {
        match self {
            Relational::Except { .. } => "Except",
            Relational::Delete { .. } => "Delete",
            Relational::Insert { .. } => "Insert",
            Relational::Intersect { .. } => "Intersect",
            Relational::Update { .. } => "Update",
            Relational::Join { .. } => "Join",
            Relational::Limit { .. } => "Limit",
            Relational::Motion { .. } => "Motion",
            Relational::Projection { .. } => "Projection",
            Relational::ScanCte { .. } => "CTE",
            Relational::ScanRelation { .. } => "Scan",
            Relational::ScanSubQuery { .. } => "Subquery",
            Relational::Selection { .. } => "Selection",
            Relational::GroupBy { .. } => "GroupBy",
            Relational::OrderBy { .. } => "OrderBy",
            Relational::Having { .. } => "Having",
            Relational::Union { .. } => "Union",
            Relational::UnionAll { .. } => "UnionAll",
            Relational::Values { .. } => "Values",
            Relational::ValuesRow { .. } => "ValuesRow",
        }
    }

    #[must_use]
    pub fn get_rel_owned(&self) -> RelOwned {
        match self {
            Relational::Delete(del) => RelOwned::Delete((*del).clone()),
            Relational::Except(except) => RelOwned::Except((*except).clone()),
            Relational::GroupBy(group_by) => RelOwned::GroupBy((*group_by).clone()),
            Relational::Having(having) => RelOwned::Having((*having).clone()),
            Relational::Insert(insert) => RelOwned::Insert((*insert).clone()),
            Relational::Intersect(intersect) => RelOwned::Intersect((*intersect).clone()),
            Relational::Join(join) => RelOwned::Join((*join).clone()),
            Relational::Limit(join) => RelOwned::Limit((*join).clone()),
            Relational::Motion(motion) => RelOwned::Motion((*motion).clone()),
            Relational::OrderBy(order_by) => RelOwned::OrderBy((*order_by).clone()),
            Relational::Projection(proj) => RelOwned::Projection((*proj).clone()),
            Relational::ScanCte(scan_cte) => RelOwned::ScanCte((*scan_cte).clone()),
            Relational::ScanRelation(scan_rel) => RelOwned::ScanRelation((*scan_rel).clone()),
            Relational::ScanSubQuery(ssubquery) => RelOwned::ScanSubQuery((*ssubquery).clone()),
            Relational::Selection(sel) => RelOwned::Selection((*sel).clone()),
            Relational::Union(un) => RelOwned::Union((*un).clone()),
            Relational::UnionAll(union_all) => RelOwned::UnionAll((*union_all).clone()),
            Relational::Update(upd) => RelOwned::Update((*upd).clone()),
            Relational::Values(values) => RelOwned::Values((*values).clone()),
            Relational::ValuesRow(values_row) => RelOwned::ValuesRow((*values_row).clone()),
        }
    }
}