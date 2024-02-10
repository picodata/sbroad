use crate::ir::{Entity, Node, Plan, SbroadError};
use serde::{Deserialize, Serialize};
use tarantool::decimal::Decimal;

use super::ddl::ParamDef;

::tarantool::define_str_enum! {
    /// Revoked or granted privilege.
    pub enum Privilege {
        Read = "read",
        Write = "write",
        Execute = "execute",
        Create = "create",
        Alter = "alter",
        Drop = "drop",
        Session = "session",
        Usage = "usage",
    }
}

/// Helper enum representing one of possible revoking/granting options:
/// * Table: for specific table.
/// * Universe: for any object supporting passed privilege.
/// * Role: for making user/role a part of another role.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub enum GrantRevokeType {
    User {
        privilege: Privilege,
    },
    SpecificUser {
        privilege: Privilege,
        user_name: String,
    },
    Role {
        privilege: Privilege,
    },
    SpecificRole {
        privilege: Privilege,
        role_name: String,
    },
    Table {
        privilege: Privilege,
    },
    SpecificTable {
        privilege: Privilege,
        table_name: String,
    },
    Procedure {
        privilege: Privilege,
    },
    SpecificProcedure {
        privilege: Privilege,
        proc_name: String,
        proc_params: Option<Vec<ParamDef>>,
    },
    RolePass {
        role_name: String,
    },
}

/// Check passed privilege is in the list of accepted privileges.
fn check_privilege(privilege: Privilege, accepted: &[Privilege]) -> Result<(), SbroadError> {
    if !accepted.contains(&privilege) {
        return Err(SbroadError::Invalid(
            Entity::Privilege,
            Some(format!("Supported privileges are: {accepted:?}")),
        ));
    }
    Ok(())
}

impl GrantRevokeType {
    /// # Errors
    /// - Unacceptable privilege for user was passed.
    pub fn user(privilege: Privilege) -> Result<Self, SbroadError> {
        check_privilege(
            privilege,
            &[Privilege::Create, Privilege::Alter, Privilege::Drop],
        )?;
        Ok(Self::User { privilege })
    }

    /// # Errors
    /// - Unacceptable privilege for specific user was passed.
    pub fn specific_user(privilege: Privilege, user_name: String) -> Result<Self, SbroadError> {
        check_privilege(privilege, &[Privilege::Alter, Privilege::Drop])?;
        Ok(Self::SpecificUser {
            privilege,
            user_name,
        })
    }

    /// # Errors
    /// - Unacceptable privilege for role was passed.
    pub fn role(privilege: Privilege) -> Result<Self, SbroadError> {
        check_privilege(privilege, &[Privilege::Create, Privilege::Drop])?;
        Ok(Self::Role { privilege })
    }

    /// # Errors
    /// - Unacceptable privilege for specific role was passed.
    pub fn specific_role(privilege: Privilege, role_name: String) -> Result<Self, SbroadError> {
        check_privilege(privilege, &[Privilege::Drop])?;
        Ok(Self::SpecificRole {
            privilege,
            role_name,
        })
    }

    /// # Errors
    /// - Unacceptable privilege for table was passed.
    pub fn table(privilege: Privilege) -> Result<Self, SbroadError> {
        check_privilege(
            privilege,
            &[
                Privilege::Read,
                Privilege::Write,
                Privilege::Create,
                Privilege::Alter,
                Privilege::Drop,
            ],
        )?;
        Ok(Self::Table { privilege })
    }

    /// # Errors
    /// - Unacceptable privilege for specific table was passed.
    pub fn specific_table(privilege: Privilege, table_name: String) -> Result<Self, SbroadError> {
        check_privilege(
            privilege,
            &[
                Privilege::Read,
                Privilege::Write,
                Privilege::Alter,
                Privilege::Drop,
            ],
        )?;
        Ok(Self::SpecificTable {
            privilege,
            table_name,
        })
    }

    /// # Errors
    /// - Unacceptable privilege for procedure was passed.
    pub fn procedure(privilege: Privilege) -> Result<Self, SbroadError> {
        check_privilege(
            privilege,
            &[Privilege::Create, Privilege::Drop, Privilege::Execute],
        )?;
        Ok(Self::Procedure { privilege })
    }

    /// # Errors
    /// - Unacceptable privilege for specific procedure was passed.
    pub fn specific_procedure(
        privilege: Privilege,
        proc_name: String,
        proc_params: Option<Vec<ParamDef>>,
    ) -> Result<Self, SbroadError> {
        check_privilege(privilege, &[Privilege::Drop, Privilege::Execute])?;
        Ok(Self::SpecificProcedure {
            privilege,
            proc_name,
            proc_params,
        })
    }

    #[must_use]
    pub fn role_pass(role_name: String) -> Self {
        Self::RolePass { role_name }
    }
}

/// Type of ALTER USER command execution.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub enum AlterOption {
    Login,
    NoLogin,
    Password {
        password: String,
        auth_method: String,
    },
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub enum Acl {
    DropRole {
        name: String,
        timeout: Decimal,
    },
    DropUser {
        name: String,
        timeout: Decimal,
    },
    CreateRole {
        name: String,
        timeout: Decimal,
    },
    CreateUser {
        name: String,
        password: String,
        auth_method: String,
        timeout: Decimal,
    },
    AlterUser {
        name: String,
        alter_option: AlterOption,
        timeout: Decimal,
    },
    GrantPrivilege {
        grant_type: GrantRevokeType,
        grantee_name: String,
        timeout: Decimal,
    },
    RevokePrivilege {
        revoke_type: GrantRevokeType,
        grantee_name: String,
        timeout: Decimal,
    },
}

impl Acl {
    /// Return ACL node timeout.
    ///
    /// # Errors
    /// - timeout parsing error
    pub fn timeout(&self) -> Result<f64, SbroadError> {
        match self {
            Acl::DropRole { ref timeout, .. }
            | Acl::DropUser { ref timeout, .. }
            | Acl::CreateRole { ref timeout, .. }
            | Acl::AlterUser { ref timeout, .. }
            | Acl::CreateUser { ref timeout, .. }
            | Acl::RevokePrivilege { ref timeout, .. }
            | Acl::GrantPrivilege { ref timeout, .. } => timeout,
        }
        .to_string()
        .parse()
        .map_err(|e| {
            SbroadError::Invalid(
                Entity::SpaceMetadata,
                Some(format!("timeout parsing error {e:?}")),
            )
        })
    }
}

impl Plan {
    /// Get ACL node from the plan arena.
    ///
    /// # Errors
    /// - the node index is absent in arena
    /// - current node is not of ACL type
    pub fn get_acl_node(&self, node_id: usize) -> Result<&Acl, SbroadError> {
        let node = self.get_node(node_id)?;
        match node {
            Node::Acl(acl) => Ok(acl),
            _ => Err(SbroadError::Invalid(
                Entity::Node,
                Some(format!("node is not ACL type: {node:?}")),
            )),
        }
    }

    /// Get a mutable ACL node from the plan arena.
    ///
    /// # Errors
    /// - the node index is absent in arena
    /// - current node is not of ACL type
    pub fn get_mut_acl_node(&mut self, node_id: usize) -> Result<&mut Acl, SbroadError> {
        let node = self.get_mut_node(node_id)?;
        match node {
            Node::Acl(acl) => Ok(acl),
            _ => Err(SbroadError::Invalid(
                Entity::Node,
                Some(format!("node is not ACL type: {node:?}")),
            )),
        }
    }

    /// Take ACL node from the plan arena and replace it with parameter node.
    ///
    /// # Errors
    /// - current node is not of ACL type
    pub fn take_acl_node(&mut self, node_id: usize) -> Result<Acl, SbroadError> {
        // Check that node is ACL type (before making any distructive operations).
        let _ = self.get_acl_node(node_id)?;
        // Replace ACL with parameter node.
        let node = std::mem::replace(self.get_mut_node(node_id)?, Node::Parameter);
        match node {
            Node::Acl(acl) => Ok(acl),
            _ => Err(SbroadError::Invalid(
                Entity::Node,
                Some(format!("node is not ACL type: {node:?}")),
            )),
        }
    }
}
