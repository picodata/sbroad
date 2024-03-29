use smol_str::{SmolStr, ToSmolStr};

use crate::errors::{Entity, SbroadError};

pub mod meta;
pub mod runtime;

pub struct Statement {
    id: u32,
    pattern: SmolStr,
}

#[derive(Default)]
pub struct PreparedStmt(Option<Statement>);

impl PreparedStmt {
    /// Extract prepared statement from the cache.
    ///
    /// # Errors
    /// - Returns None instead of a regular statement (sentinel node in the cache).
    pub fn statement(&self) -> Result<&Statement, SbroadError> {
        self.0.as_ref().ok_or_else(|| {
            SbroadError::Invalid(
                Entity::Statement,
                Some("Statement is not prepared".to_smolstr()),
            )
        })
    }

    /// Get the id of the prepared statement.
    ///
    /// # Errors
    /// - Returns sentinel node from the cache (internal error).
    pub fn id(&self) -> Result<u32, SbroadError> {
        Ok(self.statement()?.id)
    }

    /// Get the pattern of the prepared statement.
    ///
    /// # Errors
    /// - Returns sentinel node from the cache (internal error).
    pub fn pattern(&self) -> Result<&str, SbroadError> {
        Ok(&self.statement()?.pattern)
    }
}

impl std::fmt::Debug for PreparedStmt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(ref stmt) = self.0 {
            write!(f, "PreparedStmt {:?}", stmt.pattern)
        } else {
            write!(f, "PreparedStmt None")
        }
    }
}
