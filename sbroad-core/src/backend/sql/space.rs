use crate::errors::SbroadError;
use crate::executor::bucket::Buckets;
use crate::executor::ir::ExecutionPlan;
use crate::ir::relation::SpaceEngine;

#[cfg(not(feature = "mock"))]
mod prod_imports {
    pub use crate::error;
    pub use crate::errors::{Action, Entity};
    pub use crate::ir::relation::Column;
    pub use crate::ir::value::{EncodedValue, Value};
    pub use tarantool::index::{FieldType, IndexOptions, IndexType, Part};
    pub use tarantool::space::{Field, Space, SpaceCreateOptions, SpaceType};
    pub use tarantool::tuple::Tuple;
}

#[cfg(not(feature = "mock"))]
use prod_imports::*;

#[allow(clippy::module_name_repetitions)]
#[derive(Hash, Eq, PartialEq, Debug)]
pub struct TmpSpace {
    pub name: String,
}

impl TmpSpace {
    /// Create a temporary space.
    ///
    /// # Errors
    /// - failed to create a space
    #[allow(unused_variables)]
    #[allow(clippy::too_many_lines)]
    pub fn initialize(
        exec_plan: &ExecutionPlan,
        base: &str,
        motion_id: usize,
        buckets: &Buckets,
        engine: &SpaceEngine,
    ) -> Result<TmpSpace, SbroadError> {
        let name = TmpSpace::generate_space_name(base, motion_id);
        #[cfg(not(feature = "mock"))]
        {
            let vtable = exec_plan.get_motion_vtable(motion_id)?;
            let mut fields: Vec<Field> = vtable
                .get_columns()
                .iter()
                .map(|c| Field::from(c.clone()))
                .collect();

            let pk_name = TmpSpace::generate_pk_name(base, motion_id);
            fields.push(Field::unsigned(pk_name.clone()));
            let fields_len = fields.len() as u32;
            // Vinyl engine does not support temporary spaces.
            let space_type = match engine {
                SpaceEngine::Memtx => SpaceType::DataTemporary,
                SpaceEngine::Vinyl => SpaceType::Normal,
            };
            let options = SpaceCreateOptions {
                format: Some(fields),
                engine: engine.into(),
                space_type,
                if_not_exists: false,
                ..Default::default()
            };

            let pk = IndexOptions {
                r#type: Some(IndexType::Tree),
                unique: Some(true),
                parts: Some(vec![Part::new(fields_len, FieldType::Unsigned)]),
                if_not_exists: Some(false),
                ..Default::default()
            };

            let space = Space::create(&name, &options).map_err(|e| {
                SbroadError::FailedTo(Action::Create, Some(Entity::Space), format!("{name}: {e}"))
            })?;
            let cleanup = |space: Space| match space.drop() {
                Ok(_) => {}
                Err(e) => {
                    error!(
                        Option::from("Temporary space"),
                        &format!("Failed to drop {}: {e}", name)
                    );
                }
            };
            match space.create_index(&pk_name, &pk) {
                Ok(_) => {}
                Err(e) => {
                    cleanup(space);
                    return Err(SbroadError::FailedTo(
                        Action::Create,
                        Some(Entity::Index),
                        format!("{pk_name} for space {name}: {e}"),
                    ));
                }
            }
            for (idx, tuples) in vtable.get_tuples_with_buckets(buckets).iter().enumerate() {
                let mut data: Vec<EncodedValue> = Vec::with_capacity(tuples.len() + 1);
                for (v, c) in tuples.iter().zip(vtable.get_columns().iter()) {
                    let casted_value = v.cast(&c.r#type)?;
                    data.push(casted_value);
                }
                data.push(EncodedValue::from(Value::Unsigned(idx as u64)));
                let tuple = match Tuple::new(&data) {
                    Ok(tuple) => tuple,
                    Err(e) => {
                        cleanup(space);
                        return Err(SbroadError::FailedTo(
                            Action::Serialize,
                            Some(Entity::Bytes),
                            format!("to tuple: {e}"),
                        ));
                    }
                };
                match space.insert(&tuple) {
                    Ok(_) => {}
                    Err(e) => {
                        cleanup(space);
                        return Err(SbroadError::FailedTo(
                            Action::Insert,
                            Some(Entity::Tuple),
                            format!("tuple {tuple:?} into {name}: {e}"),
                        ));
                    }
                }
            }
        }
        Ok(TmpSpace { name })
    }

    #[must_use]
    pub fn generate_space_name(base: &str, motion_id: usize) -> String {
        format!("TMP_{base}_{motion_id}")
    }

    #[must_use]
    pub fn generate_pk_name(base: &str, motion_id: usize) -> String {
        format!("PK_TMP_{base}_{motion_id}")
    }
}

impl Drop for TmpSpace {
    fn drop(&mut self) {
        #[cfg(not(feature = "mock"))]
        {
            if let Some(space) = Space::find(&self.name) {
                if let Err(e) = space.drop() {
                    error!(
                        Option::from("Temporary space"),
                        &format!("Failed to drop {} space: {e}", self.name)
                    );
                }
            } else {
                error!(
                    Option::from("Temporary space"),
                    &format!("Failed to find {} space for drop", self.name)
                );
            }
        }
    }
}
