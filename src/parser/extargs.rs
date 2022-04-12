use std::{collections::HashMap, convert::TryFrom, ops::Deref};

use serde::{de::Deserializer, Deserialize, Serialize};
use tarantool::tuple::{AsTuple, FunctionArgs, Tuple};

use crate::{errors::QueryPlannerError, executor::result::Value, ir::value::Value as IrValue};

#[derive(Serialize, Deserialize)]
pub struct BucketCalcArgs {
    pub rec: String,
}

impl AsTuple for BucketCalcArgs {}

#[derive(Debug, Default, Serialize, PartialEq)]
/// Tuple with space name and `key:value` map of values
pub struct BucketCalcArgsDict {
    /// Space name as `String`
    pub space: String,
    /// A key:value `HashMap` with key String and custom type IrValue
    pub rec: HashMap<String, IrValue>,
}

impl Deref for BucketCalcArgsDict {
    type Target = HashMap<String, IrValue>;

    fn deref(&self) -> &Self::Target {
        &self.rec
    }
}

impl TryFrom<FunctionArgs> for BucketCalcArgsDict {
    type Error = QueryPlannerError;

    fn try_from(value: FunctionArgs) -> Result<Self, Self::Error> {
        Tuple::from(value)
            .into_struct::<BucketCalcArgsDict>()
            .map_err(|e| {
                QueryPlannerError::CustomError(format!(
                    "Error then deserializing tuple into BucketCalcArgsDict! {:?}",
                    e
                ))
            })
    }
}

impl<'de> Deserialize<'de> for BucketCalcArgsDict {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(rename = "BucketCalcArgsDict")]
        struct StructHelper(String, HashMap<String, Value>);

        let struct_helper = StructHelper::deserialize(deserializer)?;
        Ok(BucketCalcArgsDict {
            space: struct_helper.0,
            rec: struct_helper
                .1
                .into_iter()
                .map(|(k, v)| (k, IrValue::from(v)))
                .collect(),
        })
    }
}

impl AsTuple for BucketCalcArgsDict {}

#[cfg(test)]
mod tests;
