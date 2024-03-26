use std::collections::HashMap;
use std::os::raw::c_int;

use sbroad::errors::{Entity, SbroadError};
use tarantool::tuple::{FunctionArgs, FunctionCtx, Tuple};

use serde::{de::Deserializer, Deserialize, Serialize};

use crate::api::helper::load_config;
use crate::api::COORDINATOR_ENGINE;
use sbroad::executor::engine::{Router, Vshard};
use sbroad::ir::value::{LuaValue, Value};
use sbroad::log::tarantool_error;

#[derive(Debug, Default, PartialEq, Eq)]
/// Tuple with space name and `key:value` map of values
pub struct ArgsMap {
    /// A key:value `HashMap` with key String and custom type Value
    pub rec: HashMap<String, Value>,
    /// Space name as `String`
    pub space: String,
}

/// Custom deserializer of the input function arguments
impl<'de> Deserialize<'de> for ArgsMap {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(rename = "FunctionArgs")]
        struct StructHelper(HashMap<String, LuaValue>, String);

        let mut struct_helper = StructHelper::deserialize(deserializer)?;
        let rec: HashMap<String, Value> = struct_helper
            .0
            .drain()
            .map(|(key, encoded)| (key, Value::from(encoded)))
            .collect();

        Ok(ArgsMap {
            rec,
            space: struct_helper.1,
        })
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
/// Tuple with space name and vec of values
pub struct ArgsTuple {
    /// Vec of custom type Value
    pub rec: Vec<Value>,
    /// Space name as `String`
    pub space: String,
}

/// Custom deserializer of the input function arguments
impl<'de> Deserialize<'de> for ArgsTuple {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(rename = "FunctionArgs")]
        struct StructHelper(Vec<LuaValue>, String);

        let mut struct_helper = StructHelper::deserialize(deserializer)?;
        let rec: Vec<Value> = struct_helper.0.drain(..).map(Value::from).collect();

        Ok(ArgsTuple {
            rec,
            space: struct_helper.1,
        })
    }
}

#[derive(Serialize, Deserialize)]
/// Lua function params
pub struct ArgsString {
    /// The input string for calculating bucket
    pub rec: String,
}

enum Args {
    String(ArgsString),
    Tuple(ArgsTuple),
    Map(ArgsMap),
}

impl TryFrom<FunctionArgs> for Args {
    type Error = SbroadError;

    fn try_from(value: FunctionArgs) -> Result<Self, Self::Error> {
        if let Ok(args) = Tuple::from(&value).decode::<ArgsString>() {
            return Ok(Self::String(args));
        }
        if let Ok(args) = Tuple::from(&value).decode::<ArgsTuple>() {
            return Ok(Self::Tuple(args));
        }
        if let Ok(args) = Tuple::from(&value).decode::<ArgsMap>() {
            return Ok(Self::Map(args));
        }

        Err(SbroadError::ParsingError(
            Entity::Args,
            format!(
                "expected string, tuple with a space name, or map with a space name as an argument, \
                got args {:?}",
                &value
            ),
        ))
    }
}

#[no_mangle]
extern "C" fn calculate_bucket_id(ctx: FunctionCtx, args: FunctionArgs) -> c_int {
    let ret_code = load_config(&COORDINATOR_ENGINE);
    if ret_code != 0 {
        return ret_code;
    }

    COORDINATOR_ENGINE.with(|engine| {
        let runtime = match engine.try_borrow() {
            Ok(runtime) => runtime,
            Err(e) => {
                return tarantool_error(&format!(
                    "Failed to borrow the runtime while calculating a bucket id: {e:?}",
                ));
            }
        };

        let res = match Args::try_from(args) {
            Ok(Args::String(params)) => {
                let bucket_str = Value::from(params.rec);
                runtime.determine_bucket_id(&[&bucket_str])
            }
            Ok(Args::Tuple(params)) => {
                match runtime.extract_sharding_key_from_tuple(params.space, &params.rec) {
                    Ok(tuple) => runtime.determine_bucket_id(&tuple),
                    Err(e) => Err(e),
                }
            }
            Ok(Args::Map(params)) => {
                match runtime.extract_sharding_key_from_map(params.space, &params.rec) {
                    Ok(tuple) => runtime.determine_bucket_id(&tuple),
                    Err(e) => Err(e),
                }
            }
            Err(e) => Err(e),
        };

        match res {
            Ok(bucket_id) => {
                ctx.return_mp(&bucket_id).unwrap();
                0
            }
            Err(e) => tarantool_error(&format!("{e}")),
        }
    })
}
