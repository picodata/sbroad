use std::collections::HashMap;
use std::os::raw::c_int;

use crate::errors::QueryPlannerError;
use tarantool::error::TarantoolErrorCode;
use tarantool::tuple::{FunctionArgs, FunctionCtx, Tuple};

use serde::{de::Deserializer, Deserialize, Serialize};

use crate::api::helper::load_config;
use crate::api::COORDINATOR_ENGINE;
use crate::executor::engine::Coordinator;
use crate::ir::value::Value;

#[derive(Debug, Default, Serialize, PartialEq)]
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
        #[serde(rename = "Args")]
        struct StructHelper(HashMap<String, Value>, String);

        let struct_helper = StructHelper::deserialize(deserializer)?;

        Ok(ArgsMap {
            rec: struct_helper.0,
            space: struct_helper.1,
        })
    }
}

#[derive(Debug, Default, Serialize, PartialEq, Clone)]
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
        #[serde(rename = "Args")]
        struct StructHelper(Vec<Value>, String);

        let struct_helper = StructHelper::deserialize(deserializer)?;

        Ok(ArgsTuple {
            rec: struct_helper.0,
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
    type Error = QueryPlannerError;

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

        return Err(QueryPlannerError::CustomError(format!(
            "Parsing args {:?} error, \
            expected string, tuple with a space name, or map with a space name as an argument",
            &value
        )));
    }
}

#[no_mangle]
pub extern "C" fn calculate_bucket_id(ctx: FunctionCtx, args: FunctionArgs) -> c_int {
    let ret_code = load_config(&COORDINATOR_ENGINE);
    if ret_code != 0 {
        return ret_code;
    }

    COORDINATOR_ENGINE.with(|engine| {
        let runtime = match engine.try_borrow() {
            Ok(runtime) => runtime,
            Err(e) => {
                return tarantool::set_error!(
                    TarantoolErrorCode::ProcC,
                    "Failed to borrow the runtime while calculating a bucket id: {:?}",
                    e
                )
            }
        };

        let res = match Args::try_from(args) {
            Ok(Args::String(params)) => {
                let bucket_str = Value::from(params.rec);
                let bucket_id = runtime.determine_bucket_id(&[&bucket_str]);

                Ok(bucket_id)
            }
            Ok(Args::Tuple(params)) => {
                match runtime.extract_sharding_keys_from_tuple(params.space, &params.rec) {
                    Ok(tuple) => {
                        let bucket_id = runtime.determine_bucket_id(&tuple);
                        Ok(bucket_id)
                    }
                    Err(e) => Err(e),
                }
            }
            Ok(Args::Map(params)) => {
                match runtime.extract_sharding_keys_from_map(params.space, &params.rec) {
                    Ok(tuple) => {
                        let bucket_id = runtime.determine_bucket_id(&tuple);
                        Ok(bucket_id)
                    }
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
            Err(e) => tarantool::set_error!(TarantoolErrorCode::ProcC, "{:?}", e),
        }
    })
}
