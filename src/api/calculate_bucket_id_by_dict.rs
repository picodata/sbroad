use std::collections::HashMap;
use std::os::raw::c_int;

use serde::de::Error;
use serde::{de::Deserializer, Deserialize, Serialize};
use tarantool::error::TarantoolErrorCode;
use tarantool::tuple::{FunctionArgs, FunctionCtx, Tuple};

use crate::api::helper::load_metadata;
use crate::api::QUERY_ENGINE;
use crate::errors::QueryPlannerError;
use crate::executor::engine::Engine;
use crate::executor::result::Value;
use crate::ir::value::{AsIrVal, Value as IrValue};

#[derive(Debug, Default, Serialize, PartialEq)]
/// Tuple with space name and `key:value` map of values
struct Args {
    /// Space name as `String`
    space: String,
    /// A key:value `HashMap` with key String and custom type IrValue
    rec: HashMap<String, IrValue>,
}

impl TryFrom<FunctionArgs> for Args {
    type Error = QueryPlannerError;

    fn try_from(value: FunctionArgs) -> Result<Self, Self::Error> {
        Tuple::from(value)
            .into_struct::<Args>()
            .map_err(|e| QueryPlannerError::CustomError(format!("Parsing args error {:?}", e)))
    }
}

/// Custom deserializer of the input function arguments
impl<'de> Deserialize<'de> for Args {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(rename = "Args")]
        struct StructHelper(String, HashMap<String, Value>);

        let struct_helper = StructHelper::deserialize(deserializer)?;

        let mut rec = HashMap::new();
        for (k, v) in struct_helper.1 {
            rec.insert(k, v.as_ir_value().map_err(Error::custom)?);
        }

        Ok(Args {
            space: struct_helper.0,
            rec,
        })
    }
}

/// Calculate the target bucket by a Lua table
#[no_mangle]
pub extern "C" fn calculate_bucket_id_by_dict(ctx: FunctionCtx, args: FunctionArgs) -> c_int {
    let params = match Args::try_from(args) {
        Ok(param) => param,
        Err(e) => return tarantool::set_error!(TarantoolErrorCode::ProcC, "{:?}", e),
    };

    let ret_code = load_metadata();
    if ret_code != 0 {
        return ret_code;
    }
    QUERY_ENGINE.with(|e| {
        let engine = &*e.borrow();

        match engine.extract_sharding_keys(params.space, &params.rec) {
            Ok(tuple) => {
                let bucket_id = engine.determine_bucket_id(&tuple);
                ctx.return_mp(&bucket_id).unwrap();
                0
            }
            Err(e) => tarantool::set_error!(TarantoolErrorCode::ProcC, "{:?}", e),
        }
    })
}
