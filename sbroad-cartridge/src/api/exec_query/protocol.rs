use opentelemetry::trace::TraceContextExt;
use opentelemetry::Context;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tarantool::tlua::{self, AsLua, PushGuard, PushInto, PushOneInto, Void};

use sbroad::backend::sql::tree::OrderedSyntaxNodes;
use sbroad::debug;
use sbroad::errors::QueryPlannerError;
use sbroad::executor::ir::{ExecutionPlan, QueryType};
use sbroad::ir::value::Value;
use sbroad::otm::{
    current_id, extract_context, force_trace, get_tracer, inject_context, QueryTracer,
};

#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub struct Binary(Vec<u8>);

impl From<Vec<u8>> for Binary {
    fn from(value: Vec<u8>) -> Self {
        Binary(value)
    }
}

impl<L> PushInto<L> for Binary
where
    L: AsLua,
{
    type Err = Void;

    fn push_into_lua(self, lua: L) -> Result<PushGuard<L>, (Void, L)> {
        let encoded = unsafe { String::from_utf8_unchecked(self.0) };
        encoded.push_into_lua(lua)
    }
}

impl<L> PushOneInto<L> for Binary where L: AsLua {}

#[derive(PushInto, Debug, Deserialize, Serialize, PartialEq)]
pub struct Message {
    required: Binary,
    optional: Binary,
}

impl From<(Binary, Binary)> for Message {
    fn from(value: (Binary, Binary)) -> Self {
        Message {
            required: value.0,
            optional: value.1,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct RequiredData {
    pub(crate) plan_id: String,
    pub(crate) parameters: Vec<Value>,
    pub(crate) query_type: QueryType,
    pub(crate) can_be_cached: bool,
    context: ContextCarrier,
    force_trace: bool,
    trace_id: Option<String>,
}

impl Default for RequiredData {
    fn default() -> Self {
        RequiredData {
            plan_id: String::new(),
            parameters: vec![],
            query_type: QueryType::DQL,
            can_be_cached: true,
            context: ContextCarrier::empty(),
            force_trace: false,
            trace_id: None,
        }
    }
}

impl TryFrom<RequiredData> for Vec<u8> {
    type Error = QueryPlannerError;

    fn try_from(value: RequiredData) -> Result<Self, Self::Error> {
        bincode::serialize(&value).map_err(|e| {
            QueryPlannerError::CustomError(format!(
                "Failed to serialize required data to binary: {:?}",
                e
            ))
        })
    }
}

impl TryFrom<&[u8]> for RequiredData {
    type Error = QueryPlannerError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        bincode::deserialize(value).map_err(|e| {
            QueryPlannerError::CustomError(format!("Failed to deserialize required data: {e:?}"))
        })
    }
}

impl RequiredData {
    pub fn new(
        plan_id: String,
        parameters: Vec<Value>,
        query_type: QueryType,
        can_be_cached: bool,
    ) -> Self {
        let mut carrier = HashMap::new();
        inject_context(&mut carrier);
        let force_trace = force_trace();
        if carrier.is_empty() {
            RequiredData {
                plan_id,
                parameters,
                query_type,
                can_be_cached,
                context: ContextCarrier::empty(),
                force_trace,
                trace_id: None,
            }
        } else {
            RequiredData {
                plan_id,
                parameters,
                query_type,
                can_be_cached,
                context: ContextCarrier::new(carrier),
                force_trace,
                trace_id: Some(current_id()),
            }
        }
    }

    pub fn tracer(&self) -> QueryTracer {
        get_tracer(
            self.force_trace,
            self.trace_id.as_ref(),
            Some(&self.context.payload),
        )
    }

    pub fn id(&self) -> &str {
        match &self.trace_id {
            Some(trace_id) => trace_id,
            None => &self.plan_id,
        }
    }

    pub fn extract_context(&mut self) -> Context {
        (&mut self.context).into()
    }
}

pub struct EncodedRequiredData(Vec<u8>);

impl From<Vec<u8>> for EncodedRequiredData {
    fn from(value: Vec<u8>) -> Self {
        EncodedRequiredData(value)
    }
}

impl From<EncodedRequiredData> for Vec<u8> {
    fn from(value: EncodedRequiredData) -> Self {
        value.0
    }
}

impl TryFrom<RequiredData> for EncodedRequiredData {
    type Error = QueryPlannerError;

    fn try_from(value: RequiredData) -> Result<Self, Self::Error> {
        let bytes: Vec<u8> = value.try_into()?;
        Ok(EncodedRequiredData(bytes))
    }
}

impl TryFrom<EncodedRequiredData> for RequiredData {
    type Error = QueryPlannerError;

    fn try_from(value: EncodedRequiredData) -> Result<Self, Self::Error> {
        let ir: RequiredData = value.0.as_slice().try_into()?;
        Ok(ir)
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct OptionalData {
    pub(crate) exec_plan: ExecutionPlan,
    pub(crate) ordered: OrderedSyntaxNodes,
}

impl TryFrom<OptionalData> for Vec<u8> {
    type Error = QueryPlannerError;

    fn try_from(value: OptionalData) -> Result<Self, Self::Error> {
        bincode::serialize(&value).map_err(|e| {
            QueryPlannerError::CustomError(format!(
                "Failed to serialize required data to binary: {:?}",
                e
            ))
        })
    }
}

impl TryFrom<&[u8]> for OptionalData {
    type Error = QueryPlannerError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        bincode::deserialize(value).map_err(|e| {
            QueryPlannerError::CustomError(format!("Failed to deserialize required data: {e:?}"))
        })
    }
}

impl OptionalData {
    pub fn new(exec_plan: ExecutionPlan, ordered: OrderedSyntaxNodes) -> Self {
        OptionalData { exec_plan, ordered }
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, QueryPlannerError> {
        bincode::serialize(self).map_err(|e| {
            QueryPlannerError::CustomError(format!(
                "Failed to serialize required data to binary: {:?}",
                e
            ))
        })
    }

    pub fn try_from_bytes(bytes: &[u8]) -> Result<Self, QueryPlannerError> {
        bincode::deserialize(bytes).map_err(|e| {
            QueryPlannerError::CustomError(format!("Failed to deserialize required data: {e:?}"))
        })
    }
}

pub struct EncodedOptionalData(Vec<u8>);

impl From<Vec<u8>> for EncodedOptionalData {
    fn from(value: Vec<u8>) -> Self {
        EncodedOptionalData(value)
    }
}

impl From<EncodedOptionalData> for Vec<u8> {
    fn from(value: EncodedOptionalData) -> Self {
        value.0
    }
}

impl TryFrom<OptionalData> for EncodedOptionalData {
    type Error = QueryPlannerError;

    fn try_from(value: OptionalData) -> Result<Self, Self::Error> {
        let bytes: Vec<u8> = value.try_into()?;
        Ok(EncodedOptionalData(bytes))
    }
}

impl TryFrom<EncodedOptionalData> for OptionalData {
    type Error = QueryPlannerError;

    fn try_from(value: EncodedOptionalData) -> Result<Self, Self::Error> {
        let ir: OptionalData = value.0.as_slice().try_into()?;
        Ok(ir)
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct ContextCarrier {
    payload: HashMap<String, String>,
}

impl ContextCarrier {
    pub fn new(payload: HashMap<String, String>) -> Self {
        ContextCarrier { payload }
    }

    pub fn empty() -> Self {
        ContextCarrier {
            payload: HashMap::new(),
        }
    }
}

impl From<&mut ContextCarrier> for Context {
    fn from(carrier: &mut ContextCarrier) -> Self {
        if carrier.payload.is_empty() {
            Context::new()
        } else {
            debug!(
                Option::from("dispatched IR"),
                &format!("Serialized OTM span context: {:?}", carrier.payload),
            );
            let ctx = extract_context(&mut carrier.payload);
            debug!(
                Option::from("dispatched IR"),
                &format!("Deserialized OTM span context: {:?}", ctx.span()),
            );
            ctx
        }
    }
}
