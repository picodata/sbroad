use opentelemetry::Context;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tarantool::tlua::{self, AsLua, PushGuard, PushInto, PushOneInto, Void};

use crate::backend::sql::tree::OrderedSyntaxNodes;
use crate::debug;
use crate::errors::{Action, Entity, SbroadError};
use crate::executor::ir::{ExecutionPlan, QueryType};
use crate::ir::value::Value;
use crate::otm::{current_id, current_tracer, extract_context, inject_context, QueryTracer};

use crate::ir::Options;
#[cfg(not(feature = "mock"))]
use opentelemetry::trace::TraceContextExt;

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

/// Set of fields needed for query execution.
/// See `RequiredData` and `OptionalData` for more info (they are transformed from `Binary`
/// and backwards).
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
    // Unique ID for concrete plan represented in a view of BLAKE3 hash.
    // Needed for plans execution results being cached
    // (e.g. see `encode_plan` -> `QueryType::DQL` -> `pattern_id`).
    pub plan_id: String,
    pub parameters: Vec<Value>,
    pub query_type: QueryType,
    pub can_be_cached: bool,
    context: ContextCarrier,
    tracer: QueryTracer,
    trace_id: Option<String>,
    pub options: Options,
}

impl Default for RequiredData {
    fn default() -> Self {
        RequiredData {
            plan_id: String::new(),
            parameters: vec![],
            query_type: QueryType::DQL,
            can_be_cached: true,
            context: ContextCarrier::empty(),
            tracer: QueryTracer::default(),
            trace_id: None,
            options: Options::default(),
        }
    }
}

impl TryFrom<RequiredData> for Vec<u8> {
    type Error = SbroadError;

    fn try_from(value: RequiredData) -> Result<Self, Self::Error> {
        bincode::serialize(&value).map_err(|e| {
            SbroadError::FailedTo(
                Action::Serialize,
                Some(Entity::RequiredData),
                format!("to binary: {e:?}"),
            )
        })
    }
}

impl TryFrom<&[u8]> for RequiredData {
    type Error = SbroadError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        bincode::deserialize(value).map_err(|e| {
            SbroadError::FailedTo(
                Action::Deserialize,
                Some(Entity::RequiredData),
                format!("{e:?}"),
            )
        })
    }
}

impl RequiredData {
    #[must_use]
    pub fn new(
        plan_id: String,
        parameters: Vec<Value>,
        query_type: QueryType,
        can_be_cached: bool,
        options: Options,
    ) -> Self {
        let mut carrier = HashMap::new();
        inject_context(&mut carrier);
        let tracer = current_tracer();
        if carrier.is_empty() {
            RequiredData {
                plan_id,
                parameters,
                query_type,
                can_be_cached,
                context: ContextCarrier::empty(),
                tracer,
                trace_id: None,
                options,
            }
        } else {
            RequiredData {
                plan_id,
                parameters,
                query_type,
                can_be_cached,
                context: ContextCarrier::new(carrier),
                tracer,
                trace_id: Some(current_id()),
                options,
            }
        }
    }

    #[must_use]
    pub fn tracer(&self) -> QueryTracer {
        self.tracer.clone()
    }

    #[must_use]
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
    type Error = SbroadError;

    fn try_from(value: RequiredData) -> Result<Self, Self::Error> {
        let bytes: Vec<u8> = value.try_into()?;
        Ok(EncodedRequiredData(bytes))
    }
}

impl TryFrom<EncodedRequiredData> for RequiredData {
    type Error = SbroadError;

    fn try_from(value: EncodedRequiredData) -> Result<Self, Self::Error> {
        let ir: RequiredData = value.0.as_slice().try_into()?;
        Ok(ir)
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct OptionalData {
    pub exec_plan: ExecutionPlan,
    pub ordered: OrderedSyntaxNodes,
}

impl TryFrom<OptionalData> for Vec<u8> {
    type Error = SbroadError;

    fn try_from(value: OptionalData) -> Result<Self, Self::Error> {
        bincode::serialize(&value).map_err(|e| {
            SbroadError::FailedTo(
                Action::Serialize,
                Some(Entity::RequiredData),
                format!("to binary: {e:?}"),
            )
        })
    }
}

impl TryFrom<&[u8]> for OptionalData {
    type Error = SbroadError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        bincode::deserialize(value).map_err(|e| {
            SbroadError::FailedTo(
                Action::Deserialize,
                Some(Entity::RequiredData),
                format!("{e:?}"),
            )
        })
    }
}

impl OptionalData {
    #[must_use]
    pub fn new(exec_plan: ExecutionPlan, ordered: OrderedSyntaxNodes) -> Self {
        OptionalData { exec_plan, ordered }
    }

    /// Serialize to bytes
    ///
    /// # Errors
    /// - If the `OptionalData` cannot be serialized to bytes.
    pub fn to_bytes(&self) -> Result<Vec<u8>, SbroadError> {
        bincode::serialize(self).map_err(|e| {
            SbroadError::FailedTo(
                Action::Serialize,
                Some(Entity::RequiredData),
                format!("to binary: {e:?}"),
            )
        })
    }

    /// Try to deserialize from bytes
    ///
    /// # Errors
    /// - If the bytes cannot be deserialized to an `OptionalData`.
    pub fn try_from_bytes(bytes: &[u8]) -> Result<Self, SbroadError> {
        bincode::deserialize(bytes).map_err(|e| {
            SbroadError::FailedTo(
                Action::Deserialize,
                Some(Entity::OptionalData),
                format!("{e:?}"),
            )
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
    type Error = SbroadError;

    fn try_from(value: OptionalData) -> Result<Self, Self::Error> {
        let bytes: Vec<u8> = value.try_into()?;
        Ok(EncodedOptionalData(bytes))
    }
}

impl TryFrom<EncodedOptionalData> for OptionalData {
    type Error = SbroadError;

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
    #[must_use]
    pub fn new(payload: HashMap<String, String>) -> Self {
        ContextCarrier { payload }
    }

    #[must_use]
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
