use opentelemetry::Context;
use rmp::decode::{read_array_len, Bytes, RmpRead};
use serde::{Deserialize, Serialize};
use smol_str::{format_smolstr, SmolStr};
use std::collections::HashMap;
use tarantool::tlua::{self, AsLua, Push, PushGuard, PushInto, PushOne, PushOneInto, Void};
use tarantool::tuple::{Tuple, TupleBuffer};

use crate::backend::sql::tree::OrderedSyntaxNodes;
use crate::debug;
use crate::errors::{Action, Entity, SbroadError};
use crate::executor::ir::{ExecutionPlan, QueryType};
use crate::ir::value::Value;
use crate::otm::{current_id, extract_context, inject_context};

use crate::executor::engine::TableVersionMap;
use crate::ir::{NodeId, Options};
#[cfg(not(feature = "mock"))]
use opentelemetry::trace::TraceContextExt;

use super::engine::helpers::vshard::CacheInfo;
use super::vtable::VirtualTableMeta;

pub type VTablesMeta = HashMap<usize, VirtualTableMeta>;

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
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

impl<L> Push<L> for Binary
where
    L: AsLua,
{
    type Err = Void;

    fn push_to_lua(&self, lua: L) -> Result<PushGuard<L>, (Self::Err, L)> {
        let encoded = unsafe { std::str::from_utf8_unchecked(&self.0) };
        encoded.push_to_lua(lua)
    }
}

impl<L> PushOne<L> for Binary where L: AsLua {}
impl<L> PushOneInto<L> for Binary where L: AsLua {}

#[derive(PushInto, Push, Debug, Deserialize, Serialize, PartialEq)]
pub struct RequiredMessage {
    pub required: Binary,
    pub cache_hint: CacheInfo,
}

impl From<Binary> for RequiredMessage {
    fn from(value: Binary) -> Self {
        RequiredMessage {
            required: value,
            cache_hint: CacheInfo::CacheableFirstRequest,
        }
    }
}

#[derive(PushInto, Push, Debug, Deserialize, Serialize, PartialEq)]
pub struct FullMessage {
    pub required: Binary,
    pub optional: Binary,
    pub cache_hint: CacheInfo,
}

impl FullMessage {
    #[must_use]
    pub fn new(required: Binary, optional: Binary) -> Self {
        FullMessage {
            required,
            optional,
            cache_hint: CacheInfo::CacheableSecondRequest,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Default)]
pub struct SchemaInfo {
    pub router_version_map: TableVersionMap,
}

impl SchemaInfo {
    #[must_use]
    pub fn new(router_version_map: TableVersionMap) -> Self {
        SchemaInfo { router_version_map }
    }
}

/// Helper struct for storing tracing related information
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct TracingMetadata {
    /// Context passed between nodes
    pub context: ContextCarrier,
    /// Id of a trace
    pub trace_id: String,
}

impl TracingMetadata {
    #[must_use]
    pub fn new(context: ContextCarrier, trace_id: String) -> Self {
        Self { context, trace_id }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct EncodedRows {
    /// Lengths of encoded rows.
    pub marking: Vec<usize>,
    /// Encoded rows as msgpack array.
    pub encoded: Binary,
}

impl EncodedRows {
    #[must_use]
    pub fn new(marking: Vec<usize>, encoded: Binary) -> Self {
        EncodedRows { marking, encoded }
    }

    #[must_use]
    pub fn iter(&self) -> EncodedRowsIter {
        <&Self as IntoIterator>::into_iter(self)
    }
}

impl<'e> IntoIterator for &'e EncodedRows {
    type Item = Tuple;
    type IntoIter = EncodedRowsIter<'e>;

    fn into_iter(self) -> Self::IntoIter {
        let capacity = *self.marking.iter().max().unwrap_or(&0);
        EncodedRowsIter {
            stream: Bytes::from(self.encoded.0.as_slice()),
            marking: &self.marking,
            position: 0,
            // Allocate buffer for encoded row.
            buf: Vec::with_capacity(capacity),
        }
    }
}

pub struct EncodedRowsIter<'e> {
    /// Encoded tuples as msgpack array stream.
    stream: Bytes<'e>,
    /// Lengths of encoded rows.
    marking: &'e [usize],
    /// Current stream position.
    position: usize,
    /// Buffer for encoded tuple.
    buf: Vec<u8>,
}

impl<'e> Iterator for EncodedRowsIter<'e> {
    type Item = Tuple;

    fn next(&mut self) -> Option<Self::Item> {
        let cur_pos = self.position;
        self.position += 1;
        if cur_pos == 0 {
            let array_len = read_array_len(&mut self.stream).expect("encoded rows length");
            assert_eq!(array_len as usize, self.marking.len());
        }
        let Some(row_len) = self.marking.get(cur_pos) else {
            return None;
        };
        // We take a preallocated buffer, clear it (preserving allocated rust memory) and
        // fill it with encoded tuple (copying from stream). Then we create a tuple from
        // this buffer (i.e. allocate tarantool memory and copy from buffer) and return
        // rust memory back to the buffer vector. As a result we make only one tarantool
        // memory allocation per tuple and two copies of encoded tuple bytes.

        // TODO: refactor this code when we switch to rust-allocated tuples.
        self.buf.clear();
        let mut tmp = Vec::new();
        std::mem::swap(&mut self.buf, &mut tmp);
        tmp.resize(*row_len, 0);
        self.stream.read_exact_buf(&mut tmp).expect("encoded tuple");
        let tbuf = TupleBuffer::try_from_vec(tmp).expect("tuple buffer");
        let tuple = Tuple::from(&tbuf);
        // Return back previously allocated buffer.
        self.buf = Vec::from(tbuf);
        Some(tuple)
    }
}

/// Encoded virtual tables data.
pub type EncodedTables = HashMap<NodeId, EncodedRows>;

/// Query data used for executing of the cached statements.
/// Note that it contains only meta-information about SQL query (
/// e.g. cached plan id and params).
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct RequiredData {
    /// A unique ID of the IR subtree that is used as a key for prepared statements
    /// in tarantool SQL cache.
    pub plan_id: SmolStr,
    pub parameters: Vec<Value>,
    pub query_type: QueryType,
    pub options: Options,
    pub schema_info: SchemaInfo,
    pub tracing_meta: Option<TracingMetadata>,
    pub tables: EncodedTables,
}

impl Default for RequiredData {
    fn default() -> Self {
        RequiredData {
            plan_id: SmolStr::default(),
            parameters: vec![],
            query_type: QueryType::DQL,
            options: Options::default(),
            schema_info: SchemaInfo::default(),
            tracing_meta: None,
            tables: EncodedTables::default(),
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
                format_smolstr!("to binary: {e:?}"),
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
                format_smolstr!("{e:?}"),
            )
        })
    }
}

impl RequiredData {
    #[must_use]
    pub fn new(
        plan_id: SmolStr,
        parameters: Vec<Value>,
        query_type: QueryType,
        options: Options,
        schema_info: SchemaInfo,
        tables: EncodedTables,
    ) -> Self {
        let mut tracing_meta = None;
        if let Some(trace_id) = current_id() {
            let mut carrier = HashMap::new();
            inject_context(&mut carrier);
            tracing_meta = Some(TracingMetadata::new(ContextCarrier::new(carrier), trace_id));
        }

        RequiredData {
            plan_id,
            parameters,
            query_type,
            options,
            schema_info,
            tracing_meta,
            tables,
        }
    }

    #[must_use]
    pub fn plan_id(&self) -> &str {
        &self.plan_id
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

/// Query data used for executing non-cachable queries.
/// Note that it contains a full plan with all needed vtables.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct OptionalData {
    pub exec_plan: ExecutionPlan,
    pub ordered: OrderedSyntaxNodes,
    pub vtables_meta: VTablesMeta,
}

impl TryFrom<OptionalData> for Vec<u8> {
    type Error = SbroadError;

    fn try_from(value: OptionalData) -> Result<Self, Self::Error> {
        bincode::serialize(&value).map_err(|e| {
            SbroadError::FailedTo(
                Action::Serialize,
                Some(Entity::OptionalData),
                format_smolstr!("to binary: {e:?}"),
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
                Some(Entity::OptionalData),
                format_smolstr!("{e:?}"),
            )
        })
    }
}

impl OptionalData {
    #[must_use]
    pub fn new(
        exec_plan: ExecutionPlan,
        ordered: OrderedSyntaxNodes,
        vtables_meta: VTablesMeta,
    ) -> Self {
        OptionalData {
            exec_plan,
            ordered,
            vtables_meta,
        }
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
                format_smolstr!("to binary: {e:?}"),
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
                format_smolstr!("{e:?}"),
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
