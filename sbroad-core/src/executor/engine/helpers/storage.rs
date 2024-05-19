use rmp::encode::write_array_len;
use sbroad_proc::otm_child_span;
use smol_str::{format_smolstr, SmolStr};
use std::any::Any;
use std::collections::HashMap;
use std::io::{Read, Write};
use std::sync::OnceLock;
use tarantool::session::with_su;
use tarantool::space::Space;
use tarantool::sql::{prepare_and_execute_raw, Statement};
use tarantool::tuple::{Tuple, TupleBuffer};

use crate::backend::sql::space::ADMIN_ID;
use crate::error;
use crate::errors::SbroadError;
use crate::executor::engine::helpers::proxy::sql_cache_proxy;
use crate::executor::engine::helpers::table_name;
use crate::executor::lru::DEFAULT_CAPACITY;
use crate::ir::value::{EncodedValue, Value};
use crate::ir::expression::NodeId;
use crate::otm::child_span;
use crate::utils::ByteCounter;

const IPROTO_DATA: u8 = 0x30;
const IPROTO_META: u8 = 0x32;

pub enum DQLStorageReturnFormat {
    // Return result as a vector of bytes, no need
    // to allocate Tuple. Used for global tables
    // reads (exec_ir_on_any) or DML queries, that
    // materialize reading subtree locally.
    Raw,
    // Return result as a Tuple of three elements:
    // [rows count, cache miss flag, producer result]
    // Used by all dql queries who go through vshard
    // (and so iproto), where we always must return
    // Tuple
    Vshard,
}

/// Storage runtime configuration.
#[allow(clippy::module_name_repetitions)]
pub struct StorageMetadata {
    /// Prepared statements cache capacity (on the storage).
    pub storage_capacity: usize,
    /// Prepared statements cache size in bytes (on the storage).
    /// If a new statement is bigger doesn't fit into the cache,
    /// it would not be cached but executed directly.
    pub storage_size_bytes: usize,
}

impl Default for StorageMetadata {
    fn default() -> Self {
        Self::new()
    }
}

impl StorageMetadata {
    #[must_use]
    pub fn new() -> Self {
        StorageMetadata {
            storage_capacity: DEFAULT_CAPACITY,
            storage_size_bytes: 1024 * DEFAULT_CAPACITY,
        }
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.storage_capacity == 0 && self.storage_size_bytes == 0
    }
}

#[otm_child_span("tarantool.statement.prepare")]
pub fn prepare(pattern: String) -> Result<Statement, SbroadError> {
    let proxy = sql_cache_proxy();
    let stmt = proxy.prepare(pattern).map_err(|e| {
        error!(Option::from("prepare"), &format!("{e:?}"));
        SbroadError::from(e)
    })?;
    Ok(stmt)
}

#[otm_child_span("tarantool.statement.unprepare")]
pub fn unprepare(
    plan_id: &SmolStr,
    entry: &mut (Statement, Vec<NodeId>),
) -> Result<(), SbroadError> {
    let (stmt, table_ids) = std::mem::take(entry);

    // Remove the statement from the instance cache.
    let proxy = sql_cache_proxy();
    proxy.unprepare(stmt).map_err(|e| {
        error!(Option::from("unprepare"), &format!("{e:?}"));
        SbroadError::from(e)
    })?;

    // Remove temporary tables from the instance.
    for node_id in table_ids {
        let table = table_name(plan_id, node_id);
        Space::find(table.as_str()).map(|space| with_su(ADMIN_ID, || space.drop()));
    }

    Ok(())
}

type EncodedMetadata = rmpv::Value;
type EncodedData = rmpv::Value;

fn parse_dql(stream: &mut (impl Read + Sized)) -> (EncodedMetadata, EncodedData) {
    let map_len = rmp::decode::read_map_len(stream).expect("failed to read DQL result");
    assert_eq!(map_len, 2, "invalid format for DQL result");
    let mut meta = None;
    let mut data = None;
    for _ in 0..map_len {
        let key = rmp::decode::read_pfix(stream).expect("failed to read IPROTO key");
        match key {
            IPROTO_META => {
                meta = Some(rmpv::decode::read_value(stream).expect("failed to read DQL metadata"));
            }
            IPROTO_DATA => {
                data = Some(rmpv::decode::read_value(stream).expect("failed to read DQL data"));
            }
            _ => {
                unreachable!("unexpected IPROTO key: {}", key);
            }
        }
    }
    (
        meta.expect("missing DQL metadata"),
        data.expect("missing DQL data"),
    )
}

fn repack_meta(meta: EncodedMetadata) -> (rmpv::Value, rmpv::Value) {
    let mut maps = rmpv::ext::from_value::<Vec<HashMap<u32, rmpv::Value>>>(meta)
        .expect("failed to decode DQL metadata");
    let mut repacked = Vec::with_capacity(2);
    for mut map in maps.drain(..) {
        let col_name = map.remove(&0).expect("missing name in DQL metadata");
        let col_type = map.remove(&1).expect("missing type in DQL metadata");
        // repacked.push(rmpv::Value::Map(vec![(col_name, col_type)]));
        repacked.push(rmpv::Value::Map(vec![
            (rmpv::Value::String("name".into()), col_name),
            (rmpv::Value::String("type".into()), col_type),
        ]));
    }
    ("metadata".into(), rmpv::Value::Array(repacked))
}

fn repack_data(
    data: EncodedData,
    max_rows: u64,
) -> Result<(rmpv::Value, rmpv::Value), SbroadError> {
    let rows = rmpv::ext::from_value::<Vec<rmpv::Value>>(data).expect("failed to decode DQL data");
    if rows.len() as u64 > max_rows {
        return Err(SbroadError::UnexpectedNumberOfValues(format_smolstr!(
            "expected at most {} rows, got {}",
            max_rows,
            rows.len()
        )));
    }
    Ok(("rows".into(), rmpv::Value::Array(rows)))
}

pub(crate) fn dql_cache_miss_result() -> Tuple {
    const RESULT_SIZE: usize = 20;

    fn create_result<W>(buf_ref: &mut W)
    where
        W: Write,
    {
        let empty_result = {
            let new_meta: (rmpv::Value, rmpv::Value) =
                ("metadata".into(), rmpv::Value::Array(vec![]));
            let new_data: (rmpv::Value, rmpv::Value) = ("rows".into(), rmpv::Value::Array(vec![]));
            rmpv::Value::Map(vec![new_meta, new_data])
        };

        write_array_len(buf_ref, 3).expect("failed to write array length");
        // write number of rows in result
        rmpv::encode::write_value(buf_ref, &rmpv::Value::from(0))
            .expect("failed to write rows count");
        // write cache miss flag
        rmpv::encode::write_value(buf_ref, &rmpv::Value::from(true))
            .expect("failed to write cache miss flag");
        rmpv::encode::write_value(buf_ref, &empty_result).expect("failed to write result");
    }

    fn check_result_size() -> bool {
        let mut buf = Vec::with_capacity(100);
        create_result(&mut buf);
        buf.len() == RESULT_SIZE
    }

    debug_assert!(check_result_size());

    static RESULT_BYTES: OnceLock<[u8; RESULT_SIZE]> = OnceLock::new();
    let bytes = RESULT_BYTES.get_or_init(|| {
        let mut res = [0; RESULT_SIZE];
        create_result(&mut res.as_mut_slice());
        res
    });

    Tuple::try_from_slice(bytes.as_slice()).expect("failed to create cache miss tuple")
}

fn dql_result_to_tuple(
    stream: &mut (impl Read + Sized),
    max_rows: u64,
    format: &DQLStorageReturnFormat,
) -> Result<Box<dyn Any>, SbroadError> {
    let (meta, data) = parse_dql(stream);
    let rows_len: u64 = if let rmpv::Value::Array(rows) = &data {
        rows.len() as u64
    } else {
        panic!("Unexpected rows format!");
    };
    let new_meta = repack_meta(meta);
    let new_data = repack_data(data, max_rows)?;
    let repacked = rmpv::Value::Map(vec![new_meta, new_data]);
    // To pre-allocate the buffer, we need to know the size of the encoded value.
    let mut byte_counter = ByteCounter::default();
    rmpv::encode::write_value(&mut byte_counter, &repacked).expect("failed to write result");
    let mut buf = Vec::with_capacity(byte_counter.bytes());

    let res: Box<dyn Any> = match format {
        DQLStorageReturnFormat::Raw => {
            write_array_len(&mut buf, 1).expect("failed to write array length");
            rmpv::encode::write_value(&mut buf, &repacked).expect("failed to write result");
            Box::new(buf)
        }
        DQLStorageReturnFormat::Vshard => {
            write_array_len(&mut buf, 3).expect("failed to write array length");
            rmpv::encode::write_value(&mut buf, &rmpv::Value::from(rows_len))
                .expect("failed to write rows count");
            rmpv::encode::write_value(&mut buf, &rmpv::Value::from(false))
                .expect("failed to write cache miss flag");
            let inner = rmpv::Value::Array(vec![repacked]);
            rmpv::encode::write_value(&mut buf, &inner).expect("failed to write result");
            let tup_buf =
                TupleBuffer::try_from_vec(buf).expect("failed to convert to tuple buffer");
            Box::new(Tuple::from(&tup_buf))
        }
    };
    Ok(res)
}

fn encoded_params(params: &[Value]) -> Vec<EncodedValue> {
    params.iter().map(EncodedValue::from).collect()
}

#[otm_child_span("tarantool.statement.prepared.execute")]
pub fn execute_prepared(
    stmt: &Statement,
    params: &[Value],
    vdbe_max_steps: u64,
    max_rows: u64,
    format: &DQLStorageReturnFormat,
) -> Result<Box<dyn Any>, SbroadError> {
    let encoded_params = encoded_params(params);
    let mut stream = with_su(ADMIN_ID, || {
        stmt.execute_raw(&encoded_params, vdbe_max_steps)
    })??;
    dql_result_to_tuple(&mut stream, max_rows, format)
}

#[otm_child_span("tarantool.statement.unprepared.execute")]
pub fn execute_unprepared(
    query: &str,
    params: &[Value],
    vdbe_max_steps: u64,
    max_rows: u64,
    format: &DQLStorageReturnFormat,
) -> Result<Box<dyn Any>, SbroadError> {
    let encoded_params = encoded_params(params);
    let mut stream = with_su(ADMIN_ID, || {
        prepare_and_execute_raw(query, &encoded_params, vdbe_max_steps)
    })??;
    dql_result_to_tuple(&mut stream, max_rows, format)
}
