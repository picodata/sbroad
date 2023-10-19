//! Statistics table module.
//!
//! This module contains the implementation of the statistics spaces used by
//! the `statistics` span processor. There are two spaces and one hash table
//! used to store the statistics:
//! - `_sql_query` space - stores the queries that are currently being executed.
//!   Its query id is used as a key for the `_sql_stat` space.
//! - `_sql_stat` space - stores the statistics for the query spans. The spans
//!   are stored as a flat tree for each query.
//! - `SpanMap` hash table - stores the mapping between the span id and the span name.
//!   The reason is that the span context contains only the span id, so we use
//!   this table to save the span name when create the span (and remove it when
//!   the span is finished).
//!
//! Keep in mind that the spaces are created in a "lazy" way, i.e. they are created
//! only when statistics processor tries to write to them. As we collect statistics
//! only for 1% of the queries, there is a chance that right after the instance start
//! the statistics spaces still would not exist. Though it is not a problem, such
//! situation can surprise the user. Also we should remember that read-only replicas
//! can not create spaces (even the temporary ones). As a result, the spaces can be
//! "lazily" created on the storages only when some of them are dispatched to the
//! master instance.

use ahash::AHashMap;
use index::Part;
use opentelemetry::trace::SpanId;
use space::Field;
use std::borrow::Cow;
use std::cell::RefCell;
use tarantool::index::{FieldType, IndexOptions, IndexType, IteratorType};
use tarantool::space::{Space, SpaceCreateOptions, SpaceEngineType, SpaceType};
use tarantool::{index, space};

use crate::{debug, warn};

thread_local!(pub static QUERY: RefCell<QuerySpace> = RefCell::new(QuerySpace::new()));
thread_local!(pub static SPAN: RefCell<SpanMap> = RefCell::new(SpanMap::new()));
thread_local!(pub static STAT: RefCell<StatSpace> = RefCell::new(StatSpace::new()));

pub trait RustMap {
    type Key;
    type Value;

    fn new() -> Self;
    fn get(&self, key: &Self::Key) -> Option<&Self::Value>;
    fn get_mut(&mut self, key: &Self::Key) -> Option<&mut Self::Value>;
    fn push(&mut self, key: Self::Key, value: Self::Value);
    fn pop(&mut self, key: &Self::Key) -> Option<Self::Value>;
}

pub struct SpanMap(AHashMap<SpanId, SpanName>);

impl RustMap for SpanMap {
    type Key = SpanId;
    type Value = SpanName;

    fn new() -> Self {
        Self(AHashMap::new())
    }

    fn get(&self, key: &Self::Key) -> Option<&Self::Value> {
        self.0.get(key)
    }

    fn get_mut(&mut self, key: &Self::Key) -> Option<&mut Self::Value> {
        self.0.get_mut(key)
    }

    fn push(&mut self, key: Self::Key, value: Self::Value) {
        self.0.insert(key, value);
    }

    fn pop(&mut self, key: &Self::Key) -> Option<Self::Value> {
        self.0.remove(key)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct SpanName(String);

impl SpanName {
    #[must_use]
    pub fn value(&self) -> &str {
        &self.0
    }
}

impl From<&str> for SpanName {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

impl From<Cow<'static, str>> for SpanName {
    fn from(s: Cow<'static, str>) -> Self {
        Self(s.to_string())
    }
}

pub trait TarantoolSpace {
    type Key;
    type Tuple;

    fn new() -> Self;
    fn get(&mut self, key: &Self::Key) -> Option<Self::Tuple>;
    fn upsert(&mut self, tuple: Self::Tuple);
    fn delete(&mut self, key: &Self::Key) -> Option<Self::Tuple>;

    /// Space name.
    fn name(&self) -> &str;

    /// Reference to the cached space.
    fn space(&self) -> &Option<Space>;

    /// Mutable reference to the cached space.
    fn space_mut(&mut self) -> &mut Option<Space>;

    /// The space can be created only on the master node of the
    /// replica set. The read-only replicas can't create spaces
    /// (DDL would be replicated from master some moment later).
    fn try_create_space(&mut self);

    /// Update the cached space information. This method should
    /// be called before any space operation.
    fn space_update_cache(&mut self) {
        let name = self.name().to_string();
        match self.space_mut() {
            Some(space) => {
                let space_id = space.id();
                // Find the space to validate the its id.
                if let Some(found_space) = Space::find(&name) {
                    // The space was recreated by user, need to update the cache.
                    if found_space.id() != space_id {
                        warn!(
                            Option::from("space"),
                            &format!(
                                "Space {} was found with different id: {} != {}",
                                &name,
                                space_id,
                                found_space.id()
                            )
                        );
                        *space = found_space;
                    }
                } else {
                    // Someone has removed the space from the instance.
                    // Remove the cached space and try to create it again.
                    *self.space_mut() = None;
                    self.try_create_space();
                }
            }
            None => {
                // Try to find the space.
                if let Some(space) = Space::find(&name) {
                    *self.space_mut() = Some(space);
                } else {
                    // Try to create the space.
                    self.try_create_space();
                }
            }
        }
    }
}

pub struct QuerySpace {
    space: Option<Space>,
    name: String,
}

impl TarantoolSpace for QuerySpace {
    type Key = (String,);
    type Tuple = (String, String, u64);

    fn new() -> Self {
        let mut space = Self {
            space: None,
            name: "_sql_query".to_string(),
        };
        space.space_update_cache();
        space
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn space(&self) -> &Option<Space> {
        &self.space
    }

    fn space_mut(&mut self) -> &mut Option<Space> {
        &mut self.space
    }

    fn get(&mut self, key: &Self::Key) -> Option<Self::Tuple> {
        self.space_update_cache();
        if let Some(space) = self.space() {
            match space.select(IteratorType::Eq, key) {
                Ok(mut iter) => {
                    if let Some(tuple) = iter.next() {
                        match tuple.decode::<Self::Tuple>() {
                            Ok(tuple) => return Some(tuple),
                            Err(_e) => {
                                warn!(
                                    Option::from("space query"),
                                    &format!("Failed to decode tuple: {}", _e)
                                );
                            }
                        }
                    }
                }
                Err(_e) => {
                    warn!(
                        Option::from("space query"),
                        &format!("Space {} select error: {}", self.name(), _e)
                    );
                }
            }
        }
        None
    }

    fn upsert(&mut self, tuple: Self::Tuple) {
        self.space_update_cache();
        if let Some(space) = self.space_mut() {
            if let Err(_e) = space.upsert(&tuple, [("+", 2, 1)]) {
                warn!(
                    Option::from("space query"),
                    &format!("Space {} upsert error: {}", self.name(), _e)
                );
            } else {
                debug!(
                    Option::from("space query"),
                    &format!("increment ref_counter")
                );
            }
        }
    }

    fn delete(&mut self, key: &Self::Key) -> Option<Self::Tuple> {
        self.space_update_cache();
        if let Some(space) = self.space_mut() {
            match space.select(IteratorType::Eq, key) {
                Ok(mut iterator) => {
                    if let Some(tuple) = iterator.next() {
                        match tuple.decode::<Self::Tuple>() {
                            Ok((_, _, ref_counter)) => {
                                if ref_counter > 0 {
                                    if let Err(_e) = space.upsert(&tuple, [("-", 2, 1)]) {
                                        warn!(
                                            Option::from("space query"),
                                            &format!("Space {} upsert error: {}", self.name(), _e)
                                        );
                                    } else {
                                        debug!(
                                            Option::from("space query"),
                                            &format!("decrement ref_counter")
                                        );
                                    }
                                } else {
                                    let result = match space.delete(key) {
                                        Ok(tuple) => {
                                            if let Some(tuple) = tuple {
                                                match tuple.decode::<Self::Tuple>() {
                                                    Ok(tuple) => {
                                                        debug!(
                                                            Option::from("space query"),
                                                            &format!("delete tuple")
                                                        );
                                                        Some(tuple)
                                                    }
                                                    Err(_e) => {
                                                        warn!(
                                                            Option::from("space query"),
                                                            &format!(
                                                                "Failed to decode tuple: {}",
                                                                _e
                                                            )
                                                        );
                                                        None
                                                    }
                                                }
                                            } else {
                                                None
                                            }
                                        }
                                        Err(_e) => {
                                            warn!(
                                                Option::from("space query"),
                                                &format!(
                                                    "Space {} delete error: {}",
                                                    self.name(),
                                                    _e
                                                )
                                            );
                                            None
                                        }
                                    };
                                    STAT.with(|stat| {
                                        let query_id = key.0.as_str();
                                        stat.borrow_mut().delete_query(query_id);
                                    });

                                    return result;
                                }
                            }
                            Err(err) => {
                                let _msg = format!("Can't decode tuple: {err}");
                                warn!(Option::from("space query"), &_msg);
                            }
                        }
                    }
                }
                Err(err) => {
                    let _msg = &format!("failed to select tuple: {err}");
                    warn!(Option::from("space query"), &_msg);
                }
            }
        }
        None
    }

    fn try_create_space(&mut self) {
        let options = SpaceCreateOptions {
            format: Some(vec![
                Field::string("query_id"),
                Field::string("query_text"),
                Field::unsigned("ref_counter"),
            ]),
            engine: SpaceEngineType::Memtx,
            space_type: SpaceType::DataTemporary,
            if_not_exists: true,
            ..Default::default()
        };

        let pk = IndexOptions {
            r#type: Some(IndexType::Tree),
            unique: Some(true),
            parts: Some(vec![Part::new(1, FieldType::String)]),
            if_not_exists: Some(true),
            ..Default::default()
        };

        match Space::create(&self.name, &options) {
            Ok(table) => {
                debug!(Option::from("space query"), "Space created");
                match table.create_index("_sql_query_pk", &pk) {
                    Ok(_) => {
                        debug!(Option::from("space query"), "Index _sql_query_pk created");
                        let space = self.space_mut();
                        *space = Some(table);
                    }
                    Err(err) => {
                        let _msg = &format!("failed to create index _sql_query_pk: {err}");
                        warn!(Option::from("space query"), &_msg);
                    }
                }
            }
            Err(_e) => {
                warn!(
                    Option::from("space query"),
                    &format!("Failed to create a space: {}", _e)
                );
            }
        }
    }
}

pub struct StatSpace {
    space: Option<Space>,
    name: String,
}

type StatSpaceTuple = (String, String, String, f64, f64, f64, u64);

impl TarantoolSpace for StatSpace {
    type Key = (String, String, String);
    type Tuple = StatSpaceTuple;

    fn new() -> Self {
        let mut space = Self {
            space: None,
            name: "_sql_stat".to_string(),
        };
        space.space_update_cache();
        space
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn space(&self) -> &Option<Space> {
        &self.space
    }

    fn space_mut(&mut self) -> &mut Option<Space> {
        &mut self.space
    }

    fn try_create_space(&mut self) {
        let options = SpaceCreateOptions {
            format: Some(vec![
                Field::string("query_id"),
                Field::string("span"),
                Field::string("parent_span"),
                Field::double("min"),
                Field::double("max"),
                Field::double("sum"),
                Field::unsigned("count"),
            ]),
            engine: SpaceEngineType::Memtx,
            space_type: SpaceType::DataTemporary,
            if_not_exists: true,
            ..Default::default()
        };

        let pk = IndexOptions {
            r#type: Some(IndexType::Tree),
            unique: Some(true),
            parts: Some(vec![
                Part::new(1, FieldType::String),
                Part::new(2, FieldType::String),
                Part::new(3, FieldType::String),
            ]),
            if_not_exists: Some(true),
            ..Default::default()
        };

        match Space::create(&self.name, &options) {
            Ok(table) => {
                debug!(Option::from("space stat"), "Space created");
                match table.create_index("_sql_stat_pk", &pk) {
                    Ok(_) => {
                        debug!(Option::from("space stat"), "Index _sql_stat_pk created");
                        let space = self.space_mut();
                        *space = Some(table);
                    }
                    Err(err) => {
                        let _msg = &format!("failed to create index _sql_stat_pk: {err}");
                        warn!(Option::from("space stat"), &_msg);
                    }
                }
            }
            Err(_e) => {
                warn!(
                    Option::from("space stat"),
                    &format!("Failed to create a space: {}", _e)
                );
            }
        }
    }

    fn get(&mut self, key: &Self::Key) -> Option<Self::Tuple> {
        self.space_update_cache();
        if let Some(space) = self.space() {
            match space.select(IteratorType::Eq, key) {
                Err(_e) => {
                    warn!(
                        Option::from("space stat"),
                        &format!("Space {} select error: {}", self.name(), _e)
                    );
                }
                Ok(mut iter) => {
                    if let Some(tuple) = iter.next() {
                        match tuple.decode::<Self::Tuple>() {
                            Ok(tuple) => {
                                debug!(Option::from("space stat"), "get tuple");
                                return Some(tuple);
                            }
                            Err(_e) => {
                                warn!(
                                    Option::from("space stat"),
                                    &format!("Failed to decode tuple: {}", _e)
                                );
                            }
                        }
                    }
                }
            }
        }
        None
    }

    fn upsert(&mut self, tuple: Self::Tuple) {
        self.space_update_cache();
        match self.get(&(tuple.0.clone(), tuple.1.clone(), tuple.2.clone())) {
            None => {
                debug!(Option::from("space stat"), &format!("insert {:?}", tuple));
                if let Some(space) = self.space_mut() {
                    match space.insert(&tuple) {
                        Ok(_) => {
                            debug!(Option::from("space stat"), "inserted");
                        }
                        Err(_e) => {
                            warn!(
                                Option::from("space stat"),
                                &format!("Space {} insert error: {}", self.name(), _e)
                            );
                        }
                    }
                }
            }
            Some(mut found_tuple) => {
                debug!(
                    Option::from("space stat"),
                    &format!("update {:?} with {:?}", found_tuple, tuple)
                );
                found_tuple.3 = tuple.3.min(found_tuple.3);
                found_tuple.4 = tuple.4.max(found_tuple.4);
                found_tuple.5 += tuple.5;
                found_tuple.6 += tuple.6;
                if let Some(space) = self.space_mut() {
                    match space.replace(&found_tuple) {
                        Ok(_) => {
                            debug!(Option::from("space stat"), "replaced");
                        }
                        Err(_e) => {
                            warn!(
                                Option::from("space stat"),
                                &format!("Space {} update error: {}", self.name(), _e)
                            );
                        }
                    }
                }
            }
        }
    }

    fn delete(&mut self, key: &Self::Key) -> Option<Self::Tuple> {
        self.space_update_cache();
        if let Some(space) = self.space_mut() {
            match space.delete(&key) {
                Ok(tuple) => {
                    if let Some(tuple) = tuple {
                        match tuple.decode::<Self::Tuple>() {
                            Ok(tuple) => {
                                debug!(Option::from("space stat"), "deleted");
                                return Some(tuple);
                            }
                            Err(_e) => {
                                warn!(
                                    Option::from("space stat"),
                                    &format!("Failed to decode tuple: {}", _e)
                                );
                            }
                        }
                    }
                }
                Err(_e) => {
                    warn!(
                        Option::from("space stat"),
                        &format!("Space {} delete error: {}", self.name(), _e)
                    );
                }
            }
        }
        None
    }
}

impl StatSpace {
    fn delete_query(&mut self, query_id: &str) {
        self.space_update_cache();
        let keys = if let Some(space) = self.space() {
            let index_scan = match space.select(IteratorType::Eq, &(query_id,)) {
                Err(_e) => {
                    warn!(
                        Option::from("space stat"),
                        &format!("Space {} select error: {}", self.name(), _e)
                    );
                    return;
                }
                Ok(iter) => iter,
            };
            index_scan
                .filter_map(|tuple| match tuple.decode::<StatSpaceTuple>() {
                    Ok(tuple) => {
                        debug!(Option::from("space stat"), "get tuple");
                        Some((tuple.0, tuple.1, tuple.2))
                    }
                    Err(_e) => {
                        warn!(
                            Option::from("space stat"),
                            &format!("Failed to decode tuple: {}", _e)
                        );
                        None
                    }
                })
                .collect::<Vec<_>>()
        } else {
            return;
        };
        for key in keys {
            self.delete(&key);
        }
    }
}
