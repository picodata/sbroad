//! Statistics span processor.
//!
//! This span processor is used to collect statistics about running queries.
//! It uses sampling (1% at the moment) to reduce the overhead of collecting
//! statistics. The results are written to `_sql_query` and `_sql_stat`
//! spaces and evicted by LRU strategy (more details in the `table` and
//! `eviction` modules).
//!
//! The best way to inspect the statistics on any instance is to use local SQL.
//! For example, to get the top 5 most expensive SELECT SQL queries by the
//! average execution time:
//! ```sql
//! select distinct(q."query_text") from "_sql_stat" as s
//! join "_sql_query" as q
//!     on s."query_id" = q."query_id"
//! where lower(q."query_text") like 'select%'
//! order by s."sum"/s."count" desc
//! limit 5
//! ```
//!
//! Or to get the flame graph of the most expensive query:
//! ```sql
//! with recursive st as (
//!     select * from "_sql_stat" where "query_id" in (select qt."query_id" from qt)
//!         and "parent_span" = ''
//!     union all
//!     select s.* from "_sql_stat" as s, st on s."parent_span" = st."span"
//!         and s."query_id" in (select qt."query_id" from qt)
//! ), qt as (
//!     select s."query_id" from "_sql_stat" as s
//!     join "_sql_query" as q
//!         on s."query_id" = q."query_id"
//!     order by s."sum"/s."count" desc
//!     limit 1
//! )
//! select * from st;
//! ```

use eviction::TRACKED_QUERIES;
use opentelemetry::sdk::export::trace::SpanData;
use opentelemetry::sdk::trace::{Span, SpanProcessor};
use opentelemetry::trace::SpanId;
use opentelemetry::Context;
use std::time::Duration;
use table::{RustMap, SpanName, TarantoolSpace, QUERY, SPAN, STAT};

use crate::debug;

pub mod eviction;
pub mod table;

#[derive(Debug)]
pub struct StatCollector {}

impl StatCollector {
    #[must_use]
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for StatCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl SpanProcessor for StatCollector {
    fn on_start(&self, span: &mut Span, _cx: &Context) {
        let Some(span_data) = span.exported_data() else {
            return
        };

        let id: String = match span_data.attributes.get(&"id".into()) {
            Some(id) => id.to_string(),
            None => return,
        };

        // We are processing a top level query span. Lets register it.
        if let Some(query_sql) = span_data.attributes.get(&"query_sql".into()) {
            QUERY.with(|query_space| {
                let tuple = (id, query_sql.to_string(), 2);
                query_space.borrow_mut().upsert(tuple);
            });
            debug!(Option::from("on start"), &format!("query: {query_sql}"));
        }

        // Register current span mapping (span id: span name).
        SPAN.with(|span_table| {
            let key = span_data.span_context.span_id();
            let value = SpanName::from(span_data.name.clone());
            debug!(
                Option::from("on start"),
                &format!("key: {key:?}, value: {value:?}")
            );
            span_table.borrow_mut().push(key, value);
        });
    }

    fn on_end(&self, span: SpanData) {
        let id: String = match span.attributes.get(&"id".into()) {
            Some(id) => id.to_string(),
            None => return,
        };

        let parent_span: String = if span.parent_span_id == SpanId::INVALID {
            String::new()
        } else {
            SPAN.with(|span_table| {
                let span_table = span_table.borrow();
                span_table
                    .get(&span.parent_span_id)
                    .map_or_else(String::new, |span_name| span_name.value().to_string())
            })
        };

        let duration = match span.end_time.duration_since(span.start_time) {
            Ok(duration) => duration,
            // The clock may have gone backwards.
            Err(_) => Duration::from_secs(0),
        }
        .as_secs_f64();
        // Update statistics.
        STAT.with(|stat_space| {
            let tuple = (
                id.to_string(),
                String::from(span.name),
                parent_span,
                duration,
                duration,
                duration,
                1,
            );
            stat_space.borrow_mut().upsert(tuple);
        });

        // Remove current span id to name mapping.
        SPAN.with(|span_table| {
            span_table.borrow_mut().pop(&span.span_context.span_id());
        });

        // Unreference the query for the top level query span.
        // We don't want to remove the query while some other
        // fiber is still collecting statistics for it.
        if span.attributes.get(&"query_sql".into()).is_some() {
            QUERY.with(|query_space| {
                query_space.borrow_mut().delete(&(id.to_string(),));
            });
        }

        // Evict old queries.
        TRACKED_QUERIES.with(|tracked_queries| {
            let mut tracked_queries = tracked_queries.borrow_mut();
            tracked_queries.push(id).unwrap();
        });
    }

    fn force_flush(&self) -> opentelemetry::trace::TraceResult<()> {
        Ok(())
    }

    fn shutdown(&mut self) -> opentelemetry::trace::TraceResult<()> {
        Ok(())
    }
}
