//! What a caller binds into its own SQL.
//!
//! SQLite has one parameter space and four spellings for a slot in it: `?`,
//! `?N`, `:name`, `@name` and `$name`. A store that accepted only the numbered
//! form would not be refusing anything on an invariant's behalf — it would
//! simply be handing the work back, because SQL written with named
//! placeholders then has to be rewritten before it can be run, and rewriting
//! it means reading it: skipping string literals, and the literals here
//! include `json_extract(data, '$.n')`.
//!
//! So both forms are bound, and [`Params`] is which one a call is making.

use serde_json::{Map, Value};

/// Values for a statement's placeholders, by position or by name.
///
/// Not `#[non_exhaustive]`: a caller matches on this, there is no third way to
/// bind a parameter in SQLite, and a variant for "no parameters" would be a
/// second spelling of `Positional(vec![])`.
///
/// # A name carries its sigil
///
/// The name is the placeholder **as written in the SQL**, punctuation
/// included: `(":kind", json!("placed"))` for `:kind`, `("$stream", ..)` for
/// `$stream`. That is rusqlite's rule, not this crate's addition — "the
/// initial `:` or `$` or `@` or `?` used to specify the parameter is included
/// as part of the name" — and a name without it matches no placeholder at all,
/// which is reported as [`crate::Error::Validation`] rather than bound to
/// nothing.
///
/// # Every name the statement declares must be supplied
///
/// A `Named` set is checked against the prepared statement before it runs:
/// each parameter the statement declares has to be among the names given. One
/// that is not is [`crate::Error::Validation`] naming it.
///
/// The check is this crate's, because rusqlite has none — "unbound named
/// parameters will be left to the value they previously were bound with,
/// falling back to `NULL`". A placeholder the caller forgot would otherwise
/// come back as a query that ran, answered, and silently meant something else:
/// `WHERE kind = :kind` with no `:kind` bound is `WHERE kind = NULL`, which
/// matches nothing and reports no fault. Mixing a bare `?` into a statement
/// bound by name is refused for the same reason — nothing supplies it.
///
/// A `Positional` set is not checked here because SQLite checks it: a count
/// that does not match the statement's is refused before the statement runs.
///
/// # Building one
///
/// ```
/// # use eventsdb_core::Params;
/// # use serde_json::{json, Map};
/// let by_position: Params = vec![json!("placed")].into();
/// let by_name: Params = vec![(":kind".to_string(), json!("placed"))].into();
///
/// let mut map = Map::new();
/// map.insert(":kind".to_string(), json!("placed"));
/// let also_by_name: Params = map.into();
/// assert_eq!(by_name, also_by_name);
/// ```
///
/// Two `From` impls are over a `Vec`, so an **empty** literal no longer says
/// which kind it is: write `Vec::<Value>::new()` where `vec![]` used to do.
/// A non-empty `vec![json!(..)]` is unambiguous and needs no change.
#[derive(Debug, Clone, PartialEq)]
pub enum Params {
    /// Bound to `?`, `?1`, `?2` … in the order given.
    Positional(Vec<Value>),
    /// Bound to `:name`, `@name` or `$name`, sigil included in the key.
    Named(Vec<(String, Value)>),
}

impl From<Vec<Value>> for Params {
    fn from(values: Vec<Value>) -> Self {
        Params::Positional(values)
    }
}

impl From<Vec<(String, Value)>> for Params {
    fn from(pairs: Vec<(String, Value)>) -> Self {
        Params::Named(pairs)
    }
}

impl From<Map<String, Value>> for Params {
    /// A JSON object, one entry per placeholder. `Map` preserves whatever
    /// order it was built with; binding is by name, so order does not reach
    /// the statement either way.
    fn from(map: Map<String, Value>) -> Self {
        Params::Named(map.into_iter().collect())
    }
}
