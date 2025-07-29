use crate::postgres::models::{
    collation::Collation,
    conversion::Conversion,
    event_trigger::EventTrigger,
    extension::Extension,
    fdw::{ForeignDataWrapper, ForeignTable, Server},
    global::{Role, Tablespace},
    operator::{OpClass, OpFamily, Operator},
    policy::Policy,
    publication::{Publication, PublicationTable},
    relation::Relation,
    routine::Routine,
    rule::Rule,
    schema::NamedSchema,
    sequence::Sequence,
    subscription::Subscription,
    trigger::Trigger,
    types::Type,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Represents a complete, in-memory snapshot of a PostgreSQL database's schema.
///
/// This struct is the root of the introspection model and the main result
/// produced by the introspection process.
/// Represents a complete, in-memory snapshot of a PostgreSQL database's schema.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct Database {
    // --- Global Objects (Cluster-wide) ---
    pub roles: HashMap<String, Role>,
    pub tablespaces: HashMap<String, Tablespace>,

    // --- Database-Scoped Objects (Live inside one database) ---
    pub name: Option<String>,
    pub schemas: HashMap<String, NamedSchema>,
    pub extensions: HashMap<String, Extension>,
    pub publications: HashMap<String, Publication>,
    pub subscriptions: HashMap<String, Subscription>,
    pub foreign_data_wrappers: HashMap<String, ForeignDataWrapper>,
    pub servers: HashMap<String, Server>,
    pub event_triggers: HashMap<String, EventTrigger>,

    // --- Schema-Scoped Objects (Live inside a schema) ---
    pub relations: HashMap<String, Relation>,
    pub sequences: HashMap<String, Sequence>,
    pub types: HashMap<String, Type>,
    pub routines: HashMap<String, Routine>,
    pub collations: HashMap<String, Collation>,
    pub conversions: HashMap<String, Conversion>,
    pub operators: HashMap<String, Operator>,
    pub op_classes: HashMap<String, OpClass>,
    pub op_families: HashMap<String, OpFamily>,

    // --- Relationship Objects ---
    /// Links Publications to the Tables they contain. Stored as a Vec as it's a list of relations.
    pub publication_tables: Vec<PublicationTable>,
}

// The `#[derive(Default)]` gives you `Database::default()`.
// Your manual `new()` implementation should be removed.
impl Database {
    pub fn with_name(name: String) -> Self {
        Self {
            name: Some(name),
            ..Self::default() // Use the derived default constructor
        }
    }
}
