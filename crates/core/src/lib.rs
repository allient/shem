use serde::{Deserialize, Serialize};
use std::fmt::Debug;

pub mod error;
pub mod migration;
pub mod schema;
pub mod traits;

pub use error::{Error, Result};
pub use migration::Migration;
// Re-export specific schema types that don't conflict with shared_types
pub use schema::{
    Collation, Column, ColumnStorage, Constraint, ConstraintKind, ConstraintTrigger, Domain,
    DomainConstraint, EnumType, EventTrigger, Extension, ForeignDataWrapper, ForeignKeyConstraint,
    ForeignTable, Function, Identity, Index, IndexColumn, IndexMethod, MaterializedView,
    NamedSchema, ParallelSafety, Parameter, PartitionBy, PartitionMethod, Policy, Procedure,
    Publication, RangeType, ReturnKind, ReturnType, Role, Rule, Schema, Sequence, Server,
    Subscription, Table, Tablespace, Trigger, TriggerLevel, TriggerTiming, View, Volatility,
};
pub use traits::{DatabaseConnection, DatabaseDriver, SchemaSerializer};

// Migration-specific types that are not part of the schema
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Statement {
    pub sql: String,
    pub description: Option<String>,
}
