use serde::{Deserialize, Serialize};
use std::fmt::Debug;

pub mod error;
pub mod migration;
pub mod schema;
pub mod shared_types;
pub mod traits;

pub use error::{Error, Result};
pub use migration::Migration;
// Re-export specific schema types that don't conflict with shared_types
pub use schema::{Schema, NamedSchema, Table, View, MaterializedView, Function, Procedure, Type, Domain, Sequence, Extension, Trigger, Policy, Server, EventTrigger, Collation, Rule, ConstraintTrigger, RangeType, Publication, Subscription, Role, Tablespace, ForeignTable, ForeignDataWrapper, ForeignKeyConstraint, EnumType, Column, Constraint, Index, Parameter, ReturnType, DomainConstraint, PartitionBy, TypeKind, ConstraintKind, IndexColumn, Identity, TriggerTiming, TriggerLevel, ReturnKind, Volatility, ParallelSafety, IndexMethod, ColumnStorage, PartitionMethod};
pub use shared_types::*;
pub use traits::{DatabaseConnection, DatabaseDriver, SchemaSerializer};

// Migration-specific types that are not part of the schema
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Statement {
    pub sql: String,
    pub description: Option<String>,
}