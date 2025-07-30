use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::model::trigger::Trigger;

// in models/relation.rs or a new models/index.rs
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SortOrder {
    Ascending,
    Descending,
}

// Implement Default to make struct initialization easier
impl Default for SortOrder {
    fn default() -> Self {
        SortOrder::Ascending
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IndexColumn {
    pub name: String,
    pub expression: Option<String>,
    pub order: SortOrder, // <-- Uses the enum
    pub nulls_first: bool,
    pub opclass: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum IdentityGeneration {
    Always,    // 'a'
    ByDefault, // 'd'
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Identity {
    pub generation: IdentityGeneration, // ALWAYS or BY DEFAULT
                                        // Sequence options are part of the associated Sequence object, not stored here.
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Generated {
    pub expression: String,
    // In PostgreSQL, generated columns are currently always STORED.
    // A 'kind' field could be added if VIRTUAL is supported in the future.
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ReferentialAction {
    NoAction,   // 'a'
    Restrict,   // 'r'
    Cascade,    // 'c'
    SetNull,    // 'n'
    SetDefault, // 'd'
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ColumnStorage {
    Plain,    // 'p'
    External, // 'e'
    Extended, // 'x'
    Main,     // 'm'
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum CheckOption {
    None,
    Local,
    Cascaded,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ReplicaIdentity {
    Default,
    Nothing,
    Full,
    Index(String), // The value is the name of the index
}

// This struct holds the data specific ONLY to Foreign Keys
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ForeignKeyDetail {
    pub foreign_table_oid: u32,
    pub foreign_table_schema: String,
    pub foreign_table_name: String,
    pub local_columns: Vec<String>,
    pub foreign_columns: Vec<String>,
    pub on_update: ReferentialAction,
    pub on_delete: ReferentialAction,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ConstraintType {
    Check,
    ForeignKey(ForeignKeyDetail),
    PrimaryKey,
    Unique,
    Exclusion,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum IndexMethod {
    Btree,
    Hash,
    Gist,
    SpGist,
    Gin,
    Brin,
    // You could add others here if you need to support custom access methods
    // Custom(String),
}

// A simpler, more pg_dump-like Index struct
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Index {
    pub oid: u32,
    pub name: String,
    pub table_oid: u32,
    pub definition: String, // Contains the full CREATE INDEX statement
    pub is_clustered: bool,
    pub comment: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Constraint {
    pub oid: u32,
    pub name: String,
    pub table_oid: u32,     // The OID of the table this constraint is on
    pub definition: String, // The full definition from pg_get_constraintdef()
    pub r#type: ConstraintType,
    pub is_deferrable: bool,
    pub is_initially_deferred: bool,
    pub is_not_valid: bool,
}

// Supporting types
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub type_name: String, // Fully formatted type, e.g., "character varying(255)"
    pub is_not_null: bool,
    pub has_default: bool, // Just a flag, the default value itself is a separate object

    // Properties for CREATE TABLE
    pub collation: Option<String>, // Fully qualified collation name, e.g., "public.my_collation"
    pub storage: ColumnStorage,
    pub compression: Option<String>, // e.g., "pglz", "lz4" (PG14+)

    // Special column types
    pub identity: Option<Identity>,   // For IDENTITY columns (PG10+)
    pub generated: Option<Generated>, // For GENERATED columns (PG12+)

    // Metadata
    pub comment: Option<String>,
    pub acl: Option<String>, // Per-column permissions

    // Internal & advanced properties
    pub is_dropped: bool, // Important for binary_upgrade and table structure analysis
    pub is_local: bool,   // True if defined in this table, false if inherited
    pub stats_target: Option<i32>, // Per-column statistics target (-1 is default)
    pub fdw_options: HashMap<String, String>, // Options for a column in a foreign table
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Table {
    pub oid: u32,
    pub name: String,
    pub schema: String,
    pub owner: String,
    pub columns: Vec<Column>,
    pub constraints: Vec<Constraint>,
    pub indexes: Vec<Index>,
    pub triggers: Vec<Trigger>,
    pub comment: Option<String>,
    pub tablespace: Option<String>,
    pub inherits: Vec<String>,         // List of parent table names
    pub partition_key: Option<String>, // The full "PARTITION BY ..." string
    pub replica_identity: ReplicaIdentity,
    pub acl: Option<String>,
    pub is_user_defined: bool,
    pub is_from_extension: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MaterializedView {
    pub oid: u32,
    pub name: String,
    pub schema: String,
    pub owner: String,
    pub definition: String,
    pub columns: Vec<Column>, // Matviews have columns, just like tables/views
    pub is_populated: bool,   // Authoritative flag for WITH DATA / WITH NO DATA
    pub options: HashMap<String, String>,
    pub tablespace: Option<String>,
    pub acl: Option<String>,
    pub comment: Option<String>,
    pub indexes: Vec<Index>, // Indexes are important for matviews
    pub is_user_defined: bool,
    pub is_from_extension: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct View {
    pub oid: u32,
    pub name: String,
    pub schema: String,
    pub owner: String,
    pub definition: String,
    pub columns: Vec<Column>, // Using the same Column struct as tables
    pub check_option: CheckOption,
    pub options: HashMap<String, String>, // For security_barrier and other options
    pub acl: Option<String>,
    pub comment: Option<String>,
    pub is_user_defined: bool,
    pub is_from_extension: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ForeignTable {
    pub name: String,
    pub schema: Option<String>,
    pub columns: Vec<Column>,
    pub server: String,
    pub options: HashMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Relation {
    Table(Table),
    View(View),
    MaterializedView(MaterializedView),
    ForeignTable(ForeignTable),
    // Note: Partitioned Tables are represented by the `Table` variant.
    // The presence of a `partition_key` in the `Table` struct is what
    // distinguishes them from a regular table.
}
