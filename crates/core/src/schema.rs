use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Schema {
    pub roles: HashMap<String, Role>,
    pub named_schemas: HashMap<String, NamedSchema>,
    pub extensions: HashMap<String, Extension>,
    pub types: HashMap<String, Type>,
    pub name: Option<String>,
    pub tables: HashMap<String, Table>,
    pub views: HashMap<String, View>,
    pub materialized_views: HashMap<String, MaterializedView>,
    pub functions: HashMap<String, Function>,
    pub procedures: HashMap<String, Procedure>,
    pub sequences: HashMap<String, Sequence>,
    pub triggers: HashMap<String, Trigger>,
    pub constraint_triggers: HashMap<String, ConstraintTrigger>,
    pub event_triggers: HashMap<String, EventTrigger>,
    pub policies: HashMap<String, Policy>,
    pub servers: HashMap<String, Server>,
    pub collations: HashMap<String, Collation>,
    pub rules: HashMap<String, Rule>,
    pub publications: HashMap<String, Publication>,
    pub subscriptions: HashMap<String, Subscription>,
    pub tablespaces: HashMap<String, Tablespace>,
    pub foreign_tables: HashMap<String, ForeignTable>,
    pub foreign_data_wrappers: HashMap<String, ForeignDataWrapper>,
    pub foreign_key_constraints: HashMap<String, ForeignKeyConstraint>,
}

impl Schema {
    pub fn new() -> Self {
        Self {
            name: None,
            named_schemas: HashMap::new(),
            tables: HashMap::new(),
            views: HashMap::new(),
            materialized_views: HashMap::new(),
            functions: HashMap::new(),
            procedures: HashMap::new(),
            sequences: HashMap::new(),
            extensions: HashMap::new(),
            triggers: HashMap::new(),
            constraint_triggers: HashMap::new(),
            event_triggers: HashMap::new(),
            policies: HashMap::new(),
            servers: HashMap::new(),
            collations: HashMap::new(),
            rules: HashMap::new(),
            publications: HashMap::new(),
            subscriptions: HashMap::new(),
            roles: HashMap::new(),
            tablespaces: HashMap::new(),
            foreign_tables: HashMap::new(),
            foreign_data_wrappers: HashMap::new(),
            foreign_key_constraints: HashMap::new(),
            types: HashMap::new(),
        }
    }

    pub fn with_name(name: String) -> Self {
        Self {
            name: Some(name),
            ..Self::new()
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Role {
    pub oid: u32,
    pub name: String,
    pub superuser: bool,
    pub createdb: bool,
    pub createrole: bool,
    pub inherit: bool,
    pub login: bool,
    pub replication: bool,
    pub connection_limit: i32, // pg_roles is not nullable, defaults to -1
    pub password: Option<String>, // Always None for security reasons
    pub valid_until: Option<String>,
    pub member_of: Vec<String>,
    pub config: Option<Vec<String>>, // For ALTER ROLE ... SET
    pub is_predefined: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NamedSchema {
    pub oid: u32,
    pub name: String,
    pub owner: String,
    pub acl: Option<String>,
    pub comment: Option<String>,
    pub is_user_defined: bool,
    pub is_from_extension: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Extension {
    pub oid: u32,
    pub name: String,
    pub owner: String,
    pub relocatable: bool,
    pub version: String,
    pub schema: String, // An extension must have a schema, so it's not an Option
    pub comment: Option<String>,
    pub is_user_defined: bool,
    // We will handle config tables and dependencies in a later step
    // pub config_tables: Vec<u32>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ReplicaIdentity {
    Default,
    Nothing,
    Full,
    Index(String), // The value is the name of the index
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
pub struct MaterializedView {
    pub name: String,
    pub schema: Option<String>,
    pub definition: String,
    pub check_option: CheckOption,
    pub comment: Option<String>,
    pub tablespace: Option<String>, // Added: tablespace assignment
    pub storage_parameters: HashMap<String, String>, // Added: WITH parameters
    pub indexes: Vec<Index>,        // Added: materialized view indexes
    pub populate_with_data: bool,   // Added: controls WITH DATA vs WITH NO DATA
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Function {
    pub name: String,
    pub schema: Option<String>,
    pub parameters: Vec<Parameter>,
    pub returns: ReturnType,
    pub language: String,
    pub definition: String,
    pub comment: Option<String>,
    pub volatility: Volatility, // Added: IMMUTABLE/STABLE/VOLATILE
    pub strict: bool,           // Added: STRICT/RETURNS NULL ON NULL INPUT
    pub security_definer: bool, // Added: security context
    pub parallel_safety: ParallelSafety, // Added: parallel execution safety
    pub cost: Option<f64>,      // Added: execution cost hint
    pub rows: Option<f64>,      // Added: rows estimate for set-returning functions
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Procedure {
    pub name: String,
    pub schema: Option<String>,
    pub parameters: Vec<Parameter>,
    pub language: String,
    pub definition: String,
    pub comment: Option<String>,
    pub security_definer: bool, // Added: security context
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OwnedBy {
    pub table_schema: String,
    pub table_name: String,
    pub column_name: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Sequence {
    pub oid: u32,
    pub name: String,
    pub schema: Option<String>,
    pub owner: String,
    pub data_type: String, // "smallint", "integer", or "bigint"
    pub start: i64,
    pub increment: i64,
    pub min_value: Option<i64>,
    pub max_value: Option<i64>,
    pub cache: i64,
    pub cycle: bool,
    // Data-related fields
    pub current_value: Option<i64>, // Can be NULL if sequence hasn't been used
    pub is_called: bool,            // Has nextval been called since the last setval?
    // Ownership and permissions
    pub owned_by: Option<String>, // Format: "schema.table.column"
    pub acl: Option<String>,
    pub comment: Option<String>,
    // Filtering flags
    pub is_user_defined: bool,
    pub is_from_extension: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum TriggerTiming {
    Before,
    After,
    InsteadOf,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum TriggerEvent {
    Insert,
    Update,
    Delete,
    Truncate,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum TriggerLevel {
    Row,
    Statement,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Trigger {
    pub name: String,
    pub table: String,
    pub schema: Option<String>,
    pub timing: TriggerTiming,
    pub events: Vec<TriggerEvent>,
    pub function: String,
    pub arguments: Vec<String>,
    pub condition: Option<String>, // Added: WHEN condition
    pub for_each: TriggerLevel,    // Added: FOR EACH ROW/STATEMENT
    pub comment: Option<String>,
    pub when: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Policy {
    pub name: String,
    pub table: String,
    pub schema: Option<String>, // Added: schema field
    pub command: PolicyCommand, // Enhanced: specific command type
    pub permissive: bool,
    pub roles: Vec<String>,
    pub using: Option<String>,
    pub check: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Server {
    pub name: String,
    pub foreign_data_wrapper: String,
    pub options: HashMap<String, String>,
    pub version: Option<String>, // Added: server version
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EventTrigger {
    pub name: String,
    pub event: EventTriggerEvent, // Enhanced: structured event types
    pub function: String,
    pub enabled: bool,
    pub tags: Vec<String>,
    pub condition: Option<String>, // Added: WHEN condition
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum CollationProvider {
    Libc,
    Icu,
    Builtin,
    Default,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Collation {
    pub oid: u32,
    pub name: String,
    pub owner: String,
    pub schema: String,
    pub provider: CollationProvider,
    pub deterministic: bool,
    pub lc_collate: Option<String>,
    pub lc_ctype: Option<String>,
    pub icu_locale: Option<String>, // For ICU collations (Postgres 15+)
    pub icu_rules: Option<String>,  // For ICU collations (Postgres 16+)
    pub version: Option<String>,    // (Postgres 10+)
    pub comment: Option<String>,    // We'll add this to the query
    pub is_user_defined: bool,
    pub is_from_extension: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Rule {
    pub name: String,
    pub table: String,
    pub schema: Option<String>,
    pub event: RuleEvent,
    pub instead: bool,
    pub condition: Option<String>, // Added: WHERE condition
    pub actions: Vec<String>,      // Enhanced: multiple actions
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ConstraintTrigger {
    pub name: String,
    pub table: String,
    pub schema: Option<String>,
    pub function: String,
    pub timing: TriggerTiming,
    pub events: Vec<TriggerEvent>,
    pub arguments: Vec<String>,
    pub constraint_name: String,
    pub deferrable: bool,         // Added: deferrable constraint
    pub initially_deferred: bool, // Added: initially deferred
}

// New structures for additional PostgreSQL objects
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Publication {
    pub name: String,
    pub tables: Vec<String>,
    pub all_tables: bool,
    pub insert: bool,
    pub update: bool,
    pub delete: bool,
    pub truncate: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Subscription {
    pub name: String,
    pub connection: String,
    pub publication: Vec<String>,
    pub enabled: bool,
    pub slot_name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Tablespace {
    pub oid: u32,
    pub name: String,
    pub location: String,
    pub owner: String,
    pub options: HashMap<String, String>,
    pub acl: Option<String>,
    pub comment: Option<String>,
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
pub enum ColumnStorage {
    Plain,    // 'p'
    External, // 'e'
    Extended, // 'x'
    Main,     // 'm'
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Identity {
    pub generation: IdentityGeneration, // ALWAYS or BY DEFAULT
                                        // Sequence options are part of the associated Sequence object, not stored here.
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum IdentityGeneration {
    Always,    // 'a'
    ByDefault, // 'd'
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Generated {
    pub expression: String,
    // In PostgreSQL, generated columns are currently always STORED.
    // A 'kind' field could be added if VIRTUAL is supported in the future.
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ForeignDataWrapper {
    pub name: String,
    pub handler: Option<String>,
    pub validator: Option<String>,
    pub options: HashMap<String, String>,
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Constraint {
    pub name: String,
    pub kind: ConstraintKind,
    pub definition: String,
    pub deferrable: bool,         // Added: deferrable constraint
    pub initially_deferred: bool, // Added: initially deferred
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Index {
    pub name: String,
    pub columns: Vec<IndexColumn>,
    pub unique: bool,
    pub method: IndexMethod,          // Enhanced: structured index method
    pub where_clause: Option<String>, // Added: partial index condition
    pub tablespace: Option<String>,   // Added: tablespace assignment
    pub storage_parameters: HashMap<String, String>, // Added: WITH parameters
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Parameter {
    pub name: String,
    pub type_name: String,
    pub mode: ParameterMode,
    pub default: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ReturnType {
    pub kind: ReturnKind,
    pub type_name: String,
    pub is_set: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum DomainConstraintType {
    Check,
    NotNull,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PartitionBy {
    pub method: PartitionMethod,
    pub columns: Vec<String>,
}

// Enhanced enums
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ConstraintKind {
    PrimaryKey,
    ForeignKey {
        references: String,
        on_delete: Option<ReferentialAction>,
        on_update: Option<ReferentialAction>,
    },
    Unique,
    Check,
    Exclusion,
    NotNull,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IndexColumn {
    pub name: String,
    pub expression: Option<String>, // Added: expression indexes
    pub order: SortOrder,
    pub nulls_first: bool,
    pub opclass: Option<String>, // Added: operator class
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct GeneratedColumn {
    pub expression: String,
    pub stored: bool,
}

// Enhanced enums with more options
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum CheckOption {
    None,
    Local,
    Cascaded,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ParameterMode {
    In,
    Out,
    InOut,
    Variadic,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum SortOrder {
    Ascending,
    Descending,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ReturnKind {
    Table,
    SetOf,
    Scalar,
    Void, // Added: procedures return void
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum RuleEvent {
    Select,
    Update,
    Insert,
    Delete,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Volatility {
    Immutable,
    Stable,
    Volatile,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ParallelSafety {
    Safe,
    Restricted,
    Unsafe,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum PolicyCommand {
    All,
    Select,
    Insert,
    Update,
    Delete,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum EventTriggerEvent {
    DdlCommandStart,
    DdlCommandEnd,
    TableRewrite,
    SqlDrop,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum IndexMethod {
    Btree,
    Hash,
    Gist,
    Spgist,
    Gin,
    Brin,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum PartitionMethod {
    Range,
    List,
    Hash,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ReferentialAction {
    NoAction,
    Restrict,
    Cascade,
    SetNull,
    SetDefault,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ForeignKeyConstraint {
    pub name: String,
    pub table: String,
    pub schema: Option<String>,
    pub columns: Vec<String>,
    pub references_table: String,
    pub references_schema: Option<String>,
    pub references_columns: Vec<String>,
    pub on_delete: Option<ReferentialAction>,
    pub on_update: Option<ReferentialAction>,
    pub deferrable: bool,
    pub initially_deferred: bool,
}

// The specific struct for a pseudo-type.
// It might not even need any fields beyond the common ones.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PseudoType {
    pub info: TypeInfo,
    // You could add specific flags here if needed, but it's often unnecessary.
    // For example, is_polymorphic: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Type {
    Base(BaseType),
    Composite(CompositeType),
    Domain(Domain),
    Enum(EnumType),
    Range(RangeType),
    Pseudo(PseudoType), // For things like 'any', 'void', etc. // Array and Multirange types are properties of other types, not distinct kinds.
}

// Common information for all types
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TypeInfo {
    pub oid: u32,
    pub name: String,
    pub schema: String,
    pub owner: String,
    pub acl: Option<String>,
    pub comment: Option<String>,
    pub is_user_defined: bool,
    pub is_from_extension: bool,
    pub array_type_oid: Option<u32>,
}

// Base Type ('b')
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BaseType {
    pub info: TypeInfo,
    pub internal_length: i16,
    pub is_passed_by_value: bool,
    pub alignment: char,
    pub storage: char,
    pub category: char,
    pub is_preferred: bool,
    pub default_value: Option<String>,
    pub element_type_oid: Option<u32>, // 0 if not an array type
    pub delimiter: char,
    pub is_collatable: bool,
    // I/O functions are critical for CREATE TYPE
    pub input_fn: String,
    pub output_fn: String,
    pub receive_fn: Option<String>,
    pub send_fn: Option<String>,
    pub typmod_in_fn: Option<String>,
    pub typmod_out_fn: Option<String>,
    pub analyze_fn: Option<String>,
}

// Composite Type ('c')
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CompositeType {
    pub info: TypeInfo,
    pub attributes: Vec<Attribute>,
    pub class_oid: u32, // OID of the backing pg_class entry
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Attribute {
    pub name: String,
    pub type_name: String,         // Fully formatted type
    pub collation: Option<String>, // Fully qualified collation name
}

// Domain Type ('d')
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Domain {
    pub info: TypeInfo,
    pub base_type: String,         // Fully formatted base type
    pub collation: Option<String>, // Fully qualified collation name
    pub not_null: bool,
    pub default: Option<String>,
    pub constraints: Vec<DomainConstraint>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DomainConstraint {
    pub oid: u32,
    pub name: String,
    pub definition: String, // The full CHECK (...) text
    pub not_valid: bool,
}

// Enum Type ('e')
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EnumType {
    pub info: TypeInfo,
    pub values: Vec<EnumValue>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EnumValue {
    pub oid: u32,
    pub label: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ArrayType {
    pub name: String,
    pub schema: Option<String>,
    pub element_type: String,
    pub element_schema: Option<String>,
    pub comment: Option<String>,
}

// Range Type ('r')
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RangeType {
    pub info: TypeInfo,
    pub subtype: String,
    pub subtype_opclass: String,   // Fully qualified opclass name
    pub collation: Option<String>, // Fully qualified collation name
    pub canonical_fn: Option<String>,
    pub subtype_diff_fn: Option<String>,
    pub multirange_type_oid: Option<u32>,
}
