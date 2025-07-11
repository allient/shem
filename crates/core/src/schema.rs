use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Schema {
    pub name: Option<String>,
    pub named_schemas: HashMap<String, NamedSchema>,
    pub tables: HashMap<String, Table>,
    pub views: HashMap<String, View>,
    pub materialized_views: HashMap<String, MaterializedView>,
    pub functions: HashMap<String, Function>,
    pub procedures: HashMap<String, Procedure>,
    pub enums: HashMap<String, EnumType>,
    pub domains: HashMap<String, Domain>,
    pub sequences: HashMap<String, Sequence>,
    pub extensions: HashMap<String, Extension>,
    pub triggers: HashMap<String, Trigger>,
    pub constraint_triggers: HashMap<String, ConstraintTrigger>,
    pub event_triggers: HashMap<String, EventTrigger>,
    pub policies: HashMap<String, Policy>,
    pub servers: HashMap<String, Server>,
    pub collations: HashMap<String, Collation>,
    pub rules: HashMap<String, Rule>,
    pub range_types: HashMap<String, RangeType>,
    pub publications: HashMap<String, Publication>,
    pub subscriptions: HashMap<String, Subscription>,
    pub roles: HashMap<String, Role>,
    pub tablespaces: HashMap<String, Tablespace>,
    pub foreign_tables: HashMap<String, ForeignTable>,
    pub foreign_data_wrappers: HashMap<String, ForeignDataWrapper>,
    pub foreign_key_constraints: HashMap<String, ForeignKeyConstraint>,
    pub composite_types: HashMap<String, CompositeType>,
    pub base_types: HashMap<String, BaseType>,
    pub array_types: HashMap<String, ArrayType>,
    pub multirange_types: HashMap<String, MultirangeType>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NamedSchema {
    pub name: String,
    pub owner: Option<String>,
    pub comment: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Table {
    pub name: String,
    pub schema: Option<String>,
    pub columns: Vec<Column>,
    pub constraints: Vec<Constraint>,
    pub indexes: Vec<Index>,
    pub comment: Option<String>,
    pub tablespace: Option<String>,
    pub inherits: Vec<String>,
    pub partition_by: Option<PartitionBy>,
    pub storage_parameters: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct View {
    pub name: String,
    pub schema: Option<String>,
    pub definition: String,
    pub check_option: CheckOption,
    pub comment: Option<String>,
    pub security_barrier: bool, // Added: security barrier views
    pub columns: Vec<String>,   // Added: explicit column list
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Domain {
    pub name: String,
    pub schema: Option<String>,
    pub base_type: String,
    pub constraints: Vec<DomainConstraint>, // Enhanced: structured constraints
    pub default: Option<String>,            // Added: default value
    pub not_null: bool,                     // Added: NOT NULL constraint
    pub comment: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Sequence {
    pub name: String,
    pub schema: Option<String>,
    pub data_type: String, // Added: sequence data type (bigint, integer, smallint)
    pub start: i64,
    pub increment: i64,
    pub min_value: Option<i64>,
    pub max_value: Option<i64>,
    pub cache: i64,
    pub cycle: bool,
    pub owned_by: Option<String>, // Added: OWNED BY column
    pub comment: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Extension {
    pub name: String,
    pub schema: Option<String>,
    pub version: String,
    pub cascade: bool, // Added: CASCADE option
    pub comment: Option<String>,
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Collation {
    pub name: String,
    pub schema: Option<String>,
    pub locale: Option<String>,
    pub lc_collate: Option<String>,  // Enhanced: separate LC_COLLATE
    pub lc_ctype: Option<String>,    // Enhanced: separate LC_CTYPE
    pub provider: CollationProvider, // Enhanced: structured provider
    pub deterministic: bool,         // Added: deterministic flag
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RangeType {
    pub name: String,
    pub schema: Option<String>,
    pub subtype: String,
    pub subtype_opclass: Option<String>,
    pub collation: Option<String>,
    pub canonical: Option<String>,
    pub subtype_diff: Option<String>,
    pub multirange_type_name: Option<String>, // Added: multirange type
    pub comment: Option<String>,
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
pub struct Role {
    pub name: String,
    pub superuser: bool,
    pub createdb: bool,
    pub createrole: bool,
    pub inherit: bool,
    pub login: bool,
    pub replication: bool,
    pub connection_limit: Option<i32>,
    pub password: Option<String>,
    pub valid_until: Option<String>,
    pub member_of: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Tablespace {
    pub name: String,
    pub location: String,
    pub owner: String,
    pub options: HashMap<String, String>,
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ForeignDataWrapper {
    pub name: String,
    pub handler: Option<String>,
    pub validator: Option<String>,
    pub options: HashMap<String, String>,
}

// Supporting types
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Column {
    pub name: String,
    pub type_name: String,
    pub nullable: bool,
    pub default: Option<String>,
    pub identity: Option<Identity>,
    pub generated: Option<GeneratedColumn>,
    pub comment: Option<String>,
    pub collation: Option<String>,      // Added: column-level collation
    pub storage: Option<ColumnStorage>, // Added: storage type
    pub compression: Option<String>,    // Added: compression method
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
pub struct DomainConstraint {
    pub name: Option<String>,
    pub check: String,
    pub not_valid: bool, // Added: NOT VALID constraint
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
pub struct Identity {
    pub always: bool,
    pub start: i64,
    pub increment: i64,
    pub min_value: Option<i64>,
    pub max_value: Option<i64>,
    pub cache: Option<i64>, // Made optional
    pub cycle: bool,
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
pub enum CollationProvider {
    Libc,
    Icu,
    Builtin,
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
pub enum ColumnStorage {
    Plain,
    External,
    Extended,
    Main,
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EnumType {
    pub name: String,
    pub schema: Option<String>,
    pub values: Vec<String>,
    pub comment: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CompositeType {
    pub name: String,
    pub schema: Option<String>,
    pub values: Vec<String>,
    pub comment: Option<String>,
    pub attributes: Vec<Column>,
    pub definition: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BaseType {
    pub name: String,
    pub schema: Option<String>,
    pub internal_length: Option<i32>,
    pub is_passed_by_value: bool,
    pub alignment: String,
    pub storage: String,
    pub category: Option<String>,
    pub preferred: bool,
    pub default: Option<String>,
    pub element: Option<String>,
    pub delimiter: Option<String>,
    pub collatable: bool,
    pub comment: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ArrayType {
    pub name: String,
    pub schema: Option<String>,
    pub element_type: String,
    pub element_schema: Option<String>,
    pub comment: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MultirangeType {
    pub name: String,
    pub schema: Option<String>,
    pub range_type: String,
    pub range_schema: Option<String>,
    pub comment: Option<String>,
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
            enums: HashMap::new(),
            domains: HashMap::new(),
            sequences: HashMap::new(),
            extensions: HashMap::new(),
            triggers: HashMap::new(),
            constraint_triggers: HashMap::new(),
            event_triggers: HashMap::new(),
            policies: HashMap::new(),
            servers: HashMap::new(),
            collations: HashMap::new(),
            rules: HashMap::new(),
            range_types: HashMap::new(),
            publications: HashMap::new(),
            subscriptions: HashMap::new(),
            roles: HashMap::new(),
            tablespaces: HashMap::new(),
            foreign_tables: HashMap::new(),
            foreign_data_wrappers: HashMap::new(),
            foreign_key_constraints: HashMap::new(),
            composite_types: HashMap::new(),
            base_types: HashMap::new(),
            array_types: HashMap::new(),
            multirange_types: HashMap::new(),
        }
    }

    pub fn with_name(name: String) -> Self {
        Self {
            name: Some(name),
            ..Self::new()
        }
    }
}
