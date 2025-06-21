use serde::{Serialize, Deserialize};
use std::collections::HashMap;
use shem_shared_types::*;

/// Schema definition containing all database objects
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaDefinition {
    pub named_schemas: Vec<CreateSchema>,
    pub tables: Vec<CreateTable>,
    pub views: Vec<CreateView>,
    pub materialized_views: Vec<CreateMaterializedView>,
    pub functions: Vec<CreateFunction>,
    pub procedures: Vec<CreateProcedure>,
    pub enums: Vec<CreateEnum>,
    pub types: Vec<CreateType>,
    pub domains: Vec<CreateDomain>,
    pub sequences: Vec<CreateSequence>,
    pub extensions: Vec<CreateExtension>,
    pub triggers: Vec<CreateTrigger>,
    pub constraint_triggers: Vec<CreateConstraintTrigger>,
    pub event_triggers: Vec<CreateEventTrigger>,
    pub policies: Vec<CreatePolicy>,
    pub servers: Vec<CreateServer>,
    pub collations: Vec<CreateCollation>,
    pub rules: Vec<CreateRule>,
    pub range_types: Vec<CreateRangeType>,
    pub publications: Vec<CreatePublication>,
    pub subscriptions: Vec<CreateSubscription>,
    pub roles: Vec<CreateRole>,
    pub tablespaces: Vec<CreateTablespace>,
    pub foreign_tables: Vec<CreateForeignTable>,
    pub foreign_data_wrappers: Vec<CreateForeignDataWrapper>,
    pub foreign_key_constraints: Vec<CreateForeignKeyConstraint>,
}

impl SchemaDefinition {
    pub fn new() -> Self {
        Self {
            named_schemas: Vec::new(),
            tables: Vec::new(),
            views: Vec::new(),
            materialized_views: Vec::new(),
            functions: Vec::new(),
            procedures: Vec::new(),
            enums: Vec::new(),
            types: Vec::new(),
            domains: Vec::new(),
            sequences: Vec::new(),
            extensions: Vec::new(),
            triggers: Vec::new(),
            constraint_triggers: Vec::new(),
            event_triggers: Vec::new(),
            policies: Vec::new(),
            servers: Vec::new(),
            collations: Vec::new(),
            rules: Vec::new(),
            range_types: Vec::new(),
            publications: Vec::new(),
            subscriptions: Vec::new(),
            roles: Vec::new(),
            tablespaces: Vec::new(),
            foreign_tables: Vec::new(),
            foreign_data_wrappers: Vec::new(),
            foreign_key_constraints: Vec::new(),
        }
    }
}

/// SQL statement types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Statement {
    CreateSchema(CreateSchema),
    CreateTable(CreateTable),
    CreateView(CreateView),
    CreateMaterializedView(CreateMaterializedView),
    CreateFunction(CreateFunction),
    CreateProcedure(CreateProcedure),
    CreateEnum(CreateEnum),
    CreateType(CreateType),
    CreateDomain(CreateDomain),
    CreateSequence(CreateSequence),
    CreateExtension(CreateExtension),
    CreateTrigger(CreateTrigger),
    CreateConstraintTrigger(CreateConstraintTrigger),
    CreateEventTrigger(CreateEventTrigger),
    CreatePolicy(CreatePolicy),
    CreateServer(CreateServer),
    CreateCollation(CreateCollation),
    CreateRule(CreateRule),
    CreateRangeType(CreateRangeType),
    CreatePublication(CreatePublication),
    CreateSubscription(CreateSubscription),
    CreateRole(CreateRole),
    CreateTablespace(CreateTablespace),
    CreateForeignTable(CreateForeignTable),
    CreateForeignDataWrapper(CreateForeignDataWrapper),
    CreateForeignKeyConstraint(CreateForeignKeyConstraint),
    AlterTable(AlterTable),
    DropObject(DropObject),
}

/// Table definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateTable {
    pub name: String,
    pub schema: Option<String>,
    pub columns: Vec<ColumnDefinition>,
    pub constraints: Vec<TableConstraint>,
    pub partition_by: Option<PartitionDefinition>,
    pub inherits: Vec<String>,
    pub with_options: HashMap<String, String>,
    pub tablespace: Option<String>,
    pub comment: Option<String>,
}

/// Column definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDefinition {
    pub name: String,
    pub data_type: DataType,
    pub default: Option<Expression>,
    pub not_null: bool,
    pub generated: Option<GeneratedColumn>,
    pub identity: Option<IdentityColumn>,
    pub comment: Option<String>,
}

/// Data type
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DataType {
    SmallInt,
    Integer,
    BigInt,
    Decimal(Option<u32>, Option<u32>),
    Numeric(Option<u32>, Option<u32>),
    Real,
    DoublePrecision,
    SmallSerial,
    Serial,
    BigSerial,
    Money,
    Character(Option<u32>),
    CharacterVarying(Option<u32>),
    Text,
    ByteA,
    Timestamp(Option<bool>),
    TimestampTz(Option<bool>),
    Date,
    Time(Option<bool>),
    TimeTz(Option<bool>),
    Interval(Option<IntervalField>),
    Boolean,
    Bit(Option<u32>),
    BitVarying(Option<u32>),
    Uuid,
    Json,
    JsonB,
    Xml,
    Array(Box<DataType>),
    Custom(String),
}

/// Table constraint
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TableConstraint {
    PrimaryKey {
        columns: Vec<String>,
        name: Option<String>,
    },
    ForeignKey {
        columns: Vec<String>,
        references: ForeignKeyReference,
        name: Option<String>,
    },
    Unique {
        columns: Vec<String>,
        name: Option<String>,
    },
    Check {
        expression: Expression,
        name: Option<String>,
    },
    Exclusion {
        elements: Vec<ExclusionElement>,
        using: String,
        name: Option<String>,
    },
}

/// View definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateView {
    pub name: String,
    pub schema: Option<String>,
    pub columns: Vec<String>,
    pub query: String,
    pub with_options: HashMap<String, String>,
    pub check_option: Option<CheckOption>,
    pub comment: Option<String>,
}

/// Materialized view definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateMaterializedView {
    pub name: String,
    pub schema: Option<String>,
    pub columns: Vec<String>,
    pub query: String,
    pub with_options: HashMap<String, String>,
    pub tablespace: Option<String>,
    pub comment: Option<String>,
}

/// Function definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateFunction {
    pub name: String,
    pub schema: Option<String>,
    pub parameters: Vec<FunctionParameter>,
    pub returns: FunctionReturn,
    pub language: String,
    pub behavior: FunctionBehavior,
    pub security: SecurityType,
    pub parallel: ParallelType,
    pub cost: Option<u32>,
    pub rows: Option<u32>,
    pub support: Option<String>,
    pub body: String,
    pub comment: Option<String>,
}

/// Procedure definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateProcedure {
    pub name: String,
    pub schema: Option<String>,
    pub parameters: Vec<FunctionParameter>,
    pub language: String,
    pub behavior: FunctionBehavior,
    pub security: SecurityType,
    pub parallel: ParallelType,
    pub cost: Option<u32>,
    pub rows: Option<u32>,
    pub body: String,
    pub comment: Option<String>,
}

/// Enum type definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateEnum {
    pub name: String,
    pub schema: Option<String>,
    pub values: Vec<String>,
}

/// Custom type definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateType {
    pub name: String,
    pub schema: Option<String>,
    pub attributes: Vec<TypeAttribute>,
    pub internallength: Option<i32>,
    pub input: Option<String>,
    pub output: Option<String>,
    pub receive: Option<String>,
    pub send: Option<String>,
    pub typmod_in: Option<String>,
    pub typmod_out: Option<String>,
    pub analyze: Option<String>,
    pub alignment: Option<String>,
    pub storage: Option<String>,
    pub category: Option<String>,
    pub preferred: Option<bool>,
    pub default: Option<String>,
    pub element: Option<String>,
    pub delimiter: Option<char>,
    pub collatable: Option<bool>,
}

/// Domain type definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateDomain {
    pub name: String,
    pub schema: Option<String>,
    pub data_type: DataType,
    pub default: Option<Expression>,
    pub not_null: bool,
    pub check: Option<Expression>,
    pub comment: Option<String>,
}

/// Sequence definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateSequence {
    pub name: String,
    pub schema: Option<String>,
    pub start: Option<i64>,
    pub increment: Option<i64>,
    pub min_value: Option<i64>,
    pub max_value: Option<i64>,
    pub cache: Option<i64>,
    pub cycle: bool,
    pub owned_by: Option<String>,
}

/// Extension definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateExtension {
    pub name: String,
    pub schema: Option<String>,
    pub version: Option<String>,
    pub cascade: bool,
}

/// Trigger definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateTrigger {
    pub name: String,
    pub table: String,
    pub schema: Option<String>,
    pub when: TriggerWhen,
    pub events: Vec<TriggerEvent>,
    pub function: String,
    pub arguments: Vec<String>,
}

/// Policy definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreatePolicy {
    pub name: String,
    pub table: String,
    pub schema: Option<String>,
    pub command: PolicyCommand,
    pub permissive: bool,
    pub roles: Vec<String>,
    pub using: Option<Expression>,
    pub with_check: Option<Expression>,
}

/// Foreign server definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateServer {
    pub name: String,
    pub server_type: Option<String>,
    pub version: Option<String>,
    pub foreign_data_wrapper: String,
    pub options: HashMap<String, String>,
}

/// Schema definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateSchema {
    pub name: String,
    pub owner: Option<String>,
    pub comment: Option<String>,
}

/// Constraint trigger definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateConstraintTrigger {
    pub name: String,
    pub table: String,
    pub schema: Option<String>,
    pub function: String,
    pub timing: TriggerWhen,
    pub events: Vec<TriggerEvent>,
    pub arguments: Vec<String>,
    pub constraint_name: String,
    pub deferrable: bool,
    pub initially_deferred: bool,
}

/// Event trigger definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateEventTrigger {
    pub name: String,
    pub event: EventTriggerEvent,
    pub function: String,
    pub enabled: bool,
    pub tags: Vec<String>,
    pub condition: Option<Expression>,
}

/// Collation definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateCollation {
    pub name: String,
    pub schema: Option<String>,
    pub lc_collate: Option<String>,
    pub lc_ctype: Option<String>,
    pub provider: CollationProvider,
    pub deterministic: bool,
}

/// Rule definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateRule {
    pub name: String,
    pub table: String,
    pub schema: Option<String>,
    pub event: RuleEvent,
    pub instead: bool,
    pub condition: Option<Expression>,
    pub actions: Vec<String>,
}

/// Range type definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateRangeType {
    pub name: String,
    pub schema: Option<String>,
    pub subtype: String,
    pub subtype_opclass: Option<String>,
    pub collation: Option<String>,
    pub canonical: Option<String>,
    pub subtype_diff: Option<String>,
    pub multirange_type_name: Option<String>,
}

/// Publication definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreatePublication {
    pub name: String,
    pub tables: Vec<String>,
    pub all_tables: bool,
    pub insert: bool,
    pub update: bool,
    pub delete: bool,
    pub truncate: bool,
}

/// Subscription definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateSubscription {
    pub name: String,
    pub connection: String,
    pub publication: Vec<String>,
    pub enabled: bool,
    pub slot_name: Option<String>,
}

/// Role definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateRole {
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

/// Tablespace definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateTablespace {
    pub name: String,
    pub location: String,
    pub owner: String,
    pub options: HashMap<String, String>,
}

/// Foreign table definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateForeignTable {
    pub name: String,
    pub schema: Option<String>,
    pub columns: Vec<ColumnDefinition>,
    pub server: String,
    pub options: HashMap<String, String>,
}

/// Foreign data wrapper definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateForeignDataWrapper {
    pub name: String,
    pub handler: Option<String>,
    pub validator: Option<String>,
    pub options: HashMap<String, String>,
}

/// Foreign key constraint definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateForeignKeyConstraint {
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

// Additional types for constraints and expressions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForeignKeyReference {
    pub table: String,
    pub columns: Vec<String>,
    pub on_delete: Option<ReferentialAction>,
    pub on_update: Option<ReferentialAction>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ReferentialAction {
    NoAction,
    Restrict,
    Cascade,
    SetNull,
    SetDefault,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExclusionElement {
    pub expression: Expression,
    pub operator: String,
    pub order: Option<SortOrder>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SortOrder {
    Asc,
    Desc,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CheckOption {
    Local,
    Cascaded,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionParameter {
    pub name: Option<String>,
    pub data_type: DataType,
    pub default: Option<Expression>,
    pub mode: Option<ParameterMode>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ParameterMode {
    In,
    Out,
    InOut,
    Variadic,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FunctionReturn {
    Type(DataType),
    Table(Vec<TableColumn>),
    SetOf(DataType),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableColumn {
    pub name: String,
    pub data_type: DataType,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FunctionBehavior {
    Immutable,
    Stable,
    Volatile,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SecurityType {
    Invoker,
    Definer,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ParallelType {
    Unsafe,
    Restricted,
    Safe,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TypeAttribute {
    pub name: String,
    pub data_type: DataType,
    pub collation: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeneratedColumn {
    pub expression: Expression,
    pub stored: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdentityColumn {
    pub always: bool,
    pub start: Option<i64>,
    pub increment: Option<i64>,
    pub min_value: Option<i64>,
    pub max_value: Option<i64>,
    pub cache: Option<i64>,
    pub cycle: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionDefinition {
    pub strategy: PartitionStrategy,
    pub columns: Vec<String>,
    pub partitions: Vec<Partition>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PartitionStrategy {
    Range,
    List,
    Hash,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Partition {
    pub name: String,
    pub bounds: PartitionBounds,
    pub tablespace: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PartitionBounds {
    Range(Vec<Expression>),
    List(Vec<Expression>),
    Hash(Expression),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum IntervalField {
    Year,
    Month,
    Day,
    Hour,
    Minute,
    Second,
    YearToMonth,
    DayToHour,
    DayToMinute,
    DayToSecond,
    HourToMinute,
    HourToSecond,
    MinuteToSecond,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Expression {
    Literal(Literal),
    Column(String),
    FunctionCall {
        name: String,
        arguments: Vec<Expression>,
    },
    BinaryOp {
        left: Box<Expression>,
        op: String,
        right: Box<Expression>,
    },
    UnaryOp {
        op: String,
        expr: Box<Expression>,
    },
    Case {
        condition: Option<Box<Expression>>,
        when_clauses: Vec<WhenClause>,
        else_clause: Option<Box<Expression>>,
    },
    Subquery(String),
    Array(Vec<Expression>),
    Row(Vec<Expression>),
    Cast {
        expr: Box<Expression>,
        data_type: DataType,
    },
    Collate {
        expr: Box<Expression>,
        collation: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Literal {
    Null,
    Boolean(bool),
    String(String),
    Number(String),
    Array(Vec<Literal>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WhenClause {
    pub condition: Expression,
    pub result: Expression,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlterTable {
    pub name: String,
    pub schema: Option<String>,
    pub actions: Vec<AlterTableAction>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AlterTableAction {
    AddColumn(ColumnDefinition),
    DropColumn(String),
    AlterColumn {
        name: String,
        action: AlterColumnAction,
    },
    AddConstraint(TableConstraint),
    DropConstraint(String),
    RenameColumn {
        old_name: String,
        new_name: String,
    },
    RenameTo(String),
    SetSchema(String),
    SetTablespace(String),
    SetOptions(HashMap<String, String>),
    Inherit(String),
    NoInherit(String),
    EnableRowLevelSecurity,
    DisableRowLevelSecurity,
    ForceRowLevelSecurity,
    NoForceRowLevelSecurity,
    ClusterOn(String),
    SetWithoutCluster,
    SetLogged,
    SetUnlogged,
    AddPartition(Partition),
    DropPartition(String),
    AttachPartition {
        name: String,
        bounds: PartitionBounds,
    },
    DetachPartition(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AlterColumnAction {
    SetDataType(DataType),
    SetDefault(Expression),
    DropDefault,
    SetNotNull,
    DropNotNull,
    SetGenerated(GeneratedColumn),
    DropGenerated,
    SetIdentity(IdentityColumn),
    DropIdentity,
    SetStorage(String),
    SetCompression(String),
    SetCollation(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DropObject {
    pub object_type: ObjectType,
    pub name: String,
    pub schema: Option<String>,
    pub cascade: bool,
    pub restrict: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ObjectType {
    Schema,
    Table,
    View,
    MaterializedView,
    Function,
    Procedure,
    Type,
    Domain,
    Sequence,
    Extension,
    Trigger,
    ConstraintTrigger,
    EventTrigger,
    Policy,
    Server,
    Collation,
    Rule,
    RangeType,
    Publication,
    Subscription,
    Role,
    Tablespace,
    ForeignTable,
    ForeignDataWrapper,
    ForeignKeyConstraint,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TriggerWhen {
    Before,
    After,
    InsteadOf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TriggerEvent {
    Insert,
    Update,
    Delete,
    Truncate,
}

#[derive(Debug, Clone)]
pub enum DropBehavior {
    Restrict,
    Cascade,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum PolicyCommand {
    All,
    Select,
    Insert,
    Update,
    Delete,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EventTriggerEvent {
    DdlCommandStart,
    DdlCommandEnd,
    TableRewrite,
    SqlDrop,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CollationProvider {
    Libc,
    Icu,
    Builtin,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RuleEvent {
    Select,
    Update,
    Insert,
    Delete,
} 