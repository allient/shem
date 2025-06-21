use serde::{Deserialize, Serialize};

// Shared types that can be used by both parser and core

/// Data type definitions
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
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

/// Expression types
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Literal {
    Null,
    Boolean(bool),
    String(String),
    Number(String),
    Array(Vec<Literal>),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WhenClause {
    pub condition: Expression,
    pub result: Expression,
}

/// Column definition
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ColumnDefinition {
    pub name: String,
    pub data_type: DataType,
    pub default: Option<Expression>,
    pub not_null: bool,
    pub generated: Option<GeneratedColumn>,
    pub identity: Option<IdentityColumn>,
    pub comment: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct GeneratedColumn {
    pub expression: Expression,
    pub stored: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IdentityColumn {
    pub always: bool,
    pub start: Option<i64>,
    pub increment: Option<i64>,
    pub min_value: Option<i64>,
    pub max_value: Option<i64>,
    pub cache: Option<i64>,
    pub cycle: bool,
}

/// Table constraint types
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ForeignKeyReference {
    pub table: String,
    pub columns: Vec<String>,
    pub on_delete: Option<ReferentialAction>,
    pub on_update: Option<ReferentialAction>,
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
pub struct ExclusionElement {
    pub expression: Expression,
    pub operator: String,
    pub order: Option<SortOrder>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum SortOrder {
    Asc,
    Desc,
}

/// Function and procedure types
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FunctionParameter {
    pub name: Option<String>,
    pub data_type: DataType,
    pub default: Option<Expression>,
    pub mode: Option<ParameterMode>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ParameterMode {
    In,
    Out,
    InOut,
    Variadic,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum FunctionReturn {
    Type(DataType),
    Table(Vec<TableColumn>),
    SetOf(DataType),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TableColumn {
    pub name: String,
    pub data_type: DataType,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum FunctionBehavior {
    Immutable,
    Stable,
    Volatile,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum SecurityType {
    Invoker,
    Definer,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ParallelType {
    Unsafe,
    Restricted,
    Safe,
}

/// Trigger types
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum TriggerWhen {
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

/// Policy types
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum PolicyCommand {
    All,
    Select,
    Insert,
    Update,
    Delete,
}

/// Check option types
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum CheckOption {
    Local,
    Cascaded,
}

/// Type attribute
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TypeAttribute {
    pub name: String,
    pub data_type: DataType,
    pub collation: Option<String>,
}

/// Partition types
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PartitionDefinition {
    pub strategy: PartitionStrategy,
    pub columns: Vec<String>,
    pub partitions: Vec<Partition>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum PartitionStrategy {
    Range,
    List,
    Hash,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Partition {
    pub name: String,
    pub bounds: PartitionBounds,
    pub tablespace: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum PartitionBounds {
    Range(Vec<Expression>),
    List(Vec<Expression>),
    Hash(Expression),
}

/// Event trigger types
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum EventTriggerEvent {
    DdlCommandStart,
    DdlCommandEnd,
    TableRewrite,
    SqlDrop,
}

/// Collation provider
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum CollationProvider {
    Libc,
    Icu,
    Builtin,
}

/// Rule event
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum RuleEvent {
    Select,
    Update,
    Insert,
    Delete,
} 