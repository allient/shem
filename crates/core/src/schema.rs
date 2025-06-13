use std::collections::HashMap;
use serde::{Serialize, Deserialize};

// Remove the circular import since all types are defined in this file
// use crate::{Table, View, MaterializedView, Function, Procedure, Type, Domain, Sequence, Extension, Trigger, Policy, Server};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Schema {
    pub tables: HashMap<String, Table>,
    pub views: HashMap<String, View>,
    pub materialized_views: HashMap<String, MaterializedView>,
    pub functions: HashMap<String, Function>,
    pub procedures: HashMap<String, Procedure>,
    pub types: HashMap<String, Type>,
    pub domains: HashMap<String, Domain>,
    pub sequences: HashMap<String, Sequence>,
    pub extensions: HashMap<String, Extension>,
    pub triggers: HashMap<String, Trigger>,
    pub policies: HashMap<String, Policy>,
    pub servers: HashMap<String, Server>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Table {
    pub name: String,
    pub schema: Option<String>,
    pub columns: Vec<Column>,
    pub constraints: Vec<Constraint>,
    pub indexes: Vec<Index>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct View {
    pub name: String,
    pub schema: Option<String>,
    pub definition: String,
    pub check_option: CheckOption,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MaterializedView {
    pub name: String,
    pub schema: Option<String>,
    pub definition: String,
    pub check_option: CheckOption,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Function {
    pub name: String,
    pub schema: Option<String>,
    pub parameters: Vec<Parameter>,
    pub returns: ReturnType,
    pub language: String,
    pub definition: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Procedure {
    pub name: String,
    pub schema: Option<String>,
    pub parameters: Vec<Parameter>,
    pub language: String,
    pub definition: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Type {
    pub name: String,
    pub schema: Option<String>,
    pub kind: TypeKind,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Domain {
    pub name: String,
    pub schema: Option<String>,
    pub base_type: String,
    pub constraints: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Sequence {
    pub name: String,
    pub schema: Option<String>,
    pub start: i64,
    pub increment: i64,
    pub min_value: Option<i64>,
    pub max_value: Option<i64>,
    pub cache: i64,
    pub cycle: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Extension {
    pub name: String,
    pub schema: Option<String>,
    pub version: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Trigger {
    pub name: String,
    pub table: String,
    pub timing: TriggerTiming,
    pub events: Vec<TriggerEvent>,
    pub function: String,
    pub arguments: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Policy {
    pub name: String,
    pub table: String,
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
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Constraint {
    pub name: String,
    pub kind: ConstraintKind,
    pub definition: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Index {
    pub name: String,
    pub columns: Vec<IndexColumn>,
    pub unique: bool,
    pub method: String,
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
pub enum TypeKind {
    Composite,
    Enum,
    Domain,
    Range,
    Base,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ConstraintKind {
    PrimaryKey,
    ForeignKey,
    Unique,
    Check,
    Exclusion,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IndexColumn {
    pub name: String,
    pub order: SortOrder,
    pub nulls_first: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Identity {
    pub always: bool,
    pub start: i64,
    pub increment: i64,
    pub min_value: Option<i64>,
    pub max_value: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct GeneratedColumn {
    pub expression: String,
    pub stored: bool,
}

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
pub enum ReturnKind {
    Table,
    SetOf,
    Scalar,
}

impl Schema {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
            views: HashMap::new(),
            materialized_views: HashMap::new(),
            functions: HashMap::new(),
            procedures: HashMap::new(),
            types: HashMap::new(),
            domains: HashMap::new(),
            sequences: HashMap::new(),
            extensions: HashMap::new(),
            triggers: HashMap::new(),
            policies: HashMap::new(),
            servers: HashMap::new(),
        }
    }
} 