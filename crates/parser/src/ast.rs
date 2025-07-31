use serde::{Serialize, Deserialize};
use std::collections::HashMap;

// Import specific types from the pg crate
use pg::model::{
    CheckOption, DataType, Expression, FunctionBehavior, FunctionParameter, FunctionReturn,
    Literal, ParallelType, ParameterMode, PolicyCommand, RuleEvent, SecurityType, TableConstraint,
    TriggerEvent, TriggerWhen,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Statement {
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
    CreatePolicy(CreatePolicy),
    CreateServer(CreateServer),
    CreateSchema(CreateSchema),
    CreatePublication(CreatePublication),
    CreateRangeType(CreateRangeType),
    CreateRole(CreateRole),
    CreateRule(CreateRule),
    CreateForeignTable(CreateForeignTable),
    CreateForeignDataWrapper(CreateForeignDataWrapper),
    CreateSubscription(CreateSubscription),
    CreateTablespace(CreateTablespace),
    AlterTable(AlterTable),
    DropObject(DropObject),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateTable {
    pub name: String,
    pub schema: Option<String>,
    pub columns: Vec<ColumnDefinition>,
    pub constraints: Vec<TableConstraint>,
    pub inherits: Vec<String>,
    pub partition_by: Option<PartitionDefinition>,
    pub tablespace: Option<String>,
    pub with_options: HashMap<String, String>,
    pub comment: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateView {
    pub name: String,
    pub schema: Option<String>,
    pub query: String,
    pub check_option: Option<CheckOption>,
    pub comment: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateMaterializedView {
    pub name: String,
    pub schema: Option<String>,
    pub query: String,
    pub tablespace: Option<String>,
    pub with_options: HashMap<String, String>,
    pub comment: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateFunction {
    pub name: String,
    pub schema: Option<String>,
    pub parameters: Vec<FunctionParameter>,
    pub return_type: FunctionReturn,
    pub language: String,
    pub behavior: FunctionBehavior,
    pub security: SecurityType,
    pub parallel: ParallelType,
    pub cost: Option<u32>,
    pub rows: Option<u32>,
    pub comment: Option<String>,
    pub body: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateProcedure {
    pub name: String,
    pub schema: Option<String>,
    pub parameters: Vec<FunctionParameter>,
    pub language: String,
    pub comment: Option<String>,
    pub body: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateEnum {
    pub name: String,
    pub schema: Option<String>,
    pub values: Vec<String>,
    pub comment: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateType {
    pub name: String,
    pub schema: Option<String>,
    pub columns: Vec<ColumnDefinition>,
    pub comment: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateDomain {
    pub name: String,
    pub schema: Option<String>,
    pub base_type: DataType,
    pub default: Option<Expression>,
    pub constraints: Vec<String>,
    pub comment: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateSequence {
    pub name: String,
    pub schema: Option<String>,
    pub data_type: Option<DataType>,
    pub start: Option<i64>,
    pub increment: Option<i64>,
    pub min_value: Option<i64>,
    pub max_value: Option<i64>,
    pub cache: Option<i64>,
    pub cycle: bool,
    pub owned_by: Option<String>,
    pub comment: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateExtension {
    pub name: String,
    pub schema: Option<String>,
    pub version: Option<String>,
    pub comment: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateTrigger {
    pub name: String,
    pub table: String,
    pub schema: Option<String>,
    pub events: Vec<TriggerEvent>,
    pub when: Option<TriggerWhen>,
    pub function: String,
    pub arguments: Vec<String>,
    pub comment: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreatePolicy {
    pub name: String,
    pub table: String,
    pub schema: Option<String>,
    pub command: PolicyCommand,
    pub roles: Vec<String>,
    pub using: Option<String>,
    pub with_check: Option<String>,
    pub comment: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateServer {
    pub name: String,
    pub foreign_data_wrapper: String,
    pub host: Option<String>,
    pub port: Option<u16>,
    pub database: Option<String>,
    pub options: HashMap<String, String>,
    pub comment: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateSchema {
    pub name: String,
    pub comment: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreatePublication {
    pub name: String,
    pub tables: Vec<String>,
    pub for_all_tables: bool,
    pub comment: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateRangeType {
    pub name: String,
    pub schema: Option<String>,
    pub subtype: DataType,
    pub subtype_op_class: Option<String>,
    pub collation: Option<String>,
    pub canonical: Option<String>,
    pub subtype_diff: Option<String>,
    pub comment: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateRole {
    pub name: String,
    pub superuser: bool,
    pub createdb: bool,
    pub createrole: bool,
    pub inherit: bool,
    pub login: bool,
    pub replication: bool,
    pub bypass_rls: bool,
    pub connection_limit: Option<i32>,
    pub password: Option<String>,
    pub valid_until: Option<String>,
    pub comment: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateRule {
    pub name: String,
    pub table: String,
    pub schema: Option<String>,
    pub event: RuleEvent,
    pub instead: bool,
    pub condition: Option<String>,
    pub actions: Vec<String>,
    pub comment: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateForeignTable {
    pub name: String,
    pub schema: Option<String>,
    pub columns: Vec<ColumnDefinition>,
    pub constraints: Vec<TableConstraint>,
    pub server: String,
    pub options: HashMap<String, String>,
    pub comment: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateForeignDataWrapper {
    pub name: String,
    pub handler: Option<String>,
    pub validator: Option<String>,
    pub options: HashMap<String, String>,
    pub comment: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateSubscription {
    pub name: String,
    pub connection: String,
    pub publication: String,
    pub options: HashMap<String, String>,
    pub comment: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateTablespace {
    pub name: String,
    pub owner: Option<String>,
    pub location: String,
    pub options: HashMap<String, String>,
    pub comment: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlterTable {
    pub table: String,
    pub schema: Option<String>,
    pub actions: Vec<AlterTableAction>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AlterTableAction {
    AddColumn(ColumnDefinition),
    DropColumn(String),
    AlterColumn {
        column: String,
        action: AlterColumnAction,
    },
    AddConstraint(TableConstraint),
    DropConstraint(String),
    EnableRowLevelSecurity,
    DisableRowLevelSecurity,
    ForceRowLevelSecurity,
    NoForceRowLevelSecurity,
    SetLogged,
    SetUnlogged,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AlterColumnAction {
    SetDataType(DataType),
    SetDefault(Expression),
    DropDefault,
    SetNotNull,
    DropNotNull,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DropObject {
    pub object_type: ObjectType,
    pub names: Vec<String>,
    pub if_exists: bool,
    pub cascade: bool,
    pub restrict: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ObjectType {
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
    Policy,
    Server,
    Schema,
    Publication,
    Role,
    Rule,
    ForeignTable,
    ForeignDataWrapper,
    Subscription,
    Tablespace,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDefinition {
    pub name: String,
    pub data_type: DataType,
    pub default: Option<Expression>,
    pub generated: Option<GeneratedColumn>,
    pub identity: Option<IdentityColumn>,
    pub not_null: bool,
    pub comment: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeneratedColumn {
    pub expression: Expression,
    pub stored: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdentityColumn {
    pub sequence_options: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionDefinition {
    pub strategy: String,
    pub columns: Vec<String>,
}

// Re-export the types from the pg crate
pub mod pg {
    pub use pg::model::*;
}