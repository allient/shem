use serde::{Deserialize, Serialize};
use super::data_types::DataType;
use super::expressions::Expression;

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