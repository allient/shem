use serde::{Deserialize, Serialize};
use super::data_types::DataType;

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