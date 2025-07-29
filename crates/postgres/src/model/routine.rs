use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Routine {
    Function(Function),
    Procedure(Procedure),
    Aggregate(Aggregate),
}

/// Represents a procedure (prokind = 'p').
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Procedure {
    pub oid: u32,
    pub name: String,
    pub schema: String,
    pub owner: String,
    /// The full "CREATE OR REPLACE PROCEDURE..." statement from pg_get_functiondef().
    pub definition: String,
    /// A minimal signature for identification, e.g., "my_proc(integer, text)".
    pub identity_arguments: String,
    pub acl: Option<String>,
    pub comment: Option<String>,
    pub is_from_extension: bool,
}

/// Represents a regular function (prokind = 'f') or a window function (prokind = 'w').
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Function {
    pub oid: u32,
    pub name: String,
    pub schema: String,
    pub owner: String,
    // The full "CREATE OR REPLACE FUNCTION..." statement from pg_get_functiondef()
    pub definition: String,
    // A simplified signature for identification and dependency mapping,
    // e.g., "my_func(integer, text)"
    pub identity_arguments: String,
    pub acl: Option<String>,
    pub comment: Option<String>,
    pub is_from_extension: bool,
}

/// Represents an aggregate function (prokind = 'a').
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Aggregate {
    pub oid: u32,
    pub name: String,
    pub schema: String,
    pub owner: String,
    /// The full "CREATE AGGREGATE..." statement, which we will construct.
    pub definition: String,
    /// The signature for identification, e.g., "my_agg(integer)".
    pub identity_arguments: String,
    pub acl: Option<String>,
    pub comment: Option<String>,
    pub is_from_extension: bool,
}
