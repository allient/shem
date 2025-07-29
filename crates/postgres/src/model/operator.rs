use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Operator {
    pub oid: u32,
    pub name: String,
    pub schema: String,
    pub owner: String,
    pub definition: String, // From pg_get_operator_def() or similar
    pub comment: Option<String>,
    pub is_from_extension: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OpFamily {
    pub oid: u32,
    pub name: String,
    pub schema: String,
    pub owner: String,
    pub index_method: String,
    pub comment: Option<String>,
    pub is_from_extension: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OpClass {
    pub oid: u32,
    pub name: String,
    pub schema: String,
    pub owner: String,
    pub definition: String, // From pg_get_opclass_def() or similar
    pub comment: Option<String>,
    pub is_from_extension: bool,
}