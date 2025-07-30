use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Rule {
    pub oid: u32,
    pub name: String,
    pub table_oid: u32,
    pub table_name: String,
    pub schema: String,
    pub definition: String, // The full, raw CREATE RULE statement text
    pub comment: Option<String>,
    pub is_user_defined: bool, // Though all non-_RETURN rules are
    pub is_from_extension: bool,
}