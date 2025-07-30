use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Extension {
    pub oid: u32,
    pub name: String,
    pub owner: String,
    pub relocatable: bool,
    pub version: String,
    pub schema: String, // An extension must have a schema, so it's not an Option
    pub comment: Option<String>,
    pub is_user_defined: bool,
    // We will handle config tables and dependencies in a later step
    // pub config_tables: Vec<u32>,
}