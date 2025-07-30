use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Trigger {
    pub oid: u32,
    pub name: String,
    pub table_oid: u32,
    pub table_name: String,
    pub schema: String,
    /// The full, raw "CREATE CONSTRAINT TRIGGER..." or "CREATE TRIGGER..." statement.
    pub definition: String,
    pub is_constraint: bool,
    pub comment: Option<String>,
    pub is_from_extension: bool,
}

