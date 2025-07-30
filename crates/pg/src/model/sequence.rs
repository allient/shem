use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OwnedBy {
    pub table_schema: String,
    pub table_name: String,
    pub column_name: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Sequence {
    pub oid: u32,
    pub name: String,
    pub schema: Option<String>,
    pub owner: String,
    pub data_type: String, // "smallint", "integer", or "bigint"
    pub start: i64,
    pub increment: i64,
    pub min_value: Option<i64>,
    pub max_value: Option<i64>,
    pub cache: i64,
    pub cycle: bool,
    // Data-related fields
    pub current_value: Option<i64>, // Can be NULL if sequence hasn't been used
    pub is_called: bool,            // Has nextval been called since the last setval?
    // Ownership and permissions
    pub owned_by: Option<String>, // Format: "schema.table.column"
    pub acl: Option<String>,
    pub comment: Option<String>,
    // Filtering flags
    pub is_user_defined: bool,
    pub is_from_extension: bool,
}
