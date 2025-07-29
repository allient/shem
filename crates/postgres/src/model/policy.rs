use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum PolicyCommand {
    All,
    Select,
    Insert,
    Update,
    Delete,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Policy {
    pub oid: u32,
    pub name: Option<String>, // None signifies 'ENABLE ROW LEVEL SECURITY'
    pub table_oid: u32,
    pub table_name: String,
    pub schema: String,
    pub command: PolicyCommand,
    pub permissive: bool,
    pub roles: Vec<String>, // List of role names
    pub using: Option<String>,
    pub check: Option<String>,
    pub is_user_defined: bool,
    pub is_from_extension: bool,
}