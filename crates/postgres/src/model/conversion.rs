use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Conversion {
    pub oid: u32,
    pub name: String,
    pub schema: String,
    pub owner: String,
    pub for_encoding: String,
    pub to_encoding: String,
    pub function_name: String, // Fully qualified
    pub is_default: bool,
    pub comment: Option<String>,
    pub is_from_extension: bool,
}