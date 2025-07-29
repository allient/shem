use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Subscription {
    pub oid: u32,
    pub name: String,
    pub owner: String,
    pub connection_info: String,
    pub publication_names: Vec<String>,
    pub slot_name: Option<String>,
    pub is_enabled: bool,
    // Add other properties like binary, streaming, etc. as needed
    pub comment: Option<String>,
    pub is_from_extension: bool, // Though unlikely
}