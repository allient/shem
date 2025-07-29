use serde::{Deserialize, Serialize};


#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Schema {
    pub oid: u32,
    pub name: String,
    pub owner: String,
    pub acl: Option<String>,
    pub comment: Option<String>,
    pub is_user_defined: bool,
    pub is_from_extension: bool,
}