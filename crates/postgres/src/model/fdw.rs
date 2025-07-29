use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ForeignDataWrapper {
    pub oid: u32,
    pub name: String,
    pub owner: String,
    pub handler: Option<String>,
    pub validator: Option<String>,
    pub options: HashMap<String, String>,
    pub acl: Option<String>,
    pub comment: Option<String>,
    pub is_from_extension: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Server {
    pub oid: u32,
    pub name: String,
    pub owner: String,
    pub fdw_oid: u32, // OID of the ForeignDataWrapper it uses
    pub fdw_name: String,
    pub server_type: Option<String>,
    pub version: Option<String>,
    pub options: HashMap<String, String>,
    pub acl: Option<String>,
    pub comment: Option<String>,
    pub is_from_extension: bool,
}