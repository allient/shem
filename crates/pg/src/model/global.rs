use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Tablespace {
    pub oid: u32,
    pub name: String,
    pub location: String,
    pub owner: String,
    pub options: HashMap<String, String>,
    pub acl: Option<String>,
    pub comment: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Role {
    pub oid: u32,
    pub name: String,
    pub superuser: bool,
    pub createdb: bool,
    pub createrole: bool,
    pub inherit: bool,
    pub login: bool,
    pub replication: bool,
    pub connection_limit: i32, // pg_roles is not nullable, defaults to -1
    pub password: Option<String>, // Always None for security reasons
    pub valid_until: Option<String>,
    pub member_of: Vec<String>,
    pub config: Option<Vec<String>>, // For ALTER ROLE ... SET
    pub is_predefined: bool,
}
