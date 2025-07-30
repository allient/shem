use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum CollationProvider {
    Libc,
    Icu,
    Builtin,
    Default,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Collation {
    pub oid: u32,
    pub name: String,
    pub owner: String,
    pub schema: String,
    pub provider: CollationProvider,
    pub deterministic: bool,
    pub lc_collate: Option<String>,
    pub lc_ctype: Option<String>,
    pub icu_locale: Option<String>, // For ICU collations (Postgres 15+)
    pub icu_rules: Option<String>,  // For ICU collations (Postgres 16+)
    pub version: Option<String>,    // (Postgres 10+)
    pub comment: Option<String>,    // We'll add this to the query
    pub is_user_defined: bool,
    pub is_from_extension: bool,
}
