use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Publication {
    pub oid: u32,
    pub name: String,
    pub owner: String,
    pub all_tables: bool,
    pub insert: bool,
    pub update: bool,
    pub delete: bool,
    pub truncate: bool,
    pub publish_via_partition_root: bool, // (PG13+)
    pub comment: Option<String>,
    // The list of tables is now stored in a separate struct
    pub is_user_defined: bool, // Though all publications are
    pub is_from_extension: bool,
}


#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PublicationTable {
    pub oid: u32, // OID of the pg_publication_rel entry
    pub publication_oid: u32,
    pub table_oid: u32,
    pub table_schema: String,
    pub table_name: String,

    // --- ADDED FOR PG15+ ---
    /// The row-filter expression, e.g., "created_at > '2023-01-01'".
    /// This corresponds to the WHERE clause.
    pub row_filter: Option<String>,

    /// An optional list of columns to be published.
    /// If None, all columns are published.
    pub column_list: Option<Vec<String>>,
}