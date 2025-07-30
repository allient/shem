use std::collections::HashMap;

use common::error::Result;
use tokio_postgres::GenericClient;

/// Fetches the last system OID for the current database.
///
/// This OID serves as a boundary marker. Any object with an OID greater than
/// this value is considered a "user-defined" object and is a candidate for
/// being dumped. This is the most reliable method for filtering out built-in
/// system objects.
///
/// This function checks for the existence of the `datlastsysoid` column, which
/// was introduced in PostgreSQL 9.6. For older versions, it returns a safe,
/// hardcoded fallback value.
///
/// # Arguments
/// * `client`: A generic `tokio-postgres` client.
///
/// # Returns
/// A `Result` containing the last system OID as a `u32`.
pub async fn get_last_system_oid<C: GenericClient>(client: &C) -> Result<u32> {
    // First, check if the `datlastsysoid` column exists. It was added in PostgreSQL 9.6.
    let column_exists_query = r#"
        SELECT EXISTS (
            SELECT 1 FROM information_schema.columns 
            WHERE table_schema = 'pg_catalog' AND table_name = 'pg_database' AND column_name = 'datlastsysoid'
        );
    "#;

    let row = client.query_one(column_exists_query, &[]).await?;
    let column_exists: bool = row.get(0);

    if column_exists {
        // The modern, reliable way: query the catalog directly.
        let oid_query = "SELECT datlastsysoid FROM pg_database WHERE datname = current_database()";
        let row = client.query_one(oid_query, &[]).await?;
        Ok(row.get("datlastsysoid"))
    } else {
        // Fallback for very old PostgreSQL versions (< 9.6).
        // The value 16384 is the standard `FirstNormalObjectId` in PostgreSQL's C code.
        // It's a very safe bet that no system objects will have an OID higher than this
        // on old versions.
        Ok(16384)
    }
}

// Simple utility for quoting identifiers, needed by the helper
pub fn quote_ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

// Helper to fetch OID -> "schema"."name" mappings
pub async fn get_qualified_name_map<C: GenericClient>(
    client: &C,
    table: &str,
    namecol: &str,
    nspcol: &str,
) -> Result<HashMap<u32, String>> {
    let query = format!(
        "SELECT t.oid, n.nspname, t.{} FROM pg_catalog.{} t JOIN pg_catalog.pg_namespace n ON t.{} = n.oid",
        namecol, table, nspcol
    );
    let rows = client.query(&query, &[]).await?;
    Ok(rows
        .into_iter()
        .map(|row| {
            let oid: u32 = row.get(0);
            let schema: String = row.get(1);
            let name: String = row.get(2);
            (
                oid,
                format!("{}.{}", quote_ident(&schema), quote_ident(&name)),
            )
        })
        .collect())
}

/// Parses an array of "key=value" strings into a HashMap.
pub fn parse_options(options_array: &[String]) -> HashMap<String, String> {
    let mut options_map = HashMap::new();
    for opt in options_array {
        if let Some((key, value)) = opt.split_once('=') {
            options_map.insert(key.to_string(), value.to_string());
        }
    }
    options_map
}

pub fn pg_options_to_map(options_array: Option<Vec<String>>) -> HashMap<String, String> {
    // If the input is None, return a new empty HashMap.
    let Some(options) = options_array else {
        return HashMap::new();
    };

    options
        .into_iter()
        .filter_map(|opt_string| {
            // Split the "key=value" string at the first '='.
            // `split_once` is perfect for this.
            opt_string.split_once('=').map(|(key, value)| {
                // The value might be quoted if it contains special characters.
                // A simple trim of quotes is usually sufficient for common cases.
                let cleaned_value = value.trim_matches('\'').to_string();
                (key.to_string(), cleaned_value)
            })
        })
        .collect()
}
