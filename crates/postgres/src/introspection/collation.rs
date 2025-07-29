use crate::model::collation::Collation;
use tokio_postgres::{Client, Error, GenericClient};

pub async fn introspect_collations<C: GenericClient>(
    client: &C,
    include_predefined: bool,
) -> Result<Vec<Collation>> {
    // 1. Get server version and last system OID for robust filtering
    let version_row = client.query_one("SHOW server_version_num", &[]).await?;
    let server_version_num: i32 = version_row.get::<_, String>(0).parse().unwrap_or(0);

    // Check if datlastsysoid column exists in pg_database
    let column_exists_query = r#"
        SELECT EXISTS (
            SELECT 1 FROM information_schema.columns 
            WHERE table_schema = 'pg_catalog' 
            AND table_name = 'pg_database' 
            AND column_name = 'datlastsysoid'
        ) as column_exists;
    "#;
    let column_exists_row = client.query_one(column_exists_query, &[]).await?;
    let datlastsysoid_exists: bool = column_exists_row.get("column_exists");

    // Get the last system OID to reliably distinguish user-defined from system objects
    let last_system_oid: u32 = if datlastsysoid_exists {
        let last_system_oid_row = client
            .query_one(
                "SELECT datlastsysoid FROM pg_database WHERE datname = current_database()",
                &[],
            )
            .await?;
        last_system_oid_row.get("datlastsysoid")
    } else {
        // Fallback for older versions: use a reasonable default
        16384
    };

    // 2. Build a version-aware query to fetch ALL collations
    let mut query = String::from(
        r#"
        SELECT
            c.oid,
            c.collname AS name,
            n.nspname AS schema_name,
            pg_get_userbyid(c.collowner) AS owner,
            c.collcollate AS lc_collate,
            c.collctype AS lc_ctype,
            obj_description(c.oid, 'pg_collation') AS comment,
            EXISTS (
                SELECT 1 FROM pg_depend d
                WHERE d.objid = c.oid AND d.classid = 'pg_collation'::regclass AND d.deptype = 'e'
            ) AS is_from_extension
    "#,
    );

    // Add version-specific columns with fallbacks for older versions
    if server_version_num >= 100000 {
        query.push_str(", c.collprovider, c.collversion AS version");
    } else {
        query.push_str(", 'c'::char AS collprovider, NULL AS version");
    }
    if server_version_num >= 120000 {
        query.push_str(", c.collisdeterministic AS deterministic");
    } else {
        query.push_str(", true AS deterministic");
    }
    if server_version_num >= 170000 {
        // colllocale is new in v17
        query.push_str(", c.colllocale AS icu_locale");
    } else if server_version_num >= 150000 {
        // colliculocale was the old name
        query.push_str(", c.colliculocale AS icu_locale");
    } else {
        query.push_str(", NULL AS icu_locale");
    }
    if server_version_num >= 160000 {
        query.push_str(", c.collicurules AS icu_rules");
    } else {
        query.push_str(", NULL AS icu_rules");
    }

    query.push_str(" FROM pg_collation c JOIN pg_namespace n ON c.collnamespace = n.oid");

    let rows = client.query(query.as_str(), &[]).await?;
    let mut all_collations = Vec::new();

    for row in rows {
        let oid: u32 = row.get("oid");
        let provider_char: i8 = row.get("collprovider"); // 'c', 'i', 'b', 'd'

        let provider_enum = match provider_char as u8 as char {
            'c' => CollationProvider::Libc,
            'i' => CollationProvider::Icu,
            'b' => CollationProvider::Builtin,
            'd' => CollationProvider::Default,
            _ => CollationProvider::Libc, // Safe fallback
        };

        all_collations.push(Collation {
            oid,
            name: row.get("name"),
            owner: row.get("owner"),
            schema: row.get("schema_name"),
            provider: provider_enum,
            deterministic: row.get("deterministic"),
            lc_collate: row.get("lc_collate"),
            lc_ctype: row.get("lc_ctype"),
            icu_locale: row.get("icu_locale"),
            icu_rules: row.get("icu_rules"),
            version: row.get("version"),
            comment: row.get("comment"),
            is_user_defined: oid > last_system_oid,
            is_from_extension: row.get("is_from_extension"),
        });
    }

    // 3. Apply filtering based on the flag
    if include_predefined {
        Ok(all_collations)
    } else {
        let dumpable_collations = all_collations
            .into_iter()
            .filter(|c| c.is_user_defined && !c.is_from_extension)
            .collect();
        Ok(dumpable_collations)
    }
}
