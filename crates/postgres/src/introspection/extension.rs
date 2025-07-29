use crate::model::extension::Extension;
use tokio_postgres::{Client, Error, GenericClient};

// Introspect extensions
pub async fn introspect_extensions<C: GenericClient>(
    client: &C,
    include_predefined: bool,
) -> Result<Vec<Extension>> {
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

    tracing::debug!(
        "datlastsysoid column exists (extensions): {}",
        datlastsysoid_exists
    );

    // Get the last system OID to reliably distinguish system objects from user objects.
    let last_system_oid: u32 = if datlastsysoid_exists {
        let last_system_oid_row = client
            .query_one(
                "SELECT datlastsysoid FROM pg_database WHERE datname = current_database()",
                &[],
            )
            .await?;
        last_system_oid_row.get("datlastsysoid")
    } else {
        // Fallback for older PostgreSQL versions - use a reasonable default
        // This is the OID where user objects typically start
        16384
    };

    tracing::debug!("Last system OID (extensions): {}", last_system_oid);

    // 2. The query is now simpler. It fetches ALL extensions and more data fields.
    let query = r#"
        SELECT
            e.oid,
            e.extname,
            pg_get_userbyid(e.extowner) AS extowner,
            e.extrelocatable,
            e.extversion,
            n.nspname AS extschema,
            obj_description(e.oid, 'pg_extension') AS comment
        FROM pg_catalog.pg_extension e
        JOIN pg_catalog.pg_namespace n ON e.extnamespace = n.oid;
    "#;

    let rows = client.query(query, &[]).await?;
    let mut extensions = Vec::new();

    // 3. Process the rows and populate our more complete struct
    for row in rows {
        let oid: u32 = row.get("oid");

        extensions.push(Extension {
            oid,
            name: row.get("extname"),
            owner: row.get("extowner"),
            relocatable: row.get("extrelocatable"),
            version: row.get("extversion"),
            schema: row.get("extschema"),
            comment: row.get("comment"),
            is_user_defined: oid > last_system_oid,
        });
    }

    // --- Apply the filter based on the new flag ---
    if include_predefined {
        // If the flag is true, return everything we collected.
        return Ok(extensions);
    }
    let dumpable_extensions = extensions
        .into_iter()
        .filter(|e| e.is_user_defined)
        .collect();
    Ok(dumpable_extensions)
}
