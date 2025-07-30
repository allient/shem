use crate::{helpers::get_last_system_oid, model::schema::Schema};
use common::error::Result;
use tokio_postgres::GenericClient;

// Introspect schemas
pub async fn introspect_named_schemas<C: GenericClient>(
    client: &C,
    include_predefined: bool,
) -> Result<Vec<Schema>> {
    // Check if datlastsysoid column exists in pg_database
    let last_system_oid = get_last_system_oid(client).await?;

    tracing::debug!("Last system OID: {}", last_system_oid);

    // 2. Query for ALL schemas, including system ones, and also get extension info.
    //    We need all of them initially so that every object can be linked to a schema.
    let query = r#"
        SELECT 
            n.oid,
            n.nspname AS name,
            pg_get_userbyid(n.nspowner) AS owner,
            n.nspacl::text AS acl,
            obj_description(n.oid, 'pg_namespace') AS comment,
            EXISTS (
                SELECT 1 FROM pg_depend d
                WHERE d.objid = n.oid AND d.classid = 'pg_namespace'::regclass AND d.deptype = 'e'
            ) AS is_from_extension
        FROM pg_namespace n;
    "#;

    let rows = client.query(query, &[]).await?;
    let mut schemas = Vec::new();

    for row in rows {
        let oid: u32 = row.get("oid");
        let name: String = row.get("name");

        let mut is_user_defined = false;

        // 3. Apply pg_dump's filtering logic in Rust.
        if oid > last_system_oid {
            // Any schema with a high OID is definitely user-defined.
            is_user_defined = true;
        } else {
            // For low-OID schemas, only include 'public'.
            // pg_dump has special logic for 'public'. Other pg_% schemas are ignored.
            if name == "public" {
                is_user_defined = true;
            }
        }

        // Exclude temporary schemas which are session-specific.
        if name.starts_with("pg_temp_") {
            is_user_defined = false;
        }

        // A schema belonging to an extension is generally not dumped on its own.
        let is_from_extension: bool = row.get("is_from_extension");
        if is_from_extension {
            // You might still want to know about it, but you wouldn't generate a
            // `CREATE SCHEMA` statement for it. We keep the flag for clarity.
        }

        schemas.push(Schema {
            oid,
            name,
            owner: row.get("owner"),
            acl: row.get("acl"),
            comment: row.get("comment"),
            is_user_defined,
            is_from_extension,
        });
    }

    // --- Apply the filter based on the new flag ---
    if include_predefined {
        // If the flag is true, return everything we collected.
        return Ok(schemas);
    }
    // If the flag is false, filter out the predefined schemas.
    let dumpable_schemas = schemas
        .into_iter()
        .filter(|s| !s.is_from_extension)
        .collect();
    Ok(dumpable_schemas)
}
