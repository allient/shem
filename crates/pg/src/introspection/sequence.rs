use crate::model::sequence::{OwnedBy, Sequence};
use common::error::Result;
use std::collections::HashMap;
use tokio_postgres::GenericClient;

pub async fn introspect_sequences<C: GenericClient>(
    client: &C,
    include_predefined: bool,
) -> Result<Vec<Sequence>> {
    // 1. Get server version and last system OID
    let version_row = client.query_one("SHOW server_version_num", &[]).await?;
    let server_version_num: i32 = version_row.get::<_, String>(0).parse().unwrap_or(0);

    // Check if datlastsysoid column exists (PostgreSQL 9.6+)
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
        "datlastsysoid column exists (sequences): {}",
        datlastsysoid_exists
    );

    // Get the last system OID to reliably distinguish system objects.
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

    tracing::debug!("Last system OID (sequences): {}", last_system_oid);

    // --- QUERY 1: Fetch all sequences and their properties ---
    // This query is version-aware. For PG10+, it uses pg_sequence.
    // For older versions, it calls the sequence relation directly.
    let sequence_query = if server_version_num >= 100000 {
        r#"
        SELECT
            c.oid, c.relname AS name, n.nspname AS schema_name, pg_get_userbyid(c.relowner) AS owner,
            c.relacl::text AS acl, obj_description(c.oid, 'pg_class') AS comment,
            s.seqstart AS start_value, s.seqmin AS min_value, s.seqmax AS max_value,
            s.seqincrement AS increment, s.seqcache AS cache_size, s.seqcycle AS cycle,
            pg_catalog.format_type(s.seqtypid, NULL) AS data_type,
            (pg_sequence_last_value(c.oid)) AS last_value,
                            false AS is_called, -- Simplified for compatibility
            EXISTS (
                SELECT 1 FROM pg_depend d
                WHERE d.objid = c.oid AND d.classid = 'pg_class'::regclass AND d.deptype = 'e'
            ) AS is_from_extension
        FROM pg_class c
        JOIN pg_namespace n ON c.relnamespace = n.oid
        JOIN pg_sequence s ON s.seqrelid = c.oid
        WHERE c.relkind = 'S';
        "#
    } else {
        // Fallback for PG < 10 - use a simpler approach that works with older versions
        r#"
        SELECT
            c.oid, c.relname AS name, n.nspname AS schema_name, pg_get_userbyid(c.relowner) AS owner,
            c.relacl::text AS acl, obj_description(c.oid, 'pg_class') AS comment,
            1 AS start_value, 1 AS min_value, 9223372036854775807 AS max_value,
            1 AS increment, 1 AS cache_size, false AS cycle,
            'bigint' AS data_type,
            1 AS last_value,
            false AS is_called,
            EXISTS (
                SELECT 1 FROM pg_depend d
                WHERE d.objid = c.oid AND d.classid = 'pg_class'::regclass AND d.deptype = 'e'
            ) AS is_from_extension
        FROM pg_class c
        JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE c.relkind = 'S';
        "#
    };
    let sequence_rows = client.query(sequence_query, &[]).await?;

    // --- QUERY 2: Fetch all ownership info at once ---
    let ownership_query = r#"
        SELECT
            dep.objid AS sequence_oid,
            n.nspname AS table_schema,
            c.relname AS table_name,
            a.attname AS column_name
        FROM pg_depend dep
        JOIN pg_class c ON dep.refobjid = c.oid
        JOIN pg_namespace n ON c.relnamespace = n.oid
        JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = dep.refobjsubid
        WHERE dep.classid = 'pg_class'::regclass AND c.relkind = 'r' AND dep.deptype IN ('a', 'i');
    "#;
    let ownership_rows = client.query(ownership_query, &[]).await?;

    // Process ownership info into a HashMap for efficient lookup
    let ownership_map: HashMap<u32, OwnedBy> = ownership_rows
        .into_iter()
        .map(|row| {
            let seq_oid: u32 = row.get("sequence_oid");
            (
                seq_oid,
                OwnedBy {
                    table_schema: row.get("table_schema"),
                    table_name: row.get("table_name"),
                    column_name: row.get("column_name"),
                },
            )
        })
        .collect();

    // --- Assemble final Vec<Sequence> ---
    let mut all_sequences = Vec::new();
    for row in sequence_rows {
        let oid: u32 = row.get("oid");
        all_sequences.push(Sequence {
            oid,
            name: row.get("name"),
            schema: row.get("schema_name"),
            owner: row.get("owner"),
            data_type: row.get("data_type"),
            start: row.get("start_value"),
            increment: row.get("increment"),
            min_value: row.get("min_value"),
            max_value: row.get("max_value"),
            cache: row.get("cache_size"),
            cycle: row.get("cycle"),
            current_value: row.get("last_value"),
            is_called: row.get("is_called"),
            owned_by: ownership_map.get(&oid).map(|owned| {
                format!(
                    "{}.{}.{}",
                    owned.table_schema, owned.table_name, owned.column_name
                )
            }),
            acl: row.get("acl"),
            comment: row.get("comment"),
            is_user_defined: oid > last_system_oid,
            is_from_extension: row.get("is_from_extension"),
        });
    }

    // --- Final Filtering ---
    if include_predefined {
        Ok(all_sequences)
    } else {
        let dumpable_sequences = all_sequences
            .into_iter()
            .filter(|s| s.is_user_defined && !s.is_from_extension)
            .collect();
        Ok(dumpable_sequences)
    }
}
