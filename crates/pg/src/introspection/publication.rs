use crate::model::publication::{Publication, PublicationTable};
use common::error::Result;
use tokio_postgres::GenericClient;

pub async fn introspect_publications<C: GenericClient>(client: &C) -> Result<Vec<Publication>> {
    // 1. Get server version and OID threshold
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

    // --- QUERY: Fetch all publications ---
    let mut query = String::from(
        r#"
        SELECT
            p.oid, p.pubname AS name, pg_get_userbyid(p.pubowner) AS owner,
            p.puballtables AS all_tables, p.pubinsert AS "insert", p.pubupdate AS "update",
            p.pubdelete AS "delete",
            obj_description(p.oid, 'pg_publication') AS comment,
            EXISTS (
                SELECT 1 FROM pg_depend d
                WHERE d.objid = p.oid AND d.classid = 'pg_publication'::regclass AND d.deptype = 'e'
            ) AS is_from_extension
    "#,
    );

    // Add version-specific columns
    if server_version_num >= 110000 {
        query.push_str(r#", p.pubtruncate AS "truncate""#);
    } else {
        query.push_str(r#", false AS "truncate""#);
    }
    if server_version_num >= 130000 {
        query.push_str(", p.pubviaroot AS publish_via_partition_root");
    } else {
        query.push_str(", false AS publish_via_partition_root");
    }

    query.push_str(" FROM pg_publication p");

    let rows = client.query(&query, &[]).await?;
    let mut publications = Vec::new();

    for row in rows {
        let oid: u32 = row.get("oid");
        publications.push(Publication {
            oid,
            name: row.get("name"),
            owner: row.get("owner"),
            all_tables: row.get("all_tables"),
            insert: row.get("insert"),
            update: row.get("update"),
            delete: row.get("delete"),
            truncate: row.get("truncate"),
            publish_via_partition_root: row.get("publish_via_partition_root"),
            comment: row.get("comment"),
            is_user_defined: oid > last_system_oid,
            is_from_extension: row.get("is_from_extension"),
        });
    }

    Ok(publications)
}

// This is the full, version-aware implementation.
pub async fn introspect_publication_tables<C: GenericClient>(
    client: &C,
) -> Result<Vec<PublicationTable>> {
    // 1. Get server version to decide which columns to query
    let version_row = client.query_one("SHOW server_version_num", &[]).await?;
    let server_version_num: i32 = version_row.get::<_, String>(0).parse().unwrap_or(0);

    // 2. Build the version-aware query
    let mut query = String::from(
        r#"
        SELECT
            pr.oid,
            pr.prpubid AS publication_oid,
            pr.prrelid AS table_oid,
            n.nspname AS table_schema,
            c.relname AS table_name
    "#,
    );

    // Add columns for row filters and column lists only if on PG15+
    if server_version_num >= 150000 {
        query.push_str(
            r#"
            , pg_get_expr(pr.prqual, pr.prrelid) AS row_filter,
            (
                SELECT array_agg(a.attname ORDER BY a.attnum)
                FROM pg_attribute a
                WHERE a.attrelid = pr.prrelid AND a.attnum = ANY(pr.prattrs)
            ) AS column_list
        "#,
        );
    } else {
        // Provide NULL fallbacks for older versions
        query.push_str(
            r#"
            , NULL AS row_filter,
            NULL AS column_list
        "#,
        );
    }

    query.push_str(
        r#"
        FROM pg_publication_rel pr
        JOIN pg_class c ON pr.prrelid = c.oid
        JOIN pg_namespace n ON c.relnamespace = n.oid;
    "#,
    );

    let rows = client.query(&query, &[]).await?;
    let mut publication_tables = Vec::new();

    for row in rows {
        publication_tables.push(PublicationTable {
            oid: row.get("oid"),
            publication_oid: row.get("publication_oid"),
            table_oid: row.get("table_oid"),
            table_schema: row.get("table_schema"),
            table_name: row.get("table_name"),
            // These fields will be correctly populated with Some(...) on PG15+
            // and None on older versions because of the NULL fallback.
            row_filter: row.get("row_filter"),
            column_list: row.get("column_list"),
        });
    }

    Ok(publication_tables)
}
