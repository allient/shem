use crate::{helpers::parse_options, model::global::{Role, Tablespace}};
use common::error::Result;
use std::collections::HashMap;
use tokio_postgres::GenericClient;

// Introspect roles
pub async fn introspect_roles<C: GenericClient>(
    client: &C,
    include_predefined: bool,
) -> Result<Vec<Role>> {
    // Determine server version to use the best filtering method
    let version_row = client.query_one("SHOW server_version_num", &[]).await?;
    let server_version_num: i32 = version_row.get::<_, String>(0).parse().unwrap_or(0);

    tracing::debug!("PostgreSQL server version: {}", server_version_num);

    // Check if rolsystem column exists in pg_roles
    let column_exists_query = r#"
        SELECT EXISTS (
            SELECT 1 FROM information_schema.columns 
            WHERE table_schema = 'pg_catalog' 
            AND table_name = 'pg_roles' 
            AND column_name = 'rolsystem'
        ) as column_exists;
    "#;
    let column_exists_row = client.query_one(column_exists_query, &[]).await?;
    let rolsystem_exists: bool = column_exists_row.get("column_exists");

    tracing::debug!("rolsystem column exists: {}", rolsystem_exists);

    // --- Query 1: Fetch all roles and their properties ---
    let mut role_query = String::from(
        r#"
        SELECT 
            r.oid,
            r.rolname AS name,
            r.rolsuper AS superuser,
            r.rolcreatedb AS createdb,
            r.rolcreaterole AS createrole,
            r.rolinherit AS inherit,
            r.rolcanlogin AS login,
            r.rolreplication AS replication,
            r.rolconnlimit AS connection_limit,
            r.rolvaliduntil::text AS valid_until,
            r.rolconfig AS config
    "#,
    );

    // Use `rolsystem` if it exists, otherwise fallback to name-based detection
    if rolsystem_exists {
        tracing::debug!("Using rolsystem column for predefined role detection");
        role_query.push_str(", r.rolsystem AS is_predefined ");
    } else {
        // Fallback for older versions or when rolsystem doesn't exist
        tracing::debug!("Using fallback query for predefined role detection");
        role_query.push_str(", r.rolname LIKE 'pg_%' AS is_predefined ");
    }

    role_query.push_str("FROM pg_catalog.pg_roles r ORDER BY r.rolname");

    tracing::debug!("Role query: {}", role_query);

    let role_rows = client.query(role_query.as_str(), &[]).await?;

    // --- Query 2: Fetch all role memberships at once ---
    let membership_query = r#"
        SELECT
            member AS member_oid,
            roleid AS group_oid
        FROM pg_catalog.pg_auth_members;
    "#;
    let membership_rows = client.query(membership_query, &[]).await?;

    // --- Process and Join data in memory ---

    // Map memberships for efficient lookup: member_oid -> [group_oid, group_oid, ...]
    let mut memberships: HashMap<u32, Vec<u32>> = HashMap::new();
    for row in membership_rows {
        let member_oid: u32 = row.get("member_oid");
        let group_oid: u32 = row.get("group_oid");
        memberships.entry(member_oid).or_default().push(group_oid);
    }

    // Create a temporary map of oid -> name for converting membership OIDs to names
    let oid_to_name: HashMap<u32, String> = role_rows
        .iter()
        .map(|row| (row.get("oid"), row.get("name")))
        .collect();

    // Build the final Vec<Role>
    let mut roles = Vec::with_capacity(role_rows.len());
    for row in role_rows {
        let oid: u32 = row.get("oid");
        let name: String = row.get("name");

        let member_of_oids = memberships.get(&oid).cloned().unwrap_or_default();
        let mut member_of_names: Vec<String> = member_of_oids
            .iter()
            .filter_map(|group_oid| oid_to_name.get(group_oid).cloned())
            .collect();
        member_of_names.sort(); // For consistent output

        roles.push(Role {
            oid,
            name,
            superuser: row.get("superuser"),
            createdb: row.get("createdb"),
            createrole: row.get("createrole"),
            inherit: row.get("inherit"),
            login: row.get("login"),
            replication: row.get("replication"),
            connection_limit: row.get("connection_limit"),
            password: None, // Cannot be read safely
            valid_until: row.get("valid_until"),
            member_of: member_of_names,
            config: row.get("config"),
            is_predefined: row.get("is_predefined"),
        });
    }

    // --- Apply the filter based on the new flag ---
    if include_predefined {
        // If the flag is true, return everything we collected.
        return Ok(roles);
    }
    // If the flag is false, filter out the predefined roles.
    let dumpable_roles = roles.into_iter().filter(|r| !r.is_predefined).collect();
    Ok(dumpable_roles)
}

pub async fn introspect_tablespaces<C: GenericClient>(client: &C) -> Result<Vec<Tablespace>> {
    // The query is now more robust and complete.
    let query = r#"
        SELECT 
            t.oid,
            t.spcname AS name,
            pg_get_userbyid(t.spcowner) AS owner,
            pg_tablespace_location(t.oid) AS location,
            t.spcoptions AS options,
            t.spcacl AS acl,
            obj_description(t.oid, 'pg_tablespace') AS comment
        FROM pg_tablespace t
        WHERE t.spcname NOT IN ('pg_default', 'pg_global')
        AND NOT EXISTS (
            SELECT 1 FROM pg_depend d
            WHERE d.objid = t.oid AND d.classid = 'pg_tablespace'::regclass AND d.deptype = 'e'
        )
        ORDER BY t.spcname
    "#;

    let rows = client.query(query, &[]).await?;
    let mut tablespaces = Vec::new();

    for row in rows {
        // The `options` column is of type text[], so we get it as Vec<String>.
        let options_vec: Option<Vec<String>> = row.get("options");

        tablespaces.push(Tablespace {
            oid: row.get("oid"),
            name: row.get("name"),
            location: row.get("location"),
            owner: row.get("owner"),
            options: options_vec.map_or_else(HashMap::new, |opts| parse_options(&opts)),
            acl: row.get("acl"),
            comment: row.get("comment"),
        });
    }

    Ok(tablespaces)
}
