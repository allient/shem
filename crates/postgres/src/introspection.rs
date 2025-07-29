use std::collections::HashMap;
use crate::get_qualified_name_map;
use crate::parse_options;
use shem_core::Result;
use shem_core::schema::*;
use parser::pg_options_to_map;
use tokio_postgres::GenericClient;
use tracing::debug;

/// Introspect PostgreSQL database schema
pub async fn introspect_schema<C>(client: &C) -> Result<Schema>
where
    C: GenericClient + Sync,
{
    let mut schema = Schema::new();

    // Independent Objects (Standalone)

    // Introspect roles
    // Purpose: Manage authentication and permissions.
    // CREATE ROLE analyst WITH LOGIN PASSWORD 'secure123';
    // GRANT SELECT ON ALL TABLES IN SCHEMA public TO analyst;
    let roles = introspect_roles(&*client, false).await?;
    for role in roles {
        schema.roles.insert(role.name.clone(), role);
    }

    // Introspect extensions
    let extensions = introspect_extensions(&*client, false).await?;
    for ext in extensions {
        schema.extensions.insert(ext.name.clone(), ext);
    }

    // Introspect named schemas
    // Purpose: Namespace to organize objects (tables, functions, etc.).
    let named_schemas = introspect_named_schemas(&*client, false).await?;
    for named_schema in named_schemas {
        schema
            .named_schemas
            .insert(named_schema.name.clone(), named_schema);
    }

    // Introspect collations
    //Purpose: Define string sorting/rules (e.g., case-insensitive comparison).
    let collations = introspect_collations(&*client, false).await?;
    for collation in collations {
        schema.collations.insert(collation.name.clone(), collation);
    }

    // Introspect tablespaces
    // Purpose: Control physical storage locations on disk.
    let tablespaces = introspect_tablespaces(&*client).await?;
    for tablespace in tablespaces {
        schema
            .tablespaces
            .insert(tablespace.name.clone(), tablespace);
    }

    // Introspect types
    // Introspect enums
    //Purpose: Define a static set of values (e.g., statuses, categories).
    // Introspect domains
    // Purpose: Create a custom type with constraints (e.g., positive integers).

    // Introspect base types
    // Purpose: Fundamental types like INTEGER, TEXT, JSONB.

    // Introspect composite types
    // Purpose: Combine multiple base types (e.g., address with street, city, state).

    // Introspect range types separately for detailed information
    // Purpose: Represent a range of values (e.g., dates, numbers).

    // Introspect multirange types
    // Purpose: Discontinuous ranges (PostgreSQL 14+).
    // SELECT '[2023-01-01, 2023-01-05), [2023-02-01, 2023-02-03)'::DATEMULTIRANGE;

    // Introspect array types
    // Purpose: Store arrays of any base/composite type.
    let postgres_types = introspect_types(&*client, false).await?;
    for postgres_type in postgres_types {
        let type_name = match &postgres_type {
            Type::Base(t) => t.info.name.clone(),
            Type::Composite(t) => t.info.name.clone(),
            Type::Domain(t) => t.info.name.clone(),
            Type::Enum(t) => t.info.name.clone(),
            Type::Range(t) => t.info.name.clone(),
            Type::Pseudo(t) => t.info.name.clone(),
        };
        schema.types.insert(type_name, postgres_type);
    }

    // Introspect sequences
    //Purpose: Generate auto-incrementing IDs.
    let sequences = introspect_sequences(&*client, false).await?;
    for seq in sequences {
        schema.sequences.insert(seq.name.clone(), seq);
    }

    // Semi-Independent Objects

    // Introspect tables
    // Purpose: Store data.
    let tables = introspect_tables_unified(&*client).await?;
    for table in tables {
        schema.tables.insert(table.name.clone(), table);
    }

    // Introspect views
    // Purpose: Virtual table from a query.
    let views = introspect_views(&*client).await?;
    for view in views {
        schema.views.insert(view.name.clone(), view);
    }

    // Introspect materialized views
    let materialized_views = introspect_materialized_views(&*client).await?;
    for view in materialized_views {
        schema.materialized_views.insert(view.name.clone(), view);
    }

    // Introspect policies
    let policies = introspect_policies(&*client).await?;
    for policy in policies {
        debug!("Policy: {:?}", policy);
        schema.policies.insert(policy.name.clone(), policy);
    }

    // Introspect rules
    let rules = introspect_rules(&*client).await?;
    for rule in &rules {
        debug!("Rule: {:?}", rule);
    }
    for rule in rules {
        schema.rules.insert(rule.name.clone(), rule);
    }

    // Introspect publications
    let publications = introspect_publications(&*client).await?;
    for publication in publications {
        schema
            .publications
            .insert(publication.name.clone(), publication);
    }

    // Introspect foreign key constraints separately
    let foreign_key_constraints = introspect_foreign_key_constraints(&*client).await?;
    for constraint in foreign_key_constraints {
        schema
            .foreign_key_constraints
            .insert(constraint.name.clone(), constraint);
    }

    // Introspect functions
    let functions = introspect_functions(&*client).await?;
    for func in functions {
        schema.functions.insert(func.name.clone(), func);
    }

    // Introspect procedures
    let procedures = introspect_procedures(&*client).await?;
    for proc in procedures {
        schema.procedures.insert(proc.name.clone(), proc);
    }

    // Introspect triggers
    let triggers = introspect_triggers(&*client).await?;
    for trigger in triggers {
        schema.triggers.insert(trigger.name.clone(), trigger);
    }

    // Introspect constraint triggers separately
    let constraint_triggers = introspect_constraint_triggers(&*client).await?;
    for trigger in constraint_triggers {
        schema
            .constraint_triggers
            .insert(trigger.name.clone(), trigger);
    }

    // Introspect event triggers
    let event_triggers = introspect_event_triggers(&*client).await?;
    for trigger in event_triggers {
        schema.event_triggers.insert(trigger.name.clone(), trigger);
    }

    // // Introspect servers
    // let servers = introspect_servers(&*client).await?;
    // for server in servers {
    //     schema.servers.insert(server.name.clone(), server);
    // }

    // // Introspect foreign tables
    // let foreign_tables = introspect_foreign_tables(&*client).await?;
    // for table in foreign_tables {
    //     schema.foreign_tables.insert(table.name.clone(), table);
    // }

    // // Introspect subscriptions
    // let subscriptions = introspect_subscriptions(&*client).await?;
    // for subscription in subscriptions {
    //     schema
    //         .subscriptions
    //         .insert(subscription.name.clone(), subscription);
    // }

    // // Introspect foreign data wrappers
    // let foreign_data_wrappers = introspect_foreign_data_wrappers(&*client).await?;
    // for fdw in foreign_data_wrappers {
    //     schema.foreign_data_wrappers.insert(fdw.name.clone(), fdw);
    // }

    Ok(schema)
}

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

// Introspect schemas
async fn introspect_named_schemas<C: GenericClient>(
    client: &C,
    include_predefined: bool,
) -> Result<Vec<NamedSchema>> {
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

    tracing::debug!("datlastsysoid column exists: {}", datlastsysoid_exists);

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

        schemas.push(NamedSchema {
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

// This function now fetches all the details for the complete Column struct.
pub async fn introspect_all_columns<C: GenericClient>(
    client: &C,
    table_oids: &[u32],
) -> Result<HashMap<u32, Vec<Column>>> {
    if table_oids.is_empty() {
        return Ok(HashMap::new());
    }

    let version_row = client.query_one("SHOW server_version_num", &[]).await?;
    let server_version_num: i32 = version_row.get::<_, String>(0).parse().unwrap_or(0);

    // Build a version-aware query to fetch all column details for the given tables.
    let mut query = String::from(
        r#"
        SELECT
            a.attrelid,
            a.attname AS name,
            pg_catalog.format_type(a.atttypid, a.atttypmod) AS type_name,
            a.attnotnull AS is_not_null,
            a.atthasdef AS has_default,
            a.attisdropped,
            a.attislocal,
            CASE WHEN a.attstattarget = -1 THEN NULL ELSE a.attstattarget::integer END AS stats_target,
            a.attstorage,
            a.attacl::text AS acl,
            col_description(a.attrelid, a.attnum) AS comment,
            CASE WHEN a.attcollation <> t.typcollation THEN a.attcollation ELSE 0 END AS collation_oid,
            a.attfdwoptions
    "#,
    );

    // Add columns that only exist in newer PostgreSQL versions with safe fallbacks.
    if server_version_num >= 100000 {
        query.push_str(", a.attidentity");
    } else {
        query.push_str(", ''::char AS attidentity");
    }
    
    // Check if attgenerated and attgeneratedbin columns exist (PostgreSQL 12+)
    let generated_column_exists_query = r#"
        SELECT 
            EXISTS (
                SELECT 1 FROM information_schema.columns 
                WHERE table_schema = 'pg_catalog' 
                AND table_name = 'pg_attribute' 
                AND column_name = 'attgenerated'
            ) as attgenerated_exists,
            EXISTS (
                SELECT 1 FROM information_schema.columns 
                WHERE table_schema = 'pg_catalog' 
                AND table_name = 'pg_attribute' 
                AND column_name = 'attgeneratedbin'
            ) as attgeneratedbin_exists;
    "#;
    let generated_column_exists_row = client.query_one(generated_column_exists_query, &[]).await?;
    let attgenerated_exists: bool = generated_column_exists_row.get("attgenerated_exists");
    let attgeneratedbin_exists: bool = generated_column_exists_row.get("attgeneratedbin_exists");
    
    if server_version_num >= 120000 && attgenerated_exists {
        if attgeneratedbin_exists {
            query.push_str(", a.attgenerated, pg_get_expr(a.attgeneratedbin, a.attrelid) as generated_expr");
        } else {
            query.push_str(", a.attgenerated, NULL as generated_expr");
        }
    } else {
        query.push_str(", ''::char AS attgenerated, NULL AS generated_expr");
    }
    if server_version_num >= 140000 {
        query.push_str(", a.attcompression");
    } else {
        query.push_str(", ''::char AS attcompression");
    }

    query.push_str(
        r#"
        FROM pg_attribute a
        JOIN pg_type t ON a.atttypid = t.oid
        WHERE a.attrelid = ANY($1) AND a.attnum > 0
        ORDER BY a.attrelid, a.attnum;
    "#,
    );

    let rows = client.query(&query, &[&table_oids]).await?;

    // Helper data: Fetch all collations for name resolution in one go.
    let collations_map =
        get_qualified_name_map(client, "pg_collation", "collname", "collnamespace").await?;

    let mut map: HashMap<u32, Vec<Column>> = HashMap::new();
    for row in rows {
        let table_oid: u32 = row.get("attrelid");

        // Parse single-character fields into enums or strings
        let storage_char: i8 = row.get("attstorage");
        let storage = match storage_char as u8 as char {
            'p' => ColumnStorage::Plain,
            'e' => ColumnStorage::External,
            'x' => ColumnStorage::Extended,
            'm' => ColumnStorage::Main,
            _ => ColumnStorage::Plain, // Should not happen
        };

        let compression_char: i8 = row.get("attcompression");
        let compression = match compression_char as u8 as char {
            'p' => Some("pglz".to_string()),
            'l' => Some("lz4".to_string()),
            _ => None,
        };

        let identity_char: i8 = row.get("attidentity");
        let identity = match identity_char as u8 as char {
            'a' => Some(Identity {
                generation: IdentityGeneration::Always,
            }),
            'd' => Some(Identity {
                generation: IdentityGeneration::ByDefault,
            }),
            _ => None,
        };

        let generated_char: i8 = row.get("attgenerated");
        let generated_expr: Option<String> = row.get("generated_expr");
        let generated = match generated_char as u8 as char {
            's' => Some(Generated { 
                expression: generated_expr.unwrap_or_else(|| "(generated expression)".to_string()) 
            }),
            _ => None,
        };

        let collation_oid: u32 = row.get("collation_oid");

        let column = Column {
            name: row.get("name"),
            type_name: row.get("type_name"),
            is_not_null: row.get("is_not_null"),
            has_default: row.get("has_default"),
            is_dropped: row.get("attisdropped"),
            is_local: row.get("attislocal"),
            stats_target: row.get("stats_target"),
            storage,
            compression,
            identity,
            generated,
            acl: row.get("acl"),
            comment: row.get("comment"),
            collation: if collation_oid > 0 {
                collations_map.get(&collation_oid).cloned()
            } else {
                None
            },
            fdw_options: pg_options_to_map(row.get("attfdwoptions")),
        };

        map.entry(table_oid).or_default().push(column);
    }
    Ok(map)
}


// Bulk introspection functions for unified table introspection
async fn introspect_all_constraints<C: GenericClient>(
    client: &C,
    table_oids: &[u32],
) -> Result<HashMap<u32, Vec<Constraint>>> {
    if table_oids.is_empty() {
        return Ok(HashMap::new());
    }

    let oids_str = table_oids
        .iter()
        .map(|oid| oid.to_string())
        .collect::<Vec<_>>()
        .join(",");

    let query = format!(
        r#"
        SELECT 
            c.conrelid as table_oid,
            c.conname as constraint_name,
            c.contype::text as constraint_type,
            pg_get_constraintdef(c.oid) as constraint_definition,
            c.condeferrable as deferrable,
            c.condeferred as initially_deferred
        FROM pg_constraint c
        WHERE c.conrelid IN ({})
        "#,
        oids_str
    );

    let rows = client.query(&query, &[]).await?;
    let mut constraints_map = HashMap::new();

    for row in rows {
        let table_oid: u32 = row.get("table_oid");
        let name: String = row.get("constraint_name");
        let constraint_type_str: String = row.get("constraint_type");
        let constraint_type: char = constraint_type_str.chars().next().unwrap_or('x');
        let definition: String = row.get("constraint_definition");
        let deferrable: bool = row.get("deferrable");
        let initially_deferred: bool = row.get("initially_deferred");

        let kind = match constraint_type {
            'p' => ConstraintKind::PrimaryKey,
            'f' => {
                let references = if let Some(ref_match) = definition.find("REFERENCES ") {
                    let ref_part = &definition[ref_match + 11..];
                    if let Some(paren_pos) = ref_part.find('(') {
                        ref_part[..paren_pos].trim().to_string()
                    } else {
                        ref_part.trim().to_string()
                    }
                } else {
                    "unknown".to_string()
                };

                ConstraintKind::ForeignKey {
                    references,
                    on_delete: None,
                    on_update: None,
                }
            }
            'u' => ConstraintKind::Unique,
            'c' => ConstraintKind::Check,
            'x' => ConstraintKind::Exclusion,
            _ => continue,
        };

        let constraint = Constraint {
            name,
            kind,
            definition,
            deferrable,
            initially_deferred,
        };

        constraints_map.entry(table_oid).or_insert_with(Vec::new).push(constraint);
    }

    Ok(constraints_map)
}

async fn introspect_all_indexes<C: GenericClient>(
    client: &C,
    table_oids: &[u32],
) -> Result<HashMap<u32, Vec<Index>>> {
    if table_oids.is_empty() {
        return Ok(HashMap::new());
    }

    let oids_str = table_oids
        .iter()
        .map(|oid| oid.to_string())
        .collect::<Vec<_>>()
        .join(",");

    let query = format!(
        r#"
        SELECT 
            t.oid as table_oid,
            i.relname as index_name,
            a.attname as column_name,
            ix.indisunique as is_unique,
            am.amname as index_method,
            pg_get_expr(ix.indpred, ix.indrelid) as where_clause,
            pg_get_indexdef(ix.indexrelid) as index_definition,
            i.reltablespace as tablespace_oid,
            i.reloptions as storage_parameters,
            ix.indkey as index_keys,
            ix.indoption as index_options
        FROM pg_class t
        JOIN pg_index ix ON ix.indrelid = t.oid
        JOIN pg_class i ON i.oid = ix.indexrelid
        JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
        JOIN pg_am am ON am.oid = i.relam
        WHERE t.oid IN ({})
        ORDER BY t.oid, i.relname, array_position(ix.indkey, a.attnum)
        "#,
        oids_str
    );

    let rows = client.query(&query, &[]).await?;
    let mut indexes_map = HashMap::new();
    let mut current_index = None;

    for row in rows {
        let table_oid: u32 = row.get("table_oid");
        let name: String = row.get("index_name");
        let column_name: String = row.get("column_name");
        let is_unique: bool = row.get("is_unique");
        let method: String = row.get("index_method");
        let where_clause: Option<String> = row.get("where_clause");
        let definition: String = row.get("index_definition");
        let tablespace_oid: Option<u32> = row.get("tablespace_oid");
        let storage_parameters: Option<Vec<String>> = row.get("storage_parameters");

        let index_method = match method.as_str() {
            "btree" => IndexMethod::Btree,
            "hash" => IndexMethod::Hash,
            "gist" => IndexMethod::Gist,
            "gin" => IndexMethod::Gin,
            "brin" => IndexMethod::Brin,
            "spgist" => IndexMethod::Spgist,
            _ => IndexMethod::Btree,
        };

        if current_index.as_ref().map(|idx: &Index| idx.name.as_str()) != Some(name.as_str()) {
            // New index
            let index = Index {
                name: name.clone(),
                columns: vec![IndexColumn {
                    name: column_name.clone(),
                    expression: None,
                    order: SortOrder::Ascending,
                    nulls_first: false,
                    opclass: None,
                }],
                method: index_method,
                unique: is_unique,
                where_clause,
                tablespace: tablespace_oid.map(|oid| oid.to_string()),
                storage_parameters: storage_parameters.map(|params| {
                    params.into_iter()
                        .filter_map(|param| {
                            if let Some((key, value)) = param.split_once('=') {
                                Some((key.to_string(), value.to_string()))
                            } else {
                                None
                            }
                        })
                        .collect()
                }).unwrap_or_default(),
            };
            current_index = Some(index);
            indexes_map.entry(table_oid).or_insert_with(Vec::new).push(current_index.as_ref().unwrap().clone());
        } else {
            // Same index, add column
            if let Some(index) = current_index.as_mut() {
                index.columns.push(IndexColumn {
                    name: column_name,
                    expression: None,
                    order: SortOrder::Ascending,
                    nulls_first: false,
                    opclass: None,
                });
            }
        }
    }

    Ok(indexes_map)
}

async fn introspect_all_inheritance<C: GenericClient>(
    client: &C,
) -> Result<HashMap<u32, Vec<String>>> {
    let query = r#"
        SELECT 
            inhrelid as table_oid,
            c.relname as parent_table
        FROM pg_inherits i
        JOIN pg_class c ON c.oid = i.inhparent
        JOIN pg_namespace n ON n.oid = c.relnamespace
    "#;

    let rows = client.query(query, &[]).await?;
    let mut inheritance_map = HashMap::new();

    for row in rows {
        let table_oid: u32 = row.get("table_oid");
        let parent_table: String = row.get("parent_table");
        inheritance_map.entry(table_oid).or_insert_with(Vec::new).push(parent_table);
    }

    Ok(inheritance_map)
}

async fn introspect_all_partition_keys<C: GenericClient>(
    client: &C,
) -> Result<HashMap<u32, String>> {
    // Check if partkey column exists in pg_partitioned_table (PostgreSQL 10+)
    let partkey_column_exists_query = r#"
        SELECT EXISTS (
            SELECT 1 FROM information_schema.columns 
            WHERE table_schema = 'pg_catalog' 
            AND table_name = 'pg_partitioned_table' 
            AND column_name = 'partkey'
        ) as column_exists;
    "#;
    let partkey_column_exists_row = client.query_one(partkey_column_exists_query, &[]).await?;
    let partkey_exists: bool = partkey_column_exists_row.get("column_exists");

    if !partkey_exists {
        // Return empty map for older PostgreSQL versions that don't support partitioning
        return Ok(HashMap::new());
    }

    let query = r#"
        SELECT 
            c.oid as table_oid,
            pt.partstrat as partition_strategy,
            array_agg(a.attname ORDER BY array_position(pt.partkey, a.attnum)) as column_names
        FROM pg_class c
        JOIN pg_partitioned_table pt ON pt.partrelid = c.oid
        JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY(pt.partkey)
        WHERE c.relkind = 'p'
        GROUP BY c.oid, pt.partstrat
        ORDER BY c.oid
    "#;

    let rows = client.query(query, &[]).await?;
    let mut partition_key_map = HashMap::new();

    for row in rows {
        let table_oid: u32 = row.get("table_oid");
        let partition_strategy: char = row.get("partition_strategy");
        let column_names: Vec<String> = row.get("column_names");
        
        let strategy_str = match partition_strategy {
            'r' => "RANGE",
            'l' => "LIST", 
            'h' => "HASH",
            _ => "UNKNOWN"
        };
        
        let partition_expression = format!("{} ({})", strategy_str, column_names.join(", "));
        partition_key_map.insert(table_oid, partition_expression);
    }

    Ok(partition_key_map)
}

pub async fn introspect_tables_unified<C: GenericClient>(client: &C) -> Result<Vec<Table>> {
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

    tracing::debug!("datlastsysoid column exists: {}", datlastsysoid_exists);

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

    tracing::debug!("Last system OID: {}", last_system_oid);

    // --- QUERY 1: Fetch all relation-like objects with Replica Identity info ---
    let relations_query = r#"
        SELECT
            c.oid, c.relname AS name, n.nspname AS schema_name, pg_get_userbyid(c.relowner) AS owner,
            c.relkind, obj_description(c.oid, 'pg_class') AS comment,
            ts.spcname AS tablespace, c.relacl::text AS acl,
            c.relreplident AS replica_identity_char,
            ri_class.relname AS replica_identity_index_name, -- Get the index name directly
            EXISTS (
                SELECT 1 FROM pg_depend d
                WHERE d.objid = c.oid AND d.classid = 'pg_class'::regclass AND d.deptype = 'e'
            ) AS is_from_extension
        FROM pg_class c
        JOIN pg_namespace n ON c.relnamespace = n.oid
        LEFT JOIN pg_tablespace ts ON c.reltablespace = ts.oid
        -- Join to find the replica identity index, if one is set
        LEFT JOIN pg_index ri ON ri.indrelid = c.oid AND ri.indisreplident
        LEFT JOIN pg_class ri_class ON ri_class.oid = ri.indexrelid
        WHERE c.relkind IN ('r', 'p'); -- Only tables and partitioned tables
    "#;
    let relation_rows = client.query(relations_query, &[]).await?;

    let all_relation_oids: Vec<u32> = relation_rows.iter().map(|row| row.get("oid")).collect();

    // --- BULK QUERIES for sub-objects ---
    let columns_map = introspect_all_columns(client, &all_relation_oids).await?;
    let constraints_map = introspect_all_constraints(client, &all_relation_oids).await?;
    let indexes_map = introspect_all_indexes(client, &all_relation_oids).await?;
    let inheritance_map = introspect_all_inheritance(client).await?;
    let partition_key_map = introspect_all_partition_keys(client).await?;

    // --- Assemble the final Vec<Table> ---
    let mut tables = Vec::new();
    for row in relation_rows {
        let oid: u32 = row.get("oid");

        let is_user_defined = oid > last_system_oid;
        let is_from_extension: bool = row.get("is_from_extension");

        // Filter for dumpable tables
        if is_user_defined && !is_from_extension {
            let replica_identity_char: i8 = row.get("replica_identity_char");

            let replica_identity = match replica_identity_char as u8 as char {
                'd' => ReplicaIdentity::Default,
                'n' => ReplicaIdentity::Nothing,
                'f' => ReplicaIdentity::Full,
                'i' => {
                    let index_name: Option<String> = row.get("replica_identity_index_name");
                    ReplicaIdentity::Index(index_name.unwrap_or_default())
                }
                _ => ReplicaIdentity::Default, // Should not happen
            };

            tables.push(Table {
                oid,
                name: row.get("name"),
                schema: row.get("schema_name"),
                owner: row.get("owner"),
                comment: row.get("comment"),
                tablespace: row.get("tablespace"),
                acl: row.get("acl"),
                columns: columns_map.get(&oid).cloned().unwrap_or_default(),
                constraints: constraints_map.get(&oid).cloned().unwrap_or_default(),
                indexes: indexes_map.get(&oid).cloned().unwrap_or_default(),
                inherits: inheritance_map.get(&oid).cloned().unwrap_or_default(),
                partition_key: partition_key_map.get(&oid).cloned(),
                replica_identity,
                is_user_defined,
                is_from_extension,
            });
        }
    }

    Ok(tables)
}

async fn introspect_constraints<C: GenericClient>(
    client: &C,
    schema: &Option<String>,
    table: &str,
) -> Result<Vec<Constraint>> {
    let query = r#"
        SELECT 
            c.conname as constraint_name,
            c.contype::text as constraint_type,
            array_agg(a.attname ORDER BY array_position(c.conkey, a.attnum)) as column_names,
            c.condeferrable as deferrable,
            c.condeferred as initially_deferred,
            pg_get_constraintdef(c.oid) as constraint_definition
        FROM pg_catalog.pg_constraint c
        JOIN pg_catalog.pg_class t ON c.conrelid = t.oid
        JOIN pg_catalog.pg_namespace n ON t.relnamespace = n.oid
        JOIN pg_catalog.pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(c.conkey)
        WHERE n.nspname = $1
        AND t.relname = $2
        GROUP BY c.oid, c.conname, c.contype, c.condeferrable, c.condeferred, c.conkey
        ORDER BY c.conname
    "#;

    let rows = client.query(query, &[schema, &table.to_string()]).await?;
    let mut constraints = Vec::new();

    for row in rows {
        let name: String = row.get("constraint_name");
        let constraint_type_str: String = row.get("constraint_type");
        let constraint_type: char = constraint_type_str.chars().next().unwrap_or('x');
        let _column_names: Vec<String> = row.get("column_names");
        let deferrable: bool = row.get("deferrable");
        let initially_deferred: bool = row.get("initially_deferred");
        let definition: String = row.get("constraint_definition");

        let kind = match constraint_type {
            'p' => ConstraintKind::PrimaryKey,
            'f' => {
                // For foreign keys, we'll extract the referenced table from the constraint definition
                let references = if let Some(ref_match) = definition.find("REFERENCES ") {
                    let ref_part = &definition[ref_match + 11..];
                    if let Some(paren_pos) = ref_part.find('(') {
                        ref_part[..paren_pos].trim().to_string()
                    } else {
                        ref_part.trim().to_string()
                    }
                } else {
                    "unknown".to_string()
                };

                ConstraintKind::ForeignKey {
                    references,
                    on_delete: None, // TODO: Parse from definition if needed
                    on_update: None,
                }
            }
            'u' => ConstraintKind::Unique,
            'c' => ConstraintKind::Check,
            'x' => ConstraintKind::Exclusion,
            _ => continue,
        };

        constraints.push(Constraint {
            name,
            kind,
            definition,
            deferrable,
            initially_deferred,
        });
    }

    Ok(constraints)
}

async fn introspect_indexes<C: GenericClient>(
    client: &C,
    schema: &Option<String>,
    table: &str,
) -> Result<Vec<Index>> {
    let query = r#"
        SELECT 
            i.relname as index_name,
            a.attname as column_name,
            ix.indisunique as is_unique,
            am.amname as index_method,
            pg_get_expr(ix.indpred, ix.indrelid) as where_clause,
            pg_get_indexdef(ix.indexrelid) as index_definition,
            i.reltablespace as tablespace_oid,
            i.reloptions as storage_parameters,
            ix.indkey as index_keys,
            ix.indoption as index_options
        FROM pg_class t
        JOIN pg_index ix ON ix.indrelid = t.oid
        JOIN pg_class i ON i.oid = ix.indexrelid
        JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
        JOIN pg_am am ON am.oid = i.relam
        WHERE t.relname = $2
        AND t.relnamespace = (
            SELECT oid FROM pg_namespace WHERE nspname = $1
        )
        ORDER BY i.relname, array_position(ix.indkey, a.attnum)
    "#;

    let rows = client.query(query, &[schema, &table.to_string()]).await?;
    let mut indexes = Vec::new();
    let mut current_index = None;

    for row in rows {
        let name: String = row.get("index_name");
        let column_name: String = row.get("column_name");
        let is_unique: bool = row.get("is_unique");
        let method: String = row.get("index_method");
        let where_clause: Option<String> = row.get("where_clause");
        let _definition: String = row.get("index_definition");
        let tablespace_oid: Option<u32> = row.get("tablespace_oid");
        let storage_parameters: Option<Vec<String>> = row.get("storage_parameters");
        let index_keys: Vec<i16> = row.get("index_keys");
        let index_options: Vec<i16> = row.get("index_options");

        // Convert method string to IndexMethod enum
        let index_method = match method.as_str() {
            "btree" => IndexMethod::Btree,
            "hash" => IndexMethod::Hash,
            "gist" => IndexMethod::Gist,
            "spgist" => IndexMethod::Spgist,
            "gin" => IndexMethod::Gin,
            "brin" => IndexMethod::Brin,
            _ => IndexMethod::Btree, // Default fallback
        };

        // Get tablespace name if available
        let tablespace = if let Some(oid) = tablespace_oid {
            let ts_query = "SELECT spcname FROM pg_tablespace WHERE oid = $1";
            if let Ok(ts_rows) = client.query(ts_query, &[&oid]).await {
                ts_rows.first().map(|row| row.get::<_, String>("spcname"))
            } else {
                None
            }
        } else {
            None
        };

        // Parse storage parameters
        let storage_params = storage_parameters
            .as_deref()
            .map(parse_server_options)
            .unwrap_or_default();

        // Determine if this is an expression index
        let expression = if column_name.starts_with('(') && column_name.ends_with(')') {
            Some(column_name.clone())
        } else {
            None
        };

        // Get sort order and nulls first from index options
        let column_position = index_keys.iter().position(|&k| k > 0).unwrap_or(0);
        let index_option = index_options.get(column_position).copied().unwrap_or(0);

        let order = if (index_option & 1) != 0 {
            SortOrder::Descending
        } else {
            SortOrder::Ascending
        };

        let nulls_first = (index_option & 2) != 0;

        // Get operator class if available (simplified)
        let opclass = None; // TODO: Extract from definition if needed

        if current_index
            .as_ref()
            .map(|i: &Index| i.name != name)
            .unwrap_or(true)
        {
            if let Some(idx) = current_index.take() {
                indexes.push(idx);
            }
            current_index = Some(Index {
                name,
                columns: vec![IndexColumn {
                    name: column_name,
                    expression,
                    order,
                    nulls_first,
                    opclass,
                }],
                unique: is_unique,
                method: index_method,
                where_clause,
                tablespace,
                storage_parameters: storage_params,
            });
        } else if let Some(idx) = &mut current_index {
            idx.columns.push(IndexColumn {
                name: column_name,
                expression,
                order,
                nulls_first,
                opclass,
            });
        }
    }

    if let Some(idx) = current_index {
        indexes.push(idx);
    }

    Ok(indexes)
}

async fn introspect_views<C: GenericClient>(client: &C) -> Result<Vec<View>> {
    let query = r#"
        SELECT 
            v.table_schema,
            v.table_name,
            v.view_definition,
            v.check_option,
            pgc.relowner as owner,
            pgc.reloptions as options,
            obj_description(pgc.oid, 'pg_class') as comment
        FROM information_schema.views v
        JOIN pg_class pgc ON pgc.relname = v.table_name
        JOIN pg_namespace n ON pgc.relnamespace = n.oid AND n.nspname = v.table_schema
        WHERE v.table_schema NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
        AND pgc.relowner > 1  -- exclude system-owned views
        AND NOT EXISTS (
            -- Exclude views that are part of extensions
            SELECT 1 FROM pg_depend d
            JOIN pg_extension e ON d.refobjid = e.oid
            WHERE d.objid = pgc.oid AND d.deptype = 'e'
        )
    "#;

    let rows = client.query(query, &[]).await?;
    let mut views = Vec::new();

    for row in rows {
        let schema: Option<String> = row.get("table_schema");
        let name: String = row.get("table_name");
        let definition: String = row.get("view_definition");
        let check_option: Option<String> = row.get("check_option");
        let options: Option<Vec<String>> = row.get("options");
        let comment: Option<String> = row.get("comment");

        let check_option_enum = match check_option.as_deref() {
            Some("LOCAL") => CheckOption::Local,
            Some("CASCADED") => CheckOption::Cascaded,
            _ => CheckOption::None,
        };

        // Check for security barrier option
        let security_barrier = options
            .as_deref()
            .map(|opts| opts.iter().any(|opt| opt == "security_barrier=true"))
            .unwrap_or(false);

        // Get explicit column list if available
        let columns_query = r#"
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = $1 AND table_name = $2
            ORDER BY ordinal_position
        "#;
        let column_rows = client.query(columns_query, &[&schema, &name]).await?;
        let columns: Vec<String> = column_rows
            .iter()
            .map(|row| row.get::<_, String>("column_name"))
            .collect();

        views.push(View {
            name,
            schema,
            definition,
            check_option: check_option_enum,
            comment,
            security_barrier,
            columns,
        });
    }

    Ok(views)
}

async fn introspect_materialized_views<C: GenericClient>(
    client: &C,
) -> Result<Vec<MaterializedView>> {
    let query = r#"
        SELECT 
            mv.schemaname,
            mv.matviewname,
            mv.definition,
            c.reloptions as storage_parameters,
            c.reltablespace as tablespace_oid,
            -- Check if the materialized view has been populated with data
            -- Materialized views are typically created WITH DATA by default unless explicitly specified WITH NO DATA
            -- We check if the view has any tuples, but this might not be reliable for empty tables
            (SELECT EXISTS (
                SELECT 1 FROM pg_class c 
                JOIN pg_namespace n ON c.relnamespace = n.oid 
                WHERE c.relname = mv.matviewname 
                AND n.nspname = mv.schemaname 
                AND c.reltuples >= 0  -- Changed from > 0 to >= 0 since empty tables are still valid
            )) as has_data,
            -- Get comment on the materialized view
            (SELECT description FROM pg_description d
             JOIN pg_class c2 ON d.objoid = c2.oid
             JOIN pg_namespace n2 ON c2.relnamespace = n2.oid
             WHERE c2.relname = mv.matviewname 
             AND n2.nspname = mv.schemaname
             AND d.objsubid = 0) as comment
        FROM pg_matviews mv
        JOIN pg_class c ON c.relname = mv.matviewname
        JOIN pg_namespace n ON c.relnamespace = n.oid AND n.nspname = mv.schemaname
        WHERE mv.schemaname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
        AND c.relowner > 1
        AND NOT EXISTS (
            SELECT 1 FROM pg_depend d
            JOIN pg_extension e ON d.refobjid = e.oid
            WHERE d.objid = c.oid AND d.deptype = 'e'
        )
    "#;

    let rows = client.query(query, &[]).await?;
    let mut views = Vec::new();

    for row in rows {
        let schema: Option<String> = row.get("schemaname");
        let name: String = row.get("matviewname");
        let definition: String = row.get("definition");
        let storage_parameters: Option<Vec<String>> = row.get("storage_parameters");
        let tablespace_oid: Option<u32> = row.get("tablespace_oid");
        let comment: Option<String> = row.get("comment");

        // Materialized views are created WITH DATA by default unless explicitly specified WITH NO DATA
        // Since we can't reliably determine this from the system catalogs, we assume WITH DATA for existing views
        // The user can explicitly create views with WITH NO DATA if needed
        let populate_with_data = true;

        // Get tablespace name if available
        let tablespace = if let Some(oid) = tablespace_oid {
            let ts_query = "SELECT spcname FROM pg_tablespace WHERE oid = $1";
            if let Ok(ts_rows) = client.query(ts_query, &[&oid]).await {
                ts_rows.first().map(|row| row.get::<_, String>("spcname"))
            } else {
                None
            }
        } else {
            None
        };

        // Get indexes for this materialized view
        let indexes = introspect_indexes(client, &schema, &name).await?;

        // Parse storage parameters
        let storage_params = storage_parameters
            .as_deref()
            .map(parse_server_options)
            .unwrap_or_default();

        views.push(MaterializedView {
            name,
            schema,
            definition,
            check_option: CheckOption::None, // Materialized views don't have check options
            comment,
            tablespace,
            storage_parameters: storage_params,
            indexes,
            populate_with_data, // Use actual data presence to determine WITH DATA vs WITH NO DATA
        });
    }

    Ok(views)
}

async fn introspect_functions<C: GenericClient>(client: &C) -> Result<Vec<Function>> {
    let query = r#"
        SELECT 
            p.proname as function_name,
            n.nspname as schema_name,
            p.prosrc as function_body,
            l.lanname as language,
            pg_get_function_result(p.oid) as return_type,
            pg_get_function_arguments(p.oid) as arguments,
            p.proowner as owner,
            p.prokind as kind,
            p.provolatile::text as volatility,
            p.proleakproof as leakproof,
            p.proisstrict as strict,
            p.prosecdef as security_definer,
            p.proparallel::text as parallel_safety,
            p.procost::float8 as cost,
            p.prorows::float8 as rows,
            obj_description(p.oid, 'pg_proc') as comment
        FROM pg_proc p
        JOIN pg_namespace n ON p.pronamespace = n.oid
        JOIN pg_language l ON p.prolang = l.oid
        WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
        AND p.prokind = 'f'  -- user-defined functions only
        AND p.proowner > 1
        AND l.lanname NOT IN ('internal', 'c')  -- exclude internal and C functions
        AND NOT EXISTS (
            SELECT 1 FROM pg_depend d
            JOIN pg_extension e ON d.refobjid = e.oid
            WHERE d.objid = p.oid AND d.deptype = 'e'
        )
        AND NOT EXISTS (
            SELECT 1 WHERE p.prosrc IS NULL OR p.prosrc = ''
        )
        AND NOT EXISTS (
            -- Exclude automatically generated functions (like multirange constructors)
            SELECT 1 FROM pg_depend d
            JOIN pg_type t ON d.refobjid = t.oid
            WHERE d.objid = p.oid 
            AND d.deptype = 'a'  -- auto dependency
            AND t.typtype = 'r'  -- range type
        )
        AND p.proname NOT LIKE '%_multirange'  -- exclude multirange functions
        AND p.proname NOT LIKE '%_constructor%'  -- exclude constructor functions
        AND p.proname NOT LIKE '%_send'  -- exclude send functions
        AND p.proname NOT LIKE '%_recv'  -- exclude receive functions
        AND p.proname NOT LIKE '%_in'  -- exclude input functions
        AND p.proname NOT LIKE '%_out'  -- exclude output functions
        AND p.proname NOT LIKE '%_typmod'  -- exclude typmod functions
        AND p.proname NOT LIKE '%_analyze'  -- exclude analyze functions
        AND p.proname NOT LIKE '%_options'  -- exclude options functions
        AND p.proname NOT LIKE '%_canonical'  -- exclude canonical functions
        AND p.proname NOT LIKE '%_subtype_diff'  -- exclude subtype diff functions
    "#;

    let rows = client.query(query, &[]).await?;
    let mut functions = Vec::new();

    for row in rows {
        let name: String = row.get("function_name");
        let schema: Option<String> = row.get("schema_name");
        let definition: String = row.get("function_body");
        let language: String = row.get("language");
        let return_type: String = row.get("return_type");
        let arguments: String = row.get("arguments");
        let volatility_code: String = row.get("volatility");
        let strict: bool = row.get("strict");
        let security_definer: bool = row.get("security_definer");
        let parallel_safety_code: String = row.get("parallel_safety");
        let cost: Option<f64> = row.get("cost");
        let rows: Option<f64> = row.get("rows");
        let comment: Option<String> = row.get("comment");

        // Parse parameters from the arguments string
        let parameters = parse_function_parameters(&arguments);

        // Determine return type kind
        let returns = if return_type.contains("TABLE") {
            ReturnType {
                kind: ReturnKind::Table,
                type_name: return_type,
                is_set: false,
            }
        } else if return_type.contains("SETOF") {
            ReturnType {
                kind: ReturnKind::SetOf,
                type_name: return_type.replace("SETOF ", ""),
                is_set: true,
            }
        } else {
            ReturnType {
                kind: ReturnKind::Scalar,
                type_name: return_type,
                is_set: false,
            }
        };

        // Convert volatility code to enum
        let volatility = match volatility_code.as_str() {
            "i" => Volatility::Immutable,
            "s" => Volatility::Stable,
            "v" => Volatility::Volatile,
            _ => Volatility::Volatile,
        };

        // Convert parallel safety code to enum
        let parallel_safety = match parallel_safety_code.as_str() {
            "s" => ParallelSafety::Safe,
            "r" => ParallelSafety::Restricted,
            "u" => ParallelSafety::Unsafe,
            _ => ParallelSafety::Unsafe,
        };

        functions.push(Function {
            name,
            schema,
            parameters,
            returns,
            language,
            definition,
            comment,
            volatility,
            strict,
            security_definer,
            parallel_safety,
            cost,
            rows,
        });
    }

    Ok(functions)
}

async fn introspect_procedures<C: GenericClient>(client: &C) -> Result<Vec<Procedure>> {
    let query = r#"
        SELECT 
            p.proname as procedure_name,
            n.nspname as schema_name,
            p.prosrc as procedure_body,
            l.lanname as language,
            pg_get_function_arguments(p.oid) as arguments,
            p.proowner as owner,
            p.prosecdef as security_definer,
            obj_description(p.oid, 'pg_proc') as comment
        FROM pg_proc p
        JOIN pg_namespace n ON p.pronamespace = n.oid
        JOIN pg_language l ON p.prolang = l.oid
        WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
        AND p.prokind = 'p'  -- procedures only
        AND p.proowner > 1  -- exclude system-owned procedures
        AND NOT EXISTS (
            -- Exclude procedures that are part of extensions
            SELECT 1 FROM pg_depend d
            JOIN pg_extension e ON d.refobjid = e.oid
            WHERE d.objid = p.oid AND d.deptype = 'e'
        )
        AND NOT EXISTS (
            -- Exclude internal procedures (those with no source or C language procedures)
            SELECT 1 WHERE p.prosrc IS NULL OR p.prosrc = '' OR l.lanname = 'c'
        )
    "#;

    let rows = client.query(query, &[]).await?;
    let mut procedures = Vec::new();

    for row in rows {
        let name: String = row.get("procedure_name");
        let schema: Option<String> = row.get("schema_name");
        let definition: String = row.get("procedure_body");
        let language: String = row.get("language");
        let arguments: String = row.get("arguments");
        let security_definer: bool = row.get("security_definer");
        let comment: Option<String> = row.get("comment");

        // Parse parameters from the arguments string
        let parameters = parse_function_parameters(&arguments);

        procedures.push(Procedure {
            name,
            schema,
            parameters,
            language,
            definition,
            comment,
            security_definer,
        });
    }

    Ok(procedures)
}

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

fn parse_trigger_from_definition(
    trigger_definition: &str,
) -> (TriggerTiming, Vec<TriggerEvent>, TriggerLevel) {
    debug!("Parsing trigger definition: {}", trigger_definition);

    let mut timing = TriggerTiming::Before; // default
    let mut events = Vec::new();
    let mut for_each = TriggerLevel::Row; // default

    // Parse timing
    if trigger_definition.contains(" AFTER ") {
        timing = TriggerTiming::After;
    } else if trigger_definition.contains(" INSTEAD OF ") {
        timing = TriggerTiming::InsteadOf;
    }

    // Parse events
    if trigger_definition.contains(" INSERT") {
        events.push(TriggerEvent::Insert);
    }
    if trigger_definition.contains(" DELETE") {
        events.push(TriggerEvent::Delete);
    }
    if trigger_definition.contains(" UPDATE") {
        events.push(TriggerEvent::Update);
    }
    if trigger_definition.contains(" TRUNCATE") {
        events.push(TriggerEvent::Truncate);
    }

    // Parse FOR EACH level
    if trigger_definition.contains(" FOR EACH STATEMENT") {
        for_each = TriggerLevel::Statement;
    }

    debug!(
        "  Parsed timing: {:?}, events: {:?}, for_each: {:?}",
        timing, events, for_each
    );
    (timing, events, for_each)
}

fn parse_trigger_arguments(bytes: &[u8]) -> Vec<String> {
    let mut args = Vec::new();
    let mut current_arg = Vec::new();

    for &byte in bytes {
        if byte == 0 {
            // Null terminator - end of argument
            if !current_arg.is_empty() {
                if let Ok(arg) = String::from_utf8(current_arg.clone()) {
                    args.push(arg);
                }
                current_arg.clear();
            }
        } else {
            current_arg.push(byte);
        }
    }

    // Handle last argument if it doesn't end with null
    if !current_arg.is_empty() {
        if let Ok(arg) = String::from_utf8(current_arg) {
            args.push(arg);
        }
    }

    args
}

fn parse_when_condition(trigger_definition: &str) -> Option<String> {
    // Look for WHEN clause in the trigger definition
    let when_start = trigger_definition.find(" WHEN ")?;
    let when_clause = &trigger_definition[when_start + 6..];

    // Find the end of the WHEN clause (before EXECUTE)
    let execute_pos = when_clause.find(" EXECUTE ")?;
    let condition = when_clause[..execute_pos].trim();

    if condition.is_empty() {
        None
    } else {
        Some(condition.to_string())
    }
}

async fn introspect_triggers<C: GenericClient + Sync>(client: &C) -> Result<Vec<Trigger>> {
    let query = r#"
        SELECT 
            t.tgname AS trigger_name,
            c.relname AS table_name,
            n.nspname AS schema_name,
            p.proname AS function_name,
            t.tgtype AS trigger_type,
            t.tgargs AS trigger_arguments,
            t.tgconstraint AS constraint_oid,
            t.tgenabled::text AS enabled,
            pg_get_triggerdef(t.oid) AS trigger_definition,
            c.relowner AS owner,
            obj_description(t.oid, 'pg_trigger') as comment
        FROM pg_trigger t
        JOIN pg_class c ON t.tgrelid = c.oid
        JOIN pg_namespace n ON c.relnamespace = n.oid
        JOIN pg_proc p ON t.tgfoid = p.oid
        WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
          AND NOT t.tgisinternal
          AND NOT EXISTS (
              SELECT 1 FROM pg_depend d
              JOIN pg_extension e ON d.refobjid = e.oid
              WHERE d.objid = c.oid AND d.deptype = 'e'
          )
        ORDER BY n.nspname, c.relname, t.tgname
    "#;

    let rows = client.query(query, &[]).await?;
    let mut triggers = Vec::new();

    for row in rows {
        let name: String = row.get("trigger_name");
        let table: String = row.get("table_name");
        let schema: String = row.get("schema_name");
        let function: String = row.get("function_name");
        let trigger_type: i16 = row.get("trigger_type");
        let arguments: Option<Vec<u8>> = row.get("trigger_arguments");
        let constraint_oid: Option<u32> = row.get("constraint_oid");
        let trigger_definition: String = row.get("trigger_definition");
        let comment: Option<String> = row.get("comment");

        debug!("Trigger: {} on {}.{}", name, schema, table);
        debug!("  trigger_type: {} (0x{:x})", trigger_type, trigger_type);
        debug!("  trigger_definition: {}", trigger_definition);

        // Skip constraint triggers - they are handled separately
        if constraint_oid.is_some() && constraint_oid.unwrap() != 0 {
            continue;
        }

        let (timing, events, for_each) = parse_trigger_from_definition(&trigger_definition);
        let args = arguments
            .map(|bytes| parse_trigger_arguments(&bytes))
            .unwrap_or_default();

        // Parse WHEN condition from trigger definition
        let when = parse_when_condition(&trigger_definition);

        triggers.push(Trigger {
            name,
            table,
            schema: Some(schema),
            function,
            timing,
            events,
            arguments: args,
            condition: when.clone(), // Use the parsed WHEN condition
            for_each,
            comment,
            when,
        });
    }

    Ok(triggers)
}

async fn introspect_policies<C: GenericClient>(client: &C) -> Result<Vec<Policy>> {
    let query = r#"
        SELECT 
            p.polname as policy_name,
            c.relname as table_name,
            n.nspname as schema_name,
            p.polpermissive as permissive,
            p.polroles as roles,
                            p.polcmd::text as command,
            pg_get_expr(p.polqual, p.polrelid) as using_expression,
            pg_get_expr(p.polwithcheck, p.polrelid) as check_expression,
            c.relowner as owner
        FROM pg_policy p
        JOIN pg_class c ON p.polrelid = c.oid
        JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
        AND c.relowner > 1  -- exclude system-owned tables
        AND NOT EXISTS (
            -- Exclude policies on tables that are part of extensions
            SELECT 1 FROM pg_depend d
            JOIN pg_extension e ON d.refobjid = e.oid
            WHERE d.objid = c.oid AND d.deptype = 'e'
        )
    "#;

    let rows = client.query(query, &[]).await?;
    let mut policies = Vec::new();

    for row in rows {
        let name: String = row.get("policy_name");
        let table: String = row.get("table_name");
        let schema: Option<String> = row.get("schema_name");
        let permissive: bool = row.get("permissive");
        let roles: Vec<u32> = row.get("roles");
        let command: &str = row.get("command");
        let command_char = command.chars().next().unwrap_or('*');
        // Parse command to PolicyCommand enum
        // PostgreSQL stores: 'r'=SELECT, 'a'=INSERT, 'w'=UPDATE, 'd'=DELETE, '*'=ALL
        debug!("Raw command value from PostgreSQL: {}", command_char);
        let policy_command = match command_char {
            'r' => PolicyCommand::Select,
            'a' => PolicyCommand::Insert,
            'w' => PolicyCommand::Update,
            'd' => PolicyCommand::Delete,
            '*' => PolicyCommand::All,
            _ => PolicyCommand::All, // Default fallback
        };
        let using_expr: Option<String> = row.get("using_expression");
        let check_expr: Option<String> = row.get("check_expression");

        // Convert role OIDs to role names
        let role_names = if !roles.is_empty() {
            let role_query = "SELECT rolname FROM pg_roles WHERE oid = ANY($1)";
            if let Ok(role_rows) = client.query(role_query, &[&roles]).await {
                role_rows
                    .iter()
                    .map(|row| row.get::<_, String>("rolname"))
                    .collect()
            } else {
                roles.iter().map(|&oid| oid.to_string()).collect()
            }
        } else {
            Vec::new()
        };

        policies.push(Policy {
            name,
            table,
            schema,
            command: policy_command,
            permissive,
            roles: role_names,
            using: using_expr,
            check: check_expr,
        });
    }

    Ok(policies)
}

async fn _introspect_servers<C: GenericClient + Sync>(client: &C) -> Result<Vec<Server>> {
    let query = r#"
        SELECT 
            s.srvname AS server_name,
            f.fdwname AS foreign_data_wrapper_name,
            s.srvoptions AS server_options,
            s.srvowner AS owner
        FROM pg_foreign_server s
        JOIN pg_foreign_data_wrapper f ON s.srvfdw = f.oid
        WHERE s.srvowner > 1
        AND NOT EXISTS (
            SELECT 1
            FROM pg_depend d
            JOIN pg_extension e ON d.refobjid = e.oid
            WHERE d.objid = s.oid AND d.deptype = 'e'
        )
    "#;

    let rows = client.query(query, &[]).await?;
    let mut servers = Vec::new();

    for row in rows {
        let name: String = row.get("server_name");
        let foreign_data_wrapper: String = row.get("foreign_data_wrapper_name");
        let options: Option<Vec<String>> = row.get("server_options");

        let options_map = options
            .as_deref()
            .map(parse_server_options)
            .unwrap_or_default();

        servers.push(Server {
            name,
            foreign_data_wrapper,
            options: options_map,
            version: None, // Optional: implement if needed
        });
    }

    Ok(servers)
}

async fn introspect_event_triggers<C: GenericClient + Sync>(
    client: &C,
) -> Result<Vec<EventTrigger>> {
    let query = r#"
        SELECT 
            e.evtname AS trigger_name,
            e.evtevent AS event,
            e.evtfoid AS function_oid,
            e.evtenabled::text AS enabled,
            e.evttags AS tags,
            e.evtowner AS owner
        FROM pg_event_trigger e
        WHERE e.evtowner > 1
          AND NOT EXISTS (
              SELECT 1
              FROM pg_depend d
              JOIN pg_extension x ON d.refobjid = x.oid
              WHERE d.objid = e.oid AND d.deptype = 'e'
          )
    "#;

    let rows = client.query(query, &[]).await?;
    let mut event_triggers = Vec::new();

    for row in rows {
        let name: String = row.get("trigger_name");
        let event: String = row.get("event");
        let function_oid: u32 = row.get("function_oid");
        let enabled_str: String = row.get("enabled");
        let enabled = enabled_str.starts_with('O'); // 'O' = ENABLED, from 'O', 'D', 'R', etc.
        let tags: Option<Vec<String>> = row.get("tags");

        // Lookup function name from pg_proc
        let function_name: String = {
            let func_rows = client
                .query(
                    "SELECT proname FROM pg_proc WHERE oid = $1",
                    &[&function_oid],
                )
                .await?;
            func_rows
                .get(0)
                .map(|r| r.get("proname"))
                .unwrap_or_else(|| "unknown_function".to_string())
        };

        // Map event type
        let event_enum = match event.as_str() {
            "ddl_command_start" => EventTriggerEvent::DdlCommandStart,
            "ddl_command_end" => EventTriggerEvent::DdlCommandEnd,
            "sql_drop" => EventTriggerEvent::SqlDrop,
            "table_rewrite" => EventTriggerEvent::TableRewrite,
            _ => EventTriggerEvent::DdlCommandStart, // default fallback
        };

        event_triggers.push(EventTrigger {
            name,
            event: event_enum,
            function: function_name,
            enabled,
            tags: tags.unwrap_or_default(),
            condition: None, // TODO: support WHEN condition if needed
        });
    }

    Ok(event_triggers)
}

async fn introspect_rules<C: GenericClient>(client: &C) -> Result<Vec<Rule>>
where
    C: GenericClient + Sync,
{
    let query = r#"
        SELECT 
            r.rulename AS rule_name,
            c.relname AS table_name,
            n.nspname AS schema_name,
            r.ev_type::text AS event_type,
            r.is_instead AS is_instead,
            pg_get_ruledef(r.oid) AS rule_definition
        FROM pg_rewrite r
        JOIN pg_class c ON r.ev_class = c.oid
        JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
          AND r.rulename != '_RETURN'
          AND NOT EXISTS (
              SELECT 1 FROM pg_depend d
              JOIN pg_extension e ON d.refobjid = e.oid
              WHERE (d.objid = c.oid OR d.objid = r.oid) AND d.deptype = 'e'
          )
    "#;

    let rows = client.query(query, &[]).await?;
    let mut rules = Vec::new();

    for row in rows {
        let name: String = row.get("rule_name");
        let table: String = row.get("table_name");
        let schema: Option<String> = row.get("schema_name");
        let event_type: String = row.get("event_type");
        let is_instead: bool = row.get("is_instead");
        let definition: String = row.get("rule_definition");

        // Parse event type code
        let event = match event_type.as_str() {
            "1" => RuleEvent::Select,
            "2" => RuleEvent::Update,
            "3" => RuleEvent::Insert,
            "4" => RuleEvent::Delete,
            _ => RuleEvent::Select,
        };

        // Parse the rule definition to extract WHERE condition and action
        let (condition, action) = parse_rule_definition(&definition);

        rules.push(Rule {
            name,
            table,
            schema,
            event,
            instead: is_instead,
            condition,
            actions: vec![action], // Store just the action part
        });
    }

    Ok(rules)
}

async fn introspect_constraint_triggers<C: GenericClient>(
    client: &C,
) -> Result<Vec<ConstraintTrigger>> {
    let query = r#"
        SELECT 
            t.tgname as trigger_name,
            c.relname as table_name,
            n.nspname as schema_name,
            p.proname as function_name,
            t.tgtype as trigger_type,
            t.tgargs as trigger_arguments,
            t.tgconstraint as constraint_oid,
            c.relowner as owner,
            pg_get_triggerdef(t.oid) AS trigger_definition,
            obj_description(t.oid, 'pg_trigger') as comment
        FROM pg_trigger t
        JOIN pg_class c ON t.tgrelid = c.oid
        JOIN pg_namespace n ON c.relnamespace = n.oid
        JOIN pg_proc p ON t.tgfoid = p.oid
        WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
        AND NOT t.tgisinternal
        AND t.tgconstraint IS NOT NULL
        AND NOT EXISTS (
            SELECT 1 FROM pg_depend d
            JOIN pg_extension e ON d.refobjid = e.oid
            WHERE (d.objid = t.oid OR d.objid = c.oid) AND d.deptype = 'e'
        )
    "#;

    let rows = client.query(query, &[]).await?;
    let mut constraint_triggers = Vec::new();

    for row in rows {
        let name: String = row.get("trigger_name");
        let table: String = row.get("table_name");
        let schema: Option<String> = row.get("schema_name");
        let function: String = row.get("function_name");
        let trigger_type: i16 = row.get("trigger_type");
        let arguments: Option<Vec<u8>> = row.get("trigger_arguments");
        let constraint_oid: u32 = row.get("constraint_oid");
        let trigger_definition: String = row.get("trigger_definition");

        debug!(
            "Constraint Trigger: {} on {}.{}",
            name,
            schema.as_deref().unwrap_or("public"),
            table
        );
        debug!("  trigger_type: {} (0x{:x})", trigger_type, trigger_type);
        debug!("  trigger_definition: {}", trigger_definition);

        // Parse trigger type into timing and events from definition
        let (timing, events, _for_each) = parse_trigger_from_definition(&trigger_definition);

        // Decode arguments (null-byte separated)
        let args = if let Some(arg_bytes) = arguments {
            parse_trigger_arguments(&arg_bytes)
        } else {
            Vec::new()
        };

        // Look up constraint name and deferrable flags
        let constraint_query = r#"
            SELECT conname, condeferrable, condeferred
            FROM pg_constraint
            WHERE oid = $1
        "#;
        let constraint_rows = client.query(constraint_query, &[&constraint_oid]).await?;

        let (constraint_name, deferrable, initially_deferred) =
            if let Some(row) = constraint_rows.first() {
                (
                    row.get::<_, String>("conname"),
                    row.get::<_, bool>("condeferrable"),
                    row.get::<_, bool>("condeferred"),
                )
            } else {
                ("unknown_constraint".to_string(), false, false)
            };

        constraint_triggers.push(ConstraintTrigger {
            name,
            table,
            schema,
            function,
            timing,
            events,
            arguments: args,
            constraint_name,
            deferrable,
            initially_deferred,
        });
    }

    Ok(constraint_triggers)
}

async fn introspect_publications<C: GenericClient>(client: &C) -> Result<Vec<Publication>> {
    let query = r#"
        SELECT 
            p.pubname AS name,
            p.puballtables AS all_tables,
            p.pubinsert AS insert,
            p.pubupdate AS update,
            p.pubdelete AS delete,
            p.pubtruncate AS truncate
        FROM pg_publication p
        WHERE p.pubowner > 1
        AND NOT EXISTS (
            SELECT 1 FROM pg_depend d
            JOIN pg_extension e ON d.refobjid = e.oid
            WHERE d.objid = p.oid AND d.deptype = 'e'
        )
        ORDER BY p.pubname
    "#;

    let rows = client.query(query, &[]).await?;
    let mut publications = Vec::new();

    for row in rows {
        let name: String = row.get("name");
        let all_tables: bool = row.get("all_tables");
        let insert: bool = row.get("insert");
        let update: bool = row.get("update");
        let delete: bool = row.get("delete");
        let truncate: bool = row.get("truncate");

        // Get tables for this publication
        let tables_query = r#"
            SELECT schemaname || '.' || tablename AS table_name
            FROM pg_publication_tables
            WHERE pubname = $1
            ORDER BY schemaname, tablename
        "#;
        let table_rows = client.query(tables_query, &[&name]).await?;
        let tables: Vec<String> = table_rows
            .iter()
            .map(|row| row.get::<_, String>("table_name"))
            .collect();

        publications.push(Publication {
            name,
            tables,
            all_tables,
            insert,
            update,
            delete,
            truncate,
        });
    }

    Ok(publications)
}

async fn _introspect_subscriptions<C: GenericClient>(client: &C) -> Result<Vec<Subscription>> {
    let query = r#"
        SELECT 
            s.subname AS name,
            s.subconninfo AS connection,
            s.subenabled AS enabled,
            s.subslotname AS slot_name
        FROM pg_subscription s
        WHERE s.subowner > 1
        AND NOT EXISTS (
            SELECT 1 FROM pg_depend d
            JOIN pg_extension e ON d.refobjid = e.oid
            WHERE d.objid = s.oid AND d.deptype = 'e'
        )
        ORDER BY s.subname
    "#;

    let rows = client.query(query, &[]).await?;
    let mut subscriptions = Vec::new();

    for row in rows {
        let name: String = row.get("name");
        let connection: String = row.get("connection");
        let enabled: bool = row.get("enabled");
        let slot_name: Option<String> = row.get("slot_name");

        // Get publications for this subscription
        let publications_query = r#"
            SELECT subpubname AS publication_name
            FROM pg_subscription_rel
            WHERE subname = $1
            ORDER BY subpubname
        "#;
        let pub_rows = client.query(publications_query, &[&name]).await?;
        let publications: Vec<String> = pub_rows
            .iter()
            .map(|row| row.get::<_, String>("publication_name"))
            .collect();

        subscriptions.push(Subscription {
            name,
            connection,
            publication: publications,
            enabled,
            slot_name,
        });
    }

    Ok(subscriptions)
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

async fn _introspect_foreign_data_wrappers<C: GenericClient>(
    client: &C,
) -> Result<Vec<ForeignDataWrapper>> {
    let query = r#"
        SELECT 
            f.fdwname AS name,
            p1.proname AS handler,
            p2.proname AS validator,
            f.fdwoptions AS options
        FROM pg_foreign_data_wrapper f
        LEFT JOIN pg_proc p1 ON f.fdwhandler = p1.oid
        LEFT JOIN pg_proc p2 ON f.fdwvalidator = p2.oid
        WHERE f.fdwowner > 1
        AND NOT EXISTS (
            SELECT 1 FROM pg_depend d
            JOIN pg_extension e ON d.refobjid = e.oid
            WHERE d.objid = f.oid AND d.deptype = 'e'
        )
        ORDER BY f.fdwname
    "#;

    let rows = client.query(query, &[]).await?;
    let mut fdws = Vec::new();

    for row in rows {
        let name: String = row.get("name");
        let handler: Option<String> = row.get("handler");
        let validator: Option<String> = row.get("validator");
        let options: Option<Vec<String>> = row.get("options");

        let options_map = options
            .as_deref()
            .map(parse_server_options)
            .unwrap_or_default();

        fdws.push(ForeignDataWrapper {
            name,
            handler,
            validator,
            options: options_map,
        });
    }

    Ok(fdws)
}

async fn introspect_columns_for_table<C: GenericClient>(
    client: &C,
    schema: &Option<String>,
    table: &str,
) -> Result<Vec<Column>> {
    let query = r#"
        SELECT 
            a.attname AS column_name,
            pg_catalog.format_type(a.atttypid, a.atttypmod) AS type_name,
            a.attnotnull AS is_not_null,
            a.atthasdef AS has_default,
            a.attcollation AS collation_oid,
            a.attstorage AS storage,
            a.attcompression AS compression,
            a.attidentity AS identity,
            a.attgenerated AS generated,
            col_description(a.attrelid, a.attnum) AS comment,
            a.attacl::text AS acl,
            a.attisdropped AS is_dropped,
            a.attislocal AS is_local,
            a.attstattarget AS stats_target,
            a.attfdwoptions AS fdw_options
        FROM pg_catalog.pg_attribute a
        JOIN pg_catalog.pg_class t ON a.attrelid = t.oid
        JOIN pg_catalog.pg_namespace n ON t.relnamespace = n.oid
        WHERE t.relname = $2
        AND n.nspname = $1
        AND a.attnum > 0
        AND NOT a.attisdropped
        ORDER BY a.attnum
    "#;

    let rows = client.query(query, &[schema, &table.to_string()]).await?;
    let mut columns = Vec::new();

    for row in rows {
        let name: String = row.get("column_name");
        let type_name: String = row.get("type_name");
        let is_not_null: bool = row.get("is_not_null");
        let has_default: bool = row.get("has_default");
        let collation_oid: Option<u32> = row.get("collation_oid");
        let storage_str: String = row.get("storage");
        let storage: char = storage_str.chars().next().unwrap_or('p');
        let compression: Option<String> = row.get("compression");
        let identity_str: Option<String> = row.get("identity");
        let identity: Option<char> = identity_str.map(|s| s.chars().next().unwrap_or('d'));
        let generated_str: Option<String> = row.get("generated");
        let generated: Option<char> = generated_str.map(|s| s.chars().next().unwrap_or('s'));
        let comment: Option<String> = row.get("comment");
        let acl: Option<String> = row.get("acl");
        let is_dropped: bool = row.get("is_dropped");
        let is_local: bool = row.get("is_local");
        let stats_target: Option<i32> = row.get("stats_target");
        let fdw_options: Option<Vec<String>> = row.get("fdw_options");

        // Convert storage char to ColumnStorage enum
        let column_storage = match storage {
            'p' => ColumnStorage::Plain,
            'e' => ColumnStorage::External,
            'x' => ColumnStorage::Extended,
            'm' => ColumnStorage::Main,
            _ => ColumnStorage::Plain,
        };

        // Convert identity char to Identity struct
        let identity_struct = identity.map(|c| Identity {
            generation: match c {
                'a' => IdentityGeneration::Always,
                'd' => IdentityGeneration::ByDefault,
                _ => IdentityGeneration::ByDefault,
            },
        });

        // Convert generated char to Generated struct
        let generated_struct = generated.map(|c| Generated {
            expression: "".to_string(), // TODO: Extract actual expression
        });

        // Get collation name if available
        let collation = if let Some(oid) = collation_oid {
            let collation_row = client
                .query_one(
                    "SELECT n.nspname || '.' || c.collname AS collation_name FROM pg_collation c JOIN pg_namespace n ON c.collnamespace = n.oid WHERE c.oid = $1",
                    &[&oid],
                )
                .await?;
            Some(collation_row.get::<_, String>("collation_name"))
        } else {
            None
        };

        columns.push(Column {
            name,
            type_name,
            is_not_null,
            has_default,
            collation,
            storage: column_storage,
            compression,
            identity: identity_struct,
            generated: generated_struct,
            comment,
            acl,
            is_dropped,
            is_local,
            stats_target,
            fdw_options: fdw_options.map(|opts| {
                opts.into_iter()
                    .filter_map(|opt| {
                        if let Some((key, value)) = opt.split_once('=') {
                            Some((key.to_string(), value.to_string()))
                        } else {
                            None
                        }
                    })
                    .collect()
            }).unwrap_or_default(),
        });
    }

    Ok(columns)
}

async fn _introspect_foreign_tables<C: GenericClient>(client: &C) -> Result<Vec<ForeignTable>> {
    let query = r#"
        SELECT 
            c.relname AS table_name,
            n.nspname AS schema_name,
            s.srvname AS server_name,
            c.reloptions AS options
        FROM pg_class c
        JOIN pg_namespace n ON c.relnamespace = n.oid
        JOIN pg_foreign_table ft ON c.oid = ft.ftrelid
        JOIN pg_foreign_server s ON ft.ftserver = s.oid
        WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
        AND c.relkind = 'f'
        AND c.relowner > 1
        AND NOT EXISTS (
            SELECT 1 FROM pg_depend d
            JOIN pg_extension e ON d.refobjid = e.oid
            WHERE d.objid = c.oid AND d.deptype = 'e'
        )
        ORDER BY n.nspname, c.relname
    "#;

    let rows = client.query(query, &[]).await?;
    let mut foreign_tables = Vec::new();

    for row in rows {
        let name: String = row.get("table_name");
        let schema: Option<String> = row.get("schema_name");
        let server: String = row.get("server_name");
        let options: Option<Vec<String>> = row.get("options");

        // Get columns for this foreign table
        let columns = introspect_columns_for_table(client, &schema, &name).await?;

        let options_map = options
            .as_deref()
            .map(parse_server_options)
            .unwrap_or_default();

        foreign_tables.push(ForeignTable {
            name,
            schema,
            columns,
            server,
            options: options_map,
        });
    }

    Ok(foreign_tables)
}

async fn introspect_foreign_key_constraints<C: GenericClient>(
    client: &C,
) -> Result<Vec<ForeignKeyConstraint>> {
    let query = r#"
        SELECT 
            c.conname AS constraint_name,
            t.relname AS table_name,
            n.nspname AS schema_name,
            rt.relname AS references_table,
            rn.nspname AS references_schema,
            c.confdeltype::text AS on_delete,
            c.confupdtype::text AS on_update,
            c.condeferrable AS deferrable,
            c.condeferred AS initially_deferred
        FROM pg_constraint c
        JOIN pg_class t ON c.conrelid = t.oid
        JOIN pg_namespace n ON t.relnamespace = n.oid
        JOIN pg_class rt ON c.confrelid = rt.oid
        JOIN pg_namespace rn ON rt.relnamespace = rn.oid
        WHERE c.contype = 'f'
        AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
        AND t.relowner > 1
        AND NOT EXISTS (
            SELECT 1 FROM pg_depend d
            JOIN pg_extension e ON d.refobjid = e.oid
            WHERE d.objid = t.oid AND d.deptype = 'e'
        )
        ORDER BY n.nspname, t.relname, c.conname
    "#;

    let rows = client.query(query, &[]).await?;
    let mut constraints = Vec::new();

    for row in rows {
        let name: String = row.get("constraint_name");
        let table: String = row.get("table_name");
        let schema: Option<String> = row.get("schema_name");
        let references_table: String = row.get("references_table");
        let references_schema: Option<String> = row.get("references_schema");
        let on_delete_code: String = row.get("on_delete");
        let on_update_code: String = row.get("on_update");
        let deferrable: bool = row.get("deferrable");
        let initially_deferred: bool = row.get("initially_deferred");

        // Get the columns for this constraint
        let columns_query = r#"
            SELECT array_agg(a.attname ORDER BY array_position(c.conkey, a.attnum)) AS column_names
            FROM pg_constraint c
            JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = ANY(c.conkey)
            WHERE c.conname = $1
        "#;
        let columns_row = client.query_one(columns_query, &[&name]).await?;
        let columns: Vec<String> = columns_row.get("column_names");

        // Get the referenced columns for this constraint
        let ref_columns_query = r#"
            SELECT array_agg(a.attname ORDER BY array_position(c.confkey, a.attnum)) AS references_columns
            FROM pg_constraint c
            JOIN pg_attribute a ON a.attrelid = c.confrelid AND a.attnum = ANY(c.confkey)
            WHERE c.conname = $1
        "#;
        let ref_columns_row = client.query_one(ref_columns_query, &[&name]).await?;
        let references_columns: Vec<String> = ref_columns_row.get("references_columns");

        // Convert action codes to ReferentialAction enum
        let on_delete = match on_delete_code.as_str() {
            "a" => Some(ReferentialAction::NoAction),
            "r" => Some(ReferentialAction::Restrict),
            "c" => Some(ReferentialAction::Cascade),
            "n" => Some(ReferentialAction::SetNull),
            "d" => Some(ReferentialAction::SetDefault),
            _ => None,
        };

        let on_update = match on_update_code.as_str() {
            "a" => Some(ReferentialAction::NoAction),
            "r" => Some(ReferentialAction::Restrict),
            "c" => Some(ReferentialAction::Cascade),
            "n" => Some(ReferentialAction::SetNull),
            "d" => Some(ReferentialAction::SetDefault),
            _ => None,
        };

        constraints.push(ForeignKeyConstraint {
            name,
            table,
            schema,
            columns,
            references_table,
            references_schema,
            references_columns,
            on_delete,
            on_update,
            deferrable,
            initially_deferred,
        });
    }

    Ok(constraints)
}

fn parse_function_parameters(arguments: &str) -> Vec<Parameter> {
    if arguments.is_empty() {
        return Vec::new();
    }

    let mut parameters = Vec::new();
    let parts: Vec<&str> = arguments.split(',').collect();

    for part in parts {
        let trimmed = part.trim();
        if trimmed.is_empty() {
            continue;
        }

        // Parse parameter mode and name
        let param_parts: Vec<&str> = trimmed.split_whitespace().collect();
        let mut mode = ParameterMode::In;
        let mut name = String::new();
        let mut type_name = String::new();

        if param_parts.is_empty() {
            continue;
        }

        // Check for parameter mode keywords
        let start_idx = match param_parts[0].to_uppercase().as_str() {
            "IN" => {
                mode = ParameterMode::In;
                1
            }
            "OUT" => {
                mode = ParameterMode::Out;
                1
            }
            "INOUT" => {
                mode = ParameterMode::InOut;
                1
            }
            "VARIADIC" => {
                mode = ParameterMode::Variadic;
                1
            }
            _ => 0,
        };

        if param_parts.len() > start_idx {
            if start_idx < param_parts.len() - 1 {
                // We have both name and type
                name = param_parts[start_idx].to_string();
                type_name = param_parts[start_idx + 1].to_string();
            } else {
                // Only type name (no parameter name)
                type_name = param_parts[start_idx].to_string();
            }
        }

        if !type_name.is_empty() {
            parameters.push(Parameter {
                name,
                type_name,
                mode: if start_idx == 0 {
                    ParameterMode::In
                } else {
                    mode
                }, // Only use explicit mode if keyword was found
                default: None,
            });
        }
    }

    parameters
}

fn parse_server_options(options: &[String]) -> std::collections::HashMap<String, String> {
    let mut options_map = std::collections::HashMap::new();

    for option in options {
        if let Some((key, value)) = option.split_once('=') {
            options_map.insert(key.to_string(), value.to_string());
        }
    }

    options_map
}

pub async fn introspect_types<C: GenericClient>(
    client: &C,
    include_predefined: bool,
) -> Result<Vec<Type>> {
    // 1. Check if datlastsysoid column exists in pg_database
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
        "datlastsysoid column exists (types): {}",
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

    tracing::debug!("Last system OID (types): {}", last_system_oid);

    // --- QUERY 1: Fetch all types from pg_type with all common and base-type properties ---
    let types_query = r#"
        SELECT
            t.oid, t.typname AS name, n.nspname AS schema_name, pg_get_userbyid(t.typowner) AS owner,
            t.typtype, t.typacl::text AS acl, obj_description(t.oid, 'pg_type') AS comment,
            t.typarray AS array_type_oid,
            -- Base type properties
            t.typlen AS internal_length, t.typbyval AS is_passed_by_value, t.typalign, t.typstorage,
            t.typcategory, t.typispreferred AS is_preferred, pg_get_expr(t.typdefaultbin, 0) AS default_value,
            t.typelem AS element_type_oid, t.typdelim AS delimiter, (t.typcollation <> 0) AS is_collatable,
            t.typinput::regproc::text AS input_fn, t.typoutput::regproc::text AS output_fn,
            t.typreceive::regproc::text AS receive_fn, t.typsend::regproc::text AS send_fn,
            t.typmodin::regproc::text AS typmod_in_fn, t.typmodout::regproc::text AS typmod_out_fn,
            t.typanalyze::regproc::text AS analyze_fn,
            -- Domain properties
            pg_catalog.format_type(t.typbasetype, t.typtypmod) AS base_type,
            t.typnotnull AS not_null,
            CASE WHEN t.typcollation <> bt.typcollation THEN t.typcollation ELSE 0 END AS collation_oid,
            -- Composite type properties
            t.typrelid as class_oid,
            -- Extension dependency
            EXISTS (
                SELECT 1 FROM pg_depend d
                WHERE d.objid = t.oid AND d.classid = 'pg_type'::regclass AND d.deptype = 'e'
            ) AS is_from_extension
        FROM pg_type t
        JOIN pg_namespace n ON t.typnamespace = n.oid
        LEFT JOIN pg_type bt ON t.typbasetype = bt.oid;
    "#;
    let type_rows = client.query(types_query, &[]).await?;

    // --- QUERY 2: Fetch all composite type attributes at once ---
    let attributes_query = r#"
        SELECT
            a.attrelid AS class_oid,
            a.attname AS name,
            pg_catalog.format_type(a.atttypid, a.atttypmod) AS type_name,
            CASE WHEN a.attcollation <> t.typcollation THEN a.attcollation ELSE 0 END AS collation_oid
        FROM pg_attribute a
        JOIN pg_type t ON a.atttypid = t.oid
        WHERE a.attnum > 0 AND NOT a.attisdropped
          AND EXISTS (SELECT 1 FROM pg_class c WHERE c.oid = a.attrelid AND c.relkind = 'c')
        ORDER BY a.attrelid, a.attnum;
    "#;
    let attribute_rows = client.query(attributes_query, &[]).await?;

    // --- QUERY 3: Fetch all enum labels at once ---
    let enums_query =
        "SELECT oid, enumtypid, enumlabel FROM pg_enum ORDER BY enumtypid, enumsortorder";
    let enum_rows = client.query(enums_query, &[]).await?;

    // --- QUERY 4: Fetch all range type details at once ---
    let ranges_query = r#"
        SELECT
            r.rngtypid AS oid,
            pg_catalog.format_type(r.rngsubtype, NULL) AS subtype,
            r.rngsubopc AS subtype_opclass_oid,
            r.rngcollation AS collation_oid,
            r.rngcanonical::regproc::text AS canonical_fn,
            r.rngsubdiff::regproc::text AS subtype_diff_fn,
            r.rngmultitypid as multirange_type_oid
        FROM pg_range r;
    "#;
    let range_rows = client.query(ranges_query, &[]).await?;

    // --- QUERY 5: Fetch all domain constraints ---
    let domain_constraints_query = "SELECT oid, conname, contypid, pg_get_constraintdef(oid) AS definition, NOT convalidated AS not_valid FROM pg_constraint WHERE contypid != 0 AND contype = 'c'";
    let domain_constraint_rows = client.query(domain_constraints_query, &[]).await?;

    // --- Helper Data: Fetch collations and opclasses for name resolution ---
    let collations_map =
        get_qualified_name_map(client, "pg_collation", "collname", "collnamespace").await?;
    let opclasses_map =
        get_qualified_name_map(client, "pg_opclass", "opcname", "opcnamespace").await?;

    // --- Process and Assemble in Rust ---
    let mut types_map: HashMap<u32, Type> = HashMap::new();

    for row in type_rows {
        let oid: u32 = row.get("oid");
        let name: String = row.get("name");

        let info = TypeInfo {
            oid,
            name: name.clone(),
            schema: row.get("schema_name"),
            owner: row.get("owner"),
            acl: row.get("acl"),
            comment: row.get("comment"),
            is_user_defined: oid > last_system_oid,
            is_from_extension: row.get("is_from_extension"),
            array_type_oid: row.get::<_, u32>("array_type_oid").checked_sub(0),
        };

        let type_char: i8 = row.get("typtype");
        let new_type = match type_char as u8 as char {
            'b' => {
                let receive_fn: Option<String> = row.get("receive_fn");
                let send_fn: Option<String> = row.get("send_fn");
                Type::Base(BaseType {
                    info,
                    internal_length: row.get("internal_length"),
                    is_passed_by_value: row.get("is_passed_by_value"),
                    alignment: row.get::<_, i8>("typalign") as u8 as char,
                    storage: row.get::<_, i8>("typstorage") as u8 as char,
                    category: row.get::<_, i8>("typcategory") as u8 as char,
                    is_preferred: row.get("is_preferred"),
                    default_value: row.get("default_value"),
                    element_type_oid: row.get::<_, u32>("element_type_oid").checked_sub(0),
                    delimiter: row.get::<_, i8>("delimiter") as u8 as char,
                    is_collatable: row.get("is_collatable"),
                    input_fn: row.get("input_fn"),
                    output_fn: row.get("output_fn"),
                    receive_fn: if receive_fn.as_deref() == Some("-") {
                        None
                    } else {
                        receive_fn
                    },
                    send_fn: if send_fn.as_deref() == Some("-") {
                        None
                    } else {
                        send_fn
                    },
                    typmod_in_fn: row.get("typmod_in_fn"),
                    typmod_out_fn: row.get("typmod_out_fn"),
                    analyze_fn: row.get("analyze_fn"),
                })
            }
            'c' => Type::Composite(CompositeType {
                info,
                attributes: Vec::new(), // Will be filled in below
                class_oid: row.get("class_oid"),
            }),
            'd' => {
                let collation_oid: u32 = row.get("collation_oid");
                Type::Domain(Domain {
                    info,
                    base_type: row.get("base_type"),
                    collation: if collation_oid > 0 {
                        // For built-in collations (pg_catalog schema), return just the name
                        // For user-defined collations, return the fully qualified name
                        if let Some(qualified_name) = collations_map.get(&collation_oid) {
                            if qualified_name.starts_with("\"pg_catalog\".") {
                                // Extract just the collation name from "pg_catalog"."name"
                                let name_part = qualified_name.split('.').nth(1);
                                name_part.map(|s| s.to_string())
                            } else {
                                Some(qualified_name.clone())
                            }
                        } else {
                            None
                        }
                    } else {
                        None
                    },
                    not_null: row.get("not_null"),
                    default: row.get("default_value"),
                    constraints: Vec::new(), // Will be filled in below
                })
            }
            'e' => Type::Enum(EnumType {
                info,
                values: Vec::new(), // Will be filled in below
            }),
            'r' => Type::Range(RangeType {
                info,
                subtype: String::new(), // Will be filled in below
                subtype_opclass: String::new(),
                collation: None,
                canonical_fn: None,
                subtype_diff_fn: None,
                multirange_type_oid: None,
            }),
            'p' => Type::Pseudo(PseudoType { info }),
            _ => continue, // Ignore other types like internal array types handled by `typarray`
        };
        types_map.insert(oid, new_type);
    }

    // Populate composite attributes
    for row in attribute_rows {
        let class_oid: u32 = row.get("class_oid");
        if let Some(Type::Composite(c)) = types_map.values_mut().find(|t| match t {
            Type::Composite(c) => c.class_oid == class_oid,
            _ => false,
        }) {
            let collation_oid: u32 = row.get("collation_oid");
            c.attributes.push(Attribute {
                name: row.get("name"),
                type_name: row.get("type_name"),
                collation: if collation_oid > 0 {
                    collations_map.get(&collation_oid).cloned()
                } else {
                    None
                },
            });
        }
    }

    // Populate enum values
    for row in enum_rows {
        let type_oid: u32 = row.get("enumtypid");
        if let Some(Type::Enum(e)) = types_map.get_mut(&type_oid) {
            e.values.push(EnumValue {
                oid: row.get("oid"),
                label: row.get("enumlabel"),
            });
        }
    }

    // Populate range details
    for row in range_rows {
        let oid: u32 = row.get("oid");
        if let Some(Type::Range(r)) = types_map.get_mut(&oid) {
            let subtype_opclass_oid: u32 = row.get("subtype_opclass_oid");
            let collation_oid: u32 = row.get("collation_oid");
            let canonical_fn: Option<String> = row.get("canonical_fn");
            let subtype_diff_fn: Option<String> = row.get("subtype_diff_fn");

            r.subtype = row.get("subtype");
            r.subtype_opclass = opclasses_map
                .get(&subtype_opclass_oid)
                .cloned()
                .unwrap_or_default();
            r.collation = if collation_oid > 0 {
                collations_map.get(&collation_oid).cloned()
            } else {
                None
            };
            r.canonical_fn = if canonical_fn.as_deref() == Some("-") {
                None
            } else {
                canonical_fn
            };
            r.subtype_diff_fn = if subtype_diff_fn.as_deref() == Some("-") {
                None
            } else {
                subtype_diff_fn
            };
            r.multirange_type_oid = row.get::<_, u32>("multirange_type_oid").checked_sub(0);
        }
    }

    // Populate domain constraints
    for row in domain_constraint_rows {
        let domain_oid: u32 = row.get("contypid");
        if let Some(Type::Domain(d)) = types_map.get_mut(&domain_oid) {
            d.constraints.push(DomainConstraint {
                oid: row.get("oid"),
                name: row.get("conname"),
                definition: row.get("definition"),
                not_valid: row.get("not_valid"),
            });
        }
    }

    // --- Final Filtering ---
    let all_types: Vec<Type> = types_map.into_values().collect();
    if include_predefined {
        Ok(all_types)
    } else {
        let dumpable_types = all_types
            .into_iter()
            .filter(|t| match t {
                Type::Base(t) => {
                    t.info.is_user_defined
                        && !t.info.is_from_extension
                        && t.element_type_oid.is_none()
                }
                Type::Composite(t) => t.info.is_user_defined && !t.info.is_from_extension,
                Type::Domain(t) => t.info.is_user_defined && !t.info.is_from_extension,
                Type::Enum(t) => t.info.is_user_defined && !t.info.is_from_extension,
                Type::Range(t) => t.info.is_user_defined && !t.info.is_from_extension,
                Type::Pseudo(_) => false, // Never dump pseudo-types
            })
            .collect();
        Ok(dumpable_types)
    }
}

fn parse_rule_definition(definition: &str) -> (Option<String>, String) {
    // Parse rule definition like:
    // "CREATE RULE rule_name AS ON event TO table WHERE condition DO action"
    // or "CREATE RULE rule_name AS ON event TO table DO action"

    let mut condition = None;
    let mut action = definition.to_string();

    // Look for WHERE clause
    if let Some(where_pos) = definition.find(" WHERE ") {
        if let Some(do_pos) = definition.find(" DO ") {
            if do_pos > where_pos {
                // Extract condition between WHERE and DO
                let condition_start = where_pos + 7; // " WHERE " is 7 chars
                let condition_text = definition[condition_start..do_pos].trim();
                condition = Some(condition_text.to_string());

                // Extract action after DO
                action = definition[do_pos + 4..].trim().to_string(); // " DO " is 4 chars
            }
        }
    } else if let Some(do_pos) = definition.find(" DO ") {
        // No WHERE clause, just extract action after DO
        action = definition[do_pos + 4..].trim().to_string();
    }

    (condition, action)
}

fn extract_partition_columns(partition_expression: &str) -> Vec<String> {
    // Parse partition expression like "RANGE (created_date)" or "LIST (region, country)"
    // Extract column names from within parentheses
    if let Some(start) = partition_expression.find('(') {
        if let Some(end) = partition_expression.rfind(')') {
            let columns_str = &partition_expression[start + 1..end];
            columns_str
                .split(',')
                .map(|col| col.trim().to_string())
                .filter(|col| !col.is_empty())
                .collect()
        } else {
            Vec::new()
        }
    } else {
        Vec::new()
    }
}
