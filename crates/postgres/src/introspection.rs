use crate::get_qualified_name_map;
use crate::parse_options;
use crate::quote_ident;
use parser::pg_options_to_map;
use shem_core::Result;
use shem_core::schema::*;
use std::collections::HashMap;
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
    // Introspect views
    // Purpose: Virtual table from a query.
    // Introspect materialized views
    // Constraint
    // 1. Make a single call to the unified function to get ALL relations.
    let all_relations: Vec<Relation> = introspect_relations_unified(client).await?;

    // 2. Iterate over the results and use a `match` to sort them into the correct HashMaps.
    for relation in all_relations {
        match relation {
            Relation::Table(table) => {
                let key = if table.schema == "public" {
                    table.name.clone()
                } else {
                    format!("{}.{}", table.schema, table.name)
                };
                debug!("Found Table: {:?}, using key: {}", table, key);
                schema.tables.insert(key, table);
            }
            Relation::View(view) => {
                let key = if view.schema == "public" {
                    view.name.clone()
                } else {
                    format!("{}.{}", view.schema, view.name)
                };
                debug!("Found View: {:?}, using key: {}", view, key);
                schema.views.insert(key, view);
            }
            Relation::MaterializedView(matview) => {
                let key = if matview.schema == "public" {
                    matview.name.clone()
                } else {
                    format!("{}.{}", matview.schema, matview.name)
                };
                debug!("Found Materialized View: {:?}, using key: {}", matview, key);
                schema.materialized_views.insert(key, matview);
            }
            Relation::ForeignTable(ftable) => {
                let key = format!(
                    "{}.{}",
                    ftable.schema.as_deref().unwrap_or("public"),
                    ftable.name
                );
                debug!("Found Foreign Table: {:?}, using key: {}", ftable, key);
                schema.foreign_tables.insert(key, ftable);
            }
        }
    }

    // Introspect policies
    let policies = introspect_policies(&*client, false).await?;
    for policy in policies {
        // 1. Determine the correct, unique key for the HashMap.
        let key = if let Some(policy_name) = &policy.name {
            // This is a regular policy: "schema.table.policy"
            format!("{}.{}.{}", policy.schema, policy.table_name, policy_name)
        } else {
            // This is the special 'ENABLE ROW LEVEL SECURITY' object.
            // We create a synthetic, unique key for it.
            format!("{}.{}.__ENABLE_RLS__", policy.schema, policy.table_name)
        };

        debug!("Policy found: {:?}, using key: {}", policy, key);

        // 2. Insert into the HashMap using the new unique key.
        schema.policies.insert(key, policy);
    }

    // Introspect rules
    let rules = introspect_rules(&*client, false).await?;
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

    // Introspect publication tables
    let publication_tables = introspect_publication_tables(&*client).await?;
    for table in publication_tables {
        let key = format!("{}.{}", table.table_schema, table.table_name);
        schema.publication_tables.insert(key, table);
    }

    // Introspect routines
    let routines = introspect_routines(&*client).await?;
    for routine in routines {
        let (name, schema_name) = match &routine {
            Routine::Function(func) => (&func.name, &func.schema),
            Routine::Procedure(proc) => (&proc.name, &proc.schema),
            Routine::Aggregate(agg) => (&agg.name, &agg.schema),
        };

        let key = if schema_name == "public" {
            name.clone()
        } else {
            format!("{}.{}", schema_name, name)
        };
        schema.routines.insert(key, routine);
    }

    // Introspect triggers
    // Get the OIDs of all tables that can have triggers
    let table_oids: Vec<u32> = schema.tables.values().map(|t| t.oid).collect();
    let triggers_map = introspect_triggers(client, &table_oids).await?;

    // Distribute the fetched triggers into their parent Table objects and schema-level triggers.
    tracing::debug!("Triggers map: {:?}", triggers_map);
    for (_, table) in schema.tables.iter_mut() {
        tracing::debug!("Checking table {} (OID: {}) for triggers", table.name, table.oid);
        if let Some(triggers_for_this_table) = triggers_map.get(&table.oid) {
            tracing::debug!("Found {} triggers for table {}", triggers_for_this_table.len(), table.name);
            // Clone the triggers into the table's `triggers` field.
            table.triggers = triggers_for_this_table.clone();
            
            // Also add triggers to the schema-level triggers HashMap
            for trigger in triggers_for_this_table {
                schema.triggers.insert(trigger.name.clone(), trigger.clone());
            }
        } else {
            tracing::debug!("No triggers found for table {}", table.name);
        }
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
            query.push_str(
                ", a.attgenerated, pg_get_expr(a.attgeneratedbin, a.attrelid) as generated_expr",
            );
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
                expression: generated_expr.unwrap_or_else(|| "(generated expression)".to_string()),
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

pub async fn introspect_all_constraints<C: GenericClient>(
    client: &C,
    table_oids: &[u32],
) -> Result<HashMap<u32, Vec<Constraint>>> {
    if table_oids.is_empty() {
        return Ok(HashMap::new());
    }

    // This query is now safe, more complete, and avoids parsing.
    let query = r#"
        SELECT
            c.oid,
            c.conrelid AS table_oid,
            c.conname AS name,
            pg_get_constraintdef(c.oid) AS definition,
            c.contype,
            c.confrelid AS foreign_table_oid,
            -- Get local column names in constraint order
            (SELECT array_agg(a.attname ORDER BY u.ord)
             FROM unnest(c.conkey) WITH ORDINALITY u(attnum, ord)
             JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = u.attnum)
            AS foreign_key_columns,
            -- Get foreign column names in constraint order
            (SELECT array_agg(a.attname ORDER BY u.ord)
             FROM unnest(c.confkey) WITH ORDINALITY u(attnum, ord)
             JOIN pg_attribute a ON a.attrelid = c.confrelid AND a.attnum = u.attnum)
            AS primary_key_columns,
            c.confupdtype AS on_update,
            c.confdeltype AS on_delete,
            c.condeferrable AS is_deferrable,
            c.condeferred AS is_initially_deferred,
            NOT c.convalidated AS is_not_valid
        FROM pg_constraint c
        WHERE c.conrelid = ANY($1) 
          AND c.conparentid = 0 -- Exclude partition constraints that are not dumpable
        ORDER BY c.conrelid, c.conname;
    "#;

    // Use query parameterization to prevent SQL injection
    let rows = client.query(query, &[&table_oids]).await?;

    let mut map: HashMap<u32, Vec<Constraint>> = HashMap::new();
    for row in rows {
        let table_oid: u32 = row.get("table_oid");
        let contype_char: i8 = row.get("contype");
        let on_update_char: i8 = row.get("on_update");
        let on_delete_char: i8 = row.get("on_delete");

        map.entry(table_oid).or_default().push(Constraint {
            oid: row.get("oid"),
            name: row.get("name"),
            table_oid,
            definition: row.get("definition"),
            r#type: parse_constraint_type(contype_char),
            foreign_table_oid: row.get("foreign_table_oid"),
            foreign_key_columns: row.get::<&str, Option<Vec<String>>>("foreign_key_columns").unwrap_or_default(),
            primary_key_columns: row
                .get::<&str, Option<Vec<String>>>("primary_key_columns")
                .unwrap_or_default(),
            on_update: parse_ref_action(on_update_char),
            on_delete: parse_ref_action(on_delete_char),
            is_deferrable: row.get("is_deferrable"),
            is_initially_deferred: row.get("is_initially_deferred"),
            is_not_valid: row.get("is_not_valid"),
        });
    }
    Ok(map)
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
        let _definition: String = row.get("index_definition");
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
                storage_parameters: storage_parameters
                    .map(|params| {
                        params
                            .into_iter()
                            .filter_map(|param| {
                                if let Some((key, value)) = param.split_once('=') {
                                    Some((key.to_string(), value.to_string()))
                                } else {
                                    None
                                }
                            })
                            .collect()
                    })
                    .unwrap_or_default(),
                table_oid: Some(table_oid),
                table_name: None, // Will be populated when assigned to table
                schema: None,     // Will be populated when assigned to table
            };
            current_index = Some(index);
            indexes_map
                .entry(table_oid)
                .or_insert_with(Vec::new)
                .push(current_index.as_ref().unwrap().clone());
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
        inheritance_map
            .entry(table_oid)
            .or_insert_with(Vec::new)
            .push(parent_table);
    }

    Ok(inheritance_map)
}

async fn introspect_all_partition_keys<C: GenericClient>(
    client: &C,
) -> Result<HashMap<u32, String>> {
    // First, let's see what columns are actually available in pg_partitioned_table
    let columns_query = r#"
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_schema = 'pg_catalog' 
        AND table_name = 'pg_partitioned_table'
        ORDER BY ordinal_position;
    "#;
    let columns = client.query(columns_query, &[]).await?;
    tracing::debug!(
        "pg_partitioned_table columns: {:?}",
        columns
            .iter()
            .map(|row| {
                let name: String = row.get("column_name");
                let data_type: String = row.get("data_type");
                format!("{}: {}", name, data_type)
            })
            .collect::<Vec<_>>()
    );

    // Check if partattrs column exists in pg_partitioned_table (PostgreSQL 10+)
    let partattrs_column_exists_query = r#"
        SELECT EXISTS (
            SELECT 1 FROM information_schema.columns 
            WHERE table_schema = 'pg_catalog' 
            AND table_name = 'pg_partitioned_table' 
            AND column_name = 'partattrs'
        ) as column_exists;
    "#;
    let partattrs_column_exists_row = client.query_one(partattrs_column_exists_query, &[]).await?;
    let partattrs_exists: bool = partattrs_column_exists_row.get("column_exists");

    tracing::debug!("partattrs column exists: {}", partattrs_exists);

    if !partattrs_exists {
        // Return empty map for older PostgreSQL versions that don't support partitioning
        tracing::debug!("partattrs column does not exist, returning empty map");
        return Ok(HashMap::new());
    }

    let query = r#"
        SELECT 
            c.oid as table_oid,
            pt.partstrat::text as partition_strategy,
            array_agg(a.attname ORDER BY array_position(pt.partattrs, a.attnum)) as column_names
        FROM pg_class c
        JOIN pg_partitioned_table pt ON pt.partrelid = c.oid
        JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY(pt.partattrs)
        JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE c.relkind = 'p'
        AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
        GROUP BY c.oid, pt.partstrat
        ORDER BY c.oid
    "#;

    // First, let's check if there are any partitioned tables at all
    let check_query = "SELECT oid, relname FROM pg_class WHERE relkind = 'p'";
    let check_rows = client.query(check_query, &[]).await?;
    tracing::debug!(
        "Found {} partitioned tables: {:?}",
        check_rows.len(),
        check_rows
            .iter()
            .map(|row| {
                let oid: u32 = row.get("oid");
                let name: String = row.get("relname");
                format!("{}: {}", oid, name)
            })
            .collect::<Vec<_>>()
    );

    tracing::debug!("Executing partition key query: {}", query);
    let rows = client.query(query, &[]).await?;
    let mut partition_key_map = HashMap::new();

    tracing::debug!("Found {} partition key rows", rows.len());

    for row in rows {
        let table_oid: u32 = row.get("table_oid");
        let partition_strategy: String = row.get("partition_strategy");
        let column_names: Vec<String> = row.get("column_names");

        tracing::debug!(
            "Partition key for table {}: strategy={}, columns={:?}",
            table_oid,
            partition_strategy,
            column_names
        );

        let strategy_str = match partition_strategy.as_str() {
            "r" => "RANGE",
            "l" => "LIST",
            "h" => "HASH",
            _ => "UNKNOWN",
        };

        let partition_expression = format!("{} ({})", strategy_str, column_names.join(", "));
        partition_key_map.insert(table_oid, partition_expression);
    }

    tracing::debug!("Final partition key map: {:?}", partition_key_map);

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
                triggers: Vec::new(), // Will be populated later
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

        // This function is creating a different Constraint struct than expected
        // We need to create a proper Constraint with all required fields
        constraints.push(Constraint {
            oid: 0, // TODO: Get actual OID
            name,
            table_oid: 0, // TODO: Get actual table OID
            definition,
            r#type: match kind {
                ConstraintKind::PrimaryKey => ConstraintType::PrimaryKey,
                ConstraintKind::ForeignKey { .. } => ConstraintType::ForeignKey,
                ConstraintKind::Unique => ConstraintType::Unique,
                ConstraintKind::Check => ConstraintType::Check,
                ConstraintKind::Exclusion => ConstraintType::Exclusion,
                ConstraintKind::NotNull => ConstraintType::Check, // NotNull maps to Check
            },
            foreign_table_oid: None,
            foreign_key_columns: Vec::new(),
            primary_key_columns: Vec::new(),
            on_update: ReferentialAction::NoAction,
            on_delete: ReferentialAction::NoAction,
            is_deferrable: deferrable,
            is_initially_deferred: initially_deferred,
            is_not_valid: false,
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
                table_oid: None, // Will be populated when assigned to table
                table_name: None, // Will be populated when assigned to table
                schema: None,     // Will be populated when assigned to table
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

// This master function fetches all relations and then filters for views.
pub async fn introspect_views_unified<C: GenericClient>(client: &C) -> Result<Vec<View>> {
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
        "datlastsysoid column exists (views): {}",
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

    tracing::debug!("Last system OID (views): {}", last_system_oid);

    // --- QUERY 1: Fetch ALL relations, same as before ---
    // We add a few view-specific fields.
    let relations_query = r#"
        SELECT
            c.oid, c.relname AS name, n.nspname AS schema_name, pg_get_userbyid(c.relowner) AS owner,
            c.relkind, obj_description(c.oid, 'pg_class') AS comment,
            c.relacl::text AS acl,
            c.reloptions AS options,
            -- View-specific properties
            CASE
                WHEN 'check_option=local' = ANY(c.reloptions) THEN 'LOCAL'
                WHEN 'check_option=cascaded' = ANY(c.reloptions) THEN 'CASCADED'
                ELSE 'NONE'
            END AS check_option,
            pg_get_viewdef(c.oid) AS definition,
            EXISTS (
                SELECT 1 FROM pg_depend d
                WHERE d.objid = c.oid AND d.classid = 'pg_class'::regclass AND d.deptype = 'e'
            ) AS is_from_extension
        FROM pg_class c
        JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE c.relkind = 'v'; -- IMPORTANT: Filter for only views ('v')
    "#;
    let relation_rows = client.query(relations_query, &[]).await?;

    let all_view_oids: Vec<u32> = relation_rows.iter().map(|row| row.get("oid")).collect();

    // --- BULK QUERY for columns of these views ---
    let columns_map = introspect_all_columns_for_relations(client, &all_view_oids).await?;

    // --- Assemble the final Vec<View> ---
    let mut views = Vec::new();
    for row in relation_rows {
        let oid: u32 = row.get("oid");

        let is_user_defined = oid > last_system_oid;
        let is_from_extension: bool = row.get("is_from_extension");

        // Filter for dumpable views
        if is_user_defined && !is_from_extension {
            let check_option_str: &str = row.get("check_option");

            views.push(View {
                oid,
                name: row.get("name"),
                schema: row.get("schema_name"),
                owner: row.get("owner"),
                definition: row.get("definition"),
                comment: row.get("comment"),
                acl: row.get("acl"),
                columns: columns_map.get(&oid).cloned().unwrap_or_default(),
                check_option: match check_option_str {
                    "LOCAL" => CheckOption::Local,
                    "CASCADED" => CheckOption::Cascaded,
                    _ => CheckOption::None,
                },
                options: pg_options_to_map(row.get("options")),
                is_user_defined,
                is_from_extension,
            });
        }
    }

    Ok(views)
}

// --- Helper Functions ---
// We can reuse the column introspector, slightly generalized
async fn introspect_all_columns_for_relations<C: GenericClient>(
    client: &C,
    relation_oids: &[u32],
) -> Result<HashMap<u32, Vec<Column>>> {
    if relation_oids.is_empty() {
        return Ok(HashMap::new());
    }
    // This query works for both tables and views
    let query = "SELECT attrelid, attname, pg_catalog.format_type(atttypid, atttypmod) as type_name FROM pg_attribute WHERE attrelid = ANY($1) AND attnum > 0 AND NOT attisdropped ORDER BY attrelid, attnum";
    let rows = client.query(query, &[&relation_oids]).await?;

    let mut map: HashMap<u32, Vec<Column>> = HashMap::new();
    for row in rows {
        let table_oid: u32 = row.get("attrelid");
        // For views, many column properties aren't applicable, so we create a simplified Column
        map.entry(table_oid).or_default().push(Column {
            name: row.get("attname"),
            type_name: row.get("type_name"),
            // Fill with default/None for fields that don't apply to views
            is_not_null: false,
            has_default: false,
            collation: None,
            storage: ColumnStorage::Plain, // Doesn't matter for views
            compression: None,
            identity: None,
            generated: None,
            comment: None, // Can be fetched with another bulk query if needed
            acl: None,
            is_dropped: false,
            is_local: true,
            stats_target: None,
            fdw_options: HashMap::new(),
        });
    }
    Ok(map)
}

pub async fn introspect_materialized_views_unified<C: GenericClient>(
    client: &C,
) -> Result<Vec<MaterializedView>> {
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
        "datlastsysoid column exists (materialized views): {}",
        datlastsysoid_exists
    );

    let last_system_oid: u32 = if datlastsysoid_exists {
        let last_system_oid_row = client
            .query_one(
                "SELECT datlastsysoid FROM pg_database WHERE datname = current_database()",
                &[],
            )
            .await?;
        last_system_oid_row.get("datlastsysoid")
    } else {
        16384 // Default OID for older PostgreSQL versions
    };

    // --- QUERY 1: Fetch ALL relations, adding matview-specific fields ---
    let relations_query = r#"
        SELECT
            c.oid, c.relname AS name, n.nspname AS schema_name, pg_get_userbyid(c.relowner) AS owner,
            c.relkind, obj_description(c.oid, 'pg_class') AS comment,
            ts.spcname AS tablespace, c.relacl::text AS acl,
            c.reloptions AS options,
            -- Materialized View-specific properties
            c.relispopulated AS is_populated,
            pg_get_viewdef(c.oid) AS definition,
            EXISTS (
                SELECT 1 FROM pg_depend d
                WHERE d.objid = c.oid AND d.classid = 'pg_class'::regclass AND d.deptype = 'e'
            ) AS is_from_extension
        FROM pg_class c
        JOIN pg_namespace n ON c.relnamespace = n.oid
        LEFT JOIN pg_tablespace ts ON c.reltablespace = ts.oid
        WHERE c.relkind = 'm'; -- IMPORTANT: Filter for only materialized views ('m')
    "#;
    let relation_rows = client.query(relations_query, &[]).await?;

    let all_matview_oids: Vec<u32> = relation_rows.iter().map(|row| row.get("oid")).collect();

    // --- BULK QUERIES for sub-objects of these matviews ---
    let columns_map = introspect_all_columns_for_relations(client, &all_matview_oids).await?;
    let indexes_map = introspect_all_indexes(client, &all_matview_oids).await?;

    // --- Assemble the final Vec<MaterializedView> ---
    let mut matviews = Vec::new();
    for row in relation_rows {
        let oid: u32 = row.get("oid");

        let is_user_defined = oid > last_system_oid;
        let is_from_extension: bool = row.get("is_from_extension");

        // Filter for dumpable materialized views
        if is_user_defined && !is_from_extension {
            matviews.push(MaterializedView {
                oid,
                name: row.get("name"),
                schema: row.get("schema_name"),
                owner: row.get("owner"),
                definition: row.get("definition"),
                comment: row.get("comment"),
                acl: row.get("acl"),
                is_populated: row.get("is_populated"),
                columns: columns_map.get(&oid).cloned().unwrap_or_default(),
                indexes: indexes_map.get(&oid).cloned().unwrap_or_default(),
                options: pg_options_to_map(row.get("options")),
                tablespace: row.get("tablespace"),
                is_user_defined,
                is_from_extension,
            });
        }
    }

    Ok(matviews)
}

// This function now fetches all dumpable functions, procedures, and aggregates efficiently.
pub async fn introspect_routines<C: GenericClient>(client: &C) -> Result<Vec<Routine>> {
    // 1. Get system OID threshold for reliable filtering
    // Use a more compatible approach that works across PostgreSQL versions
    let last_system_oid: u32 = {
        // Check if datlastsysoid column exists in pg_database
        let column_exists_row = client
            .query_one(
                "SELECT EXISTS (
                    SELECT 1 FROM information_schema.columns 
                    WHERE table_schema = 'pg_catalog' 
                    AND table_name = 'pg_database' 
                    AND column_name = 'datlastsysoid'
                ) as column_exists",
                &[],
            )
            .await?;

        let datlastsysoid_exists: bool = column_exists_row.get("column_exists");

        if datlastsysoid_exists {
            let last_system_oid_row = client
                .query_one(
                    "SELECT datlastsysoid FROM pg_database WHERE datname = current_database()",
                    &[],
                )
                .await?;
            last_system_oid_row.get("datlastsysoid")
        } else {
            // Fallback for older PostgreSQL versions: use a reasonable default
            // This is the OID of the last system object in older versions
            16384
        }
    };

    // 2. The main query to fetch all routines from pg_proc.
    // It filters out implicitly-created routines and system routines.
    let routines_query = r#"
        SELECT
            p.oid,
            p.proname AS name,
            n.nspname AS schema_name,
            pg_get_userbyid(p.proowner) AS owner,
            p.prokind,
            -- For Functions/Procedures, this is the complete definition.
            -- For Aggregates, this is a starting point.
            pg_get_functiondef(p.oid) AS definition,
            -- This is the key for uniquely identifying any routine.
            pg_get_function_identity_arguments(p.oid) AS identity_arguments,
            p.proacl::text AS acl,
            obj_description(p.oid, 'pg_proc') AS comment,
            p.proparallel AS parallel_safety,
            p.procost AS cost,
            p.prorows AS rows,
            EXISTS (
                SELECT 1 FROM pg_depend d
                WHERE d.objid = p.oid AND d.classid = 'pg_proc'::regclass AND d.deptype = 'e'
            ) AS is_from_extension
        FROM pg_proc p
        JOIN pg_namespace n ON p.pronamespace = n.oid
        WHERE
            -- Exclude routines created implicitly by other objects (e.g., type I/O funcs)
            NOT EXISTS (
                SELECT 1 FROM pg_depend d
                WHERE d.objid = p.oid AND d.classid = 'pg_proc'::regclass AND d.deptype = 'i'
            )
            -- Only include user-defined routines (ignore built-ins)
            AND p.oid > $1;
    "#;

    let routine_rows = client.query(routines_query, &[&last_system_oid]).await?;

    // --- QUERY 2 (for Aggregates only): Fetch aggregate-specific details ---
    let aggregates_query = r#"
        SELECT
            a.aggfnoid AS oid,
            -- pg_get_aggregate_def is not a standard function, so we build it manually
            -- This is a simplified version of what pg_dump does.
            'SFUNC = ' || a.aggtransfn::regproc::text ||
            ', STYPE = ' || a.aggtranstype::regtype::text ||
            COALESCE(', FINALFUNC = ' || a.aggfinalfn::regproc::text, '') ||
            COALESCE(', INITCOND = ' || quote_literal(a.agginitval), '') ||
            COALESCE(', SORTOP = ' || op.oprname, '')
            AS aggregate_details
        FROM pg_aggregate a
        LEFT JOIN pg_operator op ON op.oid = a.aggsortop
        WHERE a.aggfnoid = ANY($1);
    "#;

    let aggregate_oids: Vec<u32> = routine_rows
        .iter()
        .filter(|row| row.get::<_, i8>("prokind") as u8 as char == 'a')
        .map(|row| row.get("oid"))
        .collect();

    let aggregate_detail_rows = client.query(aggregates_query, &[&aggregate_oids]).await?;
    let aggregate_details_map: HashMap<u32, String> = aggregate_detail_rows
        .into_iter()
        .map(|row| (row.get("oid"), row.get("aggregate_details")))
        .collect();

    // --- Assemble final Vec<Routine> ---
    let mut routines = Vec::new();
    for row in routine_rows {
        let oid: u32 = row.get("oid");
        let is_from_extension: bool = row.get("is_from_extension");

        // Skip extension members if we only want dumpable objects
        if is_from_extension {
            continue;
        }

        let prokind_char: i8 = row.get("prokind");
        let routine_option = match prokind_char as u8 as char {
            'f' | 'w' => {
                // Functions and Window Functions
                let mut definition: String = row.get("definition");
                let parallel_safety: i8 = row.get("parallel_safety");
                let cost: f32 = row.get("cost");
                let rows: f32 = row.get("rows");

                // Add parallel safety information to the definition if it's not UNSAFE (default)
                if parallel_safety != 0 {
                    // 0 = UNSAFE (default), 1 = RESTRICTED, 2 = SAFE
                    let parallel_clause = match parallel_safety {
                        1 => " PARALLEL RESTRICTED",
                        2 => " PARALLEL SAFE",
                        _ => " PARALLEL UNSAFE",
                    };

                    // Insert the parallel clause after LANGUAGE
                    if let Some(lang_pos) = definition.find("LANGUAGE") {
                        if let Some(as_pos) = definition[lang_pos..].find("AS") {
                            let insert_pos = lang_pos + as_pos;
                            definition.insert_str(insert_pos, parallel_clause);
                        }
                    }
                }

                // Add cost and rows information if they differ from defaults
                // Default cost is 1.0, default rows is 1000.0
                // Note: PostgreSQL's pg_get_functiondef() doesn't always include ROWS clauses
                // even when they were specified, so we need to add them based on the actual values
                let has_cost = definition.contains(" COST ");

                debug!("Function definition before processing: {}", definition);
                debug!("Cost: {}, Rows: {}, Has cost: {}", cost, rows, has_cost);

                // If we need to add COST or ROWS clauses, we need to handle the case where
                // COST might already be present in the definition
                let needs_cost = (cost - 1.0).abs() > f32::EPSILON;
                let needs_rows = (rows - 1000.0).abs() > f32::EPSILON;

                if needs_cost || needs_rows {
                    // Remove existing COST clause if present and we need to add it
                    if has_cost && needs_cost {
                        if let Some(cost_start) = definition.find(" COST ") {
                            if let Some(cost_end) = definition[cost_start..].find(" ") {
                                let cost_end = cost_start + cost_end;
                                definition.replace_range(cost_start..cost_end, "");
                            }
                        }
                    }

                    // Build the new clauses
                    let mut clauses = Vec::new();
                    if needs_cost {
                        clauses.push(format!(" COST {}", cost));
                    }
                    // Always add ROWS clause since pg_get_functiondef() doesn't reliably include it
                    // in the output, even when it was specified in the original function
                    clauses.push(format!(" ROWS {}", rows));

                    debug!("Clauses to add: {:?}", clauses);

                    // Insert cost and rows clauses before AS
                    if let Some(as_pos) = definition.find(" AS") {
                        let clauses_str = clauses.join("");
                        definition.insert_str(as_pos, &clauses_str);
                        debug!("Function definition after processing: {}", definition);
                    } else if let Some(as_pos) = definition.find("AS") {
                        // Try without leading space in case the definition has "PARALLEL UNSAFEAS"
                        let clauses_str = clauses.join("");
                        definition.insert_str(as_pos, &clauses_str);
                        debug!("Function definition after processing: {}", definition);
                    } else {
                        debug!("Could not find ' AS' or 'AS' in function definition");
                    }
                }

                Some(Routine::Function(Function {
                    oid,
                    name: row.get("name"),
                    schema: row.get("schema_name"),
                    owner: row.get("owner"),
                    definition,
                    identity_arguments: row.get("identity_arguments"),
                    acl: row.get("acl"),
                    comment: row.get("comment"),
                    is_from_extension,
                }))
            }
            'p' => {
                // Procedures
                Some(Routine::Procedure(Procedure {
                    oid,
                    name: row.get("name"),
                    schema: row.get("schema_name"),
                    owner: row.get("owner"),
                    definition: row.get("definition"),
                    identity_arguments: row.get("identity_arguments"),
                    acl: row.get("acl"),
                    comment: row.get("comment"),
                    is_from_extension,
                }))
            }
            'a' => {
                // Aggregates
                let name: String = row.get("name");
                let schema: String = row.get("schema_name");
                let identity_args: String = row.get("identity_arguments");
                let details = aggregate_details_map.get(&oid).cloned().unwrap_or_default();

                // Construct the full CREATE AGGREGATE statement
                let definition = format!(
                    "CREATE AGGREGATE {}.{}({}) (\n    {}\n);",
                    quote_ident(&schema),
                    quote_ident(&name),
                    identity_args,
                    details
                );

                Some(Routine::Aggregate(Aggregate {
                    oid,
                    name,
                    schema,
                    owner: row.get("owner"),
                    definition,
                    identity_arguments: identity_args,
                    acl: row.get("acl"),
                    comment: row.get("comment"),
                    is_from_extension,
                }))
            }
            _ => None,
        };

        if let Some(routine) = routine_option {
            routines.push(routine);
        }
    }

    Ok(routines)
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

pub async fn introspect_triggers<C: GenericClient>(
    client: &C,
    table_oids: &[u32],
) -> Result<HashMap<u32, Vec<Trigger>>> {
    if table_oids.is_empty() {
        return Ok(HashMap::new());
    }

    tracing::debug!("Introspecting triggers for table OIDs: {:?}", table_oids);

    // This query fetches all triggers for the given tables.
    let query = r#"
        SELECT
            t.oid,
            t.tgname AS name,
            t.tgrelid AS table_oid,
            c.relname AS table_name,
            n.nspname AS schema_name,
            -- This function gives us the complete CREATE statement
            pg_get_triggerdef(t.oid) AS definition,
            t.tgconstraint <> 0 AS is_constraint,
            obj_description(t.oid, 'pg_trigger') as comment,
            EXISTS (
                SELECT 1 FROM pg_depend d
                WHERE d.objid = t.oid AND d.classid = 'pg_trigger'::regclass AND d.deptype = 'e'
            ) AS is_from_extension
        FROM pg_trigger t
        JOIN pg_class c ON t.tgrelid = c.oid
        JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE t.tgrelid = ANY($1)
          AND NOT t.tgisinternal; -- Exclude internal triggers
    "#;
    let rows = client.query(query, &[&table_oids]).await?;
    
    tracing::debug!("Found {} trigger rows", rows.len());

    let mut triggers_map: HashMap<u32, Vec<Trigger>> = HashMap::new();
    for row in rows {
        let table_oid: u32 = row.get("table_oid");
        let trigger_name: String = row.get("name");
        let table_name: String = row.get("table_name");
        tracing::debug!("Found trigger '{}' on table '{}' (OID: {})", trigger_name, table_name, table_oid);
        triggers_map.entry(table_oid).or_default().push(Trigger {
            oid: row.get("oid"),
            name: trigger_name,
            table_oid,
            table_name,
            schema: row.get("schema_name"),
            definition: row.get("definition"),
            is_constraint: row.get("is_constraint"),
            comment: row.get("comment"),
            is_from_extension: row.get("is_from_extension"),
        });
    }

    Ok(triggers_map)
}

// This function now fetches all policies efficiently and assembles them.
pub async fn introspect_policies<C: GenericClient>(
    client: &C,
    include_predefined: bool,
) -> Result<Vec<Policy>> {
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
        "datlastsysoid column exists (policies): {}",
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

    tracing::debug!("Last system OID (policies): {}", last_system_oid);

    let roles_map = get_roles_oid_map(client).await?; // Helper to get OID -> name

    // --- QUERY 1: Fetch all policies and their properties ---
    let policies_query = r#"
        SELECT 
            p.oid, p.polname AS name,
            c.oid AS table_oid, c.relname AS table_name, n.nspname AS schema_name,
            p.polpermissive AS permissive, p.polroles AS role_oids,
            p.polcmd,
            pg_get_expr(p.polqual, p.polrelid) AS using_expression,
            pg_get_expr(p.polwithcheck, p.polrelid) AS check_expression,
            EXISTS (
                SELECT 1 FROM pg_depend d
                WHERE d.objid = p.oid AND d.classid = 'pg_policy'::regclass AND d.deptype = 'e'
            ) AS is_from_extension
        FROM pg_policy p
        JOIN pg_class c ON p.polrelid = c.oid
        JOIN pg_namespace n ON c.relnamespace = n.oid;
    "#;
    let policy_rows = client.query(policies_query, &[]).await?;

    // --- QUERY 2: Fetch all tables with Row Level Security enabled ---
    let rls_enabled_query = "SELECT oid FROM pg_class WHERE relrowsecurity";
    let rls_enabled_rows = client.query(rls_enabled_query, &[]).await?;
    let rls_enabled_oids: HashMap<u32, bool> = rls_enabled_rows
        .into_iter()
        .map(|r| (r.get("oid"), true))
        .collect();

    // --- Assemble final Vec<Policy> ---
    let mut all_policies = Vec::new();

    // First, create entries for the policies themselves
    for row in policy_rows {
        let oid: u32 = row.get("oid");
        let role_oids: Vec<u32> = row.get("role_oids");

        let roles = if role_oids.is_empty() || (role_oids.len() == 1 && role_oids[0] == 0) {
            vec!["PUBLIC".to_string()] // OID 0 in polroles means PUBLIC
        } else {
            role_oids
                .iter()
                .filter_map(|&oid| roles_map.get(&oid).cloned())
                .collect()
        };

        let command_char: i8 = row.get("polcmd");
        let policy_command = match command_char as u8 as char {
            'r' => PolicyCommand::Select,
            'a' => PolicyCommand::Insert,
            'w' => PolicyCommand::Update,
            'd' => PolicyCommand::Delete,
            '*' => PolicyCommand::All,
            _ => PolicyCommand::All,
        };

        all_policies.push(Policy {
            oid,
            name: Some(row.get("name")),
            table_oid: row.get("table_oid"),
            table_name: row.get("table_name"),
            schema: row.get("schema_name"),
            command: policy_command,
            permissive: row.get("permissive"),
            roles,
            using: row.get("using_expression"),
            check: row.get("check_expression"),
            is_user_defined: oid > last_system_oid,
            is_from_extension: row.get("is_from_extension"),
        });
    }

    // Now, find all tables that have RLS enabled but might not have explicit policies yet.
    // We create a special "Policy" entry for them.
    for (table_oid, _) in rls_enabled_oids {
        // Check if we already have a policy for this table. If so, RLS is implicitly handled.
        if all_policies.iter().any(|p| p.table_oid == table_oid) {
            continue;
        }

        // If not, we need a dedicated "ENABLE ROW LEVEL SECURITY" entry.
        // We find the table's info from the policies list (or would fetch it if needed).
        if let Some(policy_for_table) = all_policies.iter().find(|p| p.table_oid == table_oid) {
            all_policies.push(Policy {
                oid: 0,     // No real OID for this conceptual object
                name: None, // This signifies ENABLE RLS
                table_oid,
                table_name: policy_for_table.table_name.clone(),
                schema: policy_for_table.schema.clone(),
                command: PolicyCommand::All, // Not applicable
                permissive: false,           // Not applicable
                roles: Vec::new(),           // Not applicable
                using: None,                 // Not applicable
                check: None,                 // Not applicable
                is_user_defined: true,       // The act of enabling RLS is user-defined
                is_from_extension: false,    // Cannot be from an extension
            });
        }
    }

    // --- Final Filtering ---
    if include_predefined {
        Ok(all_policies)
    } else {
        let dumpable_policies = all_policies
            .into_iter()
            .filter(|p| p.is_user_defined && !p.is_from_extension)
            .collect();
        Ok(dumpable_policies)
    }
}

// Helper to fetch OID -> name mapping for all roles
async fn get_roles_oid_map<C: GenericClient>(client: &C) -> Result<HashMap<u32, String>> {
    let rows = client
        .query("SELECT oid, rolname FROM pg_roles", &[])
        .await?;
    Ok(rows
        .into_iter()
        .map(|row| (row.get("oid"), row.get("rolname")))
        .collect())
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

pub async fn introspect_event_triggers<C: GenericClient>(client: &C) -> Result<Vec<EventTrigger>> {
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

    // This query fetches all user-defined event triggers.
    let query = r#"
        SELECT
            e.oid,
            e.evtname AS name,
            pg_get_userbyid(e.evtowner) AS owner,
            obj_description(e.oid, 'pg_event_trigger') AS comment,
            -- We reconstruct the definition here, as there's no single pg_get_* function for it.
            'CREATE EVENT TRIGGER ' || quote_ident(e.evtname) ||
            ' ON ' || e.evtevent ||
            CASE WHEN e.evttags IS NOT NULL
                 THEN ' WHEN TAG IN (' || (SELECT string_agg(quote_literal(t), ', ') FROM unnest(e.evttags) t) || ')'
                 ELSE ''
            END ||
            ' EXECUTE FUNCTION ' || e.evtfoid::regprocedure::text || '();'
            AS definition,
            EXISTS (
                SELECT 1 FROM pg_depend d
                WHERE d.objid = e.oid AND d.classid = 'pg_event_trigger'::regclass AND d.deptype = 'e'
            ) AS is_from_extension
        FROM pg_event_trigger e
        WHERE e.oid > $1;
    "#;
    let rows = client.query(query, &[&last_system_oid]).await?;

    let mut event_triggers = Vec::new();
    for row in rows {
        event_triggers.push(EventTrigger {
            oid: row.get("oid"),
            name: row.get("name"),
            owner: row.get("owner"),
            definition: row.get("definition"),
            comment: row.get("comment"),
            is_from_extension: row.get("is_from_extension"),
        });
    }

    Ok(event_triggers)
}

// This function now fetches all rules efficiently.
pub async fn introspect_rules<C: GenericClient>(
    client: &C,
    include_predefined: bool, // Not very useful for rules, but for consistency
) -> Result<Vec<Rule>> {
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

    // --- QUERY: Fetch all non-implicit rules from the database ---
    let rules_query = r#"
        SELECT
            r.oid,
            r.rulename AS name,
            c.oid AS table_oid,
            c.relname AS table_name,
            n.nspname AS schema_name,
            pg_get_ruledef(r.oid) AS definition,
            obj_description(r.oid, 'pg_rewrite') AS comment,
            EXISTS (
                SELECT 1 FROM pg_depend d
                WHERE d.objid = r.oid AND d.classid = 'pg_rewrite'::regclass AND d.deptype = 'e'
            ) AS is_from_extension
        FROM pg_rewrite r
        JOIN pg_class c ON r.ev_class = c.oid
        JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE
            -- Exclude the implicit _RETURN rules created for views
            r.rulename != '_RETURN'
            -- We only care about rules on user-defined tables/views
            AND c.oid > $1;
    "#;
    let rule_rows = client.query(rules_query, &[&last_system_oid]).await?;

    // --- Assemble the final Vec<Rule> ---
    let mut all_rules = Vec::new();
    for row in rule_rows {
        let oid: u32 = row.get("oid");

        all_rules.push(Rule {
            oid,
            name: row.get("name"),
            table_oid: row.get("table_oid"),
            table_name: row.get("table_name"),
            schema: row.get("schema_name"),
            definition: row.get("definition"),
            comment: row.get("comment"),
            is_user_defined: true, // All rules we fetch here are considered user-defined
            is_from_extension: row.get("is_from_extension"),
        });
    }

    // --- Final Filtering ---
    if include_predefined {
        // Technically, no rules are "predefined", so this flag has little effect
        Ok(all_rules)
    } else {
        let dumpable_rules = all_rules
            .into_iter()
            .filter(|r| !r.is_from_extension)
            .collect();
        Ok(dumpable_rules)
    }
}

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
        let generated_struct = generated.map(|_c| Generated {
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
            fdw_options: fdw_options
                .map(|opts| {
                    opts.into_iter()
                        .filter_map(|opt| {
                            if let Some((key, value)) = opt.split_once('=') {
                                Some((key.to_string(), value.to_string()))
                            } else {
                                None
                            }
                        })
                        .collect()
                })
                .unwrap_or_default(),
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

// Helper functions for parsing the single-char codes
fn parse_constraint_type(c: i8) -> ConstraintType {
    match c as u8 as char {
        'c' => ConstraintType::Check,
        'f' => ConstraintType::ForeignKey,
        'p' => ConstraintType::PrimaryKey,
        'u' => ConstraintType::Unique,
        'x' => ConstraintType::Exclusion,
        _ => ConstraintType::Check, // Should be unreachable
    }
}

fn parse_ref_action(c: i8) -> ReferentialAction {
    match c as u8 as char {
        'a' => ReferentialAction::NoAction,
        'r' => ReferentialAction::Restrict,
        'c' => ReferentialAction::Cascade,
        'n' => ReferentialAction::SetNull,
        'd' => ReferentialAction::SetDefault,
        _ => ReferentialAction::NoAction, // Default/fallback
    }
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

// This is the new, unified function.
pub async fn introspect_relations_unified<C: GenericClient>(client: &C) -> Result<Vec<Relation>> {
    // 1. Get system OID threshold for filtering
    let last_system_oid: u32 = {
        // Check if datlastsysoid column exists in pg_database
        let column_exists_row = client
            .query_one(
                "SELECT EXISTS (
                    SELECT 1 FROM information_schema.columns 
                    WHERE table_schema = 'pg_catalog' 
                    AND table_name = 'pg_database' 
                    AND column_name = 'datlastsysoid'
                ) as column_exists",
                &[],
            )
            .await?;
        let datlastsysoid_exists: bool = column_exists_row.get("column_exists");

        if datlastsysoid_exists {
            let last_system_oid_row = client
                .query_one(
                    "SELECT datlastsysoid FROM pg_database WHERE datname = current_database()",
                    &[],
                )
                .await?;
            last_system_oid_row.get("datlastsysoid")
        } else {
            16384 // Default system OID threshold for older PostgreSQL versions
        }
    };

    // --- QUERY 1: Fetch ALL relation-like objects at once ---
    let relations_query = r#"
        SELECT
            c.oid, c.relname AS name, n.nspname AS schema_name, pg_get_userbyid(c.relowner) AS owner,
            c.relkind, obj_description(c.oid, 'pg_class') AS comment,
            ts.spcname AS tablespace, c.relacl::text AS acl,
            c.reloptions AS options,
            -- Table-specific properties
            c.relreplident AS replica_identity_char,
            ri_class.relname AS replica_identity_index_name,
            -- View/MatView-specific properties
            pg_get_viewdef(c.oid) AS definition,
            CASE
                WHEN 'check_option=local' = ANY(c.reloptions) THEN 'LOCAL'
                WHEN 'check_option=cascaded' = ANY(c.reloptions) THEN 'CASCADED'
                ELSE 'NONE'
            END AS check_option,
            c.relispopulated AS is_populated,
            -- Extension dependency
            EXISTS (
                SELECT 1 FROM pg_depend d
                WHERE d.objid = c.oid AND d.classid = 'pg_class'::regclass AND d.deptype = 'e'
            ) AS is_from_extension
        FROM pg_class c
        JOIN pg_namespace n ON c.relnamespace = n.oid
        LEFT JOIN pg_tablespace ts ON c.reltablespace = ts.oid
        LEFT JOIN pg_index ri ON ri.indrelid = c.oid AND ri.indisreplident
        LEFT JOIN pg_class ri_class ON ri_class.oid = ri.indexrelid
        WHERE c.relkind IN ('r', 'v', 'm', 'p', 'f'); -- Fetch all relation kinds
    "#;
    let relation_rows = client.query(relations_query, &[]).await?;

    let all_relation_oids: Vec<u32> = relation_rows.iter().map(|row| row.get("oid")).collect();

    // --- BULK QUERIES for sub-objects of ALL relations ---
    let columns_map = introspect_all_columns(client, &all_relation_oids).await?;
    let constraints_map = introspect_all_constraints(client, &all_relation_oids).await?;
    let indexes_map = introspect_all_indexes(client, &all_relation_oids).await?;
    let inheritance_map = introspect_all_inheritance(client).await?;
    let partition_key_map = introspect_all_partition_keys(client).await?;

    // --- Assemble the final Vec<Relation> ---
    let mut relations = Vec::new();
    for row in relation_rows {
        let oid: u32 = row.get("oid");
        let relkind: i8 = row.get("relkind");

        let is_user_defined = oid > last_system_oid;
        let is_from_extension: bool = row.get("is_from_extension");

        // Skip non-dumpable objects
        if !is_user_defined || is_from_extension {
            continue;
        }

        let relkind_char = relkind as u8 as char;
        let new_relation = match relkind_char {
            'r' | 'p' | 'f' => {
                // Tables, Partitioned Tables, Foreign Tables
                let replica_identity_char: i8 = row.get("replica_identity_char");
                let index_name: Option<String> = row.get("replica_identity_index_name");
                let replica_identity = match replica_identity_char as u8 as char {
                    'd' => ReplicaIdentity::Default,
                    'n' => ReplicaIdentity::Nothing,
                    'f' => ReplicaIdentity::Full,
                    'i' => ReplicaIdentity::Index(index_name.unwrap_or_default()),
                    _ => ReplicaIdentity::Default,
                };

                let table = Table {
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
                    triggers: Vec::new(), // Will be populated later
                    inherits: inheritance_map.get(&oid).cloned().unwrap_or_default(),
                    partition_key: partition_key_map.get(&oid).cloned(),
                    replica_identity,
                    is_user_defined,
                    is_from_extension,
                };

                if relkind_char == 'f' {
                    unimplemented!()
                } else {
                    Relation::Table(table) // Wrap in Table variant
                }
            }
            'v' => {
                let check_option_str: &str = row.get("check_option");
                Relation::View(View {
                    oid,
                    name: row.get("name"),
                    schema: row.get("schema_name"),
                    owner: row.get("owner"),
                    definition: row.get("definition"),
                    comment: row.get("comment"),
                    acl: row.get("acl"),
                    columns: columns_map.get(&oid).cloned().unwrap_or_default(),
                    check_option: match check_option_str {
                        "LOCAL" => CheckOption::Local,
                        "CASCADED" => CheckOption::Cascaded,
                        _ => CheckOption::None,
                    },
                    options: pg_options_to_map(row.get("options")),
                    is_user_defined,
                    is_from_extension,
                })
            }
            'm' => Relation::MaterializedView(MaterializedView {
                oid,
                name: row.get("name"),
                schema: row.get("schema_name"),
                owner: row.get("owner"),
                definition: row.get("definition"),
                comment: row.get("comment"),
                acl: row.get("acl"),
                is_populated: row.get("is_populated"),
                columns: columns_map.get(&oid).cloned().unwrap_or_default(),
                indexes: indexes_map.get(&oid).cloned().unwrap_or_default(),
                options: pg_options_to_map(row.get("options")),
                tablespace: row.get("tablespace"),
                is_user_defined,
                is_from_extension,
            }),
            _ => continue, // Should not happen due to WHERE clause
        };
        relations.push(new_relation);
    }

    Ok(relations)
}
