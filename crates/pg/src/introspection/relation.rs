use crate::{
    helpers::{get_last_system_oid, get_qualified_name_map, pg_options_to_map},
    model::relation::{
        CheckOption, Column, ColumnStorage, Constraint, ConstraintType, ForeignKeyDetail,
        Generated, Identity, IdentityGeneration, Index, MaterializedView, ReferentialAction,
        Relation, ReplicaIdentity, Table, View,
    },
};
use common::error::Result;
use std::collections::HashMap;
use tokio_postgres::GenericClient;

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

    // New query: We JOIN to pg_class/pg_namespace to get the foreign table's name
    let query = r#"
        SELECT
            c.oid, c.conrelid AS table_oid, c.conname AS name,
            pg_get_constraintdef(c.oid) AS definition,
            c.contype,
            c.confrelid AS foreign_table_oid,
            fn.nspname AS foreign_table_schema,
            fc.relname AS foreign_table_name,
            (SELECT array_agg(a.attname ORDER BY u.ord)
             FROM unnest(c.conkey) WITH ORDINALITY u(attnum, ord)
             JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = u.attnum)
            AS local_columns,
            (SELECT array_agg(a.attname ORDER BY u.ord)
             FROM unnest(c.confkey) WITH ORDINALITY u(attnum, ord)
             JOIN pg_attribute a ON a.attrelid = c.confrelid AND a.attnum = u.attnum)
            AS foreign_columns,
            c.confupdtype AS on_update, c.confdeltype AS on_delete,
            c.condeferrable AS is_deferrable, c.condeferred AS is_initially_deferred,
            NOT c.convalidated AS is_not_valid
        FROM pg_constraint c
        LEFT JOIN pg_class fc ON c.confrelid = fc.oid
        LEFT JOIN pg_namespace fn ON fc.relnamespace = fn.oid
        WHERE c.conrelid = ANY($1) 
          AND c.conparentid = 0
        ORDER BY c.conrelid, c.conname;
    "#;

    let rows = client.query(query, &[&table_oids]).await?;

    let mut map: HashMap<u32, Vec<Constraint>> = HashMap::new();
    for row in rows {
        let table_oid: u32 = row.get("table_oid");
        let contype_char: i8 = row.get("contype");

        // The logic to build the enum now happens here, where all data is available.
        let constraint_type = match contype_char as u8 as char {
            'c' => ConstraintType::Check,
            'p' => ConstraintType::PrimaryKey,
            'u' => ConstraintType::Unique,
            'x' => ConstraintType::Exclusion,
            'f' => {
                let detail = ForeignKeyDetail {
                    foreign_table_oid: row.get("foreign_table_oid"),
                    foreign_table_schema: row.get("foreign_table_schema"),
                    foreign_table_name: row.get("foreign_table_name"),
                    local_columns: row
                        .get::<_, Option<Vec<String>>>("local_columns")
                        .unwrap_or_default(),
                    foreign_columns: row
                        .get::<_, Option<Vec<String>>>("foreign_columns")
                        .unwrap_or_default(),
                    on_update: parse_ref_action(row.get("on_update")),
                    on_delete: parse_ref_action(row.get("on_delete")),
                };
                ConstraintType::ForeignKey(detail)
            }
            _ => continue, // Skip unknown types
        };

        map.entry(table_oid).or_default().push(Constraint {
            oid: row.get("oid"),
            name: row.get("name"),
            table_oid,
            definition: row.get("definition"),
            r#type: constraint_type, // Use the newly constructed enum
            is_deferrable: row.get("is_deferrable"),
            is_initially_deferred: row.get("is_initially_deferred"),
            is_not_valid: row.get("is_not_valid"),
        });
    }
    Ok(map)
}

// This is the new, improved function.
pub async fn introspect_all_indexes<C: GenericClient>(
    client: &C,
    table_oids: &[u32],
) -> Result<HashMap<u32, Vec<Index>>> {
    if table_oids.is_empty() {
        return Ok(HashMap::new());
    }

    // This query is much simpler and more robust. It uses pg_get_indexdef.
    let query = r#"
        SELECT
            i.indexrelid AS oid,
            c.relname AS name,
            i.indrelid AS table_oid,
            -- This function provides the complete, correct CREATE INDEX statement
            pg_get_indexdef(i.indexrelid) AS definition,
            i.indisclustered AS is_clustered,
            obj_description(i.indexrelid, 'pg_class') AS comment
        FROM pg_index i
        JOIN pg_class c ON i.indexrelid = c.oid
        WHERE i.indrelid = ANY($1)  -- Filter by the parent table OIDs
          AND i.indisvalid             -- Only include valid indexes
        ORDER BY i.indrelid, c.relname;
    "#;

    // Use safe parameterization instead of string formatting
    let rows = client.query(query, &[&table_oids]).await?;

    let mut indexes_map: HashMap<u32, Vec<Index>> = HashMap::new();

    for row in rows {
        let table_oid: u32 = row.get("table_oid");

        let index = Index {
            oid: row.get("oid"),
            name: row.get("name"),
            table_oid,
            definition: row.get("definition"),
            is_clustered: row.get("is_clustered"),
            comment: row.get("comment"),
        };

        indexes_map.entry(table_oid).or_default().push(index);
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

// This is the new, unified function.
pub async fn introspect_relations_unified<C: GenericClient>(client: &C) -> Result<Vec<Relation>> {
    // 1. Get system OID threshold for filtering
    let last_system_oid = get_last_system_oid(client).await?;

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
                    rules: Vec::new(),    // Will be populated later
                    policies: Vec::new(), // Will be populated later
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
                    rules: Vec::new(),    // Will be populated later
                    policies: Vec::new(), // Will be populated later
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
                triggers: Vec::new(), // Will be populated later
                is_user_defined,
                is_from_extension,
            }),
            _ => continue, // Should not happen due to WHERE clause
        };
        relations.push(new_relation);
    }

    Ok(relations)
}
