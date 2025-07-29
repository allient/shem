use crate::model::routine::Routine;
use std::collections::HashMap;
use tokio_postgres::{Client, Error, GenericClient};

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
