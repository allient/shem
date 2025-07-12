use shem_core::Result;
use shem_core::schema::*;
use tokio_postgres::GenericClient;
use tracing::debug;

/// Introspect PostgreSQL database schema
pub async fn introspect_schema<C>(client: &C) -> Result<Schema>
where
    C: GenericClient + Sync,
{
    let mut schema = Schema::new();

    // Independent Objects (Standalone)

    // Introspect extensions
    let extensions = introspect_extensions(&*client).await?;
    for ext in extensions {
        schema.extensions.insert(ext.name.clone(), ext);
    }

    // Introspect named schemas
    // Purpose: Namespace to organize objects (tables, functions, etc.).
    let named_schemas = introspect_named_schemas(&*client).await?;
    for named_schema in named_schemas {
        schema
            .named_schemas
            .insert(named_schema.name.clone(), named_schema);
    }

    // Introspect roles
    // Purpose: Manage authentication and permissions.
    // CREATE ROLE analyst WITH LOGIN PASSWORD 'secure123';
    // GRANT SELECT ON ALL TABLES IN SCHEMA public TO analyst;
    let roles = introspect_roles(&*client).await?;
    for role in roles {
        schema.roles.insert(role.name.clone(), role);
    }

    // Introspect collations
    //Purpose: Define string sorting/rules (e.g., case-insensitive comparison).
    let collations = introspect_collations(&*client).await?;
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

    // Introspect enums
    //Purpose: Define a static set of values (e.g., statuses, categories).
    let enums = introspect_enums(&*client).await?;
    for enum_type in enums {
        schema.enums.insert(enum_type.name.clone(), enum_type);
    }

    // Introspect domains
    // Purpose: Create a custom type with constraints (e.g., positive integers).
    let domains = introspect_domains(&*client).await?;
    for domain in domains {
        schema.domains.insert(domain.name.clone(), domain);
    }

    // Introspect base types
    // Purpose: Fundamental types like INTEGER, TEXT, JSONB.
    //CREATE TYPE rgb_color AS ENUM ('red', 'green', 'blue');  -- Extends base types
    let base_types = introspect_base_types(&*client).await?;
    for base_type in base_types {
        schema.base_types.insert(base_type.name.clone(), base_type);
    }

    // Introspect composite types
    // Purpose: Combine multiple base types (e.g., address with street, city, state).
    // CREATE TYPE address AS (street TEXT, city TEXT, zip VARCHAR(10));
    let composite_types = introspect_composite_types(&*client).await?;
    for composite_type in composite_types {
        schema
            .composite_types
            .insert(composite_type.name.clone(), composite_type);
    }

    // Introspect range types separately for detailed information
    // Purpose: Represent a range of values (e.g., dates, numbers).
    let range_types = introspect_range_types(&*client).await?;
    for range_type in range_types {
        // Store range types in the types collection with a special prefix
        schema
            .range_types
            .insert(range_type.name.clone(), range_type);
    }

    // Introspect multirange types
    // Purpose: Discontinuous ranges (PostgreSQL 14+).
    // SELECT '[2023-01-01, 2023-01-05), [2023-02-01, 2023-02-03)'::DATEMULTIRANGE;
    let multirange_types = introspect_multirange_types(&*client).await?;
    for multirange_type in multirange_types {
        schema
            .multirange_types
            .insert(multirange_type.name.clone(), multirange_type);
    }

    // Introspect array types
    // Purpose: Store arrays of any base/composite type.
    let array_types = introspect_array_types(&*client).await?;
    for array_type in array_types {
        schema
            .array_types
            .insert(array_type.name.clone(), array_type);
    }

    // Introspect sequences
    //Purpose: Generate auto-incrementing IDs.
    let sequences = introspect_sequences(&*client).await?;
    for seq in sequences {
        schema.sequences.insert(seq.name.clone(), seq);
    }

    // Semi-Independent Objects

    // Introspect tables
    // Purpose: Store data.
    let tables = introspect_tables(&*client).await?;
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

async fn introspect_tables<C: GenericClient>(client: &C) -> Result<Vec<Table>> {
    let query = r#"
        SELECT 
            t.table_schema,
            t.table_name,
            obj_description(pgc.oid, 'pg_class') as comment,
            pgc.relowner as owner,
            pgc.reltablespace as tablespace_oid,
            pgc.reloptions as storage_parameters
        FROM information_schema.tables t
        JOIN pg_class pgc ON pgc.relname = t.table_name
        JOIN pg_namespace n ON pgc.relnamespace = n.oid AND n.nspname = t.table_schema
        WHERE t.table_schema NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
        AND t.table_type = 'BASE TABLE'
        AND pgc.relowner > 1  -- exclude system-owned tables
        AND NOT EXISTS (
            -- Exclude tables that are part of extensions
            SELECT 1 FROM pg_depend d
            JOIN pg_extension e ON d.refobjid = e.oid
            WHERE d.objid = pgc.oid AND d.deptype = 'e'
        )
    "#;

    let rows = client.query(query, &[]).await?;
    let mut tables = Vec::new();

    for row in rows {
        let schema: Option<String> = row.get("table_schema");
        let name: String = row.get("table_name");
        let comment: Option<String> = row.get("comment");
        let tablespace_oid: Option<u32> = row.get("tablespace_oid");
        let storage_parameters: Option<Vec<String>> = row.get("storage_parameters");

        // Get columns
        let columns = introspect_columns(client, &schema, &name).await?;

        // Get constraints
        let constraints = introspect_constraints(client, &schema, &name).await?;

        // Get indexes
        let indexes = introspect_indexes(client, &schema, &name).await?;

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

        // Get inheritance information
        let inherits_query = r#"
            SELECT c.relname as parent_table
            FROM pg_inherits i
            JOIN pg_class c ON i.inhparent = c.oid
            JOIN pg_class child ON i.inhrelid = child.oid
            JOIN pg_namespace n ON child.relnamespace = n.oid
            WHERE child.relname = $1 AND n.nspname = $2
            ORDER BY c.relname
        "#;
        let inherits_rows = client
            .query(
                inherits_query,
                &[&name, &schema.as_deref().unwrap_or("public")],
            )
            .await?;
        let inherits: Vec<String> = inherits_rows
            .iter()
            .map(|row| row.get::<_, String>("parent_table"))
            .collect();

        // Get partitioning information
        let partition_by = if inherits.is_empty() {
            // Check if this table is a partitioned table (has partitions)
            let partition_query = r#"
                SELECT c.relname as partition_name
                FROM pg_inherits i
                JOIN pg_class c ON i.inhrelid = c.oid
                JOIN pg_class parent ON i.inhparent = parent.oid
                JOIN pg_namespace n ON parent.relnamespace = n.oid
                WHERE parent.relname = $1 AND n.nspname = $2
                LIMIT 1
            "#;
            let partition_rows = client
                .query(
                    partition_query,
                    &[&name, &schema.as_deref().unwrap_or("public")],
                )
                .await?;

            if !partition_rows.is_empty() {
                // This is a partitioned table, get the partition strategy and columns
                let partition_info_query = r#"
                    SELECT 
                        pg_get_partkeydef(parent.oid) as partition_expression,
                        parent.relpartbound as partition_bound
                    FROM pg_class parent
                    JOIN pg_namespace n ON parent.relnamespace = n.oid
                    WHERE parent.relname = $1 AND n.nspname = $2
                "#;
                let partition_info_rows = client
                    .query(
                        partition_info_query,
                        &[&name, &schema.as_deref().unwrap_or("public")],
                    )
                    .await?;

                if let Some(row) = partition_info_rows.first() {
                    let partition_expression: Option<String> = row.get("partition_expression");
                    if let Some(expr) = partition_expression {
                        // Parse the partition expression to extract method and columns
                        // Example: "RANGE (created_date)" or "LIST (region)"
                        if expr.to_uppercase().contains("RANGE") {
                            // Extract column names from the expression
                            let columns = extract_partition_columns(&expr);
                            Some(PartitionBy {
                                method: PartitionMethod::Range,
                                columns,
                            })
                        } else if expr.to_uppercase().contains("LIST") {
                            let columns = extract_partition_columns(&expr);
                            Some(PartitionBy {
                                method: PartitionMethod::List,
                                columns,
                            })
                        } else if expr.to_uppercase().contains("HASH") {
                            let columns = extract_partition_columns(&expr);
                            Some(PartitionBy {
                                method: PartitionMethod::Hash,
                                columns,
                            })
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                } else {
                    None
                }
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

        tables.push(Table {
            name,
            schema,
            columns,
            constraints,
            indexes,
            comment,
            tablespace,
            inherits,
            partition_by,
            storage_parameters: storage_params,
        });
    }

    Ok(tables)
}

async fn introspect_columns<C: GenericClient>(
    client: &C,
    schema: &Option<String>,
    table: &str,
) -> Result<Vec<Column>> {
    let query = r#"
        SELECT 
            a.attname as column_name,
            pg_catalog.format_type(a.atttypid, a.atttypmod) as type_name,
            NOT a.attnotnull as is_nullable,
            pg_get_expr(ad.adbin, ad.adrelid) as column_default,
            c.identity_generation,
            c.generation_expression,
            a.attcollation as collation_oid,
            col.collname as collation_name,
            obj_description(a.attrelid, 'pg_class') as table_comment,
            col_description(a.attrelid, a.attnum) as column_comment
        FROM pg_catalog.pg_attribute a
        JOIN pg_catalog.pg_class t ON a.attrelid = t.oid
        JOIN pg_catalog.pg_namespace n ON t.relnamespace = n.oid
        LEFT JOIN pg_catalog.pg_attrdef ad ON ad.adrelid = a.attrelid AND ad.adnum = a.attnum
        LEFT JOIN information_schema.columns c ON 
            c.table_schema = n.nspname 
            AND c.table_name = t.relname 
            AND c.column_name = a.attname
        LEFT JOIN pg_catalog.pg_collation col ON col.oid = a.attcollation
        WHERE n.nspname = $1
        AND t.relname = $2
        AND a.attnum > 0
        AND NOT a.attisdropped
        ORDER BY a.attnum
    "#;

    let rows = client.query(query, &[schema, &table.to_string()]).await?;
    let mut columns = Vec::new();

    for row in rows {
        let name: String = row.get("column_name");
        let type_name: String = row.get("type_name");
        let nullable: bool = row.get("is_nullable");
        let default: Option<String> = row.get("column_default");
        let identity: Option<Identity> = match row.get::<_, Option<String>>("identity_generation") {
            Some(identity_type) if identity_type == "ALWAYS" => Some(Identity {
                always: true,
                start: 1,
                increment: 1,
                min_value: None,
                max_value: None,
                cache: None,
                cycle: false,
            }),
            Some(identity_type) if identity_type == "BY DEFAULT" => Some(Identity {
                always: false,
                start: 1,
                increment: 1,
                min_value: None,
                max_value: None,
                cache: None,
                cycle: false,
            }),
            _ => None,
        };
        let generated: Option<GeneratedColumn> = row
            .get::<_, Option<String>>("generation_expression")
            .map(|expr| GeneratedColumn {
                expression: expr,
                stored: true,
            });
        let collation: Option<String> = row.get("collation_name");
        let column_comment: Option<String> = row.get("column_comment");

        columns.push(Column {
            name,
            type_name,
            nullable,
            default,
            identity,
            generated,
            comment: column_comment,
            collation,
            storage: None,     // TODO: Get storage type
            compression: None, // TODO: Get compression method
        });
    }

    Ok(columns)
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

async fn introspect_composite_types<C: GenericClient>(client: &C) -> Result<Vec<CompositeType>>
where
    C: GenericClient + Sync,
{
    let query = r#"
        SELECT 
            t.typname AS name,
            n.nspname AS schema,
            att.attname AS attribute_name,
            pg_catalog.format_type(att.atttypid, att.atttypmod) AS attribute_type,
            att.attnum,
            att.attnotnull AS is_not_null,
            att.attcollation AS collation_oid,
            att.attstorage AS storage_type,
            att.attcompression AS compression,
            pg_get_expr(ad.adbin, ad.adrelid) AS default_expr,
            col.collname AS collation_name,
            obj_description(t.oid, 'pg_type') AS type_comment,
            obj_description(att.attrelid, 'pg_class') AS class_comment,
            t.typowner AS owner
        FROM pg_type t
        JOIN pg_namespace n ON n.oid = t.typnamespace
        JOIN pg_class c ON c.relname = t.typname AND c.relnamespace = t.typnamespace AND c.relkind = 'c'
        JOIN pg_attribute att ON att.attrelid = c.oid
        LEFT JOIN pg_attrdef ad ON ad.adrelid = att.attrelid AND ad.adnum = att.attnum
        LEFT JOIN pg_collation col ON col.oid = att.attcollation
        WHERE t.typtype = 'c'
          AND att.attnum > 0
          AND NOT att.attisdropped
          AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
          AND t.typowner > 1
          AND NOT EXISTS (
              SELECT 1
              FROM pg_depend dep
              JOIN pg_extension e ON dep.refobjid = e.oid
              WHERE dep.objid = t.oid AND dep.deptype = 'e'
          )
        ORDER BY n.nspname, t.typname, att.attnum
    "#;

    let rows = client.query(query, &[]).await?;

    use std::collections::BTreeMap;
    let mut grouped: BTreeMap<(String, String), (Vec<Column>, Option<String>, u32)> =
        BTreeMap::new();

    for row in rows {
        let name: String = row.get("name");
        let schema: String = row.get("schema");
        let attr_name: String = row.get("attribute_name");
        let attr_type: String = row.get("attribute_type");
        let is_not_null: bool = row.get("is_not_null");
        let collation_name: Option<String> = row.get("collation_name");
        let storage_type: Option<i8> = row.get("storage_type");
        let compression: Option<i8> = row.get("compression");
        let default_expr: Option<String> = row.get("default_expr");
        let type_comment: Option<String> = row.get("type_comment");
        let class_comment: Option<String> = row.get("class_comment");
        let owner: u32 = row.get("owner");

        let storage = match storage_type.and_then(|b| std::char::from_u32(b as u32)) {
            Some('p') => Some(ColumnStorage::Plain),
            Some('e') => Some(ColumnStorage::External),
            Some('x') => Some(ColumnStorage::Extended),
            Some('m') => Some(ColumnStorage::Main),
            _ => None,
        };

        // More robust compression handling
        let compression = compression
            .and_then(|b| std::char::from_u32(b as u32))
            .map(|c| c.to_string());

        let column = Column {
            name: attr_name,
            type_name: attr_type,
            nullable: !is_not_null,
            default: default_expr,
            identity: None,         // Composite types don't have identity columns
            generated: None,        // Composite types don't have generated columns
            comment: class_comment, // Could be enhanced to get column comments if needed
            collation: collation_name,
            storage,
            compression,
        };

        let entry = grouped.entry((schema.clone(), name.clone())).or_insert((
            Vec::new(),
            type_comment,
            owner,
        ));
        entry.0.push(column);
    }

    let mut types = Vec::new();
    for ((schema, name), (attrs, comment, _owner)) in grouped {
        types.push(CompositeType {
            name,
            schema: Some(schema),
            values: vec![], // Composite types don't have enum values
            comment,
            attributes: attrs,
            definition: None, // Could be computed if needed
        });
    }

    Ok(types)
}

async fn introspect_domains<C: GenericClient>(client: &C) -> Result<Vec<Domain>>
where
    C: GenericClient + Sync,
{
    let query = r#"
        SELECT 
            t.typname AS domain_name,
            n.nspname AS domain_schema,
            bt.typname AS base_type,
            pg_catalog.format_type(t.typbasetype, t.typtypmod) AS formatted_base_type,
            pg_get_expr(t.typdefaultbin, 0) AS domain_default,
            c.conname AS constraint_name,
            pg_get_constraintdef(c.oid) AS check_clause,
            c.convalidated AS is_valid,
            t.typnotnull AS is_not_null,
            t.typowner AS owner,
            obj_description(t.oid, 'pg_type') AS domain_comment,
            t.typtypmod AS type_modifier
        FROM pg_type t
        JOIN pg_namespace n ON t.typnamespace = n.oid
        JOIN pg_type bt ON t.typbasetype = bt.oid
        LEFT JOIN pg_constraint c ON c.contypid = t.oid AND c.contype = 'c'
        WHERE t.typtype = 'd'
          AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
          AND t.typowner > 1
          AND NOT EXISTS (
              SELECT 1
              FROM pg_depend dep
              JOIN pg_extension e ON dep.refobjid = e.oid
              WHERE dep.objid = t.oid AND dep.deptype = 'e'
          )
        ORDER BY n.nspname, t.typname
    "#;

    let rows = client.query(query, &[]).await?;
    let mut domain_map = std::collections::HashMap::<(String, String), Domain>::new();

    for row in rows {
        let name: String = row.get("domain_name");
        let schema: String = row.get("domain_schema");
        let formatted_base_type: String = row.get("formatted_base_type");
        let default: Option<String> = row.get("domain_default");
        let check_clause: Option<String> = row.get("check_clause");
        let is_valid: Option<bool> = row.get("is_valid");
        let not_null: bool = row.get("is_not_null");
        let comment: Option<String> = row.get("domain_comment");

        let key = (schema.clone(), name.clone());

        let domain = domain_map.entry(key.clone()).or_insert(Domain {
            name: name.clone(),
            schema: Some(schema),
            base_type: formatted_base_type,
            constraints: vec![],
            default,
            not_null,
            comment,
        });

        if let Some(check) = check_clause {
            let constraint_name: Option<String> = row.get("constraint_name");
            domain.constraints.push(DomainConstraint {
                name: constraint_name,
                check,
                not_valid: is_valid == Some(false),
            });
        }
    }

    Ok(domain_map.into_values().collect())
}

async fn introspect_sequences<C: GenericClient>(client: &C) -> Result<Vec<Sequence>>
where
    C: GenericClient + Sync,
{
    let query = r#"
        WITH owned_info AS (
            SELECT
                dep.objid AS sequence_oid,
                n.nspname AS table_schema,
                c.relname AS table_name,
                a.attname AS column_name
            FROM pg_depend dep
            JOIN pg_class c ON dep.refobjid = c.oid
            JOIN pg_namespace n ON c.relnamespace = n.oid
            JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = dep.refobjsubid
            WHERE dep.deptype IN ('a', 'i')
        )
        SELECT 
            c.relname AS sequence_name,
            n.nspname AS sequence_schema,
            s.seqstart AS start_value,
            s.seqmin AS minimum_value,
            s.seqmax AS maximum_value,
            s.seqincrement AS increment,
            s.seqcache AS cache_value,
            s.seqcycle AS cycle_option,
            c.relowner AS owner,
            obj_description(c.oid, 'pg_class') AS sequence_comment,
            oi.table_schema,
            oi.table_name,
            oi.column_name
        FROM pg_class c
        JOIN pg_namespace n ON c.relnamespace = n.oid
        JOIN pg_sequence s ON s.seqrelid = c.oid
        LEFT JOIN owned_info oi ON oi.sequence_oid = c.oid
        WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
          AND c.relkind = 'S'
          AND c.relowner > 1
          AND NOT EXISTS (
              SELECT 1
              FROM pg_depend d
              JOIN pg_extension e ON d.refobjid = e.oid
              WHERE d.objid = c.oid AND d.deptype = 'e'
          )
        ORDER BY n.nspname, c.relname
    "#;

    let rows = client.query(query, &[]).await?;
    let mut sequences = Vec::new();

    for row in rows {
        let name: String = row.get("sequence_name");
        let schema: String = row.get("sequence_schema");
        let start: i64 = row.get("start_value");
        let min_value: i64 = row.get("minimum_value");
        let max_value: i64 = row.get("maximum_value");
        let increment: i64 = row.get("increment");
        let cache: i64 = row.get("cache_value");
        let cycle: bool = row.get("cycle_option");
        let comment: Option<String> = row.get("sequence_comment");

        let data_type = if min_value >= i16::MIN as i64 && max_value <= i16::MAX as i64 {
            "smallint"
        } else if min_value >= i32::MIN as i64 && max_value <= i32::MAX as i64 {
            "integer"
        } else {
            "bigint"
        };

        let owned_by = match (
            row.get::<_, Option<String>>("table_schema"),
            row.get::<_, Option<String>>("table_name"),
            row.get::<_, Option<String>>("column_name"),
        ) {
            (Some(schema), Some(table), Some(column)) => {
                Some(format!("{}.{}.{}", schema, table, column))
            }
            _ => None,
        };

        sequences.push(Sequence {
            name,
            schema: Some(schema),
            data_type: data_type.to_string(),
            start,
            increment,
            min_value: Some(min_value),
            max_value: Some(max_value),
            cache,
            cycle,
            owned_by,
            comment,
        });
    }

    Ok(sequences)
}

async fn introspect_extensions<C: GenericClient>(client: &C) -> Result<Vec<Extension>> {
    let query = r#"
        SELECT 
            e.oid,
            e.extname AS extension_name,
            e.extversion AS extension_version,
            n.nspname AS schema_name,
            obj_description(e.oid, 'pg_extension') AS comment,
            e.extname NOT IN (
                'plpgsql', 'pg_catalog', 'pg_trgm', 'pg_stat_statements',
                'pgstattuple', 'pg_buffercache', 'pg_prewarm',
                'pg_visibility', 'pg_freespacemap', 'pgrowlocks'
            ) AS is_user_extension
        FROM pg_extension e
        JOIN pg_namespace n ON e.extnamespace = n.oid
        WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
            AND n.nspname !~ '^pg_toast'
            AND n.nspname !~ '^pg_temp'
            AND e.extname NOT IN ('plpgsql');
    "#;

    let rows = client.query(query, &[]).await?;
    let mut extensions = Vec::new();

    for row in rows {
        let name: String = row.get("extension_name");
        let version: String = row.get("extension_version");
        let schema: Option<String> = row.get("schema_name");
        let comment: Option<String> = row.get("comment");

        extensions.push(Extension {
            name,
            version,
            schema,
            cascade: false, // TODO: Detect CASCADE from extension dependencies
            comment,
        });
    }

    Ok(extensions)
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

async fn introspect_collations<C: GenericClient>(client: &C) -> Result<Vec<Collation>>
where
    C: GenericClient + Sync,
{
    let query = r#"
        SELECT 
            c.collname AS collation_name,
            n.nspname AS schema_name,
            c.collcollate AS lc_collate,
            c.collctype AS lc_ctype,
            c.collprovider::text AS provider,
            c.collisdeterministic AS deterministic,
            c.collowner AS owner
        FROM pg_collation c
        JOIN pg_namespace n ON c.collnamespace = n.oid
        WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
          AND NOT EXISTS (
              SELECT 1 FROM pg_depend d
              JOIN pg_extension e ON d.refobjid = e.oid
              WHERE d.objid = c.oid AND d.deptype = 'e'
          )
    "#;

    let rows = client.query(query, &[]).await?;
    let mut collations = Vec::new();

    for row in rows {
        let name: String = row.get("collation_name");
        let schema: Option<String> = row.get("schema_name");
        let lc_collate: Option<String> = row.get("lc_collate");
        let lc_ctype: Option<String> = row.get("lc_ctype");
        let provider: String = row.get("provider");
        let deterministic: bool = row.get("deterministic");

        let provider_enum = match provider.as_str() {
            "libc" => CollationProvider::Libc,
            "icu" => CollationProvider::Icu,
            "builtin" => CollationProvider::Builtin,
            _ => CollationProvider::Libc, // fallback
        };

        // Use lc_collate as the primary locale, fallback to lc_ctype if needed
        let locale = lc_collate.clone().or(lc_ctype.clone());

        collations.push(Collation {
            name,
            schema,
            locale,
            lc_collate,
            lc_ctype,
            provider: provider_enum,
            deterministic,
        });
    }

    Ok(collations)
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

async fn introspect_range_types<C: GenericClient>(client: &C) -> Result<Vec<RangeType>>
where
    C: GenericClient + Sync,
{
    let query = r#"
    SELECT 
        t.typname AS type_name,
        n.nspname AS schema_name,
        r.rngsubtype AS subtype_oid,
        r.rngsubopc AS subtype_opclass_oid,
        r.rngcollation AS collation_oid,
        p1.proname AS canonical_function,
        p2.proname AS subtype_diff_function,
        t.typowner AS owner,
        obj_description(t.oid, 'pg_type') AS comment,
        pg_catalog.format_type(r.rngsubtype, NULL) AS subtype_name,  -- <-- Corrected here
        opc.opcname AS subtype_opclass_name,
        coll.collname AS collation_name
    FROM pg_type t
    JOIN pg_namespace n ON t.typnamespace = n.oid
    JOIN pg_range r ON t.oid = r.rngtypid
    LEFT JOIN pg_proc p1 ON p1.oid = r.rngcanonical
    LEFT JOIN pg_proc p2 ON p2.oid = r.rngsubdiff
    LEFT JOIN pg_opclass opc ON opc.oid = r.rngsubopc
    LEFT JOIN pg_collation coll ON coll.oid = r.rngcollation
    WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
    AND t.typowner > 1
    AND NOT EXISTS (
        SELECT 1 FROM pg_depend d
        JOIN pg_extension e ON d.refobjid = e.oid
        WHERE d.objid = t.oid AND d.deptype = 'e'
    )
    "#;

    let rows = client.query(query, &[]).await?;
    let mut range_types = Vec::new();

    for row in rows {
        let name: String = row.get("type_name");
        let schema: Option<String> = row.get("schema_name");
        let canonical = row.get::<_, Option<String>>("canonical_function");
        let subtype_diff = row.get::<_, Option<String>>("subtype_diff_function");
        let comment = row.get::<_, Option<String>>("comment");
        let subtype = row
            .get::<_, Option<String>>("subtype_name")
            .unwrap_or_else(|| "unknown".to_string());
        let subtype_opclass = row.get::<_, Option<String>>("subtype_opclass_name");
        let collation = row.get::<_, Option<String>>("collation_name");

        range_types.push(RangeType {
            name,
            schema,
            subtype,
            subtype_opclass,
            collation,
            canonical,
            subtype_diff,
            comment,
            multirange_type_name: None, // TODO: Add when needed
        });
    }

    Ok(range_types)
}

async fn introspect_enums<C: GenericClient>(client: &C) -> Result<Vec<EnumType>> {
    let query = r#"
        SELECT
            t.typname                                            AS name,
            n.nspname                                            AS schema,
            array_agg(e.enumlabel ORDER BY e.enumsortorder)      AS values,
            obj_description(t.oid, 'pg_type')                    AS comment          -- NEW
        FROM pg_type       t
        JOIN pg_enum       e ON e.enumtypid   = t.oid
        JOIN pg_namespace  n ON n.oid         = t.typnamespace
        WHERE t.typtype = 'e'
        AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
        -- exclude enums that belong to installed extensions
        AND NOT EXISTS (
                SELECT 1
                FROM pg_depend    d
                JOIN pg_extension x ON x.oid = d.refobjid
                WHERE d.objid = t.oid
                AND d.deptype = 'e'
        )
        GROUP BY t.typname, n.nspname, t.oid             -- t.oid needed for comment
        ORDER  BY n.nspname, t.typname;
    "#;

    let rows = client.query(query, &[]).await?;
    let mut enums = Vec::new();

    for row in rows {
        let name: String = row.get("name");
        let schema: Option<String> = row.get("schema");
        let values: Vec<String> = row.get("values");
        let comment: Option<String> = row.get("comment");

        enums.push(EnumType {
            name,
            schema,
            values,
            comment,
        });
    }

    Ok(enums)
}

// Missing introspection functions

async fn introspect_named_schemas<C: GenericClient>(client: &C) -> Result<Vec<NamedSchema>> {
    let query = r#"
        SELECT 
            n.nspname AS name,
            r.rolname AS owner,
            obj_description(n.oid, 'pg_namespace') AS comment
        FROM pg_namespace n
        LEFT JOIN pg_roles r ON n.nspowner = r.oid
        WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
        AND n.nspname NOT LIKE 'pg_%'
        AND n.nspowner > 1
        AND NOT EXISTS (
            SELECT 1 FROM pg_depend d
            JOIN pg_extension e ON d.refobjid = e.oid
            WHERE d.objid = n.oid AND d.deptype = 'e'
        )
        ORDER BY n.nspname
    "#;

    let rows = client.query(query, &[]).await?;
    let mut schemas = Vec::new();

    for row in rows {
        let name: String = row.get("name");
        let owner: Option<String> = row.get("owner");
        let comment: Option<String> = row.get("comment");

        schemas.push(NamedSchema {
            name,
            owner,
            comment,
        });
    }

    Ok(schemas)
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

async fn introspect_roles<C: GenericClient>(client: &C) -> Result<Vec<Role>> {
    let query = r#"
        SELECT 
            r.rolname AS name,
            r.rolsuper AS superuser,
            r.rolcreatedb AS createdb,
            r.rolcreaterole AS createrole,
            r.rolinherit AS inherit,
            r.rolcanlogin AS login,
            r.rolreplication AS replication,
            r.rolconnlimit AS connection_limit,
            r.rolvaliduntil::text AS valid_until
        FROM pg_roles r
        WHERE r.oid > 10  -- Default roles have OIDs <= 10
        AND NOT r.rolname LIKE 'pg\\_%'  -- Exclude all pg_* roles (note escaped underscore)
        AND r.rolname NOT IN (
            -- Explicitly exclude common default roles that might slip through
            'postgres',
            'pg_read_all_data',
            'pg_write_all_data',
            'pg_use_reserved_connections',
            'pg_read_server_files',
            'pg_write_server_files',
            'pg_read_all_settings',
            'pg_database_owner',
            'pg_execute_server_program',
            'pg_read_all_stats',
            'pg_monitor',
            'pg_checkpoint',
            'pg_create_subscription',
            'pg_stat_scan_tables',
            'pg_signal_backend'
        )
        AND NOT EXISTS (
            SELECT 1 FROM pg_depend d
            JOIN pg_extension e ON d.refobjid = e.oid
            WHERE d.objid = r.oid AND d.deptype = 'e'
        )
        ORDER BY r.rolname
    "#;

    let rows = client.query(query, &[]).await?;
    let mut roles = Vec::new();

    for row in rows {
        let name: String = row.get("name");
        let superuser: bool = row.get("superuser");
        let createdb: bool = row.get("createdb");
        let createrole: bool = row.get("createrole");
        let inherit: bool = row.get("inherit");
        let login: bool = row.get("login");
        let replication: bool = row.get("replication");
        let connection_limit: Option<i32> = row.get("connection_limit");
        let valid_until: Option<String> = row.get("valid_until");

        // Get member_of information
        let member_query = r#"
            SELECT m.rolname AS member_of
            FROM pg_auth_members am
            JOIN pg_roles m ON am.roleid = m.oid
            JOIN pg_roles r ON am.member = r.oid
            WHERE r.rolname = $1
            ORDER BY m.rolname
        "#;
        let member_rows = client.query(member_query, &[&name]).await?;
        let member_of: Vec<String> = member_rows
            .iter()
            .map(|row| row.get::<_, String>("member_of"))
            .collect();

        roles.push(Role {
            name,
            superuser,
            createdb,
            createrole,
            inherit,
            login,
            replication,
            connection_limit,
            password: None, // Password information is not accessible
            valid_until,
            member_of,
        });
    }

    Ok(roles)
}

async fn introspect_tablespaces<C: GenericClient>(client: &C) -> Result<Vec<Tablespace>> {
    let query = r#"
        SELECT 
            t.spcname AS name,
            pg_tablespace_location(t.oid) AS location,
            r.rolname AS owner,
            t.spcoptions AS options,
            obj_description(t.oid, 'pg_tablespace') AS comment
        FROM pg_tablespace t
        LEFT JOIN pg_roles r ON t.spcowner = r.oid
        WHERE t.spcname NOT IN ('pg_default', 'pg_global')
        AND t.spcowner > 1
        AND NOT EXISTS (
            SELECT 1 FROM pg_depend d
            JOIN pg_extension e ON d.refobjid = e.oid
            WHERE d.objid = t.oid AND d.deptype = 'e'
        )
        ORDER BY t.spcname
    "#;

    let rows = client.query(query, &[]).await?;
    let mut tablespaces = Vec::new();

    for row in rows {
        let name: String = row.get("name");
        let location: String = row.get("location");
        let owner: String = row.get("owner");
        let options: Option<Vec<String>> = row.get("options");
        let comment: Option<String> = row.get("comment");
        let options_map = options
            .as_deref()
            .map(parse_server_options)
            .unwrap_or_default();

        tablespaces.push(Tablespace {
            name,
            location,
            owner,
            options: options_map,
            comment,
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
        let columns = introspect_columns(client, &schema, &name).await?;

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

async fn introspect_base_types<C: GenericClient>(client: &C) -> Result<Vec<BaseType>>
where
    C: GenericClient + Sync,
{
    let query = r#"
        SELECT 
            t.typname AS name,
            n.nspname AS schema,
            t.typlen AS internal_length,
            t.typbyval AS is_passed_by_value,
            t.typalign::text AS alignment,
            t.typstorage::text AS storage,
            t.typcategory::text AS category,
            t.typispreferred AS preferred,
            t.typdefault AS default_value,
            t.typrelid AS element_oid,
            t.typdelim::text AS delimiter,
            false AS collatable,
            obj_description(t.oid, 'pg_type') AS comment
        FROM pg_type t
        JOIN pg_namespace n ON t.typnamespace = n.oid
        WHERE t.typtype = 'b'  -- base types only
        AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
        AND t.typowner > 1
        AND NOT EXISTS (
            SELECT 1 FROM pg_depend d
            JOIN pg_extension e ON d.refobjid = e.oid
            WHERE d.objid = t.oid AND d.deptype = 'e'
        )
        ORDER BY n.nspname, t.typname
    "#;

    let rows = client.query(query, &[]).await?;
    let mut base_types = Vec::new();

    for row in rows {
        let name: String = row.get("name");
        let schema: Option<String> = row.get("schema");
        let internal_length: i16 = row.get("internal_length");
        let is_passed_by_value: bool = row.get("is_passed_by_value");
        let alignment: String = row.get("alignment");
        let storage: String = row.get("storage");
        let category: Option<String> = row.get("category");
        let preferred: bool = row.get("preferred");
        let default_value: Option<String> = row.get("default_value");
        let element_oid: Option<u32> = row.get("element_oid");
        let delimiter: String = row.get("delimiter");
        let collatable: bool = row.get("collatable");
        let comment: Option<String> = row.get("comment");

        // Get element type name if available
        let element = if let Some(oid) = element_oid {
            let element_query = "SELECT typname FROM pg_type WHERE oid = $1";
            if let Ok(element_rows) = client.query(element_query, &[&oid]).await {
                element_rows.first().map(|row| row.get("typname"))
            } else {
                None
            }
        } else {
            None
        };

        base_types.push(BaseType {
            name,
            schema,
            internal_length: Some(internal_length as i32),
            is_passed_by_value,
            alignment,
            storage,
            category,
            preferred,
            default: default_value,
            element,
            delimiter: Some(delimiter),
            collatable,
            comment,
        });
    }

    Ok(base_types)
}

async fn introspect_array_types<C: GenericClient>(client: &C) -> Result<Vec<ArrayType>>
where
    C: GenericClient + Sync,
{
    let query = r#"
        SELECT 
            t.typname AS name,
            n.nspname AS schema,
            et.typname AS element_type,
            en.nspname AS element_schema,
            obj_description(t.oid, 'pg_type') AS comment
        FROM pg_type t
        JOIN pg_namespace n ON t.typnamespace = n.oid
        JOIN pg_type et ON t.typelem = et.oid
        JOIN pg_namespace en ON et.typnamespace = en.oid
        WHERE t.typtype = 'b'  -- base types
        AND t.typelem != 0     -- has element type (is array)
        AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
        AND t.typowner > 1
        AND NOT EXISTS (
            SELECT 1 FROM pg_depend d
            JOIN pg_extension e ON d.refobjid = e.oid
            WHERE d.objid = t.oid AND d.deptype = 'e'
        )
        ORDER BY n.nspname, t.typname
    "#;

    let rows = client.query(query, &[]).await?;
    let mut array_types = Vec::new();

    for row in rows {
        let name: String = row.get("name");
        let schema: Option<String> = row.get("schema");
        let element_type: String = row.get("element_type");
        let element_schema: Option<String> = row.get("element_schema");
        let comment: Option<String> = row.get("comment");

        array_types.push(ArrayType {
            name,
            schema,
            element_type,
            element_schema,
            comment,
        });
    }

    Ok(array_types)
}

async fn introspect_multirange_types<C: GenericClient>(client: &C) -> Result<Vec<MultirangeType>>
where
    C: GenericClient + Sync,
{
    let query = r#"
        SELECT 
            mrt.typname AS name,
            n.nspname AS schema,
            rt.typname AS range_type,
            rn.nspname AS range_schema,
            obj_description(mrt.oid, 'pg_type') AS comment
        FROM pg_range r
        JOIN pg_type rt ON r.rngtypid = rt.oid
        JOIN pg_namespace rn ON rt.typnamespace = rn.oid
        JOIN pg_type mrt ON r.rngmultitypid = mrt.oid
        JOIN pg_namespace n ON mrt.typnamespace = n.oid
        WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
        AND NOT EXISTS (
            SELECT 1 FROM pg_depend d
            JOIN pg_extension e ON d.refobjid = e.oid
            WHERE d.objid = mrt.oid AND d.deptype = 'e'
        )
        ORDER BY n.nspname, mrt.typname
    "#;

    let rows = client.query(query, &[]).await?;
    let mut multirange_types = Vec::new();

    for row in rows {
        let name: String = row.get("name");
        let schema: Option<String> = row.get("schema");
        let range_type: String = row.get("range_type");
        let range_schema: Option<String> = row.get("range_schema");
        let comment: Option<String> = row.get("comment");

        multirange_types.push(MultirangeType {
            name,
            schema,
            range_type,
            range_schema,
            comment,
        });
    }

    Ok(multirange_types)
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
