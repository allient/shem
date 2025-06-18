use shem_core::Result;
use shem_core::schema::*;
use tokio_postgres::GenericClient;
use std::collections::HashMap;

/// Introspect PostgreSQL database schema
pub async fn introspect_schema<C>(client: &C) -> Result<Schema>
where
    C: GenericClient + Sync,
{
    let mut schema = Schema::new();

    // Introspect tables
    let tables = introspect_tables(&*client).await?;
    for table in tables {
        schema.tables.insert(table.name.clone(), table);
    }

    // Introspect views
    let views = introspect_views(&*client).await?;
    for view in views {
        schema.views.insert(view.name.clone(), view);
    }

    // Introspect materialized views
    let materialized_views = introspect_materialized_views(&*client).await?;
    for view in materialized_views {
        schema.materialized_views.insert(view.name.clone(), view);
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

    // Introspect types (composite types, not enums)
    let composite_types = introspect_composite_types(&*client).await?;
    for type_def in composite_types {
        // Store composite types in a separate collection or handle them differently
        // For now, we'll skip them as they're not enums
    }

    // Introspect range types separately for detailed information
    let range_types = introspect_range_types(&*client).await?;
    for range_type in range_types {
        // Store range types in the types collection with a special prefix
        schema.range_types.insert(range_type.name.clone(), range_type);
    }

    // Introspect enums
    let enums = introspect_enums(&*client).await?;
    for enum_type in enums {
        schema.enums.insert(enum_type.name.clone(), enum_type);
    }

    // Introspect domains
    let domains = introspect_domains(&*client).await?;
    for domain in domains {
        schema.domains.insert(domain.name.clone(), domain);
    }

    // Introspect sequences
    let sequences = introspect_sequences(&*client).await?;
    for seq in sequences {
        schema.sequences.insert(seq.name.clone(), seq);
    }

    // Introspect extensions
    let extensions = introspect_extensions(&*client).await?;
    for ext in extensions {
        schema.extensions.insert(ext.name.clone(), ext);
    }

    // Introspect triggers
    let triggers = introspect_triggers(&*client).await?;
    for trigger in triggers {
        schema.triggers.insert(trigger.name.clone(), trigger);
    }

    // Introspect constraint triggers separately
    let constraint_triggers = introspect_constraint_triggers(&*client).await?;
    for trigger in constraint_triggers {
        schema.constraint_triggers.insert(trigger.name.clone(), trigger);
    }

    // Introspect event triggers
    let event_triggers = introspect_event_triggers(&*client).await?;
    for trigger in event_triggers {
        schema.event_triggers.insert(trigger.name.clone(), trigger);
    }

    // Introspect policies
    let policies = introspect_policies(&*client).await?;
    for policy in policies {
        schema.policies.insert(policy.name.clone(), policy);
    }

    // Introspect servers
    let servers = introspect_servers(&*client).await?;
    for server in servers {
        schema.servers.insert(server.name.clone(), server);
    }

    // Introspect collations
    let collations = introspect_collations(&*client).await?;
    for collation in collations {
        schema.collations.insert(collation.name.clone(), collation);
    }

    // Introspect rules
    let rules = introspect_rules(&*client).await?;
    for rule in rules {
        schema.rules.insert(rule.name.clone(), rule);
    }

    Ok(schema)
}

async fn introspect_tables<C: GenericClient>(client: &C) -> Result<Vec<Table>> {
    let query = r#"
        SELECT 
            t.table_schema,
            t.table_name,
            obj_description(pgc.oid, 'pg_class') as comment,
            pgc.relowner as owner
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

        // Get columns
        let columns = introspect_columns(client, &schema, &name).await?;

        // Get constraints
        let constraints = introspect_constraints(client, &schema, &name).await?;

        // Get indexes
        let indexes = introspect_indexes(client, &schema, &name).await?;

        tables.push(Table {
            name,
            schema,
            columns,
            constraints,
            indexes,
            comment,
            tablespace: None, // TODO: Get tablespace information
            inherits: Vec::new(), // TODO: Get inheritance information
            partition_by: None, // TODO: Get partitioning information
            storage_parameters: HashMap::new(), // TODO: Get storage parameters
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
            c.column_name,
            c.data_type,
            c.is_nullable = 'YES' as is_nullable,
            c.column_default,
            c.identity_generation,
            c.generation_expression
        FROM information_schema.columns c
        JOIN pg_class pgc ON pgc.relname = c.table_name
        WHERE c.table_schema = $1
        AND c.table_name = $2
        ORDER BY c.ordinal_position
    "#;

    let rows = client.query(query, &[schema, &table.to_string()]).await?;
    let mut columns = Vec::new();

    for row in rows {
        let name: String = row.get("column_name");
        let type_name: String = row.get("data_type");
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

        columns.push(Column {
            name,
            type_name,
            nullable,
            default,
            identity,
            generated,
            comment: None,
            collation: None, // TODO: Get column collation
            storage: None, // TODO: Get storage type
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
            tc.constraint_name,
            tc.constraint_type,
            kcu.column_name,
            ccu.table_schema AS foreign_table_schema,
            ccu.table_name AS foreign_table_name,
            ccu.column_name AS foreign_column_name,
            rc.update_rule,
            rc.delete_rule
        FROM information_schema.table_constraints tc
        LEFT JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        LEFT JOIN information_schema.referential_constraints rc
            ON tc.constraint_name = rc.constraint_name
            AND tc.table_schema = rc.constraint_schema
        LEFT JOIN information_schema.constraint_column_usage ccu
            ON rc.unique_constraint_name = ccu.constraint_name
            AND rc.constraint_schema = ccu.table_schema
        WHERE tc.table_schema = $1
        AND tc.table_name = $2
    "#;

    let rows = client.query(query, &[schema, &table.to_string()]).await?;
    let mut constraints = Vec::new();

    for row in rows {
        let name: String = row.get("constraint_name");
        let constraint_type: String = row.get("constraint_type");
        let kind = match constraint_type.as_str() {
            "PRIMARY KEY" => ConstraintKind::PrimaryKey,
            "FOREIGN KEY" => {
                let foreign_table = format!(
                    "{}.{}",
                    row.get::<_, String>("foreign_table_schema"),
                    row.get::<_, String>("foreign_table_name")
                );
                let update_rule = row.get::<_, Option<String>>("update_rule");
                let delete_rule = row.get::<_, Option<String>>("delete_rule");
                
                ConstraintKind::ForeignKey {
                    references: foreign_table,
                    on_delete: None, // TODO: Parse referential action
                    on_update: None, // TODO: Parse referential action
                }
            },
            "UNIQUE" => ConstraintKind::Unique,
            "CHECK" => ConstraintKind::Check,
            _ => continue,
        };

        let definition = match kind {
            ConstraintKind::PrimaryKey => {
                format!("PRIMARY KEY ({})", row.get::<_, String>("column_name"))
            }
            ConstraintKind::ForeignKey { ref references, .. } => {
                let foreign_column = row.get::<_, String>("foreign_column_name");
                let update_rule = row.get::<_, Option<String>>("update_rule");
                let delete_rule = row.get::<_, Option<String>>("delete_rule");
                format!(
                    "FOREIGN KEY ({}) REFERENCES {} ({}) ON UPDATE {} ON DELETE {}",
                    row.get::<_, String>("column_name"),
                    references,
                    foreign_column,
                    update_rule.unwrap_or_else(|| "NO ACTION".to_string()),
                    delete_rule.unwrap_or_else(|| "NO ACTION".to_string())
                )
            }
            ConstraintKind::Unique => format!("UNIQUE ({})", row.get::<_, String>("column_name")),
            ConstraintKind::Check => {
                // For check constraints, we need to get the check clause from a separate query
                let check_query = r#"
                    SELECT check_clause 
                    FROM information_schema.check_constraints 
                    WHERE constraint_name = $1 AND constraint_schema = $2
                "#;
                let check_rows = client.query(check_query, &[&name, schema]).await?;
                if let Some(check_row) = check_rows.first() {
                    check_row.get::<_, String>("check_clause")
                } else {
                    format!("CHECK (unknown)")
                }
            }
            _ => continue,
        };

        constraints.push(Constraint {
            name,
            kind,
            definition,
            deferrable: false, // TODO: Get deferrable information
            initially_deferred: false, // TODO: Get initially deferred information
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
            pg_get_indexdef(ix.indexrelid) as index_definition
        FROM pg_class t
        JOIN pg_index ix ON ix.indrelid = t.oid
        JOIN pg_class i ON i.oid = ix.indexrelid
        JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
        JOIN pg_am am ON am.oid = i.relam
        WHERE t.relname = $2
        AND t.relnamespace = (
            SELECT oid FROM pg_namespace WHERE nspname = $1
        )
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
        let definition: String = row.get("index_definition");

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
                    expression: None, // TODO: Get expression for functional indexes
                    order: SortOrder::Ascending, // TODO: Get actual sort order
                    nulls_first: false,          // TODO: Get actual nulls order
                    opclass: None, // TODO: Get operator class
                }],
                unique: is_unique,
                method: index_method,
                where_clause,
                tablespace: None, // TODO: Get tablespace
                storage_parameters: HashMap::new(), // TODO: Get storage parameters
            });
        } else if let Some(idx) = &mut current_index {
            idx.columns.push(IndexColumn {
                name: column_name,
                expression: None, // TODO: Get expression for functional indexes
                order: SortOrder::Ascending, // TODO: Get actual sort order
                nulls_first: false,          // TODO: Get actual nulls order
                opclass: None, // TODO: Get operator class
            });
        }
    }

    if let Some(idx) = current_index {
        indexes.push(idx);
    }

    Ok(indexes)
}

// TODO: Implement remaining introspection functions
async fn introspect_views<C: GenericClient>(client: &C) -> Result<Vec<View>> {
    let query = r#"
        SELECT 
            v.table_schema,
            v.table_name,
            v.view_definition,
            v.check_option,
            pgc.relowner as owner
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

        let check_option_enum = match check_option.as_deref() {
            Some("LOCAL") => CheckOption::Local,
            Some("CASCADED") => CheckOption::Cascaded,
            _ => CheckOption::None,
        };

        views.push(View {
            name,
            schema,
            definition,
            check_option: check_option_enum,
            comment: None,
            security_barrier: false, // TODO: Get security barrier information
            columns: Vec::new(), // TODO: Get explicit column list
        });
    }

    Ok(views)
}

async fn introspect_materialized_views<C: GenericClient>(
    client: &C,
) -> Result<Vec<MaterializedView>> {
    let query = r#"
        SELECT 
            schemaname,
            matviewname,
            definition
        FROM pg_matviews
        WHERE schemaname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
    "#;

    let rows = client.query(query, &[]).await?;
    let mut views = Vec::new();

    for row in rows {
        let schema: Option<String> = row.get("schemaname");
        let name: String = row.get("matviewname");
        let definition: String = row.get("definition");

        views.push(MaterializedView {
            name,
            schema,
            definition,
            check_option: CheckOption::None, // Materialized views don't have check options
            comment: None,
            tablespace: None, // TODO: Get tablespace information
            storage_parameters: HashMap::new(), // TODO: Get storage parameters
            indexes: Vec::new(), // TODO: Get materialized view indexes
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
            p.prokind as kind
        FROM pg_proc p
        JOIN pg_namespace n ON p.pronamespace = n.oid
        JOIN pg_language l ON p.prolang = l.oid
        WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
        AND p.prokind = 'f'  -- functions only, not procedures
        AND NOT p.proisagg  -- exclude aggregates
        AND NOT p.proiswindow  -- exclude window functions
        AND p.proowner > 1  -- exclude system-owned functions (owner 1 is usually postgres superuser)
        AND NOT EXISTS (
            -- Exclude functions that are part of extensions
            SELECT 1 FROM pg_depend d
            JOIN pg_extension e ON d.refobjid = e.oid
            WHERE d.objid = p.oid AND d.deptype = 'e'
        )
        AND NOT EXISTS (
            -- Exclude internal functions (those with no source or C language functions)
            SELECT 1 WHERE p.prosrc IS NULL OR p.prosrc = '' OR l.lanname = 'c'
        )
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

        functions.push(Function {
            name,
            schema,
            parameters,
            returns,
            language,
            definition,
            comment: None,
            volatility: Volatility::Volatile, // TODO: Get volatility information
            strict: false, // TODO: Get strict information
            security_definer: false, // TODO: Get security definer information
            parallel_safety: ParallelSafety::Unsafe, // TODO: Get parallel safety information
            cost: None, // TODO: Get cost information
            rows: None, // TODO: Get rows information
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
            p.proowner as owner
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

        // Parse parameters from the arguments string
        let parameters = parse_function_parameters(&arguments);

        procedures.push(Procedure {
            name,
            schema,
            parameters,
            language,
            definition,
            comment: None,
            security_definer: false, // TODO: Get security definer information
        });
    }

    Ok(procedures)
}

async fn introspect_composite_types<C: GenericClient>(client: &C) -> Result<Vec<Type>> {
    let query = r#"
        SELECT 
            t.typname as name,
            n.nspname as schema,
            string_agg(att.attname || ' ' || pg_catalog.format_type(att.atttypid, att.atttypmod), ', ' ORDER BY att.attnum) as definition
        FROM pg_type t
        JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
        JOIN pg_catalog.pg_class c ON c.relname = t.typname
        JOIN pg_catalog.pg_attribute att ON att.attrelid = c.oid
        WHERE t.typtype = 'c'
        AND att.attnum > 0
        AND NOT att.attisdropped
        GROUP BY t.typname, n.nspname
        ORDER BY n.nspname, t.typname
    "#;

    let rows = client.query(query, &[]).await?;
    let mut types = Vec::new();

    for row in rows {
        let name: String = row.get("name");
        let schema: Option<String> = row.get("schema");
        let definition: Option<String> = row.get("definition");

        types.push(Type {
            name,
            schema,
            kind: TypeKind::Composite { attributes: Vec::new() }, // TODO: Parse attributes
            comment: None,
            definition,
        });
    }

    Ok(types)
}

async fn introspect_domains<C: GenericClient>(client: &C) -> Result<Vec<Domain>> {
    let query = r#"
        SELECT 
            d.domain_name,
            d.domain_schema,
            d.data_type,
            d.domain_default,
            c.check_clause,
            t.typowner as owner
        FROM information_schema.domains d
        JOIN pg_type t ON t.typname = d.domain_name
        JOIN pg_namespace n ON t.typnamespace = n.oid AND n.nspname = d.domain_schema
        LEFT JOIN information_schema.check_constraints c
            ON d.domain_name = c.constraint_name
            AND d.domain_schema = c.constraint_schema
        WHERE d.domain_schema NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
        AND t.typowner > 1  -- exclude system-owned domains
        AND NOT EXISTS (
            -- Exclude domains that are part of extensions
            SELECT 1 FROM pg_depend dep
            JOIN pg_extension e ON dep.refobjid = e.oid
            WHERE dep.objid = t.oid AND dep.deptype = 'e'
        )
    "#;

    let rows = client.query(query, &[]).await?;
    let mut domains = Vec::new();

    for row in rows {
        let name: String = row.get("domain_name");
        let schema: Option<String> = row.get("domain_schema");
        let base_type: String = row.get("data_type");
        let default: Option<String> = row.get("domain_default");
        let check_clause: Option<String> = row.get("check_clause");

        let mut constraints = Vec::new();

        if let Some(check) = &check_clause {
            constraints.push(DomainConstraint {
                name: None,
                check: check.clone(),
                not_valid: false, // TODO: Get NOT VALID information
            });
        }

        domains.push(Domain {
            name,
            schema,
            base_type,
            constraints,
            default,
            not_null: false, // TODO: Get NOT NULL information
            comment: None,
        });
    }

    Ok(domains)
}

async fn introspect_sequences<C: GenericClient>(client: &C) -> Result<Vec<Sequence>> {
    let query = r#"
        SELECT 
            s.sequence_name,
            s.sequence_schema,
            s.start_value,
            s.minimum_value,
            s.maximum_value,
            s.increment,
            s.cycle_option,
            seq.relowner as owner
        FROM information_schema.sequences s
        JOIN pg_class seq ON seq.relname = s.sequence_name
        JOIN pg_namespace n ON seq.relnamespace = n.oid AND n.nspname = s.sequence_schema
        WHERE s.sequence_schema NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
        AND seq.relowner > 1  -- exclude system-owned sequences
        AND NOT EXISTS (
            -- Exclude sequences that are part of extensions
            SELECT 1 FROM pg_depend d
            JOIN pg_extension e ON d.refobjid = e.oid
            WHERE d.objid = seq.oid AND d.deptype = 'e'
        )
    "#;

    let rows = client.query(query, &[]).await?;
    let mut sequences = Vec::new();

    for row in rows {
        let name: String = row.get("sequence_name");
        let schema: Option<String> = row.get("sequence_schema");
        let start_str: String = row.get("start_value");
        let min_str: Option<String> = row.get("minimum_value");
        let max_str: Option<String> = row.get("maximum_value");
        let increment_str: String = row.get("increment");
        let cycle_option: String = row.get("cycle_option");

        // Parse string values to i64
        let start: i64 = start_str.parse().unwrap_or(1);
        let min_value: Option<i64> = min_str.and_then(|s| s.parse().ok());
        let max_value: Option<i64> = max_str.and_then(|s| s.parse().ok());
        let increment: i64 = increment_str.parse().unwrap_or(1);

        sequences.push(Sequence {
            name,
            schema,
            data_type: "bigint".to_string(), // TODO: Get actual data type
            start,
            increment,
            min_value,
            max_value,
            cache: 1, // Default cache value
            cycle: cycle_option == "YES",
            owned_by: None, // TODO: Get owned by information
            comment: None,
        });
    }

    Ok(sequences)
}

async fn introspect_extensions<C: GenericClient>(client: &C) -> Result<Vec<Extension>> {
    let query = r#"
        SELECT 
            extname as extension_name,
            extversion as extension_version
        FROM pg_extension
    "#;

    let rows = client.query(query, &[]).await?;
    let mut extensions = Vec::new();

    for row in rows {
        let name: String = row.get("extension_name");
        let version: String = row.get("extension_version");

        extensions.push(Extension {
            name,
            schema: None, // Extensions don't have a specific schema
            version,
            cascade: false, // TODO: Get cascade information
            comment: None,
        });
    }

    Ok(extensions)
}

async fn introspect_triggers<C: GenericClient>(client: &C) -> Result<Vec<Trigger>> {
    let query = r#"
        SELECT 
            t.tgname as trigger_name,
            c.relname as table_name,
            p.proname as function_name,
            t.tgtype as trigger_type,
            t.tgargs as trigger_arguments,
            t.tgconstraint as is_constraint_trigger,
            c.relowner as owner
        FROM pg_trigger t
        JOIN pg_class c ON t.tgrelid = c.oid
        JOIN pg_proc p ON t.tgfoid = p.oid
        JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
        AND NOT t.tgisinternal  -- Exclude internal triggers
        AND c.relowner > 1  -- exclude system-owned tables
        AND NOT EXISTS (
            -- Exclude triggers on tables that are part of extensions
            SELECT 1 FROM pg_depend d
            JOIN pg_extension e ON d.refobjid = e.oid
            WHERE d.objid = c.oid AND d.deptype = 'e'
        )
    "#;

    let rows = client.query(query, &[]).await?;
    let mut triggers = Vec::new();

    for row in rows {
        let name: String = row.get("trigger_name");
        let table: String = row.get("table_name");
        let function: String = row.get("function_name");
        let trigger_type: i16 = row.get("trigger_type");
        let arguments: Option<Vec<u8>> = row.get("trigger_arguments");
        let is_constraint_trigger: Option<u32> = row.get("is_constraint_trigger");

        // Skip constraint triggers as they are handled by constraints
        if is_constraint_trigger.is_some() {
            continue;
        }

        // Parse trigger type to determine timing and events
        let (timing, events) = parse_trigger_type(trigger_type);

        // Parse arguments
        let args = if let Some(arg_bytes) = arguments {
            parse_trigger_arguments(&arg_bytes)
        } else {
            Vec::new()
        };

        triggers.push(Trigger {
            name,
            table,
            schema: None,
            timing,
            events,
            function,
            arguments: args,
            condition: None, // TODO: Get WHEN condition
            for_each: TriggerLevel::Row, // TODO: Get FOR EACH information
            comment: None,
        });
    }

    Ok(triggers)
}

async fn introspect_policies<C: GenericClient>(client: &C) -> Result<Vec<Policy>> {
    let query = r#"
        SELECT 
            p.polname as policy_name,
            c.relname as table_name,
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
        let permissive: bool = row.get("permissive");
        let roles: Vec<u32> = row.get("roles");
        let command: String = row.get("command");
        let using_expr: Option<String> = row.get("using_expression");
        let check_expr: Option<String> = row.get("check_expression");

        // Convert role OIDs to role names (simplified)
        let role_names = roles.iter().map(|&oid| oid.to_string()).collect();

        // Parse command to PolicyCommand enum
        let policy_command = match command.as_str() {
            "ALL" => PolicyCommand::All,
            "SELECT" => PolicyCommand::Select,
            "INSERT" => PolicyCommand::Insert,
            "UPDATE" => PolicyCommand::Update,
            "DELETE" => PolicyCommand::Delete,
            _ => PolicyCommand::All, // Default fallback
        };

        policies.push(Policy {
            name,
            table,
            schema: None, // TODO: Get schema information
            command: policy_command,
            permissive,
            roles: role_names,
            using: using_expr,
            check: check_expr,
        });
    }

    Ok(policies)
}

async fn introspect_servers<C: GenericClient>(client: &C) -> Result<Vec<Server>> {
    let query = r#"
        SELECT 
            srvname as server_name,
            fdwname as foreign_data_wrapper_name,
            srvoptions as server_options
        FROM pg_foreign_server s
        JOIN pg_foreign_data_wrapper f ON s.srvfdw = f.oid
    "#;

    let rows = client.query(query, &[]).await?;
    let mut servers = Vec::new();

    for row in rows {
        let name: String = row.get("server_name");
        let foreign_data_wrapper: String = row.get("foreign_data_wrapper_name");
        let options: Option<Vec<String>> = row.get("server_options");

        let options_map = if let Some(opt_array) = options {
            parse_server_options(&opt_array)
        } else {
            HashMap::new()
        };

        servers.push(Server {
            name,
            foreign_data_wrapper,
            options: options_map,
            version: None, // TODO: Get server version
        });
    }

    Ok(servers)
}

async fn introspect_event_triggers<C: GenericClient>(client: &C) -> Result<Vec<EventTrigger>> {
    let query = r#"
        SELECT 
            evtname as trigger_name,
            evtevent as event,
            evtowner as owner,
            evtfoid as function_oid,
            evtenabled::text as enabled,
            evttags as tags
        FROM pg_event_trigger
    "#;

    let rows = client.query(query, &[]).await?;
    let mut event_triggers = Vec::new();

    for row in rows {
        let name: String = row.get("trigger_name");
        let event: String = row.get("event");
        let owner: u32 = row.get("owner");
        let function_oid: u32 = row.get("function_oid");
        let enabled_str: String = row.get("enabled");
        let enabled = enabled_str.chars().next() == Some('O');
        let tags: Option<Vec<String>> = row.get("tags");

        // Get function name from OID
        let func_query = "SELECT proname FROM pg_proc WHERE oid = $1";
        let func_rows = client.query(func_query, &[&function_oid]).await?;
        let function_name = if let Some(func_row) = func_rows.first() {
            func_row.get::<_, String>("proname")
        } else {
            "unknown_function".to_string()
        };

        // Parse event to EventTriggerEvent enum
        let event_enum = match event.as_str() {
            "ddl_command_start" => EventTriggerEvent::DdlCommandStart,
            "ddl_command_end" => EventTriggerEvent::DdlCommandEnd,
            "table_rewrite" => EventTriggerEvent::TableRewrite,
            "sql_drop" => EventTriggerEvent::SqlDrop,
            _ => EventTriggerEvent::DdlCommandStart, // Default fallback
        };

        event_triggers.push(EventTrigger {
            name,
            event: event_enum,
            function: function_name,
            enabled,
            tags: tags.unwrap_or_default(),
            condition: None, // TODO: Get WHEN condition
        });
    }

    Ok(event_triggers)
}

async fn introspect_collations<C: GenericClient>(client: &C) -> Result<Vec<Collation>> {
    let query = r#"
        SELECT 
            c.collname as collation_name,
            n.nspname as schema_name,
            c.collcollate as locale,
            c.collctype as ctype,
            c.collprovider::text as provider,
            c.collowner as owner
        FROM pg_collation c
        JOIN pg_namespace n ON c.collnamespace = n.oid
        WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
        AND c.collowner > 1  -- exclude system-owned collations
        AND NOT EXISTS (
            -- Exclude collations that are part of extensions
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
        let locale: Option<String> = row.get("locale");
        let ctype: Option<String> = row.get("ctype");
        let provider: String = row.get("provider");

        // Parse provider to CollationProvider enum
        let provider_enum = match provider.as_str() {
            "libc" => CollationProvider::Libc,
            "icu" => CollationProvider::Icu,
            _ => CollationProvider::Libc, // Default fallback
        };

        collations.push(Collation {
            name,
            schema,
            locale: locale.clone(),
            lc_collate: locale.clone(),
            lc_ctype: ctype.clone(),
            provider: provider_enum,
            deterministic: true, // TODO: Get deterministic information
        });
    }

    Ok(collations)
}

async fn introspect_rules<C: GenericClient>(client: &C) -> Result<Vec<Rule>> {
    let query = r#"
        SELECT 
            r.rulename as rule_name,
            c.relname as table_name,
            n.nspname as schema_name,
            r.ev_type as event_type,
            r.is_instead as is_instead,
            pg_get_ruledef(r.oid) as rule_definition,
            c.relowner as owner
        FROM pg_rewrite r
        JOIN pg_class c ON r.ev_class = c.oid
        JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
        AND r.rulename != '_RETURN'  -- Exclude default rules
        AND c.relowner > 1  -- exclude system-owned tables
        AND NOT EXISTS (
            -- Exclude rules on tables that are part of extensions
            SELECT 1 FROM pg_depend d
            JOIN pg_extension e ON d.refobjid = e.oid
            WHERE d.objid = c.oid AND d.deptype = 'e'
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

        // Parse event type
        let event = match event_type.as_str() {
            "1" => RuleEvent::Select,
            "2" => RuleEvent::Update,
            "3" => RuleEvent::Insert,
            "4" => RuleEvent::Delete,
            _ => RuleEvent::Select, // Default fallback
        };

        rules.push(Rule {
            name,
            table,
            schema,
            event,
            instead: is_instead,
            condition: None, // TODO: Get WHERE condition
            actions: vec![definition], // TODO: Parse actions properly
        });
    }

    Ok(rules)
}

async fn introspect_constraint_triggers<C: GenericClient>(client: &C) -> Result<Vec<ConstraintTrigger>> {
    let query = r#"
        SELECT 
            t.tgname as trigger_name,
            c.relname as table_name,
            n.nspname as schema_name,
            p.proname as function_name,
            t.tgtype as trigger_type,
            t.tgargs as trigger_arguments,
            t.tgconstraint as constraint_oid,
            c.relowner as owner
        FROM pg_trigger t
        JOIN pg_class c ON t.tgrelid = c.oid
        JOIN pg_namespace n ON c.relnamespace = n.oid
        JOIN pg_proc p ON t.tgfoid = p.oid
        WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
        AND NOT t.tgisinternal
        AND t.tgconstraint IS NOT NULL
        AND c.relowner > 1  -- exclude system-owned tables
        AND NOT EXISTS (
            -- Exclude constraint triggers on tables that are part of extensions
            SELECT 1 FROM pg_depend d
            JOIN pg_extension e ON d.refobjid = e.oid
            WHERE d.objid = c.oid AND d.deptype = 'e'
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

        // Parse trigger type to determine timing and events
        let (timing, events) = parse_trigger_type(trigger_type);

        // Parse arguments
        let args = if let Some(arg_bytes) = arguments {
            parse_trigger_arguments(&arg_bytes)
        } else {
            Vec::new()
        };

        // Get constraint name from OID
        let constraint_query = "SELECT conname FROM pg_constraint WHERE oid = $1";
        let constraint_rows = client.query(constraint_query, &[&constraint_oid]).await?;
        let constraint_name = if let Some(constraint_row) = constraint_rows.first() {
            constraint_row.get::<_, String>("conname")
        } else {
            "unknown_constraint".to_string()
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
            deferrable: false, // TODO: Get deferrable information
            initially_deferred: false, // TODO: Get initially deferred information
        });
    }

    Ok(constraint_triggers)
}

// Helper functions for parsing

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

        // Simple parsing - in a real implementation, you'd want more sophisticated parsing
        let mut param_parts: Vec<&str> = trimmed.split_whitespace().collect();

        if param_parts.len() >= 2 {
            let name = param_parts[0].to_string();
            let type_name = param_parts[1].to_string();

            parameters.push(Parameter {
                name,
                type_name,
                mode: ParameterMode::In, // Default to IN
                default: None,
            });
        }
    }

    parameters
}

fn parse_trigger_type(trigger_type: i16) -> (TriggerTiming, Vec<TriggerEvent>) {
    let mut timing = TriggerTiming::Before;
    let mut events = Vec::new();

    // Parse trigger type bits
    if (trigger_type & 66) != 0 {
        // TG_AFTER
        timing = TriggerTiming::After;
    } else if (trigger_type & 64) != 0 {
        // TG_INSTEAD
        timing = TriggerTiming::InsteadOf;
    }

    if (trigger_type & 1) != 0 {
        // TG_INSERT
        events.push(TriggerEvent::Insert);
    }
    if (trigger_type & 2) != 0 {
        // TG_DELETE
        events.push(TriggerEvent::Delete);
    }
    if (trigger_type & 4) != 0 {
        // TG_UPDATE
        events.push(TriggerEvent::Update { columns: None });
    }
    if (trigger_type & 8) != 0 {
        // TG_TRUNCATE
        events.push(TriggerEvent::Truncate);
    }

    (timing, events)
}

fn parse_trigger_arguments(arg_bytes: &[u8]) -> Vec<String> {
    // Convert bytes to strings - this is a simplified implementation
    // In a real implementation, you'd need to properly parse the argument format
    if arg_bytes.is_empty() {
        return Vec::new();
    }

    // Simple conversion - split by null bytes and convert to strings
    let args_str = String::from_utf8_lossy(arg_bytes);
    args_str
        .split('\0')
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect()
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

async fn introspect_range_types<C: GenericClient>(client: &C) -> Result<Vec<RangeType>> {
    let query = r#"
        SELECT 
            t.typname as type_name,
            n.nspname as schema_name,
            r.rngsubtype as subtype_oid,
            r.rngsubopc as subtype_opclass_oid,
            r.rngcollation as collation_oid,
            r.rngcanonical as canonical_oid,
            r.rngsubdiff as subtype_diff_oid,
            t.typowner as owner
        FROM pg_type t
        JOIN pg_namespace n ON t.typnamespace = n.oid
        JOIN pg_range r ON t.oid = r.rngtypid
        WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
        AND t.typowner > 1  -- exclude system-owned types
        AND NOT EXISTS (
            -- Exclude range types that are part of extensions
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
        let subtype_oid: u32 = row.get("subtype_oid");
        let subtype_opclass_oid: Option<u32> = row.get("subtype_opclass_oid");
        let collation_oid: Option<u32> = row.get("collation_oid");
        let canonical_oid: Option<u32> = row.get("canonical_oid");
        let subtype_diff_oid: Option<u32> = row.get("subtype_diff_oid");

        // Get subtype name
        let subtype_query = "SELECT typname FROM pg_type WHERE oid = $1";
        let subtype_rows = client.query(subtype_query, &[&subtype_oid]).await?;
        let subtype = if let Some(subtype_row) = subtype_rows.first() {
            subtype_row.get::<_, String>("typname")
        } else {
            "unknown".to_string()
        };

        // Get opclass name if available
        let subtype_opclass = if let Some(opclass_oid) = subtype_opclass_oid {
            let opclass_query = "SELECT opcname FROM pg_opclass WHERE oid = $1";
            let opclass_rows = client.query(opclass_query, &[&opclass_oid]).await?;
            if let Some(opclass_row) = opclass_rows.first() {
                Some(opclass_row.get::<_, String>("opcname"))
            } else {
                None
            }
        } else {
            None
        };

        // Get collation name if available
        let collation = if let Some(coll_oid) = collation_oid {
            let coll_query = "SELECT collname FROM pg_collation WHERE oid = $1";
            let coll_rows = client.query(coll_query, &[&coll_oid]).await?;
            if let Some(coll_row) = coll_rows.first() {
                Some(coll_row.get::<_, String>("collname"))
            } else {
                None
            }
        } else {
            None
        };

        // Get canonical function name if available
        let canonical = if let Some(canon_oid) = canonical_oid {
            let canon_query = "SELECT proname FROM pg_proc WHERE oid = $1";
            let canon_rows = client.query(canon_query, &[&canon_oid]).await?;
            if let Some(canon_row) = canon_rows.first() {
                Some(canon_row.get::<_, String>("proname"))
            } else {
                None
            }
        } else {
            None
        };

        // Get subtype diff function name if available
        let subtype_diff = if let Some(diff_oid) = subtype_diff_oid {
            let diff_query = "SELECT proname FROM pg_proc WHERE oid = $1";
            let diff_rows = client.query(diff_query, &[&diff_oid]).await?;
            if let Some(diff_row) = diff_rows.first() {
                Some(diff_row.get::<_, String>("proname"))
            } else {
                None
            }
        } else {
            None
        };

        range_types.push(RangeType {
            name,
            schema,
            subtype,
            subtype_opclass,
            collation,
            canonical,
            subtype_diff,
            multirange_type_name: None, // TODO: Get multirange type name
        });
    }

    Ok(range_types)
}

async fn introspect_enums<C: GenericClient>(client: &C) -> Result<Vec<EnumType>> {
    let query = r#"
        SELECT 
            t.typname as name,
            n.nspname as schema,
            array_agg(e.enumlabel ORDER BY e.enumsortorder) as values
        FROM pg_type t
        JOIN pg_enum e ON t.oid = e.enumtypid
        JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
        WHERE t.typtype = 'e'
        GROUP BY t.typname, n.nspname
        ORDER BY n.nspname, t.typname
    "#;

    let rows = client.query(query, &[]).await?;
    let mut enums = Vec::new();

    for row in rows {
        let name: String = row.get("name");
        let schema: Option<String> = row.get("schema");
        let values: Vec<String> = row.get("values");

        enums.push(EnumType {
            name,
            schema,
            values,
            comment: None, // TODO: Get enum comment
        });
    }

    Ok(enums)
}
