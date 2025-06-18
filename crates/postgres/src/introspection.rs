use shem_core::Result;
use shem_core::schema::*;
use tokio_postgres::GenericClient;

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

    // Introspect types
    let types = introspect_types(&*client).await?;
    for type_def in types {
        schema.types.insert(type_def.name.clone(), type_def);
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

    Ok(schema)
}

async fn introspect_tables<C: GenericClient>(client: &C) -> Result<Vec<Table>> {
    let query = r#"
        SELECT 
            t.table_schema,
            t.table_name,
            obj_description(pgc.oid, 'pg_class') as comment
        FROM information_schema.tables t
        JOIN pg_class pgc ON pgc.relname = t.table_name
        WHERE t.table_schema NOT IN ('pg_catalog', 'information_schema')
        AND t.table_type = 'BASE TABLE'
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
            c.generation_expression,
            col_description(pgc.oid, c.ordinal_position) as comment
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
            }),
            Some(identity_type) if identity_type == "BY DEFAULT" => Some(Identity {
                always: false,
                start: 1,
                increment: 1,
                min_value: None,
                max_value: None,
            }),
            _ => None,
        };
        let generated: Option<GeneratedColumn> = row
            .get::<_, Option<String>>("generation_expression")
            .map(|expr| GeneratedColumn {
                expression: expr,
                stored: true,
            });
        let comment: Option<String> = row.get("comment");

        columns.push(Column {
            name,
            type_name,
            nullable,
            default,
            identity,
            generated,
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
            "FOREIGN KEY" => ConstraintKind::ForeignKey,
            "UNIQUE" => ConstraintKind::Unique,
            "CHECK" => ConstraintKind::Check,
            _ => continue,
        };

        let definition = match kind {
            ConstraintKind::PrimaryKey => {
                format!("PRIMARY KEY ({})", row.get::<_, String>("column_name"))
            }
            ConstraintKind::ForeignKey => {
                let foreign_table = format!(
                    "{}.{}",
                    row.get::<_, String>("foreign_table_schema"),
                    row.get::<_, String>("foreign_table_name")
                );
                let foreign_column = row.get::<_, String>("foreign_column_name");
                let update_rule = row.get::<_, Option<String>>("update_rule");
                let delete_rule = row.get::<_, Option<String>>("delete_rule");
                format!(
                    "FOREIGN KEY ({}) REFERENCES {} ({}) ON UPDATE {} ON DELETE {}",
                    row.get::<_, String>("column_name"),
                    foreign_table,
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
                    order: SortOrder::Ascending, // TODO: Get actual sort order
                    nulls_first: false,          // TODO: Get actual nulls order
                }],
                unique: is_unique,
                method,
            });
        } else if let Some(idx) = &mut current_index {
            idx.columns.push(IndexColumn {
                name: column_name,
                order: SortOrder::Ascending, // TODO: Get actual sort order
                nulls_first: false,          // TODO: Get actual nulls order
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
            v.check_option
        FROM information_schema.views v
        WHERE v.table_schema NOT IN ('pg_catalog', 'information_schema')
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
        WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
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
            pg_get_function_arguments(p.oid) as arguments
        FROM pg_proc p
        JOIN pg_namespace n ON p.pronamespace = n.oid
        JOIN pg_language l ON p.prolang = l.oid
        WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
        AND p.prokind = 'f'  -- functions only, not procedures
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
            pg_get_function_arguments(p.oid) as arguments
        FROM pg_proc p
        JOIN pg_namespace n ON p.pronamespace = n.oid
        JOIN pg_language l ON p.prolang = l.oid
        WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
        AND p.prokind = 'p'  -- procedures only
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
        });
    }

    Ok(procedures)
}

async fn introspect_types<C: GenericClient>(client: &C) -> Result<Vec<Type>> {
    let query = r#"
        SELECT 
            t.typname as type_name,
            n.nspname as schema_name,
            t.typtype as type_kind
        FROM pg_type t
        JOIN pg_namespace n ON t.typnamespace = n.oid
        WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
        AND t.typtype IN ('c', 'e', 'r')  -- composite, enum, range
    "#;

    let rows = client.query(query, &[]).await?;
    let mut types = Vec::new();

    for row in rows {
        let name: String = row.get("type_name");
        let schema: Option<String> = row.get("schema_name");
        let type_kind: i8 = row.get("type_kind");

        let kind = match type_kind as u8 as char {
            'c' => TypeKind::Composite,
            'e' => TypeKind::Enum,
            'r' => TypeKind::Range,
            _ => TypeKind::Base,
        };

        types.push(Type { name, schema, kind });
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
            c.check_clause
        FROM information_schema.domains d
        LEFT JOIN information_schema.check_constraints c
            ON d.domain_name = c.constraint_name
            AND d.domain_schema = c.constraint_schema
        WHERE d.domain_schema NOT IN ('pg_catalog', 'information_schema')
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

        if let Some(default_val) = default {
            constraints.push(format!("DEFAULT {}", default_val));
        }

        if let Some(check) = &check_clause {
            if check.contains("NOT NULL") {
                constraints.push("NOT NULL".to_string());
            }
            constraints.push(check.clone());
        }

        domains.push(Domain {
            name,
            schema,
            base_type,
            constraints,
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
            s.cycle_option
        FROM information_schema.sequences s
        WHERE s.sequence_schema NOT IN ('pg_catalog', 'information_schema')
    "#;

    let rows = client.query(query, &[]).await?;
    let mut sequences = Vec::new();

    for row in rows {
        let name: String = row.get("sequence_name");
        let schema: Option<String> = row.get("sequence_schema");
        let start: i64 = row.get("start_value");
        let min_value: Option<i64> = row.get("minimum_value");
        let max_value: Option<i64> = row.get("maximum_value");
        let increment: i64 = row.get("increment");
        let cycle_option: String = row.get("cycle_option");

        sequences.push(Sequence {
            name,
            schema,
            start,
            increment,
            min_value,
            max_value,
            cache: 1, // Default cache value
            cycle: cycle_option == "YES",
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
            t.tgargs as trigger_arguments
        FROM pg_trigger t
        JOIN pg_class c ON t.tgrelid = c.oid
        JOIN pg_proc p ON t.tgfoid = p.oid
        JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
        AND NOT t.tgisinternal  -- Exclude internal triggers
    "#;

    let rows = client.query(query, &[]).await?;
    let mut triggers = Vec::new();

    for row in rows {
        let name: String = row.get("trigger_name");
        let table: String = row.get("table_name");
        let function: String = row.get("function_name");
        let trigger_type: i16 = row.get("trigger_type");
        let arguments: Option<Vec<u8>> = row.get("trigger_arguments");

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
            timing,
            events,
            function,
            arguments: args,
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
            p.polcmd as command,
            p.polqual as using_expression,
            p.polwithcheck as check_expression
        FROM pg_policy p
        JOIN pg_class c ON p.polrelid = c.oid
        JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
    "#;

    let rows = client.query(query, &[]).await?;
    let mut policies = Vec::new();

    for row in rows {
        let name: String = row.get("policy_name");
        let table: String = row.get("table_name");
        let permissive: bool = row.get("permissive");
        let roles: Vec<i32> = row.get("roles");
        let command: String = row.get("command");
        let using_expr: Option<String> = row.get("using_expression");
        let check_expr: Option<String> = row.get("check_expression");

        // Convert role OIDs to role names (simplified)
        let role_names = roles.iter().map(|&oid| oid.to_string()).collect();

        policies.push(Policy {
            name,
            table,
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
            std::collections::HashMap::new()
        };

        servers.push(Server {
            name,
            foreign_data_wrapper,
            options: options_map,
        });
    }

    Ok(servers)
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
        events.push(TriggerEvent::Update);
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
