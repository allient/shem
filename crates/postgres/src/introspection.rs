use shem_core::{Result, Error};
use tokio_postgres::{Row, GenericClient};
use shem_core::schema::*;

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
        let generated: Option<GeneratedColumn> = row.get::<_, Option<String>>("generation_expression")
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
            tc.check_clause,
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
            ConstraintKind::PrimaryKey => format!("PRIMARY KEY ({})", row.get::<_, String>("column_name")),
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
            },
            ConstraintKind::Unique => format!("UNIQUE ({})", row.get::<_, String>("column_name")),
            ConstraintKind::Check => row.get::<_, String>("check_clause"),
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
        JOIN pg_class i ON i.oid = ANY(pg_index.indrelid)
        JOIN pg_index ix ON ix.indrelid = t.oid
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

        if current_index.as_ref().map(|i: &Index| i.name != name).unwrap_or(true) {
            if let Some(idx) = current_index.take() {
                indexes.push(idx);
            }
            current_index = Some(Index {
                name,
                columns: vec![IndexColumn {
                    name: column_name,
                    order: SortOrder::Ascending, // TODO: Get actual sort order
                    nulls_first: false, // TODO: Get actual nulls order
                }],
                unique: is_unique,
                method,
            });
        } else if let Some(idx) = &mut current_index {
            idx.columns.push(IndexColumn {
                name: column_name,
                order: SortOrder::Ascending, // TODO: Get actual sort order
                nulls_first: false, // TODO: Get actual nulls order
            });
        }
    }

    if let Some(idx) = current_index {
        indexes.push(idx);
    }

    Ok(indexes)
}

// TODO: Implement remaining introspection functions
async fn introspect_views<C: GenericClient>(_client: &C) -> Result<Vec<View>> {
    // TODO: Implement view introspection
    Ok(Vec::new())
}

async fn introspect_materialized_views<C: GenericClient>(_client: &C) -> Result<Vec<MaterializedView>> {
    // TODO: Implement materialized view introspection
    Ok(Vec::new())
}

async fn introspect_functions<C: GenericClient>(_client: &C) -> Result<Vec<Function>> {
    // TODO: Implement function introspection
    Ok(Vec::new())
}

async fn introspect_procedures<C: GenericClient>(_client: &C) -> Result<Vec<Procedure>> {
    // TODO: Implement procedure introspection
    Ok(Vec::new())
}

async fn introspect_types<C: GenericClient>(_client: &C) -> Result<Vec<Type>> {
    // TODO: Implement type introspection
    Ok(Vec::new())
}

async fn introspect_domains<C: GenericClient>(_client: &C) -> Result<Vec<Domain>> {
    // TODO: Implement domain introspection
    Ok(Vec::new())
}

async fn introspect_sequences<C: GenericClient>(_client: &C) -> Result<Vec<Sequence>> {
    // TODO: Implement sequence introspection
    Ok(Vec::new())
}

async fn introspect_extensions<C: GenericClient>(_client: &C) -> Result<Vec<Extension>> {
    // TODO: Implement extension introspection
    Ok(Vec::new())
}

async fn introspect_triggers<C: GenericClient>(_client: &C) -> Result<Vec<Trigger>> {
    // TODO: Implement trigger introspection
    Ok(Vec::new())
}

async fn introspect_policies<C: GenericClient>(_client: &C) -> Result<Vec<Policy>> {
    // TODO: Implement policy introspection
    Ok(Vec::new())
}

async fn introspect_servers<C: GenericClient>(_client: &C) -> Result<Vec<Server>> {
    // TODO: Implement foreign server introspection
    Ok(Vec::new())
}