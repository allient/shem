use shem_core::Result;
use shem_core::schema::*;
use std::collections::HashMap;
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

    // Introspect types (composite types, not enums)
    let composite_types = introspect_composite_types(&*client).await?;
    for type_def in composite_types {
        // Store composite types in the types collection
        schema.types.insert(type_def.name.clone(), type_def);
    }

    // Introspect range types separately for detailed information
    let range_types = introspect_range_types(&*client).await?;
    for range_type in range_types {
        // Store range types in the types collection with a special prefix
        schema
            .range_types
            .insert(range_type.name.clone(), range_type.clone());
        
        // Also store in types collection for serialization
        let type_def = Type {
            name: range_type.name.clone(),
            schema: range_type.schema.clone(),
            kind: TypeKind::Range,
            comment: None,
            definition: Some(range_type.subtype.clone()),
        };
        schema.types.insert(range_type.name.clone(), type_def);
    }

    // Introspect enums
    let enums = introspect_enums(&*client).await?;
    for enum_type in enums {
        schema.enums.insert(enum_type.name.clone(), enum_type.clone());
        
        // Also store in types collection for serialization
        let type_def = Type {
            name: enum_type.name.clone(),
            schema: enum_type.schema.clone(),
            kind: TypeKind::Enum { values: enum_type.values.clone() },
            comment: enum_type.comment.clone(),
            definition: None,
        };
        schema.types.insert(enum_type.name.clone(), type_def);
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
        schema
            .constraint_triggers
            .insert(trigger.name.clone(), trigger);
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
            tablespace: None,                   // TODO: Get tablespace information
            inherits: Vec::new(),               // TODO: Get inheritance information
            partition_by: None,                 // TODO: Get partitioning information
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
            a.attname as column_name,
            pg_catalog.format_type(a.atttypid, a.atttypmod) as type_name,
            NOT a.attnotnull as is_nullable,
            pg_get_expr(ad.adbin, ad.adrelid) as column_default,
            c.identity_generation,
            c.generation_expression,
            a.attcollation as collation_oid,
            col.collname as collation_name
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

        columns.push(Column {
            name,
            type_name,
            nullable,
            default,
            identity,
            generated,
            comment: None,
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
        let column_names: Vec<String> = row.get("column_names");
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
                    nulls_first: false, // TODO: Get actual nulls order
                    opclass: None,    // TODO: Get operator class
                }],
                unique: is_unique,
                method: index_method,
                where_clause,
                tablespace: None,                   // TODO: Get tablespace
                storage_parameters: HashMap::new(), // TODO: Get storage parameters
            });
        } else if let Some(idx) = &mut current_index {
            idx.columns.push(IndexColumn {
                name: column_name,
                expression: None, // TODO: Get expression for functional indexes
                order: SortOrder::Ascending, // TODO: Get actual sort order
                nulls_first: false, // TODO: Get actual nulls order
                opclass: None,    // TODO: Get operator class
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
            columns: Vec::new(),     // TODO: Get explicit column list
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
            tablespace: None,                   // TODO: Get tablespace information
            storage_parameters: HashMap::new(), // TODO: Get storage parameters
            indexes: Vec::new(),                // TODO: Get materialized view indexes
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
            strict: false,                    // TODO: Get strict information
            security_definer: false,          // TODO: Get security definer information
            parallel_safety: ParallelSafety::Unsafe, // TODO: Get parallel safety information
            cost: None,                       // TODO: Get cost information
            rows: None,                       // TODO: Get rows information
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

async fn introspect_composite_types<C: GenericClient>(client: &C) -> Result<Vec<Type>>
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
    let mut grouped: BTreeMap<(String, String), (Vec<Column>, Option<String>, u32)> = BTreeMap::new();

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
        let owner: u32 = row.get("owner");

        let storage = match storage_type.map(|b| b as u8 as char) {
            Some('p') => Some(ColumnStorage::Plain),
            Some('e') => Some(ColumnStorage::External),
            Some('x') => Some(ColumnStorage::Extended),
            Some('m') => Some(ColumnStorage::Main),
            _ => None,
        };

        let compression = compression.map(|b| (b as u8 as char).to_string());

        let column = Column {
            name: attr_name,
            type_name: attr_type,
            nullable: !is_not_null,
            default: default_expr,
            identity: None, // Composite types don't have identity columns
            generated: None, // Composite types don't have generated columns
            comment: None, // Could be enhanced to get column comments if needed
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
        types.push(Type {
            name,
            schema: Some(schema),
            kind: TypeKind::Composite { attributes: attrs },
            comment,
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
            pg_get_expr(d.adbin, d.adrelid) AS domain_default,
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
        LEFT JOIN pg_attrdef d ON t.typrelid = d.adrelid AND t.typtypmod = d.adnum
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
        let base_type: String = row.get("base_type");
        let formatted_base_type: String = row.get("formatted_base_type");
        let default: Option<String> = row.get("domain_default");
        let check_clause: Option<String> = row.get("check_clause");
        let is_valid: Option<bool> = row.get("is_valid");
        let not_null: bool = row.get("is_not_null");
        let comment: Option<String> = row.get("domain_comment");
        let _owner: u32 = row.get("owner");
        let _type_modifier: i32 = row.get("type_modifier");

        let key = (schema.clone(), name.clone());

        let domain = domain_map.entry(key.clone()).or_insert(Domain {
            name: name.clone(),
            schema: Some(schema),
            base_type: formatted_base_type, // Use formatted type which includes length/precision
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

async fn introspect_sequences<C: GenericClient>(client: &C) -> Result<Vec<Sequence>> {
    let query = r#"
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
            pg_get_expr(ad.adbin, ad.adrelid) AS sequence_default,
            dep.refobjid AS owned_by_table_oid,
            dep.refobjsubid AS owned_by_column_num
        FROM pg_class c
        JOIN pg_namespace n ON c.relnamespace = n.oid
        JOIN pg_sequence s ON s.seqrelid = c.oid
        LEFT JOIN pg_attrdef ad ON ad.adrelid = c.oid
        LEFT JOIN pg_depend dep ON dep.objid = c.oid AND dep.deptype = 'a'
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
        let schema: Option<String> = row.get("sequence_schema");
        let start: i64 = row.get("start_value");
        let min_value: i64 = row.get("minimum_value");
        let max_value: i64 = row.get("maximum_value");
        let increment: i64 = row.get("increment");
        let cache: i64 = row.get("cache_value");
        let cycle: bool = row.get("cycle_option");
        let comment: Option<String> = row.get("sequence_comment");
        let _owner: u32 = row.get("owner");
        let _default: Option<String> = row.get("sequence_default");
        let owned_by_table_oid: Option<u32> = row.get("owned_by_table_oid");
        let owned_by_column_num: Option<i32> = row.get("owned_by_column_num");

        // Determine sequence data type based on min/max values
        let data_type = if min_value >= i32::MIN as i64 && max_value <= i32::MAX as i64 {
            "integer"
        } else if min_value >= i16::MIN as i64 && max_value <= i16::MAX as i64 {
            "smallint"
        } else {
            "bigint"
        };

        // Get owned_by information if available
        let owned_by = if let (Some(table_oid), Some(column_num)) = (owned_by_table_oid, owned_by_column_num) {
            // Query to get table and column name
            let owned_query = r#"
                SELECT 
                    c.relname AS table_name,
                    n.nspname AS table_schema,
                    a.attname AS column_name
                FROM pg_class c
                JOIN pg_namespace n ON c.relnamespace = n.oid
                JOIN pg_attribute a ON a.attrelid = c.oid
                WHERE c.oid = $1 AND a.attnum = $2
            "#;
            
            if let Ok(owned_rows) = client.query(owned_query, &[&(table_oid as i32), &column_num]).await {
                if let Some(owned_row) = owned_rows.first() {
                    let table_schema: String = owned_row.get("table_schema");
                    let table_name: String = owned_row.get("table_name");
                    let column_name: String = owned_row.get("column_name");
                    Some(format!("{}.{}.{}", table_schema, table_name, column_name))
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        };

        sequences.push(Sequence {
            name,
            schema,
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
        WHERE e.extname NOT IN ('plpgsql')
    "#;

    let rows = client.query(query, &[]).await?;
    let mut extensions = Vec::new();

    for row in rows {
        let name: String = row.get("extension_name");
        let version: String = row.get("extension_version");
        let schema: Option<String> = row.get("schema_name");

        extensions.push(Extension {
            name,
            version,
            schema,
            cascade: false, // Still no direct way to fetch this info
            comment: None,  // Optional: you can later query obj_description for it
        });
    }

    Ok(extensions)
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
            c.relowner AS owner
        FROM pg_trigger t
        JOIN pg_class c ON t.tgrelid = c.oid
        JOIN pg_namespace n ON c.relnamespace = n.oid
        JOIN pg_proc p ON t.tgfoid = p.oid
        WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
          AND NOT t.tgisinternal
          AND c.relowner > 1
          AND NOT EXISTS (
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
        let schema: String = row.get("schema_name");
        let function: String = row.get("function_name");
        let trigger_type: i16 = row.get("trigger_type");
        let arguments: Option<Vec<u8>> = row.get("trigger_arguments");
        let constraint_oid: Option<u32> = row.get("constraint_oid");

        // Skip constraint triggers
        if constraint_oid.is_some() {
            continue;
        }

        let (timing, events) = parse_trigger_type(trigger_type);
        let args = arguments
            .map(|bytes| parse_trigger_arguments(&bytes))
            .unwrap_or_default();

        triggers.push(Trigger {
            name,
            table,
            schema: Some(schema),
            function,
            timing,
            events,
            arguments: args,
            condition: None, // Optional: parse from pg_get_triggerdef if needed
            for_each: TriggerLevel::Row, // Optional: improve if you parse tgtype bits
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

async fn introspect_servers<C: GenericClient + Sync>(client: &C) -> Result<Vec<Server>> {
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

        // Parse the rule definition to extract just the action part
        let action = if definition.contains("DO ") {
            // Extract everything after "DO "
            if let Some(do_pos) = definition.find("DO ") {
                definition[do_pos + 3..].trim().to_string()
            } else {
                definition.clone()
            }
        } else {
            definition.clone()
        };

        rules.push(Rule {
            name,
            table,
            schema,
            event,
            instead: is_instead,
            condition: None,           // TODO: parse WHERE condition from definition
            actions: vec![action],     // Store just the action part
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
            c.relowner as owner
        FROM pg_trigger t
        JOIN pg_class c ON t.tgrelid = c.oid
        JOIN pg_namespace n ON c.relnamespace = n.oid
        JOIN pg_proc p ON t.tgfoid = p.oid
        WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
        AND NOT t.tgisinternal
        AND t.tgconstraint IS NOT NULL
        AND c.relowner > 1
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

        // Parse trigger type into timing and events
        let (timing, events) = parse_trigger_type(trigger_type);

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
            t.typowner AS owner
        FROM pg_type t
        JOIN pg_namespace n ON t.typnamespace = n.oid
        JOIN pg_range r ON t.oid = r.rngtypid
        LEFT JOIN pg_proc p1 ON p1.oid = r.rngcanonical
        LEFT JOIN pg_proc p2 ON p2.oid = r.rngsubdiff
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
        let subtype_oid: u32 = row.get("subtype_oid");
        let subtype_opclass_oid: Option<u32> = row.get("subtype_opclass_oid");
        let collation_oid: Option<u32> = row.get("collation_oid");
        let canonical = row.get::<_, Option<String>>("canonical_function");
        let subtype_diff = row.get::<_, Option<String>>("subtype_diff_function");

        // Get subtype name
        let subtype_query = "SELECT typname FROM pg_type WHERE oid = $1";
        let subtype_rows = client.query(subtype_query, &[&subtype_oid]).await?;
        let subtype = if let Some(subtype_row) = subtype_rows.first() {
            subtype_row.get::<_, String>("typname")
        } else {
            "unknown".to_string()
        };

        // Get opclass name
        let subtype_opclass = if let Some(opclass_oid) = subtype_opclass_oid {
            let opclass_query = "SELECT opcname FROM pg_opclass WHERE oid = $1";
            let opclass_rows = client.query(opclass_query, &[&opclass_oid]).await?;
            opclass_rows.first().map(|row| row.get("opcname"))
        } else {
            None
        };

        // Get collation name
        let collation = if let Some(coll_oid) = collation_oid {
            let coll_query = "SELECT collname FROM pg_collation WHERE oid = $1";
            let coll_rows = client.query(coll_query, &[&coll_oid]).await?;
            coll_rows.first().map(|row| row.get("collname"))
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
            multirange_type_name: None, // TODO: Add when needed
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
        JOIN pg_namespace n ON n.oid = t.typnamespace
        WHERE t.typtype = 'e'
        AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
        AND NOT EXISTS (
            SELECT 1
            FROM pg_depend d
            JOIN pg_extension e ON d.refobjid = e.oid
            WHERE d.objid = t.oid AND d.deptype = 'e'
        )
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
            comment: None, // Optional: use pg_description if needed
        });
    }

    Ok(enums)
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

        // Parse parameter mode and name
        let mut param_parts: Vec<&str> = trimmed.split_whitespace().collect();
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
                mode: if start_idx == 0 { ParameterMode::In } else { mode }, // Only use explicit mode if keyword was found
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
