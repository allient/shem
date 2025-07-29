use crate::config::Config;
use anyhow::{Context, Result};
use parser::{ast::Statement as ParserStatement, parse_file};
use postgres::PostgresDriver;
use shared_types::{
    CheckOption, DataType, FunctionReturn, ParameterMode, PolicyCommand, TableConstraint,
    TriggerWhen,
};
use shem_core::{
    DatabaseDriver, EnumValue, Schema, schema::{ReplicaIdentity, IdentityGeneration, Generated, ColumnStorage, ConstraintType, ReferentialAction},
};
use std::collections::BTreeMap;
use std::path::PathBuf;
use tracing::{info, warn};

pub async fn execute(
    schema: PathBuf,
    output: Option<PathBuf>,
    database_url: Option<String>,
    name: Option<String>,
    config: &Config,
) -> Result<()> {
    // Try to load schema files from config first, fall back to provided path
    let schema_files = if config.declarative.enabled && !config.declarative.schema_paths.is_empty()
    {
        info!("Using declarative schema paths from config");
        config.load_schema_files()?
    } else {
        info!("Using provided schema path: {}", schema.display());
        vec![schema]
    };

    // Load schema from files
    let target_schema = load_schema_from_files(&schema_files)?;

    info!("Target schema: {:?}", target_schema);

    // Get current database schema if URL provided
    let current_schema = if let Some(url) = database_url.or_else(|| config.database_url.clone()) {
        info!("Connecting to database to get current schema");
        let driver = get_driver()?;
        let conn = driver.connect(&url).await?;
        Some(conn.introspect().await?)
    } else {
        None
    };

    // Generate migration
    let migration = if let Some(current) = current_schema {
        info!("Generating migration from database schema");
        //generate_migration(&current, &target_schema)?
    } else {
        info!("Generating initial migration");
        //generate_migration(&Schema::new(), &target_schema)?
    };

    // Write migration file
    let output_path = output.unwrap_or_else(|| {
        let timestamp = chrono::Utc::now().format("%Y%m%d%H%M%S");
        let filename = if let Some(migration_name) = name {
            // Sanitize the name for use in filename
            let sanitized_name = migration_name
                .chars()
                .map(|c| {
                    if c.is_alphanumeric() || c == '_' || c == '-' {
                        c
                    } else {
                        '_'
                    }
                })
                .collect::<String>();
            format!("migrations/{}_{}.sql", timestamp, sanitized_name)
        } else {
            format!("migrations/{}.sql", timestamp)
        };
        PathBuf::from(filename)
    });

    // Create migrations directory if it doesn't exist
    if let Some(parent) = output_path.parent() {
        std::fs::create_dir_all(parent).context("Failed to create migrations directory")?;
    }

    //write_migration(&output_path, &migration)?;
    info!("Migration written to {}", output_path.display());

    Ok(())
}

fn load_schema_from_files(files: &[PathBuf]) -> Result<Schema> {
    let mut schema = Schema::new();

    for file_path in files {
        if file_path.is_file() {
            // Load single schema file
            info!("Loading schema from file: {}", file_path.display());
            let statements = parse_file(file_path)?;
            for stmt in statements {
                add_statement_to_schema(&mut schema, &stmt)?;
            }
        } else if file_path.is_dir() {
            // Load all .sql files in directory, ordered by filename
            info!("Loading schemas from directory: {}", file_path.display());

            // Use BTreeMap to maintain order by filename
            let mut ordered_files = BTreeMap::new();

            // First, collect all SQL files and their paths
            for entry in walkdir::WalkDir::new(file_path)
                .into_iter()
                .filter_map(|e| e.ok())
                .filter(|e| e.path().extension().map_or(false, |ext| ext == "sql"))
            {
                let path = entry.path().to_path_buf();
                let filename = path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .ok_or_else(|| anyhow::anyhow!("Invalid filename: {}", path.display()))?;

                ordered_files.insert(filename.to_string(), path);
            }

            // Then process them in order
            for (filename, filepath) in ordered_files {
                info!("Processing schema file: {}", filename);
                let statements = parse_file(&filepath)?;
                for stmt in statements {
                    add_statement_to_schema(&mut schema, &stmt)?;
                }
            }
        } else {
            anyhow::bail!("Schema path does not exist: {}", file_path.display());
        }
    }

    Ok(schema)
}

fn add_statement_to_schema(schema: &mut Schema, stmt: &ParserStatement) -> Result<()> {
    match stmt {
        ParserStatement::CreateTable(create) => {
            dbg!(create);
            let mut table = shem_core::Table {
                oid: 0,
                name: create.name.clone(),
                schema: create.schema.clone().unwrap_or_default(),
                owner: "".to_string(),
                columns: Vec::new(),
                constraints: Vec::new(),
                indexes: Vec::new(),
                comment: None,
                tablespace: None,
                inherits: Vec::new(),
                partition_key: None,
                replica_identity: shem_core::schema::ReplicaIdentity::Default,
                acl: None,
                is_user_defined: true,
                is_from_extension: false,
            };

            // Add columns
            for col in &create.columns {
                let type_name = match &col.data_type {
                    DataType::Text => "TEXT".to_string(),
                    DataType::Integer => "INTEGER".to_string(),
                    DataType::BigInt => "BIGINT".to_string(),
                    DataType::SmallInt => "SMALLINT".to_string(),
                    DataType::Serial => "SERIAL".to_string(),
                    DataType::BigSerial => "BIGSERIAL".to_string(),
                    DataType::SmallSerial => "SMALLSERIAL".to_string(),
                    DataType::Boolean => "BOOLEAN".to_string(),
                    DataType::Real => "REAL".to_string(),
                    DataType::DoublePrecision => "DOUBLE PRECISION".to_string(),
                    DataType::Decimal(precision, scale) => {
                        if let (Some(p), Some(s)) = (precision, scale) {
                            format!("DECIMAL({}, {})", p, s)
                        } else if let Some(p) = precision {
                            format!("DECIMAL({})", p)
                        } else {
                            "DECIMAL".to_string()
                        }
                    }
                    DataType::Numeric(precision, scale) => {
                        if let (Some(p), Some(s)) = (precision, scale) {
                            format!("NUMERIC({}, {})", p, s)
                        } else if let Some(p) = precision {
                            format!("NUMERIC({})", p)
                        } else {
                            "NUMERIC".to_string()
                        }
                    }
                    DataType::Date => "DATE".to_string(),
                    DataType::Time(precision) => {
                        if let Some(p) = precision {
                            format!("TIME({})", p)
                        } else {
                            "TIME".to_string()
                        }
                    }
                    DataType::Timestamp(precision) => {
                        if let Some(p) = precision {
                            format!("TIMESTAMP({})", p)
                        } else {
                            "TIMESTAMP".to_string()
                        }
                    }
                    DataType::TimestampTz(precision) => {
                        if let Some(p) = precision {
                            format!("TIMESTAMPTZ({})", p)
                        } else {
                            "TIMESTAMPTZ".to_string()
                        }
                    }
                    DataType::Interval(precision) => {
                        if let Some(p) = precision {
                            format!("INTERVAL({:?})", p)
                        } else {
                            "INTERVAL".to_string()
                        }
                    }
                    DataType::Uuid => "UUID".to_string(),
                    DataType::Json => "JSON".to_string(),
                    DataType::JsonB => "JSONB".to_string(),
                    DataType::ByteA => "BYTEA".to_string(),
                    DataType::Character(length) => {
                        if let Some(l) = length {
                            format!("CHAR({})", l)
                        } else {
                            "CHAR".to_string()
                        }
                    }
                    DataType::CharacterVarying(length) => {
                        if let Some(l) = length {
                            format!("VARCHAR({})", l)
                        } else {
                            "VARCHAR".to_string()
                        }
                    }
                    DataType::Custom(name) => name.clone(),
                    _ => format!("{:?}", col.data_type), // Fallback for other types
                };

                let column = shem_core::Column {
                    name: col.name.clone(),
                    type_name,
                    is_not_null: !col.not_null,
                    has_default: col.default.is_some(),
                    identity: col.identity.as_ref().map(|i| shem_core::schema::Identity {
                        generation: if i.always { 
                            shem_core::schema::IdentityGeneration::Always 
                        } else { 
                            shem_core::schema::IdentityGeneration::ByDefault 
                        },
                    }),
                    generated: col
                        .generated
                        .as_ref()
                        .map(|g| shem_core::schema::Generated {
                            expression: format!("{:?}", g.expression),
                        }),
                    comment: None,
                    collation: None,
                    storage: shem_core::schema::ColumnStorage::Plain,
                    compression: None,
                    acl: None,
                    is_dropped: false,
                    is_local: true,
                    stats_target: None,
                    fdw_options: std::collections::HashMap::new(),
                };
                table.columns.push(column);
            }

            // Add constraints
            for constraint in &create.constraints {
                let definition = match constraint {
                    TableConstraint::PrimaryKey { columns, .. } => {
                        format!("PRIMARY KEY ({})", columns.join(", "))
                    }
                    TableConstraint::Unique { columns, .. } => {
                        format!("UNIQUE ({})", columns.join(", "))
                    }
                    TableConstraint::ForeignKey { columns, .. } => {
                        format!("FOREIGN KEY ({})", columns.join(", "))
                    }
                    TableConstraint::Check { .. } => "CHECK (...)".to_string(),
                    TableConstraint::Exclusion { .. } => "EXCLUDE (...)".to_string(),
                };
                let constraint = shem_core::Constraint {
                    oid: 0,
                    name: match constraint {
                        TableConstraint::PrimaryKey { name, .. } => {
                            name.clone().unwrap_or_default()
                        }
                        TableConstraint::ForeignKey { name, .. } => {
                            name.clone().unwrap_or_default()
                        }
                        TableConstraint::Unique { name, .. } => name.clone().unwrap_or_default(),
                        TableConstraint::Check { name, .. } => name.clone().unwrap_or_default(),
                        TableConstraint::Exclusion { name, .. } => name.clone().unwrap_or_default(),
                    },
                    table_oid: 0,
                    definition,
                                                                r#type: match constraint {
                        TableConstraint::PrimaryKey { .. } => shem_core::schema::ConstraintType::PrimaryKey,
                        TableConstraint::ForeignKey { .. } => shem_core::schema::ConstraintType::ForeignKey,
                        TableConstraint::Unique { .. } => shem_core::schema::ConstraintType::Unique,
                        TableConstraint::Check { .. } => shem_core::schema::ConstraintType::Check,
                        TableConstraint::Exclusion { .. } => shem_core::schema::ConstraintType::Exclusion,
                    },
                    foreign_table_oid: None,
                    foreign_key_columns: Vec::new(),
                    primary_key_columns: Vec::new(),
                    on_update: shem_core::schema::ReferentialAction::NoAction,
                    on_delete: shem_core::schema::ReferentialAction::NoAction,
                    is_deferrable: false,
                    is_initially_deferred: false,
                    is_not_valid: false,
                };
                table.constraints.push(constraint);
            }

            schema.tables.insert(table.name.clone(), table);
        }
        ParserStatement::CreateView(create) => {
            let view = shem_core::View {
                oid: 0, // Will be assigned during introspection
                name: create.name.clone(),
                schema: create.schema.clone().unwrap_or_else(|| "public".to_string()),
                owner: "".to_string(),
                definition: create.query.clone(),
                columns: Vec::new(),
                check_option: create
                    .check_option
                    .clone()
                    .map(|opt| match opt {
                        CheckOption::Local => shem_core::schema::CheckOption::Local,
                        CheckOption::Cascaded => shem_core::schema::CheckOption::Cascaded,
                    })
                    .unwrap_or(shem_core::schema::CheckOption::None),
                options: std::collections::HashMap::new(),
                acl: None,
                comment: None,
                is_user_defined: true,
                is_from_extension: false,
            };
            schema.views.insert(view.name.clone(), view);
        }
        ParserStatement::CreateMaterializedView(create) => {
            let view = shem_core::MaterializedView {
                oid: 0, // Will be assigned during introspection
                name: create.name.clone(),
                schema: create.schema.clone().unwrap_or_else(|| "public".to_string()),
                owner: "postgres".to_string(), // Default owner
                definition: create.query.clone(),
                columns: Vec::new(), // Will be populated during introspection
                is_populated: true, // Default to WITH DATA for parsed statements
                options: std::collections::HashMap::new(),
                tablespace: None,
                acl: None,
                comment: None,
                indexes: Vec::new(),
                is_user_defined: true,
                is_from_extension: false,
            };
            schema.materialized_views.insert(view.name.clone(), view);
        }
        ParserStatement::CreateFunction(create) => {
            let mut parameters = Vec::new();
            for param in &create.parameters {
                let parameter = shem_core::Parameter {
                    name: param.name.clone().unwrap_or_default(),
                    type_name: format!("{:?}", param.data_type),
                    mode: param
                        .mode
                        .clone()
                        .map(|mode| match mode {
                            ParameterMode::In => shem_core::schema::ParameterMode::In,
                            ParameterMode::Out => shem_core::schema::ParameterMode::Out,
                            ParameterMode::InOut => shem_core::schema::ParameterMode::InOut,
                            ParameterMode::Variadic => shem_core::schema::ParameterMode::Variadic,
                        })
                        .unwrap_or(shem_core::schema::ParameterMode::In),
                    default: param.default.as_ref().map(|d| format!("{:?}", d)),
                };
                parameters.push(parameter);
            }

            let returns = match &create.returns {
                FunctionReturn::Type(t) => shem_core::ReturnType {
                    kind: shem_core::ReturnKind::Scalar,
                    type_name: format!("{:?}", t),
                    is_set: false,
                },
                FunctionReturn::Table(cols) => shem_core::ReturnType {
                    kind: shem_core::ReturnKind::Table,
                    type_name: format!("{:?}", cols),
                    is_set: false,
                },
                FunctionReturn::SetOf(t) => shem_core::ReturnType {
                    kind: shem_core::ReturnKind::SetOf,
                    type_name: format!("{:?}", t),
                    is_set: true,
                },
            };

            let function = shem_core::Function {
                name: create.name.clone(),
                schema: create.schema.clone(),
                parameters,
                returns,
                language: create.language.clone(),
                definition: create.body.clone(),
                comment: None,
                volatility: shem_core::Volatility::Volatile,
                strict: false,
                security_definer: false,
                parallel_safety: shem_core::ParallelSafety::Unsafe,
                cost: None,
                rows: None,
            };
            schema.functions.insert(function.name.clone(), function);
        }
        ParserStatement::CreateProcedure(create) => {
            let mut parameters = Vec::new();
            for param in &create.parameters {
                let parameter = shem_core::Parameter {
                    name: param.name.clone().unwrap_or_default(),
                    type_name: format!("{:?}", param.data_type),
                    mode: param
                        .mode
                        .clone()
                        .map(|mode| match mode {
                            ParameterMode::In => shem_core::schema::ParameterMode::In,
                            ParameterMode::Out => shem_core::schema::ParameterMode::Out,
                            ParameterMode::InOut => shem_core::schema::ParameterMode::InOut,
                            ParameterMode::Variadic => shem_core::schema::ParameterMode::Variadic,
                        })
                        .unwrap_or(shem_core::schema::ParameterMode::In),
                    default: param.default.as_ref().map(|d| format!("{:?}", d)),
                };
                parameters.push(parameter);
            }

            let procedure = shem_core::Procedure {
                name: create.name.clone(),
                schema: create.schema.clone(),
                parameters,
                language: create.language.clone(),
                definition: create.body.clone(),
                comment: None,
                security_definer: false,
            };
            schema.procedures.insert(procedure.name.clone(), procedure);
        }
        ParserStatement::CreateEnum(create) => {
            let enum_type = shem_core::EnumType {
                info: shem_core::schema::TypeInfo {
                    oid: 0,
                    name: create.name.clone(),
                    schema: create
                        .schema
                        .clone()
                        .unwrap_or_else(|| "public".to_string()),
                    owner: "".to_string(),
                    acl: None,
                    comment: None,
                    is_user_defined: true,
                    is_from_extension: false,
                    array_type_oid: None,
                },
                values: create
                    .values
                    .iter()
                    .map(|v| EnumValue {
                        oid: 0,
                        label: v.clone(),
                    })
                    .collect(),
            };
            schema.types.insert(
                enum_type.info.name.clone(),
                shem_core::schema::Type::Enum(enum_type),
            );
        }
        ParserStatement::CreateType(_create) => {
            // Handle composite types - they can be stored in a separate collection if needed
            // For now, we'll skip them as they're not enums
        }
        ParserStatement::CreateDomain(create) => {
            let domain = shem_core::Domain {
                info: shem_core::schema::TypeInfo {
                    oid: 0,
                    name: create.name.clone(),
                    schema: create
                        .schema
                        .clone()
                        .unwrap_or_else(|| "public".to_string()),
                    owner: "".to_string(),
                    acl: None,
                    comment: None,
                    is_user_defined: true,
                    is_from_extension: false,
                    array_type_oid: None,
                },
                base_type: format!("{:?}", create.data_type),
                collation: None,
                not_null: false,
                default: None,
                constraints: vec![], // TODO: Parse domain constraints
            };
            schema.types.insert(
                domain.info.name.clone(),
                shem_core::schema::Type::Domain(domain),
            );
        }
        ParserStatement::CreateSequence(create) => {
            let sequence = shem_core::Sequence {
                oid: 0,
                name: create.name.clone(),
                schema: create.schema.clone(),
                owner: "".to_string(),
                data_type: "bigint".to_string(),
                start: create.start.unwrap_or(1),
                increment: create.increment.unwrap_or(1),
                min_value: create.min_value,
                max_value: create.max_value,
                cache: create.cache.unwrap_or(1),
                cycle: create.cycle,
                current_value: None,
                is_called: false,
                owned_by: None,
                acl: None,
                comment: None,
                is_user_defined: true,
                is_from_extension: false,
            };
            schema.sequences.insert(sequence.name.clone(), sequence);
        }
        ParserStatement::CreateExtension(create) => {
            let extension = shem_core::Extension {
                oid: 0,
                owner: "".to_string(),
                name: create.name.clone(),
                schema: create.schema.clone().unwrap_or_default(),
                version: create.version.clone().unwrap_or_default(),
                relocatable: false,
                is_user_defined: false,
                comment: None,
            };
            schema.extensions.insert(extension.name.clone(), extension);
        }
        ParserStatement::CreateTrigger(create) => {
            let trigger = shem_core::Trigger {
                name: create.name.clone(),
                table: create.table.clone(),
                schema: None,
                timing: match create.when {
                    TriggerWhen::Before => shem_core::TriggerTiming::Before,
                    TriggerWhen::After => shem_core::TriggerTiming::After,
                    TriggerWhen::InsteadOf => shem_core::TriggerTiming::InsteadOf,
                },
                events: vec![shem_core::schema::TriggerEvent::Insert], // Default
                function: create.function.clone(),
                arguments: create.arguments.clone(),
                condition: None,
                for_each: shem_core::TriggerLevel::Row,
                when: None,
                comment: None,
            };
            schema.triggers.insert(trigger.name.clone(), trigger);
        }
        ParserStatement::CreatePolicy(create) => {
            let policy = shem_core::Policy {
                oid: 0, // Will be assigned during introspection
                name: Some(create.name.clone()),
                table_oid: 0, // Will be assigned during introspection
                table_name: create.table.clone(),
                schema: "public".to_string(), // Default schema
                command: match create.command {
                    PolicyCommand::All => shem_core::schema::PolicyCommand::All,
                    PolicyCommand::Select => shem_core::schema::PolicyCommand::Select,
                    PolicyCommand::Insert => shem_core::schema::PolicyCommand::Insert,
                    PolicyCommand::Update => shem_core::schema::PolicyCommand::Update,
                    PolicyCommand::Delete => shem_core::schema::PolicyCommand::Delete,
                },
                permissive: create.permissive,
                roles: create.roles.clone(),
                using: create.using.as_ref().map(|u| format!("{:?}", u)),
                check: create.with_check.as_ref().map(|c| format!("{:?}", c)),
                is_user_defined: true,
                is_from_extension: false,
            };
            let key = format!("{}.{}.{}", policy.schema, policy.table_name, policy.name.as_ref().unwrap());
            schema.policies.insert(key, policy);
        }
        ParserStatement::CreateServer(create) => {
            let server = shem_core::Server {
                name: create.name.clone(),
                foreign_data_wrapper: create.foreign_data_wrapper.clone(),
                options: create.options.clone(),
                version: None,
            };
            schema.servers.insert(server.name.clone(), server);
        }
        ParserStatement::AlterTable(alter) => {
            // Find the table in the schema and add constraints
            if let Some(table) = schema.tables.get_mut(&alter.name) {
                for action in &alter.actions {
                    match action {
                        parser::ast::AlterTableAction::AddConstraint(constraint) => {
                            match constraint {
                                TableConstraint::PrimaryKey { columns, name } => {
                                    if !columns.is_empty() {
                                        let c = shem_core::Constraint {
                                            oid: 0,
                                            name: name.clone().unwrap_or_default(),
                                            table_oid: 0,
                                            definition: format!(
                                                "PRIMARY KEY ({})",
                                                columns.join(", ")
                                            ),
                                            r#type: shem_core::schema::ConstraintType::PrimaryKey,
                                            foreign_table_oid: None,
                                            foreign_key_columns: Vec::new(),
                                            primary_key_columns: Vec::new(),
                                            on_update: shem_core::schema::ReferentialAction::NoAction,
                                            on_delete: shem_core::schema::ReferentialAction::NoAction,
                                            is_deferrable: false,
                                            is_initially_deferred: false,
                                            is_not_valid: false,
                                        };
                                        table.constraints.push(c);
                                    }
                                }
                                TableConstraint::Unique { columns, name } => {
                                    if !columns.is_empty() {
                                        let c = shem_core::Constraint {
                                            oid: 0,
                                            name: name.clone().unwrap_or_default(),
                                            table_oid: 0,
                                            definition: format!("UNIQUE ({})", columns.join(", ")),
                                            r#type: shem_core::schema::ConstraintType::Unique,
                                            foreign_table_oid: None,
                                            foreign_key_columns: Vec::new(),
                                            primary_key_columns: Vec::new(),
                                            on_update: shem_core::schema::ReferentialAction::NoAction,
                                            on_delete: shem_core::schema::ReferentialAction::NoAction,
                                            is_deferrable: false,
                                            is_initially_deferred: false,
                                            is_not_valid: false,
                                        };
                                        table.constraints.push(c);
                                    }
                                }
                                _ => {}
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
        _ => {
            warn!("Unsupported statement type: {:?}", stmt);
        }
    }

    Ok(())
}

fn get_driver() -> Result<Box<dyn DatabaseDriver>> {
    Ok(Box::new(PostgresDriver::new()))
}
