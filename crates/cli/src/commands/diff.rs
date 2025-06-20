use crate::config::Config;
use anyhow::{Context, Result};
use shem_core::{
    DatabaseDriver, Schema,
    migration::{generate_migration, write_migration},
};
use shem_parser::{
    ast::{
        CheckOption, ParameterMode, PolicyCommand, Statement as ParserStatement, TableConstraint,
        TriggerWhen,
    },
    parse_file,
};
use shem_postgres::PostgresDriver;
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
        generate_migration(&current, &target_schema)?
    } else {
        info!("Generating initial migration");
        generate_migration(&Schema::new(), &target_schema)?
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

    write_migration(&output_path, &migration)?;
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
                name: create.name.clone(),
                schema: create.schema.clone(),
                columns: Vec::new(),
                constraints: Vec::new(),
                indexes: Vec::new(),
                comment: None,
                tablespace: None,
                inherits: Vec::new(),
                partition_by: None,
                storage_parameters: std::collections::HashMap::new(),
            };

            // Add columns
            for col in &create.columns {
                let type_name = match &col.data_type {
                    shem_parser::ast::DataType::Text => "TEXT".to_string(),
                    shem_parser::ast::DataType::Integer => "INTEGER".to_string(),
                    shem_parser::ast::DataType::BigInt => "BIGINT".to_string(),
                    shem_parser::ast::DataType::SmallInt => "SMALLINT".to_string(),
                    shem_parser::ast::DataType::Serial => "SERIAL".to_string(),
                    shem_parser::ast::DataType::BigSerial => "BIGSERIAL".to_string(),
                    shem_parser::ast::DataType::SmallSerial => "SMALLSERIAL".to_string(),
                    shem_parser::ast::DataType::Boolean => "BOOLEAN".to_string(),
                    shem_parser::ast::DataType::Real => "REAL".to_string(),
                    shem_parser::ast::DataType::DoublePrecision => "DOUBLE PRECISION".to_string(),
                    shem_parser::ast::DataType::Decimal(precision, scale) => {
                        if let (Some(p), Some(s)) = (precision, scale) {
                            format!("DECIMAL({}, {})", p, s)
                        } else if let Some(p) = precision {
                            format!("DECIMAL({})", p)
                        } else {
                            "DECIMAL".to_string()
                        }
                    }
                    shem_parser::ast::DataType::Numeric(precision, scale) => {
                        if let (Some(p), Some(s)) = (precision, scale) {
                            format!("NUMERIC({}, {})", p, s)
                        } else if let Some(p) = precision {
                            format!("NUMERIC({})", p)
                        } else {
                            "NUMERIC".to_string()
                        }
                    }
                    shem_parser::ast::DataType::Date => "DATE".to_string(),
                    shem_parser::ast::DataType::Time(precision) => {
                        if let Some(p) = precision {
                            format!("TIME({})", p)
                        } else {
                            "TIME".to_string()
                        }
                    }
                    shem_parser::ast::DataType::Timestamp(precision) => {
                        if let Some(p) = precision {
                            format!("TIMESTAMP({})", p)
                        } else {
                            "TIMESTAMP".to_string()
                        }
                    }
                    shem_parser::ast::DataType::TimestampTz(precision) => {
                        if let Some(p) = precision {
                            format!("TIMESTAMPTZ({})", p)
                        } else {
                            "TIMESTAMPTZ".to_string()
                        }
                    }
                    shem_parser::ast::DataType::Interval(precision) => {
                        if let Some(p) = precision {
                            format!("INTERVAL({:?})", p)
                        } else {
                            "INTERVAL".to_string()
                        }
                    }
                    shem_parser::ast::DataType::Uuid => "UUID".to_string(),
                    shem_parser::ast::DataType::Json => "JSON".to_string(),
                    shem_parser::ast::DataType::JsonB => "JSONB".to_string(),
                    shem_parser::ast::DataType::ByteA => "BYTEA".to_string(),
                    shem_parser::ast::DataType::Character(length) => {
                        if let Some(l) = length {
                            format!("CHAR({})", l)
                        } else {
                            "CHAR".to_string()
                        }
                    }
                    shem_parser::ast::DataType::CharacterVarying(length) => {
                        if let Some(l) = length {
                            format!("VARCHAR({})", l)
                        } else {
                            "VARCHAR".to_string()
                        }
                    }
                    shem_parser::ast::DataType::Custom(name) => name.clone(),
                    _ => format!("{:?}", col.data_type), // Fallback for other types
                };

                let column = shem_core::Column {
                    name: col.name.clone(),
                    type_name,
                    nullable: !col.not_null,
                    default: col.default.as_ref().map(|d| format!("{:?}", d)),
                    identity: col.identity.as_ref().map(|i| shem_core::Identity {
                        always: i.always,
                        start: i.start.unwrap_or(1),
                        increment: i.increment.unwrap_or(1),
                        min_value: i.min_value,
                        max_value: i.max_value,
                        cache: None,
                        cycle: false,
                    }),
                    generated: col.generated.as_ref().map(|g| shem_core::GeneratedColumn {
                        expression: format!("{:?}", g.expression),
                        stored: g.stored,
                    }),
                    comment: None,
                    collation: None,
                    storage: None,
                    compression: None,
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
                    kind: match constraint {
                        TableConstraint::PrimaryKey { .. } => shem_core::ConstraintKind::PrimaryKey,
                        TableConstraint::ForeignKey { .. } => {
                            shem_core::ConstraintKind::ForeignKey {
                                references: "".to_string(),
                                on_delete: None,
                                on_update: None,
                            }
                        }
                        TableConstraint::Unique { .. } => shem_core::ConstraintKind::Unique,
                        TableConstraint::Check { .. } => shem_core::ConstraintKind::Check,
                        TableConstraint::Exclusion { .. } => shem_core::ConstraintKind::Exclusion,
                    },
                    definition,
                    deferrable: false,
                    initially_deferred: false,
                };
                table.constraints.push(constraint);
            }

            schema.tables.insert(table.name.clone(), table);
        }
        ParserStatement::CreateView(create) => {
            let view = shem_core::View {
                name: create.name.clone(),
                schema: create.schema.clone(),
                definition: create.query.clone(),
                check_option: create
                    .check_option
                    .clone()
                    .map(|opt| match opt {
                        CheckOption::Local => shem_core::CheckOption::Local,
                        CheckOption::Cascaded => shem_core::CheckOption::Cascaded,
                    })
                    .unwrap_or(shem_core::CheckOption::None),
                comment: None,
                security_barrier: false,
                columns: Vec::new(),
            };
            schema.views.insert(view.name.clone(), view);
        }
        ParserStatement::CreateMaterializedView(create) => {
            let view = shem_core::MaterializedView {
                name: create.name.clone(),
                schema: create.schema.clone(),
                definition: create.query.clone(),
                check_option: shem_core::CheckOption::None, // Materialized views don't have check options
                comment: None,
                tablespace: None,
                storage_parameters: std::collections::HashMap::new(),
                indexes: Vec::new(),
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
                            ParameterMode::In => shem_core::ParameterMode::In,
                            ParameterMode::Out => shem_core::ParameterMode::Out,
                            ParameterMode::InOut => shem_core::ParameterMode::InOut,
                            ParameterMode::Variadic => shem_core::ParameterMode::Variadic,
                        })
                        .unwrap_or(shem_core::ParameterMode::In),
                    default: param.default.as_ref().map(|d| format!("{:?}", d)),
                };
                parameters.push(parameter);
            }

            let returns = match &create.returns {
                shem_parser::ast::FunctionReturn::Type(t) => shem_core::ReturnType {
                    kind: shem_core::ReturnKind::Scalar,
                    type_name: format!("{:?}", t),
                    is_set: false,
                },
                shem_parser::ast::FunctionReturn::Table(cols) => shem_core::ReturnType {
                    kind: shem_core::ReturnKind::Table,
                    type_name: format!("{:?}", cols),
                    is_set: false,
                },
                shem_parser::ast::FunctionReturn::SetOf(t) => shem_core::ReturnType {
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
                            ParameterMode::In => shem_core::ParameterMode::In,
                            ParameterMode::Out => shem_core::ParameterMode::Out,
                            ParameterMode::InOut => shem_core::ParameterMode::InOut,
                            ParameterMode::Variadic => shem_core::ParameterMode::Variadic,
                        })
                        .unwrap_or(shem_core::ParameterMode::In),
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
                name: create.name.clone(),
                schema: create.schema.clone(),
                values: create.values.clone(),
                comment: None,
            };
            schema.enums.insert(enum_type.name.clone(), enum_type);
        }
        ParserStatement::CreateType(_create) => {
            // Handle composite types - they can be stored in a separate collection if needed
            // For now, we'll skip them as they're not enums
        }
        ParserStatement::CreateDomain(create) => {
            let domain = shem_core::Domain {
                name: create.name.clone(),
                schema: create.schema.clone(),
                base_type: format!("{:?}", create.data_type),
                constraints: vec![], // TODO: Parse domain constraints
                default: None,
                not_null: false,
                comment: None,
            };
            schema.domains.insert(domain.name.clone(), domain);
        }
        ParserStatement::CreateSequence(create) => {
            let sequence = shem_core::Sequence {
                name: create.name.clone(),
                schema: create.schema.clone(),
                data_type: "bigint".to_string(),
                start: create.start.unwrap_or(1),
                increment: create.increment.unwrap_or(1),
                min_value: create.min_value,
                max_value: create.max_value,
                cache: create.cache.unwrap_or(1),
                cycle: create.cycle,
                owned_by: None,
                comment: None,
            };
            schema.sequences.insert(sequence.name.clone(), sequence);
        }
        ParserStatement::CreateExtension(create) => {
            let extension = shem_core::Extension {
                name: create.name.clone(),
                schema: create.schema.clone(),
                version: create.version.clone().unwrap_or_default(),
                cascade: false,
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
                events: vec![shem_core::TriggerEvent::Insert], // Default
                function: create.function.clone(),
                arguments: create.arguments.clone(),
                condition: None,
                for_each: shem_core::TriggerLevel::Row,
                comment: None,
            };
            schema.triggers.insert(trigger.name.clone(), trigger);
        }
        ParserStatement::CreatePolicy(create) => {
            let policy = shem_core::Policy {
                name: create.name.clone(),
                table: create.table.clone(),
                schema: None,
                command: match create.command {
                    PolicyCommand::All => shem_core::PolicyCommand::All,
                    PolicyCommand::Select => shem_core::PolicyCommand::Select,
                    PolicyCommand::Insert => shem_core::PolicyCommand::Insert,
                    PolicyCommand::Update => shem_core::PolicyCommand::Update,
                    PolicyCommand::Delete => shem_core::PolicyCommand::Delete,
                },
                permissive: create.permissive,
                roles: create.roles.clone(),
                using: create.using.as_ref().map(|u| format!("{:?}", u)),
                check: create.with_check.as_ref().map(|c| format!("{:?}", c)),
            };
            schema.policies.insert(policy.name.clone(), policy);
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
                        shem_parser::ast::AlterTableAction::AddConstraint(constraint) => {
                            match constraint {
                                shem_parser::ast::TableConstraint::PrimaryKey { columns, name } => {
                                    if !columns.is_empty() {
                                        let c = shem_core::Constraint {
                                            name: name.clone().unwrap_or_default(),
                                            kind: shem_core::ConstraintKind::PrimaryKey,
                                            definition: format!(
                                                "PRIMARY KEY ({})",
                                                columns.join(", ")
                                            ),
                                            deferrable: false,
                                            initially_deferred: false,
                                        };
                                        table.constraints.push(c);
                                    }
                                }
                                shem_parser::ast::TableConstraint::Unique { columns, name } => {
                                    if !columns.is_empty() {
                                        let c = shem_core::Constraint {
                                            name: name.clone().unwrap_or_default(),
                                            kind: shem_core::ConstraintKind::Unique,
                                            definition: format!("UNIQUE ({})", columns.join(", ")),
                                            deferrable: false,
                                            initially_deferred: false,
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
