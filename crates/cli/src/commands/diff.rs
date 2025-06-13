use crate::config::Config;
use anyhow::{Context, Result};
use shem_core::{
    DatabaseDriver, Schema,
    migration::{generate_migration, write_migration},
};
use shem_parser::{
    ast::{
        CheckOption, ParameterMode, Statement as ParserStatement, TableConstraint, TriggerEvent,
        TriggerWhen,
    },
    parse_file,
};
use shem_postgres::PostgresDriver;
use std::path::PathBuf;
use tracing::{info, warn};
use std::collections::BTreeMap;

pub async fn execute(
    schema: PathBuf,
    output: Option<PathBuf>,
    database_url: Option<String>,
    config: &Config,
) -> Result<()> {
    // Load schema from files
    let target_schema = load_schema(&schema)?;

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
        PathBuf::from(format!("migrations/{}.sql", timestamp))
    });

    // Create migrations directory if it doesn't exist
    if let Some(parent) = output_path.parent() {
        std::fs::create_dir_all(parent).context("Failed to create migrations directory")?;
    }

    write_migration(&output_path, &migration)?;
    info!("Migration written to {}", output_path.display());

    Ok(())
}

fn load_schema(path: &PathBuf) -> Result<Schema> {
    let mut schema = Schema::new();

    if path.is_file() {
        // Load single schema file
        info!("Loading schema from file: {}", path.display());
        let statements = parse_file(path)?;
        for stmt in statements {
            add_statement_to_schema(&mut schema, &stmt)?;
        }
    } else if path.is_dir() {
        // Load all .sql files in directory, ordered by filename
        info!("Loading schemas from directory: {}", path.display());
        
        // Use BTreeMap to maintain order by filename
        let mut ordered_files = BTreeMap::new();
        
        // First, collect all SQL files and their paths
        for entry in walkdir::WalkDir::new(path)
            .into_iter()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().map_or(false, |ext| ext == "sql"))
        {
            let path = entry.path().to_path_buf();
            let filename = path.file_name()
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
        anyhow::bail!("Schema path does not exist: {}", path.display());
    }

    Ok(schema)
}

fn add_statement_to_schema(schema: &mut Schema, stmt: &ParserStatement) -> Result<()> {
    match stmt {
        ParserStatement::CreateTable(create) => {
            let mut table = shem_core::Table {
                name: create.name.clone(),
                schema: create.schema.clone(),
                columns: Vec::new(),
                constraints: Vec::new(),
                indexes: Vec::new(),
            };

            // Add columns
            for col in &create.columns {
                let column = shem_core::Column {
                    name: col.name.clone(),
                    type_name: format!("{:?}", col.data_type),
                    nullable: !col.not_null,
                    default: col.default.as_ref().map(|d| format!("{:?}", d)),
                    identity: col.identity.as_ref().map(|i| shem_core::Identity {
                        always: i.always,
                        start: i.start.unwrap_or(1),
                        increment: i.increment.unwrap_or(1),
                        min_value: i.min_value,
                        max_value: i.max_value,
                    }),
                    generated: col.generated.as_ref().map(|g| shem_core::GeneratedColumn {
                        expression: format!("{:?}", g.expression),
                        stored: g.stored,
                    }),
                };
                table.columns.push(column);
            }

            // Add constraints
            for constraint in &create.constraints {
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
                        TableConstraint::ForeignKey { .. } => shem_core::ConstraintKind::ForeignKey,
                        TableConstraint::Unique { .. } => shem_core::ConstraintKind::Unique,
                        TableConstraint::Check { .. } => shem_core::ConstraintKind::Check,
                        TableConstraint::Exclusion { .. } => shem_core::ConstraintKind::Exclusion,
                    },
                    definition: format!("{:?}", constraint),
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
            };
            schema.views.insert(view.name.clone(), view);
        }
        ParserStatement::CreateMaterializedView(create) => {
            let view = shem_core::MaterializedView {
                name: create.name.clone(),
                schema: create.schema.clone(),
                definition: create.query.clone(),
                check_option: shem_core::CheckOption::None, // Materialized views don't have check options
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
            };
            schema.procedures.insert(procedure.name.clone(), procedure);
        }
        ParserStatement::CreateEnum(create) => {
            let type_ = shem_core::Type {
                name: create.name.clone(),
                schema: create.schema.clone(),
                kind: shem_core::TypeKind::Enum,
            };
            schema.types.insert(type_.name.clone(), type_);
        }
        ParserStatement::CreateType(create) => {
            let type_ = shem_core::Type {
                name: create.name.clone(),
                schema: create.schema.clone(),
                kind: shem_core::TypeKind::Composite,
            };
            schema.types.insert(type_.name.clone(), type_);
        }
        ParserStatement::CreateDomain(create) => {
            let domain = shem_core::Domain {
                name: create.name.clone(),
                schema: create.schema.clone(),
                base_type: format!("{:?}", create.data_type),
                constraints: vec![], // TODO: Parse domain constraints
            };
            schema.domains.insert(domain.name.clone(), domain);
        }
        ParserStatement::CreateSequence(create) => {
            let sequence = shem_core::Sequence {
                name: create.name.clone(),
                schema: create.schema.clone(),
                start: create.start.unwrap_or(1),
                increment: create.increment.unwrap_or(1),
                min_value: create.min_value,
                max_value: create.max_value,
                cache: create.cache.unwrap_or(1),
                cycle: create.cycle,
            };
            schema.sequences.insert(sequence.name.clone(), sequence);
        }
        ParserStatement::CreateExtension(create) => {
            let extension = shem_core::Extension {
                name: create.name.clone(),
                schema: create.schema.clone(),
                version: create.version.clone().unwrap_or_default(),
            };
            schema.extensions.insert(extension.name.clone(), extension);
        }
        ParserStatement::CreateTrigger(create) => {
            let trigger = shem_core::Trigger {
                name: create.name.clone(),
                table: create.table.clone(),
                timing: match create.when {
                    TriggerWhen::Before => shem_core::TriggerTiming::Before,
                    TriggerWhen::After => shem_core::TriggerTiming::After,
                    TriggerWhen::InsteadOf => shem_core::TriggerTiming::InsteadOf,
                },
                events: create
                    .events
                    .iter()
                    .map(|event| match event {
                        TriggerEvent::Insert => shem_core::TriggerEvent::Insert,
                        TriggerEvent::Update => shem_core::TriggerEvent::Update,
                        TriggerEvent::Delete => shem_core::TriggerEvent::Delete,
                        TriggerEvent::Truncate => shem_core::TriggerEvent::Truncate,
                    })
                    .collect(),
                function: create.function.clone(),
                arguments: create.arguments.clone(),
            };
            schema.triggers.insert(trigger.name.clone(), trigger);
        }
        ParserStatement::CreatePolicy(create) => {
            let policy = shem_core::Policy {
                name: create.name.clone(),
                table: create.table.clone(),
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
            };
            schema.servers.insert(server.name.clone(), server);
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
