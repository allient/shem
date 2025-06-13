use crate::config::Config;
use anyhow::{Context, Result};
use shem_parser::ast::Statement;
use shem_parser::parse_file;
use std::path::Path;
use tracing::{error, info};

pub async fn execute(path: &str, config: &Config) -> Result<()> {
    let path = Path::new(path);

    if !path.exists() {
        anyhow::bail!("Schema path does not exist: {}", path.display());
    }

    let mut has_errors = false;

    if path.is_file() {
        validate_file(path, &mut has_errors)?;
    } else if path.is_dir() {
        // Validate all .sql files in directory
        for entry in walkdir::WalkDir::new(path)
            .into_iter()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().map_or(false, |ext| ext == "sql"))
        {
            validate_file(entry.path(), &mut has_errors)?;
        }
    }

    if has_errors {
        anyhow::bail!("Schema validation failed");
    }

    info!("Schema validation successful");
    Ok(())
}

fn validate_file(path: &Path, has_errors: &mut bool) -> Result<()> {
    info!("Validating {}", path.display());

    match parse_file(path) {
        Ok(statements) => {
            // Validate each statement
            for (i, stmt) in statements.iter().enumerate() {
                if let Err(e) = validate_statement(stmt) {
                    error!("Error in {} at statement {}: {}", path.display(), i + 1, e);
                    *has_errors = true;
                }
            }
        }
        Err(e) => {
            error!("Failed to parse {}: {}", path.display(), e);
            *has_errors = true;
        }
    }

    Ok(())
}

fn validate_statement(stmt: &Statement) -> Result<()> {
    // TODO: Add more validation rules
    match stmt {
        Statement::CreateTable(create) => {
            if create.name.is_empty() {
                anyhow::bail!("Table name cannot be empty");
            }
            if create.columns.is_empty() {
                anyhow::bail!("Table must have at least one column");
            }
            // Validate column names are unique
            let mut names = std::collections::HashSet::new();
            for col in &create.columns {
                if !names.insert(&col.name) {
                    anyhow::bail!("Duplicate column name: {}", col.name);
                }
            }
        }
        Statement::CreateView(create) => {
            if create.name.is_empty() {
                anyhow::bail!("View name cannot be empty");
            }
            if create.query.is_empty() {
                anyhow::bail!("View query cannot be empty");
            }
        }
        Statement::CreateMaterializedView(create) => {
            if create.name.is_empty() {
                anyhow::bail!("Materialized view name cannot be empty");
            }
            if create.query.is_empty() {
                anyhow::bail!("Materialized view query cannot be empty");
            }
        }
        Statement::CreateFunction(create) => {
            if create.name.is_empty() {
                anyhow::bail!("Function name cannot be empty");
            }
            if create.body.is_empty() {
                anyhow::bail!("Function body cannot be empty");
            }
        }
        Statement::CreateProcedure(create) => {
            if create.name.is_empty() {
                anyhow::bail!("Procedure name cannot be empty");
            }
            if create.body.is_empty() {
                anyhow::bail!("Procedure body cannot be empty");
            }
        }
        Statement::CreateEnum(create) => {
            if create.name.is_empty() {
                anyhow::bail!("Enum name cannot be empty");
            }
            if create.values.is_empty() {
                anyhow::bail!("Enum must have at least one value");
            }
        }
        Statement::CreateType(create) => {
            if create.name.is_empty() {
                anyhow::bail!("Type name cannot be empty");
            }
            if create.attributes.is_empty() {
                anyhow::bail!("Type must have at least one attribute");
            }
        }
        Statement::CreateDomain(create) => {
            if create.name.is_empty() {
                anyhow::bail!("Domain name cannot be empty");
            }
        }
        Statement::CreateSequence(create) => {
            if create.name.is_empty() {
                anyhow::bail!("Sequence name cannot be empty");
            }
        }
        Statement::CreateExtension(create) => {
            if create.name.is_empty() {
                anyhow::bail!("Extension name cannot be empty");
            }
        }
        Statement::CreateTrigger(create) => {
            if create.name.is_empty() {
                anyhow::bail!("Trigger name cannot be empty");
            }
            if create.table.is_empty() {
                anyhow::bail!("Trigger table cannot be empty");
            }
            if create.function.is_empty() {
                anyhow::bail!("Trigger function cannot be empty");
            }
        }
        Statement::CreatePolicy(create) => {
            if create.name.is_empty() {
                anyhow::bail!("Policy name cannot be empty");
            }
            if create.table.is_empty() {
                anyhow::bail!("Policy table cannot be empty");
            }
        }
        Statement::CreateServer(create) => {
            if create.name.is_empty() {
                anyhow::bail!("Server name cannot be empty");
            }
            if create.foreign_data_wrapper.is_empty() {
                anyhow::bail!("Foreign data wrapper cannot be empty");
            }
        }
        _ => {} // Other statements don't need validation yet
    }

    Ok(())
}
