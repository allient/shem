use anyhow::{Result, Context};
use std::path::Path;
use tracing::info;
use shem_parser::parse_file;
use shem_parser::ast::Statement;
use crate::config::Config;

pub async fn execute(path: &str, config: &Config) -> Result<()> {
    let path = Path::new(path);
    
    if !path.exists() {
        anyhow::bail!("Schema path does not exist: {}", path.display());
    }
    
    let mut stats = SchemaStats::new();
    
    if path.is_file() {
        inspect_file(path, &mut stats)?;
    } else if path.is_dir() {
        // Inspect all .sql files in directory
        for entry in walkdir::WalkDir::new(path)
            .into_iter()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().map_or(false, |ext| ext == "sql"))
        {
            inspect_file(entry.path(), &mut stats)?;
        }
    }
    
    // Print schema statistics
    info!("Schema Statistics:");
    info!("  Tables: {}", stats.tables);
    info!("  Views: {}", stats.views);
    info!("  Materialized Views: {}", stats.materialized_views);
    info!("  Functions: {}", stats.functions);
    info!("  Procedures: {}", stats.procedures);
    info!("  Types: {}", stats.enums);
    info!("  Domains: {}", stats.domains);
    info!("  Sequences: {}", stats.sequences);
    info!("  Extensions: {}", stats.extensions);
    info!("  Triggers: {}", stats.triggers);
    info!("  Policies: {}", stats.policies);
    info!("  Foreign Servers: {}", stats.servers);
    
    // Print object names
    if !stats.table_names.is_empty() {
        info!("\nTables:");
        for name in stats.table_names {
            info!("  {}", name);
        }
    }
    
    if !stats.view_names.is_empty() {
        info!("\nViews:");
        for name in stats.view_names {
            info!("  {}", name);
        }
    }
    
    if !stats.function_names.is_empty() {
        info!("\nFunctions:");
        for name in stats.function_names {
            info!("  {}", name);
        }
    }
    
    if !stats.enum_names.is_empty() {
        info!("\nEnums:");
        for name in stats.enum_names {
            info!("  {}", name);
        }
    }
    
    Ok(())
}

#[derive(Default)]
struct SchemaStats {
    tables: usize,
    views: usize,
    materialized_views: usize,
    functions: usize,
    procedures: usize,
    enums: usize,
    domains: usize,
    sequences: usize,
    extensions: usize,
    triggers: usize,
    policies: usize,
    servers: usize,
    table_names: Vec<String>,
    view_names: Vec<String>,
    function_names: Vec<String>,
    enum_names: Vec<String>,
}

impl SchemaStats {
    fn new() -> Self {
        Self::default()
    }
}

fn inspect_file(path: &Path, stats: &mut SchemaStats) -> Result<()> {
    info!("Inspecting {}", path.display());
    
    let statements = parse_file(path)?;
    
    for stmt in statements {
        match stmt {
            Statement::CreateTable(create) => {
                stats.tables += 1;
                stats.table_names.push(create.name);
            }
            Statement::CreateView(create) => {
                stats.views += 1;
                stats.view_names.push(create.name);
            }
            Statement::CreateMaterializedView(create) => {
                stats.materialized_views += 1;
                stats.view_names.push(create.name);
            }
            Statement::CreateFunction(create) => {
                stats.functions += 1;
                stats.function_names.push(create.name);
            }
            Statement::CreateProcedure(create) => {
                stats.procedures += 1;
                stats.function_names.push(create.name);
            }
            Statement::CreateEnum(create) => {
                stats.enums += 1;
                stats.enum_names.push(create.name);
            }
            Statement::CreateType(create) => {
                stats.enums += 1;
            }
            Statement::CreateDomain(create) => {
                stats.domains += 1;
            }
            Statement::CreateSequence(create) => {
                stats.sequences += 1;
            }
            Statement::CreateExtension(create) => {
                stats.extensions += 1;
            }
            Statement::CreateTrigger(create) => {
                stats.triggers += 1;
            }
            Statement::CreatePolicy(create) => {
                stats.policies += 1;
            }
            Statement::CreateServer(create) => {
                stats.servers += 1;
            }
            _ => {}
        }
    }
    
    Ok(())
} 