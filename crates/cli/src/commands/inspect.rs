use crate::config::Config;
use anyhow::{Result, bail};
use shem_parser::ast::Statement;
use shem_parser::parse_file;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use tracing::info;

fn resolve_and_check(path_str: &str, base_dir: &Path) -> Result<PathBuf> {
    let path = fs::canonicalize(path_str)?;
    if !path.starts_with(base_dir) {
        bail!("Access denied: path outside allowed directory");
    }
    Ok(path)
}

pub async fn execute(path: &str, config: &Config) -> Result<()> {
    let base_dir = std::env::current_dir()?;
    let path = resolve_and_check(path, &base_dir)?;

    if !path.exists() {
        bail!("Schema path does not exist: {}", path.display());
    }

    let mut stats = SchemaStats::default();

    let sql_files: Vec<_> = if path.is_file() {
        vec![path.to_path_buf()]
    } else {
        walkdir::WalkDir::new(path)
            .into_iter()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().map_or(false, |ext| ext == "sql"))
            .map(|e| e.path().to_path_buf())
            .collect()
    };

    info!("Found {} SQL files", sql_files.len());

    for file in sql_files {
        inspect_file(&file, &mut stats)?;
    }

    stats.print_summary();
    Ok(())
}

#[derive(Default)]
struct SchemaStats {
    counters: HashMap<&'static str, usize>,
    named_lists: HashMap<&'static str, Vec<String>>,
}

impl SchemaStats {
    fn count(&mut self, category: &'static str) {
        *self.counters.entry(category).or_default() += 1;
    }

    fn add_name(&mut self, category: &'static str, name: String) {
        self.named_lists.entry(category).or_default().push(name);
    }

    fn print_summary(&self) {
        info!("Schema Statistics:");
        for (key, count) in &self.counters {
            info!("  {}: {}", key, count);
        }

        for (key, names) in &self.named_lists {
            if !names.is_empty() {
                info!("\n{}:", capitalize(key));
                for name in names {
                    info!("  {}", name);
                }
            }
        }
    }
}

fn inspect_file(path: &Path, stats: &mut SchemaStats) -> Result<()> {
    info!("Inspecting {}", path.display());
    let statements = parse_file(path)?;

    for stmt in statements {
        match stmt {
            Statement::CreateTable(c) => {
                stats.count("tables");
                stats.add_name("tables", c.name);
            }
            Statement::CreateView(c) => {
                stats.count("views");
                stats.add_name("views", c.name);
            }
            Statement::CreateMaterializedView(c) => {
                stats.count("materialized_views");
                stats.add_name("views", c.name); // same list
            }
            Statement::CreateFunction(c) => {
                stats.count("functions");
                stats.add_name("functions", c.name);
            }
            Statement::CreateProcedure(c) => {
                stats.count("procedures");
                stats.add_name("procedures", c.name);
            }
            Statement::CreateEnum(c) => {
                stats.count("enums");
                stats.add_name("enums", c.name);
            }
            Statement::CreateType(_) => stats.count("types"),
            Statement::CreateDomain(_) => stats.count("domains"),
            Statement::CreateSequence(_) => stats.count("sequences"),
            Statement::CreateExtension(_) => stats.count("extensions"),
            Statement::CreateTrigger(_) => stats.count("triggers"),
            Statement::CreatePolicy(_) => stats.count("policies"),
            Statement::CreateServer(_) => stats.count("servers"),
            _ => {}
        }
    }

    Ok(())
}

fn capitalize(s: &str) -> String {
    let mut chars = s.chars();
    match chars.next() {
        Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
        None => String::new(),
    }
}
