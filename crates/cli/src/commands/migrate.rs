use anyhow::{Result, Context};
use std::path::PathBuf;
use tracing::{info, warn};
use crate::config::Config;
use shem_core::{
    DatabaseDriver,
    DatabaseConnection,
    Transaction,
    Schema,
    traits::SqlGenerator,
    Statement
};
use shem_postgres::PostgresDriver;

pub async fn execute(
    migrations: PathBuf,
    database_url: Option<String>,
    dry_run: bool,
    config: &Config,
) -> Result<()> {
    // Get database URL
    let url = database_url.or_else(|| config.database_url.clone())
        .context("No database URL provided")?;
    
    // Connect to database
    let driver = get_driver()?;
    let conn: Box<dyn DatabaseConnection> = driver.connect(<std::string::String as AsRef<str>>::as_ref(&url)).await?;
    
    // Create migrations table if it doesn't exist
    create_migrations_table(conn.as_ref()).await?;
    
    // Get applied migrations
    let applied = get_applied_migrations(conn.as_ref()).await?;
    
    // Get migration files
    let files = get_migration_files(&migrations)?;
    
    // Filter out already applied migrations
    let pending: Vec<_> = files.into_iter()
        .filter(|f| !applied.contains(&f.id))
        .collect();
    
    if pending.is_empty() {
        info!("No pending migrations");
        return Ok(());
    }
    
    // Sort migrations by dependencies
    let sorted = sort_migrations(&pending)?;
    
    // Apply migrations
    if dry_run {
        info!("Dry run - would apply {} migrations:", sorted.len());
        for migration in &sorted {
            info!("  {}: {}", migration.id, migration.name);
            for stmt in &migration.up {
                info!("    {}", stmt);
            }
        }
    } else {
        let mut tx = conn.begin().await?;
        
        for migration in &sorted {
            info!("Applying migration {}: {}", migration.id, migration.name);
            
            // Execute up migration
            for stmt in &migration.up {
                tx.execute(stmt).await?;
            }
            
            // Record migration
            record_migration(&mut tx, &migration.id, &migration.name).await?;
        }
        
        tx.commit().await?;
        info!("Applied {} migrations", sorted.len());
    }
    
    Ok(())
}

async fn create_migrations_table(conn: &dyn DatabaseConnection) -> Result<()> {
    conn.execute(
        r#"
        CREATE TABLE IF NOT EXISTS schema_migrations (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        "#
    ).await?;
    
    Ok(())
}

async fn get_applied_migrations(conn: &dyn DatabaseConnection) -> Result<Vec<String>> {
    let rows = conn.query(
        "SELECT id FROM schema_migrations ORDER BY applied_at"
    ).await?;
    
    let mut migrations = Vec::new();
    for row in rows {
        if let Some(id) = row.get("id").and_then(|v| v.as_str()) {
            migrations.push(id.to_string());
        }
    }
    
    Ok(migrations)
}

fn get_migration_files(dir: &PathBuf) -> Result<Vec<Migration>> {
    let mut files = Vec::new();
    
    for entry in walkdir::WalkDir::new(dir)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map_or(false, |ext| ext == "sql"))
    {
        let path = entry.path();
        let content = std::fs::read_to_string(path)
            .context("Failed to read migration file")?;
            
        let migration = parse_migration_file(path, &content)?;
        files.push(migration);
    }
    
    // Sort by ID (timestamp)
    files.sort_by(|a, b| a.id.cmp(&b.id));
    
    Ok(files)
}

fn parse_migration_file(path: &std::path::Path, content: &str) -> Result<Migration> {
    // Extract migration ID from filename
    let id = path.file_stem()
        .and_then(|s| s.to_str())
        .context("Invalid migration filename")?
        .to_string();
        
    // Parse migration name from header
    let name = content.lines()
        .find(|line| line.starts_with("-- Migration:"))
        .and_then(|line| line.split_once(":"))
        .map(|(_, name)| name.trim().to_string())
        .unwrap_or_else(|| format!("migration_{}", id));
        
    // Split content into up and down migrations
    let parts: Vec<_> = content.split("-- Down Migration").collect();
    let up = parts[0]
        .lines()
        .filter(|line| !line.starts_with("--"))
        .map(|line| line.trim().to_string())
        .filter(|line| !line.is_empty())
        .collect();
        
    let down = if parts.len() > 1 {
        parts[1]
            .lines()
            .filter(|line| !line.starts_with("--"))
            .map(|line| line.trim().to_string())
            .filter(|line| !line.is_empty())
            .collect()
    } else {
        Vec::new()
    };
    
    Ok(Migration {
        id,
        name,
        up,
        down,
        dependencies: Vec::new(), // TODO: Parse dependencies from comments
    })
}

async fn record_migration(tx: &mut Box<dyn Transaction>, id: &str, name: &str) -> Result<()> {
    tx.execute(&format!(
        "INSERT INTO schema_migrations (id, name) VALUES ('{}', '{}')",
        id, name
    )).await?;
    
    Ok(())
}

fn sort_migrations(migrations: &[Migration]) -> Result<Vec<Migration>> {
    // TODO: Implement topological sort based on dependencies
    Ok(migrations.to_vec())
}

fn get_driver() -> Result<Box<dyn DatabaseDriver>> {
    Ok(Box::new(PostgresDriver::new()))
}

#[derive(Debug, Clone)]
struct Migration {
    id: String,
    name: String,
    up: Vec<String>,
    down: Vec<String>,
    dependencies: Vec<String>,
} 