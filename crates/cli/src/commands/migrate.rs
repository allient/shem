use anyhow::{Result, Context};
use std::path::PathBuf;
use tracing::{info, warn, error};
use crate::config::Config;
use shem_core::{
    DatabaseDriver,
    DatabaseConnection,
    migration::Migration,
    Transaction,
};
use shem_postgres::PostgresDriver;
use std::fs;
use std::path::Path;
use serde_json;

pub async fn execute(
    migrations: PathBuf,
    database_url: Option<String>,
    dry_run: bool,
    config: &Config,
) -> Result<()> {
    let url = database_url.or_else(|| config.database_url.clone())
        .ok_or_else(|| anyhow::anyhow!("No database URL provided"))?;
    
    // Connect to database
    let driver = get_driver()?;
    let conn = driver.connect(&url).await?;
    
    // Create migrations table if it doesn't exist
    if !dry_run {
        create_migrations_table(&conn).await?;
    }
    
    // Get applied migrations
    let applied = if !dry_run {
        get_applied_migrations(&conn).await?
    } else {
        vec![]
    };
    
    // Find migration files
    let migration_files = find_migration_files(&migrations)?;
    
    // Apply pending migrations
    for file in migration_files {
        let name = file.file_stem()
            .and_then(|s| s.to_str())
            .ok_or_else(|| anyhow::anyhow!("Invalid migration filename"))?;
            
        if applied.contains(&name.to_string()) {
            info!("Migration {} already applied, skipping", name);
            continue;
        }
        
        info!("Applying migration {}", name);
        
        // Read and parse migration
        let content = fs::read_to_string(&file)?;
        let migration = parse_migration(&content)?;
        
        if dry_run {
            info!("Would apply migration {}:", name);
            for stmt in &migration.statements {
                info!("  {}", stmt);
            }
            continue;
        }
        
        // Begin transaction
        let tx = conn.begin().await?;
        
        // Apply migration
        for stmt in &migration.statements {
            tx.execute(stmt).await?;
        }
        
        // Record migration
        record_migration(&tx, name, &migration).await?;
        
        // Commit transaction
        tx.commit().await?;
        
        info!("Migration {} applied successfully", name);
    }
    
    Ok(())
}

async fn create_migrations_table(conn: &Box<dyn DatabaseConnection>) -> Result<()> {
    let sql = r#"
        CREATE TABLE IF NOT EXISTS schema_migrations (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL UNIQUE,
            applied_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    "#;
    conn.execute(sql).await?;
    Ok(())
}

async fn get_applied_migrations(conn: &Box<dyn DatabaseConnection>) -> Result<Vec<String>> {
    let rows = conn.query("SELECT name FROM schema_migrations ORDER BY id").await?;
    let mut migrations = Vec::with_capacity(rows.len());
    for row in rows {
        match row {
            serde_json::Value::Object(obj) => {
                if let Some(serde_json::Value::String(name)) = obj.get("name") {
                    migrations.push(name.clone());
                }
            }
            _ => continue,
        }
    }
    Ok(migrations)
}

fn find_migration_files(migrations_dir: &Path) -> Result<Vec<PathBuf>> {
    if !migrations_dir.exists() {
        anyhow::bail!("Migrations directory does not exist: {}", migrations_dir.display());
    }
    
    let mut files: Vec<_> = fs::read_dir(migrations_dir)?
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map_or(false, |ext| ext == "sql"))
        .map(|e| e.path())
        .collect();
        
    files.sort();
    Ok(files)
}

fn parse_migration(content: &str) -> Result<Migration> {
    // Split content into up and down migrations
    let parts: Vec<_> = content.split("-- migrate:down").collect();
    let up = parts[0].trim().to_string();
    let down = parts.get(1).map(|s| s.trim().to_string()).unwrap_or_default();
    
    // Split into statements
    let up_statements: Vec<_> = up.split(';')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(String::from)
        .collect();
        
    let down_statements: Vec<_> = down.split(';')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(String::from)
        .collect();
    
    Ok(Migration {
        version: chrono::Utc::now().format("%Y%m%d%H%M%S").to_string(),
        description: "Migration".to_string(),
        statements: up_statements,
        rollback_statements: down_statements,
        created_at: chrono::Utc::now(),
    })
}

async fn record_migration(tx: &Box<dyn Transaction>, name: &str, migration: &Migration) -> Result<()> {
    let sql = "INSERT INTO schema_migrations (name) VALUES ($1)";
    tx.execute(sql).await?;
    Ok(())
}

fn get_driver() -> Result<Box<dyn DatabaseDriver>> {
    Ok(Box::new(PostgresDriver::new()))
} 