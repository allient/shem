use clap::{Parser, Subcommand};
use anyhow::Result;
use tracing::{info, error};
use std::path::PathBuf;

mod commands;
mod config;

use crate::commands::*;
use crate::config::Config;

#[derive(Parser)]
#[command(name = "shem")]
#[command(about = "Declarative database schema management")]
#[command(version)]
pub struct Cli {
    /// Configuration file path
    #[arg(short, long)]
    pub config: Option<PathBuf>,
    
    /// Verbose output
    #[arg(short, long)]
    pub verbose: bool,
    
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Initialize a new schema project
    Init {
        /// Project directory
        #[arg(default_value = ".")]
        path: PathBuf,
    },
    /// Generate migration from schema changes
    Diff {
        /// Schema file or directory
        #[arg(short, long, default_value = "schema")]
        schema: PathBuf,
        /// Output migration file
        #[arg(short, long)]
        output: Option<PathBuf>,
        /// Database connection string
        #[arg(short, long)]
        database_url: Option<String>,
    },
    /// Apply migrations to database
    Migrate {
        /// Migration directory
        #[arg(short, long, default_value = "migrations")]
        migrations: PathBuf,
        /// Database connection string
        #[arg(short, long)]
        database_url: Option<String>,
        /// Dry run - show what would be executed
        #[arg(long)]
        dry_run: bool,
    },
    /// Validate schema files
    Validate {
        /// Schema file or directory
        #[arg(short, long, default_value = "schema")]
        schema: PathBuf,
    },
    /// Introspect database and generate schema
    Introspect {
        /// Database connection string
        #[arg(long)]
        database_url: String,
        /// Output directory
        #[arg(short, long, default_value = "schema")]
        output: PathBuf,
    },
    /// Show schema information
    Inspect {
        /// Schema file or directory
        #[arg(short, long, default_value = "schema")]
        schema: PathBuf,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    
    // Initialize tracing
    let level = if cli.verbose { "debug" } else { "info" };
    tracing_subscriber::fmt()
        .with_env_filter(format!("shem={}", level))
        .init();
    
    info!("Starting shem CLI");
    
    // Load config from file or use defaults
    let config = if let Some(config_path) = cli.config {
        Config::from_path(&config_path)?
    } else {
        Config::default()
    };
    
    // Execute command
    let result = match cli.command {
        Commands::Init { path } => commands::init::execute(path.to_str().unwrap(), &config).await,
        Commands::Diff { schema, output, database_url } => {
            commands::diff::execute(
                schema,
                output,
                database_url.or_else(|| config.database_url.clone()),
                &config
            ).await
        }
        Commands::Migrate { migrations, database_url, dry_run } => {
            commands::migrate::execute(
                migrations,
                database_url.or_else(|| config.database_url.clone()),
                dry_run,
                &config
            ).await
        }
        Commands::Validate { schema } => commands::validate::execute(schema.to_str().unwrap(), &config).await,
        Commands::Introspect { database_url, output } => {
            commands::introspect::execute(database_url, output, &config).await
        }
        Commands::Inspect { schema } => commands::inspect::execute(schema.to_str().unwrap(), &config).await,
    };
    
    if let Err(e) = result {
        error!("Command failed: {}", e);
        std::process::exit(1);
    }
    
    Ok(())
}