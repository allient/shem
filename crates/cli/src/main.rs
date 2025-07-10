use anyhow::Result;
use clap::{Parser, Subcommand};
use std::path::PathBuf;
use tracing::{error, info};

mod commands;
mod config;
use crate::{commands::*, config::Config};

#[derive(Parser, Debug)]
#[command(
    name = "shem",
    version,
    about = "Declarative database schema management",
    author
)]
pub struct Cli {
    /// Configuration file path
    #[arg(short, long)]
    pub config: Option<PathBuf>,

    /// Verbose output
    #[arg(short, long, default_value = "false")]
    pub verbose: bool,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    /// Initialize a new schema project
    Init {
        /// Project directory
        #[arg(default_value = "db_schema")]
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
        /// Migration name (will be used in filename)
        #[arg(short, long)]
        name: Option<String>,
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
        database_url: Option<String>,
        /// Output directory
        #[arg(short, long, default_value = "schema")]
        output: PathBuf,
        /// Verbose output
        #[arg(short, long)]
        verbose: bool,
    },
    /// Show schema information
    Inspect {
        /// Schema file or directory
        #[arg(short, long, default_value = "schema")]
        schema: PathBuf,
    },
}

fn find_config_file() -> Option<PathBuf> {
    // Look for config files in current directory
    let config_files = ["shem.toml"];

    for filename in config_files {
        let path = PathBuf::from(filename);
        if path.exists() {
            return Some(path);
        }
    }

    None
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
    } else if let Some(config_path) = find_config_file() {
        info!("Found config file: {}", config_path.display());
        Config::from_path(&config_path)?
    } else {
        info!("No config file found, using defaults");
        Config::default()
    };

    // Execute command
    let result = match cli.command {
        Command::Init { path } => init::execute(path, &config).await,
        Command::Diff {
            schema,
            output,
            database_url,
            name,
        } => {
            diff::execute(
                schema,
                output,
                database_url.or_else(|| config.database_url.clone()),
                name,
                &config,
            )
            .await
        }
        Command::Migrate {
            migrations,
            database_url,
            dry_run,
        } => {
            migrate::execute(
                migrations,
                database_url.or_else(|| config.database_url.clone()),
                dry_run,
                &config,
            )
            .await
        }
        Command::Validate { schema } => validate::execute(schema.to_str().unwrap(), &config).await,
        Command::Introspect {
            database_url,
            output,
            verbose,
        } => introspect::execute(
            database_url.or_else(|| config.database_url.clone()),
            output,
            &config,
            verbose,
        )
        .await,
        Command::Inspect { schema } => inspect::execute(schema.to_str().unwrap(), &config).await,
    };

    match result {
        Ok(_) => info!("Command completed successfully"),
        Err(e) => {
            error!("Command failed: {}", e);
            std::process::exit(1);
        }
    }

    Ok(())
}
