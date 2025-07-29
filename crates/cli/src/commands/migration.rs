use crate::config::Config;
use anyhow::Result;
use postgres::PostgresDriver;
use serde_json;
use shem_core::{DatabaseConnection, DatabaseDriver, traits::Transaction};
use std::fs;
use std::path::Path;
use std::path::PathBuf;
use tracing::info;

pub async fn execute(
    migrations: PathBuf,
    database_url: Option<String>,
    dry_run: bool,
    config: &Config,
) -> Result<()> {
    unimplemented!()
}
