use anyhow::{Context, Result};
use glob::glob;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub database_url: Option<String>,
    pub schema_dir: PathBuf,
    pub migrations_dir: PathBuf,
    pub postgres: PostgresConfig,
    pub declarative: DeclarativeConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostgresConfig {
    pub search_path: Vec<String>,
    pub extensions: Vec<String>,
    pub exclude_tables: Vec<String>,
    pub exclude_schemas: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeclarativeConfig {
    pub enabled: bool,
    pub schema_paths: Vec<String>,
    pub shadow_port: u16,
    pub auto_cleanup: bool,
    pub safety_checks: SafetyConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SafetyConfig {
    pub warn_on_drop: bool,
    pub require_confirmation: bool,
    pub backup_before_apply: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            database_url: None,
            schema_dir: PathBuf::from("schema"),
            migrations_dir: PathBuf::from("migrations"),
            postgres: PostgresConfig {
                search_path: vec!["public".to_string()],
                extensions: vec![],
                exclude_tables: vec![],
                exclude_schemas: vec!["information_schema".to_string(), "pg_catalog".to_string()],
            },
            declarative: DeclarativeConfig {
                enabled: true,
                schema_paths: vec!["./schema/*.sql".to_string()],
                shadow_port: 5433,
                auto_cleanup: true,
                safety_checks: SafetyConfig {
                    warn_on_drop: true,
                    require_confirmation: true,
                    backup_before_apply: false,
                },
            },
        }
    }
}

impl Config {
    pub fn from_path(path: &Path) -> Result<Self> {
        // 🔐 Reject unsupported formats
        if path.extension().and_then(|s| s.to_str()) != Some("toml") {
            return Err(anyhow::anyhow!("Only .toml config files are supported"));
        }

        let content = std::fs::read_to_string(path)
            .with_context(|| format!("Failed to read config file: {}", path.display()))?;

        toml::from_str(&content).with_context(|| "Failed to parse TOML config")
    }

    pub fn load_schema_files(&self) -> Result<Vec<PathBuf>> {
        let mut files = Vec::new();
        let mut seen = HashSet::new();

        for pattern in &self.declarative.schema_paths {
            let matches =
                glob(pattern).with_context(|| format!("Invalid glob pattern: {}", pattern))?;

            for entry in matches {
                let path =
                    entry.with_context(|| format!("Failed to read glob pattern: {}", pattern))?;

                if !seen.contains(&path) {
                    seen.insert(path.clone());
                    files.push(path);
                }
            }
        }

        files.sort_by(|a, b| {
            a.file_name()
                .unwrap_or_default()
                .cmp(&b.file_name().unwrap_or_default())
        });

        Ok(files)
    }
}