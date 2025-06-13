use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use anyhow::{Result, Context};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub database_url: Option<String>,
    pub schema_dir: PathBuf,
    pub migrations_dir: PathBuf,
    pub postgres: PostgresConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostgresConfig {
    pub search_path: Vec<String>,
    pub extensions: Vec<String>,
    pub exclude_tables: Vec<String>,
    pub exclude_schemas: Vec<String>,
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
        }
    }
}

impl Config {
    pub fn from_path(path: &Path) -> Result<Self> {
        let content = std::fs::read_to_string(path)?;
        let config = serde_yaml::from_str(&content)?;
        Ok(config)
    }
}

pub async fn load_config(path: &Path) -> Result<Config> {
    if !path.exists() {
        return Ok(Config::default());
    }
    
    let content = tokio::fs::read_to_string(path)
        .await
        .with_context(|| format!("Failed to read config file: {}", path.display()))?;
    
    let config: Config = if path.extension().and_then(|s| s.to_str()) == Some("json") {
        serde_json::from_str(&content)
            .with_context(|| "Failed to parse JSON config")?
    } else {
        serde_yaml::from_str(&content)
            .with_context(|| "Failed to parse YAML config")?
    };
    
    Ok(config)
}

pub async fn save_config(config: &Config, path: &Path) -> Result<()> {
    let content = serde_yaml::to_string(config)
        .with_context(|| "Failed to serialize config")?;
    
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await
            .with_context(|| format!("Failed to create config directory: {}", parent.display()))?;
    }
    
    tokio::fs::write(path, content).await
        .with_context(|| format!("Failed to write config file: {}", path.display()))?;
    
    Ok(())
}