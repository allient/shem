use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Database error: {0}")]
    Database(String),

    #[error("Schema error: {0}")]
    Schema(String),

    #[error("Migration error: {0}")]
    Migration(String),

    #[error("SQL generation error: {0}")]
    SqlGeneration(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("Anyhow error: {0}")]
    Anyhow(#[from] anyhow::Error),

    #[error("PostgreSQL error: {0}")]
    Postgres(#[from] tokio_postgres::Error),
}

pub type Result<T> = std::result::Result<T, Error>; 