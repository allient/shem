use std::fmt;
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
}

pub type Result<T> = std::result::Result<T, Error>; 