use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Database error: {0}")]
    Database(#[from] DatabaseError),
    
    #[error("Schema error: {0}")]
    Schema(#[from] SchemaError),
    
    #[error("Migration error: {0}")]
    Migration(#[from] MigrationError),
    
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    
    #[error("Serialization error: {0}")]
    Serialization(String),
    
    #[error("Configuration error: {0}")]
    Config(String),
    
    #[error("Validation error: {0}")]
    Validation(String),
    
    #[error("Connection error: {0}")]
    Connection(String),
    
    #[error("Feature not supported: {0}")]
    Unsupported(String),
    
    #[error("Parse error: {0}")]
    Parse(String),
    
    #[error("Internal error: {0}")]
    Internal(String),
}

#[derive(Error, Debug)]
pub enum DatabaseError {
    #[error("Connection failed: {0}")]
    ConnectionFailed(String),
    
    #[error("Query failed: {0}")]
    QueryFailed(String),
    
    #[error("Transaction failed: {0}")]
    TransactionFailed(String),
    
    #[error("Schema not foun