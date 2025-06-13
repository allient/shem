use std::fmt::Debug;
use serde::{Serialize, Deserialize};
use async_trait::async_trait;
use anyhow::Result;

pub mod error;
pub mod traits;
pub mod schema;
pub mod migration;

pub use error::*;
pub use schema::*;  // This re-exports all types from schema.rs
pub use traits::*;

// Migration-specific types that are not part of the schema
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Migration {
    pub version: String,
    pub description: String,
    pub statements: Vec<String>,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Statement {
    pub sql: String,
    pub description: Option<String>,
}