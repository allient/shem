use async_trait::async_trait;
use std::fmt::Debug;
use crate::*;
use crate::error::Result;

use crate::schema::{
    Schema, Table, View, MaterializedView, Function, Procedure,
    EnumType, Domain, Sequence, Extension, Trigger, Policy, Server,
    Column, Constraint, Index, Parameter, ReturnType,
    TypeKind, ConstraintKind, IndexColumn, Identity, CheckOption,
    ParameterMode, SortOrder, TriggerTiming, TriggerEvent, ReturnKind
};

/// Database driver trait
#[async_trait]
pub trait DatabaseDriver: Send + Sync {
    /// Get the driver name
    fn name(&self) -> &str;
    
    /// Get the SQL dialect
    fn dialect(&self) -> &str;
    
    /// Get supported features
    fn features(&self) -> &[Feature];
    
    /// Get supported data types
    fn data_types(&self) -> Vec<String>;
    
    /// Get SQL generator
    fn sql_generator(&self) -> Box<dyn SqlGenerator>;
    
    /// Connect to the database
    async fn connect(&self, url: &str) -> Result<Box<dyn DatabaseConnection>>;
}

/// Database connection trait
#[async_trait]
pub trait DatabaseConnection: Send + Sync {
    /// Get the driver
    fn driver(&self) -> &dyn DatabaseDriver;
    
    /// Introspect the database schema
    async fn introspect(&self) -> Result<Schema>;
    
    /// Execute SQL statement
    async fn execute(&self, sql: &str) -> Result<()>;
    
    /// Execute SQL query
    async fn query(&self, sql: &str) -> Result<Vec<serde_json::Value>>;
    
    /// Begin transaction
    async fn begin(&self) -> Result<Box<dyn Transaction>>;
    
    /// Close connection
    async fn close(self: Box<Self>) -> Result<()>;
    
    /// Get connection metadata
    async fn metadata(&self) -> Result<ConnectionMetadata>;
}

/// Transaction trait
#[async_trait]
pub trait Transaction: Send + Sync {
    /// Execute SQL statement
    async fn execute(&self, sql: &str) -> Result<()>;
    
    /// Execute SQL query
    async fn query(&self, sql: &str) -> Result<Vec<serde_json::Value>>;
    
    /// Commit transaction
    async fn commit(self: Box<Self>) -> Result<()>;
    
    /// Rollback transaction
    async fn rollback(self: Box<Self>) -> Result<()>;
}

/// SQL generator trait
#[async_trait]
pub trait SqlGenerator: Send + Sync {
    /// Generate CREATE TABLE SQL
    fn generate_create_table(&self, table: &crate::Table) -> Result<String>;
    
    /// Generate ALTER TABLE SQL
    fn generate_alter_table(&self, old: &crate::Table, new: &crate::Table) -> Result<(Vec<String>, Vec<String>)>;
    
    /// Generate DROP TABLE SQL
    fn generate_drop_table(&self, table: &crate::Table) -> Result<String>;
    
    /// Generate CREATE VIEW SQL
    fn create_view(&self, view: &View) -> Result<String>;
    
    /// Generate CREATE MATERIALIZED VIEW SQL
    fn create_materialized_view(&self, view: &MaterializedView) -> Result<String>;
    
    /// Generate CREATE FUNCTION SQL
    fn create_function(&self, func: &Function) -> Result<String>;
    
    /// Generate CREATE PROCEDURE SQL
    fn create_procedure(&self, proc: &Procedure) -> Result<String>;
    
    /// Generate CREATE TYPE SQL
    fn generate_create_type(&self, type_def: &crate::Type) -> Result<String>;
    
    /// Generate CREATE ENUM SQL
    fn create_enum(&self, enum_type: &EnumType) -> Result<String>;
    
    /// Generate ALTER ENUM SQL
    fn alter_enum(&self, old: &EnumType, new: &EnumType) -> Result<(Vec<String>, Vec<String>)>;
    
    /// Generate CREATE DOMAIN SQL
    fn create_domain(&self, domain: &Domain) -> Result<String>;
    
    /// Generate CREATE SEQUENCE SQL
    fn create_sequence(&self, seq: &Sequence) -> Result<String>;
    
    /// Generate ALTER SEQUENCE SQL
    fn alter_sequence(&self, old: &Sequence, new: &Sequence) -> Result<(Vec<String>, Vec<String>)>;
    
    /// Generate CREATE EXTENSION SQL
    fn create_extension(&self, ext: &Extension) -> Result<String>;
    
    /// Generate CREATE TRIGGER SQL
    fn create_trigger(&self, trigger: &Trigger) -> Result<String>;
    
    /// Generate CREATE POLICY SQL
    fn create_policy(&self, policy: &Policy) -> Result<String>;
    
    /// Generate CREATE SERVER SQL
    fn create_server(&self, server: &Server) -> Result<String>;
}

/// Database features
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Feature {
    Tables,
    Views,
    MaterializedViews,
    Functions,
    Procedures,
    Enums,
    Domains,
    Sequences,
    Extensions,
    Triggers,
    Policies,
    ForeignServers,
    Partitions,
    Inheritance,
    RowLevelSecurity,
    GeneratedColumns,
    IdentityColumns,
    CheckConstraints,
    ExclusionConstraints,
    ForeignKeys,
    Indexes,
    Schemas,
    Roles,
    Grants,
    Comments,
}

/// Connection metadata
#[derive(Debug, Clone)]
pub struct ConnectionMetadata {
    /// Database version
    pub version: String,
    
    /// Database name
    pub database: String,
    
    /// Database user
    pub user: String,
    
    /// Database host
    pub host: String,
    
    /// Database port
    pub port: u16,
    
    /// Database encoding
    pub encoding: String,
    
    /// Database timezone
    pub timezone: String,
    
    /// Database collation
    pub collation: String,
    
    /// Database locale
    pub locale: String,
    
    /// Database maximum connections
    pub max_connections: Option<i32>,
    
    /// Database shared buffers
    pub shared_buffers: Option<String>,
    
    /// Database work memory
    pub work_mem: Option<String>,
    
    /// Database maintenance work memory
    pub maintenance_work_mem: Option<String>,
}

/// Schema serializer trait
#[async_trait]
pub trait SchemaSerializer: Send + Sync {
    /// Serialize schema to string
    async fn serialize(&self, schema: &Schema) -> Result<String>;
    
    /// Deserialize schema from string
    async fn deserialize(&self, content: &str) -> Result<Schema>;
    
    /// Get file extension
    fn extension(&self) -> &'static str;
}

/// Migration generator trait
#[async_trait]
pub trait MigrationGenerator: Send + Sync {
    /// Generate migration from schema diff
    async fn generate(&self, from: &Schema, to: &Schema) -> Result<Migration>;
}

/// Migration representation
#[derive(Debug, Clone)]
pub struct Migration {
    pub id: String,
    pub name: String,
    pub up: Vec<String>,
    pub down: Vec<String>,
    pub dependencies: Vec<String>,
}

#[async_trait]
pub trait AsyncSqlGenerator: Send + Sync {
    async fn generate_create_table_async(&self, table: &crate::Table) -> Result<String>;
    async fn generate_alter_table_async(&self, old: &crate::Table, new: &crate::Table) -> Result<(Vec<String>, Vec<String>)>;
    async fn generate_drop_table_async(&self, table: &crate::Table) -> Result<String>;
    async fn generate_create_type_async(&self, type_def: &crate::Type) -> Result<String>;
}

#[async_trait]
impl DatabaseConnection for Box<dyn DatabaseConnection> {
    fn driver(&self) -> &dyn DatabaseDriver {
        self.as_ref().driver()
    }

    async fn introspect(&self) -> Result<Schema> {
        self.as_ref().introspect().await
    }

    async fn execute(&self, sql: &str) -> Result<()> {
        self.as_ref().execute(sql).await
    }

    async fn query(&self, sql: &str) -> Result<Vec<serde_json::Value>> {
        self.as_ref().query(sql).await
    }

    async fn begin(&self) -> Result<Box<dyn Transaction>> {
        self.as_ref().begin().await
    }

    async fn close(self: Box<Self>) -> Result<()> {
        (*self).close().await
    }

    async fn metadata(&self) -> Result<ConnectionMetadata> {
        self.as_ref().metadata().await
    }
}

#[async_trait]
impl Transaction for Box<dyn Transaction> {
    async fn execute(&self, sql: &str) -> Result<()> {
        self.as_ref().execute(sql).await
    }

    async fn query(&self, sql: &str) -> Result<Vec<serde_json::Value>> {
        self.as_ref().query(sql).await
    }

    async fn commit(self: Box<Self>) -> Result<()> {
        (*self).commit().await
    }

    async fn rollback(self: Box<Self>) -> Result<()> {
        (*self).rollback().await
    }
}