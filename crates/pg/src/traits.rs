use crate::model::{
    collation::Collation,
    conversion::Conversion,
    // Assuming models are accessible from this crate
    event_trigger::EventTrigger,
    extension::Extension,
    fdw::{ForeignDataWrapper, Server},
    global::{Role, Tablespace},
    operator::OpClass,
    operator::OpFamily,
    operator::Operator,
    publication::Publication,
    publication::PublicationTable,
    relation::Relation,
    routine::Routine,
    schema::Schema,
    sequence::Sequence,
    subscription::Subscription,
    types::Type,
};
use crate::database::DatabaseModel;
use async_trait::async_trait;
use common::error::Result;
use std::fmt::Debug;

/// Supported PostgreSQL features
#[derive(Debug, Clone, PartialEq)]
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
    async fn introspect(&self) -> Result<DatabaseModel>;

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

/// A trait for generating dialect-specific SQL from introspected schema models.
///
/// This trait defines the public API for converting the in-memory `Database`
/// model into a series of executable SQL statements. Each method corresponds
/// to a top-level, dumpable object. The unified enums (`Relation`, `Type`, `Routine`)
/// are used to handle different kinds of related objects through a single interface.
#[async_trait]
pub trait SqlGenerator: Send + Sync {
    // --- Core Unified Object Generators ---

    /// Generates the `CREATE` statement(s) for any kind of relation.
    /// This includes the base `CREATE` (TABLE/VIEW/etc.) and all dependent
    /// objects like indexes, triggers, constraints, rules, and policies.
    fn create_relation(&self, relation: &Relation) -> Result<String>;

    /// Generates the `DROP` statement for any kind of relation.
    fn drop_relation(&self, relation: &Relation) -> Result<String>;

    /// Generates the `CREATE` statement for any kind of type.
    fn create_type(&self, t: &Type) -> Result<String>;

    /// Generates the `DROP` statement for any kind of type.
    fn drop_type(&self, t: &Type) -> Result<String>;

    /// Generates the `CREATE` statement for any kind of routine.
    fn create_routine(&self, routine: &Routine) -> Result<String>;

    /// Generates the `DROP` statement for any kind of routine.
    fn drop_routine(&self, routine: &Routine) -> Result<String>;

    // --- Top-Level, Non-Unified Object Generators ---

    fn create_schema(&self, schema: &Schema) -> Result<String>;
    fn drop_schema(&self, schema: &Schema) -> Result<String>;

    fn create_extension(&self, ext: &Extension) -> Result<String>;
    fn drop_extension(&self, ext: &Extension) -> Result<String>;

    fn create_sequence(&self, seq: &Sequence) -> Result<String>;
    fn alter_sequence(&self, old_seq: &Sequence, new_seq: &Sequence) -> Result<String>;
    fn drop_sequence(&self, seq: &Sequence) -> Result<String>;

    fn create_collation(&self, collation: &Collation) -> Result<String>;
    fn drop_collation(&self, collation: &Collation) -> Result<String>;

    fn create_conversion(&self, conversion: &Conversion) -> Result<String>;
    fn drop_conversion(&self, conversion: &Conversion) -> Result<String>;

    fn create_foreign_data_wrapper(&self, fdw: &ForeignDataWrapper) -> Result<String>;
    fn drop_foreign_data_wrapper(&self, fdw: &ForeignDataWrapper) -> Result<String>;

    fn create_server(&self, server: &Server) -> Result<String>;
    fn drop_server(&self, server: &Server) -> Result<String>;

    fn create_publication(&self, publication: &Publication) -> Result<String>;
    fn drop_publication(&self, publication: &Publication) -> Result<String>;

    fn create_subscription(&self, subscription: &Subscription) -> Result<String>;
    fn drop_subscription(&self, subscription: &Subscription) -> Result<String>;

    fn create_event_trigger(&self, trigger: &EventTrigger) -> Result<String>;
    fn drop_event_trigger(&self, trigger: &EventTrigger) -> Result<String>;

    fn create_operator(&self, operator: &Operator) -> Result<String>;
    fn drop_operator(&self, operator: &Operator) -> Result<String>;

    fn create_op_class(&self, op_class: &OpClass) -> Result<String>;
    fn drop_op_class(&self, op_class: &OpClass) -> Result<String>;

    fn create_op_family(&self, op_family: &OpFamily) -> Result<String>;
    fn drop_op_family(&self, op_family: &OpFamily) -> Result<String>;

    // --- Global Object Generators ---

    fn create_role(&self, role: &Role) -> Result<String>;
    fn drop_role(&self, role: &Role) -> Result<String>;

    fn create_tablespace(&self, tablespace: &Tablespace) -> Result<String>;
    fn drop_tablespace(&self, tablespace: &Tablespace) -> Result<String>;

    // --- Relationship and Metadata Generators ---

    /// Generates the `ALTER PUBLICATION ... ADD TABLE ...` statement.
    fn add_table_to_publication(&self, pub_table: &PublicationTable) -> Result<String>;

    /// Generates `COMMENT ON ...` statements. This is a generic helper.
    fn comment_on(&self, object_type: &str, qualified_name: &str, comment: &str) -> Result<String>;

    /// Generates `GRANT` and `REVOKE` statements based on an ACL string.
    /// A real implementation would likely need a more structured input than just strings.
    fn grant_revoke(&self, object_type: &str, qualified_name: &str, acl: &str) -> Result<String>;

    /// Generates `ALTER ... OWNER TO ...` statements.
    fn alter_owner(&self, object_type: &str, qualified_name: &str, owner: &str) -> Result<String>;

    // --- Migration-Specific Methods (for the future) ---

    // async fn generate_migration(&self, from: &Database, to: &Database) -> Result<Migration>;
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

#[async_trait]
impl DatabaseConnection for Box<dyn DatabaseConnection> {
    fn driver(&self) -> &dyn DatabaseDriver {
        self.as_ref().driver()
    }

    async fn introspect(&self) -> Result<DatabaseModel> {
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
