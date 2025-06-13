use anyhow::Result;
use async_trait::async_trait;
use anyhow::anyhow;
use tokio_postgres::{Client, Config, NoTls, types::FromSql};
use tracing::info;
use std::sync::Arc;

use shem_core::{
    DatabaseDriver, DatabaseConnection, ConnectionMetadata,
    Transaction, Schema, Table, View, MaterializedView,
    Function, Procedure, Type, Domain, Sequence, Extension,
    Trigger, Policy, Server, Feature, SqlGenerator
};

pub mod introspection;
pub mod sql_generator;

pub use introspection::introspect_schema;
pub use sql_generator::PostgresSqlGenerator;

/// PostgreSQL database driver
#[derive(Debug, Clone)]
pub struct PostgresDriver {
    sql_generator: Arc<PostgresSqlGenerator>,
}

impl PostgresDriver {
    pub fn new() -> Self {
        Self {
            sql_generator: Arc::new(PostgresSqlGenerator),
        }
    }
}

impl Default for PostgresDriver {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl DatabaseDriver for PostgresDriver {
    fn name(&self) -> &str {
        "postgres"
    }

    fn dialect(&self) -> &str {
        "postgresql"
    }

    fn features(&self) -> &[Feature] {
        static FEATURES: &[Feature] = &[
            Feature::Tables,
            Feature::Views,
            Feature::MaterializedViews,
            Feature::Functions,
            Feature::Procedures,
            Feature::Types,
            Feature::Enums,
            Feature::Domains,
            Feature::Sequences,
            Feature::Extensions,
            Feature::Triggers,
            Feature::Policies,
            Feature::ForeignServers,
            Feature::Partitions,
            Feature::Inheritance,
            Feature::RowLevelSecurity,
            Feature::GeneratedColumns,
            Feature::IdentityColumns,
            Feature::CheckConstraints,
            Feature::ExclusionConstraints,
            Feature::ForeignKeys,
            Feature::Indexes,
            Feature::Schemas,
            Feature::Roles,
            Feature::Grants,
            Feature::Comments,
        ];
        FEATURES
    }

    fn data_types(&self) -> Vec<String> {
        vec![
            "smallint".to_string(), "integer".to_string(), "bigint".to_string(),
            "decimal".to_string(), "numeric".to_string(), "real".to_string(), "double precision".to_string(),
            "smallserial".to_string(), "serial".to_string(), "bigserial".to_string(),
            "money".to_string(),
            "character varying".to_string(), "varchar".to_string(), "character".to_string(), "char".to_string(), "text".to_string(),
            "bytea".to_string(),
            "timestamp".to_string(), "timestamp with time zone".to_string(), "timestamptz".to_string(),
            "date".to_string(), "time".to_string(), "time with time zone".to_string(), "timetz".to_string(),
            "interval".to_string(),
            "boolean".to_string(), "bool".to_string(),
            "point".to_string(), "line".to_string(), "lseg".to_string(), "box".to_string(), "path".to_string(), "polygon".to_string(), "circle".to_string(),
            "cidr".to_string(), "inet".to_string(), "macaddr".to_string(), "macaddr8".to_string(),
            "bit".to_string(), "bit varying".to_string(),
            "uuid".to_string(),
            "xml".to_string(),
            "json".to_string(), "jsonb".to_string(),
            "array".to_string(),
            "hstore".to_string(),
            "ltree".to_string(),
            "pg_lsn".to_string(),
            "pg_snapshot".to_string(),
            "tsquery".to_string(), "tsvector".to_string(),
            "txid_snapshot".to_string(),
            "int4range".to_string(), "int8range".to_string(), "numrange".to_string(), "tsrange".to_string(), "tstzrange".to_string(), "daterange".to_string(),
        ]
    }

    fn sql_generator(&self) -> Box<dyn SqlGenerator> {
        Box::new(PostgresSqlGenerator)
    }

    async fn connect(&self, url: &str) -> Result<Box<dyn DatabaseConnection>> {
        let config = url.parse::<Config>()?;
        let (client, connection) = config.connect(NoTls).await?;

        // Spawn connection task
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });

        Ok(Box::new(PostgresConnection {
            client: Arc::new(Mutex::new(client)),
            driver: Arc::new(self.clone()),
        }))
    }
}

use tokio::sync::Mutex;

/// PostgreSQL database connection
#[derive(Debug)]
pub struct PostgresConnection {
    client: Arc<Mutex<Client>>,
    driver: Arc<PostgresDriver>,
}

#[async_trait]
impl DatabaseConnection for PostgresConnection {
    fn driver(&self) -> &dyn DatabaseDriver {
        &*self.driver
    }

    async fn metadata(&self) -> Result<ConnectionMetadata> {
        let client = self.client.lock().await;
        let row = client.query_one(
            "SELECT version(), current_database(), current_user, inet_server_addr(), inet_server_port(), pg_encoding_to_char(encoding), current_setting('timezone')",
            &[]
        ).await?;

        let version: String = row.get(0);
        let database: String = row.get(1);
        let user: String = row.get(2);
        let host: Option<String> = row.get(3);
        let port: Option<i32> = row.get(4);
        let encoding: String = row.get(5);
        let timezone: String = row.get(6);

        // Get server settings
        let settings = client.query(
            "SELECT name, setting FROM pg_settings WHERE name IN ('server_version_num', 'max_identifier_length', 'max_connections')",
            &[]
        ).await?;

        let mut server_settings = std::collections::HashMap::new();
        for row in settings {
            let name: String = row.get(0);
            let setting: String = row.get(1);
            server_settings.insert(name, setting);
        }

        Ok(ConnectionMetadata {
            version,
            database,
            user,
            host: host.unwrap_or_default(),
            port: port.unwrap_or(5432) as u16,
            encoding,
            timezone,
            collation: String::new(),
            locale: String::new(),
            max_connections: None,
            shared_buffers: None,
            work_mem: None,
            maintenance_work_mem: None,
        })
    }

    async fn introspect(&self) -> Result<Schema> {
        let client = self.client.lock().await;
        let client_ref = &*client;
        introspect_schema(client_ref).await
    }

    async fn execute(&self, sql: &str) -> Result<()> {
        let client = self.client.lock().await;
        client.execute(sql, &[]).await?;
        Ok(())
    }

    async fn query(&self, sql: &str) -> Result<Vec<serde_json::Value>> {
        let client = self.client.lock().await;
        let rows = client.query(sql, &[]).await?;
        let mut results = Vec::new();

        for row in rows {
            let mut map = serde_json::Map::new();
            for (i, column) in row.columns().iter().enumerate() {
                let value = match column.type_().name() {
                    "json" | "jsonb" => {
                        let json_str: String = row.get(i);
                        serde_json::from_str(&json_str)?
                    }
                    "bool" => serde_json::Value::Bool(row.get(i)),
                    "int2" | "int4" | "int8" => serde_json::Value::Number(serde_json::Number::from(row.get::<_, i64>(i))),
                    "float4" | "float8" => {
                        let float_val: f64 = row.get(i);
                        match serde_json::Number::from_f64(float_val) {
                            Some(num) => serde_json::Value::Number(num),
                            None => serde_json::Value::Number(serde_json::Number::from_f64(0.0).unwrap())
                        }
                    },
                    "text" | "varchar" | "char" | "name" | "uuid" => serde_json::Value::String(row.get(i)),
                    "timestamp" | "timestamptz" | "date" | "time" | "timetz" => {
                        let ts: chrono::DateTime<chrono::Utc> = row.get(i);
                        serde_json::Value::String(ts.to_rfc3339())
                    }
                    "bytea" => {
                        let bytes: Vec<u8> = row.get(i);
                        serde_json::Value::String(base64::encode(&bytes))
                    }
                    _ => {
                        // For other types, convert to string
                        let s: String = row.get(i);
                        serde_json::Value::String(s)
                    }
                };
                map.insert(column.name().to_string(), value);
            }
            results.push(serde_json::Value::Object(map));
        }

        Ok(results)
    }

    async fn begin(&self) -> Result<Box<dyn Transaction>> {
        let mut client = self.client.lock().await;
        let transaction = client.transaction().await?;
        // Convert the transaction to 'static by moving it into a Box
        let transaction = unsafe {
            std::mem::transmute::<tokio_postgres::Transaction<'_>, tokio_postgres::Transaction<'static>>(transaction)
        };
        Ok(Box::new(PostgresTransaction { 
            client: Arc::clone(&self.client),
            transaction: Some(transaction)
        }))
    }

    async fn close(self: Box<Self>) -> Result<()> {
        // Drop the Arc to ensure the client is properly closed
        drop(Arc::try_unwrap(self.client).map_err(|_| anyhow::anyhow!("Failed to drop client"))?);
        Ok(())
    }
}

/// PostgreSQL transaction
pub struct PostgresTransaction {
    client: Arc<Mutex<Client>>,
    transaction: Option<tokio_postgres::Transaction<'static>>,
}

// Manually implement Debug since tokio_postgres::Transaction doesn't implement it
impl std::fmt::Debug for PostgresTransaction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresTransaction")
            .field("transaction", &"<tokio_postgres::Transaction>")
            .finish()
    }
}

#[async_trait]
impl Transaction for PostgresTransaction {
    async fn execute(&self, sql: &str) -> Result<()> {
        if let Some(transaction) = &self.transaction {
            transaction.execute(sql, &[]).await?;
        }
        Ok(())
    }

    async fn query(&self, sql: &str) -> Result<Vec<serde_json::Value>> {
        if let Some(transaction) = &self.transaction {
            let rows = transaction.query(sql, &[]).await?;
            let mut results = Vec::new();

            for row in rows {
                let mut map = serde_json::Map::new();
                for (i, column) in row.columns().iter().enumerate() {
                    let value = match column.type_().name() {
                        "json" | "jsonb" => {
                            let json_str: String = row.get(i);
                            serde_json::from_str(&json_str)?
                        }
                        "bool" => serde_json::Value::Bool(row.get(i)),
                        "int2" | "int4" | "int8" => serde_json::Value::Number(serde_json::Number::from(row.get::<_, i64>(i))),
                        "float4" | "float8" => {
                            let float_val: f64 = row.get(i);
                            match serde_json::Number::from_f64(float_val) {
                                Some(num) => serde_json::Value::Number(num),
                                None => serde_json::Value::Number(serde_json::Number::from_f64(0.0).unwrap())
                            }
                        },
                        "text" | "varchar" | "char" | "name" | "uuid" => serde_json::Value::String(row.get(i)),
                        "timestamp" | "timestamptz" | "date" | "time" | "timetz" => {
                            let ts: chrono::DateTime<chrono::Utc> = row.get(i);
                            serde_json::Value::String(ts.to_rfc3339())
                        }
                        "bytea" => {
                            let bytes: Vec<u8> = row.get(i);
                            serde_json::Value::String(base64::encode(&bytes))
                        }
                        _ => {
                            // For other types, convert to string
                            let s: String = row.get(i);
                            serde_json::Value::String(s)
                        }
                    };
                    map.insert(column.name().to_string(), value);
                }
                results.push(serde_json::Value::Object(map));
            }
            return Ok(results);
        }
        Ok(Vec::new())
    }

    async fn commit(mut self: Box<Self>) -> Result<()> {
        if let Some(transaction) = self.transaction.take() {
            transaction.commit().await?;
        }
        Ok(())
    }

    async fn rollback(mut self: Box<Self>) -> Result<()> {
        if let Some(transaction) = self.transaction.take() {
            transaction.rollback().await?;
        }
        Ok(())
    }
}