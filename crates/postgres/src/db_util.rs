use crate::{DatabaseConnection, DatabaseDriver, PostgresDriver};
use std::collections::HashMap;
use uuid::Uuid;

const ADMIN_URL: &str = "postgresql://postgres:postgres@localhost:5432/postgres";

/// Holds the test-database name + connection.
/// Call `.cleanup().await` at the end of the test.
pub struct TestDb {
    pub name: String,
    pub conn: Box<dyn DatabaseConnection>,
}

impl TestDb {
    /// Creates a brand-new database, connects to it and returns the handle.
    pub async fn new() -> Result<Self, Box<dyn std::error::Error>> {
        // unique, lowercase, no dashes
        let db_name = format!("test_db_{}", Uuid::new_v4().simple());
        let driver = PostgresDriver::new();

        // 1) connect to the *admin* database and create the new one
        let admin = driver.connect(ADMIN_URL).await?;
        admin
            .execute(&format!("CREATE DATABASE \"{db_name}\""))
            .await?;

        // 2) connect to the freshly created database
        let conn = driver
            .connect(&format!(
                "postgresql://postgres:postgres@localhost:5432/{db_name}"
            ))
            .await?;

        Ok(Self {
            name: db_name,
            conn,
        })
    }

    /// Drops the test database (you must close all connections first).
    pub async fn cleanup(self) -> Result<(), Box<dyn std::error::Error>> {
        drop(self.conn); // close client first

        let driver = PostgresDriver::new();
        let admin = driver.connect(ADMIN_URL).await?;

        // Terminate any straggling connections the test might have left
        admin
            .execute(&format!(
                r#"
                SELECT pg_terminate_backend(pid)
                FROM   pg_stat_activity
                WHERE  datname = '{name}'
                  AND  pid <> pg_backend_pid();
                "#,
                name = self.name
            ))
            .await?;

        admin
            .execute(&format!("DROP DATABASE IF EXISTS \"{}\"", self.name))
            .await?;
        Ok(())
    }
}

/// Converts PostgreSQL type names to their common abbreviations
/// https://neon.com/postgresql/postgresql-tutorial/postgresql-double-precision-type
pub struct PostgreSQLTypeConverter {
    type_mappings: HashMap<&'static str, &'static str>,
}

impl PostgreSQLTypeConverter {
    /// Creates a new type converter with predefined mappings
    pub fn new() -> Self {
        let mut type_mappings = HashMap::new();

        // Timestamp types
        type_mappings.insert("timestamp with time zone", "timestamptz");
        type_mappings.insert("timestamp without time zone", "timestamp");
        type_mappings.insert("time with time zone", "timetz");
        type_mappings.insert("time without time zone", "time");

        // Character types
        type_mappings.insert("character varying", "varchar");
        type_mappings.insert("character", "char");
        type_mappings.insert("\"char\"", "char");

        // Numeric types
        type_mappings.insert("double precision", "float8");
        type_mappings.insert("real", "float4");
        type_mappings.insert("bigint", "int8");
        type_mappings.insert("integer", "int4");
        type_mappings.insert("smallint", "int2");
        type_mappings.insert("bigserial", "serial8");
        type_mappings.insert("serial", "serial4");
        type_mappings.insert("smallserial", "serial2");
        type_mappings.insert("decimal", "numeric");

        // Boolean
        type_mappings.insert("boolean", "bool");

        // Binary types
        type_mappings.insert("bit varying", "varbit");

        // Network types
        type_mappings.insert("inet", "inet");
        type_mappings.insert("cidr", "cidr");
        type_mappings.insert("macaddr", "macaddr");
        type_mappings.insert("macaddr8", "macaddr8");

        // Geometric types
        type_mappings.insert("point", "point");
        type_mappings.insert("line", "line");
        type_mappings.insert("lseg", "lseg");
        type_mappings.insert("box", "box");
        type_mappings.insert("path", "path");
        type_mappings.insert("polygon", "polygon");
        type_mappings.insert("circle", "circle");

        // Range types (keep as is, but normalize)
        type_mappings.insert("int4range", "int4range");
        type_mappings.insert("int8range", "int8range");
        type_mappings.insert("numrange", "numrange");
        type_mappings.insert("tsrange", "tsrange");
        type_mappings.insert("tstzrange", "tstzrange");
        type_mappings.insert("daterange", "daterange");

        // JSON types
        type_mappings.insert("json", "json");
        type_mappings.insert("jsonb", "jsonb");

        // UUID
        type_mappings.insert("uuid", "uuid");

        // XML
        type_mappings.insert("xml", "xml");

        // Arrays (special handling needed)
        type_mappings.insert("ARRAY", "[]");

        Self { type_mappings }
    }

    /// Converts a PostgreSQL type name to its abbreviated form
    pub fn convert_type(&self, pg_type: &str) -> String {
        // Handle array types first
        if pg_type.ends_with("[]") {
            let base_type = &pg_type[..pg_type.len() - 2];
            return format!("{}[]", self.convert_base_type(base_type));
        }

        // Handle parametrized types (e.g., "varchar(100)", "numeric(10,2)")
        if let Some(paren_pos) = pg_type.find('(') {
            let base_type = &pg_type[..paren_pos];
            let params = &pg_type[paren_pos..];

            let converted_base = self.convert_base_type(base_type);
            return format!("{}{}", converted_base, params);
        }

        // Handle non-parametrized types
        self.convert_base_type(pg_type)
    }

    /// Converts the base type name without parameters
    fn convert_base_type(&self, base_type: &str) -> String {
        let lower_type = base_type.to_lowercase();

        // Try exact match first
        if let Some(&abbreviated) = self.type_mappings.get(lower_type.as_str()) {
            return abbreviated.to_string();
        }

        // Handle character varying with different formats
        if lower_type.starts_with("character varying") {
            return "varchar".to_string();
        }

        // Handle timestamp variations
        if lower_type.contains("timestamp") {
            if lower_type.contains("with time zone") {
                return "timestamptz".to_string();
            } else if lower_type.contains("without time zone") {
                return "timestamp".to_string();
            }
        }

        // Handle time variations
        if lower_type.starts_with("time") {
            if lower_type.contains("with time zone") {
                return "timetz".to_string();
            } else if lower_type.contains("without time zone") {
                return "time".to_string();
            }
        }

        // If no conversion found, return original
        base_type.to_string()
    }

    /// Converts a type name to its "canonical" PostgreSQL form (opposite direction)
    pub fn to_canonical(&self, abbreviated_type: &str) -> String {
        // Handle array types
        if abbreviated_type.ends_with("[]") {
            let base_type = &abbreviated_type[..abbreviated_type.len() - 2];
            return format!("{}[]", self.to_canonical_base(base_type));
        }

        // Handle parametrized types
        if let Some(paren_pos) = abbreviated_type.find('(') {
            let base_type = &abbreviated_type[..paren_pos];
            let params = &abbreviated_type[paren_pos..];

            let canonical_base = self.to_canonical_base(base_type);
            return format!("{}{}", canonical_base, params);
        }

        self.to_canonical_base(abbreviated_type)
    }

    /// Converts abbreviated type to canonical form
    fn to_canonical_base(&self, abbreviated_type: &str) -> String {
        // Reverse lookup in the mappings
        for (&canonical, &abbreviated) in &self.type_mappings {
            if abbreviated == abbreviated_type {
                return canonical.to_string();
            }
        }

        // Special reverse mappings
        match abbreviated_type {
            "timestamptz" => "timestamp with time zone".to_string(),
            "timestamp" => "timestamp without time zone".to_string(),
            "timetz" => "time with time zone".to_string(),
            "time" => "time without time zone".to_string(),
            "varchar" => "character varying".to_string(),
            "char" => "character".to_string(),
            "bool" => "boolean".to_string(),
            "float8" => "double precision".to_string(),
            "float4" => "real".to_string(),
            "int8" => "bigint".to_string(),
            "int4" => "integer".to_string(),
            "int2" => "smallint".to_string(),
            _ => abbreviated_type.to_string(),
        }
    }

    /// Normalizes a type name (converts to abbreviated form and back for consistency)
    pub fn normalize(&self, type_name: &str) -> String {
        self.convert_type(type_name)
    }
}
