use shem_postgres::PostgresDriver;
use shem_core::{DatabaseConnection, Schema};
use std::process::Command;
use std::time::Duration;
use tokio::time::sleep;

pub struct TestDatabase {
    pub name: String,
    pub connection: Box<dyn DatabaseConnection>,
}

/// Setup a test database with a unique name
pub async fn setup_test_database() -> Result<TestDatabase, Box<dyn std::error::Error>> {
    let db_name = format!("shem_test_{}", uuid::Uuid::new_v4().to_string().replace("-", "_"));
    
    // Create database using psql
    let status = Command::new("psql")
        .args(&["-h", "localhost", "-U", "postgres", "-d", "postgres", "-c", &format!("CREATE DATABASE {}", db_name)])
        .status()?;
    
    if !status.success() {
        return Err("Failed to create test database".into());
    }
    
    // Wait a bit for database to be ready
    sleep(Duration::from_millis(500)).await;
    
    // Connect to the new database
    let driver = PostgresDriver::new();
    let connection_string = format!("postgresql://postgres@localhost/{}", db_name);
    let connection = driver.connect(&connection_string).await?;
    
    Ok(TestDatabase {
        name: db_name,
        connection,
    })
}

/// Cleanup test database
pub async fn cleanup_test_database(test_db: TestDatabase) -> Result<(), Box<dyn std::error::Error>> {
    // Close connection first
    test_db.connection.close().await?;
    
    // Drop database
    let status = Command::new("psql")
        .args(&["-h", "localhost", "-U", "postgres", "-d", "postgres", "-c", &format!("DROP DATABASE IF EXISTS {}", test_db.name)])
        .status()?;
    
    if !status.success() {
        eprintln!("Warning: Failed to drop test database {}", test_db.name);
    }
    
    Ok(())
}

/// Execute SQL script on test database
pub async fn execute_sql(connection: &Box<dyn DatabaseConnection>, sql: &str) -> Result<(), Box<dyn std::error::Error>> {
    connection.execute(sql).await?;
    Ok(())
}

/// Introspect schema and return it
pub async fn introspect_schema(connection: &Box<dyn DatabaseConnection>) -> Result<Schema, Box<dyn std::error::Error>> {
    let schema = connection.introspect().await?;
    Ok(schema)
}

/// Assert that a table exists in the schema
pub fn assert_table_exists(schema: &Schema, table_name: &str) {
    assert!(
        schema.tables.contains_key(table_name),
        "Table '{}' not found in schema. Available tables: {:?}",
        table_name,
        schema.tables.keys().collect::<Vec<_>>()
    );
}

/// Assert that a view exists in the schema
pub fn assert_view_exists(schema: &Schema, view_name: &str) {
    assert!(
        schema.views.contains_key(view_name),
        "View '{}' not found in schema. Available views: {:?}",
        view_name,
        schema.views.keys().collect::<Vec<_>>()
    );
}

/// Assert that a function exists in the schema
pub fn assert_function_exists(schema: &Schema, function_name: &str) {
    assert!(
        schema.functions.contains_key(function_name),
        "Function '{}' not found in schema. Available functions: {:?}",
        function_name,
        schema.functions.keys().collect::<Vec<_>>()
    );
}

/// Assert that an enum exists in the schema
pub fn assert_enum_exists(schema: &Schema, enum_name: &str) {
    assert!(
        schema.enums.contains_key(enum_name),
        "Enum '{}' not found in schema. Available enums: {:?}",
        enum_name,
        schema.enums.keys().collect::<Vec<_>>()
    );
}

/// Assert that a domain exists in the schema
pub fn assert_domain_exists(schema: &Schema, domain_name: &str) {
    assert!(
        schema.domains.contains_key(domain_name),
        "Domain '{}' not found in schema. Available domains: {:?}",
        domain_name,
        schema.domains.keys().collect::<Vec<_>>()
    );
}

/// Assert that a sequence exists in the schema
pub fn assert_sequence_exists(schema: &Schema, sequence_name: &str) {
    assert!(
        schema.sequences.contains_key(sequence_name),
        "Sequence '{}' not found in schema. Available sequences: {:?}",
        sequence_name,
        schema.sequences.keys().collect::<Vec<_>>()
    );
}

/// Assert that an extension exists in the schema
pub fn assert_extension_exists(schema: &Schema, extension_name: &str) {
    assert!(
        schema.extensions.contains_key(extension_name),
        "Extension '{}' not found in schema. Available extensions: {:?}",
        extension_name,
        schema.extensions.keys().collect::<Vec<_>>()
    );
}

/// Assert that a trigger exists in the schema
pub fn assert_trigger_exists(schema: &Schema, trigger_name: &str) {
    assert!(
        schema.triggers.contains_key(trigger_name),
        "Trigger '{}' not found in schema. Available triggers: {:?}",
        trigger_name,
        schema.triggers.keys().collect::<Vec<_>>()
    );
}

/// Assert that a policy exists in the schema
pub fn assert_policy_exists(schema: &Schema, policy_name: &str) {
    assert!(
        schema.policies.contains_key(policy_name),
        "Policy '{}' not found in schema. Available policies: {:?}",
        policy_name,
        schema.policies.keys().collect::<Vec<_>>()
    );
}

/// Print schema summary for debugging
pub fn print_schema_summary(schema: &Schema) {
    println!("=== Schema Summary ===");
    println!("Tables: {}", schema.tables.len());
    println!("Views: {}", schema.views.len());
    println!("Materialized Views: {}", schema.materialized_views.len());
    println!("Functions: {}", schema.functions.len());
    println!("Procedures: {}", schema.procedures.len());
    println!("Enums: {}", schema.enums.len());
    println!("Domains: {}", schema.domains.len());
    println!("Sequences: {}", schema.sequences.len());
    println!("Extensions: {}", schema.extensions.len());
    println!("Triggers: {}", schema.triggers.len());
    println!("Policies: {}", schema.policies.len());
    println!("=====================");
} 