use tracing::debug;
use postgres::TestDb;
use shem_core::DatabaseConnection;

/// Test helper function to execute SQL on the test database
async fn execute_sql(
    connection: &Box<dyn DatabaseConnection>,
    sql: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    connection.execute(sql).await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_basic_schema() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create a basic schema
    execute_sql(&connection, "CREATE SCHEMA test_schema;").await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the schema was introspected
    let named_schema = schema.named_schemas.get("test_schema");
    debug!("Named schema: {:?}", named_schema);
    assert!(
        named_schema.is_some(),
        "Schema 'test_schema' should be introspected"
    );

    let schema_obj = named_schema.unwrap();
    assert_eq!(schema_obj.name, "test_schema");
    assert_eq!(schema_obj.comment, None, "Schema should not have a comment");

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_schema_with_owner() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create schema with owner
    execute_sql(
        &connection,
        "CREATE SCHEMA test_schema_owner AUTHORIZATION postgres;",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the schema was introspected with owner
    let named_schema = schema.named_schemas.get("test_schema_owner");
    assert!(
        named_schema.is_some(),
        "Schema 'test_schema_owner' should be introspected"
    );

    let schema_obj = named_schema.unwrap();
    assert_eq!(schema_obj.name, "test_schema_owner");
    assert_eq!(
        schema_obj.owner,
        Some("postgres".to_string()),
        "Schema should have the specified owner"
    );

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_schema_with_comment() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create schema and add comment
    execute_sql(&connection, "CREATE SCHEMA test_schema_comment;").await?;
    execute_sql(
        &connection,
        "COMMENT ON SCHEMA test_schema_comment IS 'Test schema for introspection';",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the schema was introspected with comment
    let named_schema = schema.named_schemas.get("test_schema_comment");
    assert!(
        named_schema.is_some(),
        "Schema 'test_schema_comment' should be introspected"
    );

    let schema_obj = named_schema.unwrap();
    assert_eq!(schema_obj.name, "test_schema_comment");
    assert_eq!(
        schema_obj.comment,
        Some("Test schema for introspection".to_string()),
        "Schema should have the specified comment"
    );

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_multiple_schemas() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create multiple schemas
    execute_sql(&connection, "CREATE SCHEMA schema1;").await?;
    execute_sql(&connection, "CREATE SCHEMA schema2;").await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify both schemas were introspected
    assert!(
        schema.named_schemas.contains_key("schema1"),
        "Schema 'schema1' should be introspected"
    );
    assert!(
        schema.named_schemas.contains_key("schema2"),
        "Schema 'schema2' should be introspected"
    );

    // Verify schema details
    let schema1 = schema.named_schemas.get("schema1").unwrap();
    let schema2 = schema.named_schemas.get("schema2").unwrap();

    assert_eq!(schema1.name, "schema1");
    assert_eq!(schema2.name, "schema2");

    // Clean up
    db.cleanup().await?;
    Ok(())
}
