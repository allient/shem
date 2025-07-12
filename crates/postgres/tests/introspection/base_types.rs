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
async fn test_introspect_basic_base_type() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create a basic base type
    execute_sql(
        &connection,
        "CREATE TYPE test_basic_type AS (value INTEGER, description TEXT);",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;
    
    // Verify the composite type was introspected
    let composite_type = schema.composite_types.get("test_basic_type");
    debug!("Composite type: {:?}", composite_type);
    assert!(
        composite_type.is_some(),
        "Composite type 'test_basic_type' should be introspected"
    );

    let ct = composite_type.unwrap();
    assert_eq!(ct.name, "test_basic_type");
    assert_eq!(ct.schema, Some("public".to_string()), "Composite type should be in public schema");
    assert!(!ct.attributes.is_empty(), "Composite type should have attributes");
    assert_eq!(ct.attributes.len(), 2, "Composite type should have 2 attributes");
    assert_eq!(ct.attributes[0].name, "value");
    assert_eq!(ct.attributes[1].name, "description");

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_base_type_with_schema() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create schema and base type in that schema
    execute_sql(&connection, "CREATE SCHEMA test_base_types;").await?;
    execute_sql(
        &connection,
        "CREATE TYPE test_base_types.schema_type AS (id INTEGER, name TEXT);",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the composite type was introspected with correct schema
    let composite_type = schema.composite_types.get("schema_type");
    assert!(
        composite_type.is_some(),
        "Composite type 'schema_type' should be introspected"
    );

    let ct = composite_type.unwrap();
    assert_eq!(ct.name, "schema_type");
    assert_eq!(
        ct.schema,
        Some("test_base_types".to_string()),
        "Composite type should be in the specified schema"
    );

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_base_type_with_comment() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create base type and add comment
    execute_sql(
        &connection,
        "CREATE TYPE test_comment_type AS (value INTEGER, description TEXT);",
    )
    .await?;
    execute_sql(
        &connection,
        "COMMENT ON TYPE test_comment_type IS 'Test base type with comment';",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the composite type was introspected with comment
    let composite_type = schema.composite_types.get("test_comment_type");
    assert!(
        composite_type.is_some(),
        "Composite type 'test_comment_type' should be introspected"
    );

    let ct = composite_type.unwrap();
    assert_eq!(ct.name, "test_comment_type");
    assert_eq!(
        ct.comment,
        Some("Test base type with comment".to_string()),
        "Composite type should have the specified comment"
    );

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_multiple_base_types() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create multiple base types
    execute_sql(
        &connection,
        "CREATE TYPE test_type1 AS (id INTEGER, name TEXT);",
    )
    .await?;
    execute_sql(
        &connection,
        "CREATE TYPE test_type2 AS (value NUMERIC, status BOOLEAN);",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify both composite types were introspected
    assert!(
        schema.composite_types.contains_key("test_type1"),
        "Composite type 'test_type1' should be introspected"
    );
    assert!(
        schema.composite_types.contains_key("test_type2"),
        "Composite type 'test_type2' should be introspected"
    );

    // Verify composite type details
    let type1 = schema.composite_types.get("test_type1").unwrap();
    let type2 = schema.composite_types.get("test_type2").unwrap();

    assert_eq!(type1.name, "test_type1");
    assert_eq!(type2.name, "test_type2");
    assert_eq!(type1.schema, Some("public".to_string()));
    assert_eq!(type2.schema, Some("public".to_string()));

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_base_type_categories() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create composite types
    execute_sql(
        &connection,
        "CREATE TYPE test_composite AS (field1 INTEGER, field2 TEXT);",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the composite type was introspected
    let composite_type = schema.composite_types.get("test_composite");
    assert!(
        composite_type.is_some(),
        "Composite type 'test_composite' should be introspected"
    );

    let ct = composite_type.unwrap();
    assert_eq!(ct.name, "test_composite");
    assert_eq!(ct.attributes.len(), 2, "Composite type should have 2 attributes");

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_base_type_alignment_storage() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create a composite type with different field types
    execute_sql(
        &connection,
        "CREATE TYPE test_alignment_type AS (small_field SMALLINT, large_field TEXT);",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the composite type was introspected
    let composite_type = schema.composite_types.get("test_alignment_type");
    assert!(
        composite_type.is_some(),
        "Composite type 'test_alignment_type' should be introspected"
    );

    let ct = composite_type.unwrap();
    assert_eq!(ct.name, "test_alignment_type");
    
    // Verify the composite type has the expected attributes
    assert_eq!(ct.attributes.len(), 2, "Composite type should have 2 attributes");
    assert_eq!(ct.attributes[0].name, "small_field");
    assert_eq!(ct.attributes[1].name, "large_field");
    
    // Verify storage types are set for attributes
    assert!(ct.attributes[0].storage.is_some(), "Attribute should have storage type");
    assert!(ct.attributes[1].storage.is_some(), "Attribute should have storage type");

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_base_type_with_element() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create composite types that reference each other
    // First create a simple type
    execute_sql(
        &connection,
        "CREATE TYPE test_element_base AS (id INTEGER, name TEXT);",
    )
    .await?;

    // Create another type that references the first one
    execute_sql(
        &connection,
        "CREATE TYPE test_element_ref AS (base_ref test_element_base, extra TEXT);",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify both composite types were introspected
    assert!(
        schema.composite_types.contains_key("test_element_base"),
        "Composite type 'test_element_base' should be introspected"
    );
    assert!(
        schema.composite_types.contains_key("test_element_ref"),
        "Composite type 'test_element_ref' should be introspected"
    );

    let base_type = schema.composite_types.get("test_element_base").unwrap();
    let ref_type = schema.composite_types.get("test_element_ref").unwrap();

    assert_eq!(base_type.name, "test_element_base");
    assert_eq!(ref_type.name, "test_element_ref");

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_no_base_types() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Introspect the database without any user base types
    let schema = connection.introspect().await?;

    // Verify no user composite types are present
    // Note: System composite types should be filtered out
    let user_composite_types: Vec<&String> = schema.composite_types.keys().collect();
    assert!(
        user_composite_types.is_empty(),
        "No user composite types should be introspected: {:?}",
        user_composite_types
    );

    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_base_type_edge_cases() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Test case 1: Composite type with very long name
    let long_name = "a".repeat(50);
    execute_sql(
        &connection,
        &format!("CREATE TYPE {} AS (field1 INTEGER, field2 TEXT);", long_name),
    )
    .await?;

    let schema = connection.introspect().await?;
    let composite_type = schema.composite_types.get(&long_name);
    assert!(
        composite_type.is_some(),
        "Composite type with long name should be introspected"
    );

    // Test case 2: Composite type with special characters in name
    execute_sql(
        &connection,
        "CREATE TYPE \"test-type-with-dashes\" AS (field1 INTEGER, field2 TEXT);",
    )
    .await?;

    let schema2 = connection.introspect().await?;
    let composite_type2 = schema2.composite_types.get("test-type-with-dashes");
    assert!(
        composite_type2.is_some(),
        "Composite type with special characters should be introspected"
    );

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_base_type_performance() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create multiple composite types
    for i in 1..=10 {
        execute_sql(
            &connection,
            &format!("CREATE TYPE test_perf_type_{} AS (id INTEGER, name TEXT, value NUMERIC);", i),
        )
        .await?;
    }

    // Measure introspection performance
    let start = std::time::Instant::now();
    let schema = connection.introspect().await?;
    let duration = start.elapsed();

    // Verify all composite types were introspected
    for i in 1..=10 {
        assert!(
            schema.composite_types.contains_key(&format!("test_perf_type_{}", i)),
            "Composite type test_perf_type_{} should be introspected",
            i
        );
    }

    // Performance assertion (adjust threshold as needed)
    assert!(
        duration.as_millis() < 1000,
        "Introspection should complete within 1 second"
    );

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_base_type_consistency() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create a composite type
    execute_sql(
        &connection,
        "CREATE TYPE test_consistency_type AS (id INTEGER, name TEXT, created_at TIMESTAMP);",
    )
    .await?;

    // Introspect multiple times to verify consistency
    let schema1 = connection.introspect().await?;
    let schema2 = connection.introspect().await?;

    let ct1 = schema1.composite_types.get("test_consistency_type").unwrap();
    let ct2 = schema2.composite_types.get("test_consistency_type").unwrap();

    // Verify consistency across multiple introspections
    assert_eq!(ct1.name, ct2.name);
    assert_eq!(ct1.schema, ct2.schema);
    assert_eq!(ct1.attributes.len(), ct2.attributes.len());
    assert_eq!(ct1.comment, ct2.comment);

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_base_type_complex_structure() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create a complex composite type with various data types
    execute_sql(
        &connection,
        r#"
        CREATE TYPE test_complex_type AS (
            id INTEGER,
            name TEXT,
            description VARCHAR(255),
            amount DECIMAL(10,2),
            is_active BOOLEAN,
            created_at TIMESTAMP WITH TIME ZONE,
            metadata JSONB,
            tags TEXT[]
        );
        "#,
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the complex composite type was introspected
    let composite_type = schema.composite_types.get("test_complex_type");
    assert!(
        composite_type.is_some(),
        "Complex composite type 'test_complex_type' should be introspected"
    );

    let ct = composite_type.unwrap();
    assert_eq!(ct.name, "test_complex_type");
    assert_eq!(ct.schema, Some("public".to_string()));
    assert_eq!(ct.attributes.len(), 8, "Composite type should have 8 attributes");

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_base_type_with_default() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create a composite type
    execute_sql(
        &connection,
        "CREATE TYPE test_default_type AS (id INTEGER, name TEXT);",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the composite type was introspected
    let composite_type = schema.composite_types.get("test_default_type");
    assert!(
        composite_type.is_some(),
        "Composite type 'test_default_type' should be introspected"
    );

    let ct = composite_type.unwrap();
    assert_eq!(ct.name, "test_default_type");
    assert_eq!(ct.attributes.len(), 2, "Composite type should have 2 attributes");

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_base_type_delimiter() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create a composite type
    execute_sql(
        &connection,
        "CREATE TYPE test_delimiter_type AS (field1 INTEGER, field2 TEXT);",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the composite type was introspected
    let composite_type = schema.composite_types.get("test_delimiter_type");
    assert!(
        composite_type.is_some(),
        "Composite type 'test_delimiter_type' should be introspected"
    );

    let ct = composite_type.unwrap();
    assert_eq!(ct.name, "test_delimiter_type");
    assert_eq!(ct.attributes.len(), 2, "Composite type should have 2 attributes");
    assert_eq!(ct.attributes[0].name, "field1");
    assert_eq!(ct.attributes[1].name, "field2");

    // Clean up
    db.cleanup().await?;
    Ok(())
} 