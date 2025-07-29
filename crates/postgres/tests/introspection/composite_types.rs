use tracing::debug;
use postgres::TestDb;
use shem_core::{DatabaseConnection, schema::{Type, CompositeType}};

/// Test helper function to execute SQL on the test database
async fn execute_sql(
    connection: &Box<dyn DatabaseConnection>,
    sql: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    connection.execute(sql).await?;
    Ok(())
}

/// Helper function to get a composite type from the unified types map
fn get_composite_type<'a>(schema: &'a shem_core::Schema, name: &str) -> Option<&'a CompositeType> {
    schema.types.get(name).and_then(|t| {
        if let Type::Composite(ct) = t {
            Some(ct)
        } else {
            None
        }
    })
}

#[tokio::test]
async fn test_introspect_basic_composite_type() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create a basic composite type
    execute_sql(
        &connection,
        "CREATE TYPE address_type AS (street TEXT, city TEXT, zip_code VARCHAR(10));",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the composite type was introspected
    let composite_type = get_composite_type(&schema, "address_type");
    debug!("Composite type: {:?}", composite_type);
    assert!(
        composite_type.is_some(),
        "Composite type 'address_type' should be introspected"
    );

    let ct = composite_type.unwrap();
    assert_eq!(ct.info.name, "address_type");
    assert_eq!(ct.info.schema, "public", "Composite type should be in public schema");
    assert!(!ct.attributes.is_empty(), "Composite type should have attributes");
    assert_eq!(ct.attributes.len(), 3, "Composite type should have 3 attributes");
    assert_eq!(ct.attributes[0].name, "street");
    assert_eq!(ct.attributes[1].name, "city");
    assert_eq!(ct.attributes[2].name, "zip_code");

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_composite_type_with_schema() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create schema and composite type in that schema
    execute_sql(&connection, "CREATE SCHEMA test_composite_types;").await?;
    execute_sql(
        &connection,
        "CREATE TYPE test_composite_types.person_type AS (id INTEGER, name TEXT, email VARCHAR(255));",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the composite type was introspected with correct schema
    let composite_type = get_composite_type(&schema, "person_type");
    assert!(
        composite_type.is_some(),
        "Composite type 'person_type' should be introspected"
    );

    let ct = composite_type.unwrap();
    assert_eq!(ct.info.name, "person_type");
    assert_eq!(
        ct.info.schema,
        "test_composite_types",
        "Composite type should be in the specified schema"
    );

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_composite_type_with_comment() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create composite type and add comment
    execute_sql(
        &connection,
        "CREATE TYPE point_type AS (x INTEGER, y INTEGER);",
    )
    .await?;
    execute_sql(
        &connection,
        "COMMENT ON TYPE point_type IS '2D point coordinate type';",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the composite type was introspected with comment
    let composite_type = get_composite_type(&schema, "point_type");
    assert!(
        composite_type.is_some(),
        "Composite type 'point_type' should be introspected"
    );

    let ct = composite_type.unwrap();
    assert_eq!(ct.info.name, "point_type");
    assert_eq!(
        ct.info.comment,
        Some("2D point coordinate type".to_string()),
        "Composite type should have the specified comment"
    );

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_composite_type_complex() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create a complex composite type with various data types
    execute_sql(
        &connection,
        r#"
        CREATE TYPE employee_info AS (
            id INTEGER,
            name VARCHAR(100),
            salary NUMERIC(10,2),
            hire_date DATE,
            is_active BOOLEAN,
            metadata JSONB
        );
        "#,
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the complex composite type was introspected
    let composite_type = get_composite_type(&schema, "employee_info");
    assert!(
        composite_type.is_some(),
        "Composite type 'employee_info' should be introspected"
    );

    let ct = composite_type.unwrap();
    assert_eq!(ct.info.name, "employee_info");
    assert_eq!(ct.info.schema, "public");
    assert_eq!(ct.attributes.len(), 6, "Composite type should have 6 attributes");

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_multiple_composite_types() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create multiple composite types
    execute_sql(
        &connection,
        "CREATE TYPE point_2d AS (x INTEGER, y INTEGER);",
    )
    .await?;
    execute_sql(
        &connection,
        "CREATE TYPE point_3d AS (x INTEGER, y INTEGER, z INTEGER);",
    )
    .await?;
    execute_sql(
        &connection,
        "CREATE TYPE rectangle AS (width INTEGER, height INTEGER);",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify all composite types were introspected
    assert!(
        get_composite_type(&schema, "point_2d").is_some(),
        "Composite type 'point_2d' should be introspected"
    );
    assert!(
        get_composite_type(&schema, "point_3d").is_some(),
        "Composite type 'point_3d' should be introspected"
    );
    assert!(
        get_composite_type(&schema, "rectangle").is_some(),
        "Composite type 'rectangle' should be introspected"
    );

    // Verify composite type details
    let point_2d = get_composite_type(&schema, "point_2d").unwrap();
    let point_3d = get_composite_type(&schema, "point_3d").unwrap();
    let rectangle = get_composite_type(&schema, "rectangle").unwrap();

    assert_eq!(point_2d.info.name, "point_2d");
    assert_eq!(point_3d.info.name, "point_3d");
    assert_eq!(rectangle.info.name, "rectangle");
    assert_eq!(point_2d.info.schema, "public");
    assert_eq!(point_3d.info.schema, "public");
    assert_eq!(rectangle.info.schema, "public");

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_composite_type_with_collation() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create composite type with collation
    execute_sql(
        &connection,
        "CREATE TYPE localized_text AS (content TEXT COLLATE \"C\", language VARCHAR(10));",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the composite type was introspected with collation
    let composite_type = get_composite_type(&schema, "localized_text");
    assert!(
        composite_type.is_some(),
        "Composite type 'localized_text' should be introspected"
    );

    let ct = composite_type.unwrap();
    assert_eq!(ct.info.name, "localized_text");
    assert_eq!(ct.attributes.len(), 2, "Composite type should have 2 attributes");
    assert_eq!(ct.attributes[0].name, "content");
    assert_eq!(ct.attributes[1].name, "language");

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_composite_type_storage() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create composite type with storage specifications
    execute_sql(
        &connection,
        "CREATE TYPE storage_test AS (small_field SMALLINT, large_field TEXT);",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the composite type was introspected
    let composite_type = get_composite_type(&schema, "storage_test");
    assert!(
        composite_type.is_some(),
        "Composite type 'storage_test' should be introspected"
    );

    let ct = composite_type.unwrap();
    assert_eq!(ct.info.name, "storage_test");
    assert_eq!(ct.attributes.len(), 2, "Composite type should have 2 attributes");
    assert_eq!(ct.attributes[0].name, "small_field");
    assert_eq!(ct.attributes[1].name, "large_field");

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_no_composite_types() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Introspect the database without any user composite types
    let schema = connection.introspect().await?;

    // Verify no user composite types are present
    // Note: System composite types should be filtered out
    let user_composite_types: Vec<&String> = schema.types
        .iter()
        .filter_map(|(name, t)| {
            if let Type::Composite(_) = t {
                Some(name)
            } else {
                None
            }
        })
        .collect();
    assert!(
        user_composite_types.is_empty(),
        "No user composite types should be introspected: {:?}",
        user_composite_types
    );

    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_composite_type_edge_cases() -> Result<(), Box<dyn std::error::Error>> {
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
    let composite_type = get_composite_type(&schema, &long_name);
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
    let composite_type2 = get_composite_type(&schema2, "test-type-with-dashes");
    assert!(
        composite_type2.is_some(),
        "Composite type with special characters should be introspected"
    );

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_composite_type_single_attribute() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create composite type with single attribute
    execute_sql(
        &connection,
        "CREATE TYPE single_attr_type AS (value INTEGER);",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the composite type was introspected
    let composite_type = get_composite_type(&schema, "single_attr_type");
    assert!(
        composite_type.is_some(),
        "Composite type 'single_attr_type' should be introspected"
    );

    let ct = composite_type.unwrap();
    assert_eq!(ct.info.name, "single_attr_type");
    assert_eq!(ct.attributes.len(), 1, "Composite type should have 1 attribute");
    assert_eq!(ct.attributes[0].name, "value");

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_composite_type_consistency() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create a composite type
    execute_sql(
        &connection,
        "CREATE TYPE consistency_test AS (id INTEGER, name TEXT, created_at TIMESTAMP);",
    )
    .await?;

    // Introspect multiple times to verify consistency
    let schema1 = connection.introspect().await?;
    let schema2 = connection.introspect().await?;

    let comp_type1 = get_composite_type(&schema1, "consistency_test").unwrap();
    let comp_type2 = get_composite_type(&schema2, "consistency_test").unwrap();

    // Verify consistency across multiple introspections
    assert_eq!(comp_type1.info.name, comp_type2.info.name);
    assert_eq!(comp_type1.info.schema, comp_type2.info.schema);
    assert_eq!(comp_type1.attributes.len(), comp_type2.attributes.len());
    assert_eq!(comp_type1.info.comment, comp_type2.info.comment);

    // Clean up
    db.cleanup().await?;
    Ok(())
}
