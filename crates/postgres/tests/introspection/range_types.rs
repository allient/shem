use tracing::debug;
use postgres::TestDb;
use shem_core::{DatabaseConnection, schema::{Type, RangeType}};

/// Test helper function to execute SQL on the test database
async fn execute_sql(
    connection: &Box<dyn DatabaseConnection>,
    sql: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    connection.execute(sql).await?;
    Ok(())
}

/// Helper function to get a range type from the unified types map
fn get_range_type<'a>(schema: &'a shem_core::Schema, name: &str) -> Option<&'a RangeType> {
    schema.types.get(name).and_then(|t| {
        if let Type::Range(rt) = t {
            Some(rt)
        } else {
            None
        }
    })
}

#[tokio::test]
async fn test_introspect_basic_range_type() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create a basic range type
    execute_sql(
        &connection,
        "CREATE TYPE int_range AS RANGE (SUBTYPE = INTEGER);",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the range type was introspected
    let range_type = get_range_type(&schema, "int_range");
    debug!("Range type: {:?}", range_type);
    assert!(
        range_type.is_some(),
        "Range type 'int_range' should be introspected"
    );

    let rt = range_type.unwrap();
    assert_eq!(rt.info.name, "int_range");
    assert_eq!(rt.info.schema, "public", "Range type should be in public schema");
    assert_eq!(rt.subtype, "integer", "Range type should have correct subtype");

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_range_type_with_schema() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create schema and range type in that schema
    execute_sql(&connection, "CREATE SCHEMA test_range_types;").await?;
    execute_sql(
        &connection,
        "CREATE TYPE test_range_types.date_range AS RANGE (SUBTYPE = DATE);",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the range type was introspected with correct schema
    let range_type = get_range_type(&schema, "date_range");
    assert!(
        range_type.is_some(),
        "Range type 'date_range' should be introspected"
    );

    let rt = range_type.unwrap();
    assert_eq!(rt.info.name, "date_range");
    assert_eq!(
        rt.info.schema,
        "test_range_types",
        "Range type should be in the specified schema"
    );

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_range_type_with_comment() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create range type and add comment
    execute_sql(
        &connection,
        "CREATE TYPE timestamp_range AS RANGE (SUBTYPE = TIMESTAMP);",
    )
    .await?;
    execute_sql(
        &connection,
        "COMMENT ON TYPE timestamp_range IS 'Range type for timestamp values';",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the range type was introspected with comment
    let range_type = get_range_type(&schema, "timestamp_range");
    assert!(
        range_type.is_some(),
        "Range type 'timestamp_range' should be introspected"
    );

    let rt = range_type.unwrap();
    assert_eq!(rt.info.name, "timestamp_range");
    assert_eq!(
        rt.info.comment,
        Some("Range type for timestamp values".to_string()),
        "Range type should have the specified comment"
    );

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_range_type_with_collation() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create range type with collation
    execute_sql(
        &connection,
        "CREATE TYPE text_range AS RANGE (SUBTYPE = TEXT, COLLATION = \"C\");",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the range type was introspected with collation
    let range_type = get_range_type(&schema, "text_range");
    assert!(
        range_type.is_some(),
        "Range type 'text_range' should be introspected"
    );

    let rt = range_type.unwrap();
    assert_eq!(rt.info.name, "text_range");
    assert_eq!(rt.subtype, "text", "Range type should have correct subtype");
    assert_eq!(rt.collation, Some("\"C\"".to_string()), "Range type should have collation");

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_range_type_with_canonical() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create range type with canonical function
    execute_sql(
        &connection,
        "CREATE OR REPLACE FUNCTION canonical_int_range(int_range) RETURNS int_range AS 'SELECT $1;' LANGUAGE SQL IMMUTABLE;",
    )
    .await?;
    execute_sql(
        &connection,
        "CREATE TYPE canonical_int_range AS RANGE (SUBTYPE = INTEGER, CANONICAL = canonical_int_range);",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the range type was introspected with canonical function
    let range_type = get_range_type(&schema, "canonical_int_range");
    assert!(
        range_type.is_some(),
        "Range type 'canonical_int_range' should be introspected"
    );

    let rt = range_type.unwrap();
    assert_eq!(rt.info.name, "canonical_int_range");
    assert_eq!(rt.subtype, "integer", "Range type should have correct subtype");
    assert_eq!(rt.canonical_fn, Some("canonical_int_range".to_string()), "Range type should have canonical function");

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_range_type_with_subtype_diff() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create range type with subtype_diff function
    execute_sql(
        &connection,
        "CREATE OR REPLACE FUNCTION int_range_diff(INTEGER, INTEGER) RETURNS DOUBLE PRECISION AS 'SELECT $1 - $2;' LANGUAGE SQL IMMUTABLE;",
    )
    .await?;
    execute_sql(
        &connection,
        "CREATE TYPE int_range_with_diff AS RANGE (SUBTYPE = INTEGER, SUBTYPE_DIFF = int_range_diff);",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the range type was introspected with subtype_diff function
    let range_type = get_range_type(&schema, "int_range_with_diff");
    assert!(
        range_type.is_some(),
        "Range type 'int_range_with_diff' should be introspected"
    );

    let rt = range_type.unwrap();
    assert_eq!(rt.info.name, "int_range_with_diff");
    assert_eq!(rt.subtype, "integer", "Range type should have correct subtype");
    assert_eq!(rt.subtype_diff_fn, Some("int_range_diff".to_string()), "Range type should have subtype_diff function");

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_multiple_range_types() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create multiple range types
    execute_sql(
        &connection,
        "CREATE TYPE int_range AS RANGE (SUBTYPE = INTEGER);",
    )
    .await?;
    execute_sql(
        &connection,
        "CREATE TYPE date_range AS RANGE (SUBTYPE = DATE);",
    )
    .await?;
    execute_sql(
        &connection,
        "CREATE TYPE numeric_range AS RANGE (SUBTYPE = NUMERIC);",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify all range types were introspected
    assert!(
        get_range_type(&schema, "int_range").is_some(),
        "Range type 'int_range' should be introspected"
    );
    assert!(
        get_range_type(&schema, "date_range").is_some(),
        "Range type 'date_range' should be introspected"
    );
    assert!(
        get_range_type(&schema, "numeric_range").is_some(),
        "Range type 'numeric_range' should be introspected"
    );

    // Verify range type details
    let int_range = get_range_type(&schema, "int_range").unwrap();
    let date_range = get_range_type(&schema, "date_range").unwrap();
    let numeric_range = get_range_type(&schema, "numeric_range").unwrap();

    assert_eq!(int_range.info.name, "int_range");
    assert_eq!(date_range.info.name, "date_range");
    assert_eq!(numeric_range.info.name, "numeric_range");
    assert_eq!(int_range.info.schema, "public");
    assert_eq!(date_range.info.schema, "public");
    assert_eq!(numeric_range.info.schema, "public");

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_no_range_types() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Introspect the database without any user range types
    let schema = connection.introspect().await?;

    // Verify no user range types are present
    // Note: System range types should be filtered out
    let user_range_types: Vec<&String> = schema.types
        .iter()
        .filter_map(|(name, t)| {
            if let Type::Range(_) = t {
                Some(name)
            } else {
                None
            }
        })
        .collect();
    assert!(
        user_range_types.is_empty(),
        "No user range types should be introspected: {:?}",
        user_range_types
    );

    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_range_type_edge_cases() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Test case 1: Range type with very long name
    let long_name = "a".repeat(50);
    execute_sql(
        &connection,
        &format!("CREATE TYPE {} AS RANGE (SUBTYPE = INTEGER);", long_name),
    )
    .await?;

    let schema = connection.introspect().await?;
    let range_type = get_range_type(&schema, &long_name);
    assert!(
        range_type.is_some(),
        "Range type with long name should be introspected"
    );

    // Test case 2: Range type with special characters in name
    execute_sql(
        &connection,
        "CREATE TYPE \"test-range-with-dashes\" AS RANGE (SUBTYPE = INTEGER);",
    )
    .await?;

    let schema2 = connection.introspect().await?;
    let range_type2 = get_range_type(&schema2, "test-range-with-dashes");
    assert!(
        range_type2.is_some(),
        "Range type with special characters should be introspected"
    );

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_range_type_consistency() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create a range type
    execute_sql(
        &connection,
        "CREATE TYPE consistency_test_range AS RANGE (SUBTYPE = INTEGER);",
    )
    .await?;

    // Introspect multiple times to verify consistency
    let schema1 = connection.introspect().await?;
    let schema2 = connection.introspect().await?;

    let range_type1 = get_range_type(&schema1, "consistency_test_range").unwrap();
    let range_type2 = get_range_type(&schema2, "consistency_test_range").unwrap();

    // Verify consistency across multiple introspections
    assert_eq!(range_type1.info.name, range_type2.info.name);
    assert_eq!(range_type1.info.schema, range_type2.info.schema);
    assert_eq!(range_type1.subtype, range_type2.subtype);
    assert_eq!(range_type1.collation, range_type2.collation);
    assert_eq!(range_type1.canonical_fn, range_type2.canonical_fn);
    assert_eq!(range_type1.subtype_diff_fn, range_type2.subtype_diff_fn);
    assert_eq!(range_type1.info.comment, range_type2.info.comment);

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_range_type_complex() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create functions for complex range type
    execute_sql(
        &connection,
        "CREATE OR REPLACE FUNCTION complex_range_diff(NUMERIC, NUMERIC) RETURNS DOUBLE PRECISION AS 'SELECT $1 - $2;' LANGUAGE SQL IMMUTABLE;",
    )
    .await?;

    // Create complex range type with all features
    execute_sql(
        &connection,
        "CREATE TYPE complex_range AS RANGE (SUBTYPE = NUMERIC, SUBTYPE_DIFF = complex_range_diff);",
    )
    .await?;

    // Create canonical function after the type exists
    execute_sql(
        &connection,
        "CREATE OR REPLACE FUNCTION complex_range_canonical(complex_range) RETURNS complex_range AS 'SELECT $1;' LANGUAGE SQL IMMUTABLE;",
    )
    .await?;

    // Add canonical function to the range type
    execute_sql(
        &connection,
        "ALTER TYPE complex_range SET (CANONICAL = complex_range_canonical);",
    )
    .await?;

    // Add comment
    execute_sql(
        &connection,
        "COMMENT ON TYPE complex_range IS 'Complex range type with all features';",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the complex range type was introspected
    let range_type = get_range_type(&schema, "complex_range");
    assert!(
        range_type.is_some(),
        "Range type 'complex_range' should be introspected"
    );

    let rt = range_type.unwrap();
    assert_eq!(rt.info.name, "complex_range");
    assert_eq!(rt.subtype, "numeric", "Range type should have correct subtype");
    assert_eq!(rt.collation, None, "Range type should not have collation for numeric subtype");
    assert_eq!(rt.canonical_fn, Some("complex_range_canonical".to_string()), "Range type should have canonical function");
    assert_eq!(rt.subtype_diff_fn, Some("complex_range_diff".to_string()), "Range type should have subtype_diff function");
    assert_eq!(
        rt.info.comment,
        Some("Complex range type with all features".to_string()),
        "Range type should have comment"
    );

    // Clean up
    db.cleanup().await?;
    Ok(())
} 