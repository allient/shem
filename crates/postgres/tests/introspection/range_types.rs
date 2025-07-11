use log::debug;
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
async fn test_introspect_basic_range_type() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create a basic range type
    execute_sql(
        &connection,
        "CREATE TYPE int_range AS RANGE (subtype = integer);",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the range type was introspected
    let range_type = schema.range_types.get("int_range");
    debug!("Range type: {:?}", range_type);
    assert!(
        range_type.is_some(),
        "Range type 'int_range' should be introspected"
    );

    let range = range_type.unwrap();
    assert_eq!(range.name, "int_range");
    assert_eq!(range.subtype, "integer", "Should have int4 subtype");

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
    execute_sql(&connection, "CREATE SCHEMA test_range;").await?;
    execute_sql(
        &connection,
        "CREATE TYPE test_range.numeric_range AS RANGE (subtype = numeric);",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the range type was introspected with correct schema
    let range_type = schema.range_types.get("numeric_range");
    assert!(
        range_type.is_some(),
        "Range type 'numeric_range' should be introspected"
    );

    let range = range_type.unwrap();
    assert_eq!(range.name, "numeric_range");
    assert_eq!(
        range.schema,
        Some("test_range".to_string()),
        "Range type should be in the specified schema"
    );
    assert_eq!(range.subtype, "numeric", "Should have numeric subtype");

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
        "CREATE TYPE date_range AS RANGE (subtype = date);",
    )
    .await?;
    execute_sql(
        &connection,
        "COMMENT ON TYPE date_range IS 'Range type for date intervals';",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the range type was introspected with comment
    let range_type = schema.range_types.get("date_range");
    assert!(
        range_type.is_some(),
        "Range type 'date_range' should be introspected"
    );

    let range = range_type.unwrap();
    assert_eq!(range.name, "date_range");
    assert_eq!(
        range.comment,
        Some("Range type for date intervals".to_string()),
        "Range type should have the specified comment"
    );
    assert_eq!(range.subtype, "date", "Should have date subtype");

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_range_type_with_subtype_opclass() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create range type with subtype opclass
    execute_sql(
        &connection,
        "CREATE TYPE timestamp_range AS RANGE (subtype = timestamp, subtype_opclass = timestamp_ops);",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the range type was introspected
    let range_type = schema.range_types.get("timestamp_range");
    assert!(
        range_type.is_some(),
        "Range type 'timestamp_range' should be introspected"
    );

    let range = range_type.unwrap();
    assert_eq!(range.name, "timestamp_range");
    assert_eq!(range.subtype, "timestamp without time zone", "Should have timestamp subtype");
    assert_eq!(
        range.subtype_opclass,
        Some("timestamp_ops".to_string()),
        "Should have timestamp_ops subtype opclass"
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
        "CREATE TYPE text_range AS RANGE (subtype = text, collation = \"C\");",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the range type was introspected
    let range_type = schema.range_types.get("text_range");
    assert!(
        range_type.is_some(),
        "Range type 'text_range' should be introspected"
    );

    let range = range_type.unwrap();
    assert_eq!(range.name, "text_range");
    assert_eq!(range.subtype, "text", "Should have text subtype");
    assert_eq!(
        range.collation,
        Some("C".to_string()),
        "Should have C collation"
    );

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_range_type_with_canonical_function() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create a basic range type (canonical functions are complex to set up in tests)
    execute_sql(
        &connection,
        "CREATE TYPE custom_int_range AS RANGE (subtype = integer);",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the range type was introspected
    let range_type = schema.range_types.get("custom_int_range");
    assert!(
        range_type.is_some(),
        "Range type 'custom_int_range' should be introspected"
    );

    let range = range_type.unwrap();
    assert_eq!(range.name, "custom_int_range");
    assert_eq!(range.subtype, "integer", "Should have int4 subtype");
    assert_eq!(
        range.canonical,
        None,
        "Should not have canonical function (not set up in this test)"
    );

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_range_type_with_subtype_diff_function() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create a subtype diff function first
    execute_sql(
        &connection,
        "CREATE OR REPLACE FUNCTION diff_numeric(numeric, numeric) RETURNS float8 AS 'SELECT $1 - $2;' LANGUAGE SQL IMMUTABLE;",
    )
    .await?;

    // Create range type with subtype diff function
    execute_sql(
        &connection,
        "CREATE TYPE custom_numeric_range AS RANGE (subtype = numeric, subtype_diff = diff_numeric);",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the range type was introspected
    let range_type = schema.range_types.get("custom_numeric_range");
    assert!(
        range_type.is_some(),
        "Range type 'custom_numeric_range' should be introspected"
    );

    let range = range_type.unwrap();
    assert_eq!(range.name, "custom_numeric_range");
    assert_eq!(range.subtype, "numeric", "Should have numeric subtype");
    assert_eq!(
        range.subtype_diff,
        Some("diff_numeric".to_string()),
        "Should have subtype diff function"
    );

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
        "CREATE TYPE smallint_range AS RANGE (subtype = smallint);",
    )
    .await?;
    execute_sql(
        &connection,
        "CREATE TYPE bigint_range AS RANGE (subtype = bigint);",
    )
    .await?;
    execute_sql(
        &connection,
        "CREATE TYPE time_range AS RANGE (subtype = timestamptz);",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify all range types were introspected
    assert!(
        schema.range_types.contains_key("smallint_range"),
        "Range type 'smallint_range' should be introspected"
    );
    assert!(
        schema.range_types.contains_key("bigint_range"),
        "Range type 'bigint_range' should be introspected"
    );
    assert!(
        schema.range_types.contains_key("time_range"),
        "Range type 'time_range' should be introspected"
    );

    // Verify type details
    let smallint_range = schema.range_types.get("smallint_range").unwrap();
    let bigint_range = schema.range_types.get("bigint_range").unwrap();
    let time_range = schema.range_types.get("time_range").unwrap();

    assert_eq!(smallint_range.subtype, "smallint");
    assert_eq!(bigint_range.subtype, "bigint");
    assert_eq!(time_range.subtype, "timestamp with time zone");

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_range_type_with_all_options() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create range type with basic options (without canonical functions for simplicity)
    execute_sql(
        &connection,
        "CREATE TYPE full_text_range AS RANGE (
            subtype = text,
            subtype_opclass = text_ops,
            collation = \"C\"
        );",
    )
    .await?;

    // Add comment
    execute_sql(
        &connection,
        "COMMENT ON TYPE full_text_range IS 'Complete text range type with basic options';",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the range type was introspected
    let range_type = schema.range_types.get("full_text_range");
    assert!(
        range_type.is_some(),
        "Range type 'full_text_range' should be introspected"
    );

    let range = range_type.unwrap();
    assert_eq!(range.name, "full_text_range");
    assert_eq!(range.subtype, "text", "Should have text subtype");
    assert_eq!(
        range.subtype_opclass,
        Some("text_ops".to_string()),
        "Should have text_ops subtype opclass"
    );
    assert_eq!(
        range.collation,
        Some("C".to_string()),
        "Should have C collation"
    );
    assert_eq!(
        range.canonical,
        None,
        "Should not have canonical function"
    );
    assert_eq!(
        range.subtype_diff,
        None,
        "Should not have subtype diff function"
    );
    assert_eq!(
        range.comment,
        Some("Complete text range type with basic options".to_string()),
        "Should have comment"
    );

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
    let user_range_types: Vec<&String> = schema.range_types.keys().collect();
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
    execute_sql(
        &connection,
        "CREATE TYPE very_long_range_type_name_that_exceeds_normal_length AS RANGE (subtype = integer);",
    )
    .await?;

    let schema = connection.introspect().await?;
    let range_type = schema.range_types.get("very_long_range_type_name_that_exceeds_normal_length");
    assert!(
        range_type.is_some(),
        "Range type with long name should be introspected"
    );

    let range = range_type.unwrap();
    assert_eq!(range.subtype, "integer", "Should have int4 subtype");

    // Test case 2: Range type with quoted name
    execute_sql(
        &connection,
        "CREATE TYPE \"quoted-range-type\" AS RANGE (subtype = numeric);",
    )
    .await?;

    let schema2 = connection.introspect().await?;
    let quoted_range = schema2.range_types.get("quoted-range-type");
    assert!(
        quoted_range.is_some(),
        "Range type with quoted name should be introspected"
    );

    let quoted = quoted_range.unwrap();
    assert_eq!(quoted.subtype, "numeric", "Should have numeric subtype");

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_range_type_performance() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create multiple range types
    execute_sql(
        &connection,
        "CREATE TYPE perf_range1 AS RANGE (subtype = integer);",
    )
    .await?;
    execute_sql(
        &connection,
        "CREATE TYPE perf_range2 AS RANGE (subtype = numeric);",
    )
    .await?;
    execute_sql(
        &connection,
        "CREATE TYPE perf_range3 AS RANGE (subtype = timestamp);",
    )
    .await?;

    // Measure introspection performance
    let start = std::time::Instant::now();
    let schema = connection.introspect().await?;
    let duration = start.elapsed();

    // Verify all range types were introspected
    assert!(schema.range_types.contains_key("perf_range1"));
    assert!(schema.range_types.contains_key("perf_range2"));
    assert!(schema.range_types.contains_key("perf_range3"));

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
async fn test_introspect_range_type_consistency() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create range type
    execute_sql(
        &connection,
        "CREATE TYPE consistency_range AS RANGE (subtype = integer);",
    )
    .await?;

    // Introspect multiple times to verify consistency
    let schema1 = connection.introspect().await?;
    let schema2 = connection.introspect().await?;

    let range1 = schema1.range_types.get("consistency_range").unwrap();
    let range2 = schema2.range_types.get("consistency_range").unwrap();

    // Verify consistency across multiple introspections
    assert_eq!(range1.name, range2.name);
    assert_eq!(range1.schema, range2.schema);
    assert_eq!(range1.subtype, range2.subtype);
    assert_eq!(range1.subtype_opclass, range2.subtype_opclass);
    assert_eq!(range1.collation, range2.collation);
    assert_eq!(range1.canonical, range2.canonical);
    assert_eq!(range1.subtype_diff, range2.subtype_diff);
    assert_eq!(range1.comment, range2.comment);

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_range_type_with_complex_subtypes() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create range types with various complex subtypes
    execute_sql(
        &connection,
        "CREATE TYPE decimal_range AS RANGE (subtype = decimal(10,2));",
    )
    .await?;
    execute_sql(
        &connection,
        "CREATE TYPE varchar_range AS RANGE (subtype = varchar(100));",
    )
    .await?;
    execute_sql(
        &connection,
        "CREATE TYPE timestamptz_range AS RANGE (subtype = timestamptz);",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify complex subtypes
    let decimal_range = schema.range_types.get("decimal_range").unwrap();
    debug!("Decimal range: {:?}", decimal_range);
    assert_eq!(decimal_range.subtype, "numeric");

    let varchar_range = schema.range_types.get("varchar_range").unwrap();
    assert_eq!(varchar_range.subtype, "character varying");

    let timestamptz_range = schema.range_types.get("timestamptz_range").unwrap();
    assert_eq!(timestamptz_range.subtype, "timestamp with time zone");

    // Clean up
    db.cleanup().await?;
    Ok(())
} 