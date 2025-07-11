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
async fn test_introspect_basic_multirange_type() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create a basic range type which will automatically create a multirange type
    execute_sql(
        &connection,
        "CREATE TYPE test_basic_range AS RANGE (subtype = integer);",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the multirange type was introspected
    let multirange_type = schema.multirange_types.get("test_basic_multirange");
    debug!("Multirange type: {:?}", multirange_type);
    assert!(
        multirange_type.is_some(),
        "Multirange type 'test_basic_multirange' should be introspected"
    );

    let mrt = multirange_type.unwrap();
    assert_eq!(mrt.name, "test_basic_multirange");
    assert_eq!(mrt.range_type, "test_basic_range");
    assert!(
        mrt.comment.is_none(),
        "Multirange type should not have comment initially"
    );

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_multirange_type_with_schema() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create schema and range type in that schema
    execute_sql(&connection, "CREATE SCHEMA test_multirange_schema;").await?;
    execute_sql(
        &connection,
        "CREATE TYPE test_multirange_schema.schema_range AS RANGE (subtype = numeric);",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the multirange type was introspected with correct schema
    let multirange_type = schema.multirange_types.get("schema_multirange");
    assert!(
        multirange_type.is_some(),
        "Multirange type 'schema_multirange' should be introspected"
    );

    let mrt = multirange_type.unwrap();
    assert_eq!(mrt.name, "schema_multirange");
    assert_eq!(
        mrt.schema,
        Some("test_multirange_schema".to_string()),
        "Multirange type should be in the specified schema"
    );
    assert_eq!(mrt.range_type, "schema_range");
    assert_eq!(
        mrt.range_schema,
        Some("test_multirange_schema".to_string()),
        "Range type should be in the specified schema"
    );

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_multirange_type_with_comment() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create range type and add comment
    execute_sql(
        &connection,
        "CREATE TYPE test_comment_range AS RANGE (subtype = timestamp);",
    )
    .await?;
    execute_sql(
        &connection,
        "COMMENT ON TYPE test_comment_multirange IS 'Time range with comment';",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the multirange type was introspected with comment
    let multirange_type = schema.multirange_types.get("test_comment_multirange");
    assert!(
        multirange_type.is_some(),
        "Multirange type 'test_comment_multirange' should be introspected"
    );

    let mrt = multirange_type.unwrap();
    assert_eq!(mrt.name, "test_comment_multirange");
    assert_eq!(
        mrt.comment,
        Some("Time range with comment".to_string()),
        "Multirange type should have the specified comment"
    );

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_multiple_multirange_types() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create multiple range types
    execute_sql(
        &connection,
        "CREATE TYPE test_range1 AS RANGE (subtype = integer);",
    )
    .await?;
    execute_sql(
        &connection,
        "CREATE TYPE test_range2 AS RANGE (subtype = text);",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    debug!("Schema: {:?}", schema);

    // Verify both multirange types were introspected
    assert!(
        schema.multirange_types.contains_key("test_multirange1"),
        "Multirange type 'test_multirange1' should be introspected"
    );
    assert!(
        schema.multirange_types.contains_key("test_multirange2"),
        "Multirange type 'test_multirange2' should be introspected"
    );

    // Verify multirange type details
    let mrt1 = schema.multirange_types.get("test_multirange1").unwrap();
    let mrt2 = schema.multirange_types.get("test_multirange2").unwrap();

    assert_eq!(mrt1.name, "test_multirange1");
    assert_eq!(mrt2.name, "test_multirange2");
    assert_eq!(mrt1.range_type, "test_range1");
    assert_eq!(mrt2.range_type, "test_range2");

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_multirange_type_range_types() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create range types with different subtypes
    execute_sql(
        &connection,
        "CREATE TYPE test_numeric_range AS RANGE (subtype = numeric(10,2));",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the multirange type was introspected with correct range type
    let multirange_type = schema.multirange_types.get("test_numeric_multirange");
    assert!(
        multirange_type.is_some(),
        "Multirange type 'test_numeric_multirange' should be introspected"
    );

    let mrt = multirange_type.unwrap();
    assert_eq!(mrt.name, "test_numeric_multirange");
    assert_eq!(mrt.range_type, "test_numeric_range");

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_no_multirange_types() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Introspect the database without any user multirange types
    let schema = connection.introspect().await?;

    // Verify no user multirange types are present
    // Note: System multirange types should be filtered out
    let user_multirange_types: Vec<&String> = schema.multirange_types.keys().collect();
    assert!(
        user_multirange_types.is_empty(),
        "No user multirange types should be introspected: {:?}",
        user_multirange_types
    );

    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_multirange_type_edge_cases() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Test case 1: Multirange type with very long name
    let long_name = "a".repeat(50);
    execute_sql(
        &connection,
        &format!("CREATE TYPE {} AS RANGE (subtype = integer);", long_name),
    )
    .await?;

    let schema = connection.introspect().await?;
    let multirange_type = schema
        .multirange_types
        .get(&format!("{}_multirange", long_name));
    assert!(
        multirange_type.is_some(),
        "Multirange type with long name should be introspected"
    );

    // Test case 2: Multirange type with special characters in name
    execute_sql(
        &connection,
        "CREATE TYPE \"test-multirange-with-dashes\" AS RANGE (subtype = text);",
    )
    .await?;

    let schema2 = connection.introspect().await?;

    let multirange_type2 = schema2
        .multirange_types
        .get("test-multimultirange-with-dashes");
    assert!(
        multirange_type2.is_some(),
        "Multirange type with special characters should be introspected"
    );

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_multirange_type_performance() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create multiple range types
    for i in 1..=10 {
        execute_sql(
            &connection,
            &format!(
                "CREATE TYPE test_perf_range_{} AS RANGE (subtype = integer);",
                i
            ),
        )
        .await?;
    }

    // Measure introspection performance
    let start = std::time::Instant::now();
    let schema = connection.introspect().await?;
    let duration = start.elapsed();

    // Verify all multirange types were introspected
    for i in 1..=10 {
        assert!(
            schema
                .multirange_types
                .contains_key(&format!("test_perf_multirange_{}", i)),
            "Multirange type test_perf_multirange_{} should be introspected",
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
async fn test_introspect_multirange_type_consistency() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create a range type
    execute_sql(
        &connection,
        "CREATE TYPE test_consistency_range AS RANGE (subtype = timestamp with time zone);",
    )
    .await?;

    // Introspect multiple times to verify consistency
    let schema1 = connection.introspect().await?;
    let schema2 = connection.introspect().await?;

    let mrt1 = schema1
        .multirange_types
        .get("test_consistency_multirange")
        .unwrap();
    let mrt2 = schema2
        .multirange_types
        .get("test_consistency_multirange")
        .unwrap();

    // Verify consistency across multiple introspections
    assert_eq!(mrt1.name, mrt2.name);
    assert_eq!(mrt1.schema, mrt2.schema);
    assert_eq!(mrt1.range_type, mrt2.range_type);
    assert_eq!(mrt1.range_schema, mrt2.range_schema);
    assert_eq!(mrt1.comment, mrt2.comment);

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_multirange_type_complex_structure()
-> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create a complex range type with subtype opclass
    execute_sql(
        &connection,
        "CREATE TYPE test_complex_range AS RANGE (subtype = numeric, subtype_opclass = numeric_ops);",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the complex multirange type was introspected
    let multirange_type = schema.multirange_types.get("test_complex_multirange");
    assert!(
        multirange_type.is_some(),
        "Complex multirange type 'test_complex_multirange' should be introspected"
    );

    let mrt = multirange_type.unwrap();
    assert_eq!(mrt.name, "test_complex_multirange");
    assert_eq!(mrt.range_type, "test_complex_range");

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_multirange_type_schema_consistency()
-> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create schema and range type
    execute_sql(
        &connection,
        "CREATE SCHEMA test_multirange_schema_consistency;",
    )
    .await?;
    execute_sql(
        &connection,
        "CREATE TYPE test_multirange_schema_consistency.schema_consistency_range AS RANGE (subtype = date);",
    )
    .await?;

    // Introspect multiple times to verify consistency
    let schema1 = connection.introspect().await?;
    let schema2 = connection.introspect().await?;

    let mrt1 = schema1
        .multirange_types
        .get("schema_consistency_multirange")
        .unwrap();
    let mrt2 = schema2
        .multirange_types
        .get("schema_consistency_multirange")
        .unwrap();

    // Verify consistency across multiple introspections
    assert_eq!(mrt1.name, mrt2.name);
    assert_eq!(mrt1.schema, mrt2.schema);
    assert_eq!(mrt1.range_type, mrt2.range_type);
    assert_eq!(mrt1.range_schema, mrt2.range_schema);
    assert_eq!(mrt1.comment, mrt2.comment);

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_multirange_type_range_schema() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create schema and range type in that schema
    execute_sql(&connection, "CREATE SCHEMA test_range_schema;").await?;
    execute_sql(
        &connection,
        "CREATE TYPE test_range_schema.range_type AS RANGE (subtype = time);",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    debug!("Schema: {:?}", schema);
    // Verify the multirange type was introspected with correct range schema
    let multirange_type = schema.multirange_types.get("multirange_type");
    assert!(
        multirange_type.is_some(),
        "Multirange type 'multirange_type' should be introspected"
    );

    let mrt = multirange_type.unwrap();
    assert_eq!(mrt.name, "multirange_type");
    assert_eq!(
        mrt.schema,
        Some("test_range_schema".to_string()),
        "Multirange type should be in the specified schema"
    );
    assert_eq!(mrt.range_type, "range_type");
    assert_eq!(
        mrt.range_schema,
        Some("test_range_schema".to_string()),
        "Range type should be in the specified schema"
    );

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_multirange_type_comment_consistency()
-> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create range type with comment
    execute_sql(
        &connection,
        "CREATE TYPE test_comment_consistency_range AS RANGE (subtype = interval);",
    )
    .await?;
    execute_sql(
        &connection,
        "COMMENT ON TYPE test_comment_consistency_multirange IS 'Interval range type';",
    )
    .await?;

    // Introspect multiple times to verify comment consistency
    let schema1 = connection.introspect().await?;
    let schema2 = connection.introspect().await?;

    let mrt1 = schema1
        .multirange_types
        .get("test_comment_consistency_multirange")
        .unwrap();
    let mrt2 = schema2
        .multirange_types
        .get("test_comment_consistency_multirange")
        .unwrap();

    // Verify comment consistency across multiple introspections
    assert_eq!(mrt1.comment, mrt2.comment);
    assert_eq!(
        mrt1.comment,
        Some("Interval range type".to_string()),
        "Multirange type should have the correct comment"
    );

    // Clean up
    db.cleanup().await?;
    Ok(())
}
