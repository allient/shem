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
async fn test_introspect_basic_array_type() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create a custom base type first, then create an array type for it
    execute_sql(
        &connection,
        "CREATE TYPE test_basic_type AS (value integer);",
    )
    .await?;

    // Create an array type for our custom type
    execute_sql(
        &connection,
        "CREATE TYPE test_basic_array AS test_basic_type[];",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the array type was introspected
    let array_type = schema.array_types.get("test_basic_array");
    debug!("Array type: {:?}", array_type);
    assert!(
        array_type.is_some(),
        "Array type 'test_basic_array' should be introspected"
    );

    let at = array_type.unwrap();
    assert_eq!(at.name, "test_basic_array");
    assert_eq!(at.schema, None, "Array type should be in public schema");
    assert_eq!(at.element_type, "test_basic_type");
    assert_eq!(
        at.element_schema, None,
        "Element type should be in public schema"
    );
    assert!(
        at.comment.is_none(),
        "Array type should not have comment initially"
    );

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_array_type_with_schema() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create schema and types in that schema
    execute_sql(&connection, "CREATE SCHEMA test_array_types;").await?;
    execute_sql(
        &connection,
        "CREATE TYPE test_array_types.schema_type AS (id integer, name text);",
    )
    .await?;
    execute_sql(
        &connection,
        "CREATE TYPE test_array_types.schema_array AS test_array_types.schema_type[];",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the array type was introspected with correct schema
    let array_type = schema.array_types.get("schema_array");
    assert!(
        array_type.is_some(),
        "Array type 'schema_array' should be introspected"
    );

    let at = array_type.unwrap();
    assert_eq!(at.name, "schema_array");
    assert_eq!(
        at.schema,
        Some("test_array_types".to_string()),
        "Array type should be in the specified schema"
    );
    assert_eq!(at.element_type, "schema_type");
    assert_eq!(
        at.element_schema,
        Some("test_array_types".to_string()),
        "Element type should be in the specified schema"
    );

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_array_type_with_comment() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create base type and array type with comment
    execute_sql(
        &connection,
        "CREATE TYPE test_comment_type AS (color text);",
    )
    .await?;
    execute_sql(
        &connection,
        "CREATE TYPE test_comment_array AS test_comment_type[];",
    )
    .await?;
    execute_sql(
        &connection,
        "COMMENT ON TYPE test_comment_array IS 'Array of color types with comment';",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the array type was introspected with comment
    let array_type = schema.array_types.get("test_comment_array");
    assert!(
        array_type.is_some(),
        "Array type 'test_comment_array' should be introspected"
    );

    let at = array_type.unwrap();
    assert_eq!(at.name, "test_comment_array");
    assert_eq!(
        at.comment,
        Some("Array of color types with comment".to_string()),
        "Array type should have the specified comment"
    );

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_multiple_array_types() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create multiple base types and their array types
    execute_sql(&connection, "CREATE TYPE test_type1 AS (value integer);").await?;
    execute_sql(&connection, "CREATE TYPE test_array1 AS test_type1[];").await?;

    execute_sql(&connection, "CREATE TYPE test_type2 AS (name text);").await?;
    execute_sql(&connection, "CREATE TYPE test_array2 AS test_type2[];").await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify both array types were introspected
    assert!(
        schema.array_types.contains_key("test_array1"),
        "Array type 'test_array1' should be introspected"
    );
    assert!(
        schema.array_types.contains_key("test_array2"),
        "Array type 'test_array2' should be introspected"
    );

    // Verify array type details
    let array1 = schema.array_types.get("test_array1").unwrap();
    let array2 = schema.array_types.get("test_array2").unwrap();

    assert_eq!(array1.name, "test_array1");
    assert_eq!(array2.name, "test_array2");
    assert_eq!(array1.schema, None);
    assert_eq!(array2.schema, None);
    assert_eq!(array1.element_type, "test_type1");
    assert_eq!(array2.element_type, "test_type2");

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_array_type_element_types() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create base type with numeric elements
    execute_sql(
        &connection,
        "CREATE TYPE test_numeric_type AS (count integer, total numeric(10,2));",
    )
    .await?;
    execute_sql(
        &connection,
        "CREATE TYPE test_numeric_array AS test_numeric_type[];",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the array type was introspected with correct element type
    let array_type = schema.array_types.get("test_numeric_array");
    assert!(
        array_type.is_some(),
        "Array type 'test_numeric_array' should be introspected"
    );

    let at = array_type.unwrap();
    assert_eq!(at.name, "test_numeric_array");
    assert_eq!(at.element_type, "test_numeric_type");
    assert_eq!(at.element_schema, None);

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_no_array_types() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Introspect the database without any user array types
    let schema = connection.introspect().await?;

    // Verify no user array types are present
    // Note: System array types should be filtered out
    let user_array_types: Vec<&String> = schema.array_types.keys().collect();
    assert!(
        user_array_types.is_empty(),
        "No user array types should be introspected: {:?}",
        user_array_types
    );

    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_array_type_edge_cases() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Test case 1: Array type with very long name
    let long_name = "a".repeat(50);
    execute_sql(&connection, "CREATE TYPE test_long_type AS (value text);").await?;
    execute_sql(
        &connection,
        &format!("CREATE TYPE {} AS test_long_type[];", long_name),
    )
    .await?;

    let schema = connection.introspect().await?;
    let array_type = schema.array_types.get(&long_name);
    assert!(
        array_type.is_some(),
        "Array type with long name should be introspected"
    );

    // Test case 2: Array type with special characters in name
    execute_sql(&connection, "CREATE TYPE test_special_type AS (data text);").await?;
    execute_sql(
        &connection,
        "CREATE TYPE \"test-array-with-dashes\" AS test_special_type[];",
    )
    .await?;

    let schema2 = connection.introspect().await?;
    let array_type2 = schema2.array_types.get("test-array-with-dashes");
    assert!(
        array_type2.is_some(),
        "Array type with special characters should be introspected"
    );

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_array_type_performance() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create multiple base types and array types
    for i in 1..=10 {
        execute_sql(
            &connection,
            &format!("CREATE TYPE test_perf_type_{} AS (val1_{} integer, val2_{} text, val3_{} numeric);", i, i, i, i),
        )
        .await?;
        execute_sql(
            &connection,
            &format!(
                "CREATE TYPE test_perf_array_{} AS test_perf_type_{}[];",
                i, i
            ),
        )
        .await?;
    }

    // Measure introspection performance
    let start = std::time::Instant::now();
    let schema = connection.introspect().await?;
    let duration = start.elapsed();

    // Verify all array types were introspected
    for i in 1..=10 {
        assert!(
            schema
                .array_types
                .contains_key(&format!("test_perf_array_{}", i)),
            "Array type test_perf_array_{} should be introspected",
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
async fn test_introspect_array_type_consistency() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create base type and array type
    execute_sql(
        &connection,
        "CREATE TYPE test_consistency_type AS (first text, second integer, third boolean);",
    )
    .await?;
    execute_sql(
        &connection,
        "CREATE TYPE test_consistency_array AS test_consistency_type[];",
    )
    .await?;

    // Introspect multiple times to verify consistency
    let schema1 = connection.introspect().await?;
    let schema2 = connection.introspect().await?;

    let at1 = schema1.array_types.get("test_consistency_array").unwrap();
    let at2 = schema2.array_types.get("test_consistency_array").unwrap();

    // Verify consistency across multiple introspections
    assert_eq!(at1.name, at2.name);
    assert_eq!(at1.schema, at2.schema);
    assert_eq!(at1.element_type, at2.element_type);
    assert_eq!(at1.element_schema, at2.element_schema);
    assert_eq!(at1.comment, at2.comment);

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_array_type_complex_structure() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create a complex base type with many fields
    execute_sql(
        &connection,
        r#"
        CREATE TYPE test_complex_type AS (
            monday text, tuesday text, wednesday text, thursday text, friday text,
            saturday text, sunday text, january text, february text, march text,
            april text, may text, june text, july text, august text,
            september text, october text, november text, december text
        );
        "#,
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the complex array type was introspected automatically
    let array_type = schema.array_types.get("test_complex_type[]");
    debug!("Array type: {:?}", array_type);
    assert!(
        array_type.is_some(),
        "Complex array type 'test_complex_type[]' should be introspected automatically"
    );

    let at = array_type.unwrap();
    assert_eq!(at.name, "test_complex_type[]");
    assert_eq!(at.schema, None); // Default public schema, adjust if your introspector includes "public"
    assert_eq!(at.element_type, "test_complex_type");
    assert_eq!(at.element_schema, None); // Default public schema, adjust if needed

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_array_type_schema_consistency() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create schema and composite type
    execute_sql(&connection, "CREATE SCHEMA test_array_schema;").await?;
    execute_sql(
        &connection,
        "CREATE TYPE test_array_schema.schema_consistency_type AS (alpha text, beta integer, gamma boolean);",
    )
    .await?;

    // Introspect multiple times to verify consistency
    let schema1 = connection.introspect().await?;
    let schema2 = connection.introspect().await?;

    let at1 = schema1
        .array_types
        .get("_schema_consistency_type")
        .expect("Array type '_schema_consistency_type' should exist in schema1");

    let at2 = schema2
        .array_types
        .get("_schema_consistency_type")
        .expect("Array type '_schema_consistency_type' should exist in schema2");

    // Verify consistency across multiple introspections
    assert_eq!(at1.name, at2.name);
    assert_eq!(at1.schema, at2.schema);
    assert_eq!(at1.element_type, at2.element_type);
    assert_eq!(at1.element_schema, at2.element_schema);
    assert_eq!(at1.comment, at2.comment);

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_array_type_element_schema() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create schema and types in that schema
    execute_sql(&connection, "CREATE SCHEMA test_element_schema;").await?;
    execute_sql(
        &connection,
        "CREATE TYPE test_element_schema.element_type AS (option1 text, option2 integer, option3 boolean);",
    )
    .await?;
    execute_sql(
        &connection,
        "CREATE TYPE test_element_schema.element_array AS test_element_schema.element_type[];",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the array type was introspected with correct element schema
    let array_type = schema.array_types.get("element_array");
    assert!(
        array_type.is_some(),
        "Array type 'element_array' should be introspected"
    );

    let at = array_type.unwrap();
    assert_eq!(at.name, "element_array");
    assert_eq!(
        at.schema,
        Some("test_element_schema".to_string()),
        "Array type should be in the specified schema"
    );
    assert_eq!(at.element_type, "element_type");
    assert_eq!(
        at.element_schema,
        Some("test_element_schema".to_string()),
        "Element type should be in the specified schema"
    );

    // Clean up
    db.cleanup().await?;
    Ok(())
}
