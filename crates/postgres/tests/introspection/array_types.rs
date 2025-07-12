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
async fn test_introspect_basic_array_type() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create a custom base type first (array type will be auto-created)
    execute_sql(
        &connection,
        "CREATE TYPE test_basic_type AS (value integer);",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the auto-created array type was introspected
    let array_type = schema.array_types.get("_test_basic_type");
    debug!("Array type: {:?}", array_type);
    assert!(
        array_type.is_some(),
        "Array type '_test_basic_type' should be introspected"
    );

    let at = array_type.unwrap();
    assert_eq!(at.name, "_test_basic_type");
    assert_eq!(at.element_type, "test_basic_type");
    assert!(
        at.comment.is_none(),
        "Array type should not have a comment initially"
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

    // Introspect the database
    let schema = connection.introspect().await?;

    // The auto-created array type will be named "_schema_type"
    let array_type = schema.array_types.get("_schema_type");
    assert!(
        array_type.is_some(),
        "Array type '_schema_type' should be introspected"
    );

    let at = array_type.unwrap();
    assert_eq!(at.name, "_schema_type");
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

    // Create base type
    execute_sql(
        &connection,
        "CREATE TYPE test_comment_type AS (color text);",
    )
    .await?;

    // Add a comment to the auto-created array type
    execute_sql(
        &connection,
        "COMMENT ON TYPE _test_comment_type IS 'Array of color types with comment';",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the array type was introspected with comment
    let array_type = schema.array_types.get("_test_comment_type");
    assert!(
        array_type.is_some(),
        "Array type '_test_comment_type' should be introspected"
    );

    let at = array_type.unwrap();
    assert_eq!(at.name, "_test_comment_type");
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

    // Create multiple base types
    execute_sql(&connection, "CREATE TYPE test_type1 AS (value integer);").await?;
    execute_sql(&connection, "CREATE TYPE test_type2 AS (name text);").await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify both auto-created array types were introspected
    assert!(
        schema.array_types.contains_key("_test_type1"),
        "Array type '_test_type1' should be introspected"
    );
    assert!(
        schema.array_types.contains_key("_test_type2"),
        "Array type '_test_type2' should be introspected"
    );

    let array1 = schema.array_types.get("_test_type1").unwrap();
    let array2 = schema.array_types.get("_test_type2").unwrap();

    assert_eq!(array1.name, "_test_type1");
    assert_eq!(array2.name, "_test_type2");
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

    // Introspect the database
    let schema = connection.introspect().await?;

    // PostgreSQL auto-creates the array type with the name "_test_numeric_type"
    let array_type = schema.array_types.get("_test_numeric_type");
    assert!(
        array_type.is_some(),
        "Array type '_test_numeric_type' should be introspected automatically"
    );

    let at = array_type.unwrap();
    assert_eq!(at.name, "_test_numeric_type");
    assert_eq!(at.element_type, "test_numeric_type");

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

    // Test case 1: Array type auto-created for a type with a long name
    let long_name = "a".repeat(50);
    let base_type_long = format!("{}_type", long_name);
    execute_sql(
        &connection,
        &format!("CREATE TYPE {} AS (value text);", base_type_long),
    )
    .await?;

    let schema = connection.introspect().await?;
    let array_type_name = format!("_{}", base_type_long);
    let array_type = schema.array_types.get(&array_type_name);
    assert!(
        array_type.is_some(),
        "Array type with long name should be introspected automatically"
    );

    // Test case 2: Array type auto-created for a type with special characters
    execute_sql(
        &connection,
        "CREATE TYPE \"test-array-with-dashes\" AS (data text);",
    )
    .await?;

    let schema2 = connection.introspect().await?;
    let array_type_name2 = "_test-array-with-dashes";
    let array_type2 = schema2.array_types.get(array_type_name2);
    assert!(
        array_type2.is_some(),
        "Array type with special characters should be introspected automatically"
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

    // Create multiple base types (arrays will be auto-created)
    for i in 1..=10 {
        execute_sql(
            &connection,
            &format!(
                "CREATE TYPE test_perf_type_{} AS (val1_{} integer, val2_{} text, val3_{} numeric);",
                i, i, i, i
            ),
        )
        .await?;
    }

    // Measure introspection performance
    let start = std::time::Instant::now();
    let schema = connection.introspect().await?;
    let duration = start.elapsed();

    // Verify all auto-created array types were introspected
    for i in 1..=10 {
        assert!(
            schema
                .array_types
                .contains_key(&format!("_test_perf_type_{}", i)),
            "Array type _test_perf_type_{} should be introspected",
            i
        );
    }

    // Performance assertion (adjust threshold as needed)
    assert!(
        duration.as_millis() < 1000,
        "Introspection should complete within 1 second, but took {:?}",
        duration
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

    // Create base type ONLY
    execute_sql(
        &connection,
        "CREATE TYPE test_consistency_type AS (first text, second integer, third boolean);",
    )
    .await?;

    // Introspect multiple times to verify consistency
    let schema1 = connection.introspect().await?;
    let schema2 = connection.introspect().await?;

    let at1 = schema1.array_types.get("_test_consistency_type").unwrap();
    let at2 = schema2.array_types.get("_test_consistency_type").unwrap();

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
    let array_type = schema.array_types.get("_test_complex_type");
    debug!("Array type: {:?}", array_type);
    assert!(
        array_type.is_some(),
        "Complex array type '_test_complex_type' should be introspected automatically"
    );

    let at = array_type.unwrap();
    assert_eq!(at.name, "_test_complex_type");
    assert_eq!(at.element_type, "test_complex_type");

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

    // Introspect the database
    let schema = connection.introspect().await?;

    // PostgreSQL automatically creates the array type with name '_element_type' in the same schema.
    let array_type = schema.array_types.get("_element_type");
    assert!(
        array_type.is_some(),
        "Array type '_element_type' should be introspected automatically"
    );

    let at = array_type.unwrap();
    assert_eq!(at.name, "_element_type");
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
