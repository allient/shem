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
async fn test_introspect_basic_enum() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create a basic enum
    execute_sql(&connection, "CREATE TYPE status_enum AS ENUM ('active', 'inactive', 'pending');").await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the enum was introspected
    let enum_type = schema.enums.get("status_enum");
    debug!("Enum: {:?}", enum_type);
    assert!(
        enum_type.is_some(),
        "Enum 'status_enum' should be introspected"
    );

    let enum_obj = enum_type.unwrap();
    assert_eq!(enum_obj.name, "status_enum");
    assert_eq!(enum_obj.values, vec!["active", "inactive", "pending"], "Enum should have the correct values");
    assert_eq!(enum_obj.comment, None, "Enum should not have a comment");

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_enum_with_schema() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create schema and enum in that schema
    execute_sql(&connection, "CREATE SCHEMA test_enum_schema;").await?;
    execute_sql(&connection, "CREATE TYPE test_enum_schema.priority_enum AS ENUM ('low', 'medium', 'high');").await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the enum was introspected with correct schema
    let enum_type = schema.enums.get("priority_enum");
    assert!(
        enum_type.is_some(),
        "Enum 'priority_enum' should be introspected"
    );

    let enum_obj = enum_type.unwrap();
    assert_eq!(enum_obj.name, "priority_enum");
    assert_eq!(enum_obj.schema, Some("test_enum_schema".to_string()), "Enum should be in the specified schema");
    assert_eq!(enum_obj.values, vec!["low", "medium", "high"], "Enum should have the correct values");

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_enum_with_comment() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create enum and add comment
    execute_sql(&connection, "CREATE TYPE color_enum AS ENUM ('red', 'green', 'blue');").await?;
    execute_sql(
        &connection,
        "COMMENT ON TYPE color_enum IS 'Color options for the application';",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the enum was introspected with comment
    let enum_type = schema.enums.get("color_enum");
    assert!(
        enum_type.is_some(),
        "Enum 'color_enum' should be introspected"
    );
    debug!("Enum: {:?}", enum_type);

    let enum_obj = enum_type.unwrap();
    assert_eq!(enum_obj.name, "color_enum");
    assert_eq!(enum_obj.values, vec!["red", "green", "blue"], "Enum should have the correct values");
    assert_eq!(
        enum_obj.comment,
        Some("Color options for the application".to_string()),
        "Enum should have the specified comment"
    );

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_multiple_enums() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create multiple enums
    execute_sql(&connection, "CREATE TYPE direction_enum AS ENUM ('north', 'south', 'east', 'west');").await?;
    execute_sql(&connection, "CREATE TYPE size_enum AS ENUM ('small', 'medium', 'large');").await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify both enums were introspected
    assert!(
        schema.enums.contains_key("direction_enum"),
        "Enum 'direction_enum' should be introspected"
    );
    assert!(
        schema.enums.contains_key("size_enum"),
        "Enum 'size_enum' should be introspected"
    );

    // Verify enum details
    let direction_enum = schema.enums.get("direction_enum").unwrap();
    let size_enum = schema.enums.get("size_enum").unwrap();

    assert_eq!(direction_enum.name, "direction_enum");
    assert_eq!(direction_enum.values, vec!["north", "south", "east", "west"]);
    assert_eq!(size_enum.name, "size_enum");
    assert_eq!(size_enum.values, vec!["small", "medium", "large"]);

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_enum_with_single_value() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create enum with single value
    execute_sql(&connection, "CREATE TYPE single_value_enum AS ENUM ('only_value');").await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the enum was introspected
    let enum_type = schema.enums.get("single_value_enum");
    assert!(
        enum_type.is_some(),
        "Enum 'single_value_enum' should be introspected"
    );

    let enum_obj = enum_type.unwrap();
    assert_eq!(enum_obj.name, "single_value_enum");
    assert_eq!(enum_obj.values, vec!["only_value"], "Enum should have the single value");

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_no_user_enums() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Introspect the database without any user enums
    let schema = connection.introspect().await?;

    // Verify no user enums are present
    // Note: System enums should be filtered out
    let user_enums: Vec<&String> = schema.enums.keys().collect();
    assert!(
        user_enums.is_empty(),
        "No user enums should be introspected: {:?}",
        user_enums
    );

    db.cleanup().await?;
    Ok(())
}