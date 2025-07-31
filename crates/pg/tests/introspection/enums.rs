use tracing::debug;
use pg::db_util::TestDb;
use pg::traits::DatabaseConnection;
use pg::model::types::{Type, EnumType};
use pg::database::DatabaseModel;

/// Test helper function to execute SQL on the test database
async fn execute_sql(
    connection: &Box<dyn DatabaseConnection>,
    sql: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    connection.execute(sql).await?;
    Ok(())
}

/// Helper function to get an enum type from the unified types map
fn get_enum_type<'a>(schema: &'a DatabaseModel, name: &str) -> Option<&'a EnumType> {
    schema.types.get(name).and_then(|t| {
        if let Type::Enum(et) = t {
            Some(et)
        } else {
            None
        }
    })
}

#[tokio::test]
async fn test_introspect_basic_enum() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create a basic enum
    execute_sql(
        &connection,
        "CREATE TYPE status_enum AS ENUM ('active', 'inactive', 'pending');",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the enum was introspected
    let enum_type = get_enum_type(&schema, "status_enum");
    debug!("Enum type: {:?}", enum_type);
    assert!(
        enum_type.is_some(),
        "Enum 'status_enum' should be introspected"
    );

    let et = enum_type.unwrap();
    assert_eq!(et.info.name, "status_enum");
    assert_eq!(et.info.schema, "public", "Enum should be in public schema");
    assert!(!et.values.is_empty(), "Enum should have values");
    assert_eq!(et.values.len(), 3, "Enum should have 3 values");
    assert_eq!(et.values[0].label, "active");
    assert_eq!(et.values[1].label, "inactive");
    assert_eq!(et.values[2].label, "pending");

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
    execute_sql(&connection, "CREATE SCHEMA test_enums;").await?;
    execute_sql(
        &connection,
        "CREATE TYPE test_enums.priority_enum AS ENUM ('low', 'medium', 'high');",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the enum was introspected with correct schema
    let enum_type = get_enum_type(&schema, "priority_enum");
    assert!(
        enum_type.is_some(),
        "Enum 'priority_enum' should be introspected"
    );

    let et = enum_type.unwrap();
    assert_eq!(et.info.name, "priority_enum");
    assert_eq!(
        et.info.schema,
        "test_enums",
        "Enum should be in the specified schema"
    );

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
    execute_sql(
        &connection,
        "CREATE TYPE color_enum AS ENUM ('red', 'green', 'blue');",
    )
    .await?;
    execute_sql(
        &connection,
        "COMMENT ON TYPE color_enum IS 'Basic color enum';",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the enum was introspected with comment
    let enum_type = get_enum_type(&schema, "color_enum");
    assert!(
        enum_type.is_some(),
        "Enum 'color_enum' should be introspected"
    );

    let et = enum_type.unwrap();
    assert_eq!(et.info.name, "color_enum");
    assert_eq!(
        et.info.comment,
        Some("Basic color enum".to_string()),
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
    execute_sql(
        &connection,
        "CREATE TYPE direction_enum AS ENUM ('north', 'south', 'east', 'west');",
    )
    .await?;
    execute_sql(
        &connection,
        "CREATE TYPE size_enum AS ENUM ('small', 'medium', 'large');",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify both enums were introspected
    assert!(
        get_enum_type(&schema, "direction_enum").is_some(),
        "Enum 'direction_enum' should be introspected"
    );
    assert!(
        get_enum_type(&schema, "size_enum").is_some(),
        "Enum 'size_enum' should be introspected"
    );

    // Verify enum details
    let direction_enum = get_enum_type(&schema, "direction_enum").unwrap();
    let size_enum = get_enum_type(&schema, "size_enum").unwrap();

    assert_eq!(direction_enum.info.name, "direction_enum");
    assert_eq!(size_enum.info.name, "size_enum");
    assert_eq!(direction_enum.info.schema, "public");
    assert_eq!(size_enum.info.schema, "public");

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_single_value_enum() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create enum with single value
    execute_sql(
        &connection,
        "CREATE TYPE single_value_enum AS ENUM ('only_value');",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the enum was introspected
    let enum_type = get_enum_type(&schema, "single_value_enum");
    assert!(
        enum_type.is_some(),
        "Enum 'single_value_enum' should be introspected"
    );

    let et = enum_type.unwrap();
    assert_eq!(et.info.name, "single_value_enum");
    assert_eq!(et.values.len(), 1, "Enum should have 1 value");
    assert_eq!(et.values[0].label, "only_value");

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_no_enums() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Introspect the database without any user enums
    let schema = connection.introspect().await?;

    // Verify no user enums are present
    // Note: System enums should be filtered out
    let user_enums: Vec<&String> = schema.types
        .iter()
        .filter_map(|(name, t)| {
            if let Type::Enum(_) = t {
                Some(name)
            } else {
                None
            }
        })
        .collect();
    assert!(
        user_enums.is_empty(),
        "No user enums should be introspected: {:?}",
        user_enums
    );

    db.cleanup().await?;
    Ok(())
}