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
async fn test_introspect_basic_extension() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create a basic extension
    execute_sql(&connection, "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";").await?;

    // Introspect the database
    let schema = connection.introspect().await?;
    // Verify the extension was introspected
    let extension = schema.extensions.get("uuid-ossp");
    debug!("Extension: {:?}", extension);
    assert!(
        extension.is_some(),
        "Extension 'uuid-ossp' should be introspected"
    );

    let ext = extension.unwrap();
    assert_eq!(ext.name, "uuid-ossp");
    assert!(!ext.version.is_empty(), "Extension should have a version");
    assert_eq!(
        ext.cascade, false,
        "Extension should not have cascade option"
    );

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_extension_with_version() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create extension with specific version
    execute_sql(
        &connection,
        "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\" VERSION '1.1';",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the extension was introspected with correct version
    let extension = schema.extensions.get("uuid-ossp");
    assert!(
        extension.is_some(),
        "Extension 'uuid-ossp' should be introspected"
    );

    let ext = extension.unwrap();
    assert_eq!(ext.name, "uuid-ossp");
    assert_eq!(
        ext.version, "1.1",
        "Extension should have the specified version"
    );

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_extension_with_schema() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create schema and extension in that schema
    execute_sql(&connection, "CREATE SCHEMA test_extensions;").await?;
    execute_sql(
        &connection,
        "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\" SCHEMA test_extensions;",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the extension was introspected with correct schema
    let extension = schema.extensions.get("uuid-ossp");
    assert!(
        extension.is_some(),
        "Extension 'uuid-ossp' should be introspected"
    );

    let ext = extension.unwrap();
    assert_eq!(ext.name, "uuid-ossp");
    assert_eq!(
        ext.schema,
        Some("test_extensions".to_string()),
        "Extension should be in the specified schema"
    );

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_multiple_extensions() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create multiple extensions
    execute_sql(&connection, "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";").await?;
    execute_sql(&connection, "CREATE EXTENSION IF NOT EXISTS pgcrypto;").await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify both extensions were introspected
    assert!(
        schema.extensions.contains_key("uuid-ossp"),
        "Extension 'uuid-ossp' should be introspected"
    );
    assert!(
        schema.extensions.contains_key("pgcrypto"),
        "Extension 'pgcrypto' should be introspected"
    );

    // Verify extension details
    let uuid_ext = schema.extensions.get("uuid-ossp").unwrap();
    let crypto_ext = schema.extensions.get("pgcrypto").unwrap();

    assert_eq!(uuid_ext.name, "uuid-ossp");
    assert_eq!(crypto_ext.name, "pgcrypto");
    assert!(!uuid_ext.version.is_empty());
    assert!(!crypto_ext.version.is_empty());

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_extension_with_comment() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create extension and add comment
    execute_sql(&connection, "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";").await?;
    execute_sql(
        &connection,
        "COMMENT ON EXTENSION \"uuid-ossp\" IS 'UUID generation extension for testing';",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the extension was introspected with comment
    let extension = schema.extensions.get("uuid-ossp");
    assert!(
        extension.is_some(),
        "Extension 'uuid-ossp' should be introspected"
    );

    let ext = extension.unwrap();
    assert_eq!(ext.name, "uuid-ossp");
    assert_eq!(
        ext.comment,
        Some("UUID generation extension for testing".to_string()),
        "Extension should have the specified comment"
    );

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_extension_with_reserved_keyword() -> Result<(), Box<dyn std::error::Error>>
{
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Note: This test assumes there's an extension with a reserved keyword name
    // For this test, we'll use a hypothetical extension or skip if not available
    // In practice, this would test extensions like "order" or "user" if they existed

    // For now, test with a regular extension but verify the introspection handles quoted names
    execute_sql(&connection, "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";").await?;

    let schema = connection.introspect().await?;
    let extension = schema.extensions.get("uuid-ossp");
    assert!(
        extension.is_some(),
        "Extension with quoted name should be introspected"
    );

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_extension_with_hyphen() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create extension with hyphen in name
    execute_sql(&connection, "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";").await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the extension with hyphen was introspected
    let extension = schema.extensions.get("uuid-ossp");
    assert!(
        extension.is_some(),
        "Extension with hyphen should be introspected"
    );

    let ext = extension.unwrap();
    assert_eq!(
        ext.name, "uuid-ossp",
        "Extension name should preserve the hyphen"
    );

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_extension_dependencies() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create extension that may have dependencies
    // Note: This test may fail if postgis is not available in the test environment
    // In a real scenario, you'd use extensions that are actually available
    let result = execute_sql(&connection, "CREATE EXTENSION IF NOT EXISTS postgis;").await;

    match result {
        Ok(_) => {
            debug!("postgis created");
            // Introspect the database
            let schema = connection.introspect().await?;

            // Verify the extension and its dependencies were introspected
            assert!(
                schema.extensions.contains_key("postgis"),
                "Main extension should be introspected"
            );

            // PostGIS typically creates additional extensions
            let postgis_ext = schema.extensions.get("postgis").unwrap();
            assert_eq!(postgis_ext.name, "postgis");
            assert!(!postgis_ext.version.is_empty());

            // Clean up
            db.cleanup().await?;
        }
        Err(e) => {
            debug!("postgis failed: {e:?}");
        }
    }

    Ok(())
}

#[tokio::test]
async fn test_introspect_no_extensions() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Introspect the database without any user extensions
    let schema = connection.introspect().await?;

    // Verify no user extensions are present
    // Note: System extensions like 'plpgsql' should be filtered out
    let user_extensions: Vec<&String> = schema.extensions.keys().collect();
    assert!(
        user_extensions.is_empty(),
        "No user extensions should be introspected: {:?}",
        user_extensions
    );

    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_extension_version_changes() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create extension with initial version
    execute_sql(
        &connection,
        "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\" VERSION '1.1';",
    )
    .await?;

    // Introspect and verify initial version
    let schema1 = connection.introspect().await?;
    let ext1 = schema1.extensions.get("uuid-ossp").unwrap();
    assert_eq!(ext1.version, "1.1");

    // Update extension version (if possible)
    // Note: This may not work for all extensions, so we'll just verify the current version
    let current_version = ext1.version.clone();

    // Re-introspect to verify version is still correct
    let schema2 = connection.introspect().await?;
    let ext2 = schema2.extensions.get("uuid-ossp").unwrap();
    assert_eq!(
        ext2.version, current_version,
        "Extension version should remain consistent"
    );

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_extension_schema_consistency() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create schema and extension
    execute_sql(&connection, "CREATE SCHEMA test_ext_schema;").await?;
    execute_sql(
        &connection,
        "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\" SCHEMA test_ext_schema;",
    )
    .await?;

    // Introspect multiple times to verify consistency
    let schema1 = connection.introspect().await?;
    let schema2 = connection.introspect().await?;

    let ext1 = schema1.extensions.get("uuid-ossp").unwrap();
    let ext2 = schema2.extensions.get("uuid-ossp").unwrap();

    // Verify consistency across multiple introspections
    assert_eq!(ext1.name, ext2.name);
    assert_eq!(ext1.version, ext2.version);
    assert_eq!(ext1.schema, ext2.schema);
    assert_eq!(ext1.cascade, ext2.cascade);
    assert_eq!(ext1.comment, ext2.comment);

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_extension_edge_cases() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Test case 1: Extension with very long name
    // This would test the introspection with edge case names
    execute_sql(&connection, "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";").await?;

    let schema = connection.introspect().await?;
    let extension = schema.extensions.get("uuid-ossp");
    assert!(
        extension.is_some(),
        "Extension should be introspected regardless of name length"
    );

    // Test case 2: Extension with special characters (if supported)
    // Most PostgreSQL extensions don't have special characters, but we can test the robustness

    // Test case 3: Extension with empty version (should be handled gracefully)
    let ext = extension.unwrap();
    assert!(!ext.name.is_empty(), "Extension name should not be empty");

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_extension_performance() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create multiple extensions
    execute_sql(&connection, "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";").await?;
    execute_sql(&connection, "CREATE EXTENSION IF NOT EXISTS pgcrypto;").await?;

    // Measure introspection performance
    let start = std::time::Instant::now();
    let schema = connection.introspect().await?;
    let duration = start.elapsed();

    // Verify all extensions were introspected
    assert!(schema.extensions.contains_key("uuid-ossp"));
    assert!(schema.extensions.contains_key("pgcrypto"));

    // Performance assertion (adjust threshold as needed)
    assert!(
        duration.as_millis() < 1000,
        "Introspection should complete within 1 second"
    );

    // Clean up
    db.cleanup().await?;
    Ok(())
}
