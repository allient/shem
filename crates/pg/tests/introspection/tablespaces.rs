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
async fn test_introspect_basic_tablespace() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create a basic tablespace
    // Note: This may fail if the user doesn't have CREATE TABLESPACE privileges
    let result = execute_sql(
        &connection,
        "CREATE TABLESPACE test_tablespace LOCATION '/tmp/test_tablespace';",
    )
    .await;

    match result {
        Ok(_) => {
            // Introspect the database
            let schema = connection.introspect().await?;

            // Verify the tablespace was introspected
            let tablespace = schema.tablespaces.get("test_tablespace");
            debug!("Tablespace: {:?}", tablespace);
            assert!(
                tablespace.is_some(),
                "Tablespace 'test_tablespace' should be introspected"
            );

            let tablespace_obj = tablespace.unwrap();
            assert_eq!(tablespace_obj.name, "test_tablespace");
            assert!(
                !tablespace_obj.owner.is_empty(),
                "Tablespace should have an owner"
            );
            assert_eq!(
                tablespace_obj.comment, None,
                "Tablespace should not have a comment"
            );

            // Clean up
            execute_sql(&connection, "DROP TABLESPACE test_tablespace;").await?;
        }
        Err(e) => {
            debug!("Tablespace creation failed (likely due to permissions): {e:?}");
            // Skip test if tablespace creation is not allowed
        }
    }

    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_tablespace_with_owner() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create tablespace with owner
    let result = execute_sql(&connection, "CREATE TABLESPACE test_tablespace_owner LOCATION '/tmp/test_tablespace_owner' OWNER postgres;").await;

    match result {
        Ok(_) => {
            // Introspect the database
            let schema = connection.introspect().await?;

            // Verify the tablespace was introspected with owner
            let tablespace = schema.tablespaces.get("test_tablespace_owner");
            assert!(
                tablespace.is_some(),
                "Tablespace 'test_tablespace_owner' should be introspected"
            );

            let tablespace_obj = tablespace.unwrap();
            assert_eq!(tablespace_obj.name, "test_tablespace_owner");
            assert_eq!(
                tablespace_obj.owner, "postgres",
                "Tablespace should have the specified owner"
            );

            // Clean up
            execute_sql(&connection, "DROP TABLESPACE test_tablespace_owner;").await?;
        }
        Err(e) => {
            debug!("Tablespace creation failed (likely due to permissions): {e:?}");
            // Skip test if tablespace creation is not allowed
        }
    }

    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_tablespace_with_comment() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create tablespace and add comment
    let result = execute_sql(
        &connection,
        "CREATE TABLESPACE test_tablespace_comment LOCATION '/tmp/test_tablespace_comment';",
    )
    .await;

    match result {
        Ok(_) => {
            execute_sql(
                &connection,
                "COMMENT ON TABLESPACE test_tablespace_comment IS 'Test tablespace for introspection';",
            )
            .await?;

            // Introspect the database
            let schema = connection.introspect().await?;

            // Verify the tablespace was introspected with comment
            let tablespace = schema.tablespaces.get("test_tablespace_comment");
            assert!(
                tablespace.is_some(),
                "Tablespace 'test_tablespace_comment' should be introspected"
            );

            let tablespace_obj = tablespace.unwrap();
            assert_eq!(tablespace_obj.name, "test_tablespace_comment");
            assert_eq!(
                tablespace_obj.comment,
                Some("Test tablespace for introspection".to_string()),
                "Tablespace should have the specified comment"
            );

            // Clean up
            execute_sql(&connection, "DROP TABLESPACE test_tablespace_comment;").await?;
        }
        Err(e) => {
            debug!("Tablespace creation failed (likely due to permissions): {e:?}");
            // Skip test if tablespace creation is not allowed
        }
    }

    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_multiple_tablespaces() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create multiple tablespaces
    let result1 = execute_sql(
        &connection,
        "CREATE TABLESPACE tablespace1 LOCATION '/tmp/tablespace1';",
    )
    .await;
    let result2 = execute_sql(
        &connection,
        "CREATE TABLESPACE tablespace2 LOCATION '/tmp/tablespace2';",
    )
    .await;

    if result1.is_ok() && result2.is_ok() {
        // Introspect the database
        let schema = connection.introspect().await?;

        // Verify both tablespaces were introspected
        assert!(
            schema.tablespaces.contains_key("tablespace1"),
            "Tablespace 'tablespace1' should be introspected"
        );
        assert!(
            schema.tablespaces.contains_key("tablespace2"),
            "Tablespace 'tablespace2' should be introspected"
        );

        // Verify tablespace details
        let tablespace1 = schema.tablespaces.get("tablespace1").unwrap();
        let tablespace2 = schema.tablespaces.get("tablespace2").unwrap();

        assert_eq!(tablespace1.name, "tablespace1");
        assert_eq!(tablespace2.name, "tablespace2");

        // Clean up
        execute_sql(&connection, "DROP TABLESPACE tablespace1;").await?;
        execute_sql(&connection, "DROP TABLESPACE tablespace2;").await?;
    } else {
        debug!("Tablespace creation failed (likely due to permissions)");
        // Skip test if tablespace creation is not allowed
    }

    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_tablespace_with_options() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create tablespace with options
    let result = execute_sql(&connection, "CREATE TABLESPACE test_tablespace_options LOCATION '/tmp/test_tablespace_options' WITH (random_page_cost = 1.1, seq_page_cost = 1.0);").await;

    match result {
        Ok(_) => {
            // Introspect the database
            let schema = connection.introspect().await?;

            // Verify the tablespace was introspected
            let tablespace = schema.tablespaces.get("test_tablespace_options");
            assert!(
                tablespace.is_some(),
                "Tablespace 'test_tablespace_options' should be introspected"
            );

            let tablespace_obj = tablespace.unwrap();
            assert_eq!(tablespace_obj.name, "test_tablespace_options");
            // Note: Options are typically not introspected in the current implementation
            // but the tablespace itself should be present

            // Clean up
            execute_sql(&connection, "DROP TABLESPACE test_tablespace_options;").await?;
        }
        Err(e) => {
            debug!("Tablespace creation failed (likely due to permissions): {e:?}");
            // Skip test if tablespace creation is not allowed
        }
    }

    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_no_user_tablespaces() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Introspect the database without any user tablespaces
    let schema = connection.introspect().await?;

    // Verify no user tablespaces are present
    // Note: System tablespaces like 'pg_default', 'pg_global' should be filtered out
    let user_tablespaces: Vec<&String> = schema.tablespaces.keys().collect();
    assert!(
        user_tablespaces.is_empty(),
        "No user tablespaces should be introspected: {:?}",
        user_tablespaces
    );

    db.cleanup().await?;
    Ok(())
}
