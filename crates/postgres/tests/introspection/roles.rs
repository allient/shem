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

// #[tokio::test]
// async fn test_introspect_no_user_roles() -> Result<(), Box<dyn std::error::Error>> {
//     env_logger::try_init().ok();
//     let db = TestDb::new().await?;
//     let connection = &db.conn;

//     // Introspect the database without any user roles
//     let schema = connection.introspect().await?;

//     // Verify no user roles are present
//     // Note: System roles like 'postgres', 'pg_signal_backend' should be filtered out
//     let user_roles: Vec<&String> = schema.roles.keys().collect();
//     debug!("User roles: {:?}", user_roles);
//     assert!(
//         user_roles.is_empty(),
//         "No user roles should be introspected: {:?}",
//         user_roles
//     );

//     db.cleanup().await?;
//     Ok(())
// }

#[tokio::test]
async fn test_introspect_basic_role() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create a basic role
    let result = execute_sql(&connection, "CREATE ROLE test_basic_role;").await;

    match result {
        Ok(_) => {
            // Introspect the database
            let schema = connection.introspect().await?;

            // Verify the role was introspected
            let role = schema.roles.get("test_basic_role");
            debug!("Role: {:?}", role);
            assert!(
                role.is_some(),
                "Role 'test_basic_role' should be introspected"
            );

            let role_obj = role.unwrap();
            assert_eq!(role_obj.name, "test_basic_role");
            assert_eq!(role_obj.superuser, false, "Role should not be superuser");
            assert_eq!(role_obj.createdb, false, "Role should not have createdb");
            assert_eq!(
                role_obj.createrole, false,
                "Role should not have createrole"
            );
            assert_eq!(role_obj.inherit, true, "Role should inherit by default");
            assert_eq!(
                role_obj.login, false,
                "Role should not have login by default"
            );
            assert_eq!(
                role_obj.replication, false,
                "Role should not have replication"
            );
            assert_eq!(
                role_obj.connection_limit, None,
                "Role should not have connection limit"
            );
            assert_eq!(role_obj.password, None, "Role should not have password");
            assert_eq!(
                role_obj.valid_until, None,
                "Role should not have expiration"
            );
            assert!(
                role_obj.member_of.is_empty(),
                "Role should not be member of any groups"
            );

            // Clean up
            execute_sql(&connection, "DROP ROLE test_basic_role;").await?;
        }
        Err(e) => {
            debug!("Role creation failed: {e:?}");
            // Skip test if role creation is not allowed
        }
    }

    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_role_with_privileges() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create role with various privileges
    let result = execute_sql(
        &connection,
        "CREATE ROLE test_privileged_role LOGIN CREATEDB CREATEROLE;",
    )
    .await;

    match result {
        Ok(_) => {
            // Introspect the database
            let schema = connection.introspect().await?;

            // Verify the role was introspected with privileges
            let role = schema.roles.get("test_privileged_role");
            assert!(
                role.is_some(),
                "Role 'test_privileged_role' should be introspected"
            );

            let role_obj = role.unwrap();
            assert_eq!(role_obj.name, "test_privileged_role");
            assert_eq!(role_obj.superuser, false, "Role should not be superuser");
            assert_eq!(
                role_obj.createdb, true,
                "Role should have createdb privilege"
            );
            assert_eq!(
                role_obj.createrole, true,
                "Role should have createrole privilege"
            );
            assert_eq!(role_obj.inherit, true, "Role should inherit by default");
            assert_eq!(role_obj.login, true, "Role should have login privilege");
            assert_eq!(
                role_obj.replication, false,
                "Role should not have replication"
            );

            // Clean up
            execute_sql(&connection, "DROP ROLE test_privileged_role;").await?;
        }
        Err(e) => {
            debug!("Role creation failed: {e:?}");
            // Skip test if role creation is not allowed
        }
    }

    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_role_with_connection_limit() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create role with connection limit
    let result = execute_sql(
        &connection,
        "CREATE ROLE test_connection_role LOGIN CONNECTION LIMIT 5;",
    )
    .await;

    match result {
        Ok(_) => {
            // Introspect the database
            let schema = connection.introspect().await?;

            // Verify the role was introspected with connection limit
            let role = schema.roles.get("test_connection_role");
            assert!(
                role.is_some(),
                "Role 'test_connection_role' should be introspected"
            );

            let role_obj = role.unwrap();
            assert_eq!(role_obj.name, "test_connection_role");
            assert_eq!(role_obj.login, true, "Role should have login privilege");
            assert_eq!(
                role_obj.connection_limit,
                Some(5),
                "Role should have connection limit of 5"
            );

            // Clean up
            execute_sql(&connection, "DROP ROLE test_connection_role;").await?;
        }
        Err(e) => {
            debug!("Role creation failed: {e:?}");
            // Skip test if role creation is not allowed
        }
    }

    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_role_with_expiration() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create role with expiration date
    let result = execute_sql(
        &connection,
        "CREATE ROLE test_expiring_role LOGIN VALID UNTIL '2025-12-31';",
    )
    .await;

    match result {
        Ok(_) => {
            // Introspect the database
            let schema = connection.introspect().await?;

            // Verify the role was introspected with expiration
            let role = schema.roles.get("test_expiring_role");
            assert!(
                role.is_some(),
                "Role 'test_expiring_role' should be introspected"
            );

            let role_obj = role.unwrap();
            assert_eq!(role_obj.name, "test_expiring_role");
            assert_eq!(role_obj.login, true, "Role should have login privilege");
            assert!(
                role_obj.valid_until.is_some(),
                "Role should have expiration date"
            );
            assert!(
                role_obj
                    .valid_until
                    .as_ref()
                    .unwrap()
                    .contains("2025-12-31"),
                "Role should have correct expiration date"
            );

            // Clean up
            execute_sql(&connection, "DROP ROLE test_expiring_role;").await?;
        }
        Err(e) => {
            debug!("Role creation failed: {e:?}");
            // Skip test if role creation is not allowed
        }
    }

    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_role_with_group_membership() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create group role and member role
    let result1 = execute_sql(&connection, "CREATE ROLE test_group_role;").await;
    let result2 = execute_sql(&connection, "CREATE ROLE test_member_role;").await;

    if result1.is_ok() && result2.is_ok() {
        // Add member to group
        execute_sql(&connection, "GRANT test_group_role TO test_member_role;").await?;

        // Introspect the database
        let schema = connection.introspect().await?;

        // Verify the member role was introspected with group membership
        let role = schema.roles.get("test_member_role");
        assert!(
            role.is_some(),
            "Role 'test_member_role' should be introspected"
        );

        let role_obj = role.unwrap();
        assert_eq!(role_obj.name, "test_member_role");
        assert!(
            role_obj.member_of.contains(&"test_group_role".to_string()),
            "Role should be member of test_group_role"
        );

        // Verify the group role was also introspected
        let group_role = schema.roles.get("test_group_role");
        assert!(
            group_role.is_some(),
            "Group role 'test_group_role' should be introspected"
        );

        // Clean up
        execute_sql(&connection, "DROP ROLE test_member_role;").await?;
        execute_sql(&connection, "DROP ROLE test_group_role;").await?;
    } else {
        debug!("Role creation failed (likely due to permissions)");
        // Skip test if role creation is not allowed
    }

    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_multiple_roles() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create multiple roles
    let result1 = execute_sql(&connection, "CREATE ROLE test_role1;").await;
    let result2 = execute_sql(&connection, "CREATE ROLE test_role2 LOGIN;").await;
    let result3 = execute_sql(&connection, "CREATE ROLE test_role3 CREATEDB;").await;

    if result1.is_ok() && result2.is_ok() && result3.is_ok() {
        // Introspect the database
        let schema = connection.introspect().await?;

        // Verify all roles were introspected
        assert!(
            schema.roles.contains_key("test_role1"),
            "Role 'test_role1' should be introspected"
        );
        assert!(
            schema.roles.contains_key("test_role2"),
            "Role 'test_role2' should be introspected"
        );
        assert!(
            schema.roles.contains_key("test_role3"),
            "Role 'test_role3' should be introspected"
        );

        // Verify role details
        let role1 = schema.roles.get("test_role1").unwrap();
        let role2 = schema.roles.get("test_role2").unwrap();
        let role3 = schema.roles.get("test_role3").unwrap();

        assert_eq!(role1.name, "test_role1");
        assert_eq!(role1.login, false, "Role1 should not have login");

        assert_eq!(role2.name, "test_role2");
        assert_eq!(role2.login, true, "Role2 should have login");

        assert_eq!(role3.name, "test_role3");
        assert_eq!(role3.createdb, true, "Role3 should have createdb");

        // Clean up
        execute_sql(&connection, "DROP ROLE test_role1;").await?;
        execute_sql(&connection, "DROP ROLE test_role2;").await?;
        execute_sql(&connection, "DROP ROLE test_role3;").await?;
    } else {
        debug!("Role creation failed (likely due to permissions)");
        // Skip test if role creation is not allowed
    }

    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_role_edge_cases() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Test case 1: Role with very long name
    let long_name = "a".repeat(63); // PostgreSQL role name limit is 63 characters
    let result = execute_sql(&connection, &format!("CREATE ROLE {};", long_name)).await;

    match result {
        Ok(_) => {
            let schema = connection.introspect().await?;
            let role = schema.roles.get(&long_name);
            assert!(role.is_some(), "Role with long name should be introspected");

            // Clean up
            execute_sql(&connection, &format!("DROP ROLE {};", long_name)).await?;
        }
        Err(e) => {
            debug!("Long role name creation failed: {e:?}");
        }
    }

    // Test case 2: Role with special characters in name (if supported)
    // Most PostgreSQL role names don't have special characters, but we can test the robustness

    // Test case 3: Role with all privileges
    let result = execute_sql(
        &connection,
        "CREATE ROLE test_super_role LOGIN SUPERUSER CREATEDB CREATEROLE REPLICATION;",
    )
    .await;

    match result {
        Ok(_) => {
            let schema = connection.introspect().await?;
            let role = schema.roles.get("test_super_role");
            assert!(role.is_some(), "Super role should be introspected");

            let role_obj = role.unwrap();
            assert_eq!(role_obj.superuser, true, "Role should be superuser");
            assert_eq!(role_obj.createdb, true, "Role should have createdb");
            assert_eq!(role_obj.createrole, true, "Role should have createrole");
            assert_eq!(role_obj.replication, true, "Role should have replication");

            // Clean up
            execute_sql(&connection, "DROP ROLE test_super_role;").await?;
        }
        Err(e) => {
            debug!("Super role creation failed: {e:?}");
        }
    }

    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_role_performance() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create multiple roles
    let result1 = execute_sql(&connection, "CREATE ROLE test_perf_role1;").await;
    let result2 = execute_sql(&connection, "CREATE ROLE test_perf_role2;").await;
    let result3 = execute_sql(&connection, "CREATE ROLE test_perf_role3;").await;

    if result1.is_ok() && result2.is_ok() && result3.is_ok() {
        // Measure introspection performance
        let start = std::time::Instant::now();
        let schema = connection.introspect().await?;
        let duration = start.elapsed();

        // Verify all roles were introspected
        assert!(schema.roles.contains_key("test_perf_role1"));
        assert!(schema.roles.contains_key("test_perf_role2"));
        assert!(schema.roles.contains_key("test_perf_role3"));

        // Performance assertion (adjust threshold as needed)
        assert!(
            duration.as_millis() < 1000,
            "Introspection should complete within 1 second"
        );

        // Clean up
        execute_sql(&connection, "DROP ROLE test_perf_role1;").await?;
        execute_sql(&connection, "DROP ROLE test_perf_role2;").await?;
        execute_sql(&connection, "DROP ROLE test_perf_role3;").await?;
    } else {
        debug!("Role creation failed (likely due to permissions)");
        // Skip test if role creation is not allowed
    }

    db.cleanup().await?;
    Ok(())
}
