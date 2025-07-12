use tracing::debug;
use postgres::TestDb;
use shem_core::{DatabaseConnection, schema::CollationProvider};

/// Test helper function to execute SQL on the test database
async fn execute_sql(
    connection: &Box<dyn DatabaseConnection>,
    sql: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    connection.execute(sql).await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_basic_collation() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create a basic collation
    execute_sql(
        &connection,
        "CREATE COLLATION test_basic_collation (locale = 'C');",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the collation was introspected
    let collation = schema.collations.get("test_basic_collation");
    debug!("Collation: {:?}", collation);
    assert!(
        collation.is_some(),
        "Collation 'test_basic_collation' should be introspected"
    );

    let coll = collation.unwrap();
    assert_eq!(coll.name, "test_basic_collation");
    assert_eq!(coll.locale, Some("C".to_string()));
    assert_eq!(coll.lc_collate, Some("C".to_string()));
    assert_eq!(coll.lc_ctype, Some("C".to_string()));
    assert_eq!(coll.provider, CollationProvider::Libc);
    assert!(coll.deterministic, "Collation should be deterministic");

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_collation_with_schema() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create schema and collation in that schema
    execute_sql(&connection, "CREATE SCHEMA test_collation_schema;").await?;
    execute_sql(
        &connection,
        "CREATE COLLATION test_collation_schema.schema_collation (locale = 'en_US.utf8');",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the collation was introspected with correct schema
    let collation = schema.collations.get("schema_collation");
    assert!(
        collation.is_some(),
        "Collation 'schema_collation' should be introspected"
    );

    let coll = collation.unwrap();
    assert_eq!(coll.name, "schema_collation");
    assert_eq!(
        coll.schema,
        Some("test_collation_schema".to_string()),
        "Collation should be in the specified schema"
    );
    assert_eq!(coll.locale, Some("en_US.utf8".to_string()));

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_collation_providers() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create collations with different providers
    execute_sql(
        &connection,
        "CREATE COLLATION test_libc_collation (locale = 'C', provider = libc);",
    )
    .await?;

    // Note: ICU collations require ICU extension to be available
    // We'll test what's available on the system
    let schema = connection.introspect().await?;

    // Verify the libc collation was introspected
    let libc_collation = schema.collations.get("test_libc_collation");
    assert!(
        libc_collation.is_some(),
        "Collation 'test_libc_collation' should be introspected"
    );

    let coll = libc_collation.unwrap();
    assert_eq!(coll.name, "test_libc_collation");
    assert_eq!(coll.provider, CollationProvider::Libc);
    assert_eq!(coll.locale, Some("C".to_string()));

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_collation_deterministic() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create collation with deterministic flag
    execute_sql(
        &connection,
        "CREATE COLLATION test_deterministic_collation (locale = 'C', deterministic = true);",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the collation was introspected with deterministic flag
    let collation = schema.collations.get("test_deterministic_collation");
    assert!(
        collation.is_some(),
        "Collation 'test_deterministic_collation' should be introspected"
    );

    let coll = collation.unwrap();
    assert_eq!(coll.name, "test_deterministic_collation");
    assert!(coll.deterministic, "Collation should be deterministic");

    // Clean up
    db.cleanup().await?;
    Ok(())
}

// #[tokio::test]
// async fn test_introspect_collation_non_deterministic() -> Result<(), Box<dyn std::error::Error>> {
//     env_logger::try_init().ok();
//     let db = TestDb::new().await?;
//     let connection = &db.conn;

//     // Create ICU collation with non-deterministic flag
//     execute_sql(
//         &connection,
//         "CREATE COLLATION test_non_deterministic_collation (
//             provider = icu,
//             locale = 'und-u-ks-level2',
//             deterministic = false
//         );",
//     )
//     .await?;

//     // Introspect the database
//     let schema = connection.introspect().await?;

//     // Verify the collation was introspected with non-deterministic flag
//     let collation = schema.collations.get("test_non_deterministic_collation");
//     assert!(
//         collation.is_some(),
//         "Collation 'test_non_deterministic_collation' should be introspected"
//     );

//     let coll = collation.unwrap();
//     assert_eq!(coll.name, "test_non_deterministic_collation");
//     //assert_eq!(coll.provider, CollationProvider::Icu);
//     assert_eq!(coll.locale, Some("und-u-ks-level2".to_string()));
//     assert!(!coll.deterministic, "Collation should be non-deterministic");

//     // Clean up
//     db.cleanup().await?;
//     Ok(())
// }

#[tokio::test]
async fn test_introspect_collation_separate_locales() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create collation with separate lc_collate and lc_ctype
    execute_sql(
        &connection,
        "CREATE COLLATION test_separate_collation (lc_collate = 'en_US.utf8', lc_ctype = 'en_US.utf8');",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the collation was introspected with separate locales
    let collation = schema.collations.get("test_separate_collation");
    assert!(
        collation.is_some(),
        "Collation 'test_separate_collation' should be introspected"
    );

    let coll = collation.unwrap();
    assert_eq!(coll.name, "test_separate_collation");
    assert_eq!(coll.lc_collate, Some("en_US.utf8".to_string()));
    assert_eq!(coll.lc_ctype, Some("en_US.utf8".to_string()));
    assert_eq!(coll.locale, Some("en_US.utf8".to_string())); // Should use lc_collate as primary

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_multiple_collations() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create multiple collations
    execute_sql(
        &connection,
        "CREATE COLLATION test_collation1 (locale = 'C');",
    )
    .await?;
    execute_sql(
        &connection,
        "CREATE COLLATION test_collation2 (locale = 'en_US.utf8');",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify both collations were introspected
    assert!(
        schema.collations.contains_key("test_collation1"),
        "Collation 'test_collation1' should be introspected"
    );
    assert!(
        schema.collations.contains_key("test_collation2"),
        "Collation 'test_collation2' should be introspected"
    );

    // Verify collation details
    let coll1 = schema.collations.get("test_collation1").unwrap();
    let coll2 = schema.collations.get("test_collation2").unwrap();

    assert_eq!(coll1.name, "test_collation1");
    assert_eq!(coll2.name, "test_collation2");
    assert_eq!(coll1.locale, Some("C".to_string()));
    assert_eq!(coll2.locale, Some("en_US.utf8".to_string()));

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_no_collations() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Introspect the database without any user collations
    let schema = connection.introspect().await?;

    // Verify no user collations are present
    // Note: System collations should be filtered out
    let user_collations: Vec<&String> = schema.collations.keys().collect();
    assert!(
        user_collations.is_empty(),
        "No user collations should be introspected: {:?}",
        user_collations
    );

    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_collation_edge_cases() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Test case 1: Collation with very long name
    let long_name = "a".repeat(50);
    execute_sql(
        &connection,
        &format!("CREATE COLLATION {} (locale = 'C');", long_name),
    )
    .await?;

    let schema = connection.introspect().await?;
    let collation = schema.collations.get(&long_name);
    assert!(
        collation.is_some(),
        "Collation with long name should be introspected"
    );

    // Test case 2: Collation with special characters in name
    execute_sql(
        &connection,
        "CREATE COLLATION \"test-collation-with-dashes\" (locale = 'C');",
    )
    .await?;

    let schema2 = connection.introspect().await?;
    let collation2 = schema2.collations.get("test-collation-with-dashes");
    assert!(
        collation2.is_some(),
        "Collation with special characters should be introspected"
    );

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_collation_performance() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create multiple collations
    for i in 1..=5 {
        execute_sql(
            &connection,
            &format!("CREATE COLLATION test_perf_collation_{} (locale = 'C');", i),
        )
        .await?;
    }

    // Measure introspection performance
    let start = std::time::Instant::now();
    let schema = connection.introspect().await?;
    let duration = start.elapsed();

    // Verify all collations were introspected
    for i in 1..=5 {
        assert!(
            schema
                .collations
                .contains_key(&format!("test_perf_collation_{}", i)),
            "Collation test_perf_collation_{} should be introspected",
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
async fn test_introspect_collation_consistency() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create a collation
    execute_sql(
        &connection,
        "CREATE COLLATION test_consistency_collation (locale = 'en_US.utf8');",
    )
    .await?;

    // Introspect multiple times to verify consistency
    let schema1 = connection.introspect().await?;
    let schema2 = connection.introspect().await?;

    let coll1 = schema1
        .collations
        .get("test_consistency_collation")
        .unwrap();
    let coll2 = schema2
        .collations
        .get("test_consistency_collation")
        .unwrap();

    // Verify consistency across multiple introspections
    assert_eq!(coll1.name, coll2.name);
    assert_eq!(coll1.schema, coll2.schema);
    assert_eq!(coll1.locale, coll2.locale);
    assert_eq!(coll1.lc_collate, coll2.lc_collate);
    assert_eq!(coll1.lc_ctype, coll2.lc_ctype);
    assert_eq!(coll1.provider, coll2.provider);
    assert_eq!(coll1.deterministic, coll2.deterministic);

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_collation_all_features() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create collation with all features
    execute_sql(
        &connection,
        "CREATE COLLATION test_all_features_collation (locale = 'en_US.utf8', provider = libc, deterministic = true);",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the collation was introspected with all features
    let collation = schema.collations.get("test_all_features_collation");
    assert!(
        collation.is_some(),
        "Collation 'test_all_features_collation' should be introspected"
    );

    let coll = collation.unwrap();
    assert_eq!(coll.name, "test_all_features_collation");
    assert_eq!(coll.locale, Some("en_US.utf8".to_string()));
    assert_eq!(coll.lc_collate, Some("en_US.utf8".to_string()));
    assert_eq!(coll.lc_ctype, Some("en_US.utf8".to_string()));
    assert_eq!(coll.provider, CollationProvider::Libc);
    assert!(coll.deterministic, "Collation should be deterministic");

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_collation_schema_consistency() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create schema and collation
    execute_sql(
        &connection,
        "CREATE SCHEMA test_collation_schema_consistency;",
    )
    .await?;
    execute_sql(
        &connection,
        "CREATE COLLATION test_collation_schema_consistency.schema_consistency_collation (locale = 'C');",
    )
    .await?;

    // Introspect multiple times to verify consistency
    let schema1 = connection.introspect().await?;
    let schema2 = connection.introspect().await?;

    let coll1 = schema1
        .collations
        .get("schema_consistency_collation")
        .unwrap();
    let coll2 = schema2
        .collations
        .get("schema_consistency_collation")
        .unwrap();

    // Verify consistency across multiple introspections
    assert_eq!(coll1.name, coll2.name);
    assert_eq!(coll1.schema, coll2.schema);
    assert_eq!(coll1.locale, coll2.locale);
    assert_eq!(coll1.lc_collate, coll2.lc_collate);
    assert_eq!(coll1.lc_ctype, coll2.lc_ctype);
    assert_eq!(coll1.provider, coll2.provider);
    assert_eq!(coll1.deterministic, coll2.deterministic);

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_collation_locale_fallback() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create collation with locale set to 'C'
    execute_sql(
        &connection,
        "CREATE COLLATION test_fallback_collation (locale = 'C');",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the collation was introspected
    let collation = schema.collations.get("test_fallback_collation");
    assert!(
        collation.is_some(),
        "Collation 'test_fallback_collation' should be introspected"
    );

    let coll = collation.unwrap();
    assert_eq!(coll.name, "test_fallback_collation");

    // Since 'locale = C' sets both lc_collate and lc_ctype to 'C',
    // check them explicitly and confirm your fallback logic
    assert_eq!(coll.lc_collate, Some("C".to_string()));
    assert_eq!(coll.lc_ctype, Some("C".to_string()));
    assert_eq!(
        coll.locale,
        Some("C".to_string()),
        "Locale should be set to 'C' based on lc_collate"
    );

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_user_defined_collation_libc() -> Result<(), Box<dyn std::error::Error>> {
    let db = TestDb::new().await?;
    let connection = &db.conn;

    execute_sql(
        &connection,
        "CREATE COLLATION test_libc_collation (locale = 'en_US.utf8', provider = libc);",
    )
    .await?;

    let schema = connection.introspect().await?;
    let collation = schema.collations.get("test_libc_collation");
    assert!(
        collation.is_some(),
        "User-defined collation should be introspected"
    );

    let coll = collation.unwrap();
    assert_eq!(coll.name, "test_libc_collation");
    assert_eq!(coll.provider, CollationProvider::Libc);
    assert_eq!(coll.locale.as_deref(), Some("en_US.utf8"));

    db.cleanup().await?;
    Ok(())
}
