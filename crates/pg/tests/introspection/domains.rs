use tracing::debug;
use postgres::TestDb;
use shem_core::{DatabaseConnection, schema::{Type, Domain}};

/// Test helper function to execute SQL on the test database
async fn execute_sql(
    connection: &Box<dyn DatabaseConnection>,
    sql: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    connection.execute(sql).await?;
    Ok(())
}

/// Helper function to get a domain type from the unified types map
fn get_domain_type<'a>(schema: &'a shem_core::Schema, name: &str) -> Option<&'a Domain> {
    schema.types.get(name).and_then(|t| {
        if let Type::Domain(dt) = t {
            Some(dt)
        } else {
            None
        }
    })
}

#[tokio::test]
async fn test_introspect_basic_domain() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create a basic domain
    execute_sql(
        &connection,
        "CREATE DOMAIN test_basic_domain AS INTEGER;",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the domain was introspected
    let domain = get_domain_type(&schema, "test_basic_domain");
    debug!("Domain: {:?}", domain);
    assert!(
        domain.is_some(),
        "Domain 'test_basic_domain' should be introspected"
    );

    let dt = domain.unwrap();
    assert_eq!(dt.info.name, "test_basic_domain");
    assert_eq!(dt.info.schema, "public", "Domain should be in public schema");
    assert_eq!(dt.base_type, "integer");
    assert!(!dt.not_null, "Domain should not be NOT NULL by default");
    assert_eq!(dt.default, None, "Domain should not have a default value");
    assert!(dt.constraints.is_empty(), "Domain should not have constraints");

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_domain_with_schema() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create schema and domain in that schema
    execute_sql(&connection, "CREATE SCHEMA test_domains;").await?;
    execute_sql(
        &connection,
        "CREATE DOMAIN test_domains.schema_domain AS TEXT;",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the domain was introspected with correct schema
    let domain = get_domain_type(&schema, "schema_domain");
    assert!(
        domain.is_some(),
        "Domain 'schema_domain' should be introspected"
    );

    let dt = domain.unwrap();
    assert_eq!(dt.info.name, "schema_domain");
    assert_eq!(
        dt.info.schema,
        "test_domains",
        "Domain should be in the specified schema"
    );

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_domain_with_comment() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create domain and add comment
    execute_sql(
        &connection,
        "CREATE DOMAIN test_comment_domain AS VARCHAR(50);",
    )
    .await?;
    execute_sql(
        &connection,
        "COMMENT ON DOMAIN test_comment_domain IS 'Domain for storing names';",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the domain was introspected with comment
    let domain = get_domain_type(&schema, "test_comment_domain");
    assert!(
        domain.is_some(),
        "Domain 'test_comment_domain' should be introspected"
    );

    let dt = domain.unwrap();
    assert_eq!(dt.info.name, "test_comment_domain");
    assert_eq!(
        dt.info.comment,
        Some("Domain for storing names".to_string()),
        "Domain should have the specified comment"
    );

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_domain_with_constraint() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create domain with constraint
    execute_sql(
        &connection,
        "CREATE DOMAIN test_constraint_domain AS INTEGER CHECK (VALUE > 0);",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the domain was introspected with constraint
    let domain = get_domain_type(&schema, "test_constraint_domain");
    assert!(
        domain.is_some(),
        "Domain 'test_constraint_domain' should be introspected"
    );

    let dt = domain.unwrap();
    assert_eq!(dt.info.name, "test_constraint_domain");
    assert_eq!(dt.base_type, "integer");
    assert!(!dt.constraints.is_empty(), "Domain should have constraints");
    assert_eq!(dt.constraints.len(), 1, "Domain should have 1 constraint");
    assert!(dt.constraints[0].definition.contains("CHECK"), "Constraint should be a CHECK constraint");

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_domain_with_named_constraint() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create domain with named constraint
    execute_sql(
        &connection,
        "CREATE DOMAIN test_named_constraint_domain AS INTEGER CONSTRAINT positive_check CHECK (VALUE > 0);",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the domain was introspected with named constraint
    let domain = get_domain_type(&schema, "test_named_constraint_domain");
    assert!(
        domain.is_some(),
        "Domain 'test_named_constraint_domain' should be introspected"
    );

    let dt = domain.unwrap();
    assert_eq!(dt.info.name, "test_named_constraint_domain");
    assert!(!dt.constraints.is_empty(), "Domain should have constraints");
    assert_eq!(dt.constraints[0].name, "positive_check", "Constraint should have the specified name");

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_domain_with_default() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create domain with default value
    execute_sql(
        &connection,
        "CREATE DOMAIN test_default_domain AS INTEGER DEFAULT 42;",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the domain was introspected with default
    let domain = get_domain_type(&schema, "test_default_domain");
    assert!(
        domain.is_some(),
        "Domain 'test_default_domain' should be introspected"
    );

    let dt = domain.unwrap();
    assert_eq!(dt.info.name, "test_default_domain");
    assert_eq!(dt.default, Some("42".to_string()), "Domain should have the specified default value");

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_domain_not_null() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create domain with NOT NULL constraint
    execute_sql(
        &connection,
        "CREATE DOMAIN test_not_null_domain AS TEXT NOT NULL;",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the domain was introspected with NOT NULL
    let domain = get_domain_type(&schema, "test_not_null_domain");
    assert!(
        domain.is_some(),
        "Domain 'test_not_null_domain' should be introspected"
    );

    let dt = domain.unwrap();
    assert_eq!(dt.info.name, "test_not_null_domain");
    assert!(dt.not_null, "Domain should be NOT NULL");

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_domain_complex() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create domain with multiple features
    execute_sql(
        &connection,
        "CREATE DOMAIN test_complex_domain AS INTEGER NOT NULL DEFAULT 100 CHECK (VALUE BETWEEN 1 AND 1000);",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the domain was introspected with all features
    let domain = get_domain_type(&schema, "test_complex_domain");
    assert!(
        domain.is_some(),
        "Domain 'test_complex_domain' should be introspected"
    );

    let dt = domain.unwrap();
    assert_eq!(dt.info.name, "test_complex_domain");
    assert_eq!(dt.base_type, "integer");
    assert!(dt.not_null, "Domain should be NOT NULL");
    assert_eq!(dt.default, Some("100".to_string()), "Domain should have default value");
    assert!(!dt.constraints.is_empty(), "Domain should have constraints");

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_multiple_domains() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create multiple domains
    execute_sql(
        &connection,
        "CREATE DOMAIN test_domain1 AS INTEGER CHECK (VALUE > 0);",
    )
    .await?;
    execute_sql(
        &connection,
        "CREATE DOMAIN test_domain2 AS TEXT NOT NULL;",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify both domains were introspected
    assert!(
        get_domain_type(&schema, "test_domain1").is_some(),
        "Domain 'test_domain1' should be introspected"
    );
    assert!(
        get_domain_type(&schema, "test_domain2").is_some(),
        "Domain 'test_domain2' should be introspected"
    );

    // Verify domain details
    let dom1 = get_domain_type(&schema, "test_domain1").unwrap();
    let dom2 = get_domain_type(&schema, "test_domain2").unwrap();

    assert_eq!(dom1.info.name, "test_domain1");
    assert_eq!(dom2.info.name, "test_domain2");
    assert_eq!(dom1.info.schema, "public");
    assert_eq!(dom2.info.schema, "public");

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_no_domains() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Introspect the database without any user domains
    let schema = connection.introspect().await?;

    // Verify no user domains are present
    // Note: System domains should be filtered out
    let user_domains: Vec<&String> = schema.types
        .iter()
        .filter_map(|(name, t)| {
            if let Type::Domain(_) = t {
                Some(name)
            } else {
                None
            }
        })
        .collect();
    assert!(
        user_domains.is_empty(),
        "No user domains should be introspected: {:?}",
        user_domains
    );

    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_domain_edge_cases() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Test case 1: Domain with very long name
    let long_name = "a".repeat(50);
    execute_sql(
        &connection,
        &format!("CREATE DOMAIN {} AS INTEGER;", long_name),
    )
    .await?;

    let schema = connection.introspect().await?;
    let domain = get_domain_type(&schema, &long_name);
    assert!(
        domain.is_some(),
        "Domain with long name should be introspected"
    );

    // Test case 2: Domain with special characters in name
    execute_sql(
        &connection,
        "CREATE DOMAIN \"test-domain-with-dashes\" AS TEXT;",
    )
    .await?;

    let schema2 = connection.introspect().await?;
    let domain2 = get_domain_type(&schema2, "test-domain-with-dashes");
    assert!(
        domain2.is_some(),
        "Domain with special characters should be introspected"
    );

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_domain_performance() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create multiple domains
    for i in 1..=10 {
        execute_sql(
            &connection,
            &format!("CREATE DOMAIN test_perf_domain_{} AS INTEGER CHECK (VALUE > 0);", i),
        )
        .await?;
    }

    // Measure introspection performance
    let start = std::time::Instant::now();
    let schema = connection.introspect().await?;
    let duration = start.elapsed();

    // Verify all domains were introspected
    for i in 1..=10 {
        assert!(
            get_domain_type(&schema, &format!("test_perf_domain_{}", i)).is_some(),
            "Domain test_perf_domain_{} should be introspected",
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
async fn test_introspect_domain_consistency() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create a domain
    execute_sql(
        &connection,
        "CREATE DOMAIN test_consistency_domain AS INTEGER DEFAULT 42 CHECK (VALUE > 0);",
    )
    .await?;

    // Introspect multiple times to verify consistency
    let schema1 = connection.introspect().await?;
    let schema2 = connection.introspect().await?;

    let dom1 = get_domain_type(&schema1, "test_consistency_domain").unwrap();
    let dom2 = get_domain_type(&schema2, "test_consistency_domain").unwrap();

    // Verify consistency across multiple introspections
    assert_eq!(dom1.info.name, dom2.info.name);
    assert_eq!(dom1.info.schema, dom2.info.schema);
    assert_eq!(dom1.base_type, dom2.base_type);
    assert_eq!(dom1.not_null, dom2.not_null);
    assert_eq!(dom1.default, dom2.default);
    assert_eq!(dom1.constraints.len(), dom2.constraints.len());
    assert_eq!(dom1.info.comment, dom2.info.comment);

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_domain_all_features() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create a domain with all possible features
    execute_sql(
        &connection,
        "CREATE DOMAIN test_all_features_domain AS VARCHAR(100) NOT NULL DEFAULT 'default_value' CHECK (LENGTH(VALUE) > 0) CHECK (VALUE ~ '^[A-Za-z0-9_]+$');",
    )
    .await?;

    // Add comment
    execute_sql(
        &connection,
        "COMMENT ON DOMAIN test_all_features_domain IS 'Domain with all features';",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the domain was introspected with all features
    let domain = get_domain_type(&schema, "test_all_features_domain");
    assert!(
        domain.is_some(),
        "Domain 'test_all_features_domain' should be introspected"
    );

    let dt = domain.unwrap();
    assert_eq!(dt.info.name, "test_all_features_domain");
    assert_eq!(dt.base_type, "character varying(100)");
    assert!(dt.not_null, "Domain should be NOT NULL");
    assert_eq!(dt.default, Some("'default_value'::character varying".to_string()), "Domain should have default value");
    assert_eq!(dt.constraints.len(), 2, "Domain should have 2 constraints");
    assert_eq!(
        dt.info.comment,
        Some("Domain with all features".to_string()),
        "Domain should have comment"
    );

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_domain_multiple_constraints() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create a domain with multiple constraints
    execute_sql(
        &connection,
        "CREATE DOMAIN test_multiple_constraints_domain AS INTEGER CHECK (VALUE > 0) CHECK (VALUE < 1000) CHECK (VALUE % 2 = 0);",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the domain was introspected with multiple constraints
    let domain = get_domain_type(&schema, "test_multiple_constraints_domain");
    assert!(
        domain.is_some(),
        "Domain 'test_multiple_constraints_domain' should be introspected"
    );

    let dt = domain.unwrap();
    assert_eq!(dt.info.name, "test_multiple_constraints_domain");
    assert_eq!(dt.constraints.len(), 3, "Domain should have 3 constraints");

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_domain_schema_consistency() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create schema and domain
    execute_sql(&connection, "CREATE SCHEMA test_domain_schema;").await?;
    execute_sql(
        &connection,
        "CREATE DOMAIN test_domain_schema.schema_consistency_domain AS INTEGER DEFAULT 42;",
    )
    .await?;

    // Introspect multiple times
    let schema1 = connection.introspect().await?;
    let schema2 = connection.introspect().await?;

    let dom1 = get_domain_type(&schema1, "schema_consistency_domain").unwrap();
    let dom2 = get_domain_type(&schema2, "schema_consistency_domain").unwrap();

    // Verify schema consistency
    assert_eq!(dom1.info.schema, "test_domain_schema");
    assert_eq!(dom2.info.schema, "test_domain_schema");
    assert_eq!(dom1.info.schema, dom2.info.schema);

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_domain_collation() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create a domain with collation
    execute_sql(
        &connection,
        "CREATE DOMAIN test_collation_domain AS TEXT COLLATE \"C\";",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the domain was introspected with collation
    let domain = get_domain_type(&schema, "test_collation_domain");
    assert!(
        domain.is_some(),
        "Domain 'test_collation_domain' should be introspected"
    );

    let dt = domain.unwrap();
    assert_eq!(dt.info.name, "test_collation_domain");
    assert_eq!(dt.collation, Some("\"C\"".to_string()), "Domain should have collation");

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_domain_complex_base_type() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create a domain with complex base type
    execute_sql(
        &connection,
        "CREATE DOMAIN test_complex_base_domain AS NUMERIC(10,2) CHECK (VALUE >= 0);",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the domain was introspected with complex base type
    let domain = get_domain_type(&schema, "test_complex_base_domain");
    assert!(
        domain.is_some(),
        "Domain 'test_complex_base_domain' should be introspected"
    );

    let dt = domain.unwrap();
    assert_eq!(dt.info.name, "test_complex_base_domain");
    assert_eq!(dt.base_type, "numeric(10,2)", "Domain should have correct base type");

    // Clean up
    db.cleanup().await?;
    Ok(())
}
