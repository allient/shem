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
async fn test_introspect_basic_domain() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create a basic domain
    execute_sql(&connection, "CREATE DOMAIN test_basic_domain AS integer;").await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the domain was introspected
    let domain = schema.domains.get("test_basic_domain");
    debug!("Domain: {:?}", domain);
    assert!(
        domain.is_some(),
        "Domain 'test_basic_domain' should be introspected"
    );

    let dom = domain.unwrap();
    assert_eq!(dom.name, "test_basic_domain");
    assert_eq!(dom.base_type, "integer");
    assert!(
        dom.constraints.is_empty(),
        "Domain should have no constraints initially"
    );
    assert!(
        dom.default.is_none(),
        "Domain should have no default initially"
    );
    assert!(!dom.not_null, "Domain should not be NOT NULL initially");
    assert!(
        dom.comment.is_none(),
        "Domain should not have comment initially"
    );

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
    execute_sql(&connection, "CREATE SCHEMA test_domain_schema;").await?;
    execute_sql(
        &connection,
        "CREATE DOMAIN test_domain_schema.schema_domain AS text;",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the domain was introspected with correct schema
    let domain = schema.domains.get("schema_domain");
    assert!(
        domain.is_some(),
        "Domain 'schema_domain' should be introspected"
    );

    let dom = domain.unwrap();
    assert_eq!(dom.name, "schema_domain");
    assert_eq!(
        dom.schema,
        Some("test_domain_schema".to_string()),
        "Domain should be in the specified schema"
    );
    assert_eq!(dom.base_type, "text");

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
        "CREATE DOMAIN test_comment_domain AS timestamp;",
    )
    .await?;
    execute_sql(
        &connection,
        "COMMENT ON DOMAIN test_comment_domain IS 'Timestamp domain with comment';",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the domain was introspected with comment
    let domain = schema.domains.get("test_comment_domain");
    assert!(
        domain.is_some(),
        "Domain 'test_comment_domain' should be introspected"
    );

    let dom = domain.unwrap();
    assert_eq!(dom.name, "test_comment_domain");
    assert_eq!(
        dom.comment,
        Some("Timestamp domain with comment".to_string()),
        "Domain should have the specified comment"
    );

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_domain_with_constraints() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create domain with constraints
    execute_sql(
        &connection,
        "CREATE DOMAIN test_constraint_domain AS integer CHECK (VALUE > 0);",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the domain was introspected with constraints
    let domain = schema.domains.get("test_constraint_domain");
    assert!(
        domain.is_some(),
        "Domain 'test_constraint_domain' should be introspected"
    );

    let dom = domain.unwrap();
    assert_eq!(dom.name, "test_constraint_domain");
    assert_eq!(dom.base_type, "integer");
    assert_eq!(
        dom.constraints.len(),
        1,
        "Domain should have one constraint"
    );

    let constraint = &dom.constraints[0];
    debug!("Constraint: {:?}", constraint);
    assert_eq!(constraint.name, Some("test_constraint_domain_check".to_string()));
    assert_eq!(constraint.check, "CHECK ((VALUE > 0))");
    assert!(!constraint.not_valid, "Constraint should be valid");

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_domain_with_named_constraints() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create domain with named constraints
    execute_sql(
        &connection,
        "CREATE DOMAIN test_named_constraint_domain AS text CONSTRAINT test_named_constraint CHECK (LENGTH(VALUE) > 0);",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the domain was introspected with named constraints
    let domain = schema.domains.get("test_named_constraint_domain");
    assert!(
        domain.is_some(),
        "Domain 'test_named_constraint_domain' should be introspected"
    );

    let dom = domain.unwrap();
    assert_eq!(dom.name, "test_named_constraint_domain");
    assert_eq!(dom.base_type, "text");
    assert_eq!(
        dom.constraints.len(),
        1,
        "Domain should have one constraint"
    );

    let constraint = &dom.constraints[0];
    assert_eq!(constraint.name, Some("test_named_constraint".to_string()));
    assert_eq!(constraint.check, "CHECK ((length(VALUE) > 0))");
    assert!(!constraint.not_valid, "Constraint should be valid");

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
        "CREATE DOMAIN test_default_domain AS integer DEFAULT 42;",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;
    // Verify the domain was introspected with default
    let domain = schema.domains.get("test_default_domain");
    assert!(
        domain.is_some(),
        "Domain 'test_default_domain' should be introspected"
    );

    let dom = domain.unwrap();
    assert_eq!(dom.name, "test_default_domain");
    assert_eq!(dom.base_type, "integer");
    assert_eq!(
        dom.default,
        Some("42".to_string()),
        "Domain should have the specified default"
    );

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
        "CREATE DOMAIN test_not_null_domain AS text NOT NULL;",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the domain was introspected with NOT NULL
    let domain = schema.domains.get("test_not_null_domain");
    assert!(
        domain.is_some(),
        "Domain 'test_not_null_domain' should be introspected"
    );

    let dom = domain.unwrap();
    assert_eq!(dom.name, "test_not_null_domain");
    assert_eq!(dom.base_type, "text");
    assert!(dom.not_null, "Domain should be NOT NULL");

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_domain_complex_type() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create domain with complex base type
    execute_sql(
        &connection,
        "CREATE DOMAIN test_complex_domain AS numeric(10,2);",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the domain was introspected with complex base type
    let domain = schema.domains.get("test_complex_domain");
    assert!(
        domain.is_some(),
        "Domain 'test_complex_domain' should be introspected"
    );

    let dom = domain.unwrap();
    assert_eq!(dom.name, "test_complex_domain");
    assert_eq!(dom.base_type, "numeric(10,2)");

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
    execute_sql(&connection, "CREATE DOMAIN test_domain1 AS integer;").await?;
    execute_sql(&connection, "CREATE DOMAIN test_domain2 AS text;").await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify both domains were introspected
    assert!(
        schema.domains.contains_key("test_domain1"),
        "Domain 'test_domain1' should be introspected"
    );
    assert!(
        schema.domains.contains_key("test_domain2"),
        "Domain 'test_domain2' should be introspected"
    );

    // Verify domain details
    let dom1 = schema.domains.get("test_domain1").unwrap();
    let dom2 = schema.domains.get("test_domain2").unwrap();

    assert_eq!(dom1.name, "test_domain1");
    assert_eq!(dom2.name, "test_domain2");
    assert_eq!(dom1.base_type, "integer");
    assert_eq!(dom2.base_type, "text");

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
    let user_domains: Vec<&String> = schema.domains.keys().collect();
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
        &format!("CREATE DOMAIN {} AS integer;", long_name),
    )
    .await?;

    let schema = connection.introspect().await?;
    let domain = schema.domains.get(&long_name);
    assert!(
        domain.is_some(),
        "Domain with long name should be introspected"
    );

    // Test case 2: Domain with special characters in name
    execute_sql(
        &connection,
        "CREATE DOMAIN \"test-domain-with-dashes\" AS text;",
    )
    .await?;

    let schema2 = connection.introspect().await?;
    let domain2 = schema2.domains.get("test-domain-with-dashes");
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
            &format!("CREATE DOMAIN test_perf_domain_{} AS integer;", i),
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
            schema
                .domains
                .contains_key(&format!("test_perf_domain_{}", i)),
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
        "CREATE DOMAIN test_consistency_domain AS timestamp with time zone;",
    )
    .await?;

    // Introspect multiple times to verify consistency
    let schema1 = connection.introspect().await?;
    let schema2 = connection.introspect().await?;

    let dom1 = schema1.domains.get("test_consistency_domain").unwrap();
    let dom2 = schema2.domains.get("test_consistency_domain").unwrap();

    // Verify consistency across multiple introspections
    assert_eq!(dom1.name, dom2.name);
    assert_eq!(dom1.schema, dom2.schema);
    assert_eq!(dom1.base_type, dom2.base_type);
    assert_eq!(dom1.constraints, dom2.constraints);
    assert_eq!(dom1.default, dom2.default);
    assert_eq!(dom1.not_null, dom2.not_null);
    assert_eq!(dom1.comment, dom2.comment);

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_domain_all_features() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create domain with all features
    execute_sql(
        &connection,
        "CREATE DOMAIN test_all_features_domain AS numeric(8,2) NOT NULL DEFAULT 0.00 CONSTRAINT test_all_features_check CHECK (VALUE >= 0.00);",
    )
    .await?;
    execute_sql(
        &connection,
        "COMMENT ON DOMAIN test_all_features_domain IS 'Domain with all features';",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the domain was introspected with all features
    let domain = schema.domains.get("test_all_features_domain");
    assert!(
        domain.is_some(),
        "Domain 'test_all_features_domain' should be introspected"
    );

    let dom = domain.unwrap();
    assert_eq!(dom.name, "test_all_features_domain");
    assert_eq!(dom.base_type, "numeric(8,2)");
    assert_eq!(dom.default, Some("0.00".to_string()));
    assert!(dom.not_null, "Domain should be NOT NULL");
    assert_eq!(
        dom.comment,
        Some("Domain with all features".to_string()),
        "Domain should have the specified comment"
    );
    assert_eq!(
        dom.constraints.len(),
        1,
        "Domain should have one constraint"
    );

    let constraint = &dom.constraints[0];
    assert_eq!(constraint.name, Some("test_all_features_check".to_string()));
    assert_eq!(constraint.check, "CHECK ((VALUE >= 0.00))");
    assert!(!constraint.not_valid, "Constraint should be valid");

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_domain_multiple_constraints() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create domain with multiple constraints
    execute_sql(
        &connection,
        "CREATE DOMAIN test_multiple_constraints_domain AS integer CHECK (VALUE > 0) CHECK (VALUE < 1000);",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the domain was introspected with multiple constraints
    let domain = schema.domains.get("test_multiple_constraints_domain");
    assert!(
        domain.is_some(),
        "Domain 'test_multiple_constraints_domain' should be introspected"
    );

    let dom = domain.unwrap();
    assert_eq!(dom.name, "test_multiple_constraints_domain");
    assert_eq!(dom.base_type, "integer");
    assert_eq!(
        dom.constraints.len(),
        2,
        "Domain should have two constraints"
    );

    // Verify constraint details
    let constraints: Vec<&str> = dom.constraints.iter().map(|c| c.check.as_str()).collect();
    debug!("Constraints: {:?}", constraints);
    assert!(constraints.contains(&"CHECK ((VALUE > 0))"));
    assert!(constraints.contains(&"CHECK ((VALUE < 1000))"));

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
    execute_sql(&connection, "CREATE SCHEMA test_domain_schema_consistency;").await?;
    execute_sql(
        &connection,
        "CREATE DOMAIN test_domain_schema_consistency.schema_consistency_domain AS date;",
    )
    .await?;

    // Introspect multiple times to verify consistency
    let schema1 = connection.introspect().await?;
    let schema2 = connection.introspect().await?;

    let dom1 = schema1.domains.get("schema_consistency_domain").unwrap();
    let dom2 = schema2.domains.get("schema_consistency_domain").unwrap();

    // Verify consistency across multiple introspections
    assert_eq!(dom1.name, dom2.name);
    assert_eq!(dom1.schema, dom2.schema);
    assert_eq!(dom1.base_type, dom2.base_type);
    assert_eq!(dom1.constraints, dom2.constraints);
    assert_eq!(dom1.default, dom2.default);
    assert_eq!(dom1.not_null, dom2.not_null);
    assert_eq!(dom1.comment, dom2.comment);

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_domain_comment_consistency() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create domain with comment
    execute_sql(
        &connection,
        "CREATE DOMAIN test_comment_consistency_domain AS interval;",
    )
    .await?;
    execute_sql(
        &connection,
        "COMMENT ON DOMAIN test_comment_consistency_domain IS 'Interval domain type';",
    )
    .await?;

    // Introspect multiple times to verify comment consistency
    let schema1 = connection.introspect().await?;
    let schema2 = connection.introspect().await?;

    let dom1 = schema1
        .domains
        .get("test_comment_consistency_domain")
        .unwrap();
    let dom2 = schema2
        .domains
        .get("test_comment_consistency_domain")
        .unwrap();

    // Verify comment consistency across multiple introspections
    assert_eq!(dom1.comment, dom2.comment);
    assert_eq!(
        dom1.comment,
        Some("Interval domain type".to_string()),
        "Domain should have the correct comment"
    );

    // Clean up
    db.cleanup().await?;
    Ok(())
}
