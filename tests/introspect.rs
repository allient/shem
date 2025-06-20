mod introspect;

use introspect::*;

#[tokio::test]
async fn test_individual_introspect_features() {
    println!("🚀 Starting individual introspect feature tests...");
    
    // Test each feature in isolation
    test_single_feature("tables", test_tables_introspect).await;
    test_single_feature("views", test_views_introspect).await;
    test_single_feature("materialized_views", test_materialized_views_introspect).await;
    test_single_feature("functions", test_functions_introspect).await;
    test_single_feature("procedures", test_procedures_introspect).await;
    test_single_feature("enums", test_enums_introspect).await;
    test_single_feature("domains", test_domains_introspect).await;
    test_single_feature("composite_types", test_composite_types_introspect).await;
    test_single_feature("range_types", test_range_types_introspect).await;
    test_single_feature("sequences", test_sequences_introspect).await;
    test_single_feature("extensions", test_extensions_introspect).await;
    test_single_feature("triggers", test_triggers_introspect).await;
    test_single_feature("policies", test_policies_introspect).await;
    test_single_feature("servers", test_servers_introspect).await;
    test_single_feature("collations", test_collations_introspect).await;
    test_single_feature("rules", test_rules_introspect).await;
    
    println!("🎉 All individual introspect feature tests completed!");
}

async fn test_single_feature<F>(feature_name: &str, test_fn: F)
where
    F: FnOnce(&TestDatabase) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>,
{
    println!("\n📋 Testing {} introspection in isolation...", feature_name);
    
    let test_db = match setup_test_database().await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("❌ Failed to setup test database for {}: {}", feature_name, e);
            return;
        }
    };
    
    let result = std::panic::catch_unwind(|| {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            test_fn(&test_db).await;
        });
    });
    
    // Always cleanup, regardless of test result
    if let Err(cleanup_err) = cleanup_test_database(test_db).await {
        eprintln!("⚠️  Warning: Failed to cleanup test database for {}: {}", feature_name, cleanup_err);
    }
    
    match result {
        Ok(_) => println!("✅ {} introspection test passed", feature_name),
        Err(e) => {
            eprintln!("❌ {} introspection test failed: {:?}", feature_name, e);
            std::panic::resume_unwind(e);
        }
    }
}

#[tokio::test]
async fn test_introspect_with_all_objects() {
    println!("🚀 Testing introspect with all PostgreSQL objects...");
    
    let test_db = setup_test_database().await.unwrap();
    
    // Create all objects
    execute_sql(&test_db.connection, TABLES_FIXTURE).await.unwrap();
    execute_sql(&test_db.connection, VIEWS_FIXTURE).await.unwrap();
    execute_sql(&test_db.connection, MATERIALIZED_VIEWS_FIXTURE).await.unwrap();
    execute_sql(&test_db.connection, FUNCTIONS_FIXTURE).await.unwrap();
    execute_sql(&test_db.connection, PROCEDURES_FIXTURE).await.unwrap();
    execute_sql(&test_db.connection, ENUMS_FIXTURE).await.unwrap();
    execute_sql(&test_db.connection, DOMAINS_FIXTURE).await.unwrap();
    execute_sql(&test_db.connection, COMPOSITE_TYPES_FIXTURE).await.unwrap();
    execute_sql(&test_db.connection, RANGE_TYPES_FIXTURE).await.unwrap();
    execute_sql(&test_db.connection, SEQUENCES_FIXTURE).await.unwrap();
    execute_sql(&test_db.connection, EXTENSIONS_FIXTURE).await.unwrap();
    execute_sql(&test_db.connection, TRIGGERS_FIXTURE).await.unwrap();
    execute_sql(&test_db.connection, POLICIES_FIXTURE).await.unwrap();
    execute_sql(&test_db.connection, COLLATIONS_FIXTURE).await.unwrap();
    execute_sql(&test_db.connection, RULES_FIXTURE).await.unwrap();
    execute_sql(&test_db.connection, SERVERS_FIXTURE).await.unwrap();
    
    // Introspect everything
    let schema = introspect_schema(&test_db.connection).await.unwrap();
    
    // Print summary
    print_schema_summary(&schema);
    
    // Verify all objects were introspected
    assert!(!schema.tables.is_empty(), "No tables found");
    assert!(!schema.views.is_empty(), "No views found");
    assert!(!schema.materialized_views.is_empty(), "No materialized views found");
    assert!(!schema.functions.is_empty(), "No functions found");
    assert!(!schema.procedures.is_empty(), "No procedures found");
    assert!(!schema.enums.is_empty(), "No enums found");
    assert!(!schema.domains.is_empty(), "No domains found");
    assert!(!schema.types.is_empty(), "No types found");
    assert!(!schema.range_types.is_empty(), "No range types found");
    assert!(!schema.sequences.is_empty(), "No sequences found");
    assert!(!schema.extensions.is_empty(), "No extensions found");
    assert!(!schema.triggers.is_empty(), "No triggers found");
    assert!(!schema.policies.is_empty(), "No policies found");
    assert!(!schema.collations.is_empty(), "No collations found");
    assert!(!schema.rules.is_empty(), "No rules found");
    assert!(!schema.servers.is_empty(), "No servers found");
    
    cleanup_test_database(test_db).await.unwrap();
    println!("✅ Full introspect test passed");
} 