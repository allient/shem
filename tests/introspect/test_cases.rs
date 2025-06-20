use super::fixtures::*;
use super::helpers::*;
use shem_core::Schema;

/// Test tables introspection in isolation
pub async fn test_tables_introspect(test_db: &TestDatabase) {
    println!("Testing tables introspection...");
    
    // Setup: Create tables
    execute_sql(&test_db.connection, TABLES_FIXTURE).await.unwrap();
    
    // Test: Introspect schema
    let schema = introspect_schema(&test_db.connection).await.unwrap();
    
    // Assert: Tables exist
    assert_table_exists(&schema, "users");
    assert_table_exists(&schema, "posts");
    assert_table_exists(&schema, "comments");
    
    // Assert: Table details
    let users_table = schema.tables.get("users").unwrap();
    assert_eq!(users_table.columns.len(), 7); // id, username, email, age, is_active, created_at, updated_at
    assert_eq!(users_table.constraints.len(), 3); // PRIMARY KEY, UNIQUE constraints
    
    let posts_table = schema.tables.get("posts").unwrap();
    assert_eq!(posts_table.columns.len(), 6); // id, user_id, title, content, published_at, tags
    assert!(posts_table.constraints.iter().any(|c| c.kind.to_string().contains("FOREIGN KEY")));
    
    println!("✅ Tables introspection test passed");
}

/// Test views introspection in isolation
pub async fn test_views_introspect(test_db: &TestDatabase) {
    println!("Testing views introspection...");
    
    // Setup: Create tables and views
    execute_sql(&test_db.connection, TABLES_FIXTURE).await.unwrap();
    execute_sql(&test_db.connection, VIEWS_FIXTURE).await.unwrap();
    
    // Test: Introspect schema
    let schema = introspect_schema(&test_db.connection).await.unwrap();
    
    // Assert: Views exist
    assert_view_exists(&schema, "active_users");
    assert_view_exists(&schema, "post_stats");
    assert_view_exists(&schema, "recent_activity");
    
    // Assert: View details
    let active_users_view = schema.views.get("active_users").unwrap();
    assert!(!active_users_view.definition.is_empty());
    
    println!("✅ Views introspection test passed");
}

/// Test materialized views introspection in isolation
pub async fn test_materialized_views_introspect(test_db: &TestDatabase) {
    println!("Testing materialized views introspection...");
    
    // Setup: Create tables and materialized views
    execute_sql(&test_db.connection, TABLES_FIXTURE).await.unwrap();
    execute_sql(&test_db.connection, MATERIALIZED_VIEWS_FIXTURE).await.unwrap();
    
    // Test: Introspect schema
    let schema = introspect_schema(&test_db.connection).await.unwrap();
    
    // Assert: Materialized views exist
    assert!(schema.materialized_views.contains_key("user_post_counts"));
    assert!(schema.materialized_views.contains_key("popular_posts"));
    
    // Assert: Materialized view details
    let user_post_counts_view = schema.materialized_views.get("user_post_counts").unwrap();
    assert!(!user_post_counts_view.definition.is_empty());
    
    println!("✅ Materialized views introspection test passed");
}

/// Test functions introspection in isolation
pub async fn test_functions_introspect(test_db: &TestDatabase) {
    println!("Testing functions introspection...");
    
    // Setup: Create functions
    execute_sql(&test_db.connection, FUNCTIONS_FIXTURE).await.unwrap();
    
    // Test: Introspect schema
    let schema = introspect_schema(&test_db.connection).await.unwrap();
    
    // Assert: Functions exist
    assert_function_exists(&schema, "get_user_posts");
    assert_function_exists(&schema, "calculate_age");
    assert_function_exists(&schema, "update_updated_at");
    assert_function_exists(&schema, "get_post_comments");
    
    // Assert: Function details
    let get_user_posts_func = schema.functions.get("get_user_posts").unwrap();
    assert_eq!(get_user_posts_func.parameters.len(), 1); // user_id parameter
    assert!(get_user_posts_func.return_type.contains("TABLE"));
    
    println!("✅ Functions introspection test passed");
}

/// Test procedures introspection in isolation
pub async fn test_procedures_introspect(test_db: &TestDatabase) {
    println!("Testing procedures introspection...");
    
    // Setup: Create tables and procedures
    execute_sql(&test_db.connection, TABLES_FIXTURE).await.unwrap();
    execute_sql(&test_db.connection, PROCEDURES_FIXTURE).await.unwrap();
    
    // Test: Introspect schema
    let schema = introspect_schema(&test_db.connection).await.unwrap();
    
    // Assert: Procedures exist
    assert!(schema.procedures.contains_key("create_user"));
    assert!(schema.procedures.contains_key("archive_old_posts"));
    
    // Assert: Procedure details
    let create_user_proc = schema.procedures.get("create_user").unwrap();
    assert_eq!(create_user_proc.parameters.len(), 4); // 3 IN + 1 OUT parameter
    
    println!("✅ Procedures introspection test passed");
}

/// Test enums introspection in isolation
pub async fn test_enums_introspect(test_db: &TestDatabase) {
    println!("Testing enums introspection...");
    
    // Setup: Create enums
    execute_sql(&test_db.connection, ENUMS_FIXTURE).await.unwrap();
    
    // Test: Introspect schema
    let schema = introspect_schema(&test_db.connection).await.unwrap();
    
    // Assert: Enums exist
    assert_enum_exists(&schema, "user_status");
    assert_enum_exists(&schema, "post_type");
    assert_enum_exists(&schema, "comment_status");
    assert_enum_exists(&schema, "priority_level");
    
    // Assert: Enum details
    let user_status_enum = schema.enums.get("user_status").unwrap();
    assert_eq!(user_status_enum.values.len(), 4);
    assert!(user_status_enum.values.contains(&"active".to_string()));
    assert!(user_status_enum.values.contains(&"inactive".to_string()));
    
    println!("✅ Enums introspection test passed");
}

/// Test domains introspection in isolation
pub async fn test_domains_introspect(test_db: &TestDatabase) {
    println!("Testing domains introspection...");
    
    // Setup: Create domains
    execute_sql(&test_db.connection, DOMAINS_FIXTURE).await.unwrap();
    
    // Test: Introspect schema
    let schema = introspect_schema(&test_db.connection).await.unwrap();
    
    // Assert: Domains exist
    assert_domain_exists(&schema, "email_address");
    assert_domain_exists(&schema, "positive_integer");
    assert_domain_exists(&schema, "non_empty_string");
    assert_domain_exists(&schema, "url_string");
    assert_domain_exists(&schema, "phone_number");
    
    // Assert: Domain details
    let email_domain = schema.domains.get("email_address").unwrap();
    assert_eq!(email_domain.base_type, "character varying");
    assert!(email_domain.constraint.is_some()); // Should have CHECK constraint
    
    println!("✅ Domains introspection test passed");
}

/// Test composite types introspection in isolation
pub async fn test_composite_types_introspect(test_db: &TestDatabase) {
    println!("Testing composite types introspection...");
    
    // Setup: Create domains and composite types
    execute_sql(&test_db.connection, DOMAINS_FIXTURE).await.unwrap();
    execute_sql(&test_db.connection, COMPOSITE_TYPES_FIXTURE).await.unwrap();
    
    // Test: Introspect schema
    let schema = introspect_schema(&test_db.connection).await.unwrap();
    
    // Assert: Composite types exist
    assert!(schema.types.contains_key("address"));
    assert!(schema.types.contains_key("contact_info"));
    assert!(schema.types.contains_key("post_metadata"));
    
    // Assert: Composite type details
    let address_type = schema.types.get("address").unwrap();
    assert_eq!(address_type.kind.to_string(), "Composite");
    
    println!("✅ Composite types introspection test passed");
}

/// Test range types introspection in isolation
pub async fn test_range_types_introspect(test_db: &TestDatabase) {
    println!("Testing range types introspection...");
    
    // Setup: Create range types
    execute_sql(&test_db.connection, RANGE_TYPES_FIXTURE).await.unwrap();
    
    // Test: Introspect schema
    let schema = introspect_schema(&test_db.connection).await.unwrap();
    
    // Assert: Range types exist
    assert!(schema.range_types.contains_key("date_range"));
    assert!(schema.range_types.contains_key("int4_range"));
    assert!(schema.range_types.contains_key("num_range"));
    assert!(schema.range_types.contains_key("ts_range"));
    
    // Assert: Range type details
    let date_range_type = schema.range_types.get("date_range").unwrap();
    assert_eq!(date_range_type.subtype, "date");
    
    println!("✅ Range types introspection test passed");
}

/// Test sequences introspection in isolation
pub async fn test_sequences_introspect(test_db: &TestDatabase) {
    println!("Testing sequences introspection...");
    
    // Setup: Create sequences
    execute_sql(&test_db.connection, SEQUENCES_FIXTURE).await.unwrap();
    
    // Test: Introspect schema
    let schema = introspect_schema(&test_db.connection).await.unwrap();
    
    // Assert: Sequences exist
    assert_sequence_exists(&schema, "custom_user_id_seq");
    assert_sequence_exists(&schema, "order_number_seq");
    assert_sequence_exists(&schema, "invoice_seq");
    assert_sequence_exists(&schema, "ticket_seq");
    
    // Assert: Sequence details
    let custom_seq = schema.sequences.get("custom_user_id_seq").unwrap();
    assert_eq!(custom_seq.start_value, Some(1000));
    assert_eq!(custom_seq.increment, Some(5));
    
    println!("✅ Sequences introspection test passed");
}

/// Test extensions introspection in isolation
pub async fn test_extensions_introspect(test_db: &TestDatabase) {
    println!("Testing extensions introspection...");
    
    // Setup: Create extensions
    execute_sql(&test_db.connection, EXTENSIONS_FIXTURE).await.unwrap();
    
    // Test: Introspect schema
    let schema = introspect_schema(&test_db.connection).await.unwrap();
    
    // Assert: Extensions exist
    assert_extension_exists(&schema, "uuid-ossp");
    assert_extension_exists(&schema, "pg_trgm");
    assert_extension_exists(&schema, "citext");
    assert_extension_exists(&schema, "hstore");
    
    println!("✅ Extensions introspection test passed");
}

/// Test triggers introspection in isolation
pub async fn test_triggers_introspect(test_db: &TestDatabase) {
    println!("Testing triggers introspection...");
    
    // Setup: Create tables, functions, and triggers
    execute_sql(&test_db.connection, TABLES_FIXTURE).await.unwrap();
    execute_sql(&test_db.connection, FUNCTIONS_FIXTURE).await.unwrap();
    execute_sql(&test_db.connection, TRIGGERS_FIXTURE).await.unwrap();
    
    // Test: Introspect schema
    let schema = introspect_schema(&test_db.connection).await.unwrap();
    
    // Assert: Triggers exist
    assert_trigger_exists(&schema, "update_user_updated_at");
    assert_trigger_exists(&schema, "update_post_updated_at");
    assert_trigger_exists(&schema, "log_user_changes");
    
    // Assert: Trigger details
    let update_trigger = schema.triggers.get("update_user_updated_at").unwrap();
    assert_eq!(update_trigger.table_name, Some("users".to_string()));
    assert_eq!(update_trigger.function_name, "update_updated_at");
    
    println!("✅ Triggers introspection test passed");
}

/// Test policies introspection in isolation
pub async fn test_policies_introspect(test_db: &TestDatabase) {
    println!("Testing policies introspection...");
    
    // Setup: Create tables and policies
    execute_sql(&test_db.connection, TABLES_FIXTURE).await.unwrap();
    execute_sql(&test_db.connection, POLICIES_FIXTURE).await.unwrap();
    
    // Test: Introspect schema
    let schema = introspect_schema(&test_db.connection).await.unwrap();
    
    // Assert: Policies exist
    assert_policy_exists(&schema, "users_select_policy");
    assert_policy_exists(&schema, "users_insert_policy");
    assert_policy_exists(&schema, "posts_select_policy");
    assert_policy_exists(&schema, "comments_select_policy");
    
    // Assert: Policy details
    let select_policy = schema.policies.get("users_select_policy").unwrap();
    assert_eq!(select_policy.table_name, "users");
    assert_eq!(select_policy.command, "SELECT");
    
    println!("✅ Policies introspection test passed");
}

/// Test servers introspection in isolation
pub async fn test_servers_introspect(test_db: &TestDatabase) {
    println!("Testing servers introspection...");
    
    // Setup: Create servers
    execute_sql(&test_db.connection, SERVERS_FIXTURE).await.unwrap();
    
    // Test: Introspect schema
    let schema = introspect_schema(&test_db.connection).await.unwrap();
    
    // Assert: Servers exist
    assert!(schema.servers.contains_key("external_db"));
    assert!(schema.servers.contains_key("analytics_db"));
    
    // Assert: Server details
    let external_server = schema.servers.get("external_db").unwrap();
    assert_eq!(external_server.fdw_name, "postgres_fdw");
    
    println!("✅ Servers introspection test passed");
}

/// Test collations introspection in isolation
pub async fn test_collations_introspect(test_db: &TestDatabase) {
    println!("Testing collations introspection...");
    
    // Setup: Create collations
    execute_sql(&test_db.connection, COLLATIONS_FIXTURE).await.unwrap();
    
    // Test: Introspect schema
    let schema = introspect_schema(&test_db.connection).await.unwrap();
    
    // Assert: Collations exist
    assert!(schema.collations.contains_key("german_phonebook"));
    assert!(schema.collations.contains_key("french_phonebook"));
    assert!(schema.collations.contains_key("spanish_phonebook"));
    
    // Assert: Collation details
    let german_collation = schema.collations.get("german_phonebook").unwrap();
    assert_eq!(german_collation.provider, "icu");
    
    println!("✅ Collations introspection test passed");
}

/// Test rules introspection in isolation
pub async fn test_rules_introspect(test_db: &TestDatabase) {
    println!("Testing rules introspection...");
    
    // Setup: Create tables and rules
    execute_sql(&test_db.connection, TABLES_FIXTURE).await.unwrap();
    execute_sql(&test_db.connection, RULES_FIXTURE).await.unwrap();
    
    // Test: Introspect schema
    let schema = introspect_schema(&test_db.connection).await.unwrap();
    
    // Assert: Rules exist
    assert!(schema.rules.contains_key("prevent_user_deletion"));
    assert!(schema.rules.contains_key("log_post_changes"));
    assert!(schema.rules.contains_key("prevent_comment_spam"));
    
    // Assert: Rule details
    let prevent_deletion_rule = schema.rules.get("prevent_user_deletion").unwrap();
    assert_eq!(prevent_deletion_rule.table_name, "users");
    assert_eq!(prevent_deletion_rule.event, "DELETE");
    
    println!("✅ Rules introspection test passed");
} 