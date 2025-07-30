use pg::db_util::TestDb;
use pg::traits::DatabaseConnection;
use tracing::debug;

#[tokio::test]
async fn test_introspect_basic_rule() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create table and a rule
    connection.execute("CREATE TABLE test_rules (id SERIAL PRIMARY KEY, value TEXT);").await?;
    connection.execute("CREATE RULE insert_redirect AS ON INSERT TO test_rules DO INSTEAD NOTHING;").await?;

    // Introspect the database
    let schema = connection.introspect().await?;
    debug!("Introspected rules: {:?}", schema.rules);
    debug!("Available rule keys: {:?}", schema.rules.keys().collect::<Vec<_>>());

    // Verify the rule exists in the schema
    let rule = schema.rules.get("insert_redirect").expect("Rule should exist");
    assert_eq!(rule.name, "insert_redirect");
    assert_eq!(rule.table_name, "test_rules");
    assert_eq!(rule.schema, "public");
    assert!(rule.definition.to_lowercase().contains("on insert"));
    assert!(rule.definition.to_lowercase().contains("do instead nothing"));

    Ok(())
}

#[tokio::test]
async fn test_introspect_rule_with_where_condition() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    connection.execute("CREATE TABLE test_rules_where (id SERIAL PRIMARY KEY, value TEXT);").await?;
    connection.execute("CREATE RULE update_only_even AS ON UPDATE TO test_rules_where WHERE (NEW.id % 2 = 0) DO INSTEAD NOTHING;").await?;

    let schema = connection.introspect().await?;
    let rule = schema.rules.get("update_only_even").expect("Rule should exist");
    assert_eq!(rule.name, "update_only_even");
    assert_eq!(rule.table_name, "test_rules_where");
    assert_eq!(rule.schema, "public");
    assert!(rule.definition.to_lowercase().contains("on update"));
    assert!(rule.definition.to_lowercase().contains("where"));
    assert!(rule.definition.to_lowercase().contains("new.id % 2"));
    assert!(rule.definition.to_lowercase().contains("do instead nothing"));

    Ok(())
}

#[tokio::test]
async fn test_introspect_rule_with_do_instead_select() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create a table for complex rule testing
    connection.execute("CREATE TABLE test_rules_complex (id SERIAL PRIMARY KEY, value TEXT, status TEXT DEFAULT 'active');").await?;
    
    // Create a rule with complex WHERE condition
    connection.execute("CREATE RULE complex_condition_rule AS ON UPDATE TO test_rules_complex WHERE NEW.value != OLD.value AND NEW.status = 'active' DO INSTEAD NOTHING;").await?;

    let schema = connection.introspect().await?;
    let rule = schema.rules.get("complex_condition_rule").expect("Rule should exist");
    assert_eq!(rule.name, "complex_condition_rule");
    assert_eq!(rule.table_name, "test_rules_complex");
    assert_eq!(rule.schema, "public");
    assert!(rule.definition.to_lowercase().contains("on update"));
    assert!(rule.definition.to_lowercase().contains("where"));
    assert!(rule.definition.to_lowercase().contains("new.value"));
    assert!(rule.definition.to_lowercase().contains("old.value"));
    assert!(rule.definition.to_lowercase().contains("new.status"));
    assert!(rule.definition.to_lowercase().contains("active"));
    assert!(rule.definition.to_lowercase().contains("do instead nothing"));

    Ok(())
}

#[tokio::test]
async fn test_introspect_rule_for_delete_event() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    connection.execute("CREATE TABLE test_rules_delete (id SERIAL PRIMARY KEY, value TEXT);").await?;
    connection.execute("CREATE RULE delete_redirect AS ON DELETE TO test_rules_delete DO INSTEAD NOTHING;").await?;

    let schema = connection.introspect().await?;
    let rule = schema.rules.get("delete_redirect").expect("Rule should exist");
    assert_eq!(rule.name, "delete_redirect");
    assert_eq!(rule.table_name, "test_rules_delete");
    assert_eq!(rule.schema, "public");
    assert!(rule.definition.to_lowercase().contains("on delete"));
    assert!(rule.definition.to_lowercase().contains("do instead nothing"));

    Ok(())
}

#[tokio::test]
async fn test_introspect_rule_with_multiple_actions() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    connection.execute("CREATE TABLE test_rules_multi (id SERIAL PRIMARY KEY, value TEXT);").await?;
    connection.execute("CREATE RULE multi_action_rule AS ON UPDATE TO test_rules_multi DO ALSO (UPDATE test_rules_multi SET value = 'updated' WHERE id = NEW.id; NOTIFY test_rules_multi;);").await?;

    let schema = connection.introspect().await?;
    let rule = schema.rules.get("multi_action_rule").expect("Rule should exist");
    assert_eq!(rule.name, "multi_action_rule");
    assert_eq!(rule.table_name, "test_rules_multi");
    assert_eq!(rule.schema, "public");
    assert!(rule.definition.to_lowercase().contains("on update"));
    assert!(rule.definition.to_lowercase().contains("do ("));
    assert!(rule.definition.to_lowercase().contains("update test_rules_multi"));
    assert!(rule.definition.to_lowercase().contains("notify test_rules_multi"));

    Ok(())
} 