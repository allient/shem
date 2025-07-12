use postgres::TestDb;
use shem_core::DatabaseConnection;
use shem_core::schema::RuleEvent;
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

    // Verify the rule exists in the schema
    let rule = schema.rules.get("insert_redirect").expect("Rule should exist");
    assert_eq!(rule.name, "insert_redirect");
    assert_eq!(rule.table, "test_rules");
    assert_eq!(rule.event, RuleEvent::Insert);
    assert!(rule.actions.iter().any(|a| a.to_lowercase().contains("instead nothing")));

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
    assert_eq!(rule.table, "test_rules_where");
    assert_eq!(rule.event, RuleEvent::Update);
    let cond = rule.condition.as_ref().unwrap().to_lowercase();
    assert!(cond.contains("new.id % 2"), "Condition should contain 'new.id % 2', got: {}", cond);
    assert!(cond.contains("= 0"), "Condition should contain '= 0', got: {}", cond);
    assert!(rule.actions.iter().any(|a| a.to_lowercase().contains("instead nothing")));

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
    assert_eq!(rule.table, "test_rules_complex");
    assert_eq!(rule.event, RuleEvent::Update);
    assert!(rule.instead);
    
    // Check that the condition contains the expected parts
    let cond = rule.condition.as_ref().unwrap().to_lowercase();
    assert!(cond.contains("new.value"), "Condition should contain 'new.value', got: {}", cond);
    assert!(cond.contains("old.value"), "Condition should contain 'old.value', got: {}", cond);
    assert!(cond.contains("new.status"), "Condition should contain 'new.status', got: {}", cond);
    assert!(cond.contains("active"), "Condition should contain 'active', got: {}", cond);
    
    assert!(rule.actions.iter().any(|a| a.to_lowercase().contains("instead nothing")));

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
    assert_eq!(rule.table, "test_rules_delete");
    assert_eq!(rule.event, RuleEvent::Delete);
    assert!(rule.actions.iter().any(|a| a.to_lowercase().contains("instead nothing")));

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
    assert_eq!(rule.table, "test_rules_multi");
    assert_eq!(rule.event, RuleEvent::Update);
    assert!(!rule.instead);
    assert!(rule.actions.iter().any(|a| a.to_lowercase().contains("do ( update")));
    assert!(rule.actions.iter().any(|a| a.to_lowercase().contains("notify test_rules_multi")));

    Ok(())
} 