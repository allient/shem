use postgres::PostgresSqlGenerator;
use shem_core::schema::{Rule, RuleEvent};
use shem_core::traits::SqlGenerator;

#[test]
fn test_create_rule() {
    let rule = Rule {
        name: "update_users_rule".to_string(),
        table: "users".to_string(),
        schema: Some("public".to_string()),
        event: RuleEvent::Update,
        instead: true,
        condition: Some("OLD.status = 'inactive'".to_string()),
        actions: vec!["DO NOTHING".to_string()],
    };

    let generator = PostgresSqlGenerator;
    let result = generator.create_rule(&rule).unwrap();

    assert!(result.contains("CREATE RULE update_users_rule AS"));
    assert!(result.contains("ON public.users"));
    assert!(result.contains("TO UPDATE"));
    assert!(result.contains("WHERE (OLD.status = 'inactive')"));
    assert!(result.contains("DO INSTEAD NOTHING"));
}

#[test]
fn test_create_rule_also() {
    let rule = Rule {
        name: "log_updates".to_string(),
        table: "users".to_string(),
        schema: None,
        event: RuleEvent::Update,
        instead: false,
        condition: None,
        actions: vec!["INSERT INTO audit_log (table_name, action, old_data, new_data) VALUES ('users', 'UPDATE', row_to_json(OLD), row_to_json(NEW))".to_string()],
    };

    let generator = PostgresSqlGenerator;
    let result = generator.create_rule(&rule).unwrap();

    assert!(result.contains("CREATE RULE log_updates AS"));
    assert!(result.contains("ON users"));
    assert!(result.contains("TO UPDATE"));
    assert!(result.contains("DO ALSO"));
    assert!(result.contains("INSERT INTO audit_log"));
}

#[test]
fn test_create_rule_insert() {
    let rule = Rule {
        name: "validate_insert".to_string(),
        table: "users".to_string(),
        schema: None,
        event: RuleEvent::Insert,
        instead: true,
        condition: Some("NEW.email IS NULL".to_string()),
        actions: vec!["DO NOTHING".to_string()],
    };

    let generator = PostgresSqlGenerator;
    let result = generator.create_rule(&rule).unwrap();

    assert!(result.contains("CREATE RULE validate_insert AS"));
    assert!(result.contains("ON users"));
    assert!(result.contains("TO INSERT"));
    assert!(result.contains("WHERE (NEW.email IS NULL)"));
    assert!(result.contains("DO INSTEAD NOTHING"));
}

#[test]
fn test_create_rule_delete() {
    let rule = Rule {
        name: "soft_delete".to_string(),
        table: "users".to_string(),
        schema: None,
        event: RuleEvent::Delete,
        instead: true,
        condition: None,
        actions: vec!["UPDATE users SET deleted_at = NOW() WHERE id = OLD.id".to_string()],
    };

    let generator = PostgresSqlGenerator;
    let result = generator.create_rule(&rule).unwrap();

    assert!(result.contains("CREATE RULE soft_delete AS"));
    assert!(result.contains("ON users"));
    assert!(result.contains("TO DELETE"));
    assert!(result.contains("DO INSTEAD"));
    assert!(result.contains("UPDATE users SET deleted_at = NOW()"));
}

#[test]
fn test_create_rule_with_reserved_keyword() {
    let rule = Rule {
        name: "order".to_string(), // Reserved keyword
        table: "orders".to_string(),
        schema: None,
        event: RuleEvent::Update,
        instead: false,
        condition: None,
        actions: vec!["DO NOTHING".to_string()],
    };

    let generator = PostgresSqlGenerator;
    let result = generator.create_rule(&rule).unwrap();

    assert!(result.contains("CREATE RULE \"order\" AS"));
    assert!(result.contains("ON orders"));
    assert!(result.contains("TO UPDATE"));
    assert!(result.contains("DO ALSO NOTHING"));
}

#[test]
fn test_drop_rule() {
    let rule = Rule {
        name: "my_rule".to_string(),
        table: "my_table".to_string(),
        schema: None,
        event: RuleEvent::Update,
        instead: false,
        condition: None,
        actions: vec!["DO NOTHING".to_string()],
    };

    let generator = PostgresSqlGenerator;
    let result = generator.drop_rule(&rule).unwrap();

    assert_eq!(result, "DROP RULE IF EXISTS my_rule ON my_table CASCADE;");
}

#[test]
fn test_drop_rule_with_schema() {
    let rule = Rule {
        name: "my_rule".to_string(),
        table: "my_table".to_string(),
        schema: Some("public".to_string()),
        event: RuleEvent::Update,
        instead: false,
        condition: None,
        actions: vec!["DO NOTHING".to_string()],
    };

    let generator = PostgresSqlGenerator;
    let result = generator.drop_rule(&rule).unwrap();

    assert_eq!(
        result,
        "DROP RULE IF EXISTS public.my_rule ON public.my_table CASCADE;"
    );
}
