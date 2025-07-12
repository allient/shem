use shem_core::schema::{Trigger, TriggerEvent, TriggerTiming, TriggerLevel};
use shem_core::traits::SqlGenerator;
use postgres::PostgresSqlGenerator;

#[test]
fn test_create_trigger_basic() {
    let trigger = Trigger {
        name: "test_trigger".to_string(),
        table: "test_table".to_string(),
        schema: None,
        timing: TriggerTiming::Before,
        events: vec![TriggerEvent::Insert],
        function: "test_function".to_string(),
        arguments: vec![],
        condition: None,
        for_each: TriggerLevel::Row,
        comment: None,
        when: None,
    };
    let sql = PostgresSqlGenerator.create_trigger(&trigger).unwrap();
    assert!(sql.contains("CREATE TRIGGER \"test_trigger\""));
    assert!(sql.contains("BEFORE INSERT"));
    assert!(sql.contains("ON \"test_table\""));
    assert!(sql.contains("FOR EACH ROW"));
    assert!(sql.contains("EXECUTE FUNCTION test_function"));
}

#[test]
fn test_create_trigger_with_arguments() {
    let trigger = Trigger {
        name: "test_trigger".to_string(),
        table: "test_table".to_string(),
        schema: None,
        timing: TriggerTiming::After,
        events: vec![TriggerEvent::Update],
        function: "test_function".to_string(),
        arguments: vec!["arg1".to_string(), "arg2".to_string()],
        condition: None,
        for_each: TriggerLevel::Row,
        comment: None,
        when: None,
    };
    let sql = PostgresSqlGenerator.create_trigger(&trigger).unwrap();
    assert!(sql.contains("CREATE TRIGGER \"test_trigger\""));
    assert!(sql.contains("AFTER UPDATE"));
    assert!(sql.contains("ON \"test_table\""));
    assert!(sql.contains("FOR EACH ROW"));
    assert!(sql.contains("EXECUTE FUNCTION test_function(arg1, arg2)"));
}

#[test]
fn test_create_trigger_with_condition() {
    let trigger = Trigger {
        name: "test_trigger".to_string(),
        table: "test_table".to_string(),
        schema: None,
        timing: TriggerTiming::Before,
        events: vec![TriggerEvent::Insert],
        function: "test_function".to_string(),
        arguments: vec![],
        condition: Some("NEW.id > 0".to_string()),
        for_each: TriggerLevel::Row,
        comment: None,
        when: Some("NEW.id > 0".to_string()),
    };
    let sql = PostgresSqlGenerator.create_trigger(&trigger).unwrap();
    assert!(sql.contains("CREATE TRIGGER \"test_trigger\""));
    assert!(sql.contains("BEFORE INSERT"));
    assert!(sql.contains("ON \"test_table\""));
    assert!(sql.contains("FOR EACH ROW"));
    assert!(sql.contains("WHEN (NEW.id > 0)"));
    assert!(sql.contains("EXECUTE FUNCTION test_function"));
}

#[test]
fn test_create_trigger_for_each_statement() {
    let trigger = Trigger {
        name: "test_trigger".to_string(),
        table: "test_table".to_string(),
        schema: None,
        timing: TriggerTiming::After,
        events: vec![TriggerEvent::Delete],
        function: "test_function".to_string(),
        arguments: vec![],
        condition: None,
        for_each: TriggerLevel::Statement,
        comment: None,
        when: None,
    };
    let sql = PostgresSqlGenerator.create_trigger(&trigger).unwrap();
    assert!(sql.contains("CREATE TRIGGER \"test_trigger\""));
    assert!(sql.contains("AFTER DELETE"));
    assert!(sql.contains("ON \"test_table\""));
    assert!(sql.contains("FOR EACH STATEMENT"));
    assert!(sql.contains("EXECUTE FUNCTION test_function"));
}

#[test]
fn test_create_trigger_multiple_events() {
    let trigger = Trigger {
        name: "test_trigger".to_string(),
        table: "test_table".to_string(),
        schema: None,
        timing: TriggerTiming::Before,
        events: vec![TriggerEvent::Insert, TriggerEvent::Update],
        function: "test_function".to_string(),
        arguments: vec![],
        condition: None,
        for_each: TriggerLevel::Row,
        comment: None,
        when: None,
    };
    let sql = PostgresSqlGenerator.create_trigger(&trigger).unwrap();
    assert!(sql.contains("CREATE TRIGGER \"test_trigger\""));
    assert!(sql.contains("BEFORE INSERT OR UPDATE"));
    assert!(sql.contains("ON \"test_table\""));
    assert!(sql.contains("FOR EACH ROW"));
    assert!(sql.contains("EXECUTE FUNCTION test_function"));
}

#[test]
fn test_create_trigger_with_schema() {
    let trigger = Trigger {
        name: "test_trigger".to_string(),
        table: "test_table".to_string(),
        schema: Some("test_schema".to_string()),
        timing: TriggerTiming::Before,
        events: vec![TriggerEvent::Insert],
        function: "test_function".to_string(),
        arguments: vec![],
        condition: None,
        for_each: TriggerLevel::Row,
        comment: None,
        when: None,
    };
    let sql = PostgresSqlGenerator.create_trigger(&trigger).unwrap();
    assert!(sql.contains("CREATE TRIGGER \"test_trigger\""));
    assert!(sql.contains("BEFORE INSERT"));
    assert!(sql.contains("ON \"test_schema\".\"test_table\""));
    assert!(sql.contains("FOR EACH ROW"));
    assert!(sql.contains("EXECUTE FUNCTION test_function"));
}

#[test]
fn test_create_trigger_with_comment() {
    let trigger = Trigger {
        name: "test_trigger".to_string(),
        table: "test_table".to_string(),
        schema: None,
        timing: TriggerTiming::Before,
        events: vec![TriggerEvent::Insert],
        function: "test_function".to_string(),
        arguments: vec![],
        condition: None,
        for_each: TriggerLevel::Row,
        comment: Some("Test trigger comment".to_string()),
        when: None,
    };
    let sql = PostgresSqlGenerator.create_trigger(&trigger).unwrap();
    assert!(sql.contains("CREATE TRIGGER \"test_trigger\""));
    assert!(sql.contains("BEFORE INSERT"));
    assert!(sql.contains("ON \"test_table\""));
    assert!(sql.contains("FOR EACH ROW"));
    assert!(sql.contains("EXECUTE FUNCTION test_function"));
    assert!(sql.contains("COMMENT ON TRIGGER \"test_trigger\" ON \"test_table\" IS 'Test trigger comment';"));
}

#[test]
fn test_drop_trigger() {
    let trigger = Trigger {
        name: "test_trigger".to_string(),
        table: "test_table".to_string(),
        schema: None,
        timing: TriggerTiming::Before,
        events: vec![TriggerEvent::Insert],
        function: "test_function".to_string(),
        arguments: vec![],
        condition: None,
        for_each: TriggerLevel::Row,
        comment: None,
        when: None,
    };
    let sql = PostgresSqlGenerator.drop_trigger(&trigger).unwrap();
    assert_eq!(sql, "DROP TRIGGER IF EXISTS \"test_trigger\" ON \"test_table\" CASCADE;");
} 