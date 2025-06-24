use shem_core::schema::{Trigger, ConstraintTrigger, TriggerEvent, TriggerTiming, TriggerLevel};
use shem_core::traits::SqlGenerator;
use postgres::PostgresSqlGenerator;

#[test]
fn test_create_trigger() {
    let trigger = Trigger {
        name: "update_modified_at".to_string(),
        table: "users".to_string(),
        schema: Some("public".to_string()),
        timing: TriggerTiming::Before,
        events: vec![TriggerEvent::Update { columns: None }],
        function: "update_modified_column".to_string(),
        arguments: vec![],
        condition: Some("OLD.modified_at IS DISTINCT FROM NEW.modified_at".to_string()),
        for_each: TriggerLevel::Row,
        comment: Some("Update modified_at timestamp".to_string()),
        when: None,
    };

    let generator = PostgresSqlGenerator;
    let result = generator.create_trigger(&trigger).unwrap();
    
    assert!(result.contains("CREATE TRIGGER update_modified_at"));
    assert!(result.contains("BEFORE UPDATE ON users"));
    assert!(result.contains("FOR EACH ROW"));
    assert!(result.contains("WHEN (OLD.modified_at IS DISTINCT FROM NEW.modified_at)"));
    assert!(result.contains("EXECUTE FUNCTION update_modified_column()"));
}

#[test]
fn test_create_trigger_multiple_events() {
    let trigger = Trigger {
        name: "audit_changes".to_string(),
        table: "users".to_string(),
        schema: None,
        timing: TriggerTiming::After,
        events: vec![TriggerEvent::Insert, TriggerEvent::Update { columns: None }, TriggerEvent::Delete],
        function: "audit_trigger".to_string(),
        arguments: vec![],
        condition: None,
        for_each: TriggerLevel::Row,
        comment: None,
        when: None,
    };

    let generator = PostgresSqlGenerator;
    let result = generator.create_trigger(&trigger).unwrap();
    
    assert!(result.contains("CREATE TRIGGER audit_changes"));
    assert!(result.contains("AFTER INSERT OR UPDATE OR DELETE ON users"));
    assert!(result.contains("FOR EACH ROW"));
    assert!(result.contains("EXECUTE FUNCTION audit_trigger()"));
}

#[test]
fn test_create_trigger_statement_level() {
    let trigger = Trigger {
        name: "log_table_changes".to_string(),
        table: "users".to_string(),
        schema: None,
        timing: TriggerTiming::After,
        events: vec![TriggerEvent::Insert, TriggerEvent::Update { columns: None }, TriggerEvent::Delete],
        function: "log_changes".to_string(),
        arguments: vec![],
        condition: None,
        for_each: TriggerLevel::Statement,
        comment: None,
        when: None,
    };

    let generator = PostgresSqlGenerator;
    let result = generator.create_trigger(&trigger).unwrap();
    
    assert!(result.contains("CREATE TRIGGER log_table_changes"));
    assert!(result.contains("AFTER INSERT OR UPDATE OR DELETE ON users"));
    assert!(result.contains("FOR EACH STATEMENT"));
    assert!(result.contains("EXECUTE FUNCTION log_changes()"));
}

#[test]
fn test_create_trigger_with_reserved_keyword() {
    let trigger = Trigger {
        name: "order".to_string(), // Reserved keyword
        table: "orders".to_string(),
        schema: None,
        timing: TriggerTiming::Before,
        events: vec![TriggerEvent::Insert],
        function: "validate_order".to_string(),
        arguments: vec![],
        condition: None,
        for_each: TriggerLevel::Row,
        comment: None,
        when: None,
    };

    let generator = PostgresSqlGenerator;
    let result = generator.create_trigger(&trigger).unwrap();
    
    assert!(result.contains("CREATE TRIGGER \"order\""));
    assert!(result.contains("BEFORE INSERT ON orders"));
    assert!(result.contains("FOR EACH ROW"));
    assert!(result.contains("EXECUTE FUNCTION validate_order()"));
}

#[test]
fn test_drop_trigger() {
    let trigger = Trigger {
        name: "my_trigger".to_string(),
        table: "my_table".to_string(),
        schema: None,
        timing: TriggerTiming::Before,
        events: vec![TriggerEvent::Insert],
        function: "my_function".to_string(),
        arguments: vec![],
        condition: None,
        for_each: TriggerLevel::Row,
        comment: None,
        when: None,
    };

    let generator = PostgresSqlGenerator;
    let result = generator.drop_trigger(&trigger).unwrap();
    
    assert_eq!(result, "DROP TRIGGER IF EXISTS my_trigger ON my_table CASCADE;");
}

#[test]
fn test_drop_trigger_with_schema() {
    let trigger = Trigger {
        name: "my_trigger".to_string(),
        table: "my_table".to_string(),
        schema: Some("public".to_string()),
        timing: TriggerTiming::Before,
        events: vec![TriggerEvent::Insert],
        function: "my_function".to_string(),
        arguments: vec![],
        condition: None,
        for_each: TriggerLevel::Row,
        comment: None,
        when: None,
    };

    let generator = PostgresSqlGenerator;
    let result = generator.drop_trigger(&trigger).unwrap();
    
    assert_eq!(result, "DROP TRIGGER IF EXISTS public.my_trigger ON public.my_table CASCADE;");
}

#[test]
fn test_create_constraint_trigger() {
    let trigger = ConstraintTrigger {
        name: "check_user_status".to_string(),
        table: "users".to_string(),
        schema: Some("public".to_string()),
        function: "validate_user_status".to_string(),
        timing: TriggerTiming::After,
        events: vec![TriggerEvent::Insert, TriggerEvent::Update { columns: None }],
        arguments: vec![],
        constraint_name: "check_user_status_constraint".to_string(),
        deferrable: true,
        initially_deferred: false,
    };

    let generator = PostgresSqlGenerator;
    let result = generator.create_constraint_trigger(&trigger).unwrap();
    
    assert!(result.contains("CREATE CONSTRAINT TRIGGER check_user_status"));
    assert!(result.contains("AFTER INSERT OR UPDATE ON public.users"));
    assert!(result.contains("FOR EACH ROW"));
    assert!(result.contains("DEFERRABLE"));
    assert!(result.contains("INITIALLY IMMEDIATE"));
    assert!(result.contains("EXECUTE FUNCTION validate_user_status()"));
}

#[test]
fn test_create_constraint_trigger_deferred() {
    let trigger = ConstraintTrigger {
        name: "check_user_status".to_string(),
        table: "users".to_string(),
        schema: None,
        function: "validate_user_status".to_string(),
        timing: TriggerTiming::After,
        events: vec![TriggerEvent::Insert, TriggerEvent::Update { columns: None }],
        arguments: vec![],
        constraint_name: "check_user_status_constraint".to_string(),
        deferrable: true,
        initially_deferred: true,
    };

    let generator = PostgresSqlGenerator;
    let result = generator.create_constraint_trigger(&trigger).unwrap();
    
    assert!(result.contains("CREATE CONSTRAINT TRIGGER check_user_status"));
    assert!(result.contains("AFTER INSERT OR UPDATE ON users"));
    assert!(result.contains("FOR EACH ROW"));
    assert!(result.contains("DEFERRABLE"));
    assert!(result.contains("INITIALLY DEFERRED"));
    assert!(result.contains("EXECUTE FUNCTION validate_user_status()"));
}

#[test]
fn test_create_constraint_trigger_not_deferrable() {
    let trigger = ConstraintTrigger {
        name: "check_user_status".to_string(),
        table: "users".to_string(),
        schema: None,
        function: "validate_user_status".to_string(),
        timing: TriggerTiming::After,
        events: vec![TriggerEvent::Insert, TriggerEvent::Update { columns: None }],
        arguments: vec![],
        constraint_name: "check_user_status_constraint".to_string(),
        deferrable: false,
        initially_deferred: false,
    };

    let generator = PostgresSqlGenerator;
    let result = generator.create_constraint_trigger(&trigger).unwrap();
    
    assert!(result.contains("CREATE CONSTRAINT TRIGGER check_user_status"));
    assert!(result.contains("AFTER INSERT OR UPDATE ON users"));
    assert!(result.contains("FOR EACH ROW"));
    assert!(!result.contains("DEFERRABLE"));
    assert!(!result.contains("INITIALLY"));
    assert!(result.contains("EXECUTE FUNCTION validate_user_status()"));
}

#[test]
fn test_drop_constraint_trigger() {
    let trigger = ConstraintTrigger {
        name: "my_constraint_trigger".to_string(),
        table: "my_table".to_string(),
        schema: None,
        function: "my_function".to_string(),
        timing: TriggerTiming::After,
        events: vec![TriggerEvent::Insert],
        arguments: vec![],
        constraint_name: "my_constraint".to_string(),
        deferrable: false,
        initially_deferred: false,
    };

    let generator = PostgresSqlGenerator;
    let result = generator.drop_constraint_trigger(&trigger).unwrap();
    
    assert_eq!(result, "DROP TRIGGER IF EXISTS my_constraint_trigger ON my_table CASCADE;");
}

#[test]
fn test_drop_constraint_trigger_with_schema() {
    let trigger = ConstraintTrigger {
        name: "my_constraint_trigger".to_string(),
        table: "my_table".to_string(),
        schema: Some("public".to_string()),
        function: "my_function".to_string(),
        timing: TriggerTiming::After,
        events: vec![TriggerEvent::Insert],
        arguments: vec![],
        constraint_name: "my_constraint".to_string(),
        deferrable: false,
        initially_deferred: false,
    };

    let generator = PostgresSqlGenerator;
    let result = generator.drop_constraint_trigger(&trigger).unwrap();
    
    assert_eq!(result, "DROP TRIGGER IF EXISTS public.my_constraint_trigger ON public.my_table CASCADE;");
} 