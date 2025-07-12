use postgres::TestDb;
use shem_core::{DatabaseConnection, TriggerTiming, TriggerLevel};
use shem_core::schema::TriggerEvent;
use tracing::debug;

#[tokio::test]
async fn test_introspect_basic_trigger() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create a table and a trigger function
    connection.execute("CREATE TABLE test_trigger_table (id serial PRIMARY KEY, value integer);").await?;
    connection.execute(r#"
        CREATE OR REPLACE FUNCTION test_trigger_func()
        RETURNS trigger AS $$
        BEGIN
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
    "#).await?;

    // Create a basic trigger
    connection.execute(r#"
        CREATE TRIGGER test_basic_trigger
        BEFORE INSERT ON test_trigger_table
        FOR EACH ROW
        EXECUTE FUNCTION test_trigger_func();
    "#).await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the trigger was introspected
    let triggers: Vec<_> = schema.triggers.values().collect();
    debug!("Triggers: {:?}", triggers);
    let trig = triggers.iter().find(|t| t.name == "test_basic_trigger").expect("Should find trigger");
    assert_eq!(trig.name, "test_basic_trigger");
    assert_eq!(trig.table, "test_trigger_table");
    assert_eq!(trig.timing, TriggerTiming::Before);
    assert!(trig.events.iter().any(|e| matches!(e, TriggerEvent::Insert)));
    assert_eq!(trig.function, "test_trigger_func");
    assert_eq!(trig.for_each, TriggerLevel::Row);
    Ok(())
}

#[tokio::test]
async fn test_introspect_trigger_multiple_events() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create a table and a trigger function
    connection.execute("CREATE TABLE test_trigger_multi (id serial PRIMARY KEY, value integer);").await?;
    connection.execute(r#"
        CREATE OR REPLACE FUNCTION test_trigger_multi_func()
        RETURNS trigger AS $$
        BEGIN
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
    "#).await?;

    // Create a trigger for multiple events
    connection.execute(r#"
        CREATE TRIGGER test_multi_event_trigger
        AFTER INSERT OR UPDATE OR DELETE ON test_trigger_multi
        FOR EACH ROW
        EXECUTE FUNCTION test_trigger_multi_func();
    "#).await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the trigger was introspected
    let triggers: Vec<_> = schema.triggers.values().collect();
    debug!("Triggers multi: {:?}", triggers);
    let trig = triggers.iter().find(|t| t.name == "test_multi_event_trigger").expect("Should find trigger");
    assert_eq!(trig.name, "test_multi_event_trigger");
    assert_eq!(trig.table, "test_trigger_multi");
    assert_eq!(trig.timing, TriggerTiming::After);
    assert!(trig.events.iter().any(|e| matches!(e, TriggerEvent::Insert)));
    assert!(trig.events.iter().any(|e| matches!(e, TriggerEvent::Update { .. })));
    assert!(trig.events.iter().any(|e| matches!(e, TriggerEvent::Delete)));
    Ok(())
}

#[tokio::test]
async fn test_introspect_trigger_with_when_condition() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create a table and a trigger function
    connection.execute("CREATE TABLE test_trigger_when (id serial PRIMARY KEY, value integer);").await?;
    connection.execute(r#"
        CREATE OR REPLACE FUNCTION test_trigger_when_func()
        RETURNS trigger AS $$
        BEGIN
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
    "#).await?;

    // Create a trigger with a WHEN condition
    connection.execute(r#"
        CREATE TRIGGER test_when_trigger
        AFTER UPDATE ON test_trigger_when
        FOR EACH ROW
        WHEN (NEW.value > 10)
        EXECUTE FUNCTION test_trigger_when_func();
    "#).await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the trigger was introspected
    let triggers: Vec<_> = schema.triggers.values().collect();
    debug!("Triggers when: {:?}", triggers);
    let trig = triggers.iter().find(|t| t.name == "test_when_trigger").expect("Should find trigger");
    assert_eq!(trig.name, "test_when_trigger");
    assert_eq!(trig.table, "test_trigger_when");
    assert_eq!(trig.timing, TriggerTiming::After);
    assert!(trig.events.iter().any(|e| matches!(e, TriggerEvent::Update { .. })));
    assert_eq!(trig.function, "test_trigger_when_func");
    assert_eq!(trig.for_each, TriggerLevel::Row);
    assert!(trig.condition.is_some());
    Ok(())
}

#[tokio::test]
async fn test_introspect_trigger_with_arguments() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create a table and a trigger function that takes arguments
    connection.execute("CREATE TABLE test_trigger_args (id serial PRIMARY KEY, value integer);").await?;
    connection.execute(r#"
        CREATE OR REPLACE FUNCTION test_trigger_args_func()
        RETURNS trigger AS $$
        BEGIN
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
    "#).await?;

    // Create a trigger with arguments
    connection.execute(r#"
        CREATE TRIGGER test_args_trigger
        BEFORE UPDATE ON test_trigger_args
        FOR EACH ROW
        EXECUTE FUNCTION test_trigger_args_func('foo', 'bar');
    "#).await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the trigger was introspected
    let triggers: Vec<_> = schema.triggers.values().collect();
    debug!("Triggers args: {:?}", triggers);
    let trig = triggers.iter().find(|t| t.name == "test_args_trigger").expect("Should find trigger");
    assert_eq!(trig.name, "test_args_trigger");
    assert_eq!(trig.table, "test_trigger_args");
    assert_eq!(trig.timing, TriggerTiming::Before);
    assert!(trig.events.iter().any(|e| matches!(e, TriggerEvent::Update { .. })));
    assert_eq!(trig.function, "test_trigger_args_func");
    assert_eq!(trig.for_each, TriggerLevel::Row);
    assert_eq!(trig.arguments, vec!["foo".to_string(), "bar".to_string()]);
    Ok(())
}

#[tokio::test]
async fn test_introspect_trigger_for_each_statement() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create a table and a trigger function
    connection.execute("CREATE TABLE test_trigger_stmt (id serial PRIMARY KEY, value integer);").await?;
    connection.execute(r#"
        CREATE OR REPLACE FUNCTION test_trigger_stmt_func()
        RETURNS trigger AS $$
        BEGIN
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
    "#).await?;

    // Create a trigger FOR EACH STATEMENT
    connection.execute(r#"
        CREATE TRIGGER test_stmt_trigger
        AFTER DELETE ON test_trigger_stmt
        FOR EACH STATEMENT
        EXECUTE FUNCTION test_trigger_stmt_func();
    "#).await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the trigger was introspected
    let triggers: Vec<_> = schema.triggers.values().collect();
    debug!("Triggers statement: {:?}", triggers);
    let trig = triggers.iter().find(|t| t.name == "test_stmt_trigger").expect("Should find trigger");
    assert_eq!(trig.name, "test_stmt_trigger");
    assert_eq!(trig.table, "test_trigger_stmt");
    assert_eq!(trig.timing, TriggerTiming::After);
    assert!(trig.events.iter().any(|e| matches!(e, TriggerEvent::Delete)));
    assert_eq!(trig.function, "test_trigger_stmt_func");
    assert_eq!(trig.for_each, TriggerLevel::Statement);
    Ok(())
}

#[tokio::test]
async fn test_introspect_trigger_with_comment() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create a table and a trigger function
    connection.execute("CREATE TABLE test_trigger_comment (id serial PRIMARY KEY, value integer);").await?;
    connection.execute(r#"
        CREATE OR REPLACE FUNCTION test_trigger_comment_func()
        RETURNS trigger AS $$
        BEGIN
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
    "#).await?;

    // Create a trigger and add a comment
    connection.execute(r#"
        CREATE TRIGGER test_comment_trigger
        BEFORE INSERT ON test_trigger_comment
        FOR EACH ROW
        EXECUTE FUNCTION test_trigger_comment_func();
    "#).await?;
    connection.execute("COMMENT ON TRIGGER test_comment_trigger ON test_trigger_comment IS 'This is a test trigger with a comment.';").await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the trigger was introspected with comment
    let triggers: Vec<_> = schema.triggers.values().collect();
    debug!("Triggers comment: {:?}", triggers);
    let trig = triggers.iter().find(|t| t.name == "test_comment_trigger").expect("Should find trigger");
    assert_eq!(trig.name, "test_comment_trigger");
    assert_eq!(trig.table, "test_trigger_comment");
    assert_eq!(trig.timing, TriggerTiming::Before);
    assert!(trig.events.iter().any(|e| matches!(e, TriggerEvent::Insert)));
    assert_eq!(trig.function, "test_trigger_comment_func");
    assert_eq!(trig.for_each, TriggerLevel::Row);
    assert_eq!(trig.comment, Some("This is a test trigger with a comment.".to_string()));
    Ok(())
}

#[tokio::test]
async fn test_introspect_constraint_trigger() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create a table and a trigger function
    connection.execute("CREATE TABLE test_constraint_trigger_table (id serial PRIMARY KEY, value integer);").await?;
    connection.execute(r#"
        CREATE OR REPLACE FUNCTION test_constraint_trigger_func()
        RETURNS trigger AS $$
        BEGIN
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
    "#).await?;

    // Create a constraint trigger using a check constraint
    connection.execute(r#"
        ALTER TABLE test_constraint_trigger_table 
        ADD CONSTRAINT check_value_positive 
        CHECK (value > 0);
    "#).await?;

    // Create a constraint trigger manually
    connection.execute(r#"
        CREATE CONSTRAINT TRIGGER test_constraint_trigger
        AFTER INSERT OR UPDATE ON test_constraint_trigger_table
        FOR EACH ROW
        EXECUTE FUNCTION test_constraint_trigger_func();
    "#).await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the constraint trigger was introspected
    let constraint_triggers: Vec<_> = schema.constraint_triggers.values().collect();
    debug!("Constraint triggers: {:?}", constraint_triggers);

    // Should find at least one constraint trigger
    assert!(!constraint_triggers.is_empty(), "Should find constraint triggers");

    // Check that we have the expected constraint trigger
    let constraint_trigger = constraint_triggers.iter().find(|t| t.name == "test_constraint_trigger");
    assert!(constraint_trigger.is_some(), "Should find our constraint trigger");

    if let Some(trigger) = constraint_trigger {
        assert_eq!(trigger.name, "test_constraint_trigger");
        assert_eq!(trigger.table, "test_constraint_trigger_table");
        assert_eq!(trigger.schema, Some("public".to_string()));
        assert_eq!(trigger.function, "test_constraint_trigger_func");
        // The constraint name might be different, so we just check it's not empty
        assert!(!trigger.constraint_name.is_empty(), "Should have a constraint name");
    }

    Ok(())
} 