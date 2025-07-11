//! Event trigger introspection tests
//! 
//! Tests for introspecting various types of event triggers.

use log::debug;
use postgres::TestDb;
use shem_core::DatabaseConnection;
use shem_core::schema::{EventTriggerEvent};

/// Test helper function to execute SQL on the test database
async fn execute_sql(
    connection: &Box<dyn DatabaseConnection>,
    sql: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    connection.execute(sql).await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_basic_event_trigger() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create a basic event trigger
    execute_sql(&connection, r#"
        CREATE OR REPLACE FUNCTION log_ddl() RETURNS event_trigger LANGUAGE plpgsql AS $$
        BEGIN
            -- dummy
        END;
        $$;
    "#).await?;
    execute_sql(&connection, "CREATE EVENT TRIGGER test_ddl_start ON ddl_command_start EXECUTE FUNCTION log_ddl();").await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the event trigger was introspected
    let trigger = schema.event_triggers.get("test_ddl_start");
    debug!("Event Trigger: {:?}", trigger);
    assert!(
        trigger.is_some(),
        "Event trigger 'test_ddl_start' should be introspected"
    );

    let trigger_obj = trigger.unwrap();
    assert_eq!(trigger_obj.name, "test_ddl_start");
    assert_eq!(trigger_obj.event, EventTriggerEvent::DdlCommandStart);
    assert_eq!(trigger_obj.function, "log_ddl");
    assert!(trigger_obj.enabled);
    assert!(trigger_obj.tags.is_empty());

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_event_triggers_all_types() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create event trigger functions
    execute_sql(&connection, "CREATE OR REPLACE FUNCTION log_ddl() RETURNS event_trigger LANGUAGE plpgsql AS $$ BEGIN END; $$;").await?;
    execute_sql(&connection, "CREATE OR REPLACE FUNCTION log_end() RETURNS event_trigger LANGUAGE plpgsql AS $$ BEGIN END; $$;").await?;
    execute_sql(&connection, "CREATE OR REPLACE FUNCTION log_drop() RETURNS event_trigger LANGUAGE plpgsql AS $$ BEGIN END; $$;").await?;
    execute_sql(&connection, "CREATE OR REPLACE FUNCTION log_rewrite() RETURNS event_trigger LANGUAGE plpgsql AS $$ BEGIN END; $$;").await?;

    // Create event triggers for all types
    execute_sql(&connection, "CREATE EVENT TRIGGER et_start ON ddl_command_start EXECUTE FUNCTION log_ddl();").await?;
    execute_sql(&connection, "CREATE EVENT TRIGGER et_end ON ddl_command_end EXECUTE FUNCTION log_end();").await?;
    execute_sql(&connection, "CREATE EVENT TRIGGER et_drop ON sql_drop EXECUTE FUNCTION log_drop();").await?;
    execute_sql(&connection, "CREATE EVENT TRIGGER et_rewrite ON table_rewrite EXECUTE FUNCTION log_rewrite();").await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify all event triggers were introspected with correct event types
    assert_eq!(schema.event_triggers["et_start"].event, EventTriggerEvent::DdlCommandStart);
    assert_eq!(schema.event_triggers["et_end"].event, EventTriggerEvent::DdlCommandEnd);
    assert_eq!(schema.event_triggers["et_drop"].event, EventTriggerEvent::SqlDrop);
    assert_eq!(schema.event_triggers["et_rewrite"].event, EventTriggerEvent::TableRewrite);

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_event_trigger_with_tags() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create event trigger function
    execute_sql(&connection, "CREATE OR REPLACE FUNCTION log_tagged() RETURNS event_trigger LANGUAGE plpgsql AS $$ BEGIN END; $$;").await?;
    
    // Create event trigger with tags
    execute_sql(&connection, "CREATE EVENT TRIGGER et_tagged ON ddl_command_start WHEN TAG IN ('CREATE TABLE', 'ALTER TABLE') EXECUTE FUNCTION log_tagged();").await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the event trigger was introspected with tags
    let trigger = schema.event_triggers.get("et_tagged");
    assert!(
        trigger.is_some(),
        "Event trigger 'et_tagged' should be introspected"
    );

    let trigger_obj = trigger.unwrap();
    assert_eq!(trigger_obj.tags, vec!["CREATE TABLE", "ALTER TABLE"]);

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_disabled_event_trigger() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create event trigger function
    execute_sql(&connection, "CREATE OR REPLACE FUNCTION log_disabled() RETURNS event_trigger LANGUAGE plpgsql AS $$ BEGIN END; $$;").await?;
    
    // Create and disable event trigger
    execute_sql(&connection, "CREATE EVENT TRIGGER et_disabled ON ddl_command_start EXECUTE FUNCTION log_disabled();").await?;
    execute_sql(&connection, "ALTER EVENT TRIGGER et_disabled DISABLE;").await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the event trigger was introspected as disabled
    let trigger = schema.event_triggers.get("et_disabled");
    assert!(
        trigger.is_some(),
        "Event trigger 'et_disabled' should be introspected"
    );

    let trigger_obj = trigger.unwrap();
    assert!(!trigger_obj.enabled);

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_event_trigger_different_functions() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create different event trigger functions
    execute_sql(&connection, "CREATE OR REPLACE FUNCTION log_func1() RETURNS event_trigger LANGUAGE plpgsql AS $$ BEGIN END; $$;").await?;
    execute_sql(&connection, "CREATE OR REPLACE FUNCTION log_func2() RETURNS event_trigger LANGUAGE plpgsql AS $$ BEGIN END; $$;").await?;
    
    // Create event triggers with different functions
    execute_sql(&connection, "CREATE EVENT TRIGGER et_func1 ON ddl_command_start EXECUTE FUNCTION log_func1();").await?;
    execute_sql(&connection, "CREATE EVENT TRIGGER et_func2 ON ddl_command_end EXECUTE FUNCTION log_func2();").await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify event triggers have correct function names
    assert_eq!(schema.event_triggers["et_func1"].function, "log_func1");
    assert_eq!(schema.event_triggers["et_func2"].function, "log_func2");

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_no_user_event_triggers() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Introspect the database without any user event triggers
    let schema = connection.introspect().await?;

    // Verify no user event triggers are present
    // Note: System event triggers should be filtered out
    let user_event_triggers: Vec<&String> = schema.event_triggers.keys().collect();
    assert!(
        user_event_triggers.is_empty(),
        "No user event triggers should be introspected: {:?}",
        user_event_triggers
    );

    db.cleanup().await?;
    Ok(())
} 