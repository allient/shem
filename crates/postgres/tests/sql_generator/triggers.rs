use postgres::TestDb;
use shem_core::{DatabaseConnection, Trigger, TriggerEvent, TriggerTiming, TriggerLevel};
use log::debug;

#[tokio::test]
async fn test_generate_create_trigger_basic() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    let trigger = Trigger {
        name: "test_trigger".to_string(),
        table: "test_table".to_string(),
        schema: Some("public".to_string()),
        timing: TriggerTiming::Before,
        events: vec![TriggerEvent::Update],
        function: "test_function".to_string(),
        arguments: vec![],
        condition: None,
        for_each: TriggerLevel::Row,
        comment: None,
        when: None,
    };

    // Generate the CREATE TRIGGER statement
    let sql = connection.generate_create_trigger(&trigger).await?;
    debug!("Generated SQL: {}", sql);

    // Verify the SQL contains expected elements
    assert!(sql.contains("CREATE TRIGGER test_trigger"));
    assert!(sql.contains("BEFORE UPDATE"));
    assert!(sql.contains("ON public.test_table"));
    assert!(sql.contains("FOR EACH ROW"));
    assert!(sql.contains("EXECUTE FUNCTION test_function"));

    Ok(())
}

#[tokio::test]
async fn test_generate_create_trigger_multiple_events() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    let trigger = Trigger {
        name: "test_trigger".to_string(),
        table: "test_table".to_string(),
        schema: Some("public".to_string()),
        timing: TriggerTiming::After,
        events: vec![TriggerEvent::Insert, TriggerEvent::Update, TriggerEvent::Delete],
        function: "test_function".to_string(),
        arguments: vec![],
        condition: None,
        for_each: TriggerLevel::Row,
        comment: None,
        when: None,
    };

    // Generate the CREATE TRIGGER statement
    let sql = connection.generate_create_trigger(&trigger).await?;
    debug!("Generated SQL: {}", sql);

    // Verify the SQL contains expected elements
    assert!(sql.contains("CREATE TRIGGER test_trigger"));
    assert!(sql.contains("AFTER INSERT OR UPDATE OR DELETE"));
    assert!(sql.contains("ON public.test_table"));
    assert!(sql.contains("FOR EACH ROW"));
    assert!(sql.contains("EXECUTE FUNCTION test_function"));

    Ok(())
}

#[tokio::test]
async fn test_generate_create_trigger_with_arguments() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    let trigger = Trigger {
        name: "test_trigger".to_string(),
        table: "test_table".to_string(),
        schema: Some("public".to_string()),
        timing: TriggerTiming::Before,
        events: vec![TriggerEvent::Insert, TriggerEvent::Update, TriggerEvent::Delete],
        function: "test_function".to_string(),
        arguments: vec!["arg1".to_string(), "arg2".to_string()],
        condition: None,
        for_each: TriggerLevel::Row,
        comment: None,
        when: None,
    };

    // Generate the CREATE TRIGGER statement
    let sql = connection.generate_create_trigger(&trigger).await?;
    debug!("Generated SQL: {}", sql);

    // Verify the SQL contains expected elements
    assert!(sql.contains("CREATE TRIGGER test_trigger"));
    assert!(sql.contains("BEFORE INSERT OR UPDATE OR DELETE"));
    assert!(sql.contains("ON public.test_table"));
    assert!(sql.contains("FOR EACH ROW"));
    assert!(sql.contains("EXECUTE FUNCTION test_function('arg1', 'arg2')"));

    Ok(())
}

#[tokio::test]
async fn test_generate_create_trigger_instead_of() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    let trigger = Trigger {
        name: "test_trigger".to_string(),
        table: "test_table".to_string(),
        schema: Some("public".to_string()),
        timing: TriggerTiming::InsteadOf,
        events: vec![TriggerEvent::Insert],
        function: "test_function".to_string(),
        arguments: vec![],
        condition: None,
        for_each: TriggerLevel::Row,
        comment: None,
        when: None,
    };

    // Generate the CREATE TRIGGER statement
    let sql = connection.generate_create_trigger(&trigger).await?;
    debug!("Generated SQL: {}", sql);

    // Verify the SQL contains expected elements
    assert!(sql.contains("CREATE TRIGGER test_trigger"));
    assert!(sql.contains("INSTEAD OF INSERT"));
    assert!(sql.contains("ON public.test_table"));
    assert!(sql.contains("FOR EACH ROW"));
    assert!(sql.contains("EXECUTE FUNCTION test_function"));

    Ok(())
}

#[tokio::test]
async fn test_generate_create_trigger_for_each_statement() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    let trigger = Trigger {
        name: "test_trigger".to_string(),
        table: "test_table".to_string(),
        schema: Some("public".to_string()),
        timing: TriggerTiming::After,
        events: vec![TriggerEvent::Insert],
        function: "test_function".to_string(),
        arguments: vec![],
        condition: None,
        for_each: TriggerLevel::Statement,
        comment: None,
        when: None,
    };

    // Generate the CREATE TRIGGER statement
    let sql = connection.generate_create_trigger(&trigger).await?;
    debug!("Generated SQL: {}", sql);

    // Verify the SQL contains expected elements
    assert!(sql.contains("CREATE TRIGGER test_trigger"));
    assert!(sql.contains("AFTER INSERT"));
    assert!(sql.contains("ON public.test_table"));
    assert!(sql.contains("FOR EACH STATEMENT"));
    assert!(sql.contains("EXECUTE FUNCTION test_function"));

    Ok(())
}

#[tokio::test]
async fn test_generate_create_trigger_with_when_condition() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    let trigger = Trigger {
        name: "test_trigger".to_string(),
        table: "test_table".to_string(),
        schema: Some("public".to_string()),
        timing: TriggerTiming::Before,
        events: vec![TriggerEvent::Insert],
        function: "test_function".to_string(),
        arguments: vec![],
        condition: Some("NEW.value > 0".to_string()),
        for_each: TriggerLevel::Row,
        comment: None,
        when: Some("NEW.value > 0".to_string()),
    };

    // Generate the CREATE TRIGGER statement
    let sql = connection.generate_create_trigger(&trigger).await?;
    debug!("Generated SQL: {}", sql);

    // Verify the SQL contains expected elements
    assert!(sql.contains("CREATE TRIGGER test_trigger"));
    assert!(sql.contains("BEFORE INSERT"));
    assert!(sql.contains("ON public.test_table"));
    assert!(sql.contains("FOR EACH ROW"));
    assert!(sql.contains("WHEN (NEW.value > 0)"));
    assert!(sql.contains("EXECUTE FUNCTION test_function"));

    Ok(())
}

#[tokio::test]
async fn test_generate_create_trigger_with_comment() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    let trigger = Trigger {
        name: "test_trigger".to_string(),
        table: "test_table".to_string(),
        schema: Some("public".to_string()),
        timing: TriggerTiming::Before,
        events: vec![TriggerEvent::Insert],
        function: "test_function".to_string(),
        arguments: vec![],
        condition: None,
        for_each: TriggerLevel::Row,
        comment: Some("Test trigger comment".to_string()),
        when: None,
    };

    // Generate the CREATE TRIGGER statement
    let sql = connection.generate_create_trigger(&trigger).await?;
    debug!("Generated SQL: {}", sql);

    // Verify the SQL contains expected elements
    assert!(sql.contains("CREATE TRIGGER test_trigger"));
    assert!(sql.contains("BEFORE INSERT"));
    assert!(sql.contains("ON public.test_table"));
    assert!(sql.contains("FOR EACH ROW"));
    assert!(sql.contains("EXECUTE FUNCTION test_function"));

    // The comment should be added separately
    let comment_sql = connection.generate_comment_on_trigger(&trigger).await?;
    debug!("Generated comment SQL: {}", comment_sql);
    assert!(comment_sql.contains("COMMENT ON TRIGGER test_trigger"));
    assert!(comment_sql.contains("Test trigger comment"));

    Ok(())
} 