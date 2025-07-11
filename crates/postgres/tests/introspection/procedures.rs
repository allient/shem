use postgres::TestDb;
use shem_core::DatabaseConnection;
use log::debug;

#[tokio::test]
async fn test_introspect_basic_procedure() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create a basic procedure
    connection.execute(r#"
        CREATE OR REPLACE PROCEDURE test_basic_procedure()
        LANGUAGE plpgsql
        AS $$
        BEGIN
            -- do nothing
        END;
        $$;
    "#).await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the procedure was introspected
    let procedures: Vec<_> = schema.procedures.values().collect();
    debug!("Procedures: {:?}", procedures);
    let proc = procedures.iter().find(|p| p.name == "test_basic_procedure").expect("Should find procedure");
    assert_eq!(proc.name, "test_basic_procedure");
    assert_eq!(proc.schema, Some("public".to_string()));
    assert_eq!(proc.language, "plpgsql");
    assert_eq!(proc.parameters.len(), 0);
    assert_eq!(proc.comment, None);
    Ok(())
}

#[tokio::test]
async fn test_introspect_procedure_with_parameters() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create a procedure with IN, OUT, INOUT parameters
    connection.execute(r#"
        CREATE OR REPLACE PROCEDURE test_procedure_params(IN a integer, OUT b text, INOUT c boolean)
        LANGUAGE plpgsql
        AS $$
        BEGIN
            b := 'output';
            c := NOT c;
        END;
        $$;
    "#).await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the procedure was introspected
    let procedures: Vec<_> = schema.procedures.values().collect();
    debug!("Procedures with params: {:?}", procedures);
    let proc = procedures.iter().find(|p| p.name == "test_procedure_params").expect("Should find procedure");
    assert_eq!(proc.name, "test_procedure_params");
    assert_eq!(proc.language, "plpgsql");
    assert_eq!(proc.parameters.len(), 3);
    assert_eq!(proc.parameters[0].name, "a");
    assert_eq!(proc.parameters[0].type_name, "integer");
    assert_eq!(proc.parameters[1].name, "b");
    assert_eq!(proc.parameters[1].type_name, "text");
    assert_eq!(proc.parameters[2].name, "c");
    assert_eq!(proc.parameters[2].type_name, "boolean");
    Ok(())
}

#[tokio::test]
async fn test_introspect_procedure_with_comment() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create a procedure and add a comment
    connection.execute(r#"
        CREATE OR REPLACE PROCEDURE test_procedure_with_comment(x integer)
        LANGUAGE plpgsql
        AS $$
        BEGIN
            -- do nothing
        END;
        $$;
    "#).await?;
    connection.execute("COMMENT ON PROCEDURE test_procedure_with_comment(integer) IS 'This is a test procedure with a comment.';").await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the procedure was introspected with comment
    let procedures: Vec<_> = schema.procedures.values().collect();
    debug!("Procedures with comment: {:?}", procedures);
    let proc = procedures.iter().find(|p| p.name == "test_procedure_with_comment").expect("Should find procedure");
    assert_eq!(proc.name, "test_procedure_with_comment");
    assert_eq!(proc.language, "plpgsql");
    assert_eq!(proc.parameters.len(), 1);
    assert_eq!(proc.parameters[0].name, "x");
    assert_eq!(proc.parameters[0].type_name, "integer");
    assert_eq!(proc.comment, Some("This is a test procedure with a comment.".to_string()));
    Ok(())
}

#[tokio::test]
async fn test_introspect_procedure_security_definer() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create a procedure with SECURITY DEFINER
    connection.execute(r#"
        CREATE OR REPLACE PROCEDURE test_procedure_security_definer()
        LANGUAGE plpgsql
        SECURITY DEFINER
        AS $$
        BEGIN
            -- do nothing
        END;
        $$;
    "#).await?;
    connection.execute("COMMENT ON PROCEDURE test_procedure_security_definer() IS 'Procedure with security definer';").await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the procedure was introspected
    let procedures: Vec<_> = schema.procedures.values().collect();
    debug!("Procedures with security definer: {:?}", procedures);
    let proc = procedures.iter().find(|p| p.name == "test_procedure_security_definer").expect("Should find procedure");
    assert_eq!(proc.name, "test_procedure_security_definer");
    assert_eq!(proc.language, "plpgsql");
    assert_eq!(proc.parameters.len(), 0);
    assert_eq!(proc.security_definer, true);
    assert_eq!(proc.comment, Some("Procedure with security definer".to_string()));
    Ok(())
}

#[tokio::test]
async fn test_introspect_procedure_sql_language() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create a procedure in SQL language
    connection.execute(r#"
        CREATE OR REPLACE PROCEDURE test_procedure_sql()
        LANGUAGE sql
        AS $$
            SELECT 1;
        $$;
    "#).await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the procedure was introspected
    let procedures: Vec<_> = schema.procedures.values().collect();
    debug!("Procedures in SQL: {:?}", procedures);
    let proc = procedures.iter().find(|p| p.name == "test_procedure_sql").expect("Should find procedure");
    assert_eq!(proc.name, "test_procedure_sql");
    assert_eq!(proc.language, "sql");
    Ok(())
} 