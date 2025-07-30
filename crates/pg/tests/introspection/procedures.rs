use pg::db_util::TestDb;
use pg::traits::DatabaseConnection;
use tracing::debug;

// Helper function to extract procedures from routines
fn get_procedures_from_schema(schema: &pg::model::schema::Schema) -> Vec<&pg::model::routine::Procedure> {
    schema.routines.values()
        .filter_map(|r| match r {
            pg::model::routine::Routine::Procedure(p) => Some(p),
            _ => None,
        })
        .collect()
}

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
    let procedures = get_procedures_from_schema(&schema);
    debug!("Procedures: {:?}", procedures);
    let proc = procedures.iter().find(|p| p.name == "test_basic_procedure").expect("Should find procedure");
    assert_eq!(proc.name, "test_basic_procedure");
    assert_eq!(proc.schema, "public");
    assert!(proc.definition.contains("plpgsql"));
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
    let procedures = get_procedures_from_schema(&schema);
    debug!("Procedures with params: {:?}", procedures);
    let proc = procedures.iter().find(|p| p.name == "test_procedure_params").expect("Should find procedure");
    assert_eq!(proc.name, "test_procedure_params");
    assert!(proc.definition.contains("plpgsql"));
    assert!(proc.definition.contains("IN a integer"));
    assert!(proc.definition.contains("OUT b text"));
    assert!(proc.definition.contains("INOUT c boolean"));
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
    let procedures = get_procedures_from_schema(&schema);
    debug!("Procedures with comment: {:?}", procedures);
    let proc = procedures.iter().find(|p| p.name == "test_procedure_with_comment").expect("Should find procedure");
    assert_eq!(proc.name, "test_procedure_with_comment");
    assert!(proc.definition.contains("plpgsql"));
    assert!(proc.definition.contains("x integer"));
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
    let procedures = get_procedures_from_schema(&schema);
    debug!("Procedures with security definer: {:?}", procedures);
    let proc = procedures.iter().find(|p| p.name == "test_procedure_security_definer").expect("Should find procedure");
    assert_eq!(proc.name, "test_procedure_security_definer");
    assert!(proc.definition.contains("plpgsql"));
    assert!(proc.definition.contains("SECURITY DEFINER"));
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
    let procedures = get_procedures_from_schema(&schema);
    debug!("Procedures in SQL: {:?}", procedures);
    let proc = procedures.iter().find(|p| p.name == "test_procedure_sql").expect("Should find procedure");
    assert_eq!(proc.name, "test_procedure_sql");
    assert!(proc.definition.contains("sql"));
    Ok(())
} 