use postgres::TestDb;
use shem_core::{DatabaseConnection, Volatility, ParallelSafety, ReturnKind};
use log::debug;

#[tokio::test]
async fn test_introspect_function_with_comment() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create a function with a comment
    connection.execute(r#"
        CREATE OR REPLACE FUNCTION test_function_with_comment(a integer, b text)
        RETURNS integer
        LANGUAGE plpgsql
        AS $$
        BEGIN
            RETURN a + length(b);
        END;
        $$;
    "#).await?;

    // Add a comment to the function
    connection.execute("COMMENT ON FUNCTION test_function_with_comment(integer, text) IS 'This is a test function that adds an integer and the length of a text';").await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the function was introspected with comment
    let functions: Vec<_> = schema.functions.values().collect();
    debug!("Functions: {:?}", functions);
    
    let func = functions.iter().find(|f| f.name == "test_function_with_comment").expect("Should find test function");
    assert_eq!(func.name, "test_function_with_comment");
    assert_eq!(func.schema, Some("public".to_string()));
    assert_eq!(func.language, "plpgsql");
    assert_eq!(func.parameters.len(), 2);
    assert_eq!(func.parameters[0].name, "a");
    assert_eq!(func.parameters[0].type_name, "integer");
    assert_eq!(func.parameters[1].name, "b");
    assert_eq!(func.parameters[1].type_name, "text");
    assert_eq!(func.returns.type_name, "integer");
    assert_eq!(func.comment, Some("This is a test function that adds an integer and the length of a text".to_string()));
    
    Ok(())
}

#[tokio::test]
async fn test_introspect_function_without_comment() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create a function without a comment
    connection.execute(r#"
        CREATE OR REPLACE FUNCTION test_function_no_comment(x integer)
        RETURNS integer
        LANGUAGE sql
        AS $$
            SELECT x * 2;
        $$;
    "#).await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the function was introspected without comment
    let functions: Vec<_> = schema.functions.values().collect();
    debug!("Functions without comment: {:?}", functions);
    
    let func = functions.iter().find(|f| f.name == "test_function_no_comment").expect("Should find test function");
    assert_eq!(func.name, "test_function_no_comment");
    assert_eq!(func.schema, Some("public".to_string()));
    assert_eq!(func.language, "sql");
    assert_eq!(func.parameters.len(), 1);
    assert_eq!(func.parameters[0].name, "x");
    assert_eq!(func.parameters[0].type_name, "integer");
    assert_eq!(func.returns.type_name, "integer");
    assert_eq!(func.comment, None);
    
    Ok(())
}

#[tokio::test]
async fn test_introspect_function_returns_table() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create a table for the function to return
    connection.execute("CREATE TABLE test_table (id integer, name text);").await?;

    // Create a function that returns a table
    connection.execute(r#"
        CREATE OR REPLACE FUNCTION test_function_returns_table()
        RETURNS TABLE(id integer, name text)
        LANGUAGE sql
        AS $$
            SELECT * FROM test_table;
        $$;
    "#).await?;

    // Add a comment
    connection.execute("COMMENT ON FUNCTION test_function_returns_table() IS 'Function that returns a table';").await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the function was introspected correctly
    let functions: Vec<_> = schema.functions.values().collect();
    debug!("Functions returning table: {:?}", functions);
    
    let func = functions.iter().find(|f| f.name == "test_function_returns_table").expect("Should find test function");
    assert_eq!(func.name, "test_function_returns_table");
    assert_eq!(func.language, "sql");
    assert_eq!(func.parameters.len(), 0);
    assert!(func.returns.type_name.contains("TABLE"));
    assert_eq!(func.comment, Some("Function that returns a table".to_string()));
    
    Ok(())
}

#[tokio::test]
async fn test_introspect_function_with_default_parameters() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create a function with default parameters
    connection.execute(r#"
        CREATE OR REPLACE FUNCTION test_function_defaults(
            a integer DEFAULT 10,
            b text DEFAULT 'hello',
            c boolean DEFAULT true
        )
        RETURNS text
        LANGUAGE plpgsql
        AS $$
        BEGIN
            RETURN a::text || ' ' || b || ' ' || c::text;
        END;
        $$;
    "#).await?;

    // Add a comment
    connection.execute("COMMENT ON FUNCTION test_function_defaults(integer, text, boolean) IS 'Function with default parameters';").await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the function was introspected correctly
    let functions: Vec<_> = schema.functions.values().collect();
    debug!("Functions with defaults: {:?}", functions);
    
    let func = functions.iter().find(|f| f.name == "test_function_defaults").expect("Should find test function");
    assert_eq!(func.name, "test_function_defaults");
    assert_eq!(func.language, "plpgsql");
    assert_eq!(func.parameters.len(), 3);
    assert_eq!(func.parameters[0].name, "a");
    assert_eq!(func.parameters[0].type_name, "integer");
    assert_eq!(func.parameters[1].name, "b");
    assert_eq!(func.parameters[1].type_name, "text");
    assert_eq!(func.parameters[2].name, "c");
    assert_eq!(func.parameters[2].type_name, "boolean");
    assert_eq!(func.returns.type_name, "text");
    assert_eq!(func.comment, Some("Function with default parameters".to_string()));
    
    Ok(())
}

#[tokio::test]
async fn test_introspect_function_with_complex_return_type() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create a custom type
    connection.execute("CREATE TYPE test_complex_type AS (id integer, name text, active boolean);").await?;

    // Create a function that returns the complex type
    connection.execute(r#"
        CREATE OR REPLACE FUNCTION test_function_complex_return()
        RETURNS test_complex_type
        LANGUAGE plpgsql
        AS $$
        BEGIN
            RETURN ROW(1, 'test', true)::test_complex_type;
        END;
        $$;
    "#).await?;

    // Add a comment
    connection.execute("COMMENT ON FUNCTION test_function_complex_return() IS 'Function returning complex type';").await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the function was introspected correctly
    let functions: Vec<_> = schema.functions.values().collect();
    debug!("Functions with complex return: {:?}", functions);
    
    let func = functions.iter().find(|f| f.name == "test_function_complex_return").expect("Should find test function");
    assert_eq!(func.name, "test_function_complex_return");
    assert_eq!(func.language, "plpgsql");
    assert_eq!(func.parameters.len(), 0);
    assert_eq!(func.returns.type_name, "test_complex_type");
    assert_eq!(func.comment, Some("Function returning complex type".to_string()));
    
    Ok(())
}

#[tokio::test]
async fn test_introspect_function_with_security_definer() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create a function with SECURITY DEFINER
    connection.execute(r#"
        CREATE OR REPLACE FUNCTION test_function_security_definer()
        RETURNS integer
        LANGUAGE sql
        SECURITY DEFINER
        AS $$
            SELECT 42;
        $$;
    "#).await?;

    // Add a comment
    connection.execute("COMMENT ON FUNCTION test_function_security_definer() IS 'Function with security definer';").await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the function was introspected correctly
    let functions: Vec<_> = schema.functions.values().collect();
    debug!("Functions with security definer: {:?}", functions);
    
    let func = functions.iter().find(|f| f.name == "test_function_security_definer").expect("Should find test function");
    assert_eq!(func.name, "test_function_security_definer");
    assert_eq!(func.language, "sql");
    assert_eq!(func.parameters.len(), 0);
    assert_eq!(func.returns.type_name, "integer");
    assert!(func.security_definer);
    assert_eq!(func.comment, Some("Function with security definer".to_string()));
    
    Ok(())
}

#[tokio::test]
async fn test_introspect_function_with_volatility() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create a function with STABLE volatility
    connection.execute(r#"
        CREATE OR REPLACE FUNCTION test_function_stable(x integer)
        RETURNS integer
        LANGUAGE sql
        STABLE
        AS $$
            SELECT x + 1;
        $$;
    "#).await?;

    // Create a function with IMMUTABLE volatility
    connection.execute(r#"
        CREATE OR REPLACE FUNCTION test_function_immutable(x integer)
        RETURNS integer
        LANGUAGE sql
        IMMUTABLE
        AS $$
            SELECT x * 2;
        $$;
    "#).await?;

    // Add comments
    connection.execute("COMMENT ON FUNCTION test_function_stable(integer) IS 'Stable function';").await?;
    connection.execute("COMMENT ON FUNCTION test_function_immutable(integer) IS 'Immutable function';").await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the functions were introspected correctly
    let functions: Vec<_> = schema.functions.values().collect();
    debug!("Functions with volatility: {:?}", functions);
    
    let stable_func = functions.iter().find(|f| f.name == "test_function_stable").expect("Should find stable function");
    assert_eq!(stable_func.volatility, Volatility::Stable);
    assert_eq!(stable_func.comment, Some("Stable function".to_string()));
    
    let immutable_func = functions.iter().find(|f| f.name == "test_function_immutable").expect("Should find immutable function");
    assert_eq!(immutable_func.volatility, Volatility::Immutable);
    assert_eq!(immutable_func.comment, Some("Immutable function".to_string()));
    
    Ok(())
}

#[tokio::test]
async fn test_introspect_function_with_cost_and_rows() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create a function with COST and ROWS hints
    connection.execute(r#"
        CREATE OR REPLACE FUNCTION test_function_cost_rows()
        RETURNS SETOF integer
        LANGUAGE sql
        COST 100
        ROWS 1000
        AS $$
            SELECT generate_series(1, 100);
        $$;
    "#).await?;

    // Add a comment
    connection.execute("COMMENT ON FUNCTION test_function_cost_rows() IS 'Function with cost and rows hints';").await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the function was introspected correctly
    let functions: Vec<_> = schema.functions.values().collect();
    debug!("Functions with cost and rows: {:?}", functions);
    
    let func = functions.iter().find(|f| f.name == "test_function_cost_rows").expect("Should find test function");
    assert_eq!(func.name, "test_function_cost_rows");
    assert_eq!(func.language, "sql");
    assert_eq!(func.parameters.len(), 0);
    assert_eq!(func.returns.kind, ReturnKind::SetOf);
    assert_eq!(func.returns.type_name, "integer");
    assert_eq!(func.cost, Some(100.0));
    assert_eq!(func.rows, Some(1000.0));
    assert_eq!(func.comment, Some("Function with cost and rows hints".to_string()));
    
    Ok(())
}

#[tokio::test]
async fn test_introspect_function_with_parallel_safety() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create a function with PARALLEL SAFE
    connection.execute(r#"
        CREATE OR REPLACE FUNCTION test_function_parallel_safe(x integer)
        RETURNS integer
        LANGUAGE sql
        PARALLEL SAFE
        AS $$
            SELECT x + 1;
        $$;
    "#).await?;

    // Create a function with PARALLEL UNSAFE
    connection.execute(r#"
        CREATE OR REPLACE FUNCTION test_function_parallel_unsafe(x integer)
        RETURNS integer
        LANGUAGE sql
        PARALLEL UNSAFE
        AS $$
            SELECT x * 2;
        $$;
    "#).await?;

    // Add comments
    connection.execute("COMMENT ON FUNCTION test_function_parallel_safe(integer) IS 'Parallel safe function';").await?;
    connection.execute("COMMENT ON FUNCTION test_function_parallel_unsafe(integer) IS 'Parallel unsafe function';").await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the functions were introspected correctly
    let functions: Vec<_> = schema.functions.values().collect();
    debug!("Functions with parallel safety: {:?}", functions);
    
    let safe_func = functions.iter().find(|f| f.name == "test_function_parallel_safe").expect("Should find parallel safe function");
    assert_eq!(safe_func.parallel_safety, ParallelSafety::Safe);
    assert_eq!(safe_func.comment, Some("Parallel safe function".to_string()));
    
    let unsafe_func = functions.iter().find(|f| f.name == "test_function_parallel_unsafe").expect("Should find parallel unsafe function");
    assert_eq!(unsafe_func.parallel_safety, ParallelSafety::Unsafe);
    assert_eq!(unsafe_func.comment, Some("Parallel unsafe function".to_string()));
    
    Ok(())
}

#[tokio::test]
async fn test_introspect_function_with_strict() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create a function with STRICT
    connection.execute(r#"
        CREATE OR REPLACE FUNCTION test_function_strict(x integer)
        RETURNS integer
        LANGUAGE sql
        STRICT
        AS $$
            SELECT x + 1;
        $$;
    "#).await?;

    // Add a comment
    connection.execute("COMMENT ON FUNCTION test_function_strict(integer) IS 'Strict function';").await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the function was introspected correctly
    let functions: Vec<_> = schema.functions.values().collect();
    debug!("Functions with strict: {:?}", functions);
    
    let func = functions.iter().find(|f| f.name == "test_function_strict").expect("Should find test function");
    assert_eq!(func.name, "test_function_strict");
    assert_eq!(func.language, "sql");
    assert_eq!(func.parameters.len(), 1);
    assert_eq!(func.returns.type_name, "integer");
    assert!(func.strict);
    assert_eq!(func.comment, Some("Strict function".to_string()));
    
    Ok(())
} 