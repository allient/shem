use log::debug;
use postgres::TestDb;
use shem_core::DatabaseConnection;

/// Test helper function to execute SQL on the test database
async fn execute_sql(
    connection: &Box<dyn DatabaseConnection>,
    sql: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    connection.execute(sql).await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_basic_table() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create a basic table
    execute_sql(
        &connection,
        "CREATE TABLE test_basic_table (id integer PRIMARY KEY, name text);",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;
    
    // Verify the table was introspected
    let table = schema.tables.get("test_basic_table");
    debug!("Table: {:?}", table);
    assert!(
        table.is_some(),
        "Table 'test_basic_table' should be introspected"
    );

    let tbl = table.unwrap();
    assert_eq!(tbl.name, "test_basic_table");
    assert_eq!(tbl.columns.len(), 2, "Table should have 2 columns");
    assert_eq!(tbl.constraints.len(), 1, "Table should have 1 constraint (PRIMARY KEY)");
    assert_eq!(tbl.indexes.len(), 1, "Table should have custom indexes initially because pk");
    assert!(tbl.comment.is_none(), "Table should not have comment initially");
    assert!(tbl.tablespace.is_none(), "Table should not have tablespace initially");
    assert!(tbl.inherits.is_empty(), "Table should not inherit from any table");
    assert!(tbl.partition_by.is_none(), "Table should not be partitioned");
    assert!(tbl.storage_parameters.is_empty(), "Table should have no storage parameters");

    // Verify columns
    let id_column = tbl.columns.iter().find(|c| c.name == "id").unwrap();
    assert_eq!(id_column.name, "id");
    assert_eq!(id_column.type_name, "integer");
    assert!(!id_column.nullable, "Primary key column should not be nullable");
    assert!(id_column.default.is_none(), "Primary key column should not have default");

    let name_column = tbl.columns.iter().find(|c| c.name == "name").unwrap();
    assert_eq!(name_column.name, "name");
    assert_eq!(name_column.type_name, "text");
    assert!(name_column.nullable, "Text column should be nullable");
    assert!(name_column.default.is_none(), "Text column should not have default");

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_table_with_schema() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create schema and table in that schema
    execute_sql(&connection, "CREATE SCHEMA test_table_schema;").await?;
    execute_sql(
        &connection,
        "CREATE TABLE test_table_schema.schema_table (id serial PRIMARY KEY, data jsonb);",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the table was introspected with correct schema
    let table = schema.tables.get("schema_table");
    assert!(
        table.is_some(),
        "Table 'schema_table' should be introspected"
    );

    let tbl = table.unwrap();
    assert_eq!(tbl.name, "schema_table");
    assert_eq!(
        tbl.schema,
        Some("test_table_schema".to_string()),
        "Table should be in the specified schema"
    );
    assert_eq!(tbl.columns.len(), 2, "Table should have 2 columns");

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_table_with_comment() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create table and add comment
    execute_sql(
        &connection,
        "CREATE TABLE test_comment_table (id integer PRIMARY KEY);",
    )
    .await?;
    execute_sql(
        &connection,
        "COMMENT ON TABLE test_comment_table IS 'Table with comment';",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the table was introspected with comment
    let table = schema.tables.get("test_comment_table");
    assert!(
        table.is_some(),
        "Table 'test_comment_table' should be introspected"
    );

    let tbl = table.unwrap();
    assert_eq!(tbl.name, "test_comment_table");
    assert_eq!(
        tbl.comment,
        Some("Table with comment".to_string()),
        "Table should have the specified comment"
    );

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_table_with_constraints() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create table with various constraints
    execute_sql(
        &connection,
        "CREATE TABLE test_constraints_table (
            id integer PRIMARY KEY,
            email text UNIQUE NOT NULL,
            age integer CHECK (age >= 0 AND age <= 150),
            status text DEFAULT 'active' CHECK (status IN ('active', 'inactive', 'pending'))
        );",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the table was introspected with constraints
    let table = schema.tables.get("test_constraints_table");
    assert!(
        table.is_some(),
        "Table 'test_constraints_table' should be introspected"
    );

    let tbl = table.unwrap();
    assert_eq!(tbl.name, "test_constraints_table");
    assert_eq!(tbl.columns.len(), 4, "Table should have 4 columns");
    assert!(tbl.constraints.len() >= 4, "Table should have at least 4 constraints");

    // Verify columns with constraints
    let id_column = tbl.columns.iter().find(|c| c.name == "id").unwrap();
    assert!(!id_column.nullable, "Primary key column should not be nullable");

    let email_column = tbl.columns.iter().find(|c| c.name == "email").unwrap();
    assert!(!email_column.nullable, "Email column should not be nullable");

    let status_column = tbl.columns.iter().find(|c| c.name == "status").unwrap();
    debug!("Status column: {:?}", status_column);
    assert_eq!(status_column.default, Some("'active'::text".to_string()), "Status column should have default");

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_table_with_indexes() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create table with indexes
    execute_sql(
        &connection,
        "CREATE TABLE test_indexes_table (
            id integer PRIMARY KEY,
            name text,
            email text,
            created_at timestamp
        );",
    )
    .await?;
    execute_sql(
        &connection,
        "CREATE INDEX idx_test_indexes_name ON test_indexes_table (name);",
    )
    .await?;
    execute_sql(
        &connection,
        "CREATE UNIQUE INDEX idx_test_indexes_email ON test_indexes_table (email);",
    )
    .await?;
    execute_sql(
        &connection,
        "CREATE INDEX idx_test_indexes_created_at ON test_indexes_table (created_at DESC);",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the table was introspected with indexes
    let table = schema.tables.get("test_indexes_table");
    assert!(
        table.is_some(),
        "Table 'test_indexes_table' should be introspected"
    );

    let tbl = table.unwrap();
    assert_eq!(tbl.name, "test_indexes_table");
    assert_eq!(tbl.columns.len(), 4, "Table should have 4 columns");
    assert!(tbl.indexes.len() >= 3, "Table should have at least 3 custom indexes");

    // Verify indexes
    let name_index = tbl.indexes.iter().find(|i| i.name == "idx_test_indexes_name").unwrap();
    assert!(!name_index.unique, "Name index should not be unique");
    assert_eq!(name_index.method, shem_core::IndexMethod::Btree);

    let email_index = tbl.indexes.iter().find(|i| i.name == "idx_test_indexes_email").unwrap();
    assert!(email_index.unique, "Email index should be unique");

    // Clean up
    db.cleanup().await?;
    Ok(())
}

// #[tokio::test]
// async fn test_introspect_table_with_tablespace() -> Result<(), Box<dyn std::error::Error>> {
//     env_logger::try_init().ok();
//     let db = TestDb::new().await?;
//     let connection = &db.conn;

//     // Use a temporary directory that we can control
//     let temp_dir = tempfile::tempdir()?;
//     let tablespace_path = temp_dir.path().join("test_tablespace");
    
//     // Create the tablespace directory
//     std::fs::create_dir_all(&tablespace_path)?;
    
//     // Set proper permissions (readable and writable by owner)
//     #[cfg(unix)]
//     {
//         use std::os::unix::fs::PermissionsExt;
//         std::fs::set_permissions(&tablespace_path, std::fs::Permissions::from_mode(0o755))?;
//     }

//     // Create tablespace and table in that tablespace
//     execute_sql(
//         &connection,
//         &format!("CREATE TABLESPACE test_tablespace LOCATION '{}';", tablespace_path.display()),
//     )
//     .await?;
//     execute_sql(
//         &connection,
//         "CREATE TABLE test_tablespace_table (id integer PRIMARY KEY) TABLESPACE test_tablespace;",
//     )
//     .await?;

//     // Introspect the database
//     let schema = connection.introspect().await?;

//     // Verify the table was introspected with tablespace
//     let table = schema.tables.get("test_tablespace_table");
//     assert!(
//         table.is_some(),
//         "Table 'test_tablespace_table' should be introspected"
//     );

//     let tbl = table.unwrap();
//     assert_eq!(tbl.name, "test_tablespace_table");
//     assert_eq!(
//         tbl.tablespace,
//         Some("test_tablespace".to_string()),
//         "Table should be in the specified tablespace"
//     );

//     // Clean up
//     db.cleanup().await?;
//     Ok(())
// }

#[tokio::test]
async fn test_introspect_table_with_inheritance() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create parent table
    execute_sql(
        &connection,
        "CREATE TABLE parent_table (id integer PRIMARY KEY, name text);",
    )
    .await?;

    // Create child table that inherits from parent
    execute_sql(
        &connection,
        "CREATE TABLE child_table (child_specific text) INHERITS (parent_table);",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the child table was introspected with inheritance
    let table = schema.tables.get("child_table");
    assert!(
        table.is_some(),
        "Table 'child_table' should be introspected"
    );

    let tbl = table.unwrap();
    assert_eq!(tbl.name, "child_table");
    assert_eq!(tbl.inherits.len(), 1, "Child table should inherit from 1 parent");
    assert_eq!(tbl.inherits[0], "parent_table", "Child table should inherit from parent_table");

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_table_with_storage_parameters() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create table with storage parameters
    execute_sql(
        &connection,
        "CREATE TABLE test_storage_table (
            id integer PRIMARY KEY,
            data text
        ) WITH (fillfactor = 80, autovacuum_vacuum_scale_factor = 0.1);",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the table was introspected with storage parameters
    let table = schema.tables.get("test_storage_table");
    assert!(
        table.is_some(),
        "Table 'test_storage_table' should be introspected"
    );

    let tbl = table.unwrap();
    assert_eq!(tbl.name, "test_storage_table");
    assert!(!tbl.storage_parameters.is_empty(), "Table should have storage parameters");
    assert_eq!(
        tbl.storage_parameters.get("fillfactor"),
        Some(&"80".to_string()),
        "Table should have fillfactor storage parameter"
    );

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_table_with_identity_columns() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create table with identity columns
    execute_sql(
        &connection,
        "CREATE TABLE test_identity_table (
            id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            name text,
            seq_id integer GENERATED BY DEFAULT AS IDENTITY
        );",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the table was introspected with identity columns
    let table = schema.tables.get("test_identity_table");
    assert!(
        table.is_some(),
        "Table 'test_identity_table' should be introspected"
    );

    let tbl = table.unwrap();
    assert_eq!(tbl.name, "test_identity_table");
    assert_eq!(tbl.columns.len(), 3, "Table should have 3 columns");

    // Verify identity columns
    let id_column = tbl.columns.iter().find(|c| c.name == "id").unwrap();
    assert!(id_column.identity.is_some(), "ID column should be identity");
    let id_identity = id_column.identity.as_ref().unwrap();
    assert!(id_identity.always, "ID column should be GENERATED ALWAYS");

    let seq_id_column = tbl.columns.iter().find(|c| c.name == "seq_id").unwrap();
    assert!(seq_id_column.identity.is_some(), "seq_id column should be identity");
    let seq_id_identity = seq_id_column.identity.as_ref().unwrap();
    assert!(!seq_id_identity.always, "seq_id column should be GENERATED BY DEFAULT");

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_table_with_generated_columns() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create table with generated columns
    execute_sql(
        &connection,
        "CREATE TABLE test_generated_table (
            id integer PRIMARY KEY,
            first_name text,
            last_name text,
            full_name text GENERATED ALWAYS AS (first_name || ' ' || last_name) STORED
        );",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the table was introspected with generated columns
    let table = schema.tables.get("test_generated_table");
    assert!(
        table.is_some(),
        "Table 'test_generated_table' should be introspected"
    );

    let tbl = table.unwrap();
    assert_eq!(tbl.name, "test_generated_table");
    assert_eq!(tbl.columns.len(), 4, "Table should have 4 columns");

    // Verify generated column
    let full_name_column = tbl.columns.iter().find(|c| c.name == "full_name").unwrap();
    assert!(full_name_column.generated.is_some(), "full_name column should be generated");
    let generated = full_name_column.generated.as_ref().unwrap();
    assert!(generated.stored, "Generated column should be STORED");

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_multiple_tables() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create multiple tables
    execute_sql(
        &connection,
        "CREATE TABLE test_table1 (id integer PRIMARY KEY, name text);",
    )
    .await?;
    execute_sql(
        &connection,
        "CREATE TABLE test_table2 (id integer PRIMARY KEY, data jsonb);",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify both tables were introspected
    assert!(
        schema.tables.contains_key("test_table1"),
        "Table 'test_table1' should be introspected"
    );
    assert!(
        schema.tables.contains_key("test_table2"),
        "Table 'test_table2' should be introspected"
    );

    // Verify table details
    let tbl1 = schema.tables.get("test_table1").unwrap();
    let tbl2 = schema.tables.get("test_table2").unwrap();

    assert_eq!(tbl1.name, "test_table1");
    assert_eq!(tbl2.name, "test_table2");
    assert_eq!(tbl1.columns.len(), 2);
    assert_eq!(tbl2.columns.len(), 2);

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_no_tables() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Introspect the database without any user tables
    let schema = connection.introspect().await?;

    // Verify no user tables are present
    // Note: System tables should be filtered out
    let user_tables: Vec<&String> = schema.tables.keys().collect();
    assert!(
        user_tables.is_empty(),
        "No user tables should be introspected: {:?}",
        user_tables
    );

    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_table_edge_cases() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Test case 1: Table with very long name
    let long_name = "a".repeat(50);
    execute_sql(
        &connection,
        &format!("CREATE TABLE {} (id integer PRIMARY KEY);", long_name),
    )
    .await?;

    let schema = connection.introspect().await?;
    let table = schema.tables.get(&long_name);
    assert!(
        table.is_some(),
        "Table with long name should be introspected"
    );

    // Test case 2: Table with special characters in name
    execute_sql(
        &connection,
        "CREATE TABLE \"test-table-with-dashes\" (id integer PRIMARY KEY);",
    )
    .await?;

    let schema2 = connection.introspect().await?;
    let table2 = schema2.tables.get("test-table-with-dashes");
    assert!(
        table2.is_some(),
        "Table with special characters should be introspected"
    );

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_table_performance() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create multiple tables
    for i in 1..=5 {
        execute_sql(
            &connection,
            &format!("CREATE TABLE test_perf_table_{} (id integer PRIMARY KEY, name text);", i),
        )
        .await?;
    }

    // Measure introspection performance
    let start = std::time::Instant::now();
    let schema = connection.introspect().await?;
    let duration = start.elapsed();

    // Verify all tables were introspected
    for i in 1..=5 {
        assert!(
            schema.tables.contains_key(&format!("test_perf_table_{}", i)),
            "Table test_perf_table_{} should be introspected",
            i
        );
    }

    // Performance assertion (adjust threshold as needed)
    assert!(
        duration.as_millis() < 1000,
        "Introspection should complete within 1 second"
    );

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_table_consistency() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create a table
    execute_sql(
        &connection,
        "CREATE TABLE test_consistency_table (id integer PRIMARY KEY, name text);",
    )
    .await?;

    // Introspect multiple times to verify consistency
    let schema1 = connection.introspect().await?;
    let schema2 = connection.introspect().await?;

    let tbl1 = schema1.tables.get("test_consistency_table").unwrap();
    let tbl2 = schema2.tables.get("test_consistency_table").unwrap();

    // Verify consistency across multiple introspections
    assert_eq!(tbl1.name, tbl2.name);
    assert_eq!(tbl1.schema, tbl2.schema);
    assert_eq!(tbl1.columns.len(), tbl2.columns.len());
    assert_eq!(tbl1.constraints.len(), tbl2.constraints.len());
    assert_eq!(tbl1.indexes.len(), tbl2.indexes.len());
    assert_eq!(tbl1.comment, tbl2.comment);
    assert_eq!(tbl1.tablespace, tbl2.tablespace);
    assert_eq!(tbl1.inherits, tbl2.inherits);
    assert_eq!(tbl1.partition_by, tbl2.partition_by);
    assert_eq!(tbl1.storage_parameters, tbl2.storage_parameters);

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_table_schema_consistency() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create schema and table
    execute_sql(&connection, "CREATE SCHEMA test_table_schema_consistency;").await?;
    execute_sql(
        &connection,
        "CREATE TABLE test_table_schema_consistency.schema_consistency_table (id integer PRIMARY KEY);",
    )
    .await?;

    // Introspect multiple times to verify consistency
    let schema1 = connection.introspect().await?;
    let schema2 = connection.introspect().await?;

    let tbl1 = schema1.tables.get("schema_consistency_table").unwrap();
    let tbl2 = schema2.tables.get("schema_consistency_table").unwrap();

    // Verify consistency across multiple introspections
    assert_eq!(tbl1.name, tbl2.name);
    assert_eq!(tbl1.schema, tbl2.schema);
    assert_eq!(tbl1.columns.len(), tbl2.columns.len());
    assert_eq!(tbl1.constraints.len(), tbl2.constraints.len());
    assert_eq!(tbl1.indexes.len(), tbl2.indexes.len());
    assert_eq!(tbl1.comment, tbl2.comment);
    assert_eq!(tbl1.tablespace, tbl2.tablespace);
    assert_eq!(tbl1.inherits, tbl2.inherits);
    assert_eq!(tbl1.partition_by, tbl2.partition_by);
    assert_eq!(tbl1.storage_parameters, tbl2.storage_parameters);

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_table_comment_consistency() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create table with comment
    execute_sql(
        &connection,
        "CREATE TABLE test_comment_consistency_table (id integer PRIMARY KEY);",
    )
    .await?;
    execute_sql(
        &connection,
        "COMMENT ON TABLE test_comment_consistency_table IS 'Table for consistency testing';",
    )
    .await?;

    // Introspect multiple times to verify comment consistency
    let schema1 = connection.introspect().await?;
    let schema2 = connection.introspect().await?;

    let tbl1 = schema1.tables.get("test_comment_consistency_table").unwrap();
    let tbl2 = schema2.tables.get("test_comment_consistency_table").unwrap();

    // Verify comment consistency across multiple introspections
    assert_eq!(tbl1.comment, tbl2.comment);
    assert_eq!(
        tbl1.comment,
        Some("Table for consistency testing".to_string()),
        "Table should have the correct comment"
    );

    // Clean up
    db.cleanup().await?;
    Ok(())
} 

#[tokio::test]
async fn test_introspect_table_with_various_defaults() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create table with various default types
    execute_sql(
        &connection,
        "CREATE TABLE test_various_defaults_table (
            id serial PRIMARY KEY,
            int_col integer DEFAULT 42,
            bool_col boolean DEFAULT true,
            now_col timestamp DEFAULT now(),
            expr_col integer DEFAULT 1 + 2,
            text_col text DEFAULT 'hello',
            uuid_col uuid DEFAULT gen_random_uuid()
        );",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the table was introspected with defaults
    let table = schema.tables.get("test_various_defaults_table");
    assert!(
        table.is_some(),
        "Table 'test_various_defaults_table' should be introspected"
    );

    let tbl = table.unwrap();
    let int_col = tbl.columns.iter().find(|c| c.name == "int_col").unwrap();
    assert_eq!(int_col.default, Some("42".to_string()), "int_col should have default 42");

    let bool_col = tbl.columns.iter().find(|c| c.name == "bool_col").unwrap();
    assert_eq!(bool_col.default, Some("true".to_string()), "bool_col should have default true");

    let now_col = tbl.columns.iter().find(|c| c.name == "now_col").unwrap();
    // Accept both 'now()' and 'now'::text, depending on how introspection returns it
    assert!(
        now_col.default.as_deref() == Some("now()") || now_col.default.as_deref().map(|s| s.starts_with("now()")) == Some(true),
        "now_col should have a now() default, got {:?}", now_col.default
    );

    let expr_col = tbl.columns.iter().find(|c| c.name == "expr_col").unwrap();
    debug!("Expr col: {:?}", expr_col);
    assert!(
        expr_col.default.as_deref() == Some("3") || expr_col.default.as_deref() == Some("(1 + 2)"),
        "expr_col should have default 3 or '1 + 2', got {:?}", expr_col.default
    );

    let text_col = tbl.columns.iter().find(|c| c.name == "text_col").unwrap();
    debug!("Text col: {:?}", text_col);
    assert!(
        text_col.default.as_deref() == Some("'hello'::text") || text_col.default.as_deref() == Some("'hello'"),
        "text_col should have default 'hello', got {:?}", text_col.default
    );

    let uuid_col = tbl.columns.iter().find(|c| c.name == "uuid_col").unwrap();
    debug!("UUID col: {:?}", uuid_col);
    assert!(
        uuid_col.default.as_deref().map(|s| s.starts_with("gen_random_uuid()")) == Some(true),
        "uuid_col should have gen_random_uuid() default, got {:?}", uuid_col.default
    );

    // Clean up
    db.cleanup().await?;
    Ok(())
} 

#[tokio::test]
async fn test_introspect_table_with_foreign_keys() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create parent table
    execute_sql(
        &connection,
        "CREATE TABLE parent_table (id SERIAL PRIMARY KEY, name TEXT);",
    )
    .await?;

    // Create child table with foreign key
    execute_sql(
        &connection,
        "CREATE TABLE child_table (
            id SERIAL PRIMARY KEY,
            parent_id INTEGER REFERENCES parent_table(id),
            description TEXT
        );",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify both tables were introspected
    let parent_table = schema.tables.get("parent_table");
    let child_table = schema.tables.get("child_table");
    
    assert!(parent_table.is_some(), "Parent table should be introspected");
    assert!(child_table.is_some(), "Child table should be introspected");

    let child = child_table.unwrap();
    assert_eq!(child.name, "child_table");
    assert_eq!(child.columns.len(), 3, "Child table should have 3 columns");

    // Verify foreign key constraint
    let fk_constraints: Vec<_> = child.constraints.iter()
        .filter(|c| matches!(c.kind, shem_core::ConstraintKind::ForeignKey { .. }))
        .collect();
    
    assert!(!fk_constraints.is_empty(), "Child table should have foreign key constraints");
    
    let fk = fk_constraints[0];
    assert!(matches!(fk.kind, shem_core::ConstraintKind::ForeignKey { .. }), "Constraint should be foreign key");
    assert!(fk.definition.contains("parent_id"), "FK definition should contain parent_id column");

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_partitioned_table() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create partitioned table
    execute_sql(
        &connection,
        "CREATE TABLE partitioned_table (
            id INTEGER,
            created_date DATE,
            data TEXT
        ) PARTITION BY RANGE (created_date);",
    )
    .await?;

    // Create partitions
    execute_sql(
        &connection,
        "CREATE TABLE partitioned_table_2023 PARTITION OF partitioned_table
         FOR VALUES FROM ('2023-01-01') TO ('2024-01-01');",
    )
    .await?;

    execute_sql(
        &connection,
        "CREATE TABLE partitioned_table_2024 PARTITION OF partitioned_table
         FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the partitioned table was introspected
    let table = schema.tables.get("partitioned_table");
    assert!(table.is_some(), "Partitioned table should be introspected");

    let partitioned = table.unwrap();
    assert_eq!(partitioned.name, "partitioned_table");
    assert!(partitioned.partition_by.is_some(), "Table should have partition information");

    let partition_info = partitioned.partition_by.as_ref().unwrap();
    assert_eq!(partition_info.method, shem_core::PartitionMethod::Range, "Should be range partitioned");
    assert_eq!(partition_info.columns, vec!["created_date"], "Should partition by created_date");

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_unlogged_table() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create unlogged table
    execute_sql(
        &connection,
        "CREATE UNLOGGED TABLE unlogged_table (
            id SERIAL PRIMARY KEY,
            data TEXT
        );",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the unlogged table was introspected
    let table = schema.tables.get("unlogged_table");
    assert!(table.is_some(), "Unlogged table should be introspected");

    let unlogged = table.unwrap();
    assert_eq!(unlogged.name, "unlogged_table");
    assert_eq!(unlogged.columns.len(), 2, "Table should have 2 columns");

    // Note: The unlogged property might be stored in storage_parameters or as a separate field
    // This depends on how the introspection is implemented

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_table_with_rls() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create table with RLS
    execute_sql(
        &connection,
        "CREATE TABLE rls_table (
            id SERIAL PRIMARY KEY,
            user_id INTEGER,
            data TEXT
        );",
    )
    .await?;

    // Enable RLS
    execute_sql(
        &connection,
        "ALTER TABLE rls_table ENABLE ROW LEVEL SECURITY;",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the RLS table was introspected
    let table = schema.tables.get("rls_table");
    assert!(table.is_some(), "RLS table should be introspected");

    let rls_table = table.unwrap();
    assert_eq!(rls_table.name, "rls_table");
    assert_eq!(rls_table.columns.len(), 3, "Table should have 3 columns");

    // Note: RLS status might be stored in storage_parameters or as a separate field
    // This depends on how the introspection is implemented

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_table_with_column_comments() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create table
    execute_sql(
        &connection,
        "CREATE TABLE comment_columns_table (
            id SERIAL PRIMARY KEY,
            name TEXT,
            email TEXT
        );",
    )
    .await?;

    // Add column comments
    execute_sql(
        &connection,
        "COMMENT ON COLUMN comment_columns_table.id IS 'Primary key identifier';",
    )
    .await?;
    execute_sql(
        &connection,
        "COMMENT ON COLUMN comment_columns_table.name IS 'User full name';",
    )
    .await?;
    execute_sql(
        &connection,
        "COMMENT ON COLUMN comment_columns_table.email IS 'User email address';",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the table was introspected with column comments
    let table = schema.tables.get("comment_columns_table");
    assert!(table.is_some(), "Table with column comments should be introspected");

    let tbl = table.unwrap();
    assert_eq!(tbl.name, "comment_columns_table");

    // Verify column comments
    let id_column = tbl.columns.iter().find(|c| c.name == "id").unwrap();
    assert_eq!(
        id_column.comment,
        Some("Primary key identifier".to_string()),
        "ID column should have comment"
    );

    let name_column = tbl.columns.iter().find(|c| c.name == "name").unwrap();
    assert_eq!(
        name_column.comment,
        Some("User full name".to_string()),
        "Name column should have comment"
    );

    let email_column = tbl.columns.iter().find(|c| c.name == "email").unwrap();
    assert_eq!(
        email_column.comment,
        Some("User email address".to_string()),
        "Email column should have comment"
    );

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_table_with_composite_primary_key() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create table with composite primary key
    execute_sql(
        &connection,
        "CREATE TABLE composite_pk_table (
            user_id INTEGER,
            role_id INTEGER,
            assigned_date DATE,
            PRIMARY KEY (user_id, role_id)
        );",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the table was introspected
    let table = schema.tables.get("composite_pk_table");
    assert!(table.is_some(), "Table with composite PK should be introspected");

    let tbl = table.unwrap();
    assert_eq!(tbl.name, "composite_pk_table");
    assert_eq!(tbl.columns.len(), 3, "Table should have 3 columns");

    // Verify composite primary key constraint
    let pk_constraints: Vec<_> = tbl.constraints.iter()
        .filter(|c| matches!(c.kind, shem_core::ConstraintKind::PrimaryKey))
        .collect();
    
    assert!(!pk_constraints.is_empty(), "Table should have primary key constraint");
    
    let pk = pk_constraints[0];
    assert!(matches!(pk.kind, shem_core::ConstraintKind::PrimaryKey), "Constraint should be primary key");
    assert!(pk.definition.contains("user_id"), "PK definition should contain user_id");
    assert!(pk.definition.contains("role_id"), "PK definition should contain role_id");

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_table_with_exclusion_constraint() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create table with exclusion constraint
    execute_sql(
        &connection,
        "CREATE TABLE exclusion_table (
            id SERIAL PRIMARY KEY,
            period tsrange,
            EXCLUDE USING gist (period WITH &&)
        );",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the table was introspected
    let table = schema.tables.get("exclusion_table");
    assert!(table.is_some(), "Table with exclusion constraint should be introspected");

    let tbl = table.unwrap();
    assert_eq!(tbl.name, "exclusion_table");
    assert_eq!(tbl.columns.len(), 2, "Table should have 2 columns");

    // Verify exclusion constraint
    let exclusion_constraints: Vec<_> = tbl.constraints.iter()
        .filter(|c| matches!(c.kind, shem_core::ConstraintKind::Exclusion))
        .collect();
    
    assert!(!exclusion_constraints.is_empty(), "Table should have exclusion constraint");
    
    let exclusion = exclusion_constraints[0];
    assert!(matches!(exclusion.kind, shem_core::ConstraintKind::Exclusion), "Constraint should be exclusion");
    assert!(exclusion.definition.contains("EXCLUDE"), "Should contain EXCLUDE definition");

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_table_with_custom_types() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create custom enum type
    execute_sql(
        &connection,
        "CREATE TYPE status_enum AS ENUM ('active', 'inactive', 'pending');",
    )
    .await?;

    // Create custom composite type
    execute_sql(
        &connection,
        "CREATE TYPE address_type AS (
            street TEXT,
            city TEXT,
            country TEXT
        );",
    )
    .await?;

    // Create table with custom types
    execute_sql(
        &connection,
        "CREATE TABLE custom_types_table (
            id SERIAL PRIMARY KEY,
            status status_enum,
            address address_type,
            tags TEXT[]
        );",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the table was introspected
    let table = schema.tables.get("custom_types_table");
    assert!(table.is_some(), "Table with custom types should be introspected");

    let tbl = table.unwrap();
    assert_eq!(tbl.name, "custom_types_table");
    assert_eq!(tbl.columns.len(), 4, "Table should have 4 columns");

    // Verify custom type columns
    let status_column = tbl.columns.iter().find(|c| c.name == "status").unwrap();
    assert_eq!(status_column.type_name, "status_enum", "Status column should have custom enum type");

    let address_column = tbl.columns.iter().find(|c| c.name == "address").unwrap();
    assert_eq!(address_column.type_name, "address_type", "Address column should have custom composite type");

    let tags_column = tbl.columns.iter().find(|c| c.name == "tags").unwrap();
    assert_eq!(tags_column.type_name, "text[]", "Tags column should have array type");

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_table_with_complex_column_types() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create custom types for testing
    execute_sql(
        &connection,
        "CREATE TYPE user_status AS ENUM ('active', 'inactive');",
    )
    .await?;

    execute_sql(
        &connection,
        "CREATE TYPE address_composite AS (
            street TEXT,
            city TEXT,
            zip_code TEXT
        );",
    )
    .await?;

    execute_sql(
        &connection,
        "CREATE TYPE date_range AS RANGE (subtype = date);",
    )
    .await?;

    // Create table with multiple complex column types
    execute_sql(
        &connection,
        "CREATE TABLE complex_types_table (
            id SERIAL PRIMARY KEY,
            -- Base types
            name TEXT,
            age INTEGER,
            height NUMERIC(5,2),
            is_active BOOLEAN,
            created_at TIMESTAMP,
            
            -- Custom types
            status user_status,
            address address_composite,
            vacation_period date_range,
            
            -- Array types
            tags TEXT[],
            scores INTEGER[],
            flags BOOLEAN[],
            
            -- JSON types
            metadata JSON,
            config JSONB,
            
            -- Multirange types (PostgreSQL 14+)
            work_periods daterange[],
            
            -- Other complex types
            uuid_col UUID,
            binary_data BYTEA,
            xml_data XML,
            geometric_point POINT
        );",
    )
    .await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the table was introspected
    let table = schema.tables.get("complex_types_table");
    assert!(table.is_some(), "Table with complex types should be introspected");

    let tbl = table.unwrap();
    assert_eq!(tbl.name, "complex_types_table");
    assert_eq!(tbl.columns.len(), 19, "Table should have 19 columns");

    // Verify base types
    let name_column = tbl.columns.iter().find(|c| c.name == "name").unwrap();
    assert_eq!(name_column.type_name, "text", "Name should be text type");

    let age_column = tbl.columns.iter().find(|c| c.name == "age").unwrap();
    assert_eq!(age_column.type_name, "integer", "Age should be integer type");

    let height_column = tbl.columns.iter().find(|c| c.name == "height").unwrap();
    assert_eq!(height_column.type_name, "numeric(5,2)", "Height should be numeric(5,2) type");

    let is_active_column = tbl.columns.iter().find(|c| c.name == "is_active").unwrap();
    assert_eq!(is_active_column.type_name, "boolean", "Is_active should be boolean type");

    // Verify custom types
    let status_column = tbl.columns.iter().find(|c| c.name == "status").unwrap();
    assert_eq!(status_column.type_name, "user_status", "Status should be custom enum type");

    let address_column = tbl.columns.iter().find(|c| c.name == "address").unwrap();
    assert_eq!(address_column.type_name, "address_composite", "Address should be custom composite type");

    let vacation_period_column = tbl.columns.iter().find(|c| c.name == "vacation_period").unwrap();
    assert_eq!(vacation_period_column.type_name, "date_range", "Vacation period should be custom range type");

    // Verify array types
    let tags_column = tbl.columns.iter().find(|c| c.name == "tags").unwrap();
    assert_eq!(tags_column.type_name, "text[]", "Tags should be text array type");

    let scores_column = tbl.columns.iter().find(|c| c.name == "scores").unwrap();
    assert_eq!(scores_column.type_name, "integer[]", "Scores should be integer array type");

    let flags_column = tbl.columns.iter().find(|c| c.name == "flags").unwrap();
    assert_eq!(flags_column.type_name, "boolean[]", "Flags should be boolean array type");

    // Verify JSON types
    let metadata_column = tbl.columns.iter().find(|c| c.name == "metadata").unwrap();
    assert_eq!(metadata_column.type_name, "json", "Metadata should be JSON type");

    let config_column = tbl.columns.iter().find(|c| c.name == "config").unwrap();
    assert_eq!(config_column.type_name, "jsonb", "Config should be JSONB type");

    // Verify other complex types
    let uuid_column = tbl.columns.iter().find(|c| c.name == "uuid_col").unwrap();
    assert_eq!(uuid_column.type_name, "uuid", "UUID column should be UUID type");

    let binary_column = tbl.columns.iter().find(|c| c.name == "binary_data").unwrap();
    assert_eq!(binary_column.type_name, "bytea", "Binary data should be BYTEA type");

    let xml_column = tbl.columns.iter().find(|c| c.name == "xml_data").unwrap();
    assert_eq!(xml_column.type_name, "xml", "XML data should be XML type");

    let point_column = tbl.columns.iter().find(|c| c.name == "geometric_point").unwrap();
    assert_eq!(point_column.type_name, "point", "Geometric point should be POINT type");

    // Clean up
    db.cleanup().await?;
    Ok(())
} 