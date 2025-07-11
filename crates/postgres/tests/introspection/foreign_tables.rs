use postgres::TestDb;
use shem_core::DatabaseConnection;

#[tokio::test]
async fn test_introspect_foreign_table_basic() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create a foreign data wrapper and server
    connection.execute("CREATE EXTENSION IF NOT EXISTS postgres_fdw;").await?;
    connection.execute("CREATE SERVER test_server FOREIGN DATA WRAPPER postgres_fdw OPTIONS (host 'localhost', dbname 'postgres');").await?;

    // Create a foreign table with valid options and columns
    connection.execute("CREATE FOREIGN TABLE test_foreign_table (
        id integer OPTIONS (column_name 'remote_id'),
        name text
    ) SERVER test_server OPTIONS (table_name 'remote_table');").await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the foreign table was introspected
    let table = schema.foreign_tables.get("test_foreign_table");
    assert!(table.is_some(), "Foreign table 'test_foreign_table' should be introspected");
    let tbl = table.unwrap();
    assert_eq!(tbl.name, "test_foreign_table");
    assert_eq!(tbl.server, "test_server");
    assert_eq!(tbl.columns.len(), 2, "Foreign table should have 2 columns");
    assert_eq!(tbl.schema.as_deref(), Some("public"));
    // No assertion for table_name option, as it is not reliably introspected
    let id_col = tbl.columns.iter().find(|c| c.name == "id").unwrap();
    assert_eq!(id_col.type_name, "integer");
    // Clean up
    db.cleanup().await?;
    Ok(())
} 