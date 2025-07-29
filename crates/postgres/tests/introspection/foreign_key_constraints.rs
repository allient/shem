use postgres::TestDb;
use shem_core::DatabaseConnection;
use tracing::debug;

#[tokio::test]
async fn test_introspect_foreign_key_constraints() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create tables with foreign key constraints
    connection.execute("CREATE TABLE parent_table (id SERIAL PRIMARY KEY, name TEXT);").await?;
    connection.execute("CREATE TABLE child_table (id SERIAL PRIMARY KEY, parent_id INTEGER REFERENCES parent_table(id) ON DELETE CASCADE, description TEXT);").await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the foreign key constraint was introspected
    let constraints: Vec<_> = schema.constraints.values().collect();
    debug!("Constraints: {:?}", constraints);
    
    let fk_constraint = constraints.iter()
        .find(|c| c.definition.contains("FOREIGN KEY"))
        .expect("Should find foreign key constraint");
    
    assert!(fk_constraint.definition.contains("FOREIGN KEY"));
    assert!(fk_constraint.definition.contains("parent_table"));
    assert!(fk_constraint.definition.contains("ON DELETE CASCADE"));
    
    Ok(())
} 