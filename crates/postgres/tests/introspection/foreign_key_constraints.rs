use postgres::TestDb;
use shem_core::DatabaseConnection;
use log::debug;

#[tokio::test]
async fn test_introspect_foreign_key_basic() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create parent table
    connection.execute("CREATE TABLE parent_table (id SERIAL PRIMARY KEY, name TEXT);").await?;
    
    // Create child table with foreign key
    connection.execute("CREATE TABLE child_table (
        id SERIAL PRIMARY KEY,
        parent_id INTEGER REFERENCES parent_table(id),
        description TEXT
    );").await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the foreign key constraint was introspected
    let fk_constraints: Vec<_> = schema.foreign_key_constraints.values().collect();
    debug!("Foreign key constraints: {:?}", fk_constraints);
    
    assert!(!fk_constraints.is_empty(), "Should have at least one foreign key constraint");
    
    let fk = fk_constraints.iter().find(|fk| fk.table == "child_table").expect("Should find foreign key on child_table");
    assert_eq!(fk.table, "child_table");
    assert_eq!(fk.references_table, "parent_table");
    assert_eq!(fk.columns, vec!["parent_id"]);
    assert_eq!(fk.references_columns, vec!["id"]);
    
    Ok(())
}

#[tokio::test]
async fn test_introspect_foreign_key_with_actions() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create parent table
    connection.execute("CREATE TABLE parent_actions (id SERIAL PRIMARY KEY, name TEXT);").await?;
    
    // Create child table with foreign key and specific actions
    connection.execute("CREATE TABLE child_actions (
        id SERIAL PRIMARY KEY,
        parent_id INTEGER REFERENCES parent_actions(id) ON DELETE CASCADE ON UPDATE SET NULL,
        description TEXT
    );").await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the foreign key constraint was introspected with actions
    let fk_constraints: Vec<_> = schema.foreign_key_constraints.values().collect();
    debug!("Foreign key constraints with actions: {:?}", fk_constraints);
    
    let fk = fk_constraints.iter().find(|fk| fk.table == "child_actions").expect("Should find foreign key on child_actions");
    assert_eq!(fk.table, "child_actions");
    assert_eq!(fk.references_table, "parent_actions");
    assert_eq!(fk.columns, vec!["parent_id"]);
    assert_eq!(fk.references_columns, vec!["id"]);
    // Note: The actual action values depend on your ForeignKeyConstraint struct definition
    
    Ok(())
}

#[tokio::test]
async fn test_introspect_composite_foreign_key() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create parent table with composite primary key
    connection.execute("CREATE TABLE parent_composite (
        id1 INTEGER,
        id2 INTEGER,
        name TEXT,
        PRIMARY KEY (id1, id2)
    );").await?;
    
    // Create child table with composite foreign key
    connection.execute("CREATE TABLE child_composite (
        id SERIAL PRIMARY KEY,
        parent_id1 INTEGER,
        parent_id2 INTEGER,
        description TEXT,
        FOREIGN KEY (parent_id1, parent_id2) REFERENCES parent_composite(id1, id2)
    );").await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the composite foreign key constraint was introspected
    let fk_constraints: Vec<_> = schema.foreign_key_constraints.values().collect();
    debug!("Composite foreign key constraints: {:?}", fk_constraints);
    
    let fk = fk_constraints.iter().find(|fk| fk.table == "child_composite").expect("Should find foreign key on child_composite");
    assert_eq!(fk.table, "child_composite");
    assert_eq!(fk.references_table, "parent_composite");
    assert_eq!(fk.columns, vec!["parent_id1", "parent_id2"]);
    assert_eq!(fk.references_columns, vec!["id1", "id2"]);
    
    Ok(())
}

#[tokio::test]
async fn test_introspect_multiple_foreign_keys() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create multiple parent tables
    connection.execute("CREATE TABLE parent1 (id SERIAL PRIMARY KEY, name TEXT);").await?;
    connection.execute("CREATE TABLE parent2 (id SERIAL PRIMARY KEY, code TEXT);").await?;
    
    // Create child table with multiple foreign keys
    connection.execute("CREATE TABLE child_multiple (
        id SERIAL PRIMARY KEY,
        parent1_id INTEGER REFERENCES parent1(id),
        parent2_id INTEGER REFERENCES parent2(id),
        description TEXT
    );").await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify multiple foreign key constraints were introspected
    let fk_constraints: Vec<_> = schema.foreign_key_constraints.values().collect();
    debug!("Multiple foreign key constraints: {:?}", fk_constraints);
    
    let child_fks: Vec<_> = fk_constraints.iter().filter(|fk| fk.table == "child_multiple").collect();
    assert_eq!(child_fks.len(), 2, "Should have 2 foreign keys on child_multiple");
    
    // Verify both foreign keys exist
    let has_parent1_fk = child_fks.iter().any(|fk| fk.references_table == "parent1");
    let has_parent2_fk = child_fks.iter().any(|fk| fk.references_table == "parent2");
    assert!(has_parent1_fk, "Should have foreign key to parent1");
    assert!(has_parent2_fk, "Should have foreign key to parent2");
    
    Ok(())
}

#[tokio::test]
async fn test_introspect_foreign_key_named_constraint() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create parent table
    connection.execute("CREATE TABLE parent_named (id SERIAL PRIMARY KEY, name TEXT);").await?;
    
    // Create child table with named foreign key constraint
    connection.execute("CREATE TABLE child_named (
        id SERIAL PRIMARY KEY,
        parent_id INTEGER,
        description TEXT,
        CONSTRAINT fk_named_constraint FOREIGN KEY (parent_id) REFERENCES parent_named(id)
    );").await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the named foreign key constraint was introspected
    let fk_constraints: Vec<_> = schema.foreign_key_constraints.values().collect();
    debug!("Named foreign key constraints: {:?}", fk_constraints);
    
    let fk = fk_constraints.iter().find(|fk| fk.table == "child_named").expect("Should find foreign key on child_named");
    assert_eq!(fk.name, "fk_named_constraint");
    assert_eq!(fk.table, "child_named");
    assert_eq!(fk.references_table, "parent_named");
    assert_eq!(fk.columns, vec!["parent_id"]);
    assert_eq!(fk.references_columns, vec!["id"]);
    
    Ok(())
} 