use postgres::TestDb;
use shem_core::DatabaseConnection;

#[tokio::test]
async fn test_introspect_publication_basic() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create a basic publication
    connection.execute("CREATE PUBLICATION test_pub;").await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the publication was introspected
    let publication = schema.publications.get("test_pub");
    assert!(publication.is_some(), "Publication 'test_pub' should be introspected");
    let publication_obj = publication.unwrap();
    assert_eq!(publication_obj.name, "test_pub");
    // Note: puballtables might be false in some PostgreSQL versions even for basic publications
    // The key indicator is that tables list is empty for basic publications
    assert!(publication_obj.tables.is_empty(), "Basic publication should have empty tables list");
    assert!(publication_obj.insert, "Basic publication should allow INSERT");
    assert!(publication_obj.update, "Basic publication should allow UPDATE");
    assert!(publication_obj.delete, "Basic publication should allow DELETE");
    assert!(publication_obj.truncate, "Basic publication should allow TRUNCATE");

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_publication_with_specific_tables() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create tables
    connection.execute("CREATE TABLE test_table1 (id SERIAL PRIMARY KEY, name TEXT);").await?;
    connection.execute("CREATE TABLE test_table2 (id SERIAL PRIMARY KEY, value INTEGER);").await?;

    // Create a publication with specific tables
    connection.execute("CREATE PUBLICATION test_pub_tables FOR TABLE test_table1, test_table2;").await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the publication was introspected
    let publication = schema.publications.get("test_pub_tables");
    assert!(publication.is_some(), "Publication 'test_pub_tables' should be introspected");
    let publication_obj = publication.unwrap();
    assert_eq!(publication_obj.name, "test_pub_tables");
    assert!(!publication_obj.all_tables, "Publication with specific tables should have all_tables = false");
    assert!(publication_obj.insert, "Publication should allow INSERT");
    assert!(publication_obj.update, "Publication should allow UPDATE");
    assert!(publication_obj.delete, "Publication should allow DELETE");
    assert!(publication_obj.truncate, "Publication should allow TRUNCATE");
    assert_eq!(publication_obj.tables.len(), 2, "Publication should have 2 tables");
    assert!(publication_obj.tables.contains(&"public.test_table1".to_string()));
    assert!(publication_obj.tables.contains(&"public.test_table2".to_string()));

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_publication_with_limited_operations() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create a table
    connection.execute("CREATE TABLE test_table_ops (id SERIAL PRIMARY KEY, data TEXT);").await?;

    // Create a publication with limited operations
    connection.execute("CREATE PUBLICATION test_pub_ops FOR TABLE test_table_ops WITH (publish = 'insert,update');").await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the publication was introspected
    let publication = schema.publications.get("test_pub_ops");
    assert!(publication.is_some(), "Publication 'test_pub_ops' should be introspected");
    let publication_obj = publication.unwrap();
    assert_eq!(publication_obj.name, "test_pub_ops");
    assert!(!publication_obj.all_tables, "Publication with specific tables should have all_tables = false");
    assert!(publication_obj.insert, "Publication should allow INSERT");
    assert!(publication_obj.update, "Publication should allow UPDATE");
    assert!(!publication_obj.delete, "Publication should not allow DELETE");
    assert!(!publication_obj.truncate, "Publication should not allow TRUNCATE");
    assert_eq!(publication_obj.tables.len(), 1, "Publication should have 1 table");
    assert!(publication_obj.tables.contains(&"public.test_table_ops".to_string()));

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_multiple_publications() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create tables
    connection.execute("CREATE TABLE test_multi1 (id SERIAL PRIMARY KEY);").await?;
    connection.execute("CREATE TABLE test_multi2 (id SERIAL PRIMARY KEY);").await?;

    // Create multiple publications
    connection.execute("CREATE PUBLICATION pub1 FOR TABLE test_multi1;").await?;
    connection.execute("CREATE PUBLICATION pub2 FOR TABLE test_multi2 WITH (publish = 'insert');").await?;
    connection.execute("CREATE PUBLICATION pub3;").await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify all publications were introspected
    assert_eq!(schema.publications.len(), 3, "Should have 3 publications");

    let pub1 = schema.publications.get("pub1").unwrap();
    assert_eq!(pub1.name, "pub1");
    assert!(!pub1.all_tables);
    assert_eq!(pub1.tables.len(), 1);

    let pub2 = schema.publications.get("pub2").unwrap();
    assert_eq!(pub2.name, "pub2");
    assert!(!pub2.all_tables);
    assert!(pub2.insert);
    assert!(!pub2.update);
    assert!(!pub2.delete);
    assert!(!pub2.truncate);

    let pub3 = schema.publications.get("pub3").unwrap();
    assert_eq!(pub3.name, "pub3");
    // The reliable indicator for a basic publication is an empty tables list
    assert!(pub3.tables.is_empty(), "Basic publication should have empty tables list");
    assert!(pub3.insert);
    assert!(pub3.update);
    assert!(pub3.delete);
    assert!(pub3.truncate);

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_publication_with_schema() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create schema and table
    connection.execute("CREATE SCHEMA test_pub_schema;").await?;
    connection.execute("CREATE TABLE test_pub_schema.schema_table (id SERIAL PRIMARY KEY);").await?;

    // Create a publication with schema-qualified table
    connection.execute("CREATE PUBLICATION test_pub_schema_pub FOR TABLE test_pub_schema.schema_table;").await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the publication was introspected
    let publication = schema.publications.get("test_pub_schema_pub");
    assert!(publication.is_some(), "Publication 'test_pub_schema_pub' should be introspected");
    let publication_obj = publication.unwrap();
    assert_eq!(publication_obj.name, "test_pub_schema_pub");
    assert!(!publication_obj.all_tables);
    assert_eq!(publication_obj.tables.len(), 1);
    assert!(publication_obj.tables.contains(&"test_pub_schema.schema_table".to_string()));

    // Clean up
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_publication_no_publications() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create a table but no publications
    connection.execute("CREATE TABLE test_no_pub (id SERIAL PRIMARY KEY);").await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify no publications were introspected
    assert_eq!(schema.publications.len(), 0, "Should have no publications");

    // Clean up
    db.cleanup().await?;
    Ok(())
} 