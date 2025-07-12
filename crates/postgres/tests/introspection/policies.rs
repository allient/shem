use postgres::TestDb;
use shem_core::DatabaseConnection;
use shem_core::schema::PolicyCommand;
use tracing::debug;

/// Test helper function to execute SQL on the test database
async fn execute_sql(
    connection: &Box<dyn DatabaseConnection>,
    sql: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    connection.execute(sql).await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_basic_policy() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create table and enable RLS
    execute_sql(&connection, "CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT);").await?;
    execute_sql(&connection, "ALTER TABLE users ENABLE ROW LEVEL SECURITY;").await?;
    // Add a basic SELECT policy
    execute_sql(&connection, "CREATE POLICY select_policy ON users FOR SELECT USING (true);").await?;

    // Introspect the database
    let schema = connection.introspect().await?;
    debug!("Introspected policies: {:?}", schema.policies);

    // Verify the policy exists
    let policy = schema.policies.get("select_policy").expect("Policy should exist");
    assert_eq!(policy.name, "select_policy");
    assert_eq!(policy.table, "users");
    assert_eq!(policy.command, PolicyCommand::Select);
    assert!(policy.permissive); // Default is permissive
    assert_eq!(policy.using.as_deref(), Some("true"));

    Ok(())
}

#[tokio::test]
async fn test_introspect_policy_with_schema() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create schema, table and enable RLS
    execute_sql(&connection, "CREATE SCHEMA app_schema;").await?;
    execute_sql(&connection, "CREATE TABLE app_schema.products (id SERIAL PRIMARY KEY, name TEXT);").await?;
    execute_sql(&connection, "ALTER TABLE app_schema.products ENABLE ROW LEVEL SECURITY;").await?;
    execute_sql(&connection, "CREATE POLICY product_policy ON app_schema.products FOR SELECT USING (id > 0);").await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the policy exists in the schema
    let policy = schema.policies.get("product_policy").expect("Policy should exist");
    assert_eq!(policy.name, "product_policy");
    assert_eq!(policy.table, "products");
    assert_eq!(policy.schema, Some("app_schema".to_string()));
    assert_eq!(policy.command, PolicyCommand::Select);
    assert_eq!(policy.using.as_deref(), Some("(id > 0)"));

    Ok(())
}

#[tokio::test]
async fn test_introspect_policy_with_roles() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create table and enable RLS
    execute_sql(&connection, "CREATE TABLE documents (id SERIAL PRIMARY KEY, content TEXT);").await?;
    execute_sql(&connection, "ALTER TABLE documents ENABLE ROW LEVEL SECURITY;").await?;
    execute_sql(&connection, "CREATE POLICY admin_policy ON documents FOR ALL TO postgres USING (true);").await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the policy exists with roles
    let policy = schema.policies.get("admin_policy").expect("Policy should exist");
    assert_eq!(policy.name, "admin_policy");
    assert_eq!(policy.table, "documents");
    assert_eq!(policy.command, PolicyCommand::All);
    assert_eq!(policy.roles, vec!["postgres"]);
    assert_eq!(policy.using.as_deref(), Some("true"));

    Ok(())
}

#[tokio::test]
async fn test_introspect_policy_with_check() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create table and enable RLS
    execute_sql(&connection, "CREATE TABLE orders (id SERIAL PRIMARY KEY, amount DECIMAL(10,2));").await?;
    execute_sql(&connection, "ALTER TABLE orders ENABLE ROW LEVEL SECURITY;").await?;
    execute_sql(&connection, "CREATE POLICY insert_policy ON orders FOR INSERT WITH CHECK (amount > 0);").await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the policy exists with check condition
    let policy = schema.policies.get("insert_policy").expect("Policy should exist");
    assert_eq!(policy.name, "insert_policy");
    assert_eq!(policy.table, "orders");
    assert_eq!(policy.command, PolicyCommand::Insert);
    // Check for substrings to handle PostgreSQL's normalization
    let check_condition = policy.check.as_deref().unwrap();
    assert!(check_condition.contains("amount >"));
    assert!(check_condition.contains("0"));

    Ok(())
}

#[tokio::test]
async fn test_introspect_restrictive_policy() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create table and enable RLS
    execute_sql(&connection, "CREATE TABLE sensitive_data (id SERIAL PRIMARY KEY, data TEXT);").await?;
    execute_sql(&connection, "ALTER TABLE sensitive_data ENABLE ROW LEVEL SECURITY;").await?;
    execute_sql(&connection, "CREATE POLICY restrictive_policy ON sensitive_data AS RESTRICTIVE FOR SELECT USING (false);").await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the restrictive policy exists
    let policy = schema.policies.get("restrictive_policy").expect("Policy should exist");
    assert_eq!(policy.name, "restrictive_policy");
    assert_eq!(policy.table, "sensitive_data");
    assert_eq!(policy.command, PolicyCommand::Select);
    assert!(!policy.permissive); // Should be restrictive
    assert_eq!(policy.using.as_deref(), Some("false"));

    Ok(())
}

#[tokio::test]
async fn test_introspect_multiple_policies() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create table and enable RLS
    execute_sql(&connection, "CREATE TABLE employees (id SERIAL PRIMARY KEY, name TEXT, department TEXT);").await?;
    execute_sql(&connection, "ALTER TABLE employees ENABLE ROW LEVEL SECURITY;").await?;
    
    // Add multiple policies
    execute_sql(&connection, "CREATE POLICY select_employees ON employees FOR SELECT USING (department = 'IT');").await?;
    execute_sql(&connection, "CREATE POLICY insert_employees ON employees FOR INSERT WITH CHECK (name IS NOT NULL);").await?;
    execute_sql(&connection, "CREATE POLICY update_employees ON employees FOR UPDATE USING (id > 0) WITH CHECK (department IS NOT NULL);").await?;
    execute_sql(&connection, "CREATE POLICY delete_employees ON employees FOR DELETE USING (id > 100);").await?;

    // Introspect the database
    let schema = connection.introspect().await?;
    debug!("All introspected policies: {:?}", schema.policies);

    // Verify all policies exist
    assert_eq!(schema.policies.len(), 4);

    // Check select policy
    let select_policy = schema.policies.get("select_employees").expect("Select policy should exist");
    assert_eq!(select_policy.command, PolicyCommand::Select);
    assert_eq!(select_policy.using.as_deref(), Some("(department = 'IT'::text)"));

    // Check insert policy
    let insert_policy = schema.policies.get("insert_employees").expect("Insert policy should exist");
    assert_eq!(insert_policy.command, PolicyCommand::Insert);
    assert_eq!(insert_policy.check.as_deref(), Some("(name IS NOT NULL)"));

    // Check update policy
    let update_policy = schema.policies.get("update_employees").expect("Update policy should exist");
    assert_eq!(update_policy.command, PolicyCommand::Update);
    assert_eq!(update_policy.using.as_deref(), Some("(id > 0)"));
    assert_eq!(update_policy.check.as_deref(), Some("(department IS NOT NULL)"));

    // Check delete policy
    let delete_policy = schema.policies.get("delete_employees").expect("Delete policy should exist");
    assert_eq!(delete_policy.command, PolicyCommand::Delete);
    assert_eq!(delete_policy.using.as_deref(), Some("(id > 100)"));

    Ok(())
}

#[tokio::test]
async fn test_introspect_policy_with_complex_conditions() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create table and enable RLS
    execute_sql(&connection, "CREATE TABLE logs (id SERIAL PRIMARY KEY, user_id INTEGER, action TEXT, created_at TIMESTAMP);").await?;
    execute_sql(&connection, "ALTER TABLE logs ENABLE ROW LEVEL SECURITY;").await?;
    execute_sql(&connection, "CREATE POLICY user_logs ON logs FOR SELECT USING (user_id = current_setting('app.user_id')::integer AND created_at > NOW() - INTERVAL '1 day');").await?;

    // Introspect the database
    let schema = connection.introspect().await?;

    // Verify the policy with complex condition exists
    let policy = schema.policies.get("user_logs").expect("Policy should exist");
    assert_eq!(policy.name, "user_logs");
    assert_eq!(policy.table, "logs");
    assert_eq!(policy.command, PolicyCommand::Select);
    let using_condition = policy.using.as_deref().unwrap();
    assert!(using_condition.contains("user_id = (current_setting('app.user_id'::text))::integer"));
    assert!(using_condition.contains("created_at > (now() - '1 day'::interval)"));

    Ok(())
}

#[tokio::test]
async fn test_introspect_policy_consistency() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::try_init().ok();
    let db = TestDb::new().await?;
    let connection = &db.conn;

    // Create table and enable RLS
    execute_sql(&connection, "CREATE TABLE test_table (id SERIAL PRIMARY KEY, value TEXT);").await?;
    execute_sql(&connection, "ALTER TABLE test_table ENABLE ROW LEVEL SECURITY;").await?;
    execute_sql(&connection, "CREATE POLICY test_policy ON test_table FOR SELECT USING (id > 0);").await?;

    // Introspect multiple times to ensure consistency
    let schema1 = connection.introspect().await?;
    let schema2 = connection.introspect().await?;

    let policy1 = schema1.policies.get("test_policy").unwrap();
    let policy2 = schema2.policies.get("test_policy").unwrap();

    // Verify consistency
    assert_eq!(policy1.name, policy2.name);
    assert_eq!(policy1.table, policy2.table);
    assert_eq!(policy1.schema, policy2.schema);
    assert_eq!(policy1.command, policy2.command);
    assert_eq!(policy1.permissive, policy2.permissive);
    assert_eq!(policy1.using, policy2.using);
    assert_eq!(policy1.check, policy2.check);
    assert_eq!(policy1.roles, policy2.roles);

    Ok(())
} 