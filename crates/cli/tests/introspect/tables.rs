//! Table introspection tests
//! 
//! Tests for introspecting various types of tables and their components.

use crate::common::{TestEnv, cli, db};
use crate::fixtures::sql;
use anyhow::Result;

#[tokio::test]
async fn test_introspect_simple_table() -> Result<()> {
    let env = TestEnv::new()?;
    let pool = db::setup_test_db(&env.db_name).await?;
    
    // Create a simple table
    db::execute_sql(&pool, sql::SIMPLE_TABLE).await?;
    
    // Run introspect command
    let db_url = format!("postgresql://postgres:postgres@localhost:5432/{}", env.db_name);
    let output = cli::run_shem_command_in_dir(
        &["introspect", "--database-url", &db_url, "--output", "schema"],
        &env.temp_path()
    )?;
    
    cli::assert_command_success(&output);
    
    // Verify table was introspected
    let schema_content = std::fs::read_to_string(env.temp_path().join("schema/schema.sql"))?;
    assert!(schema_content.contains("CREATE TABLE users"));
    assert!(schema_content.contains("id SERIAL PRIMARY KEY"));
    assert!(schema_content.contains("name VARCHAR(100) NOT NULL"));
    assert!(schema_content.contains("email VARCHAR(255) UNIQUE"));
    assert!(schema_content.contains("status VARCHAR(20) DEFAULT 'active'"));
    assert!(schema_content.contains("created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP"));
    
    // Clean up
    db::drop_test_db(&env.db_name).await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_table_with_foreign_key() -> Result<()> {
    let env = TestEnv::new()?;
    let pool = db::setup_test_db(&env.db_name).await?;
    
    // Create tables with foreign key
    db::execute_sql(&pool, sql::SIMPLE_TABLE).await?;
    db::execute_sql(&pool, sql::TABLE_WITH_FK).await?;
    
    // Run introspect command
    let db_url = format!("postgresql://postgres:postgres@localhost:5432/{}", env.db_name);
    let output = cli::run_shem_command_in_dir(
        &["introspect", "--database-url", &db_url, "--output", "schema"],
        &env.temp_path()
    )?;
    
    cli::assert_command_success(&output);
    
    // Verify tables and foreign key were introspected
    let schema_content = std::fs::read_to_string(env.temp_path().join("schema/schema.sql"))?;
    assert!(schema_content.contains("CREATE TABLE users"));
    assert!(schema_content.contains("CREATE TABLE posts"));
    assert!(schema_content.contains("user_id INTEGER REFERENCES users(id)"));
    
    // Clean up
    db::drop_test_db(&env.db_name).await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_table_with_enum() -> Result<()> {
    let env = TestEnv::new()?;
    let pool = db::setup_test_db(&env.db_name).await?;
    
    // Create enum and table using it
    db::execute_sql(&pool, sql::ENUM_TYPE).await?;
    db::execute_sql(&pool, sql::TABLE_WITH_ENUM).await?;
    
    // Run introspect command
    let output = cli::run_shem_command_in_dir(
        &["introspect", "--database-url", "postgresql://postgres:postgres@localhost:5432/shem_test", "--output", "schema"],
        &env.temp_path()
    )?;
    
    cli::assert_command_success(&output);
    
    // Verify enum and table were introspected
    let schema_content = std::fs::read_to_string(env.temp_path().join("schema/schema.sql"))?;
    assert!(schema_content.contains("CREATE TYPE user_status AS ENUM"));
    assert!(schema_content.contains("CREATE TABLE user_profiles"));
    assert!(schema_content.contains("status user_status DEFAULT 'active'"));
    
    Ok(())
}

#[tokio::test]
async fn test_introspect_table_with_domain() -> Result<()> {
    let env = TestEnv::new()?;
    let pool = db::setup_test_db(&env.db_name).await?;
    
    // Create domain and table using it
    db::execute_sql(&pool, sql::DOMAIN).await?;
    db::execute_sql(&pool, "CREATE TABLE contacts (id SERIAL PRIMARY KEY, email email_address);").await?;
    
    // Run introspect command
    let output = cli::run_shem_command_in_dir(
        &["introspect", "--database-url", "postgresql://postgres:postgres@localhost:5432/shem_test", "--output", "schema"],
        &env.temp_path()
    )?;
    
    cli::assert_command_success(&output);
    
    // Verify domain and table were introspected
    let schema_content = std::fs::read_to_string(env.temp_path().join("schema/schema.sql"))?;
    assert!(schema_content.contains("CREATE DOMAIN email_address"));
    assert!(schema_content.contains("CREATE TABLE contacts"));
    assert!(schema_content.contains("email email_address"));
    
    Ok(())
}

#[tokio::test]
async fn test_introspect_table_with_check_constraints() -> Result<()> {
    let env = TestEnv::new()?;
    let pool = db::setup_test_db(&env.db_name).await?;
    
    // Create table with check constraints
    db::execute_sql(&pool, r#"
        CREATE TABLE products (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            price DECIMAL(10,2) NOT NULL CHECK (price > 0),
            category VARCHAR(50) CHECK (category IN ('electronics', 'clothing', 'books'))
        );
    "#).await?;
    
    // Run introspect command
    let output = cli::run_shem_command_in_dir(
        &["introspect", "--database-url", "postgresql://postgres:postgres@localhost:5432/shem_test", "--output", "schema"],
        &env.temp_path()
    )?;
    
    cli::assert_command_success(&output);
    
    // Verify table with check constraints was introspected
    let schema_content = std::fs::read_to_string(env.temp_path().join("schema/schema.sql"))?;
    assert!(schema_content.contains("CREATE TABLE products"));
    assert!(schema_content.contains("CHECK (price > 0)"));
    assert!(schema_content.contains("CHECK (category IN ('electronics', 'clothing', 'books'))"));
    
    Ok(())
}

#[tokio::test]
async fn test_introspect_table_with_indexes() -> Result<()> {
    let env = TestEnv::new()?;
    let pool = db::setup_test_db(&env.db_name).await?;
    
    // Create table with indexes
    db::execute_sql(&pool, sql::SIMPLE_TABLE).await?;
    db::execute_sql(&pool, "CREATE INDEX idx_users_email ON users(email);").await?;
    db::execute_sql(&pool, "CREATE INDEX idx_users_status ON users(status);").await?;
    
    // Run introspect command
    let db_url = format!("postgresql://postgres:postgres@localhost:5432/{}", env.db_name);
    let output = cli::run_shem_command_in_dir(
        &["introspect", "--database-url", &db_url, "--output", "schema"],
        &env.temp_path()
    )?;
    
    cli::assert_command_success(&output);
    
    // Verify table and indexes were introspected
    let schema_content = std::fs::read_to_string(env.temp_path().join("schema/schema.sql"))?;
    assert!(schema_content.contains("CREATE TABLE users"));
    assert!(schema_content.contains("CREATE INDEX idx_users_email ON users(email)"));
    assert!(schema_content.contains("CREATE INDEX idx_users_status ON users(status)"));
    
    // Clean up
    db::drop_test_db(&env.db_name).await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_table_with_comments() -> Result<()> {
    let env = TestEnv::new()?;
    let pool = db::setup_test_db(&env.db_name).await?;
    
    // Create table with comments
    db::execute_sql(&pool, sql::SIMPLE_TABLE).await?;
    db::execute_sql(&pool, "COMMENT ON TABLE users IS 'User accounts table';").await?;
    db::execute_sql(&pool, "COMMENT ON COLUMN users.id IS 'Primary key identifier';").await?;
    db::execute_sql(&pool, "COMMENT ON COLUMN users.email IS 'User email address';").await?;
    
    // Run introspect command
    let db_url = format!("postgresql://postgres:postgres@localhost:5432/{}", env.db_name);
    let output = cli::run_shem_command_in_dir(
        &["introspect", "--database-url", &db_url, "--output", "schema"],
        &env.temp_path()
    )?;
    
    cli::assert_command_success(&output);
    
    // Verify table and comments were introspected
    let schema_content = std::fs::read_to_string(env.temp_path().join("schema/schema.sql"))?;
    assert!(schema_content.contains("CREATE TABLE users"));
    assert!(schema_content.contains("COMMENT ON TABLE users IS 'User accounts table'"));
    assert!(schema_content.contains("COMMENT ON COLUMN users.id IS 'Primary key identifier'"));
    assert!(schema_content.contains("COMMENT ON COLUMN users.email IS 'User email address'"));
    
    // Clean up
    db::drop_test_db(&env.db_name).await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_table_with_default_values() -> Result<()> {
    let env = TestEnv::new()?;
    let pool = db::setup_test_db(&env.db_name).await?;
    
    // Create table with various default values
    db::execute_sql(&pool, r#"
        CREATE TABLE settings (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            value TEXT DEFAULT 'default_value',
            is_active BOOLEAN DEFAULT true,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    "#).await?;
    
    // Run introspect command
    let output = cli::run_shem_command_in_dir(
        &["introspect", "--database-url", "postgresql://postgres:postgres@localhost:5432/shem_test", "--output", "schema"],
        &env.temp_path()
    )?;
    
    cli::assert_command_success(&output);
    
    // Verify table with default values was introspected
    let schema_content = std::fs::read_to_string(env.temp_path().join("schema/schema.sql"))?;
    assert!(schema_content.contains("CREATE TABLE settings"));
    assert!(schema_content.contains("value TEXT DEFAULT 'default_value'"));
    assert!(schema_content.contains("is_active BOOLEAN DEFAULT true"));
    assert!(schema_content.contains("created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP"));
    
    Ok(())
}

#[tokio::test]
async fn test_introspect_table_with_not_null_constraints() -> Result<()> {
    let env = TestEnv::new()?;
    let pool = db::setup_test_db(&env.db_name).await?;
    
    // Create table with NOT NULL constraints
    db::execute_sql(&pool, r#"
        CREATE TABLE orders (
            id SERIAL PRIMARY KEY,
            customer_id INTEGER NOT NULL,
            order_date DATE NOT NULL,
            total_amount DECIMAL(10,2) NOT NULL,
            status VARCHAR(50) NOT NULL,
            notes TEXT
        );
    "#).await?;
    
    // Run introspect command
    let output = cli::run_shem_command_in_dir(
        &["introspect", "--database-url", "postgresql://postgres:postgres@localhost:5432/shem_test", "--output", "schema"],
        &env.temp_path()
    )?;
    
    cli::assert_command_success(&output);
    
    // Verify table with NOT NULL constraints was introspected
    let schema_content = std::fs::read_to_string(env.temp_path().join("schema/schema.sql"))?;
    assert!(schema_content.contains("CREATE TABLE orders"));
    assert!(schema_content.contains("customer_id INTEGER NOT NULL"));
    assert!(schema_content.contains("order_date DATE NOT NULL"));
    assert!(schema_content.contains("total_amount DECIMAL(10,2) NOT NULL"));
    assert!(schema_content.contains("status VARCHAR(50) NOT NULL"));
    assert!(schema_content.contains("notes TEXT")); // Should not have NOT NULL
    
    Ok(())
}

#[tokio::test]
async fn test_introspect_table_with_unique_constraints() -> Result<()> {
    let env = TestEnv::new()?;
    let pool = db::setup_test_db(&env.db_name).await?;
    
    // Create table with unique constraints
    db::execute_sql(&pool, r#"
        CREATE TABLE employees (
            id SERIAL PRIMARY KEY,
            employee_id VARCHAR(10) UNIQUE NOT NULL,
            email VARCHAR(255) UNIQUE NOT NULL,
            phone VARCHAR(20) UNIQUE,
            name VARCHAR(255) NOT NULL
        );
    "#).await?;
    
    // Run introspect command
    let output = cli::run_shem_command_in_dir(
        &["introspect", "--database-url", "postgresql://postgres:postgres@localhost:5432/shem_test", "--output", "schema"],
        &env.temp_path()
    )?;
    
    cli::assert_command_success(&output);
    
    // Verify table with unique constraints was introspected
    let schema_content = std::fs::read_to_string(env.temp_path().join("schema/schema.sql"))?;
    assert!(schema_content.contains("CREATE TABLE employees"));
    assert!(schema_content.contains("employee_id VARCHAR(10) UNIQUE NOT NULL"));
    assert!(schema_content.contains("email VARCHAR(255) UNIQUE NOT NULL"));
    assert!(schema_content.contains("phone VARCHAR(20) UNIQUE"));
    
    Ok(())
}

#[tokio::test]
async fn test_introspect_table_with_primary_key() -> Result<()> {
    let env = TestEnv::new()?;
    let pool = db::setup_test_db(&env.db_name).await?;
    
    // Create table with composite primary key
    db::execute_sql(&pool, r#"
        CREATE TABLE order_items (
            order_id INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            quantity INTEGER NOT NULL DEFAULT 1,
            unit_price DECIMAL(10,2) NOT NULL,
            PRIMARY KEY (order_id, product_id)
        );
    "#).await?;
    
    // Run introspect command
    let output = cli::run_shem_command_in_dir(
        &["introspect", "--database-url", "postgresql://postgres:postgres@localhost:5432/shem_test", "--output", "schema"],
        &env.temp_path()
    )?;
    
    cli::assert_command_success(&output);
    
    // Verify table with composite primary key was introspected
    let schema_content = std::fs::read_to_string(env.temp_path().join("schema/schema.sql"))?;
    assert!(schema_content.contains("CREATE TABLE order_items"));
    assert!(schema_content.contains("PRIMARY KEY (order_id, product_id)"));
    
    Ok(())
}

#[tokio::test]
async fn test_introspect_table_with_constraints() -> Result<()> {
    let env = TestEnv::new()?;
    let pool = db::setup_test_db(&env.db_name).await?;
    
    // Create table with constraints
    db::execute_sql(&pool, r#"
        CREATE TABLE products (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            price DECIMAL(10,2) CHECK (price > 0),
            category VARCHAR(50) NOT NULL,
            CONSTRAINT unique_product_name UNIQUE (name),
            CONSTRAINT valid_category CHECK (category IN ('electronics', 'clothing', 'books'))
        );
    "#).await?;
    
    // Run introspect command
    let db_url = format!("postgresql://postgres:postgres@localhost:5432/{}", env.db_name);
    let output = cli::run_shem_command_in_dir(
        &["introspect", "--database-url", &db_url, "--output", "schema"],
        &env.temp_path()
    )?;
    
    cli::assert_command_success(&output);
    
    // Verify table and constraints were introspected
    let schema_content = std::fs::read_to_string(env.temp_path().join("schema/schema.sql"))?;
    assert!(schema_content.contains("CREATE TABLE products"));
    assert!(schema_content.contains("price DECIMAL(10,2) CHECK (price > 0)"));
    assert!(schema_content.contains("CONSTRAINT unique_product_name UNIQUE (name)"));
    assert!(schema_content.contains("CONSTRAINT valid_category CHECK (category IN ('electronics', 'clothing', 'books'))"));
    
    // Clean up
    db::drop_test_db(&env.db_name).await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_table_with_triggers() -> Result<()> {
    let env = TestEnv::new()?;
    let pool = db::setup_test_db(&env.db_name).await?;
    
    // Create table and trigger
    db::execute_sql(&pool, sql::SIMPLE_TABLE).await?;
    db::execute_sql(&pool, r#"
        CREATE OR REPLACE FUNCTION update_updated_at()
        RETURNS TRIGGER AS $$
        BEGIN
            NEW.updated_at = CURRENT_TIMESTAMP;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
    "#).await?;
    db::execute_sql(&pool, r#"
        CREATE TRIGGER trigger_update_updated_at
        BEFORE UPDATE ON users
        FOR EACH ROW
        EXECUTE FUNCTION update_updated_at();
    "#).await?;
    
    // Run introspect command
    let db_url = format!("postgresql://postgres:postgres@localhost:5432/{}", env.db_name);
    let output = cli::run_shem_command_in_dir(
        &["introspect", "--database-url", &db_url, "--output", "schema"],
        &env.temp_path()
    )?;
    
    cli::assert_command_success(&output);
    
    // Verify table and trigger were introspected
    let schema_content = std::fs::read_to_string(env.temp_path().join("schema/schema.sql"))?;
    assert!(schema_content.contains("CREATE TABLE users"));
    assert!(schema_content.contains("CREATE OR REPLACE FUNCTION update_updated_at()"));
    assert!(schema_content.contains("CREATE TRIGGER trigger_update_updated_at"));
    
    // Clean up
    db::drop_test_db(&env.db_name).await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_table_with_partitioning() -> Result<()> {
    let env = TestEnv::new()?;
    let pool = db::setup_test_db(&env.db_name).await?;
    
    // Create partitioned table
    db::execute_sql(&pool, r#"
        CREATE TABLE logs (
            id SERIAL,
            message TEXT,
            created_at TIMESTAMP
        ) PARTITION BY RANGE (created_at);
    "#).await?;
    db::execute_sql(&pool, r#"
        CREATE TABLE logs_2024_01 PARTITION OF logs
        FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');
    "#).await?;
    
    // Run introspect command
    let db_url = format!("postgresql://postgres:postgres@localhost:5432/{}", env.db_name);
    let output = cli::run_shem_command_in_dir(
        &["introspect", "--database-url", &db_url, "--output", "schema"],
        &env.temp_path()
    )?;
    
    cli::assert_command_success(&output);
    
    // Verify partitioned table was introspected
    let schema_content = std::fs::read_to_string(env.temp_path().join("schema/schema.sql"))?;
    assert!(schema_content.contains("CREATE TABLE logs"));
    assert!(schema_content.contains("PARTITION BY RANGE (created_at)"));
    assert!(schema_content.contains("CREATE TABLE logs_2024_01 PARTITION OF logs"));
    
    // Clean up
    db::drop_test_db(&env.db_name).await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_table_with_inheritance() -> Result<()> {
    let env = TestEnv::new()?;
    let pool = db::setup_test_db(&env.db_name).await?;
    
    // Create parent and child tables
    db::execute_sql(&pool, r#"
        CREATE TABLE vehicles (
            id SERIAL PRIMARY KEY,
            make VARCHAR(50) NOT NULL,
            model VARCHAR(50) NOT NULL,
            year INTEGER NOT NULL
        );
    "#).await?;
    db::execute_sql(&pool, r#"
        CREATE TABLE cars (
            num_doors INTEGER DEFAULT 4
        ) INHERITS (vehicles);
    "#).await?;
    
    // Run introspect command
    let db_url = format!("postgresql://postgres:postgres@localhost:5432/{}", env.db_name);
    let output = cli::run_shem_command_in_dir(
        &["introspect", "--database-url", &db_url, "--output", "schema"],
        &env.temp_path()
    )?;
    
    cli::assert_command_success(&output);
    
    // Verify inherited tables were introspected
    let schema_content = std::fs::read_to_string(env.temp_path().join("schema/schema.sql"))?;
    assert!(schema_content.contains("CREATE TABLE vehicles"));
    assert!(schema_content.contains("CREATE TABLE cars"));
    assert!(schema_content.contains("INHERITS (vehicles)"));
    
    // Clean up
    db::drop_test_db(&env.db_name).await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_table_with_rls() -> Result<()> {
    let env = TestEnv::new()?;
    let pool = db::setup_test_db(&env.db_name).await?;
    
    // Create table with RLS
    db::execute_sql(&pool, sql::SIMPLE_TABLE).await?;
    db::execute_sql(&pool, "ALTER TABLE users ENABLE ROW LEVEL SECURITY;").await?;
    db::execute_sql(&pool, r#"
        CREATE POLICY users_policy ON users
        FOR ALL
        USING (status = 'active');
    "#).await?;
    
    // Run introspect command
    let db_url = format!("postgresql://postgres:postgres@localhost:5432/{}", env.db_name);
    let output = cli::run_shem_command_in_dir(
        &["introspect", "--database-url", &db_url, "--output", "schema"],
        &env.temp_path()
    )?;
    
    cli::assert_command_success(&output);
    
    // Verify table with RLS was introspected
    let schema_content = std::fs::read_to_string(env.temp_path().join("schema/schema.sql"))?;
    assert!(schema_content.contains("CREATE TABLE users"));
    assert!(schema_content.contains("ALTER TABLE users ENABLE ROW LEVEL SECURITY"));
    assert!(schema_content.contains("CREATE POLICY users_policy ON users"));
    
    // Clean up
    db::drop_test_db(&env.db_name).await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_table_with_storage_parameters() -> Result<()> {
    let env = TestEnv::new()?;
    let pool = db::setup_test_db(&env.db_name).await?;
    
    // Create table with storage parameters
    db::execute_sql(&pool, r#"
        CREATE TABLE large_table (
            id SERIAL PRIMARY KEY,
            data TEXT
        ) WITH (
            fillfactor = 70,
            autovacuum_vacuum_scale_factor = 0.1
        );
    "#).await?;
    
    // Run introspect command
    let db_url = format!("postgresql://postgres:postgres@localhost:5432/{}", env.db_name);
    let output = cli::run_shem_command_in_dir(
        &["introspect", "--database-url", &db_url, "--output", "schema"],
        &env.temp_path()
    )?;
    
    cli::assert_command_success(&output);
    
    // Verify table with storage parameters was introspected
    let schema_content = std::fs::read_to_string(env.temp_path().join("schema/schema.sql"))?;
    assert!(schema_content.contains("CREATE TABLE large_table"));
    assert!(schema_content.contains("WITH ("));
    assert!(schema_content.contains("fillfactor = 70"));
    
    // Clean up
    db::drop_test_db(&env.db_name).await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_table_with_unlogged() -> Result<()> {
    let env = TestEnv::new()?;
    let pool = db::setup_test_db(&env.db_name).await?;
    
    // Create unlogged table
    db::execute_sql(&pool, r#"
        CREATE UNLOGGED TABLE temp_data (
            id SERIAL PRIMARY KEY,
            value TEXT
        );
    "#).await?;
    
    // Run introspect command
    let db_url = format!("postgresql://postgres:postgres@localhost:5432/{}", env.db_name);
    let output = cli::run_shem_command_in_dir(
        &["introspect", "--database-url", &db_url, "--output", "schema"],
        &env.temp_path()
    )?;
    
    cli::assert_command_success(&output);
    
    // Verify unlogged table was introspected
    let schema_content = std::fs::read_to_string(env.temp_path().join("schema/schema.sql"))?;
    assert!(schema_content.contains("CREATE UNLOGGED TABLE temp_data"));
    
    // Clean up
    db::drop_test_db(&env.db_name).await?;
    Ok(())
} 