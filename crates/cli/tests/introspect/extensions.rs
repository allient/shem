//! Extension introspection tests
//! 
//! Tests for introspecting various types of extensions.

use crate::common::{TestEnv, cli, db};
use anyhow::Result;

#[tokio::test]
async fn test_introspect_simple_extension() -> Result<()> {
    let env = TestEnv::new()?;
    let pool = db::setup_test_db(&env.db_name).await?;
    
    // Create a simple extension
    db::execute_sql(&pool, "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";").await?;
    
    // Run introspect command
    let db_url = format!("postgresql://postgres:postgres@localhost:5432/{}", env.db_name);
    let output = cli::run_shem_command_in_dir(
        &["introspect", "--database-url", &db_url, "--output", "schema"],
        &env.temp_path()
    )?;
    
    cli::assert_command_success(&output);
    
    // Verify extension was introspected
    let schema_content = std::fs::read_to_string(env.temp_path().join("schema/schema.sql"))?;
    assert!(schema_content.contains("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\""));
    
    // Clean up
    db::drop_test_db(&env.db_name).await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_multiple_extensions() -> Result<()> {
    let env = TestEnv::new()?;
    let pool = db::setup_test_db(&env.db_name).await?;
    
    // Create multiple extensions
    db::execute_sql(&pool, "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";").await?;
    db::execute_sql(&pool, "CREATE EXTENSION IF NOT EXISTS \"pgcrypto\";").await?;
    db::execute_sql(&pool, "CREATE EXTENSION IF NOT EXISTS \"citext\";").await?;
    
    // Run introspect command
    let db_url = format!("postgresql://postgres:postgres@localhost:5432/{}", env.db_name);
    let output = cli::run_shem_command_in_dir(
        &["introspect", "--database-url", &db_url, "--output", "schema"],
        &env.temp_path()
    )?;
    
    cli::assert_command_success(&output);
    
    // Verify all extensions were introspected
    let schema_content = std::fs::read_to_string(env.temp_path().join("schema/schema.sql"))?;
    assert!(schema_content.contains("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\""));
    assert!(schema_content.contains("CREATE EXTENSION IF NOT EXISTS \"pgcrypto\""));
    assert!(schema_content.contains("CREATE EXTENSION IF NOT EXISTS \"citext\""));
    
    // Clean up
    db::drop_test_db(&env.db_name).await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_extension_with_version() -> Result<()> {
    let env = TestEnv::new()?;
    let pool = db::setup_test_db(&env.db_name).await?;
    
    // Create extension with specific version
    db::execute_sql(&pool, "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\" VERSION '1.1';").await?;
    
    // Run introspect command
    let db_url = format!("postgresql://postgres:postgres@localhost:5432/{}", env.db_name);
    let output = cli::run_shem_command_in_dir(
        &["introspect", "--database-url", &db_url, "--output", "schema"],
        &env.temp_path()
    )?;
    
    cli::assert_command_success(&output);
    
    // Verify extension with version was introspected
    let schema_content = std::fs::read_to_string(env.temp_path().join("schema/schema.sql"))?;
    println!("🚀 schema_content: {}", schema_content);
    assert!(schema_content.contains("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\" VERSION '1.1'"));
    
    // Clean up
    db::drop_test_db(&env.db_name).await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_extension_with_schema() -> Result<()> {
    let env = TestEnv::new()?;
    let pool = db::setup_test_db(&env.db_name).await?;
    
    // Create custom schema
    db::execute_sql(&pool, "CREATE SCHEMA IF NOT EXISTS extensions;").await?;
    
    // Create extension in custom schema
    db::execute_sql(&pool, "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\" SCHEMA extensions;").await?;
    
    // Run introspect command
    let db_url = format!("postgresql://postgres:postgres@localhost:5432/{}", env.db_name);
    let output = cli::run_shem_command_in_dir(
        &["introspect", "--database-url", &db_url, "--output", "schema"],
        &env.temp_path()
    )?;
    
    cli::assert_command_success(&output);
    
    // Verify extension with schema was introspected
    let schema_content = std::fs::read_to_string(env.temp_path().join("schema/schema.sql"))?;
    assert!(schema_content.contains("CREATE SCHEMA IF NOT EXISTS extensions"));
    assert!(schema_content.contains("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\" SCHEMA extensions"));
    
    // Clean up
    db::drop_test_db(&env.db_name).await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_extension_with_version_and_schema() -> Result<()> {
    let env = TestEnv::new()?;
    let pool = db::setup_test_db(&env.db_name).await?;
    
    // Create custom schema
    db::execute_sql(&pool, "CREATE SCHEMA IF NOT EXISTS extensions;").await?;
    
    // Create extension with version and schema
    db::execute_sql(&pool, "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\" VERSION '1.1' SCHEMA extensions;").await?;
    
    // Run introspect command
    let db_url = format!("postgresql://postgres:postgres@localhost:5432/{}", env.db_name);
    let output = cli::run_shem_command_in_dir(
        &["introspect", "--database-url", &db_url, "--output", "schema"],
        &env.temp_path()
    )?;
    
    cli::assert_command_success(&output);
    
    // Verify extension with version and schema was introspected
    let schema_content = std::fs::read_to_string(env.temp_path().join("schema/schema.sql"))?;
    assert!(schema_content.contains("CREATE SCHEMA IF NOT EXISTS extensions"));
    assert!(schema_content.contains("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\" VERSION '1.1' SCHEMA extensions"));
    
    // Clean up
    db::drop_test_db(&env.db_name).await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_extension_with_cascade() -> Result<()> {
    let env = TestEnv::new()?;
    let pool = db::setup_test_db(&env.db_name).await?;
    
    // Create extension with CASCADE option
    db::execute_sql(&pool, "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\" CASCADE;").await?;
    
    // Run introspect command
    let db_url = format!("postgresql://postgres:postgres@localhost:5432/{}", env.db_name);
    let output = cli::run_shem_command_in_dir(
        &["introspect", "--database-url", &db_url, "--output", "schema"],
        &env.temp_path()
    )?;
    
    cli::assert_command_success(&output);
    
    // Verify extension with CASCADE was introspected
    let schema_content = std::fs::read_to_string(env.temp_path().join("schema/schema.sql"))?;
    assert!(schema_content.contains("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\" CASCADE"));
    
    // Clean up
    db::drop_test_db(&env.db_name).await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_extension_without_if_not_exists() -> Result<()> {
    let env = TestEnv::new()?;
    let pool = db::setup_test_db(&env.db_name).await?;
    
    // Create extension without IF NOT EXISTS
    db::execute_sql(&pool, "CREATE EXTENSION \"uuid-ossp\";").await?;
    
    // Run introspect command
    let db_url = format!("postgresql://postgres:postgres@localhost:5432/{}", env.db_name);
    let output = cli::run_shem_command_in_dir(
        &["introspect", "--database-url", &db_url, "--output", "schema"],
        &env.temp_path()
    )?;
    
    cli::assert_command_success(&output);
    
    // Verify extension without IF NOT EXISTS was introspected
    let schema_content = std::fs::read_to_string(env.temp_path().join("schema/schema.sql"))?;
    assert!(schema_content.contains("CREATE EXTENSION \"uuid-ossp\""));
    
    // Clean up
    db::drop_test_db(&env.db_name).await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_extension_with_comment() -> Result<()> {
    let env = TestEnv::new()?;
    let pool = db::setup_test_db(&env.db_name).await?;
    
    // Create extension
    db::execute_sql(&pool, "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";").await?;
    
    // Add comment to extension
    db::execute_sql(&pool, "COMMENT ON EXTENSION \"uuid-ossp\" IS 'UUID generation extension';").await?;
    
    // Run introspect command
    let db_url = format!("postgresql://postgres:postgres@localhost:5432/{}", env.db_name);
    let output = cli::run_shem_command_in_dir(
        &["introspect", "--database-url", &db_url, "--output", "schema"],
        &env.temp_path()
    )?;
    
    cli::assert_command_success(&output);
    
    // Verify extension and comment were introspected
    let schema_content = std::fs::read_to_string(env.temp_path().join("schema/schema.sql"))?;
    assert!(schema_content.contains("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\""));
    assert!(schema_content.contains("COMMENT ON EXTENSION \"uuid-ossp\" IS 'UUID generation extension'"));
    
    // Clean up
    db::drop_test_db(&env.db_name).await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_extension_dependency_order() -> Result<()> {
    let env = TestEnv::new()?;
    let pool = db::setup_test_db(&env.db_name).await?;
    
    // Create extensions and other objects
    db::execute_sql(&pool, "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";").await?;
    db::execute_sql(&pool, "CREATE EXTENSION IF NOT EXISTS \"pgcrypto\";").await?;
    db::execute_sql(&pool, "CREATE TYPE user_status AS ENUM ('active', 'inactive');").await?;
    db::execute_sql(&pool, "CREATE TABLE users (id UUID DEFAULT gen_random_uuid(), status user_status);").await?;
    
    // Run introspect command
    let db_url = format!("postgresql://postgres:postgres@localhost:5432/{}", env.db_name);
    let output = cli::run_shem_command_in_dir(
        &["introspect", "--database-url", &db_url, "--output", "schema"],
        &env.temp_path()
    )?;
    
    cli::assert_command_success(&output);
    
    // Verify dependency order (extensions should come first)
    let schema_content = std::fs::read_to_string(env.temp_path().join("schema/schema.sql"))?;
    
    let uuid_ext_pos = schema_content.find("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\"").unwrap();
    let pgcrypto_ext_pos = schema_content.find("CREATE EXTENSION IF NOT EXISTS \"pgcrypto\"").unwrap();
    let enum_pos = schema_content.find("CREATE TYPE user_status AS ENUM").unwrap();
    let table_pos = schema_content.find("CREATE TABLE users").unwrap();
    
    assert!(uuid_ext_pos < enum_pos);
    assert!(pgcrypto_ext_pos < enum_pos);
    assert!(enum_pos < table_pos);
    
    // Clean up
    db::drop_test_db(&env.db_name).await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_extension_with_objects_using_extension() -> Result<()> {
    let env = TestEnv::new()?;
    let pool = db::setup_test_db(&env.db_name).await?;
    
    // Create extension
    db::execute_sql(&pool, "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";").await?;
    
    // Create objects that use the extension
    db::execute_sql(&pool, r#"
        CREATE TABLE documents (
            id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
            title TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    "#).await?;
    
    db::execute_sql(&pool, r#"
        CREATE FUNCTION generate_document_id() RETURNS UUID AS $$
        BEGIN
            RETURN gen_random_uuid();
        END;
        $$ LANGUAGE plpgsql;
    "#).await?;
    
    // Run introspect command
    let db_url = format!("postgresql://postgres:postgres@localhost:5432/{}", env.db_name);
    let output = cli::run_shem_command_in_dir(
        &["introspect", "--database-url", &db_url, "--output", "schema"],
        &env.temp_path()
    )?;
    
    cli::assert_command_success(&output);
    
    // Verify extension and dependent objects were introspected
    let schema_content = std::fs::read_to_string(env.temp_path().join("schema/schema.sql"))?;
    assert!(schema_content.contains("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\""));
    assert!(schema_content.contains("CREATE TABLE documents"));
    assert!(schema_content.contains("id UUID DEFAULT gen_random_uuid()"));
    assert!(schema_content.contains("CREATE FUNCTION generate_document_id()"));
    assert!(schema_content.contains("RETURN gen_random_uuid()"));
    
    // Clean up
    db::drop_test_db(&env.db_name).await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_extension_with_multiple_schemas() -> Result<()> {
    let env = TestEnv::new()?;
    let pool = db::setup_test_db(&env.db_name).await?;
    
    // Create multiple schemas
    db::execute_sql(&pool, "CREATE SCHEMA IF NOT EXISTS app1;").await?;
    db::execute_sql(&pool, "CREATE SCHEMA IF NOT EXISTS app2;").await?;
    
    // Create extensions in different schemas
    db::execute_sql(&pool, "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\" SCHEMA app1;").await?;
    db::execute_sql(&pool, "CREATE EXTENSION IF NOT EXISTS \"pgcrypto\" SCHEMA app2;").await?;
    
    // Run introspect command
    let db_url = format!("postgresql://postgres:postgres@localhost:5432/{}", env.db_name);
    let output = cli::run_shem_command_in_dir(
        &["introspect", "--database-url", &db_url, "--output", "schema"],
        &env.temp_path()
    )?;
    
    cli::assert_command_success(&output);
    
    // Verify extensions in different schemas were introspected
    let schema_content = std::fs::read_to_string(env.temp_path().join("schema/schema.sql"))?;
    assert!(schema_content.contains("CREATE SCHEMA IF NOT EXISTS app1"));
    assert!(schema_content.contains("CREATE SCHEMA IF NOT EXISTS app2"));
    assert!(schema_content.contains("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\" SCHEMA app1"));
    assert!(schema_content.contains("CREATE EXTENSION IF NOT EXISTS \"pgcrypto\" SCHEMA app2"));
    
    // Clean up
    db::drop_test_db(&env.db_name).await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_extension_with_custom_version() -> Result<()> {
    let env = TestEnv::new()?;
    let pool = db::setup_test_db(&env.db_name).await?;
    
    // Create extension with custom version
    db::execute_sql(&pool, "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\" VERSION '1.1';").await?;
    
    // Run introspect command
    let db_url = format!("postgresql://postgres:postgres@localhost:5432/{}", env.db_name);
    let output = cli::run_shem_command_in_dir(
        &["introspect", "--database-url", &db_url, "--output", "schema"],
        &env.temp_path()
    )?;
    
    cli::assert_command_success(&output);
    
    // Verify extension with custom version was introspected
    let schema_content = std::fs::read_to_string(env.temp_path().join("schema/schema.sql"))?;
    assert!(schema_content.contains("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\" VERSION '1.1'"));
    
    // Clean up
    db::drop_test_db(&env.db_name).await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_extension_with_all_options() -> Result<()> {
    let env = TestEnv::new()?;
    let pool = db::setup_test_db(&env.db_name).await?;
    
    // Create custom schema
    db::execute_sql(&pool, "CREATE SCHEMA IF NOT EXISTS extensions;").await?;
    
    // Create extension with all options
    db::execute_sql(&pool, "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\" VERSION '1.1' SCHEMA extensions CASCADE;").await?;
    
    // Add comment
    db::execute_sql(&pool, "COMMENT ON EXTENSION \"uuid-ossp\" IS 'UUID generation extension with all options';").await?;
    
    // Run introspect command
    let db_url = format!("postgresql://postgres:postgres@localhost:5432/{}", env.db_name);
    let output = cli::run_shem_command_in_dir(
        &["introspect", "--database-url", &db_url, "--output", "schema"],
        &env.temp_path()
    )?;
    
    cli::assert_command_success(&output);
    
    // Verify extension with all options was introspected
    let schema_content = std::fs::read_to_string(env.temp_path().join("schema/schema.sql"))?;
    assert!(schema_content.contains("CREATE SCHEMA IF NOT EXISTS extensions"));
    assert!(schema_content.contains("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\" VERSION '1.1' SCHEMA extensions CASCADE"));
    assert!(schema_content.contains("COMMENT ON EXTENSION \"uuid-ossp\" IS 'UUID generation extension with all options'"));
    
    // Clean up
    db::drop_test_db(&env.db_name).await?;
    Ok(())
} 