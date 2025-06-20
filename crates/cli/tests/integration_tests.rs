//! Integration tests for the Shem CLI
//!
//! This file contains the main integration tests that can be run with `cargo test`.

mod common;
mod fixtures;
mod introspect;

use anyhow::Result;
use common::{TestEnv, cli, db};
use fixtures::sql;

/// Test that the CLI can be built and run
#[test]
fn test_cli_builds() {
    // This test ensures the CLI can be built
    // If this test passes, it means the CLI compiles successfully
    assert!(true);
}

/// Test basic introspect functionality
#[tokio::test]
async fn test_basic_introspect() -> Result<()> {
    let env = TestEnv::new()?;
    let pool = db::setup_test_db(&env.db_name).await?;

    // Create a simple table
    db::execute_sql(&pool, sql::SIMPLE_TABLE).await?;

    // Run introspect command
    let db_url = format!("postgresql://postgres:postgres@localhost:5432/{}", env.db_name);
    let output = cli::run_shem_command_in_dir(
        &[
            "introspect",
            "--database-url",
            &db_url,
            "--output",
            "schema",
        ],
        &env.temp_path(),
    )?;

    cli::assert_command_success(&output);

    // Verify schema file was created
    env.assert_file_exists("schema/schema.sql");

    // Clean up
    db::drop_test_db(&env.db_name).await?;
    Ok(())
}

/// Test introspect with multiple object types
#[tokio::test]
async fn test_introspect_multiple_objects() -> Result<()> {
    let env = TestEnv::new()?;
    let pool = db::setup_test_db(&env.db_name).await?;

    // Create multiple object types
    db::execute_sql(&pool, sql::ENUM_TYPE).await?;
    db::execute_sql(&pool, sql::DOMAIN).await?;
    db::execute_sql(&pool, sql::SIMPLE_TABLE).await?;
    db::execute_sql(&pool, sql::VIEW).await?;

    // Run introspect command
    let db_url = format!("postgresql://postgres:postgres@localhost:5432/{}", env.db_name);
    let output = cli::run_shem_command_in_dir(
        &[
            "introspect",
            "--database-url",
            &db_url,
            "--output",
            "schema",
        ],
        &env.temp_path(),
    )?;

    cli::assert_command_success(&output);

    // Verify all objects were introspected
    let schema_content = std::fs::read_to_string(env.temp_path().join("schema/schema.sql"))?;
    assert!(schema_content.contains("CREATE TYPE user_status AS ENUM"));
    assert!(schema_content.contains("CREATE DOMAIN email_address"));
    assert!(schema_content.contains("CREATE TABLE users"));
    assert!(schema_content.contains("CREATE VIEW active_users"));

    // Clean up
    db::drop_test_db(&env.db_name).await?;
    Ok(())
}

/// Test introspect with extensions
#[tokio::test]
async fn test_introspect_with_extensions() -> Result<()> {
    println!("🚀 Testing introspect with extensions...");
    let env = TestEnv::new()?;
    let pool = db::setup_test_db(&env.db_name).await?;

    // Create extensions
    db::execute_sql(&pool, sql::MULTIPLE_EXTENSIONS).await?;

    // Run introspect command
    let db_url = format!("postgresql://postgres:postgres@localhost:5432/{}", env.db_name);
    let output = cli::run_shem_command_in_dir(
        &[
            "introspect",
            "--database-url",
            &db_url,
            "--output",
            "schema",
        ],
        &env.temp_path(),
    )?;

    cli::assert_command_success(&output);

    // Verify extensions were introspected
    let schema_content = std::fs::read_to_string(env.temp_path().join("schema/schema.sql"))?;
    println!("🚀 schema_content: {}", schema_content);
    assert!(schema_content.contains("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\""));
    assert!(schema_content.contains("CREATE EXTENSION IF NOT EXISTS \"pgcrypto\""));
    assert!(schema_content.contains("CREATE EXTENSION IF NOT EXISTS \"citext\""));

    // Clean up
    db::drop_test_db(&env.db_name).await?;
    Ok(())
}

/// Test introspect with error handling
#[tokio::test]
async fn test_introspect_error_handling() -> Result<()> {
    let env = TestEnv::new()?;

    // Run introspect command with invalid database URL
    let output = cli::run_shem_command_in_dir(
        &[
            "introspect",
            "--database-url",
            "postgresql://invalid:invalid@localhost:5432/nonexistent",
            "--output",
            "schema",
        ],
        &env.temp_path(),
    )?;

    // Verify command failed appropriately
    cli::assert_command_failure(&output, "connection");

    Ok(())
}

/// Test introspect with custom output directory
#[tokio::test]
async fn test_introspect_custom_output() -> Result<()> {
    let env = TestEnv::new()?;
    let pool = db::setup_test_db(&env.db_name).await?;

    // Create a simple table
    db::execute_sql(&pool, sql::SIMPLE_TABLE).await?;

    // Run introspect command with custom output
    let db_url = format!("postgresql://postgres:postgres@localhost:5432/{}", env.db_name);
    let output = cli::run_shem_command_in_dir(
        &[
            "introspect",
            "--database-url",
            &db_url,
            "--output",
            "custom_schema",
        ],
        &env.temp_path(),
    )?;

    cli::assert_command_success(&output);

    // Verify custom directory was created
    env.assert_dir_exists("custom_schema");
    env.assert_file_exists("custom_schema/schema.sql");

    // Clean up
    db::drop_test_db(&env.db_name).await?;
    Ok(())
}

/// Test introspect with verbose output
#[tokio::test]
async fn test_introspect_verbose() -> Result<()> {
    let env = TestEnv::new()?;
    let pool = db::setup_test_db(&env.db_name).await?;

    // Create a simple table
    db::execute_sql(&pool, sql::SIMPLE_TABLE).await?;

    // Run introspect command with verbose output
    let db_url = format!("postgresql://postgres:postgres@localhost:5432/{}", env.db_name);
    let output = cli::run_shem_command_in_dir(
        &[
            "introspect",
            "--database-url",
            &db_url,
            "--output",
            "schema",
            "--verbose",
        ],
        &env.temp_path(),
    )?;

    cli::assert_command_success(&output);

    // Verify verbose output contains debug information
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("debug") || stderr.contains("DEBUG"));

    // Clean up
    db::drop_test_db(&env.db_name).await?;
    Ok(())
}

/// Test introspect with comments
#[tokio::test]
async fn test_introspect_with_comments() -> Result<()> {
    let env = TestEnv::new()?;
    let pool = db::setup_test_db(&env.db_name).await?;

    // Create table with comments
    db::execute_sql(&pool, sql::SIMPLE_TABLE).await?;
    db::execute_sql(&pool, "COMMENT ON TABLE users IS 'User accounts table';").await?;
    db::execute_sql(
        &pool,
        "COMMENT ON COLUMN users.email IS 'User email address';",
    )
    .await?;

    // Run introspect command
    let output = cli::run_shem_command_in_dir(
        &[
            "introspect",
            "--database-url",
            "postgresql://postgres:postgres@localhost:5432/shem_test",
            "--output",
            "schema",
        ],
        &env.temp_path(),
    )?;

    cli::assert_command_success(&output);

    // Verify comments were preserved
    let schema_content = std::fs::read_to_string(env.temp_path().join("schema/schema.sql"))?;
    assert!(schema_content.contains("COMMENT ON TABLE users IS 'User accounts table'"));
    assert!(schema_content.contains("COMMENT ON COLUMN users.email IS 'User email address'"));

    Ok(())
}

/// Test introspect dependency ordering
#[tokio::test]
async fn test_introspect_dependency_order() -> Result<()> {
    let env = TestEnv::new()?;
    let pool = db::setup_test_db(&env.db_name).await?;

    // Create objects with dependencies
    db::execute_sql(&pool, sql::ENUM_TYPE).await?;
    db::execute_sql(&pool, sql::DOMAIN).await?;
    db::execute_sql(&pool, sql::SIMPLE_TABLE).await?;
    db::execute_sql(&pool, sql::TABLE_WITH_FK).await?;
    db::execute_sql(&pool, sql::VIEW).await?;

    // Run introspect command
    let output = cli::run_shem_command_in_dir(
        &[
            "introspect",
            "--database-url",
            "postgresql://postgres:postgres@localhost:5432/shem_test",
            "--output",
            "schema",
        ],
        &env.temp_path(),
    )?;

    cli::assert_command_success(&output);

    // Verify dependency order
    let schema_content = std::fs::read_to_string(env.temp_path().join("schema/schema.sql"))?;

    let enum_pos = schema_content
        .find("CREATE TYPE user_status AS ENUM")
        .unwrap();
    let domain_pos = schema_content.find("CREATE DOMAIN email_address").unwrap();
    let users_table_pos = schema_content.find("CREATE TABLE users").unwrap();
    let posts_table_pos = schema_content.find("CREATE TABLE posts").unwrap();
    let view_pos = schema_content.find("CREATE VIEW active_users").unwrap();

    assert!(enum_pos < users_table_pos);
    assert!(domain_pos < users_table_pos);
    assert!(users_table_pos < posts_table_pos);
    assert!(posts_table_pos < view_pos);

    Ok(())
}
