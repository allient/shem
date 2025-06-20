//! Integration tests for the introspect command
//!
//! Tests that verify the complete introspect workflow with multiple object types.

use crate::common::{TestEnv, cli, db};
use crate::fixtures::sql;
use anyhow::Result;

#[tokio::test]
async fn test_introspect_complete_schema() -> Result<()> {
    let env = TestEnv::new()?;
    let pool = db::setup_test_db(&env.db_name).await?;

    // Create a complete schema with multiple object types
    db::execute_sql(&pool, sql::COMPLETE_SCHEMA).await?;

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

    // Verify all object types were introspected
    let schema_content = std::fs::read_to_string(env.temp_path().join("schema/schema.sql"))?;

    // Check extensions
    assert!(schema_content.contains("CREATE EXTENSION"));

    // Check enums
    assert!(schema_content.contains("CREATE TYPE user_status AS ENUM"));
    assert!(schema_content.contains("CREATE TYPE post_status AS ENUM"));

    // Check domains
    assert!(schema_content.contains("CREATE DOMAIN email_address"));

    // Check sequences
    assert!(schema_content.contains("CREATE SEQUENCE custom_id_seq"));

    // Check tables
    assert!(schema_content.contains("CREATE TABLE users"));
    assert!(schema_content.contains("CREATE TABLE posts"));

    // Check views
    assert!(schema_content.contains("CREATE VIEW active_users"));
    assert!(schema_content.contains("CREATE VIEW published_posts"));

    // Check functions
    assert!(schema_content.contains("CREATE OR REPLACE FUNCTION get_user_count"));
    assert!(schema_content.contains("CREATE OR REPLACE FUNCTION get_posts_by_user"));

    // Check triggers
    assert!(schema_content.contains("CREATE OR REPLACE FUNCTION update_updated_at"));
    assert!(schema_content.contains("CREATE TRIGGER update_users_updated_at"));
    assert!(schema_content.contains("CREATE TRIGGER update_posts_updated_at"));

    // Clean up
    db::drop_test_db(&env.db_name).await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_with_custom_output_directory() -> Result<()> {
    let env = TestEnv::new()?;
    let pool = db::setup_test_db(&env.db_name).await?;

    // Create a simple table
    db::execute_sql(&pool, sql::SIMPLE_TABLE).await?;

    // Run introspect command with custom output directory
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

#[tokio::test]
async fn test_introspect_with_database_url_from_config() -> Result<()> {
    let env = TestEnv::new()?;
    let pool = db::setup_test_db(&env.db_name).await?;

    // Create config file with database URL
    let db_url = format!("postgresql://postgres:postgres@localhost:5432/{}", env.db_name);
    env.create_test_file(
        "shem.toml",
        &format!(
            r#"
        [database]
        url = "{}"
        
        [output]
        format = "sql"
        directory = "schema"
    "#,
            db_url
        ),
    )?;

    // Create a simple table
    db::execute_sql(&pool, sql::SIMPLE_TABLE).await?;

    // Run introspect command without explicit database URL
    let output =
        cli::run_shem_command_in_dir(&["introspect", "--output", "schema"], &env.temp_path())?;

    cli::assert_command_success(&output);

    // Verify schema was created
    env.assert_file_exists("schema/schema.sql");

    // Clean up
    db::drop_test_db(&env.db_name).await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_with_verbose_output() -> Result<()> {
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

    // Verify schema was created
    env.assert_file_exists("schema/schema.sql");

    // Clean up
    db::drop_test_db(&env.db_name).await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_empty_database() -> Result<()> {
    let env = TestEnv::new()?;
    let pool = db::setup_test_db(&env.db_name).await?;

    // Run introspect command on empty database
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

    // Verify schema file was created (even if empty)
    env.assert_file_exists("schema/schema.sql");

    // Verify file is empty or contains only comments
    let schema_content = std::fs::read_to_string(env.temp_path().join("schema/schema.sql"))?;
    assert!(schema_content.trim().is_empty() || schema_content.contains("--"));

    // Clean up
    db::drop_test_db(&env.db_name).await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_with_invalid_database_url() -> Result<()> {
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

    // Verify command failed
    cli::assert_command_failure(&output, "connection");

    Ok(())
}

#[tokio::test]
async fn test_introspect_with_missing_database_url() -> Result<()> {
    let env = TestEnv::new()?;

    // Run introspect command without database URL
    let output =
        cli::run_shem_command_in_dir(&["introspect", "--output", "schema"], &env.temp_path())?;

    // Verify command failed with appropriate error
    cli::assert_command_failure(&output, "Database URL");

    Ok(())
}

#[tokio::test]
async fn test_introspect_with_invalid_output_directory() -> Result<()> {
    let env = TestEnv::new()?;
    let pool = db::setup_test_db(&env.db_name).await?;

    // Create a simple table
    db::execute_sql(&pool, sql::SIMPLE_TABLE).await?;

    // Create a file with the same name as the output directory
    env.create_test_file("schema", "this is a file, not a directory")?;

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

    // This should fail because we can't create a directory where a file exists
    cli::assert_command_failure(&output, "directory");

    Ok(())
}

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

    // Verify schema file was created
    let schema_content = std::fs::read_to_string(env.temp_path().join("schema/schema.sql"))?;

    // Verify all objects are present
    assert!(schema_content.contains("CREATE TYPE user_status AS ENUM"));
    assert!(schema_content.contains("CREATE DOMAIN email_address"));
    assert!(schema_content.contains("CREATE TABLE users"));
    assert!(schema_content.contains("CREATE TABLE posts"));
    assert!(schema_content.contains("CREATE VIEW active_users"));

    // Verify dependency order (types and domains should come before tables)
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

#[tokio::test]
async fn test_introspect_with_system_objects_excluded() -> Result<()> {
    let env = TestEnv::new()?;
    let pool = db::setup_test_db(&env.db_name).await?;

    // Create a simple table
    db::execute_sql(&pool, sql::SIMPLE_TABLE).await?;

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

    // Verify schema file was created
    let schema_content = std::fs::read_to_string(env.temp_path().join("schema/schema.sql"))?;

    // Verify our custom table is included
    assert!(schema_content.contains("CREATE TABLE users"));

    // Verify system schemas are excluded (should not contain information_schema or pg_catalog objects)
    assert!(!schema_content.contains("information_schema"));
    assert!(!schema_content.contains("pg_catalog"));

    Ok(())
}

#[tokio::test]
async fn test_introspect_with_comments_preserved() -> Result<()> {
    let env = TestEnv::new()?;
    let pool = db::setup_test_db(&env.db_name).await?;

    // Create table with comments
    db::execute_sql(&pool, sql::SIMPLE_TABLE).await?;
    db::execute_sql(&pool, "COMMENT ON TABLE users IS 'User accounts table';").await?;
    db::execute_sql(&pool, "COMMENT ON COLUMN users.id IS 'Primary key';").await?;
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

    // Verify schema file was created with comments
    let schema_content = std::fs::read_to_string(env.temp_path().join("schema/schema.sql"))?;

    assert!(schema_content.contains("CREATE TABLE users"));
    assert!(schema_content.contains("COMMENT ON TABLE users IS 'User accounts table'"));
    assert!(schema_content.contains("COMMENT ON COLUMN users.id IS 'Primary key'"));
    assert!(schema_content.contains("COMMENT ON COLUMN users.email IS 'User email address'"));

    Ok(())
}
