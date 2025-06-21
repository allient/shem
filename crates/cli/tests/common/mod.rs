//! Common test utilities for CLI command tests

use std::path::PathBuf;
use tempfile::{TempDir, tempdir};
use std::fs;
use anyhow::Result;
use ::cli::Config;
use uuid::Uuid;
use chrono::Utc;

/// Test environment setup and teardown utilities
pub struct TestEnv {
    pub temp_dir: TempDir,
    pub config: Config,
    pub db_name: String,
}

impl TestEnv {
    /// Create a new test environment with a temporary directory
    pub fn new() -> Result<Self> {
        let temp_dir = tempdir()?;
        let config = Config::default();
        let db_name = db::generate_unique_db_name();
        
        Ok(Self {
            temp_dir,
            config,
            db_name,
        })
    }

    /// Get the path to the temporary directory
    pub fn temp_path(&self) -> PathBuf {
        self.temp_dir.path().to_path_buf()
    }

    /// Create a test file in the temporary directory
    pub fn create_test_file(&self, filename: &str, content: &str) -> Result<PathBuf> {
        let file_path = self.temp_path().join(filename);
        fs::write(&file_path, content)?;
        Ok(file_path)
    }

    /// Create a test directory in the temporary directory
    pub fn create_test_dir(&self, dirname: &str) -> Result<PathBuf> {
        let dir_path = self.temp_path().join(dirname);
        fs::create_dir_all(&dir_path)?;
        Ok(dir_path)
    }

    /// Assert that a file exists and contains expected content
    pub fn assert_file_content(&self, filename: &str, expected_content: &str) -> Result<()> {
        let file_path = self.temp_path().join(filename);
        assert!(file_path.exists(), "File {} does not exist", filename);
        
        let content = fs::read_to_string(&file_path)?;
        assert_eq!(content.trim(), expected_content.trim(), 
                   "File content mismatch for {}", filename);
        Ok(())
    }

    /// Assert that a file exists
    pub fn assert_file_exists(&self, filename: &str) {
        let file_path = self.temp_path().join(filename);
        assert!(file_path.exists(), "File {} does not exist", filename);
    }

    /// Assert that a directory exists
    pub fn assert_dir_exists(&self, dirname: &str) {
        let dir_path = self.temp_path().join(dirname);
        assert!(dir_path.exists(), "Directory {} does not exist", dirname);
        assert!(dir_path.is_dir(), "Path {} is not a directory", dirname);
    }
}

/// Database connection utilities for testing
pub mod db {
    use super::*;
    use sqlx::PgPool;
    use std::env;
    use std::process::Command;

    /// Generate a unique test database name
    pub fn generate_unique_db_name() -> String {
        let timestamp = Utc::now().timestamp();
        let uuid = Uuid::new_v4().simple().to_string();
        format!("shem_test_{}_{}", timestamp, uuid)
    }

    /// Get the base database URL (without specific database name)
    fn get_base_database_url() -> String {
        env::var("TEST_DATABASE_URL")
            .unwrap_or_else(|_| "postgresql://postgres:postgres@localhost:5432".to_string())
    }

    /// Get a database URL for a specific db name
    pub fn get_database_url(db_name: &str) -> String {
        let base_url = get_base_database_url();
        if base_url.ends_with(&format!("/{}", db_name)) {
            base_url
        } else {
            format!("{}/{}", base_url.trim_end_matches('/'), db_name)
        }
    }

    /// Create a test database
    pub async fn create_test_db(db_name: &str) -> Result<()> {
        let base_url = get_base_database_url();
        let postgres_url = if base_url.ends_with("/postgres") {
            base_url
        } else {
            format!("{}/postgres", base_url.trim_end_matches('/'))
        };
        let pool = PgPool::connect(&postgres_url).await?;
        sqlx::query(&format!("CREATE DATABASE {}", db_name)).execute(&pool).await?;
        Ok(())
    }

    /// Drop a test database
    pub async fn drop_test_db(db_name: &str) -> Result<()> {
        let base_url = get_base_database_url();
        let postgres_url = if base_url.ends_with("/postgres") {
            base_url
        } else {
            format!("{}/postgres", base_url.trim_end_matches('/'))
        };
        let pool = PgPool::connect(&postgres_url).await?;
        // Terminate connections to the db first
        sqlx::query(&format!(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '{}' AND pid <> pg_backend_pid();",
            db_name
        ))
        .execute(&pool)
        .await?;
        sqlx::query(&format!("DROP DATABASE IF EXISTS {}", db_name)).execute(&pool).await?;
        Ok(())
    }

    /// Get a test database connection pool
    pub async fn get_test_pool(db_name: &str) -> Result<PgPool> {
        let db_url = get_database_url(db_name);
        let pool = PgPool::connect(&db_url).await?;
        Ok(pool)
    }

    /// Create a test database and return connection pool
    pub async fn setup_test_db(db_name: &str) -> Result<PgPool> {
        create_test_db(db_name).await?;
        let pool = get_test_pool(db_name).await?;
        Ok(pool)
    }

    /// Clean up test database (drop all non-system schemas)
    pub async fn cleanup_test_db(pool: &PgPool) -> Result<()> {
        sqlx::query(r#"
            DO $$
            DECLARE
                schema_name text;
            BEGIN
                FOR schema_name IN 
                    SELECT nspname 
                    FROM pg_namespace 
                    WHERE nspname NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
                    AND nspname NOT LIKE 'pg_%'
                LOOP
                    EXECUTE 'DROP SCHEMA IF EXISTS ' || quote_ident(schema_name) || ' CASCADE';
                END LOOP;
            END $$;
        "#).execute(pool).await?;
        sqlx::query("CREATE SCHEMA IF NOT EXISTS public")
            .execute(pool)
            .await?;
        Ok(())
    }

    /// Execute SQL statements in the test database
    pub async fn execute_sql(pool: &PgPool, sql: &str) -> Result<()> {
        let statements: Vec<&str> = sql
            .split(';')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .collect();
        for statement in statements {
            if !statement.trim().is_empty() {
                sqlx::query(statement).execute(pool).await?;
            }
        }
        Ok(())
    }
}

/// CLI command execution utilities
pub mod cli {
    use super::*;
    use std::process::Command;
    use std::env;
    use std::path::PathBuf;

    /// Get the path to the CLI crate directory
    pub fn cli_crate_dir() -> PathBuf {
        // Traverse up from CARGO_MANIFEST_DIR to the cli crate
        // This works for both workspace and direct runs
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
    }

    /// Execute a shem CLI command and return the result
    pub fn run_shem_command(args: &[&str]) -> Result<std::process::Output> {
        let output = Command::new("cargo")
            .args(&["run", "--bin", "shem"])
            .args(args)
            .current_dir(cli_crate_dir())
            .output()?;
        
        Ok(output)
    }

    /// Execute a shem CLI command, but always run in the CLI crate directory.
    /// The output path should be absolute or relative to the CLI crate dir.
    pub fn run_shem_command_in_dir(args: &[&str], temp_dir: &PathBuf) -> Result<std::process::Output> {
        // Convert output path to absolute if needed
        let mut new_args: Vec<String> = vec![];
        let mut skip_next = false;
        for (i, arg) in args.iter().enumerate() {
            if skip_next {
                skip_next = false;
                continue;
            }
            if (*arg == "--output" || *arg == "-o") && i + 1 < args.len() {
                // Make output path absolute
                let output_path = temp_dir.join(args[i + 1]);
                new_args.push(arg.to_string());
                new_args.push(output_path.to_string_lossy().to_string());
                skip_next = true;
            } else {
                new_args.push(arg.to_string());
            }
        }
        let output = Command::new("cargo")
            .args(&["run", "--bin", "shem"])
            .args(&new_args)
            .current_dir(cli_crate_dir())
            .output()?;
        Ok(output)
    }

    /// Assert that a CLI command succeeds
    pub fn assert_command_success(output: &std::process::Output) {
        assert!(
            output.status.success(),
            "Command failed with status: {}\nStdout: {}\nStderr: {}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    /// Assert that a CLI command fails with expected error
    pub fn assert_command_failure(output: &std::process::Output, expected_error: &str) {
        assert!(
            !output.status.success(),
            "Command should have failed but succeeded"
        );
        
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains(expected_error),
            "Expected error '{}' not found in stderr: {}",
            expected_error,
            stderr
        );
    }
} 