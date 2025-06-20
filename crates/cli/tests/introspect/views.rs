//! View introspection tests
//! 
//! Tests for introspecting various types of views.

use crate::common::{TestEnv, cli, db};
use crate::fixtures::sql;
use anyhow::Result;

#[tokio::test]
async fn test_introspect_simple_view() -> Result<()> {
    let env = TestEnv::new()?;
    let pool = db::setup_test_db(&env.db_name).await?;
    
    // Create table and view
    db::execute_sql(&pool, sql::SIMPLE_TABLE).await?;
    db::execute_sql(&pool, sql::VIEW).await?;
    
    // Run introspect command
    let db_url = format!("postgresql://postgres:postgres@localhost:5432/{}", env.db_name);
    let output = cli::run_shem_command_in_dir(
        &["introspect", "--database-url", &db_url, "--output", "schema"],
        &env.temp_path()
    )?;
    
    cli::assert_command_success(&output);
    
    // Verify view was introspected
    let schema_content = std::fs::read_to_string(env.temp_path().join("schema/schema.sql"))?;
    assert!(schema_content.contains("CREATE VIEW active_users"));
    assert!(schema_content.contains("SELECT id, name, email"));
    assert!(schema_content.contains("FROM users"));
    assert!(schema_content.contains("WHERE status = 'active'"));
    
    // Clean up
    db::drop_test_db(&env.db_name).await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_view_with_joins() -> Result<()> {
    let env = TestEnv::new()?;
    let pool = db::setup_test_db(&env.db_name).await?;
    
    // Create tables and view with joins
    db::execute_sql(&pool, sql::SIMPLE_TABLE).await?;
    db::execute_sql(&pool, sql::TABLE_WITH_FK).await?;
    db::execute_sql(&pool, r#"
        CREATE VIEW user_posts AS
        SELECT u.id as user_id, u.name, p.id as post_id, p.title, p.created_at
        FROM users u
        LEFT JOIN posts p ON u.id = p.user_id;
    "#).await?;
    
    // Run introspect command
    let db_url = format!("postgresql://postgres:postgres@localhost:5432/{}", env.db_name);
    let output = cli::run_shem_command_in_dir(
        &["introspect", "--database-url", &db_url, "--output", "schema"],
        &env.temp_path()
    )?;
    
    cli::assert_command_success(&output);
    
    // Verify view with joins was introspected
    let schema_content = std::fs::read_to_string(env.temp_path().join("schema/schema.sql"))?;
    assert!(schema_content.contains("CREATE VIEW user_posts"));
    assert!(schema_content.contains("LEFT JOIN posts p ON u.id = p.user_id"));
    
    // Clean up
    db::drop_test_db(&env.db_name).await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_view_with_aggregation() -> Result<()> {
    let env = TestEnv::new()?;
    let pool = db::setup_test_db(&env.db_name).await?;
    
    // Create table and view with aggregation
    db::execute_sql(&pool, sql::SIMPLE_TABLE).await?;
    db::execute_sql(&pool, sql::TABLE_WITH_FK).await?;
    db::execute_sql(&pool, r#"
        CREATE VIEW user_post_counts AS
        SELECT u.id, u.name, COUNT(p.id) as post_count
        FROM users u
        LEFT JOIN posts p ON u.id = p.user_id
        GROUP BY u.id, u.name;
    "#).await?;
    
    // Run introspect command
    let db_url = format!("postgresql://postgres:postgres@localhost:5432/{}", env.db_name);
    let output = cli::run_shem_command_in_dir(
        &["introspect", "--database-url", &db_url, "--output", "schema"],
        &env.temp_path()
    )?;
    
    cli::assert_command_success(&output);
    
    // Verify view with aggregation was introspected
    let schema_content = std::fs::read_to_string(env.temp_path().join("schema/schema.sql"))?;
    assert!(schema_content.contains("CREATE VIEW user_post_counts"));
    assert!(schema_content.contains("COUNT(p.id) as post_count"));
    assert!(schema_content.contains("GROUP BY u.id, u.name"));
    
    // Clean up
    db::drop_test_db(&env.db_name).await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_view_with_subquery() -> Result<()> {
    let env = TestEnv::new()?;
    let pool = db::setup_test_db(&env.db_name).await?;
    
    // Create table and view with subquery
    db::execute_sql(&pool, sql::SIMPLE_TABLE).await?;
    db::execute_sql(&pool, sql::TABLE_WITH_FK).await?;
    db::execute_sql(&pool, r#"
        CREATE VIEW recent_posts AS
        SELECT p.*, u.name as author_name
        FROM posts p
        JOIN users u ON p.user_id = u.id
        WHERE p.created_at > (
            SELECT MAX(created_at) - INTERVAL '7 days'
            FROM posts
        );
    "#).await?;
    
    // Run introspect command
    let db_url = format!("postgresql://postgres:postgres@localhost:5432/{}", env.db_name);
    let output = cli::run_shem_command_in_dir(
        &["introspect", "--database-url", &db_url, "--output", "schema"],
        &env.temp_path()
    )?;
    
    cli::assert_command_success(&output);
    
    // Verify view with subquery was introspected
    let schema_content = std::fs::read_to_string(env.temp_path().join("schema/schema.sql"))?;
    assert!(schema_content.contains("CREATE VIEW recent_posts"));
    assert!(schema_content.contains("WHERE p.created_at > ("));
    assert!(schema_content.contains("SELECT MAX(created_at) - INTERVAL '7 days'"));
    
    // Clean up
    db::drop_test_db(&env.db_name).await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_view_with_window_functions() -> Result<()> {
    let env = TestEnv::new()?;
    let pool = db::setup_test_db(&env.db_name).await?;
    
    // Create table and view with window functions
    db::execute_sql(&pool, sql::SIMPLE_TABLE).await?;
    db::execute_sql(&pool, sql::TABLE_WITH_FK).await?;
    db::execute_sql(&pool, r#"
        CREATE VIEW user_post_rankings AS
        SELECT 
            u.id,
            u.name,
            p.title,
            p.created_at,
            ROW_NUMBER() OVER (PARTITION BY u.id ORDER BY p.created_at DESC) as post_rank
        FROM users u
        LEFT JOIN posts p ON u.id = p.user_id;
    "#).await?;
    
    // Run introspect command
    let db_url = format!("postgresql://postgres:postgres@localhost:5432/{}", env.db_name);
    let output = cli::run_shem_command_in_dir(
        &["introspect", "--database-url", &db_url, "--output", "schema"],
        &env.temp_path()
    )?;
    
    cli::assert_command_success(&output);
    
    // Verify view with window functions was introspected
    let schema_content = std::fs::read_to_string(env.temp_path().join("schema/schema.sql"))?;
    assert!(schema_content.contains("CREATE VIEW user_post_rankings"));
    assert!(schema_content.contains("ROW_NUMBER() OVER (PARTITION BY u.id ORDER BY p.created_at DESC)"));
    
    // Clean up
    db::drop_test_db(&env.db_name).await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_view_with_cte() -> Result<()> {
    let env = TestEnv::new()?;
    let pool = db::setup_test_db(&env.db_name).await?;
    
    // Create table and view with CTE
    db::execute_sql(&pool, sql::SIMPLE_TABLE).await?;
    db::execute_sql(&pool, sql::TABLE_WITH_FK).await?;
    db::execute_sql(&pool, r#"
        CREATE VIEW user_post_summary AS
        WITH user_stats AS (
            SELECT 
                user_id,
                COUNT(*) as total_posts,
                MAX(created_at) as last_post_date
            FROM posts
            GROUP BY user_id
        )
        SELECT 
            u.id,
            u.name,
            u.email,
            COALESCE(us.total_posts, 0) as total_posts,
            us.last_post_date
        FROM users u
        LEFT JOIN user_stats us ON u.id = us.user_id;
    "#).await?;
    
    // Run introspect command
    let db_url = format!("postgresql://postgres:postgres@localhost:5432/{}", env.db_name);
    let output = cli::run_shem_command_in_dir(
        &["introspect", "--database-url", &db_url, "--output", "schema"],
        &env.temp_path()
    )?;
    
    cli::assert_command_success(&output);
    
    // Verify view with CTE was introspected
    let schema_content = std::fs::read_to_string(env.temp_path().join("schema/schema.sql"))?;
    assert!(schema_content.contains("CREATE VIEW user_post_summary"));
    assert!(schema_content.contains("WITH user_stats AS ("));
    assert!(schema_content.contains("COALESCE(us.total_posts, 0) as total_posts"));
    
    // Clean up
    db::drop_test_db(&env.db_name).await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_view_with_comments() -> Result<()> {
    let env = TestEnv::new()?;
    let pool = db::setup_test_db(&env.db_name).await?;
    
    // Create table, view, and comments
    db::execute_sql(&pool, sql::SIMPLE_TABLE).await?;
    db::execute_sql(&pool, sql::VIEW).await?;
    db::execute_sql(&pool, "COMMENT ON VIEW active_users IS 'View showing only active users';").await?;
    
    // Run introspect command
    let db_url = format!("postgresql://postgres:postgres@localhost:5432/{}", env.db_name);
    let output = cli::run_shem_command_in_dir(
        &["introspect", "--database-url", &db_url, "--output", "schema"],
        &env.temp_path()
    )?;
    
    cli::assert_command_success(&output);
    
    // Verify view and comments were introspected
    let schema_content = std::fs::read_to_string(env.temp_path().join("schema/schema.sql"))?;
    assert!(schema_content.contains("CREATE VIEW active_users"));
    assert!(schema_content.contains("COMMENT ON VIEW active_users IS 'View showing only active users'"));
    
    // Clean up
    db::drop_test_db(&env.db_name).await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_view_with_security_barrier() -> Result<()> {
    let env = TestEnv::new()?;
    let pool = db::setup_test_db(&env.db_name).await?;
    
    // Create table and view with security barrier
    db::execute_sql(&pool, sql::SIMPLE_TABLE).await?;
    db::execute_sql(&pool, r#"
        CREATE VIEW secure_users WITH (security_barrier = true) AS
        SELECT id, name, email
        FROM users
        WHERE status = 'active';
    "#).await?;
    
    // Run introspect command
    let db_url = format!("postgresql://postgres:postgres@localhost:5432/{}", env.db_name);
    let output = cli::run_shem_command_in_dir(
        &["introspect", "--database-url", &db_url, "--output", "schema"],
        &env.temp_path()
    )?;
    
    cli::assert_command_success(&output);
    
    // Verify view with security barrier was introspected
    let schema_content = std::fs::read_to_string(env.temp_path().join("schema/schema.sql"))?;
    assert!(schema_content.contains("CREATE VIEW secure_users WITH (security_barrier = true)"));
    
    // Clean up
    db::drop_test_db(&env.db_name).await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_view_with_check_option() -> Result<()> {
    let env = TestEnv::new()?;
    let pool = db::setup_test_db(&env.db_name).await?;
    
    // Create table and view with check option
    db::execute_sql(&pool, sql::SIMPLE_TABLE).await?;
    db::execute_sql(&pool, r#"
        CREATE VIEW active_users_view AS
        SELECT id, name, email, status
        FROM users
        WHERE status = 'active'
        WITH CHECK OPTION;
    "#).await?;
    
    // Run introspect command
    let db_url = format!("postgresql://postgres:postgres@localhost:5432/{}", env.db_name);
    let output = cli::run_shem_command_in_dir(
        &["introspect", "--database-url", &db_url, "--output", "schema"],
        &env.temp_path()
    )?;
    
    cli::assert_command_success(&output);
    
    // Verify view with check option was introspected
    let schema_content = std::fs::read_to_string(env.temp_path().join("schema/schema.sql"))?;
    assert!(schema_content.contains("CREATE VIEW active_users_view"));
    assert!(schema_content.contains("WITH CHECK OPTION"));
    
    // Clean up
    db::drop_test_db(&env.db_name).await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_view_with_local_check_option() -> Result<()> {
    let env = TestEnv::new()?;
    let pool = db::setup_test_db(&env.db_name).await?;
    
    // Create table and view with local check option
    db::execute_sql(&pool, sql::SIMPLE_TABLE).await?;
    db::execute_sql(&pool, r#"
        CREATE VIEW active_users_local AS
        SELECT id, name, email, status
        FROM users
        WHERE status = 'active'
        WITH LOCAL CHECK OPTION;
    "#).await?;
    
    // Run introspect command
    let db_url = format!("postgresql://postgres:postgres@localhost:5432/{}", env.db_name);
    let output = cli::run_shem_command_in_dir(
        &["introspect", "--database-url", &db_url, "--output", "schema"],
        &env.temp_path()
    )?;
    
    cli::assert_command_success(&output);
    
    // Verify view with local check option was introspected
    let schema_content = std::fs::read_to_string(env.temp_path().join("schema/schema.sql"))?;
    assert!(schema_content.contains("CREATE VIEW active_users_local"));
    assert!(schema_content.contains("WITH LOCAL CHECK OPTION"));
    
    // Clean up
    db::drop_test_db(&env.db_name).await?;
    Ok(())
}

#[tokio::test]
async fn test_introspect_view_with_cascaded_check_option() -> Result<()> {
    let env = TestEnv::new()?;
    let pool = db::setup_test_db(&env.db_name).await?;
    
    // Create table and view with cascaded check option
    db::execute_sql(&pool, sql::SIMPLE_TABLE).await?;
    db::execute_sql(&pool, r#"
        CREATE VIEW active_users_cascaded AS
        SELECT id, name, email, status
        FROM users
        WHERE status = 'active'
        WITH CASCADED CHECK OPTION;
    "#).await?;
    
    // Run introspect command
    let db_url = format!("postgresql://postgres:postgres@localhost:5432/{}", env.db_name);
    let output = cli::run_shem_command_in_dir(
        &["introspect", "--database-url", &db_url, "--output", "schema"],
        &env.temp_path()
    )?;
    
    cli::assert_command_success(&output);
    
    // Verify view with cascaded check option was introspected
    let schema_content = std::fs::read_to_string(env.temp_path().join("schema/schema.sql"))?;
    assert!(schema_content.contains("CREATE VIEW active_users_cascaded"));
    assert!(schema_content.contains("WITH CASCADED CHECK OPTION"));
    
    // Clean up
    db::drop_test_db(&env.db_name).await?;
    Ok(())
} 