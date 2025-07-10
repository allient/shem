use crate::{DatabaseConnection, DatabaseDriver, PostgresDriver};
use uuid::Uuid;

const ADMIN_URL: &str = "postgresql://postgres:postgres@localhost:5432/postgres";

/// Holds the test-database name + connection.
/// Call `.cleanup().await` at the end of the test.
pub struct TestDb {
    pub name: String,
    pub conn: Box<dyn DatabaseConnection>,
}

impl TestDb {
    /// Creates a brand-new database, connects to it and returns the handle.
    pub async fn new() -> Result<Self, Box<dyn std::error::Error>> {
        // unique, lowercase, no dashes
        let db_name = format!("test_db_{}", Uuid::new_v4().simple());
        let driver = PostgresDriver::new();

        // 1) connect to the *admin* database and create the new one
        let admin = driver.connect(ADMIN_URL).await?;
        admin
            .execute(&format!("CREATE DATABASE \"{db_name}\""))
            .await?;

        // 2) connect to the freshly created database
        let conn = driver
            .connect(&format!(
                "postgresql://postgres:postgres@localhost:5432/{db_name}"
            ))
            .await?;

        Ok(Self {
            name: db_name,
            conn,
        })
    }

    /// Drops the test database (you must close all connections first).
    pub async fn cleanup(self) -> Result<(), Box<dyn std::error::Error>> {
        drop(self.conn); // close client first

        let driver = PostgresDriver::new();
        let admin = driver.connect(ADMIN_URL).await?;

        // Terminate any straggling connections the test might have left
        admin
            .execute(&format!(
                r#"
                SELECT pg_terminate_backend(pid)
                FROM   pg_stat_activity
                WHERE  datname = '{name}'
                  AND  pid <> pg_backend_pid();
                "#,
                name = self.name
            ))
            .await?;

        admin
            .execute(&format!("DROP DATABASE IF EXISTS \"{}\"", self.name))
            .await?;
        Ok(())
    }
}
