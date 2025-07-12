use crate::config::Config;
use anyhow::{Context, Result};
use std::path::PathBuf;
use tracing::{debug, info};

pub async fn execute(path: PathBuf, _config: &Config) -> Result<()> {
    // Create base directory if it doesn't exist
    std::fs::create_dir_all(&path).context("Failed to create base directory")?;

    // Create schema directory
    let schema_path = path.join("schema");
    std::fs::create_dir_all(&schema_path).context("Failed to create schema directory")?;

    // Create migrations directory
    let migrations_path = path.join("migrations");
    std::fs::create_dir_all(&migrations_path).context("Failed to create migrations directory")?;

    // Create config file
    let config_path = path.join("shem.toml");
    let config_content = r#"# Shem Configuration File
# This file configures your declarative schema management

# Database connection URL
database_url = "postgresql://postgres:postgres@localhost:5432/myapp_dev"

# Directory paths
schema_dir = "schema"
migrations_dir = "migrations"

[declarative]
enabled = true
# Schema files to include (supports glob patterns)
schema_paths = ["./schema/*.sql"]
# Port for shadow database (used for safe schema diffing)
shadow_port = 5433
auto_cleanup = true

[declarative.safety_checks]
# Warn about potentially destructive operations
warn_on_drop = true
# Require confirmation for destructive operations
require_confirmation = true
# Create backup before applying migrations
backup_before_apply = false

[postgres]
# Schemas to include in search path
search_path = ["public"]
# Extensions to include
extensions = []
# Tables to exclude from schema operations
exclude_tables = []
# Schemas to exclude from schema operations
exclude_schemas = ["information_schema", "pg_catalog"]
"#;

    std::fs::write(&config_path, config_content).context("Failed to write config file")?;

    // Create initial schema file
    let initial_schema = schema_path.join("00_initial.sql");
    let schema_content = r#"-- Initial schema file
-- Add your initial schema definitions here
-- Files in this directory are processed in alphabetical order
-- Use numeric prefixes to control the order, e.g.:
--   00_initial.sql
--   01_users.sql
--   02_profiles.sql
--   etc.

-- Recommended extensions (if not already installed)
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- Example table definition using UUID
-- CREATE TABLE example (
--     id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
--     name TEXT NOT NULL,
--     created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
-- );
"#;

    std::fs::write(&initial_schema, schema_content)
        .context("Failed to write initial schema file")?;

    // Create README.md in schema directory explaining the convention
    let readme_path = schema_path.join("README.md");
    let readme_content = r#"# Schema Directory

This directory contains SQL schema files that define your database structure. Files are processed in alphabetical order, so you can control the order of execution using numeric prefixes.

## Naming Convention

Use numeric prefixes to control the order of schema files:

- `00_initial.sql` - Initial schema setup
- `01_users.sql` - User-related tables
- `02_profiles.sql` - Profile-related tables
- etc.

## File Organization

You can organize your schema files by feature or domain:

- `00_initial.sql` - Initial setup, extensions, types
- `01_auth.sql` - Authentication tables
- `02_users.sql` - User management
- `03_profiles.sql` - User profiles
- `04_content.sql` - Content management
- etc.

## Best Practices

1. Use meaningful prefixes to indicate dependencies
2. Keep related tables in the same file
3. Use descriptive names that indicate the purpose
4. Add comments to explain complex schemas
5. Consider dependencies when ordering files

## Example

```sql
-- 01_users.sql
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    email TEXT UNIQUE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 02_profiles.sql
CREATE TABLE profiles (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id),
    name TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
```
"#;

    std::fs::write(&readme_path, readme_content).context("Failed to write schema README")?;

    info!("Initialized schema project at {}", path.display());
    info!("Created schema directory at {}", schema_path.display());
    info!(
        "Created migrations directory at {}",
        migrations_path.display()
    );
    info!("Created config file at {}", config_path.display());
    info!(
        "Created initial schema file at {}",
        initial_schema.display()
    );
    info!("Created schema README at {}", readme_path.display());
    info!("");
    info!("Next steps:");
    info!("1. Edit shem.toml and set your database URL");
    info!("2. Modify schema/00_initial.sql with your schema definitions");
    info!("3. Run 'cargo run --bin shem -- diff' to generate your first migration");

    Ok(())
}
